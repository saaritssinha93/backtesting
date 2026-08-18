"""Fetch and maintain the 1-minute futures history used by the confirmation backtests.

The live pipeline stores 5-minute bars only. The 1-minute confirmation candle
needs its own store, kept here as one file per contract covering the full
window:

    <runtime>/fno_oi/raw_contracts_1m_hist/<SYMBOL>_1minute.parquet

Kite caps a ``minute`` request at 60 days, so a longer window is chunked. Runs
are incremental: an existing file is extended from its last stored bar rather
than refetched, so the daily top-up is one short request per contract.

Blocked during market hours by default -- it shares the Kite rate limit with the
live 5-minute FnO feed.
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as dtime, timedelta
from typing import Any

import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_fetch_1m_history"
HIST_DIR = common.FNO_ROOT / "raw_contracts_1m_hist"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_1m_history.md"

MAX_REQUEST_DAYS = 60          # Kite hard limit for the 'minute' interval
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


def contract_path(symbol: str):
    return HIST_DIR / f"{common.safe_contract_stem(symbol)}_1minute.parquet"


def plan_windows(start: date, end: date) -> list[tuple[date, date]]:
    if start > end:
        return []
    out, cursor = [], start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=MAX_REQUEST_DAYS - 1))
        out.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return out


def existing_last_day(symbol: str) -> date | None:
    path = contract_path(symbol)
    if not path.exists():
        return None
    try:
        frame = pd.read_parquet(path, columns=["ts"])
    except Exception:
        return None
    if frame.empty:
        return None
    stamps = pd.to_datetime(frame["ts"], utc=True).dt.tz_convert(common.IST)
    return stamps.max().date()


def market_is_open(moment: datetime, holidays: set[date]) -> bool:
    current = moment.astimezone(common.IST)
    if not common.is_trading_day(current.date(), holidays):
        return False
    return MARKET_OPEN <= current.time() <= MARKET_CLOSE


def fetch_one(client: Any, row: dict[str, Any], start: date, end: date,
              *, pace: float, max_retries: int) -> dict[str, Any]:
    symbol = row["tradingsymbol"]
    frames: list[pd.DataFrame] = []
    requests = 0
    for a, b in plan_windows(start, end):
        for attempt in range(1, max_retries + 1):
            try:
                time.sleep(pace)
                records = client.historical_data(
                    int(row["instrument_token"]), a, b, "minute", continuous=False, oi=True
                )
                requests += 1
                if records:
                    frames.append(pd.DataFrame(records))
                break
            except Exception as exc:
                if attempt == max_retries:
                    return {"tradingsymbol": symbol, "state": "FAILED",
                            "error": f"{type(exc).__name__}: {exc}", "added": 0,
                            "requests": requests}
                time.sleep(min(8.0, 1.5 * attempt))
    if not frames:
        return {"tradingsymbol": symbol, "state": "NO_DATA", "added": 0, "requests": requests}

    incoming = pd.concat(frames, ignore_index=True)
    incoming["ts"] = pd.to_datetime(incoming["date"], utc=True)
    path = contract_path(symbol)
    before = 0
    if path.exists():
        try:
            previous = pd.read_parquet(path)
            previous["ts"] = pd.to_datetime(previous["ts"], utc=True)
            before = len(previous)
            incoming = pd.concat([previous, incoming], ignore_index=True)
        except Exception:
            pass
    incoming = (
        incoming.drop_duplicates(subset=["ts"], keep="last")
        .sort_values("ts")
        .reset_index(drop=True)
    )
    common.atomic_write_parquet(incoming, path)
    return {"tradingsymbol": symbol, "state": "WRITTEN",
            "added": int(len(incoming) - before), "rows": int(len(incoming)),
            "requests": requests}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", default="2026-05-27")
    parser.add_argument("--to-date", default="", help="Default: today.")
    parser.add_argument("--contract-month", default="26AUG")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore stored bars and refetch the whole window.")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)

    HIST_DIR.mkdir(parents=True, exist_ok=True)
    end = pd.Timestamp(args.to_date).date() if args.to_date else common.now_ist().date()
    floor = pd.Timestamp(args.from_date).date()

    universe = pd.read_parquet(common.UNIVERSE_DIR / "latest_near_month.parquet")
    if args.contract_month:
        universe = universe.loc[
            universe["tradingsymbol"].str.contains(args.contract_month, case=False, na=False)
        ]
    if args.limit > 0:
        universe = universe.head(args.limit)
    rows = universe[["tradingsymbol", "instrument_token"]].to_dict("records")
    if not rows:
        print("[PLAN] nothing to fetch", flush=True)
        return 0

    holidays = common.load_holidays()
    if market_is_open(common.now_ist(), holidays) and not args.allow_market_hours:
        print("[GUARD] Market is open; this shares the Kite rate limit with the live "
              "feed. Re-run after 15:30 IST or pass --allow-market-hours.", flush=True)
        common.publish_status(SESSION, "SKIPPED_MARKET_HOURS")
        return 0

    plans = []
    for row in rows:
        start = floor
        if not args.full_refresh:
            last = existing_last_day(row["tradingsymbol"])
            if last is not None:
                if last >= end:
                    continue                       # already current
                start = max(floor, last)           # re-pull the last day to close gaps
        plans.append((row, start))
    if not plans:
        print(f"[PLAN] all {len(rows)} contracts already current through {end}", flush=True)
        return 0
    print(f"[PLAN] {len(plans)} contracts to update | window -> {end}", flush=True)

    common.publish_status(SESSION, "RUNNING", contracts=len(plans), to_date=end.isoformat())
    started = time.monotonic()
    credentials = common.discover_kite_credentials(max_apps=args.max_apps)
    clients = [common.make_kite_client(c, timeout_sec=args.timeout_sec) for c in credentials]
    parts = [plans[i::len(clients)] for i in range(len(clients))]
    pace = max(0.34, args.request_interval_sec)

    def work(args_tuple):
        client, chunk = args_tuple
        out = []
        for row, start in chunk:
            out.append(fetch_one(client, row, start, end,
                                 pace=pace, max_retries=args.max_retries))
        return out

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(clients)) as pool:
        for chunk in pool.map(work, zip(clients, parts)):
            outcomes.extend(chunk)
            print(f"[1M] {len(outcomes)}/{len(plans)}", flush=True)

    written = sum(1 for o in outcomes if o["state"] == "WRITTEN")
    failed = [o for o in outcomes if o["state"] == "FAILED"]
    added = sum(int(o.get("added", 0)) for o in outcomes)
    duration = time.monotonic() - started

    report = [
        "# FnO 1-Minute History", "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Window end: {end}", f"- Contracts updated: {written}/{len(plans)}",
        f"- Rows added: {added:,}", f"- Failures: {len(failed)}",
        f"- Duration: {duration:.1f}s", f"- Path: `{HIST_DIR}`", "",
    ]
    if failed:
        report += ["## Failures", "", "| Contract | Error |", "| --- | --- |"]
        report += [f"| {o['tradingsymbol']} | {o.get('error', '')[:110]} |" for o in failed[:20]]
    common.atomic_write_text(REPORT_PATH, "\n".join(report) + "\n")

    common.publish_status(SESSION, "FAILED" if failed and not written else "SUCCESS",
                          written=written, failed=len(failed), added_rows=added,
                          duration_sec=round(duration, 1))
    print(f"[DONE] written={written} failed={len(failed)} added_rows={added:,} "
          f"in {duration:.0f}s", flush=True)
    return 1 if failed and not written else 0


if __name__ == "__main__":
    raise SystemExit(main())
