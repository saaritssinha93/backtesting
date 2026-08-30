"""Backfill 5-minute futures OHLCV + OI to the full life of each contract.

Two limits shape this script, both measured against the live API on 2026-08-10:

* ``continuous=True`` is rejected on every intraday interval ("invalid interval
  for continuous data"), so there is no stitched intraday series. Intraday depth
  is bounded by the life of an individual contract.
* A single ``5minute`` request may span at most 100 days.

A near-month contract is introduced roughly three months before it expires, so
its own history is ~75 trading days -- e.g. RELIANCE26AUGFUT begins 2026-05-27.
The live fetcher's ``run_bootstrap`` only reaches back ``bootstrap_days=60``, so
it leaves real, retrievable history on the table.

The larger gap this script closes is the **rollover gap**. The live fetcher only
tracks the current near month, so once AUG expires its file stops growing and no
SEP file exists for the period when SEP was still a far month. Backtesting a
near-month series across a rollover then has nothing to read. Passing
``--months all`` captures the next and far months *while they are still
tradable*, so by the time each becomes the front month its intraday history is
already on disk.

Output goes to the existing ``raw_contracts_5m/`` store in the existing schema,
so fno_oi_feature_ranker.py and fno_oi_eod_qc.py consume it unchanged.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Iterable

import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_backfill_5min"

REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_backfill_5min.md"

# Kite hard limit for a single ``5minute`` request.
MAX_REQUEST_DAYS = 100

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


@dataclass
class AppRuntime:
    app_name: str
    client: Any
    pace_seconds: float
    _last_call_at: float = 0.0
    _pace_lock: threading.Lock = field(default_factory=threading.Lock)

    def pace(self) -> None:
        with self._pace_lock:
            wait = self.pace_seconds - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()


def _build_app_runtimes(args: argparse.Namespace) -> list[AppRuntime]:
    credentials = common.discover_kite_credentials(max_apps=args.max_apps)
    runtimes: list[AppRuntime] = []
    failures: list[str] = []
    for credential in credentials:
        try:
            client = common.make_kite_client(credential, timeout_sec=args.timeout_sec)
            runtimes.append(
                AppRuntime(
                    app_name=credential.app_name,
                    client=client,
                    pace_seconds=max(0.34, float(args.request_interval_sec)),
                )
            )
        except Exception as exc:
            failures.append(f"{credential.app_name}:{type(exc).__name__}:{exc}")
    if not runtimes:
        raise RuntimeError(f"No usable Kite clients. Failures: {failures}")
    if failures:
        print(f"[AUTH][WARN] unusable apps: {failures}", flush=True)
    return runtimes


def market_is_open(moment: datetime, holidays: set[date]) -> bool:
    current = moment.astimezone(common.IST)
    if not common.is_trading_day(current.date(), holidays):
        return False
    return MARKET_OPEN <= current.time() <= MARKET_CLOSE


def plan_windows(start: date, end: date, *, max_days: int = MAX_REQUEST_DAYS) -> list[tuple[date, date]]:
    if start > end:
        return []
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=max_days - 1))
        windows.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return windows


def select_contracts(
    months: str,
    limit: int,
    underlyings: str,
    contract_months: str = "",
) -> pd.DataFrame:
    """Pick which contracts to backfill.

    ``near``  -- current front month only (matches the live fetcher's scope)
    ``near2`` -- front + next month
    ``all``   -- every currently tradable contract month (front, next, far)
    """

    master_path = common.MASTER_DIR / "latest_instrument_master.parquet"
    universe_path = common.UNIVERSE_DIR / "latest_near_month.parquet"
    if not universe_path.exists():
        raise FileNotFoundError(
            f"Near-month universe missing: {universe_path}. Run fno_oi_universe.py first."
        )
    near = pd.read_parquet(universe_path)

    requested_contract_months = {
        value.strip()
        for value in contract_months.split(",")
        if value.strip()
    }
    if requested_contract_months:
        registry_path = common.CONTRACT_REGISTRY_PATH
        if not registry_path.exists():
            raise FileNotFoundError(f"Contract registry missing: {registry_path}")
        frame = pd.read_parquet(registry_path)
        required = {
            "underlying",
            "tradingsymbol",
            "instrument_token",
            "exchange_token",
            "expiry",
            "contract_month",
            "lot_size",
            "tick_size",
            "is_index_future",
        }
        missing_columns = sorted(required - set(frame.columns))
        if missing_columns:
            raise ValueError(
                f"Contract registry is missing columns: {', '.join(missing_columns)}"
            )
        frame = frame.loc[
            frame["contract_month"].astype(str).isin(requested_contract_months)
        ].copy()
        frame = frame.drop_duplicates("tradingsymbol", keep="last")
        missing_months = requested_contract_months - set(
            frame["contract_month"].astype(str)
        )
        if missing_months:
            print(
                f"[PLAN][WARN] contract months not in registry: {sorted(missing_months)}",
                flush=True,
            )
    elif months == "near":
        frame = near.copy()
    else:
        if not master_path.exists():
            raise FileNotFoundError(f"Instrument master missing: {master_path}")
        master = pd.read_parquet(master_path)
        master["expiry"] = pd.to_datetime(master["expiry"], errors="coerce")
        ordered = master.sort_values(["underlying", "expiry"], kind="stable")
        keep = 2 if months == "near2" else 3
        frame = ordered.groupby("underlying", sort=True, as_index=False).head(keep).copy()

    if underlyings:
        wanted = {s.strip().upper() for s in underlyings.split(",") if s.strip()}
        frame = frame.loc[frame["underlying"].isin(wanted)].copy()
        missing = wanted - set(frame["underlying"])
        if missing:
            print(f"[PLAN][WARN] not in master: {sorted(missing)}", flush=True)
    frame = frame.sort_values(["underlying", "expiry"], kind="stable").reset_index(drop=True)
    if limit > 0:
        frame = frame.head(limit).reset_index(drop=True)
    return frame


def existing_span(symbol: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    path = common.raw_contract_path(symbol)
    if not path.exists():
        return None, None, 0
    try:
        frame = pd.read_parquet(path, columns=["timestamp"])
    except Exception:
        return None, None, 0
    if frame.empty:
        return None, None, 0
    stamps = common._to_ist(frame["timestamp"]).dropna()
    if stamps.empty:
        return None, None, 0
    return stamps.min(), stamps.max(), int(len(frame))


def _historical_5min(
    runtime: AppRuntime,
    token: int,
    from_dt: date,
    to_dt: date,
    *,
    max_retries: int,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, max(1, int(max_retries)) + 1):
        try:
            runtime.pace()
            return runtime.client.historical_data(
                int(token),
                from_dt,
                to_dt,
                "5minute",
                continuous=False,
                oi=True,
            )
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            if attempt >= max_retries:
                break
            if "429" in message or "too many requests" in message or "rate limit" in message:
                delay = max(2.0, 2.0**attempt)
            else:
                delay = min(8.0, 0.75 * (2 ** (attempt - 1)))
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def backfill_one(
    runtime: AppRuntime,
    contract: pd.Series,
    *,
    start: date,
    end: date,
    max_retries: int,
) -> dict[str, Any]:
    symbol = str(contract["tradingsymbol"])
    started = time.monotonic()
    before_min, before_max, before_rows = existing_span(symbol)
    try:
        windows = plan_windows(start, end)
        collected: list[pd.DataFrame] = []
        for window_start, window_stop in windows:
            records = _historical_5min(
                runtime,
                int(contract["instrument_token"]),
                window_start,
                window_stop,
                max_retries=max_retries,
            )
            if not records:
                continue
            rows = common.normalize_historical_candles(
                records,
                contract,
                fetch_timestamp=common.now_ist(),
                slot_end=None,
            )
            if not rows.empty:
                collected.append(rows)

        if not collected:
            return {
                "tradingsymbol": symbol,
                "underlying": str(contract["underlying"]),
                "app": runtime.app_name,
                "state": "NO_CANDLE",
                "added_rows": 0,
                "total_rows": before_rows,
                "requests": len(windows),
                "elapsed_sec": time.monotonic() - started,
                "error": "",
            }

        incoming = pd.concat(collected, ignore_index=True, sort=False)
        combined = common.append_contract_rows(common.raw_contract_path(symbol), incoming)
        after_min, after_max, after_rows = existing_span(symbol)
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract["underlying"]),
            "app": runtime.app_name,
            "state": "WRITTEN",
            "added_rows": int(after_rows - before_rows),
            "total_rows": int(after_rows or len(combined)),
            "first_ts": str(after_min)[:16] if after_min is not None else "",
            "last_ts": str(after_max)[:16] if after_max is not None else "",
            "extended_by_days": (
                int((before_min - after_min).days)
                if before_min is not None and after_min is not None
                else None
            ),
            "requests": len(windows),
            "elapsed_sec": time.monotonic() - started,
            "error": "",
        }
    except Exception as exc:
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract["underlying"]),
            "app": runtime.app_name,
            "state": "FAILED",
            "added_rows": 0,
            "total_rows": before_rows,
            "requests": 0,
            "elapsed_sec": time.monotonic() - started,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _partition_rows(frame: pd.DataFrame, count: int) -> list[list[pd.Series]]:
    partitions: list[list[pd.Series]] = [[] for _ in range(max(1, count))]
    for index, (_, row) in enumerate(frame.iterrows()):
        partitions[index % len(partitions)].append(row)
    return partitions


def backfill_all(
    contracts: pd.DataFrame,
    runtimes: list[AppRuntime],
    *,
    start: date,
    end: date,
    max_retries: int,
) -> list[dict[str, Any]]:
    partitions = _partition_rows(contracts, len(runtimes))
    total = int(len(contracts))
    done = 0
    done_lock = threading.Lock()

    def _run_partition(runtime: AppRuntime, rows: Iterable[pd.Series]) -> list[dict[str, Any]]:
        nonlocal done
        results: list[dict[str, Any]] = []
        for contract in rows:
            outcome = backfill_one(
                runtime,
                contract,
                start=start,
                end=end,
                max_retries=max_retries,
            )
            results.append(outcome)
            with done_lock:
                done += 1
                position = done
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                app=runtime.app_name,
                contract=contract["tradingsymbol"],
                progress=f"{position}/{total}",
            )
            if position % 25 == 0 or position == total:
                print(f"[PROGRESS] {position}/{total}", flush=True)
        return results

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=len(runtimes), thread_name_prefix="fno-oi-bf5m"
    ) as pool:
        futures = [
            pool.submit(_run_partition, runtime, partition)
            for runtime, partition in zip(runtimes, partitions)
            if partition
        ]
        for future in as_completed(futures):
            outcomes.extend(future.result())
    return sorted(outcomes, key=lambda item: str(item["tradingsymbol"]))


def render_report(
    outcomes: list[dict[str, Any]],
    *,
    start: date,
    end: date,
    months: str,
    duration_sec: float,
) -> str:
    states: dict[str, int] = {}
    for outcome in outcomes:
        states[outcome["state"]] = states.get(outcome["state"], 0) + 1
    written = [o for o in outcomes if o["state"] == "WRITTEN"]
    failed = [o for o in outcomes if o["state"] == "FAILED"]
    added = sum(int(o.get("added_rows", 0)) for o in outcomes)
    requests = sum(int(o.get("requests", 0)) for o in outcomes)

    lines = [
        "# FnO 5-Minute OI Backfill",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Requested range: {start.isoformat()} -> {end.isoformat()}",
        f"- Contract months: {months}",
        f"- Contracts processed: {len(outcomes)}",
        f"- Kite requests: {requests}",
        f"- Rows added: {added:,}",
        f"- Duration: {duration_sec:.1f}s",
        "",
        "## Outcome",
        "",
        "| State | Count |",
        "| --- | ---: |",
    ]
    for state in sorted(states):
        lines.append(f"| {state} | {states[state]} |")

    if written:
        deepest = sorted(written, key=lambda o: str(o.get("first_ts", "9999")))[:15]
        lines += [
            "",
            "## Deepest contract histories",
            "",
            "| Contract | First bar | Last bar | Rows | Added |",
            "| --- | --- | --- | ---: | ---: |",
        ]
        for outcome in deepest:
            lines.append(
                f"| {outcome['tradingsymbol']} | {outcome.get('first_ts', '')} | "
                f"{outcome.get('last_ts', '')} | {outcome.get('total_rows', 0):,} | "
                f"{outcome.get('added_rows', 0):,} |"
            )

    if failed:
        lines += ["", "## Failures", "", "| Contract | Error |", "| --- | --- |"]
        for outcome in failed[:20]:
            lines.append(f"| {outcome['tradingsymbol']} | {outcome['error'][:120]} |")

    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=MAX_REQUEST_DAYS,
        help=f"Calendar days to reach back (default {MAX_REQUEST_DAYS}). Contracts "
        "simply return nothing before they were introduced.",
    )
    parser.add_argument("--from-date", default="", help="Explicit start date; overrides --days.")
    parser.add_argument("--to-date", default="", help="End date (default: today).")
    parser.add_argument(
        "--months",
        choices=("near", "near2", "all"),
        default="all",
        help="Which contract months to backfill. 'all' captures next/far months "
        "now so the near-month series survives future rollovers (default).",
    )
    parser.add_argument(
        "--contract-months",
        default="",
        help=(
            "Comma-separated registry months such as 2026-08,2026-09. When set, "
            "selects those exact active or expired contracts from the retained "
            "contract registry and overrides --months."
        ),
    )
    parser.add_argument("--underlyings", default="", help="Comma-separated subset.")
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N contracts.")
    parser.add_argument(
        "--allow-market-hours",
        action="store_true",
        help="Permit running between 09:15 and 15:30. Off by default so the "
        "backfill cannot starve the live 5-minute FnO feed.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and exit.")
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=3)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()

    end = pd.Timestamp(args.to_date).date() if args.to_date else common.now_ist().date()
    if args.from_date:
        start = pd.Timestamp(args.from_date).date()
    else:
        # Inclusive of both endpoints, so --days 100 is exactly one request.
        start = end - timedelta(days=max(1, int(args.days)) - 1)

    contracts = select_contracts(
        args.months,
        args.limit,
        args.underlyings,
        args.contract_months,
    )
    if contracts.empty:
        print("[PLAN] Nothing to do.", flush=True)
        return 0

    windows = plan_windows(start, end)
    print(
        f"[PLAN] {len(contracts)} contracts ({args.months}) | {start} -> {end} | "
        f"{len(windows)} window(s)/contract | ~{len(contracts) * len(windows)} requests",
        flush=True,
    )
    if args.dry_run:
        preview = contracts.head(10)
        for _, row in preview.iterrows():
            existing_min, existing_max, rows = existing_span(str(row["tradingsymbol"]))
            print(
                f"  {row['tradingsymbol']:<24} stored={rows:>6} "
                f"{str(existing_min)[:16]} .. {str(existing_max)[:16]}",
                flush=True,
            )
        if len(contracts) > 10:
            print(f"  ... +{len(contracts) - 10} more", flush=True)
        return 0

    holidays = common.load_holidays()
    if market_is_open(common.now_ist(), holidays) and not args.allow_market_hours:
        print(
            "[GUARD] Market is open. This backfill shares the Kite rate limit with "
            "the live 5-minute FnO feed, so it is blocked by default. Re-run after "
            "15:30 IST, or pass --allow-market-hours to override.",
            flush=True,
        )
        common.publish_status(SESSION, "SKIPPED_MARKET_HOURS")
        return 0

    common.publish_status(
        SESSION,
        "RUNNING",
        contracts=int(len(contracts)),
        months=args.months,
        from_date=start.isoformat(),
        to_date=end.isoformat(),
    )
    try:
        runtimes = _build_app_runtimes(args)
        outcomes = backfill_all(
            contracts,
            runtimes,
            start=start,
            end=end,
            max_retries=args.max_retries,
        )
        duration = time.monotonic() - started
        report = render_report(
            outcomes, start=start, end=end, months=args.months, duration_sec=duration
        )
        common.atomic_write_text(REPORT_PATH, report)

        failed = sum(1 for o in outcomes if o["state"] == "FAILED")
        written = sum(1 for o in outcomes if o["state"] == "WRITTEN")
        added = sum(int(o.get("added_rows", 0)) for o in outcomes)
        common.publish_status(
            SESSION,
            "FAILED" if failed and not written else "SUCCESS",
            contracts=int(len(outcomes)),
            written=written,
            failed=failed,
            added_rows=added,
            duration_sec=round(duration, 2),
        )
        print(
            f"[DONE] written={written} failed={failed} added_rows={added:,} in {duration:.1f}s",
            flush=True,
        )
        print(f"[REPORT] {REPORT_PATH}", flush=True)
        return 1 if failed and not written else 0
    except Exception as exc:
        common.publish_status(SESSION, "FAILED", error=f"{type(exc).__name__}: {exc}")
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
