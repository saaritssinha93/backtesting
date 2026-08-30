# -*- coding: utf-8 -*-
"""
eqidv2_eod_scheduler_for_1min_data_live.py
==========================================
Lightweight live scheduler for the 1-minute data fetch, surfaced on the log
dashboard as the ``eod_1min_data`` card.

Design (per 2026-06-17 request):
- Weekdays only (weekends + NSE holidays skipped).
- Runs once per slot starting at 09:17:30 IST, then every 5 minutes, with the
  last slot at or before 15:40 IST. Because each slot does an INCREMENTAL fetch
  from the last stored 1-min candle, running every 5 minutes guarantees that
  *every* 1-min candle in the session is captured (a 5-minute slot picks up the
  ~5 new 1-min bars closed since the previous slot).
- Uses all 8 Kite apps: the underlying core fetcher
  (trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py)
  builds the app1..app8 session pool in setup_kite_session() and round-robins
  every per-ticker request across it (get_kite_client_rr).

Why a thin wrapper (not a clone of the 5-min minimal scheduler):
- The 1-min core already owns the 8-app pool, incremental fetch, indicator
  computation, freshness skip and verification. This scheduler only adds the
  timing loop, the trading-calendar gate and the dashboard freshness file.

Run:
    python eqidv2_eod_scheduler_for_1min_data_live.py
Optional:
    python eqidv2_eod_scheduler_for_1min_data_live.py \
        --first-slot 09:17:30 --interval-min 5 --end-cutoff 15:40 --max-workers 16
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------
# Locate the eqidv2 folder and import the 1-min core fetcher.
# ---------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
# The core resolves api_key*.txt / access_token*.txt relative to CWD, so make
# sure we run from the eqidv2 folder regardless of how we were launched.
os.chdir(SCRIPT_DIR)

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as core  # noqa: E402

# ---------------------------------------------------------------------
# Universe loader guard (mirrors the 5-min minimal scheduler fix):
# filtered_stocks_MIS_v2.selected_stocks can be a {SYMBOL: weight} dict whose
# small numeric values the core may mistake for instrument tokens (int(0.6)->0,
# which then fails the `if not tok: skip` guard inside run_mode). If the token
# map looks like weights/flags rather than real tokens, drop it so the core
# fetches real tokens via kite.instruments().
# ---------------------------------------------------------------------
_orig_load_universe = getattr(core, "load_stocks_universe", None)
RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
FIVE_MIN_UNIVERSE_MANIFEST = Path(
    os.getenv(
        "EQIDV2_1M_FEED_UNIVERSE_MANIFEST",
        str(RUNTIME_ROOT / "runtime_status" / "feed_universe_5m.json"),
    )
)


def _canonical_symbols(values) -> list[str]:
    return sorted({str(value).strip().upper() for value in values if str(value).strip()})


def _universe_sha256(symbols: list[str]) -> str:
    return hashlib.sha256("\n".join(_canonical_symbols(symbols)).encode("utf-8")).hexdigest()


def _five_min_universe_contract() -> dict:
    """Read and verify the authoritative universe published by the 5-minute feed."""

    try:
        payload = json.loads(FIVE_MIN_UNIVERSE_MANIFEST.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "symbols": []}
    symbols = _canonical_symbols(payload.get("symbols", []))
    observed_hash = _universe_sha256(symbols)
    expected_count = int(payload.get("universe_count", -1))
    expected_hash = str(payload.get("universe_sha256", "")).strip().lower()
    ok = bool(
        payload.get("schema_version") == "eqidv2_5m_feed_universe_v1"
        and symbols
        and expected_count == len(symbols)
        and expected_hash == observed_hash
    )
    return {
        "ok": ok,
        "symbols": symbols,
        "universe_count": len(symbols),
        "universe_sha256": observed_hash,
        "manifest_path": str(FIVE_MIN_UNIVERSE_MANIFEST.resolve()),
        "published_at_ist": str(payload.get("published_at_ist", "")),
        "slot_ist": str(payload.get("slot_ist", "")),
        "error": "" if ok else "manifest_schema_count_or_hash_mismatch",
    }


def _looks_like_real_tokens(token_map: dict) -> bool:
    try:
        vals = [int(v) for v in token_map.values()]
        if not vals:
            return False
        return max(vals) >= 1000
    except Exception:
        return False


def _load_stocks_universe_fixed(*args, **kwargs):
    if _orig_load_universe is None:
        raise RuntimeError("core.load_stocks_universe not found")
    tickers, token_map = _orig_load_universe(*args, **kwargs)
    if token_map and not _looks_like_real_tokens(token_map):
        logger = args[0] if args else None
        if logger is not None:
            try:
                logger.warning(
                    "Token map from filtered_stocks_MIS looks like weights/flags "
                    "(small ints). Ignoring it and forcing token fetch."
                )
            except Exception:
                pass
        token_map = {}
    contract = _five_min_universe_contract()
    if contract.get("ok"):
        tickers = list(contract["symbols"])
        allowed = set(tickers)
        token_map = {
            str(symbol).strip().upper(): int(token)
            for symbol, token in dict(token_map).items()
            if str(symbol).strip().upper() in allowed and int(token or 0) > 0
        }
        logger = args[0] if args else None
        if logger is not None:
            logger.info(
                "[1MIN] Bound to exact live 5-minute universe: symbols=%d sha256=%s manifest=%s",
                len(tickers),
                str(contract["universe_sha256"]),
                str(contract["manifest_path"]),
            )
    else:
        raise RuntimeError(
            "The authoritative 5-minute feed universe is unavailable or invalid: "
            f"{contract.get('error', 'unknown_error')} | {FIVE_MIN_UNIVERSE_MANIFEST}"
        )
    return tickers, token_map


core.load_stocks_universe = _load_stocks_universe_fixed


# The core's generic freshness rule deliberately tolerates one missing interval.
# That is inappropriate for a live 1-minute feed: a file ending at 14:44 must
# not be declared fresh for the completed 14:45 candle.  Require the exact
# completed-minute timestamp while retaining the full-history start guard.
def _ticker_is_fresh_strict(
    mode: str,
    out_path: str,
    now_ist_dt: datetime,
    holidays: set,
    intraday_ts: str,
    required_start_ist=None,
) -> bool:
    existing_path = core._resolve_existing_store_path(out_path)
    if not os.path.exists(existing_path):
        return False
    last_ts = core._read_last_ts_from_store(existing_path)
    if last_ts is None:
        return False
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(core.IST_TZ)
    else:
        last_ts = last_ts.tz_convert(core.IST_TZ)
    if not core._history_start_satisfied(
        mode, out_path, required_start_ist, holidays, intraday_ts
    ):
        return False
    expected = core.expected_last_stamp(mode, now_ist_dt, holidays, intraday_ts)["value"]
    if expected.tzinfo is None:
        expected = core.IST_TZ.localize(expected)
    return bool(last_ts >= expected - timedelta(seconds=1))


core.ticker_is_fresh = _ticker_is_fresh_strict


# ---------------------------------------------------------------------
# Defaults (env-overridable; CLI takes precedence).
# ---------------------------------------------------------------------
DEFAULT_FIRST_SLOT = os.getenv("EQIDV2_1M_FIRST_SLOT", "09:17:30")
DEFAULT_INTERVAL_MIN = int(os.getenv("EQIDV2_1M_INTERVAL_MIN", "5"))
DEFAULT_END_CUTOFF = os.getenv("EQIDV2_1M_END_CUTOFF", "15:40")
# Modest worker count: this runs alongside the live 5-min feed / v7 on the
# shared machine. The core round-robins across the 8 apps, so 16 threads = ~2
# concurrent requests per app, comfortably inside Kite rate limits.
DEFAULT_MAX_WORKERS = int(os.getenv("EQIDV2_1M_MAX_WORKERS", "16"))
DEFAULT_INTRADAY_TS = os.getenv("EQIDV2_1M_INTRADAY_TS", "end")
DEFAULT_HOLIDAYS_FILE = getattr(core, "HOLIDAYS_FILE_DEFAULT", "nse_holidays.csv")
# How often to refresh the freshness status.json while idling between slots, so
# the supervisor's freshness watchdog never trips during the 5-minute waits.
STATUS_TOUCH_SEC = int(os.getenv("EQIDV2_1M_STATUS_TOUCH_SEC", "30"))

LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATUS_PATH = LOG_DIR / "eqidv2_eod_scheduler_for_1min_data_live.status.json"


def now_ist() -> datetime:
    return datetime.now(IST)


def _parse_hhmmss(value: str) -> dtime:
    value = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(value, fmt).time()
            return t
        except ValueError:
            continue
    raise ValueError(f"Could not parse time '{value}' (expected HH:MM or HH:MM:SS)")


def _read_holidays_set(holidays_file: str) -> set:
    try:
        return set(core._read_holidays(str(holidays_file)))
    except Exception:
        return set()


def _is_trading_day(d, holidays: set) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in holidays


def _build_slots(day: datetime.date, first_slot: dtime, interval_min: int, end_cutoff: dtime) -> list:
    """Return tz-aware slot datetimes for `day`: first_slot, +interval, ... <= end_cutoff."""
    start = IST.localize(datetime.combine(day, first_slot))
    end = IST.localize(datetime.combine(day, end_cutoff))
    slots = []
    cur = start
    step = timedelta(minutes=max(1, int(interval_min)))
    while cur <= end:
        slots.append(cur)
        cur = cur + step
    return slots


def _write_status(state: str, detail: dict) -> None:
    payload = {
        "session": "eod_1min_data",
        "state": state,
        "updated_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
    }
    payload.update(detail or {})
    try:
        tmp = STATUS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, STATUS_PATH)
    except Exception as exc:  # never let status I/O kill the scheduler
        print(f"[WARN] status write failed: {exc}", flush=True)


def _sleep_until(target: datetime, *, base_detail: dict) -> None:
    """Sleep until `target` (tz-aware), refreshing status.json + emitting a stdout
    heartbeat every STATUS_TOUCH_SEC. The stdout line is what keeps the
    supervisor's hung-idle watchdog (driven by log write time) from killing a
    healthy process during the 5-minute idle waits between slots."""
    while True:
        remaining = (target - now_ist()).total_seconds()
        if remaining <= 0:
            return
        _write_status(
            "waiting",
            {**base_detail, "next_slot_ist": target.strftime("%Y-%m-%d %H:%M:%S%z"),
             "seconds_to_next_slot": round(remaining, 1)},
        )
        print(f"[WAIT] next slot {target.strftime('%H:%M:%S')} IST "
              f"in {round(remaining, 1)}s", flush=True)
        time.sleep(min(STATUS_TOUCH_SEC, max(0.5, remaining)))


def _run_one_slot(slot: datetime, *, max_workers: int, intraday_ts: str,
                  holidays: set, report_dir: str,
                  status_detail: dict | None = None) -> dict:
    started = time.perf_counter()
    started_at = now_ist()
    universe_contract = _five_min_universe_contract()
    if not universe_contract.get("ok"):
        raise RuntimeError(
            "Cannot start 1-minute slot without a valid 5-minute universe manifest: "
            f"{universe_contract.get('error', 'unknown_error')}"
        )
    slot_detail = {
        **(status_detail or {}),
        "current_slot_ist": slot.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_started_at_ist": started_at.strftime("%Y-%m-%d %H:%M:%S%z"),
        "universe_source": "LIVE_5MIN_FEED_MANIFEST",
        "universe_count": int(universe_contract["universe_count"]),
        "universe_sha256": str(universe_contract["universe_sha256"]),
        "universe_manifest": str(universe_contract["manifest_path"]),
    }
    _write_status(
        "running",
        slot_detail,
    )
    print(f"[SLOT] {slot.strftime('%H:%M:%S')} | fetching 1-min incremental "
          f"(max_workers={max_workers}, intraday_ts={intraday_ts})", flush=True)
    heartbeat_stop = threading.Event()

    def _heartbeat_while_running() -> None:
        interval = max(0.1, float(STATUS_TOUCH_SEC))
        while not heartbeat_stop.wait(interval):
            elapsed_sec = round(time.perf_counter() - started, 1)
            _write_status(
                "running",
                {**slot_detail, "slot_elapsed_sec": elapsed_sec},
            )
            print(
                f"[SLOT][HEARTBEAT] {slot.strftime('%H:%M:%S')} "
                f"still running elapsed={elapsed_sec:.1f}s",
                flush=True,
            )

    heartbeat_thread = threading.Thread(
        target=_heartbeat_while_running,
        name="eqidv2-1min-slot-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()
    summary = {}
    try:
        core.run_mode(
            "1min",
            max_workers=max_workers,
            skip_if_fresh=True,
            intraday_ts=intraday_ts,
            holidays=holidays,
            refresh_tokens=False,
            report_dir=report_dir,
            print_missing_rows=False,
            print_missing_rows_max=5,
        )
        summary["ok"] = True
    except Exception as exc:
        summary["ok"] = False
        summary["error"] = str(exc)
        print(f"[SLOT] {slot.strftime('%H:%M:%S')} FAILED: {exc}", flush=True)
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=max(1.0, float(STATUS_TOUCH_SEC) + 1.0))
    summary["elapsed_sec"] = round(time.perf_counter() - started, 2)
    summary["universe_count"] = int(universe_contract["universe_count"])
    summary["universe_sha256"] = str(universe_contract["universe_sha256"])
    summary["active_kite_apps"] = int(len(getattr(core, "kite_pool", []) or []))
    print(f"[SLOT] {slot.strftime('%H:%M:%S')} done in {summary['elapsed_sec']}s "
          f"ok={summary.get('ok')}", flush=True)
    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Live 1-minute data fetch scheduler")
    p.add_argument("--first-slot", default=DEFAULT_FIRST_SLOT, help="First fetch time IST (HH:MM:SS)")
    p.add_argument("--interval-min", type=int, default=DEFAULT_INTERVAL_MIN, help="Slot interval (minutes)")
    p.add_argument("--end-cutoff", default=DEFAULT_END_CUTOFF, help="Stop scheduling new slots after this IST time")
    p.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    p.add_argument("--intraday-ts", default=DEFAULT_INTRADAY_TS, choices=["start", "end"])
    p.add_argument("--holidays-file", default=DEFAULT_HOLIDAYS_FILE)
    p.add_argument("--report-dir", default="reports/stocks_missing_reports_1min_live")
    p.add_argument("--catch-up-on-start", action="store_true", default=True,
                   help="On a mid-day (re)start, run one immediate fetch before waiting for the next slot.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    first_slot = _parse_hhmmss(args.first_slot)
    end_cutoff = _parse_hhmmss(args.end_cutoff)
    holidays = _read_holidays_set(args.holidays_file)

    now = now_ist()
    base_detail = {
        "first_slot": args.first_slot,
        "interval_min": args.interval_min,
        "end_cutoff": args.end_cutoff,
        "max_workers": args.max_workers,
        "intraday_ts": args.intraday_ts,
    }

    if not _is_trading_day(now.date(), holidays):
        print(f"[INFO] {now.date()} is not a trading day (weekend/holiday). Exiting.", flush=True)
        _write_status("idle_non_trading_day", base_detail)
        return 0

    slots = _build_slots(now.date(), first_slot, args.interval_min, end_cutoff)
    if not slots:
        print("[INFO] No slots computed for today. Exiting.", flush=True)
        _write_status("idle_no_slots", base_detail)
        return 0

    print(f"[INFO] 1-min live scheduler: {len(slots)} slots "
          f"{slots[0].strftime('%H:%M:%S')}..{slots[-1].strftime('%H:%M:%S')} IST "
          f"every {args.interval_min}min | max_workers={args.max_workers}", flush=True)
    _write_status("starting", {**base_detail, "total_slots": len(slots)})

    # Mid-day restart catch-up: if we start after the first slot has already
    # passed, do one immediate incremental fetch so the dashboard/consumers are
    # not stuck a full interval behind while we wait for the next slot.
    pending = [s for s in slots if s > now_ist()]
    if args.catch_up_on_start and pending and pending[0] != slots[0]:
        print("[INFO] Mid-day start detected; running immediate catch-up fetch.", flush=True)
        summary = _run_one_slot(now_ist(), max_workers=args.max_workers,
                                 intraday_ts=args.intraday_ts, holidays=holidays,
                                 report_dir=args.report_dir,
                                 status_detail={**base_detail, "total_slots": len(slots)})
        _write_status("catch_up_done", {**base_detail, "last_slot_summary": summary})

    for idx, slot in enumerate(slots):
        if slot <= now_ist():
            continue  # already passed (covered by catch-up)
        _sleep_until(slot, base_detail={**base_detail, "slot_index": idx + 1, "total_slots": len(slots)})
        summary = _run_one_slot(slot, max_workers=args.max_workers,
                                intraday_ts=args.intraday_ts, holidays=holidays,
                                report_dir=args.report_dir,
                                status_detail={**base_detail, "slot_index": idx + 1,
                                               "total_slots": len(slots)})
        _write_status(
            "slot_done",
            {**base_detail, "slot_index": idx + 1, "total_slots": len(slots),
             "last_slot_ist": slot.strftime("%Y-%m-%d %H:%M:%S%z"),
             "last_slot_summary": summary},
        )

    print("[INFO] All slots complete for today. Scheduler exiting.", flush=True)
    _write_status("done", {**base_detail, "total_slots": len(slots)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
