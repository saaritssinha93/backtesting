# -*- coding: utf-8 -*-
"""
eqidv1_live_fetch_n_latestsignalprint.py
========================================

Every 15 minutes during market hours:
  1) Update EQIDV1 15m parquet (core.run_mode(mode="15min"...))
  2) Scan ONLY the latest 15m bar for signals (eqidv1_live_combined_analyser.run_one_scan)
  3) Print a compact result + let analyser write parquet/CSV as usual

Key fix:
- NO more "must be exactly on boundary within 2 seconds" logic (which misses cycles).
"""

from __future__ import annotations

import sys
import time as _time
from pathlib import Path
from datetime import datetime, timedelta, time as dtime, date as ddate

import pytz
from kiteconnect import KiteConnect

# -----------------------------------------------------------------------------
# Paths / imports
# -----------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly as core  # noqa: E402
import eqidv1_live_combined_analyser as analyser  # noqa: E402


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
STEP_MIN     = 15

SESSION_END  = dtime(15, 50)   # hard stop for this process
BOUNDARY_BUFFER_SEC = 8        # wait after candle close for broker/parquet to be ready

# Optional: allowlist extra special trading days (weekend session, etc.)
EXTRA_TRADING_DAYS: set[ddate] = set()

API_KEY_FILE = _ROOT / "api_key.txt"
ACCESS_TOKEN_FILE = _ROOT / "access_token.txt"

REPORT_DIR = _ROOT / "reports" / "eqidv1_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Auth fix (strip newlines etc.)
# -----------------------------------------------------------------------------
def _read_first_token(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                return s.split()[0]
    raise ValueError(f"{path} is empty/invalid")


def setup_kite_session_fixed() -> KiteConnect:
    api_key = _read_first_token(API_KEY_FILE)
    access_token = _read_first_token(ACCESS_TOKEN_FILE)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


# Make core use our session creator
core.setup_kite_session = setup_kite_session_fixed


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def dt_today_at(t: dtime) -> datetime:
    n = now_ist()
    return IST.localize(datetime(n.year, n.month, n.day, t.hour, t.minute, 0))


def floor_to_step(t: datetime, step_min: int) -> datetime:
    minute = (t.minute // step_min) * step_min
    return t.replace(minute=minute, second=0, microsecond=0)


def next_step_boundary(t: datetime, step_min: int) -> datetime:
    base = floor_to_step(t, step_min)
    if base <= t:
        base = base + timedelta(minutes=step_min)
    return base


def sleep_until(target: datetime) -> None:
    while True:
        n = now_ist()
        secs = (target - n).total_seconds()
        if secs <= 0:
            return
        _time.sleep(min(secs, 2.0))


def is_trading_day(d: ddate, holidays: set[ddate]) -> bool:
    if d in EXTRA_TRADING_DAYS:
        return True
    if hasattr(core, "_is_trading_day"):
        try:
            return bool(core._is_trading_day(d, holidays))
        except Exception:
            pass
    return d.weekday() < 5 and d not in holidays


# -----------------------------------------------------------------------------
# Actions
# -----------------------------------------------------------------------------
def run_update_15m_once(holidays: set[ddate]) -> None:
    core.run_mode(
        mode="15min",
        max_workers=4,
        skip_if_fresh=True,
        intraday_ts="end",
        holidays=holidays,
        refresh_tokens=False,
        report_dir=str(REPORT_DIR),
        print_missing_rows=False,
        print_missing_rows_max=200,
    )


def print_signals(signals_df) -> None:
    # signals_df schema comes from analyser.run_one_scan()
    if signals_df is None or signals_df.empty:
        print("[SIG ] No signals on latest bar.")
        return

    # Keep it readable in console
    cols = [c for c in ["ticker", "side", "bar_time_ist", "setup", "entry_price", "sl_price", "target_price", "score"]
            if c in signals_df.columns]
    view = signals_df[cols].copy()

    # Sort by score desc if available
    if "score" in view.columns:
        view = view.sort_values("score", ascending=False)

    # Print top 30
    print("[SIG ] Signals (top 30):")
    print(view.head(30).to_string(index=False))


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------
def main() -> None:
    try:
        holidays = set(core._read_holidays(core.HOLIDAYS_FILE_DEFAULT))
    except Exception:
        holidays = set()

    print("[LIVE] EQIDV1 15m FETCH + LATEST-SIGNAL scheduler started.")
    print(f"       Market: {MARKET_OPEN.strftime('%H:%M')}â€“{MARKET_CLOSE.strftime('%H:%M')} IST | step={STEP_MIN}m")
    print(f"       Buffer after boundary: {BOUNDARY_BUFFER_SEC}s | exits at {SESSION_END.strftime('%H:%M')} IST")

    last_slot_run: datetime | None = None

    while True:
        n = now_ist()
        session_end = dt_today_at(SESSION_END)

        if n >= session_end:
            print(f"[EXIT] Session end reached ({session_end.strftime('%Y-%m-%d %H:%M:%S%z')}).")
            return

        if not is_trading_day(n.date(), holidays):
            print("[INFO] Non-trading day. Exiting.")
            return

        start = dt_today_at(MARKET_OPEN)
        end = dt_today_at(MARKET_CLOSE)

        if n < start:
            print(f"[WAIT] Before market. Sleeping until {start.strftime('%Y-%m-%d %H:%M:%S%z')}.")
            sleep_until(min(start, session_end))
            continue

        if n > end:
            print(f"[DONE] After market. Sleeping until {session_end.strftime('%Y-%m-%d %H:%M:%S%z')} then exit.")
            sleep_until(session_end)
            return

        # Slot end = last completed boundary
        slot_end = floor_to_step(n, STEP_MIN)

        # Avoid double-run in the same slot
        if last_slot_run is not None and slot_end <= last_slot_run:
            nxt = next_step_boundary(n, STEP_MIN)
            sleep_until(min(nxt + timedelta(seconds=BOUNDARY_BUFFER_SEC), session_end))
            continue

        # Make sure we're a bit AFTER the boundary (data closed + parquet written)
        target = slot_end + timedelta(seconds=BOUNDARY_BUFFER_SEC)
        if n < target:
            sleep_until(min(target, session_end))

        # Run update + signal scan
        try:
            print(f"[UPD ] 15m update for slot {slot_end.strftime('%H:%M')} @ {now_ist().strftime('%H:%M:%S')}")
            run_update_15m_once(holidays)
        except Exception as e:
            print(f"[WARN] 15m update failed: {e!r}")

        try:
            print(f"[SCAN] latest-bar signals for slot {slot_end.strftime('%H:%M')} @ {now_ist().strftime('%H:%M:%S')}")
            checks_df, signals_df = analyser.run_one_scan(run_tag="S")
            # analyser already scans "latest entry signals" per ticker from parquet tail
            # We'll additionally keep only signals that match the most recent bar_time_ist in this run.
            if signals_df is not None and (not signals_df.empty) and ("bar_time_ist" in signals_df.columns):
                latest_bt = signals_df["bar_time_ist"].max()
                signals_df = signals_df[signals_df["bar_time_ist"] == latest_bt].copy()
            print_signals(signals_df)
        except Exception as e:
            print(f"[ERR ] signal scan failed: {e!r}")

        last_slot_run = slot_end


if __name__ == "__main__":
    main()

