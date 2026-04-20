# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Signal Engine (Stage 1)
==========================================
Scans ALL 1044 tickers every 5-min slot, NO filters applied.
Writes raw pattern matches to a pending pool JSON/CSV.
The Detection Engine (Stage 2) then applies all V16 filters on focused fresh data.

Operational file names stay on the v16 path, but the live scan stack imports the
v17f runner patch bundle first so short-side scan logic matches v17f exactly.

Outputs (to LIVE_SIGNALS_DIR):
  pending_signals_YYYY-MM-DD_v16_5min.json   — machine state (Detection Engine reads this)
  pending_signals_YYYY-MM-DD_v16_5min.csv    — dashboard display

Status files (to logs/):
  eqidv2_signal_engine_v16_5min.log
  eqidv2_signal_engine_v16_5min.status
  eqidv2_signal_engine_v16_5min.heartbeat

Key difference from live scanner:
  - NO RS filter, NO V16 post-scan filters
  - Does NOT write to signals_YYYY-MM-DD_v16_5min_long/short.csv
  - Writes ALL raw pattern matches → pending pool (deduplicated by signal_id)
"""

from __future__ import annotations

import csv
import json
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# V16 runner — scan helpers (NO filter imports needed here)
# ---------------------------------------------------------------------------
import avwap_combined_runner_v17f_5min as _v17f_runner  # noqa: F401
import avwap_combined_runner_v16_5min as v16_runner
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT,
    NIFTY_RS_LOOKBACK_BARS,
    NIFTY_CONTEXT_MIN_DAYMOVE_PCT,
    TEST_SHORT_TARGET_PCT,
    TEST_LONG_TARGET_PCT,
)
from avwap_v11_refactored.avwap_common_v11_v15 import (
    StrategyConfig,
    default_short_config,
    default_long_config as default_long_config_v11,
    prepare_session_bars_for_scan,
    list_tickers_15m,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker_prepared as _base_scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker_prepared as _base_scan_long_prepared,
)

import eqidv2_live_combined_analyser_csv_v15 as base_v15
from eqidv2_runtime_paths import (
    DATA_5M_DIR as RUNTIME_DATA_5M_DIR,
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    RUNTIME_STATUS_DIR,
    runtime_dir,
)
from live_v16_5min_slot_snapshot import (
    build_slot_snapshots,
    clear_rolling_cache as clear_slot_snapshot_cache,
    read_shard_snapshot,
    slot_context_path as snapshot_slot_context_path,
)

# ===========================================================================
# CONSTANTS
# ===========================================================================
IST = base_v15.IST
DIR_5M = str(RUNTIME_DATA_5M_DIR)
END_5M = "_stocks_indicators_5min.parquet"
NIFTYBEES_TICKER = "NIFTYBEES"

PENDING_JSON_PATTERN = "pending_signals_{}_v16_5min.json"
PENDING_CSV_PATTERN  = "pending_signals_{}_v16_5min.csv"

SLOT_MINUTES        = 5
START_TIME          = dtime(9, 15)
# Two-session design: morning slots (< 11:00) expire at 11:00; afternoon slots
# (11:00..<13:30) expire at 13:30. Any slot >= 13:30 would inherit an
# already-past expiry under `_compute_expires_at`, so stop scanning at 13:30.
# Detection Engine and Trade Executor remain live past this time to confirm
# and execute signals from the last afternoon slot (13:25).
END_TIME            = dtime(13, 30)
HARD_STOP_TIME      = dtime(15, 30)
TAIL_ROWS           = int(os.getenv("EQIDV16_5MIN_TAIL_ROWS", "260"))
SLOT_START_OFFSET_SECONDS   = int(os.getenv("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "45"))
SLOT_READY_MAX_WAIT_SECONDS = int(os.getenv("EQIDV16_5MIN_SLOT_READY_MAX_WAIT_SECONDS", "90"))
SLOT_READY_POLL_SECONDS     = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_POLL_SECONDS", "2")))
SLOT_READY_SAMPLE_SIZE      = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_SAMPLE_SIZE", "1030")))
SLOT_READY_MIN_FRESH_RATIO  = float(os.getenv("EQIDV16_5MIN_SLOT_READY_MIN_FRESH_RATIO", "0.95"))
USE_SCHEDULER_READY_MARKER  = str(
    os.getenv("EQIDV16_5MIN_USE_SCHEDULER_READY_MARKER", "1")
).strip().lower() not in {"0", "false", "no", "off"}
SLOT_READY_MARKER_MIN_FRESH_RATIO = float(
    os.getenv("EQIDV16_5MIN_SLOT_READY_MARKER_MIN_FRESH_RATIO", "0.95")
)
SKIP_STALE_SLOT_ON_TIMEOUT = str(
    os.getenv("EQIDV16_5MIN_SKIP_STALE_SLOT_ON_TIMEOUT", "1")
).strip().lower() not in {"0", "false", "no", "off"}
NEUTRALIZE_WEAK_NIFTY_CONTEXT = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_WEAK_NIFTY_CONTEXT", "1")
).strip().lower() not in {"0", "false", "no", "off"}
DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = str(
    os.getenv("EQIDV16_5MIN_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK", "1")
).strip().lower() not in {"0", "false", "no", "off"}
NEUTRALIZE_PARTIAL_NIFTY_SESSION = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_PARTIAL_NIFTY_SESSION", "1")
).strip().lower() not in {"0", "false", "no", "off"}
SCAN_SHARDS         = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_SHARDS", "10")))
SCAN_MAX_WORKERS    = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_MAX_WORKERS", str(SCAN_SHARDS))))
SNAPSHOT_MAX_WORKERS = max(1, int(os.getenv("EQIDV16_5MIN_SNAPSHOT_MAX_WORKERS", str(SCAN_MAX_WORKERS))))
USE_SLOT_SNAPSHOTS  = str(os.getenv("EQIDV16_5MIN_USE_SLOT_SNAPSHOTS", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
USE_SNAPSHOT_ROLLING_CACHE = str(os.getenv("EQIDV16_5MIN_USE_ROLLING_CACHE", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
INTRADAY_LEVERAGE         = 5.0
DEFAULT_POSITION_SIZE_RS  = float(os.getenv("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000"))
LIVE_MIN_BARS_FOR_SCAN    = max(4, int(os.getenv("EQIDV16_5MIN_LIVE_MIN_BARS_FOR_SCAN", "4")))
LIVE_NIFTY_CONTEXT_OR_END_TIME     = dtime(9, 20)
LIVE_NIFTY_CONTEXT_CONFIRM_TIME    = dtime(9, 20)

_SLOT_READY_PRIORITY_TICKERS_RAW = os.getenv(
    "EQIDV16_5MIN_SLOT_READY_PRIORITY_TICKERS",
    "NIFTYBEES,SBIN,RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY",
)
SLOT_READY_PRIORITY_TICKERS = [
    str(part).strip().upper()
    for part in str(_SLOT_READY_PRIORITY_TICKERS_RAW).split(",")
    if str(part).strip()
]

# Signal window expiry boundaries (IST) — per Section 4 (SIGNAL WINDOWS)
_SESSION_MORNING_END   = dtime(11, 0)    # morning session signals expire at 11:00
_SESSION_AFTNOON_LONG  = dtime(13, 30)   # afternoon LONG signals expire at 13:30
_SESSION_AFTNOON_SHORT = dtime(13, 30)  # afternoon SHORT signals expire at 13:30

v16_runner.NIFTY_CONTEXT_OR_END_TIME     = LIVE_NIFTY_CONTEXT_OR_END_TIME
v16_runner.NIFTY_CONTEXT_CONFIRM_TIME    = LIVE_NIFTY_CONTEXT_CONFIRM_TIME

_BASE_DIR  = Path(__file__).resolve().parent
_LOG_DIR   = _BASE_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
SLOT_READY_MARKER_DIR    = runtime_dir("slot_ready_5m")
SLOT_READY_STATUS_PATH   = _LOG_DIR / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"

_SCRIPT_NAME = "eqidv2_signal_engine_v16_5min.py"
_LOG_FILE    = _LOG_DIR / "eqidv2_signal_engine_v16_5min.log"

PENDING_CSV_COLUMNS = [
    "signal_id", "ticker", "side", "signal_datetime", "signal_entry_datetime_ist",
    "signal_bar_time", "added_at",
    "signal_price", "entry_price", "stop_price", "target_price",
    "quality_score", "avwap_dist_atr", "rsi_signal", "adx",
    "rs_pct", "setup", "status", "expires_at",
    "filter_reason", "detection_time", "detection_price",
]


# ===========================================================================
# STDOUT TEE
# ===========================================================================
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass


def _reconfigure_stdio() -> None:
    for name in ("stdout", "stderr", "__stdout__", "__stderr__"):
        stream = getattr(sys, name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _start_tee() -> None:
    try:
        _reconfigure_stdio()
        fh = open(_LOG_FILE, "a", encoding="utf-8", buffering=1)
        base_stdout = getattr(sys, "__stdout__", None) or sys.stdout
        base_stderr = getattr(sys, "__stderr__", None) or sys.stderr
        sys.stdout = _Tee(base_stdout, fh)  # type: ignore[assignment]
        sys.stderr = _Tee(base_stderr, fh)  # type: ignore[assignment]
    except Exception:
        pass


# ===========================================================================
# STATUS / HEARTBEAT
# ===========================================================================
def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"]    = str(RUNTIME_STATUS_DIR / "eqidv2_signal_engine_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_signal_engine_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"]    = _SCRIPT_NAME
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    base_v15._touch_runtime_heartbeat(state, **extra)


# ===========================================================================
# TIME HELPERS
# ===========================================================================
def _now_ist() -> datetime:
    return base_v15.now_ist()


def _next_5min_slot(now: datetime) -> datetime:
    now = now.astimezone(IST)
    today = now.date()
    start_dt = IST.localize(datetime.combine(today, START_TIME))
    end_dt   = IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt
    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // SLOT_MINUTES) * SLOT_MINUTES
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot <= now:
        slot += timedelta(minutes=SLOT_MINUTES)
    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _sleep_until_resilient(
    target_dt: datetime,
    *,
    phase: str,
    slot: Optional[str] = None,
    next_wake: Optional[str] = None,
    poll_seconds: float = 30.0,
) -> None:
    while True:
        now = _now_ist()
        delta = (target_dt - now).total_seconds()
        if delta <= 0:
            return
        payload: Dict[str, Any] = {"phase": phase}
        if slot:
            payload["slot"] = slot
        if next_wake:
            payload["next_wake"] = next_wake
        _touch_status("RUNNING", **payload)
        _touch_heartbeat("RUNNING", **payload)
        time.sleep(min(float(poll_seconds), max(1.0, delta)))


def _compute_expires_at(slot_ist: datetime, side: str) -> str:
    """Compute signal expiry time based on session timing and side."""
    slot_time = slot_ist.time()
    today = slot_ist.date()
    side_u = str(side).upper().strip()

    if slot_time < _SESSION_MORNING_END:
        # Morning session → expires at 12:00
        exp_dt = IST.localize(datetime.combine(today, _SESSION_MORNING_END))
    else:
        # Afternoon session
        if side_u == "SHORT":
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_SHORT))
        else:
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_LONG))

    return exp_dt.strftime("%Y-%m-%d %H:%M:%S%z")


# ===========================================================================
# TICKER LISTING
# ===========================================================================
def _list_tickers_5m() -> List[str]:
    return list_tickers_15m(DIR_5M, END_5M)


def _load_5m_parquet(ticker: str, n: int = TAIL_ROWS) -> pd.DataFrame:
    path = os.path.join(DIR_5M, f"{ticker}{END_5M}")
    df = base_v15.read_parquet_tail(path, n=n)
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()

    dt = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)

    df = df.copy()
    df["date"] = dt
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


# ===========================================================================
# SLOT READINESS (copied from live scanner — same logic)
# ===========================================================================
def _sample_tickers_for_slot_ready(tickers: List[str], sample_size: int) -> List[str]:
    if not tickers:
        return []
    uniq = sorted({str(t).upper().strip() for t in tickers if str(t).strip()})
    size = min(max(1, int(sample_size)), len(uniq))
    if size >= len(uniq):
        return uniq

    picks: List[str] = []
    for ticker in SLOT_READY_PRIORITY_TICKERS:
        if ticker in uniq and ticker not in picks:
            picks.append(ticker)
            if len(picks) >= size:
                return sorted(picks)

    remaining = [t for t in uniq if t not in picks]
    remaining_slots = size - len(picks)
    if remaining_slots <= 0:
        return sorted(picks)
    step = max(1.0, (len(remaining) - 1) / float(max(1, remaining_slots - 1)))
    for idx in range(remaining_slots):
        pos = int(round(idx * step))
        pos = max(0, min(len(remaining) - 1, pos))
        if remaining[pos] not in picks:
            picks.append(remaining[pos])
    return sorted(set(picks))


def _slot_ready_marker_path(slot: datetime) -> Path:
    slot_ts = pd.Timestamp(slot)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    return SLOT_READY_MARKER_DIR / f"slot_{slot_ts.strftime('%Y%m%d_%H%M')}.json"


def _load_scheduler_slot_status(slot: datetime) -> Optional[Dict[str, Any]]:
    path = SLOT_READY_STATUS_PATH
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    slot_ts = pd.Timestamp(slot)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    payload_slot = pd.to_datetime(payload.get("slot_ist"), errors="coerce")
    if pd.isna(payload_slot):
        return None
    if payload_slot.tzinfo is None:
        payload_slot = payload_slot.tz_localize(IST)
    else:
        payload_slot = payload_slot.tz_convert(IST)
    if payload_slot.floor("min") != slot_ts.floor("min"):
        return None
    return payload


def _load_slot_ready_marker(slot: datetime) -> Optional[Dict[str, Any]]:
    if not USE_SCHEDULER_READY_MARKER:
        return None
    path = _slot_ready_marker_path(slot)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        ratio = float(payload.get("fresh_ratio", 0.0) or 0.0)
        if ratio < float(SLOT_READY_MARKER_MIN_FRESH_RATIO):
            return None
        source = str(payload.get("source", "marker")).strip().lower()
        if source != "final":
            return None
        status_payload = _load_scheduler_slot_status(slot)
        if status_payload is not None:
            failures = list(status_payload.get("failures", []) or [])
            verification_failed_count = int(status_payload.get("verification_failed_count", 0) or 0)
            overall_state = str(status_payload.get("overall_state", "")).strip().upper()
            if overall_state == "FAIL" or failures or verification_failed_count > 0:
                return None
        return payload
    except Exception:
        return None


def _wait_for_slot_data_ready(slot: datetime, tickers: List[str]) -> Tuple[bool, float, float, int]:
    delay_target = slot + timedelta(seconds=int(SLOT_START_OFFSET_SECONDS))
    now = _now_ist()
    if now < delay_target:
        wait_secs = (delay_target - now).total_seconds()
        print(
            f"[WAIT] Delaying {wait_secs:.0f}s for 5min slot offset "
            f"(until {delay_target.strftime('%H:%M:%S')})",
            flush=True,
        )
        time.sleep(max(0.0, wait_secs))

    sample = _sample_tickers_for_slot_ready(tickers, SLOT_READY_SAMPLE_SIZE)
    max_wait = max(0, int(SLOT_READY_MAX_WAIT_SECONDS))
    marker_payload = _load_slot_ready_marker(slot)
    if marker_payload is not None:
        waited = max(0.0, (_now_ist() - now).total_seconds())
        ratio = float(marker_payload.get("fresh_ratio", 0.0) or 0.0)
        checked = int(marker_payload.get("checked_count", 0) or 0)
        print(
            f"[WAIT] 5min slot readiness accepted via scheduler marker "
            f"(ratio={ratio:.2f}, checked={checked})",
            flush=True,
        )
        return True, ratio, waited, checked

    if max_wait <= 0 or not sample:
        waited = max(0.0, (_now_ist() - now).total_seconds())
        return False, 0.0, waited, len(sample)

    target_slot = pd.Timestamp(slot)
    if target_slot.tzinfo is None:
        target_slot = target_slot.tz_localize(IST)
    else:
        target_slot = target_slot.tz_convert(IST)

    started = _now_ist()
    deadline = started + timedelta(seconds=max_wait)
    _slot_unix_ts = target_slot.timestamp()

    def _ticker_fresh_mtime(ticker: str) -> bool:
        path = os.path.join(DIR_5M, f"{ticker}{END_5M}")
        try:
            return os.stat(path).st_mtime >= _slot_unix_ts
        except OSError:
            return False

    last_ratio, last_checked = 0.0, 0
    while True:
        # Re-check marker on each iteration
        marker_payload = _load_slot_ready_marker(slot)
        if marker_payload is not None:
            waited = max(0.0, (_now_ist() - started).total_seconds())
            ratio = float(marker_payload.get("fresh_ratio", 0.0) or 0.0)
            checked = int(marker_payload.get("checked_count", 0) or 0)
            print(
                f"[WAIT] 5min slot readiness accepted via scheduler marker "
                f"(ratio={ratio:.2f}, checked={checked})",
                flush=True,
            )
            return True, ratio, waited, checked

        fresh = sum(1 for t in sample if _ticker_fresh_mtime(t))
        checked = len(sample)
        ratio = (fresh / checked) if checked > 0 else 0.0
        last_ratio, last_checked = ratio, checked
        if checked > 0 and ratio >= float(SLOT_READY_MIN_FRESH_RATIO):
            waited = (_now_ist() - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness ready after {waited:.1f}s "
                f"(fresh={fresh}/{checked}, ratio={ratio:.2f})",
                flush=True,
            )
            return True, ratio, waited, checked

        now = _now_ist()
        if now >= deadline:
            waited = (now - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness timeout after {waited:.1f}s "
                f"(ratio={ratio:.2f}, checked={checked})",
                flush=True,
            )
            return False, last_ratio, waited, last_checked
        time.sleep(min(float(SLOT_READY_POLL_SECONDS), max(0.0, (deadline - now).total_seconds())))


# ===========================================================================
# NIFTY CONTEXT
# ===========================================================================
def _resolve_nifty_context_flags(
    *,
    day_move_pct: float,
    rs_pct: float,
    session_complete: bool,
) -> Tuple[bool, bool, str]:
    if not session_complete and NEUTRALIZE_PARTIAL_NIFTY_SESSION:
        return True, True, "partial_session_neutral"
    if abs(day_move_pct) < float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT):
        if NEUTRALIZE_WEAK_NIFTY_CONTEXT:
            return True, True, "weak_daymove_neutral"
        return False, False, "weak_daymove_blocked"
    allow_long  = rs_pct >= float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
    allow_short = rs_pct <= -float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)
    if allow_long or allow_short:
        return allow_long, allow_short, "threshold"
    if DIRECTIONAL_NIFTY_CONTEXT_FALLBACK:
        if day_move_pct > 0:
            return True, False, "directional_daymove_long"
        if day_move_pct < 0:
            return False, True, "directional_daymove_short"
    return False, False, "threshold_blocked"


def _session_open_timestamp(session_date: Any) -> pd.Timestamp:
    if isinstance(session_date, pd.Timestamp):
        date_value = session_date.date()
    elif isinstance(session_date, datetime):
        date_value = session_date.date()
    else:
        date_value = session_date
    return pd.Timestamp(IST.localize(datetime.combine(date_value, START_TIME))).floor("min")


def _compute_nifty_rs_at_slot(slot_ist: datetime) -> Tuple[float, bool, bool]:
    """Returns (rs_pct, allow_long, allow_short) from live NIFTYBEES data."""
    try:
        df_nifty = _load_5m_parquet(NIFTYBEES_TICKER)
        if df_nifty is None or df_nifty.empty:
            return 0.0, True, True

        today = slot_ist.date()
        dt = pd.to_datetime(df_nifty["date"], errors="coerce")
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(IST)
        else:
            dt = dt.dt.tz_convert(IST)
        df_nifty["_dt"] = dt
        df_today = df_nifty[df_nifty["_dt"].dt.date == today].copy()
        df_today = df_today[df_today["_dt"] <= slot_ist].copy()
        if len(df_today) < 2:
            return 0.0, True, True

        df_today = df_today.sort_values("_dt")
        first_slot = pd.Timestamp(df_today["_dt"].iloc[0]).floor("min")
        session_open = _session_open_timestamp(today)
        session_complete = bool(first_slot <= session_open)

        day_open  = float(df_today["open"].iloc[0])
        day_close = float(df_today["close"].iloc[-1])
        if day_open <= 0:
            return 0.0, True, True
        day_move_pct = (day_close - day_open) / day_open * 100.0

        lookback = int(NIFTY_RS_LOOKBACK_BARS)
        if len(df_today) <= lookback:
            rs_pct = day_move_pct
        else:
            close_now  = float(df_today["close"].iloc[-1])
            close_past = float(df_today["close"].iloc[-(lookback + 1)])
            rs_pct = (close_now - close_past) / close_past * 100.0 if close_past > 0 else 0.0

        allow_long, allow_short, reason = _resolve_nifty_context_flags(
            day_move_pct=day_move_pct,
            rs_pct=rs_pct,
            session_complete=session_complete,
        )
        print(
            f"[NIFTY_RS] slot={slot_ist.strftime('%H:%M')} "
            f"day_move={day_move_pct:+.2f}% rs={rs_pct:+.2f}% "
            f"allow_long={allow_long} allow_short={allow_short} reason={reason}",
            flush=True,
        )
        return rs_pct, allow_long, allow_short
    except Exception as exc:
        print(f"[NIFTY_RS] ERROR: {exc} - defaulting rs_pct=0.0", flush=True)
        return 0.0, True, True


# ===========================================================================
# CONFIG BUILD
# ===========================================================================
def _build_v16_cfgs() -> Tuple[StrategyConfig, StrategyConfig]:
    short_builder = getattr(v16_runner, "default_short_config", None)
    short_cfg = short_builder() if callable(short_builder) else default_short_config()
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    long_cfg  = long_builder() if callable(long_builder) else default_long_config_v11()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.stop_pct   = 0.0075
    long_cfg.stop_pct    = 0.0075
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct  = float(TEST_LONG_TARGET_PCT)
    short_cfg.dir_15m = DIR_5M
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m  = DIR_5M
    long_cfg.end_15m  = END_5M
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail  = True
    # Live 5-minute flow must scan as soon as the first valid intraday setup can exist.
    short_cfg.min_bars_for_scan = LIVE_MIN_BARS_FOR_SCAN
    long_cfg.min_bars_for_scan  = LIVE_MIN_BARS_FOR_SCAN
    return short_cfg, long_cfg


def _scan_short_prepared_live(ticker: str, df_prepared: pd.DataFrame, short_cfg: StrategyConfig):
    scan_fn = getattr(v16_runner, "scan_short_prepared", None)
    if callable(scan_fn):
        return scan_fn(ticker, df_prepared, short_cfg)
    return _base_scan_short_prepared(ticker, df_prepared, short_cfg)


def _scan_long_prepared_live(ticker: str, df_prepared: pd.DataFrame, long_cfg: StrategyConfig):
    scan_fn = getattr(v16_runner, "scan_long_prepared", None)
    if callable(scan_fn):
        return scan_fn(ticker, df_prepared, long_cfg)
    return _base_scan_long_prepared(ticker, df_prepared, long_cfg)


# ===========================================================================
# PARTITION SCAN WORKER — NO FILTERS
# ===========================================================================
def _normalize_snapshot_shard_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "date" in out.columns:
        dt = pd.to_datetime(out["date"], errors="coerce")
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(IST)
        else:
            dt = dt.dt.tz_convert(IST)
        out["date"] = dt
        out = out.dropna(subset=["date"])
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    sort_cols = [c for c in ["ticker", "date"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def _split_tickers_for_scan_shards(tickers: List[str], shard_count: int) -> List[List[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    shard_count = max(1, int(shard_count))
    base_size, remainder = divmod(len(ordered), shard_count)
    out: List[List[str]] = []
    start = 0
    for idx in range(shard_count):
        size = base_size + (1 if idx < remainder else 0)
        out.append(ordered[start:start + size])
        start += size
    return out


def _scan_partition_worker(
    slot_iso: str,
    partition_name: str,
    partition_tickers: List[str],
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    use_slot_snapshots: bool,
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    """Scan one shard — both LONG and SHORT, NO filters applied."""
    slot_ts = pd.Timestamp(slot_iso)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    today = slot_ts.date()

    part_short_rows: List[dict] = []
    part_long_rows: List[dict] = []
    errors = 0
    started = time.perf_counter()
    source_rows = 0

    if use_slot_snapshots:
        shard_df = _normalize_snapshot_shard_df(read_shard_snapshot(slot_ts, partition_name))
        source_rows = int(len(shard_df))
        grouped_frames = list(shard_df.groupby("ticker", sort=True)) if not shard_df.empty else []
    else:
        grouped_frames = [(ticker, _load_5m_parquet(ticker)) for ticker in partition_tickers]

    scanned_tickers = 0
    for ticker, df in grouped_frames:
        try:
            if df is None or df.empty:
                continue
            scanned_tickers += 1

            df_prepared = prepare_session_bars_for_scan(df, short_cfg)
            if df_prepared is None or df_prepared.empty:
                continue

            if "date" in df_prepared.columns:
                df_prepared = df_prepared[pd.to_datetime(df_prepared["date"]).dt.date == today]
            if df_prepared.empty:
                continue

            if "datetime" in df_prepared.columns:
                bar_times = pd.to_datetime(df_prepared["datetime"], utc=True).dt.tz_convert(IST)
                df_prepared = df_prepared[bar_times <= slot_ts]
            if df_prepared.empty:
                continue

            # SHORT scan — no filter, just raw pattern detection
            s_trades = _scan_short_prepared_live(ticker, df_prepared, short_cfg)
            for trade in s_trades:
                row = asdict(trade) if not isinstance(trade, dict) else trade
                row["side"] = "SHORT"
                part_short_rows.append(row)

            # LONG scan — no filter, just raw pattern detection
            l_trades = _scan_long_prepared_live(ticker, df_prepared, long_cfg)
            for trade in l_trades:
                row = asdict(trade) if not isinstance(trade, dict) else trade
                row["side"] = "LONG"
                part_long_rows.append(row)

        except Exception:
            errors += 1

    elapsed = time.perf_counter() - started
    meta = {
        "partition": partition_name,
        "tickers": int(scanned_tickers),
        "errors": int(errors),
        "short_rows": len(part_short_rows),
        "long_rows": len(part_long_rows),
        "source_rows": int(source_rows),
        "elapsed_sec": round(elapsed, 3),
    }
    return meta, part_short_rows, part_long_rows


def _scan_slot(
    slot_ist: datetime,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    tickers: Optional[List[str]] = None,
    prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """Scan all tickers — no filters, returns raw signal dicts."""
    tickers = list(tickers or _list_tickers_5m())
    partitions = [p for p in _split_tickers_for_scan_shards(tickers, SCAN_SHARDS) if p]
    snapshot_meta: Dict[str, Any] = {}
    shard_names: List[str] = []

    if USE_SLOT_SNAPSHOTS:
        snapshot_meta = dict(prebuilt_snapshot_meta or {})
        if not snapshot_meta:
            snapshot_meta = build_slot_snapshots(
                slot_ist,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
        shard_names = sorted(str(k) for k in (snapshot_meta.get("snapshot_paths") or {}).keys())
    else:
        shard_names = [f"{idx:02d}" for idx, _p in enumerate(partitions, 1)]

    effective_workers = min(max(1, int(SCAN_MAX_WORKERS)), len(shard_names))

    short_rows: List[dict] = []
    long_rows: List[dict] = []
    partition_metas: List[Dict[str, Any]] = []

    print(
        f"[SCAN] Starting sharded scan: tickers={len(tickers)} "
        f"shards={len(shard_names)} workers={effective_workers} "
        f"mode={'snapshot' if USE_SLOT_SNAPSHOTS else 'raw'} [NO FILTERS]",
        flush=True,
    )

    scan_started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=effective_workers, mp_context=mp.get_context("spawn")) as executor:
        future_map = {
            executor.submit(
                _scan_partition_worker,
                slot_ist.isoformat(),
                shard_names[idx - 1],
                partition_tickers,
                short_cfg,
                long_cfg,
                USE_SLOT_SNAPSHOTS,
            ): idx
            for idx, partition_tickers in enumerate(partitions, 1)
        }
        for future in as_completed(future_map):
            meta, part_short_rows, part_long_rows = future.result()
            partition_metas.append(meta)
            short_rows.extend(part_short_rows)
            long_rows.extend(part_long_rows)
            print(
                f"  [SCAN] shard={meta['partition']} tickers={meta['tickers']} "
                f"short={meta['short_rows']} long={meta['long_rows']} "
                f"errors={meta['errors']} elapsed={meta['elapsed_sec']:.1f}s",
                flush=True,
            )

    scan_elapsed = time.perf_counter() - scan_started
    total_elapsed = float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed
    print(
        f"[SCAN] Done: raw_short={len(short_rows)} raw_long={len(long_rows)} "
        f"scan_elapsed={scan_elapsed:.1f}s total_elapsed={total_elapsed:.1f}s",
        flush=True,
    )
    meta_out = {
        "scan_elapsed_sec": round(scan_elapsed, 3),
        "total_elapsed_sec": round(total_elapsed, 3),
    }
    return short_rows, long_rows, meta_out


# ===========================================================================
# PENDING POOL — JSON / CSV
# ===========================================================================
def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        v = float(val)
        import math
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _pending_json_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / PENDING_JSON_PATTERN.format(date_str)


def _pending_csv_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / PENDING_CSV_PATTERN.format(date_str)


def _load_pending_state(date_str: str) -> Dict[str, Any]:
    """Load today's pending state JSON. Returns empty state if missing."""
    path = _pending_json_path(date_str)
    if not path.exists():
        return {"date": date_str, "last_updated": "", "signals": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"date": date_str, "last_updated": "", "signals": []}


def _write_pending_state_atomic(state: Dict[str, Any], date_str: str) -> None:
    """Atomically write the pending state JSON (temp → rename)."""
    path = _pending_json_path(date_str)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)


def _write_pending_csv(state: Dict[str, Any], date_str: str) -> None:
    """Write dashboard-readable CSV from pending state."""
    path = _pending_csv_path(date_str)
    signals = state.get("signals", [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PENDING_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for sig in signals:
            writer.writerow({
                "signal_id":     sig.get("signal_id", ""),
                "ticker":        sig.get("ticker", ""),
                "side":          sig.get("side", ""),
                "signal_datetime": sig.get("signal_datetime", ""),
                "signal_entry_datetime_ist": sig.get("signal_entry_datetime_ist", ""),
                "signal_bar_time": sig.get("signal_bar_time", ""),
                "added_at":      sig.get("added_at", ""),
                "signal_price":  sig.get("signal_price", ""),
                "entry_price":   sig.get("entry_price", ""),
                "stop_price":    sig.get("stop_price", ""),
                "target_price":  sig.get("target_price", ""),
                "quality_score": sig.get("quality_score", ""),
                "avwap_dist_atr": sig.get("avwap_dist_atr", ""),
                "rsi_signal":    sig.get("rsi_signal", ""),
                "adx":           sig.get("adx", ""),
                "rs_pct":        sig.get("rs_pct", ""),
                "setup":         sig.get("setup", ""),
                "status":        sig.get("status", ""),
                "expires_at":    sig.get("expires_at", ""),
                "filter_reason": sig.get("filter_reason", ""),
                "detection_time": sig.get("detection_time", ""),
                "detection_price": sig.get("detection_price", ""),
            })


def _write_pending_pool(
    short_rows: List[dict],
    long_rows: List[dict],
    slot_ist: datetime,
    rs_pct: float,
    date_str: str,
) -> Tuple[int, int]:
    """
    Merge new raw scan results into the pending pool JSON/CSV.

    Returns (added, deduped, total_pending).
    """
    state = _load_pending_state(date_str)
    existing_ids: set = {s["signal_id"] for s in state.get("signals", [])}

    added = 0
    deduped = 0
    now_str = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    all_rows = [(r, "SHORT") for r in short_rows] + [(r, "LONG") for r in long_rows]

    for row, side in all_rows:
        ticker = str(row.get("ticker", "")).upper().strip()
        if not ticker:
            continue

        signal_time_raw = row.get(
            "signal_time_ist",
            row.get("signal_bar_time_ist", row.get("bar_time_ist", row.get("signal_datetime", ""))),
        )
        entry_time_raw = row.get("entry_time_ist", row.get("signal_entry_datetime_ist", signal_time_raw))
        signal_time_ts = base_v15._parse_ist_timestamp(str(signal_time_raw))
        entry_time_ts  = base_v15._parse_ist_timestamp(str(entry_time_raw))
        if entry_time_ts is None:
            continue

        setup    = str(row.get("setup", ""))
        entry_t  = str(entry_time_ts)
        signal_id = base_v15._generate_signal_id(ticker, side, entry_t, setup)

        if signal_id in existing_ids:
            deduped += 1
            continue

        entry_price  = _safe_float(row.get("entry_price", 0.0))
        signal_price = _safe_float(row.get("signal_price", entry_price))
        stop_price   = _safe_float(row.get("stop_price", row.get("sl_price", 0.0)))
        target_price = _safe_float(row.get("target_price", 0.0))

        notional = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
        qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

        pending_entry: Dict[str, Any] = {
            "signal_id":       signal_id,
            "ticker":          ticker,
            "side":            side,
            "source_slot":     slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            "signal_datetime": str(signal_time_ts or entry_time_ts),
            "signal_entry_datetime_ist": entry_t,
            "signal_bar_time": str(signal_time_ts or entry_time_ts),
            "added_at":        now_str,
            "signal_price":    round(signal_price, 2),
            "entry_price":     round(entry_price, 2),
            "stop_price":      round(stop_price, 2),
            "target_price":    round(target_price, 2),
            "quality_score":   round(_safe_float(row.get("quality_score", 0.0)), 4),
            "avwap_dist_atr":  round(_safe_float(row.get("avwap_dist_atr_signal", 0.0)), 4),
            "rsi_signal":      round(_safe_float(row.get("rsi_signal", 0.0)), 2),
            "adx":             round(_safe_float(row.get("adx_signal", row.get("adx", 0.0))), 2),
            "rs_pct":          round(rs_pct, 4),
            "bars_from_open":  int(row.get("bars_from_open", 0) or 0),
            "or_high":         round(_safe_float(row.get("or_high", 0.0)), 2),
            "or_low":          round(_safe_float(row.get("or_low", 0.0)), 2),
            "entry_bar_vol_ratio": round(_safe_float(row.get("entry_bar_vol_ratio", 0.0)), 4),
            "quantity":        qty,
            "setup":           setup,
            "impulse_type":    str(row.get("impulse_type", "")),
            "expires_at":      _compute_expires_at(slot_ist, side),
            "status":          "pending",
            "detection_time":  None,
            "detection_price": None,
            "filter_reason":   None,
        }
        state["signals"].append(pending_entry)
        existing_ids.add(signal_id)
        added += 1

    state["last_updated"] = now_str
    _write_pending_state_atomic(state, date_str)
    _write_pending_csv(state, date_str)

    total_pending = sum(1 for s in state["signals"] if s.get("status") == "pending")
    return added, deduped, total_pending


# ===========================================================================
# MAIN LOOP
# ===========================================================================
def main() -> None:
    if mp.current_process().name != "MainProcess":
        return
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min SIGNAL ENGINE (Stage 1 — Raw Pool)\n"
        f"  DATA_5M_DIR   : {DIR_5M}\n"
        f"  PENDING_DIR   : {RUNTIME_LIVE_SIGNALS_DIR}\n"
        "  FILTERS        : NONE — raw pattern detection only\n"
        "  OUTPUT         : pending_signals_YYYY-MM-DD_v16_5min.json / .csv\n"
        "=" * 70,
        flush=True,
    )

    _touch_status("STARTING")
    short_cfg, long_cfg = _build_v16_cfgs()
    tickers = _list_tickers_5m()
    holidays = base_v15._read_holidays_safe()
    cache_day: Optional[Any] = None
    print(f"[INFO] Tickers in 5-min dir: {len(tickers)}", flush=True)

    while True:
        now = _now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP_TIME:
            _touch_status("STOPPED_AFTER_CUTOFF")
            _touch_heartbeat("STOPPED")
            print("[STOP] Hard-stop reached. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[SKIP] Not a trading day. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_5min_slot(now)
        if slot.date() != now.date():
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            _sleep_until_resilient(
                slot, phase="WAIT_SLOT",
                slot=slot.strftime("%H:%M"),
                next_wake=slot.strftime("%Y-%m-%d %H:%M:%S%z"),
                poll_seconds=15.0,
            )

        now = _now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        if cache_day != slot.date():
            clear_slot_snapshot_cache()
            cache_day = slot.date()

        slot_start    = time.perf_counter()
        date_str      = slot.strftime("%Y-%m-%d")
        slot_str      = slot.strftime("%H:%M")
        print(f"\n[SLOT] {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        _touch_status("RUNNING", phase="WAIT_DATA", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="WAIT_DATA", slot=slot_str)

        ready, ratio, waited, checked = _wait_for_slot_data_ready(slot, tickers)
        print(
            f"[WAIT] slot={slot_str} ready={ready} "
            f"fresh_ratio={ratio:.2f} waited={waited:.1f}s checked={checked}",
            flush=True,
        )
        if not ready and SKIP_STALE_SLOT_ON_TIMEOUT:
            _touch_status("RUNNING", phase="SKIP_STALE", slot=slot_str)
            print(
                f"[SKIP_SLOT] slot={slot_str} data freshness timed out "
                f"(ratio={ratio:.2f}). Skipping stale scan.",
                flush=True,
            )
            continue

        _touch_status("RUNNING", phase="SCAN", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="SCAN", slot=slot_str)

        prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None
        if USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = build_slot_snapshots(
                slot,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )

        # Compute NIFTY RS for context (stored in pending JSON, not used for filtering here)
        rs_pct, _allow_long, _allow_short = _compute_nifty_rs_at_slot(slot)

        # Scan ALL tickers — NO filters applied
        short_rows, long_rows, _scan_meta = _scan_slot(
            slot, short_cfg, long_cfg,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )

        # Write to pending pool (dedup by signal_id)
        added, deduped, total_pending = _write_pending_pool(
            short_rows, long_rows, slot, rs_pct, date_str
        )

        elapsed = time.perf_counter() - slot_start
        print(
            f"[SIGNAL_ENGINE] slot={slot_str} "
            f"raw_short={len(short_rows)} raw_long={len(long_rows)} "
            f"new_pending={added} ({deduped} deduped) "
            f"total_pending={total_pending} "
            f"total_elapsed={elapsed:.1f}s",
            flush=True,
        )

        _touch_status("RUNNING", phase="SCAN_DONE", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot_str)

        # Wait briefly before next slot
        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        if _now_ist() < next_slot:
            time.sleep(2.0)


if __name__ == "__main__":
    mp.freeze_support()
    if mp.current_process().name == "MainProcess":
        main()
