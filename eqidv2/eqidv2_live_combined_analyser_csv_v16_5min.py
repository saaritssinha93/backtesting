# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Live Signal Scanner
======================================
Persistent 5-min slot scanner for V16 Run 7 canonical parameters.

Operational file names stay on the v16 path, but this live analyser imports the
v17f runner patch bundle first so live scan/context/filter logic matches v17f.

Scans all 1041 NSE tickers every 5 minutes using:
  - V16 Run 7 scanner config (avwap_min_consec_closes=1, mod_impulse_min_atr=0.30, volume=0.80)
  - Backtest-parity NIFTY intraday context and per-stock RS filtering
  - V16 anti-exhaustion post-scan filters (RSI dead zone, QS two-band, AVWAP dist cap)
  - Unified SL/TGT: 0.75% stop, 1.00% target

Outputs default to shadow CSVs. Official executable signal CSVs are owned by
the SEE -> PF -> DE pipeline; set EQIDV16_5MIN_DIRECT_SIGNAL_CSV_MODE=direct
only for an explicit emergency bypass.

Status files (to logs/):
  eqidv2_live_combined_analyser_csv_v16_5min.status
  eqidv2_live_combined_analyser_csv_v16_5min.heartbeat
  eqidv2_live_combined_analyser_csv_v16_5min.log  (stdout tee)

Usage:
    python eqidv2_live_combined_analyser_csv_v16_5min.py
"""

from __future__ import annotations

import csv
import json
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# V16 runner — params, filters, scan helpers
# ---------------------------------------------------------------------------
import avwap_combined_runner_v17f_5min as _v17f_runner  # noqa: F401
import avwap_combined_runner_v16_5min as v16_runner
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    _apply_v16_post_scan_filters,
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
    read_15m_parquet,
    list_tickers_15m,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
    scan_all_days_for_ticker_prepared as _base_scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
    scan_all_days_for_ticker_prepared as _base_scan_long_prepared,
)

# ---------------------------------------------------------------------------
# V15 base — utilities (IST helpers, file locking, signal CSV writing)
# ---------------------------------------------------------------------------
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

SHORT_SIGNAL_CSV_PATTERN = "signals_{}_v16_5min_short.csv"
LONG_SIGNAL_CSV_PATTERN  = "signals_{}_v16_5min_long.csv"
SHADOW_SHORT_SIGNAL_CSV_PATTERN = "shadow_signals_{}_v16_5min_short.csv"
SHADOW_LONG_SIGNAL_CSV_PATTERN  = "shadow_signals_{}_v16_5min_long.csv"

_DIRECT_SIGNAL_CSV_MODE_RAW = str(
    os.getenv(
        "EQIDV16_5MIN_DIRECT_SIGNAL_CSV_MODE",
        os.getenv("EQIDV16_5MIN_SIGNAL_CSV_MODE", "shadow"),
    )
).strip().lower()
if _DIRECT_SIGNAL_CSV_MODE_RAW in {"1", "true", "yes", "on", "direct", "live"}:
    DIRECT_SIGNAL_CSV_MODE = "direct"
elif _DIRECT_SIGNAL_CSV_MODE_RAW in {"0", "false", "no", "off", "disabled", "disable", "none"}:
    DIRECT_SIGNAL_CSV_MODE = "disabled"
else:
    DIRECT_SIGNAL_CSV_MODE = "shadow"

SLOT_MINUTES        = 5
START_TIME          = dtime(9, 15)
END_TIME            = dtime(15, 0)
HARD_STOP_TIME      = dtime(15, 30)
TAIL_ROWS           = int(os.getenv("EQIDV16_5MIN_TAIL_ROWS", "260"))
SLOT_START_OFFSET_SECONDS = int(os.getenv("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "5"))
SLOT_READY_MAX_WAIT_SECONDS = int(os.getenv("EQIDV16_5MIN_SLOT_READY_MAX_WAIT_SECONDS", "90"))
SLOT_READY_POLL_SECONDS = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_POLL_SECONDS", "2")))
SLOT_READY_SAMPLE_SIZE = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_SAMPLE_SIZE", "24")))
SLOT_READY_MIN_FRESH_RATIO = float(os.getenv("EQIDV16_5MIN_SLOT_READY_MIN_FRESH_RATIO", "0.95"))
_SLOT_READY_PRIORITY_TICKERS_RAW = os.getenv(
    "EQIDV16_5MIN_SLOT_READY_PRIORITY_TICKERS",
    "NIFTYBEES,SBIN,RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY",
)
SLOT_READY_PRIORITY_TICKERS = [
    str(part).strip().upper()
    for part in str(_SLOT_READY_PRIORITY_TICKERS_RAW).split(",")
    if str(part).strip()
]
USE_SCHEDULER_READY_MARKER = str(
    os.getenv("EQIDV16_5MIN_USE_SCHEDULER_READY_MARKER", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
SLOT_READY_MARKER_MIN_FRESH_RATIO = float(
    os.getenv("EQIDV16_5MIN_SLOT_READY_MARKER_MIN_FRESH_RATIO", "0.95")
)
SKIP_STALE_SLOT_ON_TIMEOUT = str(os.getenv("EQIDV16_5MIN_SKIP_STALE_SLOT_ON_TIMEOUT", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
NEUTRALIZE_WEAK_NIFTY_CONTEXT = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_WEAK_NIFTY_CONTEXT", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = str(
    os.getenv("EQIDV16_5MIN_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
NEUTRALIZE_PARTIAL_NIFTY_SESSION = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_PARTIAL_NIFTY_SESSION", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
SCAN_SHARDS         = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_SHARDS", "10")))
SCAN_MAX_WORKERS    = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_MAX_WORKERS", str(SCAN_SHARDS))))
SNAPSHOT_MAX_WORKERS = max(1, int(os.getenv("EQIDV16_5MIN_SNAPSHOT_MAX_WORKERS", str(SCAN_MAX_WORKERS))))
USE_SLOT_SNAPSHOTS = str(os.getenv("EQIDV16_5MIN_USE_SLOT_SNAPSHOTS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
USE_SNAPSHOT_ROLLING_CACHE = str(os.getenv("EQIDV16_5MIN_USE_ROLLING_CACHE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
BENCHMARK_SLOT_RAW  = str(os.getenv("EQIDV16_5MIN_BENCHMARK_SLOT", "")).strip()

INTRADAY_LEVERAGE   = 5.0
DEFAULT_POSITION_SIZE_RS = float(
    os.getenv("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000")
)
LIVE_MIN_BARS_FOR_SCAN = max(4, int(os.getenv("EQIDV16_5MIN_LIVE_MIN_BARS_FOR_SCAN", "4")))
LIVE_NIFTY_CONTEXT_OR_END_TIME = dtime(9, 20)
LIVE_NIFTY_CONTEXT_CONFIRM_TIME = dtime(9, 20)
_FORCE_SIGNAL_QUANTITY_RAW = str(os.getenv("EQIDV16_5MIN_FORCE_SIGNAL_QUANTITY", "")).strip()
try:
    FORCE_SIGNAL_QUANTITY: Optional[int] = (
        max(1, int(_FORCE_SIGNAL_QUANTITY_RAW)) if _FORCE_SIGNAL_QUANTITY_RAW else None
    )
except Exception:
    FORCE_SIGNAL_QUANTITY = None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


V17B_RS_ATR_NORM_ENABLED = True
V17B_RS_ATR_NORM_THRESH_SHORT_BOTH = _env_float(
    "EQIDV17B_RS_ATR_NORM_THRESH_SHORT_BOTH", 0.50
)
V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT = _env_float(
    "EQIDV17B_RS_ATR_DIRECTIONAL_THRESH_SHORT", 0.35
)
V17B_RS_ATR_FALLBACK_PCT_SHORT = _env_float(
    "EQIDV17B_RS_ATR_FALLBACK_PCT_SHORT", 0.40
)

v16_runner.NIFTY_CONTEXT_OR_END_TIME = LIVE_NIFTY_CONTEXT_OR_END_TIME
v16_runner.NIFTY_CONTEXT_CONFIRM_TIME = LIVE_NIFTY_CONTEXT_CONFIRM_TIME

_BASE_DIR  = Path(__file__).resolve().parent
_LOG_DIR   = _BASE_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
SLOT_READY_MARKER_DIR = runtime_dir("slot_ready_5m")
SLOT_READY_STATUS_PATH = _LOG_DIR / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"

_SCRIPT_NAME = "eqidv2_live_combined_analyser_csv_v16_5min.py"
_LOG_FILE    = _LOG_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.log"

# Signal CSV schema (shared with V15 executors)
SIGNAL_CSV_COLUMNS = base_v15.SIGNAL_CSV_COLUMNS


# ===========================================================================
# STDOUT TEE — mirrors output to log file
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
        tee_out = _Tee(base_stdout, fh)
        tee_err = _Tee(base_stderr, fh)
        sys.stdout = tee_out  # type: ignore[assignment]
        sys.stderr = tee_err  # type: ignore[assignment]
    except Exception:
        pass


def _sleep_until_resilient(
    target_dt: datetime,
    *,
    phase: str,
    slot: Optional[str] = None,
    next_wake: Optional[str] = None,
    poll_seconds: float = 30.0,
) -> None:
    """Sleep in short chunks so laptop suspend/resume does not strand the loop overnight."""
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


# ===========================================================================
# HELPERS — 5-min slot management
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


def _list_tickers_5m() -> List[str]:
    """Return sorted ticker list from 5-min parquet directory."""
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


def _sample_tickers_for_slot_ready(tickers: List[str], sample_size: int) -> List[str]:
    if not tickers:
        return []
    uniq = sorted({str(t).upper().strip() for t in tickers if str(t).strip()})
    if not uniq:
        return []
    size = min(max(1, int(sample_size)), len(uniq))
    if size >= len(uniq):
        return uniq

    picks: List[str] = []
    for ticker in SLOT_READY_PRIORITY_TICKERS:
        if ticker in uniq and ticker not in picks:
            picks.append(ticker)
            if len(picks) >= size:
                return sorted(picks)

    remaining = [ticker for ticker in uniq if ticker not in picks]
    remaining_slots = size - len(picks)
    if remaining_slots <= 0:
        return sorted(picks)
    if remaining_slots == 1:
        picks.append(remaining[len(remaining) // 2])
        return sorted(set(picks))

    step = max(1.0, (len(remaining) - 1) / float(remaining_slots - 1))
    for idx in range(remaining_slots):
        pos = int(round(idx * step))
        pos = max(0, min(len(remaining) - 1, pos))
        ticker = remaining[pos]
        if ticker not in picks:
            picks.append(ticker)

    return sorted(set(picks))


def _last_bar_for_ticker_ist_5m(ticker: str) -> pd.Timestamp:
    df_tail = _load_5m_parquet(ticker, n=3)
    if df_tail.empty or "date" not in df_tail.columns:
        return pd.NaT
    ts = pd.Timestamp(df_tail.iloc[-1]["date"])
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.floor("min")


def _expected_5m_session_stamps(slot: datetime) -> List[pd.Timestamp]:
    slot_ts = pd.Timestamp(slot)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    session_open = _session_open_timestamp(slot_ts.date())
    expected: List[pd.Timestamp] = [session_open]
    first_close = session_open + pd.Timedelta(minutes=SLOT_MINUTES)
    if slot_ts >= first_close:
        expected.extend(list(pd.date_range(start=first_close, end=slot_ts.floor("min"), freq=f"{SLOT_MINUTES}min", tz=IST)))
    return expected


def _ticker_ready_for_slot_5m(ticker: str, slot: datetime) -> Tuple[bool, pd.Timestamp, List[pd.Timestamp]]:
    df_tail = _load_5m_parquet(ticker, n=max(TAIL_ROWS, 96))
    if df_tail.empty or "date" not in df_tail.columns:
        return False, pd.NaT, []

    dt = pd.to_datetime(df_tail["date"], errors="coerce").dropna()
    if dt.empty:
        return False, pd.NaT, []
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(IST)
    else:
        dt = dt.dt.tz_convert(IST)

    target_slot = pd.Timestamp(slot)
    if target_slot.tzinfo is None:
        target_slot = target_slot.tz_localize(IST)
    else:
        target_slot = target_slot.tz_convert(IST)
    target_slot = target_slot.floor("min")

    last_bar = pd.Timestamp(dt.max()).floor("min")
    if last_bar < target_slot:
        return False, last_bar, []

    expected = _expected_5m_session_stamps(target_slot.to_pydatetime())
    target_day = target_slot.date()
    actual = {
        pd.Timestamp(ts).floor("min")
        for ts in dt[dt.dt.date == target_day].tolist()
    }
    missing = [stamp for stamp in expected if stamp.floor("min") not in actual]
    if missing:
        return False, last_bar, missing

    return True, last_bar, []


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


def _wait_ready_via_marker(slot: datetime, started: datetime) -> Optional[Tuple[bool, float, float, int]]:
    payload = _load_slot_ready_marker(slot)
    if not payload:
        return None
    waited = max(0.0, (_now_ist() - started).total_seconds())
    ratio = float(payload.get("fresh_ratio", 0.0) or 0.0)
    checked = int(payload.get("checked_count", 0) or 0)
    print(
        f"[WAIT] 5min slot readiness accepted via scheduler marker "
        f"(ratio={ratio:.2f}, checked={checked}, source={payload.get('source', 'marker')})",
        flush=True,
    )
    return True, ratio, waited, checked


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
    marker_ready = _wait_ready_via_marker(slot, now)
    if marker_ready is not None:
        return marker_ready
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
    last_ratio = 0.0
    last_checked = 0

    # Fast freshness gate: check file mtime instead of reading parquet content.
    # os.stat() costs ~0.1ms per file vs ~15ms for a parquet read.
    # 1044 files * 0.1ms = ~100ms total — no threading needed.
    _slot_unix_ts = target_slot.timestamp()

    def _ticker_fresh_mtime(ticker: str) -> bool:
        path = os.path.join(DIR_5M, f"{ticker}{END_5M}")
        try:
            return os.stat(path).st_mtime >= _slot_unix_ts
        except OSError:
            return False

    while True:
        marker_ready = _wait_ready_via_marker(slot, started)
        if marker_ready is not None:
            return marker_ready
        fresh = 0
        checked = len(sample)
        for ticker in sample:
            if _ticker_fresh_mtime(ticker):
                fresh += 1

        ratio = (fresh / checked) if checked > 0 else 0.0
        last_ratio = ratio
        last_checked = checked
        if checked > 0 and ratio >= float(SLOT_READY_MIN_FRESH_RATIO):
            waited = (_now_ist() - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness ready after {waited:.1f}s "
                f"(fresh={fresh}/{checked}, ratio={ratio:.2f}, target>={target_slot.strftime('%H:%M')})",
                flush=True,
            )
            return True, ratio, waited, checked

        now = _now_ist()
        if now >= deadline:
            waited = (now - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness timeout after {waited:.1f}s "
                f"(ratio={ratio:.2f}, checked={checked}, target>={target_slot.strftime('%H:%M')})",
                flush=True,
            )
            return False, last_ratio, waited, last_checked

        time.sleep(min(float(SLOT_READY_POLL_SECONDS), max(0.0, (deadline - now).total_seconds())))


def _split_tickers_for_scan_shards(tickers: List[str], shard_count: int) -> List[List[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    shard_count = max(1, int(shard_count))
    base_size, remainder = divmod(len(ordered), shard_count)
    out: List[List[str]] = []
    start = 0
    for idx in range(shard_count):
        size = base_size + (1 if idx < remainder else 0)
        end = start + size
        out.append(ordered[start:end])
        start = end
    return out


def _session_open_timestamp(session_date: Any) -> pd.Timestamp:
    if isinstance(session_date, pd.Timestamp):
        date_value = session_date.date()
    elif isinstance(session_date, datetime):
        date_value = session_date.date()
    else:
        date_value = session_date
    return pd.Timestamp(
        IST.localize(datetime.combine(date_value, START_TIME))
    ).floor("min")


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

    allow_long = rs_pct >= float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
    allow_short = rs_pct <= -float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)
    if allow_long or allow_short:
        return allow_long, allow_short, "threshold"

    if DIRECTIONAL_NIFTY_CONTEXT_FALLBACK:
        if day_move_pct > 0:
            return True, False, "directional_daymove_long"
        if day_move_pct < 0:
            return False, True, "directional_daymove_short"

    return False, False, "threshold_blocked"


# ===========================================================================
# NIFTY RS CONTEXT — compute live BOTH-mode RS for current slot
# ===========================================================================
def _compute_nifty_rs_at_slot(slot_ist: datetime) -> Tuple[bool, bool, float]:
    """
    Load NIFTYBEES 5-min data and compute RS at the given slot.

    Returns:
        (allow_long, allow_short, rs_pct)
    """
    try:
        df_nifty = _load_5m_parquet(NIFTYBEES_TICKER)
        if df_nifty is None or df_nifty.empty:
            return True, True, 0.0  # no data — allow both (permissive fallback)

        # Filter to today up to slot.
        today = slot_ist.date()
        if "date" in df_nifty.columns:
            dt = pd.to_datetime(df_nifty["date"], errors="coerce")
            if getattr(dt.dt, "tz", None) is None:
                dt = dt.dt.tz_localize(IST)
            else:
                dt = dt.dt.tz_convert(IST)
            df_nifty["_dt"] = dt
        elif "datetime" in df_nifty.columns:
            df_nifty["_dt"] = pd.to_datetime(df_nifty["datetime"], utc=True).dt.tz_convert(IST)
        elif df_nifty.index.name in ("datetime", "timestamp") or isinstance(df_nifty.index, pd.DatetimeIndex):
            df_nifty["_dt"] = pd.to_datetime(df_nifty.index, utc=True).dt.tz_convert(IST)
        else:
            return True, True, 0.0

        df_today = df_nifty[df_nifty["_dt"].dt.date == today].copy()
        df_today = df_today[df_today["_dt"] <= slot_ist].copy()
        if len(df_today) < 2:
            return True, True, 0.0

        df_today = df_today.sort_values("_dt")
        first_session_slot = pd.Timestamp(df_today["_dt"].iloc[0]).floor("min")
        session_open = _session_open_timestamp(today)
        session_complete = bool(first_session_slot <= session_open)

        # Day move: current close vs first open
        day_open  = float(df_today["open"].iloc[0])
        day_close = float(df_today["close"].iloc[-1])
        if day_open <= 0:
            return True, True, 0.0
        day_move_pct = (day_close - day_open) / day_open * 100.0

        # RS = 4-bar pct change of NIFTYBEES close
        lookback = int(NIFTY_RS_LOOKBACK_BARS)
        if len(df_today) <= lookback:
            rs_pct = day_move_pct  # fallback: use day move
        else:
            close_now  = float(df_today["close"].iloc[-1])
            close_past = float(df_today["close"].iloc[-(lookback + 1)])
            rs_pct = (close_now - close_past) / close_past * 100.0 if close_past > 0 else 0.0

        allow_long, allow_short, decision_reason = _resolve_nifty_context_flags(
            day_move_pct=day_move_pct,
            rs_pct=rs_pct,
            session_complete=session_complete,
        )

        print(
            f"[NIFTY_RS] slot={slot_ist.strftime('%H:%M')} "
            f"day_move={day_move_pct:+.2f}% rs={rs_pct:+.2f}% "
            f"allow_long={allow_long} allow_short={allow_short} "
            f"session_complete={session_complete} reason={decision_reason}",
            flush=True,
        )
        return allow_long, allow_short, rs_pct

    except Exception as exc:
        print(f"[NIFTY_RS] ERROR: {exc} - defaulting to allow both", flush=True)
        return True, True, 0.0


# ===========================================================================
# SNAPSHOT SLOT CONTEXT
# ===========================================================================
def _load_snapshot_slot_context(slot_ist: datetime) -> Tuple[bool, bool, float, Dict[str, Any]]:
    path = snapshot_slot_context_path(slot_ist)
    try:
        if not path.exists():
            raise FileNotFoundError(str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        nifty_context = payload.get("nifty_context") or {}
        allow_long = bool(nifty_context.get("allow_long", True))
        allow_short = bool(nifty_context.get("allow_short", True))
        rs_pct = float(nifty_context.get("rs_pct", 0.0) or 0.0)
        reason = str(nifty_context.get("decision_reason", "") or "")
        print(
            f"[NIFTY_RS] slot={pd.Timestamp(slot_ist).strftime('%H:%M')} "
            f"source=snapshot allow_long={allow_long} allow_short={allow_short} "
            f"rs={rs_pct:+.2f}% reason={reason}",
            flush=True,
        )
        return allow_long, allow_short, rs_pct, payload
    except Exception as exc:
        print(f"[NIFTY_RS] snapshot context unavailable: {exc}", flush=True)
        allow_long, allow_short, rs_pct = _compute_nifty_rs_at_slot(slot_ist)
        return allow_long, allow_short, rs_pct, {}


def _build_live_v16_short_cfg() -> StrategyConfig:
    short_builder = getattr(v16_runner, "default_short_config", None)
    if callable(short_builder):
        return short_builder()
    return default_short_config()


def _build_live_v16_long_cfg() -> StrategyConfig:
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    if callable(long_builder):
        return long_builder()
    return default_long_config_v11()


_V17B_ATR_CACHE: Dict[str, Dict[str, float]] = {}


def _build_stock_atr_map_live(
    ticker: str,
    cfg: StrategyConfig,
    cache: Dict[str, Dict[str, float]],
) -> Dict[str, float]:
    ticker_u = str(ticker).strip().upper()
    if ticker_u in cache:
        return cache[ticker_u]

    out: Dict[str, float] = {}
    path = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not path.exists():
        cache[ticker_u] = out
        return out

    try:
        df = read_15m_parquet(str(path), getattr(cfg, "parquet_engine", "pyarrow"))
        if df is None or df.empty:
            cache[ticker_u] = out
            return out
        atr_col = next((c for c in df.columns if str(c).lower() == "atr"), None)
        if atr_col is None:
            cache[ticker_u] = out
            return out
        df = df.sort_values("date").reset_index(drop=True)
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        df["date"] = dt.dt.tz_convert(IST)
        df["close_num"] = pd.to_numeric(df["close"], errors="coerce")
        df["atr_num"] = pd.to_numeric(df[atr_col], errors="coerce")
        df["atr_pct"] = df["atr_num"] / df["close_num"].replace(0, np.nan) * 100.0
        for ts, atr_pct in zip(df["date"], df["atr_pct"]):
            if np.isfinite(atr_pct) and atr_pct > 0:
                out[v16_runner._ts_to_key_local(ts)] = float(atr_pct)
    except Exception:
        out = {}

    cache[ticker_u] = out
    return out


def _apply_live_short_v17b_context(
    short_df: pd.DataFrame,
    short_cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, int, int]:
    if short_df.empty:
        return short_df, 0, 0

    work = short_df.copy()
    ts_col = "entry_time_ist" if "entry_time_ist" in work.columns else "signal_time_ist"
    ts_series = pd.to_datetime(work[ts_col], errors="coerce")
    if getattr(ts_series.dt, "tz", None) is None:
        ts_series = ts_series.dt.tz_localize(IST)
    else:
        ts_series = ts_series.dt.tz_convert(IST)
    work["ts_key_local"] = ts_series.map(v16_runner._ts_to_key_local)
    work["nifty_context_mode"] = work["ts_key_local"].map(mode_map).fillna("BOTH")

    before = len(work)
    work = work[work["nifty_context_mode"].ne("LONG_ONLY")].copy()
    mode_removed = before - len(work)

    if not getattr(v16_runner, "NIFTY_RS_FILTER_ENABLED", True) or work.empty:
        if "ts_key_local" in work.columns:
            work = work.drop(columns=["ts_key_local"])
        return work, mode_removed, 0

    stock_ret_cache: Dict[str, Dict[str, float]] = {}
    keep_mask: List[bool] = []
    rel_vals: List[float] = []
    rs_removed = 0

    for row in work.itertuples(index=False):
        ts_key = getattr(row, "ts_key_local")
        mode = getattr(row, "nifty_context_mode", "BOTH")
        rel_val = np.nan
        keep = True
        apply_rs = (mode != "BOTH") or bool(getattr(v16_runner, "NIFTY_RS_BOTH_MODE_ENABLED", True))

        if apply_rs:
            stock_ret_map = v16_runner._build_stock_return_map(
                getattr(row, "ticker"), short_cfg, stock_ret_cache
            )
            stock_ret = stock_ret_map.get(ts_key, np.nan)
            nifty_ret = nifty_ret_map.get(ts_key, np.nan)

            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs = float(stock_ret - nifty_ret)
                rel_val = raw_rs
                if V17B_RS_ATR_NORM_ENABLED:
                    atr_map = _build_stock_atr_map_live(
                        getattr(row, "ticker"), short_cfg, _V17B_ATR_CACHE
                    )
                    atr_pct = atr_map.get(ts_key, np.nan)
                    if np.isfinite(atr_pct) and atr_pct > 0:
                        rs_norm = raw_rs / atr_pct
                        thresh = (
                            V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT
                            if mode != "BOTH"
                            else V17B_RS_ATR_NORM_THRESH_SHORT_BOTH
                        )
                        keep = rs_norm <= -thresh
                    else:
                        keep = raw_rs <= -V17B_RS_ATR_FALLBACK_PCT_SHORT
                else:
                    thresh = (
                        float(getattr(v16_runner, "NIFTY_RS_THRESHOLD_PCT", 0.20))
                        if mode != "BOTH"
                        else float(getattr(v16_runner, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT", 0.75))
                    )
                    keep = raw_rs <= -thresh

        keep_mask.append(bool(keep))
        rel_vals.append(rel_val)
        if not keep:
            rs_removed += 1

    work["nifty_rel_strength_pct"] = rel_vals
    work = work[pd.Series(keep_mask, index=work.index)].copy()
    if "ts_key_local" in work.columns:
        work = work.drop(columns=["ts_key_local"])
    return work, mode_removed, rs_removed


def _apply_live_v17b_hybrid_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not mode_map:
        return short_df, long_df

    short_out, long_out = v16_runner._apply_nifty_intraday_context(
        short_df,
        long_df,
        short_cfg,
        mode_map,
        nifty_ret_map,
    )
    print(
        "[NIFTY_CONTEXT] Applied runner context parity: "
        f"SHORT {len(short_df)}->{len(short_out)} | "
        f"LONG {len(long_df)}->{len(long_out)}"
    )
    return short_out, long_out


# ===========================================================================
# BUILD EFFECTIVE V16 CONFIGS
# ===========================================================================
def _build_v16_cfgs() -> Tuple[StrategyConfig, StrategyConfig]:
    short_cfg = _build_live_v16_short_cfg()
    long_cfg  = _build_live_v16_long_cfg()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    # Keep live scanner aligned with the shared v16_5min SL/TGT policy.
    short_cfg.stop_pct   = 0.0075
    long_cfg.stop_pct    = 0.0075
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct  = float(TEST_LONG_TARGET_PCT)
    # Point configs at 5-min data dir
    short_cfg.dir_15m = DIR_5M
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m  = DIR_5M
    long_cfg.end_15m  = END_5M
    # Allow scanning the most recent closed bar (tail_guard=1 instead of 3)
    # reducing signal detection lag from 3-slot to 2-slot in live mode.
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail  = True
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


def _slot_mode_flags_from_context_mode(mode: str) -> Tuple[bool, bool]:
    mode_u = str(mode or "BOTH").strip().upper()
    if mode_u == "LONG_ONLY":
        return True, False
    if mode_u == "SHORT_ONLY":
        return False, True
    return True, True


def _build_backtest_context_state(slot_ist: datetime, cfg: StrategyConfig) -> Dict[str, Any]:
    slot_ts = pd.Timestamp(slot_ist)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)

    try:
        mode_map, nifty_ret_map, source, counts = v16_runner._build_nifty_intraday_context(cfg)
    except Exception as exc:
        print(
            f"[NIFTY_CONTEXT] slot={slot_ts.strftime('%H:%M')} build error={exc!r} -> allow both",
            flush=True,
        )
        mode_map, nifty_ret_map, source, counts = {}, {}, "", {}

    mode = "BOTH"
    rs_pct = 0.0
    if mode_map:
        ts_key = v16_runner._ts_to_key_local(slot_ts)
        mode = str(mode_map.get(ts_key, "BOTH")).strip().upper() or "BOTH"
        rs_val = nifty_ret_map.get(ts_key, np.nan)
        if np.isfinite(rs_val):
            rs_pct = float(rs_val)

    allow_long, allow_short = _slot_mode_flags_from_context_mode(mode if mode_map else "BOTH")
    payload = {
        "nifty_context": {
            "source": str(source or ""),
            "mode": mode if mode_map else "BOTH",
            "allow_long": bool(allow_long),
            "allow_short": bool(allow_short),
            "rs_pct": float(rs_pct),
            "decision_reason": "backtest_intraday_context",
            "context_counts": {str(k): int(v) for k, v in (counts or {}).items()},
        }
    }
    print(
        f"[NIFTY_CONTEXT] slot={slot_ts.strftime('%H:%M')} source={str(source or 'unavailable')} "
        f"mode={payload['nifty_context']['mode']} allow_long={allow_long} "
        f"allow_short={allow_short} rs={rs_pct:+.2f}%",
        flush=True,
    )
    return {
        "allow_long": bool(allow_long),
        "allow_short": bool(allow_short),
        "mode_map": dict(mode_map or {}),
        "nifty_ret_map": dict(nifty_ret_map or {}),
        "payload": payload,
    }


# ===========================================================================
# SIGNAL CSV WRITING
# ===========================================================================
def _signal_csv_path(signal_day_str: str, side: str) -> Path:
    if DIRECT_SIGNAL_CSV_MODE == "shadow":
        pattern = (
            SHADOW_SHORT_SIGNAL_CSV_PATTERN
            if side.upper() == "SHORT"
            else SHADOW_LONG_SIGNAL_CSV_PATTERN
        )
    else:
        pattern = SHORT_SIGNAL_CSV_PATTERN if side.upper() == "SHORT" else LONG_SIGNAL_CSV_PATTERN
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / pattern.format(signal_day_str)


def _safe_float(val: Any, default: float = float("nan")) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except Exception:
        return default


def _write_side_signals_csv(signals: List[dict], side: str, signal_day_str: str) -> int:
    """Write new (deduplicated) signals to the daily side CSV. Returns written count."""
    side_upper = side.upper()
    if DIRECT_SIGNAL_CSV_MODE == "disabled":
        count = sum(1 for s in signals if str(s.get("side", "")).upper() == side_upper)
        print(
            f"[V16_5MIN {side_upper} CSV] written=0 skipped={count} "
            f"mode=disabled (PF/DE owns executable signal CSVs)",
            flush=True,
        )
        return 0
    csv_path = _signal_csv_path(signal_day_str, side_upper)

    signals_side = [s for s in signals if str(s.get("side", "")).upper() == side_upper]
    if not signals_side:
        print(f"[V16_5MIN {side_upper} CSV] written=0 (no signals)", flush=True)
        return 0

    written = 0
    skipped = 0
    received_time = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids  = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        file_exists   = csv_path.exists() and csv_path.stat().st_size > 0
        run_keys: set = set()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SIGNAL_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writeheader()

            for sig in signals_side:
                ticker    = str(sig.get("ticker", "")).upper().strip()
                setup     = str(sig.get("setup", ""))
                signal_time_raw = sig.get(
                    "signal_time_ist",
                    sig.get(
                        "signal_bar_time_ist",
                        sig.get("bar_time_ist", sig.get("signal_datetime", "")),
                    ),
                )
                entry_time_raw = sig.get(
                    "entry_time_ist",
                    sig.get("signal_entry_datetime_ist", signal_time_raw),
                )
                signal_time_ts = base_v15._parse_ist_timestamp(str(signal_time_raw))
                entry_time_ts  = base_v15._parse_ist_timestamp(str(entry_time_raw))
                if not ticker or entry_time_ts is None:
                    skipped += 1
                    continue

                signal_time = str(signal_time_ts or entry_time_ts)
                entry_time  = str(entry_time_ts)
                dedupe_key = base_v15._signal_dedupe_key(ticker, side_upper, entry_time, setup)
                if dedupe_key in existing_keys or dedupe_key in run_keys:
                    skipped += 1
                    continue
                signal_id = base_v15._generate_signal_id(ticker, side_upper, entry_time, setup)
                if signal_id in existing_ids:
                    skipped += 1
                    continue

                entry_price  = _safe_float(sig.get("entry_price",  0.0), 0.0)
                stop_price   = _safe_float(sig.get("stop_price", sig.get("sl_price", 0.0)), 0.0)
                target_price = _safe_float(sig.get("target_price", 0.0), 0.0)
                signal_price = _safe_float(sig.get("signal_price", entry_price), entry_price)
                if FORCE_SIGNAL_QUANTITY is not None:
                    qty = int(FORCE_SIGNAL_QUANTITY)
                else:
                    notional = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
                    qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

                row = {
                    "signal_id":                  signal_id,
                    "signal_datetime":             signal_time,
                    "received_time":               received_time,
                    "detected_time_ist":           received_time,
                    "logtime_ist":                 received_time,
                    "ticker":                      ticker,
                    "side":                        side_upper,
                    "setup":                       setup,
                    "impulse_type":                str(sig.get("impulse_type", "")),
                    "signal_price":                round(signal_price, 2),
                    "entry_price":                 round(entry_price, 2),
                    "stop_price":                  round(stop_price, 2),
                    "target_price":                round(target_price, 2),
                    "quality_score":               round(_safe_float(sig.get("quality_score", 0.0), 0.0), 4),
                    "atr_pct":                     round(_safe_float(sig.get("atr_pct", 0.0), 0.0), 6),
                    "rsi":                         round(_safe_float(sig.get("rsi_signal", sig.get("rsi", 0.0)), 0.0), 2),
                    "adx":                         round(_safe_float(sig.get("adx_signal", sig.get("adx", 0.0)), 0.0), 2),
                    "quantity":                    qty,
                    "signal_entry_datetime_ist":   entry_time,
                    "signal_bar_time_ist":         signal_time,
                }
                writer.writerow(row)
                existing_ids.add(signal_id)
                existing_keys.add(dedupe_key)
                run_keys.add(dedupe_key)
                written += 1

    print(
        f"[V16_5MIN {side_upper} CSV] written={written} skipped={skipped} "
        f"mode={DIRECT_SIGNAL_CSV_MODE} path={csv_path}",
        flush=True,
    )
    return written


# ===========================================================================
# SCAN ONE SLOT — scan all tickers and return signal dicts
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


def _scan_partition_worker(
    slot_iso: str,
    partition_name: str,
    partition_tickers: List[str],
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    allow_long: bool,
    allow_short: bool,
    use_slot_snapshots: bool,
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
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

            if allow_short:
                s_trades = _scan_short_prepared_live(ticker, df_prepared, short_cfg)
                for trade in s_trades:
                    row = asdict(trade) if not isinstance(trade, dict) else trade
                    row["side"] = "SHORT"
                    part_short_rows.append(row)

            if allow_long:
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
        "mode": "snapshot" if use_slot_snapshots else "raw",
    }
    return meta, part_short_rows, part_long_rows


def _scan_slot(
    slot_ist: datetime,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    allow_long: bool,
    allow_short: bool,
    tickers: Optional[List[str]] = None,
    prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """
    Scan all tickers for today's signals up to (and including) slot_ist.
    Returns (short_signal_dicts, long_signal_dicts).
    """
    tickers = list(tickers or _list_tickers_5m())
    partitions = [part for part in _split_tickers_for_scan_shards(tickers, SCAN_SHARDS) if part]
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
        shard_names = [f"{idx:02d}" for idx, _part in enumerate(partitions, 1)]

    effective_workers = min(max(1, int(SCAN_MAX_WORKERS)), len(shard_names))

    short_rows: List[dict] = []
    long_rows: List[dict] = []
    partition_metas: List[Dict[str, Any]] = []

    print(
        f"[SCAN] Starting sharded scan: tickers={len(tickers)} "
        f"shards={len(shard_names)} workers={effective_workers} "
        f"mode={'snapshot' if USE_SLOT_SNAPSHOTS else 'raw'}",
        flush=True,
    )

    scan_started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=effective_workers,
        mp_context=mp.get_context("spawn"),
    ) as executor:
        future_map = {
            executor.submit(
                _scan_partition_worker,
                slot_ist.isoformat(),
                shard_names[idx - 1],
                partition_tickers,
                short_cfg,
                long_cfg,
                allow_long,
                allow_short,
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
                f"errors={meta['errors']} rows={meta['source_rows']} "
                f"elapsed={meta['elapsed_sec']:.1f}s",
                flush=True,
            )

    scan_elapsed = time.perf_counter() - scan_started
    total_elapsed = float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed
    print(
        f"[SCAN] Done: tickers={len(tickers)} raw_short={len(short_rows)} raw_long={len(long_rows)} "
        f"partitions={len(partition_metas)} scan_elapsed={scan_elapsed:.1f}s total_elapsed={total_elapsed:.1f}s",
        flush=True,
    )
    meta = {
        "mode": "snapshot" if USE_SLOT_SNAPSHOTS else "raw",
        "snapshot_meta": snapshot_meta,
        "scan_elapsed_sec": round(scan_elapsed, 3),
        "total_elapsed_sec": round(total_elapsed, 3),
        "shards": int(len(shard_names)),
        "effective_workers": int(effective_workers),
    }
    return short_rows, long_rows, meta


# ===========================================================================
# APPLY NIFTY RS FILTER TO SIGNAL DICTS
# ===========================================================================
def _apply_rs_filter_dicts(
    short_rows: List[dict],
    long_rows: List[dict],
    allow_long: bool,
    allow_short: bool,
) -> Tuple[List[dict], List[dict]]:
    if not allow_short:
        short_rows = []
    if not allow_long:
        long_rows = []
    return short_rows, long_rows


# ===========================================================================
# APPLY V16 POST-SCAN FILTERS TO SIGNAL DICTS (via DataFrame)
# ===========================================================================
def _apply_v16_filters_to_dicts(
    short_rows: List[dict],
    long_rows: List[dict],
) -> Tuple[List[dict], List[dict]]:
    short_df = pd.DataFrame(short_rows) if short_rows else pd.DataFrame()
    long_df  = pd.DataFrame(long_rows)  if long_rows  else pd.DataFrame()

    short_df, long_df = _apply_v16_post_scan_filters(short_df, long_df)

    short_out = short_df.to_dict("records") if not short_df.empty else []
    long_out  = long_df.to_dict("records")  if not long_df.empty  else []
    return short_out, long_out


def _apply_backtest_parity_filters_to_dicts(
    short_rows: List[dict],
    long_rows: List[dict],
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    short_df = v16_runner._finalize_side_scan_df(pd.DataFrame(short_rows), short_cfg)
    long_df = v16_runner._finalize_side_scan_df(pd.DataFrame(long_rows), long_cfg)

    meta: Dict[str, Any] = {
        "raw_short": int(len(short_df)),
        "raw_long": int(len(long_df)),
    }

    if mode_map:
        short_df, long_df = _apply_live_v17b_hybrid_context(
            short_df,
            long_df,
            short_cfg,
            long_cfg,
            mode_map,
            nifty_ret_map,
        )
    meta["post_context_short"] = int(len(short_df))
    meta["post_context_long"] = int(len(long_df))

    if getattr(v16_runner, "V16_OR_GATE_ENABLED", False):
        short_df, long_df = v16_runner._enrich_with_or_levels(
            short_df,
            long_df,
            dir_15m=DIR_5M,
            parquet_suffix=END_5M,
        )
    if getattr(v16_runner, "V16_LONG_ENTRY_VOL_EXHAUST_ENABLED", False) and not long_df.empty:
        long_df = v16_runner._enrich_with_entry_vol_ratio(
            long_df,
            dir_15m=DIR_5M,
            parquet_suffix=END_5M,
        )

    short_df, long_df = _apply_v16_post_scan_filters(short_df, long_df)
    meta["final_short"] = int(len(short_df))
    meta["final_long"] = int(len(long_df))

    short_out = short_df.to_dict("records") if not short_df.empty else []
    long_out = long_df.to_dict("records") if not long_df.empty else []
    return short_out, long_out, meta


def _blocked_nifty_context_message(slot: datetime, slot_payload: Optional[Dict[str, Any]]) -> str:
    nifty_context = (slot_payload or {}).get("nifty_context") or {}
    reason = str(nifty_context.get("decision_reason", "") or "").strip()
    source = str(nifty_context.get("source", "live_nifty") or "live_nifty")
    day_move_pct = nifty_context.get("day_move_pct")
    rs_pct = nifty_context.get("rs_pct")

    parts = [f"slot={slot.strftime('%H:%M')}", f"source={source}"]
    if reason:
        parts.append(f"reason={reason}")
    try:
        if day_move_pct is not None:
            parts.append(f"day_move={float(day_move_pct):+.2f}%")
    except Exception:
        pass
    try:
        if rs_pct is not None:
            parts.append(f"rs={float(rs_pct):+.2f}%")
    except Exception:
        pass
    return "[SKIP_SLOT] No context. " + " | ".join(parts)


# ===========================================================================
# STATUS / HEARTBEAT HELPERS
# ===========================================================================
def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"] = _SCRIPT_NAME
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    base_v15._touch_runtime_heartbeat(state, **extra)


# ===========================================================================
# MAIN LOOP
# ===========================================================================
def main() -> None:
    if mp.current_process().name != "MainProcess":
        return
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min Live Scanner - Anti-exhaustion filters\n"
        f"  DATA_5M_DIR : {DIR_5M}\n"
        f"  SIGNALS_DIR : {RUNTIME_LIVE_SIGNALS_DIR}\n"
        f"  CSV_MODE    : {DIRECT_SIGNAL_CSV_MODE} "
        "(official executable CSVs are PF/DE-owned unless mode=direct)\n"
        f"  TARGET      : SHORT={TEST_SHORT_TARGET_PCT*100:.2f}%, LONG={TEST_LONG_TARGET_PCT*100:.2f}%\n"
        "  STOP        : SHORT=0.75%, LONG=0.75%\n"
        "  CONTEXT     : backtest intraday NIFTY context + per-stock RS\n"
        "=" * 70,
        flush=True,
    )

    # Set env vars for status file routing
    _touch_status("STARTING")

    short_cfg, long_cfg = _build_v16_cfgs()
    print(
        f"[CONFIG] SHORT: SL={short_cfg.stop_pct*100:.2f}% TGT={short_cfg.target_pct*100:.2f}% "
        f"ADX>={short_cfg.adx_min} volume>={short_cfg.volume_min_ratio}",
        flush=True,
    )
    print(
        f"[CONFIG] LONG:  SL={long_cfg.stop_pct*100:.2f}% TGT={long_cfg.target_pct*100:.2f}% "
        f"ADX>={long_cfg.adx_min} volume>={long_cfg.volume_min_ratio}",
        flush=True,
    )
    if FORCE_SIGNAL_QUANTITY is not None:
        print(f"[CONFIG] Quantity override: FORCE_SIGNAL_QUANTITY={FORCE_SIGNAL_QUANTITY}", flush=True)

    tickers = _list_tickers_5m()
    holidays = base_v15._read_holidays_safe()
    cache_day: Optional[Any] = None
    print(f"[INFO] Tickers in 5-min dir: {len(tickers)}", flush=True)
    print(
        f"[INFO] Slot-ready polling: offset={SLOT_START_OFFSET_SECONDS}s max_wait={SLOT_READY_MAX_WAIT_SECONDS}s "
        f"poll={SLOT_READY_POLL_SECONDS}s sample={SLOT_READY_SAMPLE_SIZE} "
        f"min_fresh_ratio={SLOT_READY_MIN_FRESH_RATIO:.2f} "
        f"ready_marker={USE_SCHEDULER_READY_MARKER}",
        flush=True,
    )
    print(
        f"[INFO] Scan sharding: shards={SCAN_SHARDS} max_workers={SCAN_MAX_WORKERS} tail_rows={TAIL_ROWS}",
        flush=True,
    )
    print(
        f"[INFO] Slot snapshots: enabled={USE_SLOT_SNAPSHOTS} "
        f"snapshot_workers={SNAPSHOT_MAX_WORKERS} rolling_cache={USE_SNAPSHOT_ROLLING_CACHE}",
        flush=True,
    )

    if BENCHMARK_SLOT_RAW:
        slot_ts = pd.Timestamp(BENCHMARK_SLOT_RAW)
        if slot_ts.tzinfo is None:
            slot_ts = slot_ts.tz_localize(IST)
        else:
            slot_ts = slot_ts.tz_convert(IST)
        slot_dt = slot_ts.to_pydatetime()
        prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None
        if USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = build_slot_snapshots(
                slot_dt,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
        context_state = _build_backtest_context_state(slot_dt, short_cfg)
        allow_long = bool(context_state["allow_long"])
        allow_short = bool(context_state["allow_short"])
        started = time.perf_counter()
        short_rows, long_rows, scan_meta = _scan_slot(
            slot_dt,
            short_cfg,
            long_cfg,
            allow_long,
            allow_short,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )
        short_rows, long_rows, filter_meta = _apply_backtest_parity_filters_to_dicts(
            short_rows,
            long_rows,
            short_cfg,
            long_cfg,
            context_state.get("mode_map", {}),
            context_state.get("nifty_ret_map", {}),
        )
        elapsed = time.perf_counter() - started
        summary = {
            "slot": slot_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
            "tickers": len(tickers),
            "allow_long": bool(allow_long),
            "allow_short": bool(allow_short),
            "raw_short": int(filter_meta.get("raw_short", len(short_rows))),
            "raw_long": int(filter_meta.get("raw_long", len(long_rows))),
            "post_context_short": int(filter_meta.get("post_context_short", len(short_rows))),
            "post_context_long": int(filter_meta.get("post_context_long", len(long_rows))),
            "final_short": int(filter_meta.get("final_short", len(short_rows))),
            "final_long": int(filter_meta.get("final_long", len(long_rows))),
            "elapsed_sec": round(elapsed, 3),
            "scan_shards": int(SCAN_SHARDS),
            "scan_max_workers": int(SCAN_MAX_WORKERS),
            "snapshot_max_workers": int(SNAPSHOT_MAX_WORKERS),
            "use_slot_snapshots": bool(USE_SLOT_SNAPSHOTS),
            "tail_rows": int(TAIL_ROWS),
            "scan_elapsed_sec": float(scan_meta.get("scan_elapsed_sec", 0.0)),
            "scan_total_elapsed_sec": float(scan_meta.get("total_elapsed_sec", 0.0)),
            "slot_context_source": "backtest_intraday_context",
        }
        snapshot_meta = scan_meta.get("snapshot_meta") or {}
        if snapshot_meta:
            summary["snapshot_build_elapsed_sec"] = float(snapshot_meta.get("total_elapsed_sec", 0.0))
        print("[BENCHMARK] " + json.dumps(summary, sort_keys=True), flush=True)
        return

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
            _sleep_until_resilient(
                nxt,
                phase="WAIT_NEXT_TRADING_DAY",
                next_wake=base_v15._fmt_ist_dt(nxt),
            )
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_5min_slot(now)
        if slot.date() != now.date():
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(
                nxt,
                phase="WAIT_NEXT_TRADING_DAY",
                next_wake=base_v15._fmt_ist_dt(nxt),
            )
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            _sleep_until_resilient(
                slot,
                phase="WAIT_SLOT",
                slot=slot.strftime("%H:%M"),
                next_wake=slot.strftime("%Y-%m-%d %H:%M:%S%z"),
                poll_seconds=15.0,
            )

        now = _now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(
                nxt,
                phase="WAIT_NEXT_TRADING_DAY",
                next_wake=base_v15._fmt_ist_dt(nxt),
            )
            holidays = base_v15._read_holidays_safe()
            continue

        # ------------------------------------------------------------------
        # Run scan for this slot
        # ------------------------------------------------------------------
        if cache_day != slot.date():
            clear_slot_snapshot_cache()
            cache_day = slot.date()
        slot_start = time.perf_counter()
        signal_day_str = slot.strftime("%Y-%m-%d")
        print(f"\n[SLOT] {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        _touch_status("RUNNING", phase="WAIT_DATA", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="WAIT_DATA", slot=slot.strftime("%H:%M"))
        ready, ratio, waited, checked = _wait_for_slot_data_ready(slot, tickers)
        print(
            f"[WAIT] slot={slot.strftime('%H:%M')} ready={ready} "
            f"fresh_ratio={ratio:.2f} waited={waited:.1f}s checked={checked}",
            flush=True,
        )
        if not ready and SKIP_STALE_SLOT_ON_TIMEOUT:
            _touch_status("RUNNING", phase="SKIP_STALE", slot=slot.strftime("%H:%M"))
            _touch_heartbeat("RUNNING", phase="SKIP_STALE", slot=slot.strftime("%H:%M"))
            print(
                f"[SKIP_SLOT] slot={slot.strftime('%H:%M')} data freshness timed out "
                f"(ratio={ratio:.2f}). Skipping stale scan.",
                flush=True,
            )
            continue
        _touch_status("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))

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
        context_state = _build_backtest_context_state(slot, short_cfg)
        allow_long = bool(context_state["allow_long"])
        allow_short = bool(context_state["allow_short"])

        short_rows, long_rows, scan_meta = _scan_slot(
            slot,
            short_cfg,
            long_cfg,
            allow_long,
            allow_short,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )
        short_rows, long_rows, filter_meta = _apply_backtest_parity_filters_to_dicts(
            short_rows,
            long_rows,
            short_cfg,
            long_cfg,
            context_state.get("mode_map", {}),
            context_state.get("nifty_ret_map", {}),
        )

        short_written = _write_side_signals_csv(short_rows, "SHORT", signal_day_str)
        long_written  = _write_side_signals_csv(long_rows,  "LONG",  signal_day_str)

        elapsed = time.perf_counter() - slot_start
        print(
            f"[SLOT_DONE] slot={slot.strftime('%H:%M')} "
            f"raw_short={int(filter_meta.get('raw_short', 0))} "
            f"raw_long={int(filter_meta.get('raw_long', 0))} "
            f"post_context_short={int(filter_meta.get('post_context_short', 0))} "
            f"post_context_long={int(filter_meta.get('post_context_long', 0))} "
            f"final_short={int(filter_meta.get('final_short', 0))} "
            f"final_long={int(filter_meta.get('final_long', 0))} "
            f"short_written={short_written} long_written={long_written} "
            f"scan_elapsed={float(scan_meta.get('scan_elapsed_sec', 0.0)):.1f}s "
            f"total_elapsed={elapsed:.1f}s",
            flush=True,
        )

        _touch_status("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))

        # Sleep until next slot
        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        now_after = _now_ist()
        if now_after < next_slot:
            time.sleep(2.0)


if __name__ == "__main__":
    mp.freeze_support()
    if mp.current_process().name == "MainProcess":
        main()
