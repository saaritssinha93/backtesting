# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Detection Engine (Stage 2)
=============================================
Reads the pending signal pool written by the Signal Engine, loads fresh 5-min
data from the shared live 5-minute directory (refreshed by the Pending Data Fetcher), applies
ALL V16 filters + price confirmation, and writes confirmed signals to the
standard signals CSV consumed by the executor.

Runs every 60 seconds, offset 30s from the Pending Data Fetcher.

Operational file names stay on the v16 path, but the live detection stack
loads the v17f runner patch bundle first so rescan/filter parity matches v17f.

For each pending signal it does:
  1. Expiry check        — skip if past expires_at window
  2. Load fresh parquet  — from stocks_indicators_5min_eq_pending/
  3. File freshness gate — skip if data file is older than MAX_DATA_AGE_SEC
  4. Price confirmation  — current close still valid vs entry_price
  5. Pattern rescan      — re-detect the original signal in fresh data
  6. RS filter           — NIFTY RS threshold (LONG >= 0.75%, SHORT <= -0.75%)
  7. V16 post-scan filters — QS dead zone, AVWAP dist, vol exhaust, OR gate
  8. Write to signals CSV (same format as existing scanner → executor works as-is)
  9. Update pending JSON with final status + filter_reason

Outputs:
  signals_YYYY-MM-DD_v16_5min_long.csv   — same format as existing scanner
  signals_YYYY-MM-DD_v16_5min_short.csv  — same format as existing scanner
  detected_signals_YYYY-MM-DD_v16_5min.csv — detection log (dashboard)

Status files (to logs/):
  eqidv2_detection_engine_v16_5min.log
  eqidv2_detection_engine_v16_5min.status
  eqidv2_detection_engine_v16_5min.heartbeat

STRATEGY V2 — DESIGN INVARIANTS (see strategy_v2.txt §F)
========================================================
F1. Pool row monotonicity:
    DE never mutates immutable fields (ticker, side, setup, entry_slot,
    entry_price, stop_price). Only outcome-side fields are updated:
    status ∈ {detected, filtered_parity, filtered_trigger_drifted,
    filtered_v16, ...}, filter_reason, detection_time, detection_price.
F2. PF/DE slot equality:
    DE's per-slot scan reads ONLY pool rows whose source_slot equals the
    slot it is confirming. Cross-slot reach is forbidden.
F3. No writes for past entry_slots:
    SE enforces on the write side; DE's freshness gate
    (`_assess_pending_parquet_freshness`) is the reader-side safety net.
F4. One-writer-many-readers:
    DE is the only writer of signals_<date>_v16_5min_{long,short}.csv
    and detected_signals_<date>_v16_5min.csv. The pool JSON / CSV are
    updated for status transitions only — SE remains the canonical
    inserter.
F5. Slot is the OPEN time:
    All slot keys (`signal_datetime`, `signal_entry_datetime_ist`,
    ready-marker filename, `source_slot`) use bar-OPEN time.

DE-specific strategy-v2 hooks:
  A1  Atomic pool CSV write (write-rename, .csv.tmp → os.replace).
  A3 / D2  Trigger-bar OHLC drift check: compare live parquet-derived
       md5(O|H|L|C)[:16] to the `trigger_ohlc_hash` stashed by SE. On
       mismatch, mark `filtered_trigger_drifted` and skip (fail-closed).
  E2  pool_lifecycle_<date>_v16_5min.jsonl audit events:
        DE_PASSED  on every confirmed signal
        DROPPED    with `reason=filter_reason` on every filtered row
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import pytz

# ---------------------------------------------------------------------------
# V16 runner — scanner + filter helpers
# ---------------------------------------------------------------------------
import avwap_combined_runner_v17f_5min as _v17f_runner  # noqa: F401
import avwap_combined_runner_v16_5min as v16_runner
import eqidv2_signal_id_selfcheck_v16_5min as signal_id_selfcheck
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    get_v16_filter_reason,
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
import eqidv2_live_combined_analyser_csv_v16_5min as live_v16
from eqidv2_runtime_paths import (
    DATA_5M_DIR as RUNTIME_DATA_5M_DIR,
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    NIFTY_SLOT_READY_DIR,
    SLOT_READY_PENDING_DIR,
    RUNTIME_STATUS_DIR,
)

# strategy_v2 §C3 — cross-process pool-JSON lock. The DE cycle performs a
# read-modify-write on pending_signals_<date>_v16_5min.json (load state at
# the top of the cycle, mutate rows to status=detected/filtered_*, atomic
# replace at the bottom). Without this lock, SE or PF can interleave a
# write during the cycle body and get silently clobbered.
from eqidv2_pool_lock import POOL_REV_FIELD, pool_lock, bump_pool_rev, load_pool_rev

IST = pytz.timezone("Asia/Kolkata")

# ===========================================================================
# CONSTANTS
# ===========================================================================
SCRIPT_DIR   = Path(__file__).resolve().parent
_LOG_DIR     = SCRIPT_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

_SCRIPT_NAME = "eqidv2_detection_engine_v16_5min.py"
_LOG_FILE    = _LOG_DIR / "eqidv2_detection_engine_v16_5min.log"

PENDING_JSON_PATTERN       = "pending_signals_{}_v16_5min.json"
PENDING_CSV_PATTERN        = "pending_signals_{}_v16_5min.csv"
POOL_LIFECYCLE_PATTERN     = "pool_lifecycle_{}_v16_5min.jsonl"
SHORT_SIGNAL_CSV_PATTERN   = "signals_{}_v16_5min_short.csv"
LONG_SIGNAL_CSV_PATTERN    = "signals_{}_v16_5min_long.csv"
DETECTED_CSV_PATTERN       = "detected_signals_{}_v16_5min.csv"

END_5M                = "_stocks_indicators_5min.parquet"
NIFTYBEES_TICKER      = "NIFTYBEES"

CHECK_INTERVAL_SEC    = int(os.getenv("EQIDV2_DETECTION_CHECK_INTERVAL_SEC", "60"))
STARTUP_OFFSET_SEC    = int(os.getenv("EQIDV2_DETECTION_STARTUP_OFFSET_SEC", "30"))
# Fix #17+ (post-2026-04-21): retry budget for signal_id self-check proof load.
# Avoids the Stage1/Stage2 startup race where Stage 2 boots faster than Stage 1
# writes the proof file. Total wait = TIMEOUT_SEC, polled every SLEEP_SEC.
SELF_CHECK_PROOF_RETRY_TIMEOUT_SEC = int(os.getenv("EQIDV2_DETECTION_SELFCHECK_RETRY_TIMEOUT_SEC", "60"))
SELF_CHECK_PROOF_RETRY_SLEEP_SEC   = max(1, int(os.getenv("EQIDV2_DETECTION_SELFCHECK_RETRY_SLEEP_SEC", "3")))
MARKET_OPEN           = dtime(9, 15)
# Fix #14: align with trade executor FORCED_CLOSE_TIME (15:20). Confirmations
# after 15:20 would be rejected as ENTRY_SKIPPED_AFTER_CUTOFF by the executor,
# so there's no point running detection past that point.
def _parse_hard_stop_hhmm(val: str) -> dtime:
    try:
        hh, mm = val.strip().split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return dtime(15, 20)
HARD_STOP             = _parse_hard_stop_hhmm(
    os.getenv("EQIDV2_DETECTION_HARD_STOP_HHMM", "15:20")
)
MAX_DATA_AGE_SEC      = int(os.getenv("EQIDV2_DETECTION_MAX_DATA_AGE_SEC", "180"))  # 3 min
ALIGN_TO_5MIN_BOUNDARY = str(
    os.getenv("EQIDV2_DETECTION_ALIGN_TO_5MIN", "0")
).strip().lower() not in {"0", "false", "no", "off"}
SLOT_OFFSET_SEC       = int(os.getenv("EQIDV2_DETECTION_SLOT_OFFSET_SEC", "4"))
SIGNAL_ENGINE_SLOT_START_OFFSET_SEC = int(
    os.getenv("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "45")
)
NO_PENDING_RECHECK_AFTER_SEC = int(
    os.getenv(
        "EQIDV2_DETECTION_NO_PENDING_RECHECK_AFTER_SEC",
        str(max(SLOT_OFFSET_SEC, SIGNAL_ENGINE_SLOT_START_OFFSET_SEC + 10)),
    )
)

# Nifty context neutralization — must match Signal Engine live-mode behavior
NEUTRALIZE_WEAK_NIFTY_CONTEXT = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_WEAK_NIFTY_CONTEXT", "1")
).strip().lower() not in {"0", "false", "no", "off"}
DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = str(
    os.getenv("EQIDV16_5MIN_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK", "1")
).strip().lower() not in {"0", "false", "no", "off"}
NEUTRALIZE_PARTIAL_NIFTY_SESSION = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_PARTIAL_NIFTY_SESSION", "1")
).strip().lower() not in {"0", "false", "no", "off"}
# A4 fix: if NIFTYBEES parquet's last bar is older than this many seconds vs
# slot_ist, demote RS to neutral instead of silently using stale direction.
# Same default as Signal Engine (420s = one 5-min bar + 2-min slack).
NIFTY_MAX_STALE_SEC = float(os.getenv("EQIDV2_NIFTY_MAX_STALE_SEC", "420"))
# strategy_v2 C1 — NF slot-ready gate. DE refuses to run detection on a slot
# until NF has written nifty_ready_<slot>.json. On timeout we log
# [ABORT] NF_STALE and emit an NF_STALE lifecycle event, so a silent NF
# outage cannot drive RS off stale NIFTYBEES data.
NF_READY_REQUIRE = str(
    os.getenv("EQIDV2_DETECTION_NF_READY_REQUIRE", "1")
).strip().lower() not in {"0", "false", "no", "off"}
NF_READY_TIMEOUT_SEC = float(os.getenv("EQIDV2_DETECTION_NF_READY_TIMEOUT_SEC", "90"))
NF_READY_POLL_SEC = max(0.2, float(os.getenv("EQIDV2_DETECTION_NF_READY_POLL_SEC", "1.0")))
USE_READY_MARKER_HANDOFF = str(
    os.getenv("EQIDV2_DETECTION_USE_READY_MARKERS", "1")
).strip().lower() not in {"0", "false", "no", "off"}
READY_MARKER_MAX_AGE_SEC = int(
    os.getenv(
        "EQIDV2_DETECTION_READY_MARKER_MAX_AGE_SEC",
        str(max(MAX_DATA_AGE_SEC, CHECK_INTERVAL_SEC + STARTUP_OFFSET_SEC + 30)),
    )
)
DETECTION_PARITY_MODE = str(
    os.getenv("EQIDV2_DETECTION_PARITY_MODE", "1")
).strip().lower() not in {"0", "false", "no", "off"}
PRICE_CONFIRM_LONG_TOLERANCE   = float(os.getenv("EQIDV2_DETECTION_LONG_PRICE_TOL", "0.005"))   # -0.5%
PRICE_CONFIRM_SHORT_TOLERANCE  = float(os.getenv("EQIDV2_DETECTION_SHORT_PRICE_TOL", "0.005"))  # +0.5%
INTRADAY_LEVERAGE         = 5.0
DEFAULT_POSITION_SIZE_RS  = float(os.getenv("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000"))
LIVE_MIN_BARS_FOR_SCAN    = max(4, int(os.getenv("EQIDV16_5MIN_LIVE_MIN_BARS_FOR_SCAN", "4")))

_SIGNAL_START_TIME = dtime(9, 15)
_LAST_NO_PENDING_LOG_KEY_PARITY: Optional[str] = None
_LAST_NO_PENDING_LOG_KEY_LEGACY: Optional[str] = None

PENDING_CSV_COLUMNS = [
    "signal_id", "ticker", "side", "signal_datetime", "signal_entry_datetime_ist",
    "signal_bar_time", "added_at",
    "signal_price", "entry_price", "stop_price", "target_price",
    "quality_score", "avwap_dist_atr", "rsi_signal", "adx",
    "rs_pct", "setup", "status", "expires_at",
    "filter_reason", "detection_time", "detection_price",
    # strategy_v2 §C1 / §A3 — trigger-bar snapshot written by SE
    "trigger_bar_iso", "trigger_open", "trigger_high",
    "trigger_low", "trigger_close", "trigger_volume",
    "trigger_ohlc_hash",
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


def _start_tee() -> None:
    try:
        for name in ("stdout", "stderr", "__stdout__", "__stderr__"):
            stream = getattr(sys, name, None)
            if stream and hasattr(stream, "reconfigure"):
                try:
                    stream.reconfigure(encoding="utf-8", errors="replace")
                except Exception:
                    pass
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
def _now_ist() -> datetime:
    return datetime.now(IST)


def _floor_to_5m(dt: datetime) -> datetime:
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"]    = str(RUNTIME_STATUS_DIR / "eqidv2_detection_engine_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_detection_engine_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"]    = _SCRIPT_NAME
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    base_v15._touch_runtime_heartbeat(state, **extra)


# ===========================================================================
# CONFIG BUILD
# ===========================================================================
def _build_v16_cfgs() -> Tuple[StrategyConfig, StrategyConfig]:
    v16_runner.NIFTY_CONTEXT_OR_END_TIME  = dtime(9, 20)
    v16_runner.NIFTY_CONTEXT_CONFIRM_TIME = dtime(9, 20)

    short_builder = getattr(v16_runner, "default_short_config", None)
    short_cfg = short_builder() if callable(short_builder) else default_short_config()
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    long_cfg  = long_builder() if callable(long_builder) else default_long_config_v11()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.stop_pct   = 0.0075
    long_cfg.stop_pct    = 0.0075
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct  = float(TEST_LONG_TARGET_PCT)
    # Use the shared live 5-minute directory and restrict by pending-pool tickers.
    short_cfg.dir_15m = str(RUNTIME_DATA_5M_DIR)
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m  = str(RUNTIME_DATA_5M_DIR)
    long_cfg.end_15m  = END_5M
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


# ===========================================================================
# DATA HELPERS
# ===========================================================================
def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        import math
        v = float(val)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _load_pending_parquet(ticker: str, tail: int = 260) -> Optional[pd.DataFrame]:
    """Load parquet from the shared live 5-minute directory."""
    path = RUNTIME_DATA_5M_DIR / f"{ticker.upper().strip()}{END_5M}"
    if not path.exists():
        return None
    df = base_v15.read_parquet_tail(str(path), n=tail)
    if df is None or df.empty:
        return None
    dt = pd.to_datetime(df.get("date", pd.Series(dtype=str)), errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)
    df = df.copy()
    df["date"] = dt
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def _pending_parquet_age_sec(ticker: str) -> float:
    """Returns age in seconds of the live parquet file for a pending ticker."""
    path = RUNTIME_DATA_5M_DIR / f"{ticker.upper().strip()}{END_5M}"
    try:
        return time.time() - path.stat().st_mtime
    except OSError:
        return float("inf")


def _format_ist_ts(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return "None"
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.strftime("%Y-%m-%d %H:%M:%S%z")


def _pending_parquet_last_row_ts(df: Optional[pd.DataFrame]) -> Optional[pd.Timestamp]:
    if df is None or df.empty or "date" not in df.columns:
        return None
    try:
        last_raw = df.iloc[-1].get("date")
    except Exception:
        return None
    last_ts = pd.to_datetime(last_raw, errors="coerce")
    if pd.isna(last_ts):
        return None
    if last_ts.tzinfo is None:
        return last_ts.tz_localize(IST)
    return last_ts.tz_convert(IST)


def _assess_pending_parquet_freshness(
    ticker: str,
    required_slot: Optional[pd.Timestamp] = None,
    df: Optional[pd.DataFrame] = None,
) -> Tuple[Optional[pd.DataFrame], float, Optional[pd.Timestamp], Optional[str]]:
    """
    Validate both recency and candle completeness for a live pending parquet.

    A freshly written file is not considered usable unless its last candle
    reaches the pending signal's source slot or slot group. mtime remains a
    secondary guard for stale files that happen to include the right slot.
    """
    if df is None:
        df = _load_pending_parquet(ticker)
    if df is None or df.empty:
        return None, float("inf"), None, "no_parquet_in_pending_dir"

    last_row_ts = _pending_parquet_last_row_ts(df)
    age_sec = _pending_parquet_age_sec(ticker)

    slot_matched = False
    if required_slot is not None:
        req_ts = pd.Timestamp(required_slot)
        if req_ts.tzinfo is None:
            req_ts = req_ts.tz_localize(IST)
        else:
            req_ts = req_ts.tz_convert(IST)
        req_ts = req_ts.floor("5min")
        if last_row_ts is None or last_row_ts < req_ts:
            return (
                df,
                age_sec,
                last_row_ts,
                f"data_incomplete (last_row={_format_ist_ts(last_row_ts)} < required_slot={_format_ist_ts(req_ts)})",
            )
        slot_matched = True

    # Fix #13: use max(mtime, last_row_ts) as the freshness clock.
    # On Windows + OneDrive, os.replace/sync can leave a just-written file with
    # a briefly-stale mtime even though its last candle is current. Trust the
    # more-recent of the two timestamps to avoid false-rejecting fresh data.
    effective_age_sec = age_sec
    if last_row_ts is not None:
        try:
            now_ist = pd.Timestamp.now(tz=IST)
            last_row_age = max(0.0, (now_ist - last_row_ts).total_seconds())
            effective_age_sec = min(age_sec, last_row_age)
        except Exception:
            pass

    # Fix #19 (post-2026-04-21): when the parquet contains the exact required
    # source slot, that is a stronger guarantee than wall-clock mtime age. On
    # 5-min bars at SLOT_OFFSET_SEC=4, mtime_age naturally runs ~270s which
    # previously tripped MAX_DATA_AGE_SEC=180 and blocked every promotion.
    if not slot_matched and effective_age_sec > MAX_DATA_AGE_SEC:
        return (
            df,
            age_sec,
            last_row_ts,
            f"data_stale ({effective_age_sec:.0f}s > {MAX_DATA_AGE_SEC}s; "
            f"mtime_age={age_sec:.0f}s; last_row={_format_ist_ts(last_row_ts)})",
        )

    return df, effective_age_sec, last_row_ts, None


def _marker_source_slot_key(marker_ts: Any) -> Optional[str]:
    """Normalize a marker's slot timestamp to a floor-to-5min IST ISO key.

    Used to compare a marker's source slot against the pending pool's expected
    source slots. This is the primary acceptance gate for ready markers — when
    the source slot matches what the detector is waiting for, wall-clock age
    is irrelevant (the data is exactly what's needed).
    """
    if marker_ts is None:
        return None
    try:
        ts = pd.Timestamp(marker_ts)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.floor("5min").isoformat()


def _load_latest_ready_marker(
    now_ist: datetime,
    allowed_slot_keys: Optional[Set[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Return the newest ready marker for today's session.

    Acceptance rules:
      - If ``allowed_slot_keys`` is provided, a marker is accepted when its
        source slot matches one of the keys. Wall-clock age is NOT applied —
        matching the expected source slot is the stronger guarantee.
      - If no slot filter is supplied, the freshest marker within
        ``READY_MARKER_MAX_AGE_SEC`` wins (legacy behavior).

    In both modes candidates are scanned newest-first and bounded to today's
    session via a date check so stale markers from prior days never leak in.
    """
    if not USE_READY_MARKER_HANDOFF:
        return None

    try:
        candidates = sorted(
            SLOT_READY_PENDING_DIR.glob("*.ready"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return None

    if not candidates:
        return None

    now_ts = pd.Timestamp(now_ist)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize(IST)
    else:
        now_ts = now_ts.tz_convert(IST)

    for path in candidates[:32]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        marker_ts = base_v15._parse_ist_timestamp(payload.get("slot", ""))
        if marker_ts is None:
            try:
                marker_ts = pd.Timestamp(path.stat().st_mtime, unit="s", tz=IST)
            except Exception:
                marker_ts = None
        if marker_ts is None:
            continue
        if getattr(marker_ts, "tzinfo", None) is None:
            marker_ts = marker_ts.tz_localize(IST)
        else:
            marker_ts = marker_ts.tz_convert(IST)
        if marker_ts.date() != now_ts.date():
            continue

        age_sec = max(0.0, float((now_ts - marker_ts).total_seconds()))

        if allowed_slot_keys:
            marker_slot_key = _marker_source_slot_key(marker_ts)
            if marker_slot_key is None or marker_slot_key not in allowed_slot_keys:
                continue
        else:
            if age_sec > float(READY_MARKER_MAX_AGE_SEC):
                continue

        ready_tickers = {
            str(t).upper().strip()
            for t in (payload.get("tickers", []) or [])
            if str(t).strip()
        }
        payload["_marker_path"] = str(path)
        payload["_marker_age_sec"] = round(age_sec, 1)
        payload["_ready_tickers"] = ready_tickers
        payload["_marker_ts"] = str(marker_ts)
        return payload

    return None


def _load_ready_marker_for_slot(slot_ts: pd.Timestamp, now_ist: datetime) -> Optional[Dict[str, Any]]:
    """Load the ready marker for a specific slot by exact filename lookup.

    Acceptance is by exact source-slot match (the filename is derived from the
    slot boundary by the fetcher). Wall-clock age is not enforced because a
    slot-match already guarantees the marker describes the candle the
    detector is waiting for.
    """
    if not USE_READY_MARKER_HANDOFF:
        return None
    slot_local = slot_ts.tz_convert(IST)
    marker_path = SLOT_READY_PENDING_DIR / f"{slot_local.strftime('%Y%m%d_%H%M')}.ready"
    if not marker_path.exists():
        return None
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    now_ts = pd.Timestamp(now_ist)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize(IST)
    else:
        now_ts = now_ts.tz_convert(IST)
    age_sec = max(0.0, float((now_ts - slot_local).total_seconds()))
    if slot_local.date() != now_ts.date():
        return None
    ready_tickers = {
        str(t).upper().strip()
        for t in (payload.get("tickers", []) or [])
        if str(t).strip()
    }
    payload["_marker_path"] = str(marker_path)
    payload["_marker_age_sec"] = round(age_sec, 1)
    payload["_ready_tickers"] = ready_tickers
    return payload


def _nf_ready_marker_path(slot_ist: datetime) -> Path:
    """strategy_v2 C1 — path for the NF slot-ready marker for `slot_ist`."""
    slot_local = slot_ist if slot_ist.tzinfo else IST.localize(slot_ist)
    slot_key = slot_local.astimezone(IST).strftime("%Y%m%d_%H%M")
    return Path(NIFTY_SLOT_READY_DIR) / f"nifty_ready_{slot_key}.json"


def _wait_for_nifty_slot_ready(slot_ist: datetime) -> Tuple[bool, float, str]:
    """
    strategy_v2 C1 — block up to NF_READY_TIMEOUT_SEC waiting for the NF
    slot-ready marker matching `slot_ist`. Returns (ready, waited_sec,
    marker_path). If NF_READY_REQUIRE is false the gate is a no-op and we
    return (True, 0.0, marker_path) to preserve legacy behaviour.
    """
    marker_path = _nf_ready_marker_path(slot_ist)
    if not NF_READY_REQUIRE:
        return True, 0.0, str(marker_path)
    start = time.monotonic()
    deadline = start + max(0.0, float(NF_READY_TIMEOUT_SEC))
    while True:
        if marker_path.exists():
            return True, time.monotonic() - start, str(marker_path)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, time.monotonic() - start, str(marker_path)
        time.sleep(min(NF_READY_POLL_SEC, max(0.1, remaining)))


def _load_niftybees_rs(slot_ist: datetime) -> Tuple[float, bool, bool]:
    """
    Compute NIFTYBEES RS at the given slot from live data directory.

    Returns (rs_pct, allow_long, allow_short) applying the same neutralization
    logic as the Signal Engine's _resolve_nifty_context_flags():
      - Partial session (data doesn't start at open) → neutral (allow both)
      - Weak daymove (abs < NIFTY_CONTEXT_MIN_DAYMOVE_PCT) → neutral if NEUTRALIZE_WEAK_NIFTY_CONTEXT
      - rs_pct meets ±threshold → directional allow
      - Fallback: use daymove direction if DIRECTIONAL_NIFTY_CONTEXT_FALLBACK
      - Error or insufficient data → neutral (allow both, fail safe)
    """
    try:
        nifty_path = RUNTIME_DATA_5M_DIR / f"{NIFTYBEES_TICKER}{END_5M}"
        df = base_v15.read_parquet_tail(str(nifty_path), n=260)
        if df is None or df.empty:
            return 0.0, True, True  # no data → neutral

        if NIFTY_MAX_STALE_SEC > 0:
            is_fresh, age_sec, last_ts = base_v15.check_niftybees_freshness(
                df, slot_ist, max_stale_sec=NIFTY_MAX_STALE_SEC
            )
            if not is_fresh:
                print(
                    f"[DETECTION_NIFTY_RS] STALE slot={slot_ist.strftime('%H:%M')} "
                    f"last_bar={last_ts} age={age_sec:.0f}s > {NIFTY_MAX_STALE_SEC:.0f}s "
                    f"-> neutral (allow_long=allow_short=True)",
                    flush=True,
                )
                return 0.0, True, True

        dt = pd.to_datetime(df["date"], errors="coerce")
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        dt = dt.dt.tz_convert(IST)
        df["_dt"] = dt
        today = slot_ist.date()
        df_today = df[df["_dt"].dt.date == today].copy()
        df_today = df_today[df_today["_dt"] <= slot_ist].sort_values("_dt")

        if len(df_today) < 2:
            return 0.0, True, True  # not enough bars → neutral

        # Day move (open-to-current)
        day_open  = float(df_today["open"].iloc[0])
        day_close = float(df_today["close"].iloc[-1])
        day_move_pct = (day_close - day_open) / day_open * 100.0 if day_open > 0 else 0.0

        # Session completeness — first bar must be at or before 09:15 open
        first_bar_time = pd.Timestamp(df_today["_dt"].iloc[0]).floor("min")
        session_open   = pd.Timestamp(
            IST.localize(datetime.combine(today, _SIGNAL_START_TIME))
        ).floor("min")
        session_complete = bool(first_bar_time <= session_open)

        # Partial session → neutral
        if not session_complete and NEUTRALIZE_PARTIAL_NIFTY_SESSION:
            rs_pct = day_move_pct  # best we can do with incomplete session
            return rs_pct, True, True

        # Compute lookback RS
        lookback = int(NIFTY_RS_LOOKBACK_BARS)
        if len(df_today) <= lookback:
            rs_pct = day_move_pct
        else:
            close_now  = float(df_today["close"].iloc[-1])
            close_past = float(df_today["close"].iloc[-(lookback + 1)])
            rs_pct = (close_now - close_past) / close_past * 100.0 if close_past > 0 else 0.0

        # Weak daymove → neutral (same as NEUTRALIZE_WEAK_NIFTY_CONTEXT in Signal Engine)
        if abs(day_move_pct) < float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT):
            if NEUTRALIZE_WEAK_NIFTY_CONTEXT:
                return rs_pct, True, True
            # If neutralization is off, block both (strict mode)
            return rs_pct, False, False

        # Threshold gate
        allow_long  = rs_pct >= float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
        allow_short = rs_pct <= -float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)

        # Directional fallback when neither threshold met
        if not allow_long and not allow_short and DIRECTIONAL_NIFTY_CONTEXT_FALLBACK:
            if day_move_pct > 0:
                allow_long = True
            elif day_move_pct < 0:
                allow_short = True

        return rs_pct, allow_long, allow_short

    except Exception:
        return 0.0, True, True  # error → fail safe (neutral)


# ===========================================================================
# PENDING STATE I/O
# ===========================================================================
def _pending_json_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / PENDING_JSON_PATTERN.format(date_str)


def _pending_csv_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / PENDING_CSV_PATTERN.format(date_str)


# ---------------------------------------------------------------------------
# E2 — pool lifecycle JSONL (append-only audit, see strategy_v2.txt §E2)
# ---------------------------------------------------------------------------
def _pool_lifecycle_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / POOL_LIFECYCLE_PATTERN.format(date_str)


def _append_pool_lifecycle_event(
    date_str: str,
    event: Dict[str, Any],
    *,
    pool_rev: Optional[int] = None,
) -> None:
    """Append a single JSONL event to the daily pool_lifecycle audit file.

    Event shape: {"signal_id": ..., "event": "WRITTEN|PF_VERIFIED|DE_PASSED|DROPPED",
                  "ts": "...", "pool_rev": N, <extra keys>}.

    strategy_v2 §J3 fix #7 — when ``pool_rev`` is provided, the event is
    stamped with the TARGET rev the cycle's state write will commit. This
    lets reconciliation tooling detect dangling events (audit saw a
    mutation, but the matching state write never landed because the
    process crashed between them). ``pool_rev=None`` events are observed
    "pool-neutral" (e.g. NF_STALE) and do not imply a state write.
    """
    try:
        path = _pool_lifecycle_path(date_str)
        payload = dict(event)
        payload.setdefault("ts", _now_ist().strftime("%Y-%m-%dT%H:%M:%S%z"))
        if pool_rev is not None:
            payload.setdefault(POOL_REV_FIELD, int(pool_rev))
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as exc:
        print(f"[WARN] pool_lifecycle append failed: {exc}", flush=True)


# ---------------------------------------------------------------------------
# A3 / D2 — trigger-bar OHLC drift check (see strategy_v2.txt §A3 / §D2)
# ---------------------------------------------------------------------------
def _compute_live_trigger_ohlc_hash(
    ticker: str,
    trigger_bar_iso: str,
) -> Optional[str]:
    """Re-read the trigger bar from the live parquet and hash its OHLC.

    Uses the same md5(O|H|L|C)[:16] scheme SE uses to stash
    `trigger_ohlc_hash` on the pool row. Returns None if the trigger bar
    cannot be located or parsed — caller should treat None as
    'cannot verify' (fail-closed per §D2).
    """
    ticker_u = str(ticker or "").strip().upper()
    if not ticker_u or not trigger_bar_iso:
        return None
    path = Path(RUNTIME_DATA_5M_DIR) / f"{ticker_u}{END_5M}"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    except Exception:
        return None
    if df.empty:
        return None
    try:
        target = pd.Timestamp(trigger_bar_iso)
        if target.tzinfo is None:
            target = target.tz_localize(IST)
        else:
            target = target.tz_convert(IST)
        target = target.floor("5min")
        dates = pd.to_datetime(df["date"])
        if getattr(dates.dt, "tz", None) is None:
            dates = dates.dt.tz_localize(IST)
        else:
            dates = dates.dt.tz_convert(IST)
        mask = dates == target
        if not bool(mask.any()):
            return None
        row = df[mask].iloc[-1]
        raw = (
            f"{float(row['open']):.4f}|{float(row['high']):.4f}|"
            f"{float(row['low']):.4f}|{float(row['close']):.4f}"
        )
        return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]
    except Exception:
        return None


def _check_trigger_bar_drift(sig: Dict[str, Any]) -> Optional[str]:
    """Return the live hash ONLY if it disagrees with the stashed SE hash
    (drift case); returns None when the check passes or cannot be performed.

    Callers should treat a non-None return as TRIGGER_BAR_DRIFTED per §D2
    and skip the entry fail-closed.
    """
    stashed = str(sig.get("trigger_ohlc_hash", "") or "").strip().lower()
    if not stashed:
        return None  # no hash written — nothing to compare against
    trigger_iso = str(sig.get("trigger_bar_iso", "") or "").strip()
    if not trigger_iso:
        return None
    live_hash = _compute_live_trigger_ohlc_hash(str(sig.get("ticker", "")), trigger_iso)
    if live_hash is None:
        return None
    if live_hash.lower() == stashed:
        return None
    return live_hash


def _load_pending_state(date_str: str) -> Dict[str, Any]:
    path = _pending_json_path(date_str)
    if not path.exists():
        return {"date": date_str, "last_updated": "", "signals": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"date": date_str, "last_updated": "", "signals": []}


def _write_pending_state_atomic(state: Dict[str, Any], date_str: str) -> None:
    path = _pending_json_path(date_str)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)


def _write_pending_csv(state: Dict[str, Any], date_str: str) -> None:
    # A1 (strategy_v2 §A1): atomic write-rename. Readers (executor / triage
    # tooling) never see a partial CSV because os.replace is atomic on the
    # same volume.
    path = _pending_csv_path(date_str)
    tmp_path = path.with_suffix(".csv.tmp")
    signals = state.get("signals", [])
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PENDING_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for sig in signals:
            writer.writerow({col: sig.get(col, "") for col in PENDING_CSV_COLUMNS})
    os.replace(tmp_path, path)


def _sync_pending_csv_if_needed(state: Dict[str, Any], date_str: str) -> None:
    csv_path = _pending_csv_path(date_str)
    json_path = _pending_json_path(date_str)
    try:
        if csv_path.exists() and json_path.exists() and csv_path.stat().st_mtime >= json_path.stat().st_mtime:
            return
    except Exception:
        pass
    _write_pending_csv(state, date_str)


# ===========================================================================
# SIGNAL CSV WRITING (same format as existing scanner → executor works as-is)
# ===========================================================================
SIGNAL_CSV_COLUMNS = base_v15.SIGNAL_CSV_COLUMNS


def _signal_csv_path(date_str: str, side: str) -> Path:
    pattern = SHORT_SIGNAL_CSV_PATTERN if side.upper() == "SHORT" else LONG_SIGNAL_CSV_PATTERN
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / pattern.format(date_str)


def _write_confirmed_signal(
    sig: Dict[str, Any],
    date_str: str,
    detected_at_str: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    qty: int,
) -> bool:
    """
    Append a confirmed signal to the standard side CSV.
    Returns True if written (not a duplicate), False if skipped.
    """
    ticker = str(sig.get("ticker", "")).upper().strip()
    side   = str(sig.get("side", "")).upper()
    setup  = str(sig.get("setup", ""))
    signal_time_raw = str(sig.get("signal_datetime", ""))
    entry_time_raw = str(sig.get("signal_entry_datetime_ist", signal_time_raw))
    signal_time_ts = base_v15._parse_ist_timestamp(signal_time_raw)
    entry_time_ts  = base_v15._parse_ist_timestamp(entry_time_raw)
    if not ticker or entry_time_ts is None:
        return False

    signal_time = str(signal_time_ts or entry_time_ts)
    entry_time = str(entry_time_ts)
    signal_id  = str(sig.get("signal_id", "")).strip() or base_v15._generate_signal_id(
        ticker, side, entry_time, setup
    )
    received_time = detected_at_str
    surfaced_at_ts = base_v15._parse_ist_timestamp(str(sig.get("added_at", "")).strip())
    if surfaced_at_ts is not None:
        received_time = surfaced_at_ts.strftime("%Y-%m-%d %H:%M:%S%z")
    sig["signal_id"] = signal_id
    csv_path   = _signal_csv_path(date_str, side)

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids  = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        file_exists   = csv_path.exists() and csv_path.stat().st_size > 0

        if signal_id in existing_ids:
            return False
        dedupe_key = base_v15._signal_dedupe_key(ticker, side, entry_time, setup)
        if dedupe_key in existing_keys:
            return False

        row = {
            "signal_id":                  signal_id,
            "signal_datetime":            signal_time,
            "signal_price":               round(_safe_float(sig.get("signal_price", entry_price)), 2),
            "received_time":              received_time,
            "detected_time_ist":          detected_at_str,
            "logtime_ist":                detected_at_str,
            "ticker":                     ticker,
            "side":                       side,
            "setup":                      setup,
            "impulse_type":               str(sig.get("impulse_type", "")),
            "entry_price":                round(entry_price, 2),
            "stop_price":                 round(stop_price, 2),
            "target_price":               round(target_price, 2),
            "quality_score":              round(_safe_float(sig.get("quality_score", 0.0)), 4),
            "atr_pct":                    round(_safe_float(sig.get("atr_pct", 0.0)), 6),
            "rsi":                        round(_safe_float(sig.get("rsi_signal", sig.get("rsi", 0.0))), 2),
            "adx":                        round(_safe_float(sig.get("adx", sig.get("adx_signal", 0.0))), 2),
            "quantity":                   qty,
            "signal_entry_datetime_ist":  entry_time,
            "signal_bar_time_ist":        str(sig.get("signal_bar_time", signal_time)),
            "stage2_detected_at_ist":     detected_at_str,
        }

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SIGNAL_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    return True


DETECTED_CSV_COLUMNS = [
    "signal_id", "ticker", "side", "setup", "signal_datetime", "signal_price",
    "signal_entry_datetime_ist",
    "detected_time", "detection_price",
    "entry_price", "stop_price", "target_price",
    "quality_score", "rsi", "adx", "quantity",
    "lag_from_signal_sec",
]


def _ensure_detected_csv_exists(date_str: str) -> None:
    """
    Create today's detected-signals CSV with just the header if it doesn't exist yet.

    This keeps dashboard/latest-file views current even on zero-signal days where the
    engine exits early with no pending rows before _write_detected_csv() would run.
    """
    path = Path(RUNTIME_LIVE_SIGNALS_DIR) / DETECTED_CSV_PATTERN.format(date_str)
    if path.exists():
        return
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DETECTED_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
    except OSError:
        pass


def _write_detected_csv(detected_signals: List[Dict[str, Any]], date_str: str) -> None:
    """Write/overwrite the detected signals CSV for dashboard display."""
    path = Path(RUNTIME_LIVE_SIGNALS_DIR) / DETECTED_CSV_PATTERN.format(date_str)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DETECTED_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for sig in detected_signals:
            # Compute lag from signal to detection
            lag_sec = ""
            try:
                sig_dt   = pd.to_datetime(sig.get("signal_datetime", ""), utc=False)
                det_dt   = pd.to_datetime(sig.get("detection_time", ""), utc=False)
                if sig_dt is not None and det_dt is not None and not pd.isna(sig_dt) and not pd.isna(det_dt):
                    lag_sec = round((det_dt - sig_dt).total_seconds())
            except Exception:
                pass
            writer.writerow({
                "signal_id":        sig.get("signal_id", ""),
                "ticker":           sig.get("ticker", ""),
                "side":             sig.get("side", ""),
                "setup":            sig.get("setup", ""),
                "signal_datetime":  sig.get("signal_datetime", ""),
                "signal_price":     sig.get("signal_price", ""),
                "signal_entry_datetime_ist": sig.get("signal_entry_datetime_ist", ""),
                "detected_time":    sig.get("detection_time", ""),
                "detection_price":  sig.get("detection_price", ""),
                "entry_price":      sig.get("entry_price", ""),
                "stop_price":       sig.get("stop_price", ""),
                "target_price":     sig.get("target_price", ""),
                "quality_score":    sig.get("quality_score", ""),
                "rsi":              sig.get("rsi_signal", ""),
                "adx":              sig.get("adx", ""),
                "quantity":         sig.get("quantity", ""),
                "lag_from_signal_sec": lag_sec,
            })


# ===========================================================================
# LIVE-PARITY HELPERS
# ===========================================================================
def _normalize_ist_timestamp(value: Any) -> Optional[pd.Timestamp]:
    ts = base_v15._parse_ist_timestamp(value)
    if ts is None:
        return None
    return pd.Timestamp(ts).tz_convert(IST) if pd.Timestamp(ts).tzinfo is not None else pd.Timestamp(ts).tz_localize(IST)


def _pending_signal_source_slot(sig: Dict[str, Any]) -> Optional[pd.Timestamp]:
    source_slot = _normalize_ist_timestamp(sig.get("source_slot", ""))
    if source_slot is not None:
        return source_slot.floor("5min")

    added_at = _normalize_ist_timestamp(sig.get("added_at", ""))
    if added_at is not None:
        return added_at.floor("5min")

    for key in ("signal_entry_datetime_ist", "signal_bar_time", "signal_datetime"):
        ts = _normalize_ist_timestamp(sig.get(key, ""))
        if ts is not None:
            return ts.floor("5min")
    return None


def _signal_id_from_rowlike(row_like: Dict[str, Any], side_override: Optional[str] = None) -> Optional[str]:
    ticker = str(row_like.get("ticker", "")).upper().strip()
    side = str(side_override or row_like.get("side", "")).upper().strip()
    setup = str(row_like.get("setup", ""))
    signal_time_raw = row_like.get(
        "signal_time_ist",
        row_like.get(
            "signal_bar_time_ist",
            row_like.get("signal_bar_time", row_like.get("signal_datetime", "")),
        ),
    )
    entry_time_raw = row_like.get(
        "entry_time_ist",
        row_like.get("signal_entry_datetime_ist", signal_time_raw),
    )
    entry_time_ts = base_v15._parse_ist_timestamp(str(entry_time_raw))
    if not ticker or not side or entry_time_ts is None:
        return None
    return base_v15._generate_signal_id(ticker, side, str(entry_time_ts), setup)


def _update_pending_signal_from_parity_row(
    sig: Dict[str, Any],
    row: Dict[str, Any],
    detected_at_str: str,
) -> None:
    entry_price = _safe_float(row.get("entry_price", sig.get("entry_price", 0.0))) or _safe_float(sig.get("entry_price", 0.0))
    signal_price = _safe_float(row.get("signal_price", entry_price)) or entry_price
    stop_price = (
        _safe_float(row.get("stop_price", row.get("sl_price", sig.get("stop_price", 0.0))))
        or _safe_float(sig.get("stop_price", 0.0))
    )
    target_price = _safe_float(row.get("target_price", sig.get("target_price", 0.0))) or _safe_float(sig.get("target_price", 0.0))
    quantity_raw = row.get("quantity", sig.get("quantity", 0))
    try:
        quantity = max(1, int(quantity_raw))
    except Exception:
        notional = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
        quantity = max(1, int(notional / entry_price)) if entry_price > 0 else 1

    signal_time_raw = row.get(
        "signal_time_ist",
        row.get("signal_bar_time_ist", row.get("signal_datetime", sig.get("signal_datetime", ""))),
    )
    signal_time_ts = base_v15._parse_ist_timestamp(str(signal_time_raw))
    entry_time_raw = row.get("entry_time_ist", row.get("signal_entry_datetime_ist", sig.get("signal_entry_datetime_ist", signal_time_raw)))
    entry_time_ts = base_v15._parse_ist_timestamp(str(entry_time_raw))
    signal_id = _signal_id_from_rowlike(row) or str(sig.get("signal_id", "")).strip()

    sig["signal_id"] = signal_id
    sig["signal_datetime"] = str(signal_time_ts or entry_time_ts or sig.get("signal_datetime", ""))
    sig["signal_entry_datetime_ist"] = str(entry_time_ts or sig.get("signal_entry_datetime_ist", ""))
    sig["signal_bar_time"] = str(signal_time_ts or entry_time_ts or sig.get("signal_bar_time", ""))
    sig["signal_price"] = round(signal_price, 2)
    sig["entry_price"] = round(entry_price, 2)
    sig["stop_price"] = round(stop_price, 2)
    sig["target_price"] = round(target_price, 2)
    sig["quality_score"] = round(_safe_float(row.get("quality_score", sig.get("quality_score", 0.0))), 4)
    sig["avwap_dist_atr"] = round(_safe_float(row.get("avwap_dist_atr_signal", sig.get("avwap_dist_atr", 0.0))), 4)
    sig["rsi_signal"] = round(_safe_float(row.get("rsi_signal", sig.get("rsi_signal", 0.0))), 2)
    sig["adx"] = round(_safe_float(row.get("adx_signal", row.get("adx", sig.get("adx", 0.0)))), 2)
    sig["impulse_type"] = str(row.get("impulse_type", sig.get("impulse_type", "")))
    sig["quantity"] = quantity
    sig["status"] = "detected"
    sig["detection_time"] = detected_at_str
    sig["detection_price"] = round(signal_price, 2)
    sig["filter_reason"] = None


def _run_detection_cycle_live_parity(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
) -> str:
    global _LAST_NO_PENDING_LOG_KEY_PARITY
    now = _now_ist()
    date_str = now.strftime("%Y-%m-%d")
    _ensure_detected_csv_exists(date_str)

    state = _load_pending_state(date_str)
    # strategy_v2 §J3 fix #7 — stamp lifecycle events with the target pool_rev
    # (= current rev + 1) the cycle's state write will commit. Pre-fix, events
    # carried no rev, so a crash between `_append_pool_lifecycle_event` and
    # `_write_pending_state_atomic` left audit and state mutually inconsistent
    # with no way to detect which side won. Downstream reconciliation tooling
    # can now compare event.pool_rev to on-disk state.pool_rev.
    target_pool_rev = load_pool_rev(state) + 1
    signals = state.get("signals", [])
    pending_sigs = [s for s in signals if str(s.get("status", "")).lower() == "pending"]

    # strategy_v2 §J3 fix #3 — parity-mode expires_at gate.
    # The legacy _process_pending_signal path (used when DETECTION_PARITY_MODE=0)
    # short-circuits expired signals at step a. The parity cycle used to skip
    # this check entirely, so a morning signal could be promoted to detected
    # after an afternoon DE restart and dispatched to the executor. The
    # executor's retry_deadline catches it eventually, but the pool row +
    # detected_csv gain spurious rows. Now we sweep expiries up front, inside
    # the pool_lock (caller already holds it), so the atomic write at the end
    # of the cycle persists status=expired_window for future cycles.
    expired_sigs: List[Dict[str, Any]] = []
    for sig in pending_sigs:
        expires_at_raw = sig.get("expires_at", "")
        if not expires_at_raw:
            continue
        parsed = base_v15._parse_ist_timestamp(str(expires_at_raw))
        if parsed is None:
            continue
        try:
            exp_ts = pd.Timestamp(parsed)
            if exp_ts.tzinfo is None:
                exp_ts = exp_ts.tz_localize(IST)
            else:
                exp_ts = exp_ts.tz_convert(IST)
            if now >= exp_ts:
                sig["status"] = "expired_window"
                sig["filter_reason"] = f"past expires_at={expires_at_raw}"
                expired_sigs.append(sig)
        except Exception:
            pass

    if expired_sigs:
        for sig in expired_sigs:
            try:
                _append_pool_lifecycle_event(
                    date_str,
                    {
                        "event":       "DROPPED",
                        "signal_id":   str(sig.get("signal_id", "")),
                        "ticker":      str(sig.get("ticker", "")),
                        "side":        str(sig.get("side", "")),
                        "setup":       str(sig.get("setup", "")),
                        "reason":      "expired_window_parity",
                        "source_slot": str(sig.get("source_slot", "")),
                    },
                    pool_rev=target_pool_rev,
                )
            except Exception as exc:
                print(f"[WARN] lifecycle DROPPED append failed: {exc}", flush=True)
        print(
            f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | expired_window={len(expired_sigs)}",
            flush=True,
        )
        pending_sigs = [s for s in pending_sigs if str(s.get("status", "")).lower() == "pending"]

    if not pending_sigs:
        if signals:
            _sync_pending_csv_if_needed(state, date_str)
        # Persist any expired→expired_window transitions even when nothing
        # remains pending, so the pool JSON + lifecycle audit reflect reality.
        if expired_sigs:
            state["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S%z")
            bump_pool_rev(state)
            _write_pending_state_atomic(state, date_str)
            _write_pending_csv(state, date_str)
        slot_log_key = _floor_to_5m(now).strftime("%Y%m%d_%H%M")
        if _LAST_NO_PENDING_LOG_KEY_PARITY != slot_log_key:
            print(
                f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | checked=0 | no pending signals",
                flush=True,
            )
            _LAST_NO_PENDING_LOG_KEY_PARITY = slot_log_key
        return "no_pending"
    _LAST_NO_PENDING_LOG_KEY_PARITY = None

    # Collect pending source slots for slot-aware marker matching.
    pending_source_slots: Set[str] = set()
    for sig in pending_sigs:
        slot_ts = _pending_signal_source_slot(sig)
        if slot_ts is not None:
            pending_source_slots.add(slot_ts.isoformat())

    ready_marker = _load_latest_ready_marker(
        now,
        allowed_slot_keys=pending_source_slots or None,
    )
    ready_tickers: Optional[Set[str]] = None
    if USE_READY_MARKER_HANDOFF:
        if ready_marker is None:
            print(
                f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                f"waiting_ready_marker (no fresh marker matching pending_slots="
                f"{sorted(pending_source_slots) if pending_source_slots else '[]'})",
                flush=True,
            )
            return "waiting_ready_marker"
        ready_tickers = set(ready_marker.get("_ready_tickers", set()) or set())
        if not ready_tickers:
            print(
                f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | waiting_ready_marker (empty ticker set)",
                flush=True,
            )
            return "waiting_ready_marker"

    slot_groups: Dict[str, Dict[str, Any]] = {}
    skipped_no_slot = 0
    for sig in pending_sigs:
        ticker = str(sig.get("ticker", "")).strip().upper()
        if ready_tickers is not None and ticker not in ready_tickers:
            continue
        slot_ts = _pending_signal_source_slot(sig)
        if slot_ts is None:
            skipped_no_slot += 1
            continue
        slot_key = slot_ts.isoformat()
        group = slot_groups.setdefault(slot_key, {"slot": slot_ts, "signals": []})
        group["signals"].append(sig)

    if not slot_groups:
        print(
            f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | ready=0 | waiting_slot_metadata",
            flush=True,
        )
        return "waiting_slot_metadata"

    marker_note = ""
    if ready_marker is not None:
        marker_note = (
            f" | ready_tickers={len(ready_tickers or set())}"
            f" | ready_age={ready_marker.get('_marker_age_sec', '?')}s"
        )
    print(
        f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
        f"ready_slots={len(slot_groups)} | parity_mode=live_combined{marker_note}",
        flush=True,
    )

    counters: Dict[str, int] = {
        "pending_at_start": len(pending_sigs),
        "confirmed": 0,
        "filtered": 0,
        "slots": 0,
        "short_written": 0,
        "long_written": 0,
        "unmatched_final": 0,
        "still_pending": 0,
        "waiting_ready": 0,
        "waiting_data": 0,
    }
    reason_counts: Counter[str] = Counter()
    # Fix #15: per-signal waiting breakdown (ticker not in ready_tickers =
    # waiting on Pending Fetcher; slot_wait_reasons = waiting on candle close).
    if ready_tickers is not None:
        for sig in pending_sigs:
            tk = str(sig.get("ticker", "")).strip().upper()
            if tk and tk not in ready_tickers:
                counters["waiting_ready"] += 1
                reason_counts["waiting_ready:ticker_not_in_ready_marker"] += 1
    for slot_key in sorted(slot_groups.keys()):
        group = slot_groups[slot_key]
        slot_ts = pd.Timestamp(group["slot"]).tz_convert(IST)
        slot_dt = slot_ts.to_pydatetime()
        slot_signals: List[Dict[str, Any]] = list(group["signals"])
        counters["slots"] += 1

        # Per-slot marker: narrow ticker set to only those confirmed ready for this exact slot.
        slot_marker = _load_ready_marker_for_slot(slot_ts, now)
        if slot_marker is not None:
            slot_ready_tickers = slot_marker.get("_ready_tickers") or set()
            slot_signals = [s for s in slot_signals if str(s.get("ticker", "")).strip().upper() in slot_ready_tickers]
            if not slot_signals:
                print(
                    f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | slot_marker_age={slot_marker.get('_marker_age_sec', '?')}s"
                    f" | all signals filtered by per-slot ticker set (size={len(slot_ready_tickers)})",
                    flush=True,
                )
                continue

        slot_wait_reasons: Dict[str, str] = {}
        slot_ready_signals: List[Dict[str, Any]] = []
        freshness_cache: Dict[str, Optional[str]] = {}
        for sig in slot_signals:
            ticker = str(sig.get("ticker", "")).strip().upper()
            if not ticker:
                slot_ready_signals.append(sig)
                continue
            if ticker not in freshness_cache:
                _df_pending, _age_sec, _last_row_ts, freshness_issue = _assess_pending_parquet_freshness(
                    ticker,
                    required_slot=slot_ts,
                )
                freshness_cache[ticker] = freshness_issue
            freshness_issue = freshness_cache[ticker]
            if freshness_issue is not None:
                slot_wait_reasons[ticker] = freshness_issue
                sig["status"] = "pending"
                sig["filter_reason"] = None
                continue
            slot_ready_signals.append(sig)

        slot_signals = slot_ready_signals
        if slot_wait_reasons:
            counters["waiting_data"] += len(slot_wait_reasons)
            for reason in slot_wait_reasons.values():
                reason_counts[f"waiting_data:{reason}"] += 1
            wait_preview = ", ".join(
                f"{ticker}:{reason}" for ticker, reason in sorted(slot_wait_reasons.items())[:5]
            )
            print(
                f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | waiting_data_complete={len(slot_wait_reasons)}"
                f" | {wait_preview}",
                flush=True,
            )
            if not slot_signals:
                continue

        # A3 / D2 — trigger-bar OHLC drift check (fail-closed per strategy_v2 §D2).
        # Runs AFTER the freshness gate: parquet is known-present, so a
        # hash mismatch genuinely means the trigger bar's OHLC changed
        # between SE-time and DE-time (Kite back-fill, late ticks, etc.).
        drifted_this_slot = 0
        drift_pass: List[Dict[str, Any]] = []
        for sig in slot_signals:
            live_hash = _check_trigger_bar_drift(sig)
            if live_hash is None:
                drift_pass.append(sig)
                continue
            stashed_hash = str(sig.get("trigger_ohlc_hash", "") or "").strip().lower()
            sig["status"] = "filtered_trigger_drifted"
            sig["filter_reason"] = "TRIGGER_BAR_DRIFTED"
            sig["detection_time"] = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
            drifted_this_slot += 1
            counters["filtered"] += 1
            reason_counts["filtered_trigger_drifted:TRIGGER_BAR_DRIFTED"] += 1
            sid = str(sig.get("signal_id", "")).strip()
            if sid:
                _append_pool_lifecycle_event(
                    date_str,
                    {
                        "signal_id": sid,
                        "event": "DROPPED",
                        "reason": "TRIGGER_BAR_DRIFTED",
                        "ticker": str(sig.get("ticker", "")),
                        "side": str(sig.get("side", "")),
                        "setup": str(sig.get("setup", "")),
                        "source_slot": slot_ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        "stashed_hash": stashed_hash,
                        "live_hash": live_hash,
                    },
                    pool_rev=target_pool_rev,
                )
            print(
                f"[DETECT_SIG] slot={slot_ts.strftime('%H:%M')} "
                f"{str(sig.get('ticker', '')).upper()} {str(sig.get('side', '')).upper()} -> "
                f"FILTERED (TRIGGER_BAR_DRIFTED stashed={stashed_hash} live={live_hash})",
                flush=True,
            )
        slot_signals = drift_pass
        if drifted_this_slot:
            print(
                f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | "
                f"trigger_drift_filtered={drifted_this_slot}",
                flush=True,
            )
            if not slot_signals:
                continue

        tickers = sorted({str(sig.get("ticker", "")).strip().upper() for sig in slot_signals if str(sig.get("ticker", "")).strip()})

        if not tickers:
            for sig in slot_signals:
                sig["status"] = "filtered_parity"
                sig["filter_reason"] = "missing_ticker"
                counters["filtered"] += 1
                reason_counts["filtered_parity:missing_ticker"] += 1
                sid = str(sig.get("signal_id", "")).strip()
                if sid:
                    _append_pool_lifecycle_event(
                        date_str,
                        {
                            "signal_id": sid,
                            "event": "DROPPED",
                            "reason": "filtered_parity:missing_ticker",
                            "ticker": str(sig.get("ticker", "")),
                            "side": str(sig.get("side", "")),
                            "source_slot": slot_ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        },
                        pool_rev=target_pool_rev,
                    )
            print(
                f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | tickers=0 | filtered={len(slot_signals)}",
                flush=True,
            )
            continue

        context_state = live_v16._build_backtest_context_state(slot_dt, short_cfg)
        allow_long = bool(context_state["allow_long"])
        allow_short = bool(context_state["allow_short"])

        short_rows, long_rows, scan_meta = live_v16._scan_slot(
            slot_dt,
            short_cfg,
            long_cfg,
            allow_long,
            allow_short,
            tickers=tickers,
        )
        short_rows, long_rows, filter_meta = live_v16._apply_backtest_parity_filters_to_dicts(
            short_rows,
            long_rows,
            short_cfg,
            long_cfg,
            context_state.get("mode_map", {}),
            context_state.get("nifty_ret_map", {}),
        )

        final_rows = list(short_rows) + list(long_rows)
        slot_detected_at_str = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
        final_by_id: Dict[str, Dict[str, Any]] = {}
        for row in final_rows:
            signal_id = _signal_id_from_rowlike(row)
            if signal_id:
                final_by_id[signal_id] = row

        pending_ids: Set[str] = set()
        matched_final_ids: Set[str] = set()
        matched_short_count = 0
        matched_long_count = 0
        slot_short_written = 0
        slot_long_written = 0
        slot_detected = 0
        slot_filtered = 0
        slot_hhmm = slot_ts.strftime('%H:%M')
        for sig in slot_signals:
            signal_id = str(sig.get("signal_id", "")).strip() or _signal_id_from_rowlike(sig)
            sig_ticker = str(sig.get("ticker", "")).strip().upper() or "?"
            sig_side = str(sig.get("side", "")).strip().upper() or "?"
            if signal_id:
                pending_ids.add(signal_id)
            if signal_id and signal_id in final_by_id:
                matched_row = final_by_id[signal_id]
                _update_pending_signal_from_parity_row(sig, matched_row, slot_detected_at_str)
                matched_final_ids.add(signal_id)
                slot_detected += 1
                counters["confirmed"] += 1
                side_upper = str(sig.get("side", matched_row.get("side", ""))).strip().upper()
                if side_upper == "SHORT":
                    matched_short_count += 1
                elif side_upper == "LONG":
                    matched_long_count += 1

                wrote = _write_confirmed_signal(
                    sig,
                    date_str,
                    slot_detected_at_str,
                    _safe_float(sig.get("entry_price", 0.0), 0.0),
                    _safe_float(sig.get("stop_price", 0.0), 0.0),
                    _safe_float(sig.get("target_price", 0.0), 0.0),
                    max(1, int(_safe_float(sig.get("quantity", 1), 1))),
                )
                if side_upper == "SHORT":
                    slot_short_written += int(wrote)
                elif side_upper == "LONG":
                    slot_long_written += int(wrote)
                # E2 (strategy_v2 §E2): DE_PASSED lifecycle event.
                if signal_id:
                    _append_pool_lifecycle_event(
                        date_str,
                        {
                            "signal_id": signal_id,
                            "event": "DE_PASSED",
                            "ticker": sig_ticker,
                            "side": side_upper,
                            "setup": str(sig.get("setup", "")),
                            "source_slot": slot_ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                            "written_to_csv": bool(wrote),
                        },
                        pool_rev=target_pool_rev,
                    )
                # Fix #15: per-signal outcome log.
                print(
                    f"[DETECT_SIG] slot={slot_hhmm} {sig_ticker} {side_upper} -> "
                    f"CONFIRMED (written={int(wrote)})",
                    flush=True,
                )
            else:
                sig["status"] = "filtered_parity"
                sig["filter_reason"] = "not_in_live_parity_final_set"
                slot_filtered += 1
                counters["filtered"] += 1
                reason_counts["filtered_parity:not_in_live_parity_final_set"] += 1
                # E2: DROPPED lifecycle event on parity miss.
                if signal_id:
                    _append_pool_lifecycle_event(
                        date_str,
                        {
                            "signal_id": signal_id,
                            "event": "DROPPED",
                            "reason": "filtered_parity:not_in_live_parity_final_set",
                            "ticker": sig_ticker,
                            "side": sig_side,
                            "setup": str(sig.get("setup", "")),
                            "source_slot": slot_ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        },
                        pool_rev=target_pool_rev,
                    )
                print(
                    f"[DETECT_SIG] slot={slot_hhmm} {sig_ticker} {sig_side} -> "
                    f"FILTERED (not_in_live_parity_final_set)",
                    flush=True,
                )

        counters["short_written"] += int(slot_short_written)
        counters["long_written"] += int(slot_long_written)

        unmatched_final = sorted(set(final_by_id.keys()) - pending_ids)
        if unmatched_final:
            counters["unmatched_final"] += len(unmatched_final)
            print(
                f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | unmatched_final={len(unmatched_final)} "
                f"(live parity produced signals missing from pending pool)",
                flush=True,
            )

        print(
            f"[DETECTION_PARITY] slot={slot_ts.strftime('%H:%M')} | tickers={len(tickers)} "
            f"raw_short={int(filter_meta.get('raw_short', 0))} raw_long={int(filter_meta.get('raw_long', 0))} "
            f"final_short={int(filter_meta.get('final_short', 0))} final_long={int(filter_meta.get('final_long', 0))} "
            f"detected={slot_detected} filtered={slot_filtered} "
            f"matched_short={matched_short_count} matched_long={matched_long_count} "
            f"short_written={slot_short_written} long_written={slot_long_written} "
            f"scan_elapsed={float(scan_meta.get('scan_elapsed_sec', 0.0)):.1f}s",
            flush=True,
        )

    counters["still_pending"] = sum(
        1 for s in signals if str(s.get("status", "")).lower() == "pending"
    )
    if skipped_no_slot:
        reason_counts["pending:missing_source_slot"] += int(skipped_no_slot)

    all_detected = [s for s in signals if str(s.get("status", "")).lower() == "detected"]
    _write_detected_csv(all_detected, date_str)

    state["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S%z")
    # strategy_v2 §C3 — bump monotonic pool revision before atomic replace
    # (lock is already held by the caller in run_loop).
    bump_pool_rev(state)
    _write_pending_state_atomic(state, date_str)
    _write_pending_csv(state, date_str)

    # Fix #15: cycle summary with explicit pending / promoted / waiting / filtered counts.
    waiting_total = counters["waiting_ready"] + counters["waiting_data"]
    reason_summary = ", ".join(
        f"{reason}={count}" for reason, count in reason_counts.most_common(8)
    )
    print(
        f"[DETECTION_CYCLE] {now.strftime('%H:%M:%S')} | pending={counters['pending_at_start']} | "
        f"slots={counters['slots']} | promoted={counters['confirmed']} | "
        f"waiting={waiting_total} (ready={counters['waiting_ready']},data={counters['waiting_data']}) | "
        f"filtered_parity={counters['filtered']} | "
        f"written=S{counters['short_written']}/L{counters['long_written']} | "
        f"still_pending={counters['still_pending']} | unmatched_final={counters['unmatched_final']} | "
        f"missing_slot_meta={skipped_no_slot}"
        f"{' | reasons=' + reason_summary if reason_summary else ''}",
        flush=True,
    )
    return "processed"


# ===========================================================================
# PATTERN RESCAN — find matching signal in fresh pending data
# ===========================================================================
def _rescan_ticker_for_signal(
    ticker: str,
    side: str,
    target_signal_id: str,
    target_signal_datetime: str,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    slot_ist: datetime,
    required_slot: Optional[pd.Timestamp] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Load fresh pending parquet, re-run scanner for this ticker,
    and look for a signal matching target_signal_id or the strategy entry timestamp.

    Returns (matched_row_dict, failure_reason).
    If matched_row_dict is not None, the signal is still valid pattern-wise.
    """
    ticker_u = ticker.upper().strip()

    # Load fresh pending data and require the source-slot candle to exist.
    df, _age_sec, _last_row_ts, freshness_issue = _assess_pending_parquet_freshness(
        ticker_u,
        required_slot=required_slot,
    )
    if freshness_issue is not None:
        return None, freshness_issue

    # Prepare for scanning (same as live scanner: use full history, then filter to today+slot)
    today = slot_ist.date()
    cfg = short_cfg if side.upper() == "SHORT" else long_cfg

    try:
        df_prepared = prepare_session_bars_for_scan(df, cfg)
        if df_prepared is None or df_prepared.empty:
            return None, "prepare_session_bars_failed"

        if "date" in df_prepared.columns:
            df_prepared = df_prepared[pd.to_datetime(df_prepared["date"]).dt.date == today]
        if df_prepared.empty:
            return None, "no_today_bars_after_prepare"

        if "datetime" in df_prepared.columns:
            bar_times = pd.to_datetime(df_prepared["datetime"], utc=True).dt.tz_convert(IST)
            df_prepared = df_prepared[bar_times <= slot_ist]
        if df_prepared.empty:
            return None, "no_bars_up_to_slot"

        # Run the appropriate scanner
        if side.upper() == "SHORT":
            trades = _scan_short_prepared_live(ticker_u, df_prepared, short_cfg)
        else:
            trades = _scan_long_prepared_live(ticker_u, df_prepared, long_cfg)

        if not trades:
            return None, "pattern_no_longer_present"

        # Try to match by signal_id (most precise) or by entry-time proximity.
        target_dt = pd.to_datetime(target_signal_datetime, errors="coerce")
        best_match: Optional[Dict[str, Any]] = None

        for trade in trades:
            row = asdict(trade) if not isinstance(trade, dict) else dict(trade)
            row["side"] = side.upper()

            # Match by signal time
            trade_time_raw = row.get(
                "signal_time_ist",
                row.get("signal_bar_time_ist", row.get("bar_time_ist", "")),
            )
            entry_time_raw = row.get("entry_time_ist", row.get("signal_entry_datetime_ist", trade_time_raw))
            entry_time_ts  = base_v15._parse_ist_timestamp(str(entry_time_raw))
            if entry_time_ts is None:
                continue

            entry_t   = str(entry_time_ts)
            setup_str = str(row.get("setup", ""))
            this_id   = base_v15._generate_signal_id(ticker_u, side.upper(), entry_t, setup_str)

            if this_id == target_signal_id:
                best_match = row
                break

            # Fallback: match if strategy entry time is within 5 minutes of target.
            if not pd.isna(target_dt) and entry_time_ts is not None:
                try:
                    trade_dt = pd.to_datetime(entry_t, errors="coerce")
                    if not pd.isna(trade_dt):
                        diff_min = abs((trade_dt - target_dt).total_seconds()) / 60.0
                        if diff_min <= 5.0:
                            best_match = row
                            break
                except Exception:
                    pass

        if best_match is None:
            return None, "pattern_no_longer_present"

        return best_match, ""

    except Exception as exc:
        return None, f"rescan_error: {exc}"


# ===========================================================================
# PROCESS ONE PENDING SIGNAL
# ===========================================================================
def _process_pending_signal(
    sig: Dict[str, Any],
    now_ist: datetime,
    rs_pct: float,
    allow_long: bool,
    allow_short: bool,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    date_str: str,
    ready_tickers: Optional[Set[str]] = None,
) -> str:
    """
    Evaluate one pending signal. Returns final status string.
    Side effects: writes to signals CSV if confirmed, updates sig dict in place.
    """
    ticker     = str(sig.get("ticker", "")).upper().strip()
    side       = str(sig.get("side", "")).upper()
    signal_id  = str(sig.get("signal_id", ""))
    signal_dt  = str(sig.get("signal_datetime", ""))
    entry_dt   = str(sig.get("signal_entry_datetime_ist", signal_dt))
    entry_price = _safe_float(sig.get("entry_price", 0.0))
    stop_price  = _safe_float(sig.get("stop_price", 0.0))
    target_price = _safe_float(sig.get("target_price", 0.0))
    notional    = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
    qty         = max(1, int(notional / entry_price)) if entry_price > 0 else 1

    # ── Step a: Expiry check ─────────────────────────────────────────────────
    expires_at_raw = sig.get("expires_at", "")
    if expires_at_raw:
        expires_at = base_v15._parse_ist_timestamp(str(expires_at_raw))
        if expires_at is not None:
            try:
                exp_ts = pd.Timestamp(expires_at)
                if exp_ts.tzinfo is None:
                    exp_ts = exp_ts.tz_localize(IST)
                else:
                    exp_ts = exp_ts.tz_convert(IST)
                if now_ist >= exp_ts:
                    sig["status"] = "expired_window"
                    sig["filter_reason"] = f"past expires_at={expires_at_raw}"
                    return "expired_window"
            except Exception:
                pass

    # ── Step b+c: Load fresh parquet + freshness gate ────────────────────────
    if ready_tickers is not None and ticker not in ready_tickers:
        return "skip_wait_ready_marker"

    required_slot = _pending_signal_source_slot(sig)
    df_pending, _age_sec, _last_row_ts, freshness_issue = _assess_pending_parquet_freshness(
        ticker,
        required_slot=required_slot,
    )
    if freshness_issue is not None:
        if freshness_issue.startswith("no_parquet"):
            # Parquet not yet written by Pending Fetcher — wait
            return "skip_no_data"
        if freshness_issue.startswith("data_incomplete"):
            print(f"  [DETECTION] {ticker} {side} → waiting_data_complete ({freshness_issue})", flush=True)
            return "skip_incomplete_data"
        # Data not yet refreshed by Pending Fetcher — skip this cycle, try next
        return "skip_stale_data"

    # ── Step d: Price confirmation ────────────────────────────────────────────
    latest_close = _safe_float(df_pending.iloc[-1].get("close", 0.0)) if not df_pending.empty else 0.0

    if entry_price > 0 and latest_close > 0:
        if side == "LONG":
            if latest_close < entry_price * (1.0 - PRICE_CONFIRM_LONG_TOLERANCE):
                reason = f"price_reversed: close={latest_close:.2f} < entry*(1-tol)={entry_price*(1-PRICE_CONFIRM_LONG_TOLERANCE):.2f}"
                sig["status"] = "expired_price"
                sig["filter_reason"] = reason
                print(f"  [DETECTION] {ticker} {side} → expired_price ({reason})", flush=True)
                return "expired_price"
        else:  # SHORT
            if latest_close > entry_price * (1.0 + PRICE_CONFIRM_SHORT_TOLERANCE):
                reason = f"price_reversed: close={latest_close:.2f} > entry*(1+tol)={entry_price*(1+PRICE_CONFIRM_SHORT_TOLERANCE):.2f}"
                sig["status"] = "expired_price"
                sig["filter_reason"] = reason
                print(f"  [DETECTION] {ticker} {side} → expired_price ({reason})", flush=True)
                return "expired_price"

    # ── Step e: Pattern rescan on fresh data ─────────────────────────────────
    matched_row, rescan_fail_reason = _rescan_ticker_for_signal(
        ticker, side, signal_id, entry_dt,
        short_cfg, long_cfg, now_ist,
        required_slot=required_slot,
    )
    if matched_row is None:
        if rescan_fail_reason.startswith("data_incomplete"):
            return "skip_incomplete_data"
        if rescan_fail_reason.startswith("data_stale") or rescan_fail_reason.startswith("no_parquet"):
            return "skip_stale_data"
        sig["status"] = "expired_price"
        sig["filter_reason"] = f"rescan_failed: {rescan_fail_reason}"
        print(f"  [DETECTION] {ticker} {side} → expired_price (rescan: {rescan_fail_reason})", flush=True)
        return "expired_price"

    # ── Step f: RS filter (NIFTY context) ────────────────────────────────────
    # Uses the full neutralization logic (weak daymove → neutral, directional fallback)
    # matching Signal Engine's _resolve_nifty_context_flags() behavior.
    if side == "LONG" and not allow_long:
        reason = f"rs_pct={rs_pct:.2f}% LONG not allowed by Nifty context"
        sig["status"] = "filtered_rs"
        sig["filter_reason"] = reason
        print(f"  [DETECTION] {ticker} {side} → filtered_rs ({reason})", flush=True)
        return "filtered_rs"
    if side == "SHORT" and not allow_short:
        reason = f"rs_pct={rs_pct:.2f}% SHORT not allowed by Nifty context"
        sig["status"] = "filtered_rs"
        sig["filter_reason"] = reason
        print(f"  [DETECTION] {ticker} {side} → filtered_rs ({reason})", flush=True)
        return "filtered_rs"

    # ── Step g: V16 post-scan filters ────────────────────────────────────────
    v16_reason = get_v16_filter_reason(matched_row, side)
    if v16_reason is not None:
        # Map reason text to a status key
        if "RSI" in v16_reason:
            status_key = "filtered_short_rsi"
        elif "QS=" in v16_reason:
            status_key = "filtered_qs"
        elif "avwap_dist_atr" in v16_reason and "cap" in v16_reason:
            status_key = "filtered_dist_cap"
        elif "avwap_dist_atr" in v16_reason and "dead zone" in v16_reason:
            status_key = "filtered_dist_dead"
        elif "vol_ratio" in v16_reason:
            status_key = "filtered_vol_exhaust"
        elif "OR gate" in v16_reason:
            status_key = "filtered_or_gate"
        else:
            status_key = "filtered_v16"
        sig["status"] = status_key
        sig["filter_reason"] = v16_reason
        print(f"  [DETECTION] {ticker} {side} → {status_key} ({v16_reason})", flush=True)
        return status_key

    # ── Step h: CONFIRMED ─────────────────────────────────────────────────────
    detected_at_str   = now_ist.strftime("%Y-%m-%d %H:%M:%S%z")
    fresh_entry_price = _safe_float(matched_row.get("entry_price", entry_price)) or entry_price
    fresh_stop_price  = _safe_float(matched_row.get("stop_price", matched_row.get("sl_price", stop_price))) or stop_price
    fresh_target_price = _safe_float(matched_row.get("target_price", target_price)) or target_price
    fresh_signal_price = _safe_float(
        matched_row.get("signal_price", sig.get("signal_price", fresh_entry_price))
    ) or fresh_entry_price
    fresh_qty = max(1, int(notional / fresh_entry_price)) if fresh_entry_price > 0 else qty

    # Merge fresh scan fields back for logging
    sig["quality_score"]    = round(_safe_float(matched_row.get("quality_score", sig.get("quality_score", 0.0))), 4)
    sig["avwap_dist_atr"]   = round(_safe_float(matched_row.get("avwap_dist_atr_signal", sig.get("avwap_dist_atr", 0.0))), 4)
    sig["rsi_signal"]       = round(_safe_float(matched_row.get("rsi_signal", sig.get("rsi_signal", 0.0))), 2)
    sig["adx"]              = round(_safe_float(matched_row.get("adx_signal", matched_row.get("adx", sig.get("adx", 0.0)))), 2)
    sig["impulse_type"]     = str(matched_row.get("impulse_type", sig.get("impulse_type", "")))
    sig["quantity"]         = fresh_qty
    sig["signal_price"]     = round(fresh_signal_price, 2)
    sig["entry_price"]      = round(fresh_entry_price, 2)
    sig["stop_price"]       = round(fresh_stop_price, 2)
    sig["target_price"]     = round(fresh_target_price, 2)
    sig["signal_entry_datetime_ist"] = str(
        matched_row.get(
            "entry_time_ist",
            matched_row.get("signal_entry_datetime_ist", entry_dt),
        )
    )
    sig["status"]           = "detected"
    sig["detection_time"]   = detected_at_str
    sig["detection_price"]  = round(latest_close, 2)
    sig["filter_reason"]    = None

    # Compute signal bar time from matched row
    signal_bar_time = str(matched_row.get(
        "signal_time_ist",
        matched_row.get("signal_bar_time_ist", matched_row.get("bar_time_ist", signal_dt)),
    ))
    sig["signal_bar_time"]  = signal_bar_time

    written = _write_confirmed_signal(
        sig, date_str, detected_at_str,
        fresh_entry_price, fresh_stop_price, fresh_target_price, fresh_qty,
    )

    # Compute original signal bar time for lag reporting
    try:
        orig_dt    = pd.to_datetime(signal_dt, errors="coerce")
        detect_dt  = pd.to_datetime(detected_at_str, errors="coerce")
        if not pd.isna(orig_dt) and not pd.isna(detect_dt):
            lag_sec = int((detect_dt - orig_dt).total_seconds())
            lag_str = f"+{lag_sec // 60}m{lag_sec % 60}s"
        else:
            lag_str = "?"
    except Exception:
        lag_str = "?"

    if written:
        print(
            f"  [DETECTED] {ticker} {side} | detected_at={detected_at_str} "
            f"| price={latest_close:.2f} | entry={fresh_entry_price:.2f} "
            f"| signal_at={signal_dt} | lag={lag_str}",
            flush=True,
        )
    else:
        print(f"  [DETECTION] {ticker} {side} → confirmed but already in CSV (dedup skipped)", flush=True)

    return "detected"


# ===========================================================================
# MAIN CHECK CYCLE
# ===========================================================================
def _run_detection_cycle(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
) -> str:
    """Run one full detection cycle over today's pending signals."""
    global _LAST_NO_PENDING_LOG_KEY_LEGACY
    now = _now_ist()
    date_str = now.strftime("%Y-%m-%d")
    _ensure_detected_csv_exists(date_str)

    state = _load_pending_state(date_str)
    signals = state.get("signals", [])
    pending_sigs = [s for s in signals if str(s.get("status", "")).lower() == "pending"]

    if not pending_sigs:
        if signals:
            _sync_pending_csv_if_needed(state, date_str)
        slot_log_key = _floor_to_5m(now).strftime("%Y%m%d_%H%M")
        if _LAST_NO_PENDING_LOG_KEY_LEGACY != slot_log_key:
            print(
                f"[DETECTION] {now.strftime('%H:%M:%S')} | checked=0 | "
                f"no pending signals",
                flush=True,
            )
            _LAST_NO_PENDING_LOG_KEY_LEGACY = slot_log_key
        return "no_pending"
    _LAST_NO_PENDING_LOG_KEY_LEGACY = None

    # Collect pending source slots for slot-aware marker matching.
    pending_source_slots_legacy: Set[str] = set()
    for sig in pending_sigs:
        slot_ts = _pending_signal_source_slot(sig)
        if slot_ts is not None:
            pending_source_slots_legacy.add(slot_ts.isoformat())

    ready_marker = _load_latest_ready_marker(
        now,
        allowed_slot_keys=pending_source_slots_legacy or None,
    )
    ready_tickers: Optional[Set[str]] = None
    if USE_READY_MARKER_HANDOFF:
        if ready_marker is None:
            print(
                f"[DETECTION] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                f"waiting_ready_marker (no fresh marker matching pending_slots="
                f"{sorted(pending_source_slots_legacy) if pending_source_slots_legacy else '[]'})",
                flush=True,
            )
            return "waiting_ready_marker"
        ready_tickers = set(ready_marker.get("_ready_tickers", set()) or set())
        if not ready_tickers:
            print(
                f"[DETECTION] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                "waiting_ready_marker (empty ticker set)",
                flush=True,
            )
            return "waiting_ready_marker"

    # Compute NIFTY RS + context once per cycle (same value used for all pending signals)
    rs_pct, allow_long, allow_short = _load_niftybees_rs(now)
    marker_note = ""
    if ready_marker is not None:
        marker_note = (
            f" | ready_tickers={len(ready_tickers or set())}"
            f" | ready_age={ready_marker.get('_marker_age_sec', '?')}s"
        )
    print(
        f"[DETECTION] {now.strftime('%H:%M:%S')} | "
        f"pending={len(pending_sigs)} | nifty_rs={rs_pct:+.2f}% "
        f"allow_long={allow_long} allow_short={allow_short}{marker_note}",
        flush=True,
    )

    counters: Dict[str, int] = {
        "confirmed": 0, "expired": 0, "filtered": 0, "skipped": 0, "still_pending": 0,
    }

    for sig in pending_sigs:
        ticker = str(sig.get("ticker", "")).strip().upper()
        side   = str(sig.get("side", "")).upper()
        result = _process_pending_signal(
            sig, now, rs_pct, allow_long, allow_short,
            short_cfg, long_cfg, date_str,
            ready_tickers=ready_tickers,
        )
        if result == "detected":
            counters["confirmed"] += 1
        elif result.startswith("expired"):
            counters["expired"] += 1
        elif result.startswith("filtered"):
            counters["filtered"] += 1
        elif result.startswith("skip"):
            counters["skipped"] += 1
            # Don't overwrite status — leave as pending for next cycle
            sig["status"] = "pending"

    # Recount still-pending after the cycle
    counters["still_pending"] = sum(
        1 for s in signals if str(s.get("status", "")).lower() == "pending"
    )

    # Write confirmed signals to detected CSV (all signals with status="detected")
    all_detected = [s for s in signals if str(s.get("status", "")).lower() == "detected"]
    _write_detected_csv(all_detected, date_str)

    # Update pending JSON
    state["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S%z")
    # strategy_v2 §C3 — bump monotonic pool revision before atomic replace
    # (lock is already held by the caller in run_loop).
    bump_pool_rev(state)
    _write_pending_state_atomic(state, date_str)
    _write_pending_csv(state, date_str)

    confirmed_tickers = [
        f"{s.get('ticker')} {s.get('side')}"
        for s in pending_sigs
        if str(s.get("status", "")).lower() == "detected"
    ]
    confirmed_str = f"({', '.join(confirmed_tickers)})" if confirmed_tickers else ""

    print(
        f"[DETECTION] {now.strftime('%H:%M:%S')} | checked={len(pending_sigs)} | "
        f"confirmed={counters['confirmed']} {confirmed_str} | "
        f"filtered={counters['filtered']} | expired={counters['expired']} | "
        f"skipped={counters['skipped']} | still_pending={counters['still_pending']}",
        flush=True,
    )
    return "processed"


def _run_startup_signal_id_self_check() -> None:
    session_date = _now_ist().strftime("%Y-%m-%d")
    # Fix #18 (2026-04-21): graceful degradation — if Stage 1's proof never
    # appears within the retry budget, log a warning and continue without the
    # cross-stage verification. The local Stage 2 fixture check still runs so
    # Stage 2's own signal-id hashing is validated. This avoids the crash loop
    # observed on 2026-04-21 when the old Stage 1 wasn't writing the proof.
    retry_deadline = time.monotonic() + float(SELF_CHECK_PROOF_RETRY_TIMEOUT_SEC)
    proof: Optional[Dict[str, Any]] = None
    while True:
        try:
            proof = signal_id_selfcheck.load_stage1_proof(session_date)
            break
        except FileNotFoundError:
            if time.monotonic() >= retry_deadline:
                print(
                    f"[STARTUP] signal_id proof still missing after "
                    f"{SELF_CHECK_PROOF_RETRY_TIMEOUT_SEC}s — starting without "
                    f"cross-stage proof verification (local fixture check still runs). "
                    f"Stage 1 must be restarted to emit its proof for full verification.",
                    flush=True,
                )
                proof = None
                break
            print(
                f"[STARTUP] signal_id proof not yet written by Stage 1; "
                f"retrying in {SELF_CHECK_PROOF_RETRY_SLEEP_SEC}s "
                f"(budget={SELF_CHECK_PROOF_RETRY_TIMEOUT_SEC}s)",
                flush=True,
            )
            time.sleep(float(SELF_CHECK_PROOF_RETRY_SLEEP_SEC))

    # Local Stage 2 fixture check — always run, regardless of proof availability.
    fixture = signal_id_selfcheck.build_stage2_fixture()
    computed_signal_id = _signal_id_from_rowlike(fixture)
    if not computed_signal_id:
        raise RuntimeError("signal-id self-check fixture did not compute in Stage 2")
    if computed_signal_id != signal_id_selfcheck.EXPECTED_SIGNAL_ID:
        raise RuntimeError(
            "Stage 2 signal-id self-check hash mismatch: "
            f"expected {signal_id_selfcheck.EXPECTED_SIGNAL_ID}, got {computed_signal_id}"
        )

    if proof is None:
        print(
            f"[STARTUP] signal_id self-check: Stage 2 local fixture OK "
            f"(stage2_signal_id={computed_signal_id}); cross-stage proof skipped.",
            flush=True,
        )
        return

    proof_expected = str(proof.get("expected_signal_id", "")).strip()
    if proof_expected != signal_id_selfcheck.EXPECTED_SIGNAL_ID:
        raise RuntimeError(
            "Stage 2 signal-id self-check expected hash mismatch in proof: "
            f"expected {signal_id_selfcheck.EXPECTED_SIGNAL_ID}, got {proof_expected or '<blank>'}"
        )

    stage1_signal_id = str(proof.get("stage1_signal_id", "")).strip()
    if not stage1_signal_id:
        raise RuntimeError("Stage 2 signal-id self-check proof missing stage1_signal_id")
    if stage1_signal_id != computed_signal_id:
        raise RuntimeError(
            "Stage 1 / Stage 2 signal-id self-check mismatch: "
            f"stage1={stage1_signal_id}, stage2={computed_signal_id}"
        )

    fixture_meta = proof.get("fixture", {}) or {}
    stage1_entry_time = str(proof.get("stage1_canonical_entry_time", "")).strip()
    expected_entry_time = str(fixture_meta.get("entry_time_canonical", "")).strip()
    if expected_entry_time and stage1_entry_time != expected_entry_time:
        raise RuntimeError(
            "Stage 1 proof canonical entry_time mismatch: "
            f"expected {expected_entry_time}, got {stage1_entry_time or '<blank>'}"
        )

    proof_path = signal_id_selfcheck.proof_path_for_date(session_date)
    print(
        "[STARTUP] signal_id self-check passed "
        f"| stage1_signal_id={stage1_signal_id} | stage2_signal_id={computed_signal_id} "
        f"| proof={proof_path}",
        flush=True,
    )


# ===========================================================================
# MAIN LOOP
# ===========================================================================
def main() -> None:
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min DETECTION ENGINE (Stage 2)\n"
        f"  LIVE_DATA_DIR    : {RUNTIME_DATA_5M_DIR}\n"
        f"  SIGNALS_DIR      : {RUNTIME_LIVE_SIGNALS_DIR}\n"
        f"  CHECK_INTERVAL   : {CHECK_INTERVAL_SEC}s\n"
        f"  STARTUP_OFFSET   : {STARTUP_OFFSET_SEC}s (offset from Pending Fetcher)\n"
        f"  MODE            : {'LIVE PARITY' if DETECTION_PARITY_MODE else 'LEGACY CONFIRMATION'}\n"
        "  Applies live-combined parity slot finalization when parity mode is enabled\n"
        "=" * 70,
        flush=True,
    )

    _touch_status("STARTING")

    # Startup offset — run AFTER Pending Data Fetcher so fresh data is available
    if STARTUP_OFFSET_SEC > 0:
        print(f"[INFO] Startup offset: sleeping {STARTUP_OFFSET_SEC}s before first check", flush=True)
        time.sleep(float(STARTUP_OFFSET_SEC))

    try:
        _run_startup_signal_id_self_check()
    except Exception as exc:
        _touch_status("FAILED", phase="STARTUP_SELF_CHECK", reason=str(exc))
        _touch_heartbeat("FAILED", phase="STARTUP_SELF_CHECK", reason=str(exc))
        print(f"[FATAL] Startup signal_id self-check failed: {exc}", flush=True)
        raise

    if DETECTION_PARITY_MODE:
        short_cfg, long_cfg = live_v16._build_v16_cfgs()
    else:
        short_cfg, long_cfg = _build_v16_cfgs()

    last_completed_slot: Optional[str] = None

    while True:
        now = _now_ist()
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP:
            _touch_status("STOPPED_AFTER_CUTOFF")
            _touch_heartbeat("STOPPED")
            print(
                f"[STOP] Hard-stop {HARD_STOP.strftime('%H:%M')} reached "
                "(aligned with executor entry cutoff). Exiting.",
                flush=True,
            )
            return

        if now.time() < MARKET_OPEN:
            market_open_dt = IST.localize(datetime.combine(now.date(), MARKET_OPEN))
            if ALIGN_TO_5MIN_BOUNDARY:
                wake = market_open_dt + timedelta(seconds=int(SLOT_OFFSET_SEC))
                phase = "WAIT_SLOT"
            else:
                wake = market_open_dt - timedelta(seconds=30)
                phase = "WAIT_MARKET_OPEN"
            _touch_status("RUNNING", phase=phase)
            time.sleep(min(60.0, max(5.0, (wake - now).total_seconds())))
            continue

        slot_key: Optional[str] = None
        slot_display: Optional[str] = None
        next_wake: Optional[datetime] = None
        no_pending_recheck_at: Optional[datetime] = None
        if ALIGN_TO_5MIN_BOUNDARY:
            slot = _floor_to_5m(now)
            slot_key = slot.strftime("%Y%m%d_%H%M")
            slot_display = slot.strftime("%H:%M")
            run_at = slot + timedelta(seconds=int(SLOT_OFFSET_SEC))
            next_wake = slot + timedelta(minutes=5, seconds=int(SLOT_OFFSET_SEC))
            no_pending_recheck_at = slot + timedelta(seconds=int(NO_PENDING_RECHECK_AFTER_SEC))
            if now < run_at:
                _touch_status("RUNNING", phase="WAIT_SLOT", slot=slot_display)
                time.sleep(max(0.5, (run_at - now).total_seconds()))
                continue
            if last_completed_slot == slot_key:
                _touch_status("RUNNING", phase="WAIT_NEXT_SLOT", slot=slot_display)
                time.sleep(max(0.5, (next_wake - now).total_seconds()))
                continue

        # strategy_v2 C1 — NF slot-ready gate. Refuse to detect this slot until
        # the NIFTY guard fetcher publishes its per-slot marker. On timeout we
        # emit [ABORT] NF_STALE + a lifecycle event and skip the slot so the
        # RS gate can never run against a stale NIFTYBEES parquet.
        nf_ready = True
        nf_waited = 0.0
        nf_marker = ""
        if ALIGN_TO_5MIN_BOUNDARY and slot_key is not None:
            _touch_status("RUNNING", phase="WAIT_NF", slot=slot_display)
            nf_ready, nf_waited, nf_marker = _wait_for_nifty_slot_ready(slot)

        if not nf_ready:
            print(
                f"[ABORT] NF_STALE slot={slot_display} waited={nf_waited:.1f}s "
                f"timeout={NF_READY_TIMEOUT_SEC:.0f}s marker={nf_marker} "
                f"reason=no_nf_slot_ready_marker",
                flush=True,
            )
            try:
                date_str = slot.astimezone(IST).strftime("%Y-%m-%d")
                _append_pool_lifecycle_event(date_str, {
                    "event":      "NF_STALE",
                    "slot":       slot_display,
                    "waited_sec": round(nf_waited, 2),
                    "timeout_sec": float(NF_READY_TIMEOUT_SEC),
                    "marker":     nf_marker,
                })
            except Exception as exc:
                print(f"[WARN] NF_STALE lifecycle append failed: {exc}", flush=True)
            _touch_status("RUNNING", phase="NF_STALE", slot=slot_display)
            cycle_result = "nf_stale"
        else:
            _touch_status("RUNNING", phase="DETECT", slot=slot_display)
            # strategy_v2 §C3 — wrap the full cycle (load → V16 filter →
            # atomic write) in the pool lock so SE/PF cannot interleave a
            # write into the same pending JSON during the cycle body. The
            # cycle typically completes in <5s; the 30s lock timeout gives
            # plenty of slack over the slot budget.
            cycle_date_str = _now_ist().strftime("%Y-%m-%d")
            try:
                with pool_lock(cycle_date_str):
                    if DETECTION_PARITY_MODE:
                        cycle_result = _run_detection_cycle_live_parity(short_cfg, long_cfg)
                    else:
                        cycle_result = _run_detection_cycle(short_cfg, long_cfg)
            except Exception as exc:
                print(f"[ERROR] Detection cycle error: {exc}", flush=True)
                cycle_result = "error"

        _touch_status("RUNNING", phase="IDLE")
        _touch_heartbeat("RUNNING", phase="IDLE")
        if ALIGN_TO_5MIN_BOUNDARY and slot_key is not None and next_wake is not None:
            if cycle_result in {"waiting_ready_marker", "waiting_slot_metadata", "error", "no_pending"}:
                now_after = _now_ist()
                if cycle_result == "no_pending" and no_pending_recheck_at is not None and now_after < no_pending_recheck_at:
                    retry_sleep = min(
                        (no_pending_recheck_at - now_after).total_seconds(),
                        (next_wake - now_after).total_seconds(),
                    )
                elif cycle_result == "waiting_ready_marker":
                    # Cap at CHECK_INTERVAL_SEC so production (2s) stays fast;
                    # also cap at 5s so high-default-interval configs don't stall.
                    ready_poll = min(float(CHECK_INTERVAL_SEC), 5.0)
                    retry_sleep = min(ready_poll, max(0.5, (next_wake - now_after).total_seconds()))
                else:
                    retry_sleep = min(float(CHECK_INTERVAL_SEC), max(0.5, (next_wake - now_after).total_seconds()))
                time.sleep(max(0.5, retry_sleep))
            else:
                last_completed_slot = slot_key
                time.sleep(max(0.5, (next_wake - _now_ist()).total_seconds()))
        else:
            if cycle_result == "waiting_ready_marker":
                time.sleep(min(float(CHECK_INTERVAL_SEC), 5.0))
            else:
                time.sleep(float(CHECK_INTERVAL_SEC))


if __name__ == "__main__":
    main()
