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
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
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
    SLOT_READY_PENDING_DIR,
    RUNTIME_STATUS_DIR,
)

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
SHORT_SIGNAL_CSV_PATTERN   = "signals_{}_v16_5min_short.csv"
LONG_SIGNAL_CSV_PATTERN    = "signals_{}_v16_5min_long.csv"
DETECTED_CSV_PATTERN       = "detected_signals_{}_v16_5min.csv"

END_5M                = "_stocks_indicators_5min.parquet"
NIFTYBEES_TICKER      = "NIFTYBEES"

CHECK_INTERVAL_SEC    = int(os.getenv("EQIDV2_DETECTION_CHECK_INTERVAL_SEC", "60"))
STARTUP_OFFSET_SEC    = int(os.getenv("EQIDV2_DETECTION_STARTUP_OFFSET_SEC", "30"))
MARKET_OPEN           = dtime(9, 15)
HARD_STOP             = dtime(15, 35)
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

    if age_sec > MAX_DATA_AGE_SEC:
        return (
            df,
            age_sec,
            last_row_ts,
            f"data_stale ({age_sec:.0f}s > {MAX_DATA_AGE_SEC}s; last_row={_format_ist_ts(last_row_ts)})",
        )

    return df, age_sec, last_row_ts, None


def _load_latest_ready_marker(
    now_ist: datetime,
    allowed_slot_keys: Optional[Set[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Return the freshest ready marker for today's session.

    When ``allowed_slot_keys`` is provided (a set of ISO-formatted floor-to-5min
    slot timestamps), iterate newest-first through ALL fresh markers and return
    the first one whose slot matches. Without the filter the freshest fresh
    marker wins — matching the original semantics.

    This replaces the old "take newest, reject on slot mismatch" pattern, which
    could leave a perfectly good older marker unused and stall the detection
    cycle in a `waiting_ready_marker` loop until its max-age expired.
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
        if age_sec > float(READY_MARKER_MAX_AGE_SEC):
            continue

        if allowed_slot_keys:
            marker_slot_key = pd.Timestamp(marker_ts).floor("5min").isoformat()
            if marker_slot_key not in allowed_slot_keys:
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
    """Load the ready marker for a specific slot by exact filename lookup."""
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
    if age_sec > float(READY_MARKER_MAX_AGE_SEC):
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
    signals = state.get("signals", [])
    pending_sigs = [s for s in signals if str(s.get("status", "")).lower() == "pending"]

    if not pending_sigs:
        if signals:
            _sync_pending_csv_if_needed(state, date_str)
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

    ready_marker = _load_latest_ready_marker(now)
    ready_tickers: Optional[Set[str]] = None
    if USE_READY_MARKER_HANDOFF:
        if ready_marker is None:
            print(
                f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | waiting_ready_marker",
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
        # Slot-aware check: the ready marker must belong to one of our pending source
        # slots. Consuming a marker from a different slot produces stale ticker sets.
        if pending_source_slots:
            marker_slot_ts = _normalize_ist_timestamp(ready_marker.get("slot", ""))
            if marker_slot_ts is not None:
                marker_slot_key = marker_slot_ts.floor("5min").isoformat()
                if marker_slot_key not in pending_source_slots:
                    print(
                        f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                        f"waiting_ready_marker (slot mismatch: marker={marker_slot_key} "
                        f"pending_slots={sorted(pending_source_slots)})",
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
        "confirmed": 0,
        "filtered": 0,
        "slots": 0,
        "short_written": 0,
        "long_written": 0,
        "unmatched_final": 0,
        "still_pending": 0,
    }
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

        tickers = sorted({str(sig.get("ticker", "")).strip().upper() for sig in slot_signals if str(sig.get("ticker", "")).strip()})

        if not tickers:
            for sig in slot_signals:
                sig["status"] = "filtered_parity"
                sig["filter_reason"] = "missing_ticker"
                counters["filtered"] += 1
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
        for sig in slot_signals:
            signal_id = str(sig.get("signal_id", "")).strip() or _signal_id_from_rowlike(sig)
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
            else:
                sig["status"] = "filtered_parity"
                sig["filter_reason"] = "not_in_live_parity_final_set"
                slot_filtered += 1
                counters["filtered"] += 1

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

    all_detected = [s for s in signals if str(s.get("status", "")).lower() == "detected"]
    _write_detected_csv(all_detected, date_str)

    state["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S%z")
    _write_pending_state_atomic(state, date_str)
    _write_pending_csv(state, date_str)

    print(
        f"[DETECTION_PARITY] {now.strftime('%H:%M:%S')} | slots={counters['slots']} | "
        f"confirmed={counters['confirmed']} | filtered={counters['filtered']} | "
        f"short_written={counters['short_written']} | long_written={counters['long_written']} | "
        f"still_pending={counters['still_pending']} | unmatched_final={counters['unmatched_final']} | "
        f"missing_slot_meta={skipped_no_slot}",
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

    ready_marker = _load_latest_ready_marker(now)
    ready_tickers: Optional[Set[str]] = None
    if USE_READY_MARKER_HANDOFF:
        if ready_marker is None:
            print(
                f"[DETECTION] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                "waiting_ready_marker",
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
        # Slot-aware check: the ready marker must belong to one of our pending source slots.
        if pending_source_slots_legacy:
            marker_slot_ts = _normalize_ist_timestamp(ready_marker.get("slot", ""))
            if marker_slot_ts is not None:
                marker_slot_key = marker_slot_ts.floor("5min").isoformat()
                if marker_slot_key not in pending_source_slots_legacy:
                    print(
                        f"[DETECTION] {now.strftime('%H:%M:%S')} | pending={len(pending_sigs)} | "
                        f"waiting_ready_marker (slot mismatch: marker={marker_slot_key} "
                        f"pending_slots={sorted(pending_source_slots_legacy)})",
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
            print("[STOP] Hard-stop reached. Exiting.", flush=True)
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

        _touch_status("RUNNING", phase="DETECT", slot=slot_display)

        try:
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
