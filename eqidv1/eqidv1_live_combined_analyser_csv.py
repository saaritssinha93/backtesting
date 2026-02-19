# -*- coding: utf-8 -*-
"""
EQIDV1 — LIVE 15m Signal Scanner (AVWAP v11 combined: LONG + SHORT)
====================================================================

Adapted from stocks_live_trading_signal_15m_v11_combined_parquet.py, but wired
to the eqidv1 backtesting ecosystem with **refined parameters** from:
    backtesting/eqidv1/avwap_v11_refactored/avwap_common.py

Key refinements vs the original live scanner:
- Tighter SL: 0.75% (was 1.0%)
- Better R:R: SHORT TGT=1.2%, LONG TGT=1.5%
- Stricter ADX: min=25, slope_min=1.25 (SHORT) / 0.80 (LONG)
- Stricter RSI: SHORT max=55, LONG min=45
- Stricter Stoch: SHORT max=75, LONG min=25
- NEW: Volume filter — impulse bar volume >= 1.2x SMA(20) volume
- NEW: ATR% volatility filter — ATR/close >= 0.20%
- Close-confirm required on entry candle by default

Data: reads from stocks_indicators_15min_eq/ (same parquet directory).
Core: backtesting/eqidv1/trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py
"""

from __future__ import annotations

import csv
import hashlib
import os
import sys
import glob
import json
import time
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

# =============================================================================
# Wire eqidv1 core into sys.path
# =============================================================================
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly as core  # noqa: E402
from avwap_v11_refactored.avwap_common import (
    default_short_config,
    default_long_config,
    prepare_indicators as ref_prepare_indicators,
    compute_day_avwap as ref_compute_day_avwap,
)
from avwap_v11_refactored.avwap_short_strategy import scan_one_day as scan_short_one_day
from avwap_v11_refactored.avwap_long_strategy import scan_one_day as scan_long_one_day


# =============================================================================
# TIMEZONE + DIRECTORIES
# =============================================================================
IST = pytz.timezone("Asia/Kolkata")

DIR_15M = "stocks_indicators_15min_eq"
END_15M = "_stocks_indicators_15min.parquet"

ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "reports" / "eqidv1_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

OUT_CHECKS_DIR = ROOT / "out_eqidv1_live_checks_15m"
OUT_SIGNALS_DIR = ROOT / "out_eqidv1_live_signals_15m"
OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

STATE_DIR = ROOT / "logs"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "eqidv1_avwap_live_state_v11.json"

PARQUET_ENGINE = "pyarrow"

# =============================================================================
# CSV BRIDGE: Write signals in the format the trade executors expect
# =============================================================================
LIVE_SIGNAL_DIR = ROOT / "live_signals"
LIVE_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
SIGNAL_CSV_PATTERN = "signals_{}.csv"

# Position sizing for CSV output
# position_size = capital/margin per trade; notional = position_size * leverage
DEFAULT_POSITION_SIZE_RS = 50_000       # Rs. margin per trade
INTRADAY_LEVERAGE = 5.0                 # MIS leverage on Zerodha

SIGNAL_CSV_COLUMNS = [
    "signal_id", "signal_datetime", "received_time", "ticker", "side",
    "setup", "impulse_type", "entry_price", "stop_price", "target_price",
    "quality_score", "atr_pct", "rsi", "adx", "quantity",
    "signal_entry_datetime_ist", "signal_bar_time_ist",
]

# =============================================================================
# SCHEDULER CONFIG
# =============================================================================
START_TIME = dtime(9, 15)
END_TIME = dtime(15, 30)
HARD_STOP_TIME = dtime(15, 40)

# Timing: data fetcher (eqidv1_eod_15min_data_stocks) takes ~1 minute after each
# 15-min boundary.  We delay the first scan and run multiple attempts so at least
# one scan sees fully-updated data.
INITIAL_DELAY_SECONDS = 60      # wait 60s after slot for data to settle
NUM_SCANS_PER_SLOT = 3          # run 3 scans per 15-min window
SCAN_INTERVAL_SECONDS = 60      # 60s gap between consecutive scans

# Update flag: set True to call eqidv1 core.run_mode("15min") before each scan
UPDATE_15M_BEFORE_CHECK = False

# How many tail rows to load per ticker parquet
TAIL_ROWS = 260


# =============================================================================
# LATEST SLOT (15m) SCAN BEHAVIOR
# =============================================================================
# If True: only evaluate signals on the latest COMPLETED 15m candle (slot_end),
# i.e. the candle whose end time is floor(now - buffer_sec) to 15m.
LATEST_SLOT_ONLY = True
LATEST_SLOT_BUFFER_SEC = 60     # wait this many seconds after boundary before trusting the new candle
LATEST_SLOT_TOLERANCE_SEC = 120  # allow small timestamp drift (seconds) in parquet timestamps

# For speed, keep only the last N bars from today (must be >= 7 for v11 logic)
MAX_BARS_PER_TICKER_TODAY = 120
# =============================================================================
# SESSION FILTER
# =============================================================================
SESSION_START = dtime(9, 15, 0)
SESSION_END = dtime(14, 30, 0)

# =============================================================================
# V11 SHORT PARAMETERS — refined from avwap_common.default_short_config()
# =============================================================================
SHORT_STOP_PCT = 0.0075          # tighter SL: 0.75% (was 1.0%)
SHORT_TARGET_PCT = 0.0120        # better R:R: 1.2% (was 0.65%)

SHORT_ADX_MIN = 25.0             # stricter (was 20)
SHORT_ADX_SLOPE_MIN = 1.25       # stricter (was 1.0)

SHORT_RSI_MAX = 55.0             # stricter (was 60)
SHORT_STOCHK_MAX = 75.0          # stricter (was 80)

SHORT_REQUIRE_EMA_TREND = True
SHORT_REQUIRE_AVWAP_BELOW = True

SHORT_USE_TIME_WINDOWS = True
SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(11, 30, 0)),
    (dtime(13, 0, 0), dtime(14, 30, 0)),
]

# Impulse thresholds
SHORT_MOD_RED_MIN_ATR = 0.45     # slightly tighter (was 0.40)
SHORT_MOD_RED_MAX_ATR = 1.00
SHORT_HUGE_RED_MIN_ATR = 1.60    # slightly tighter (was 1.50)
SHORT_HUGE_RED_MIN_RANGE_ATR = 2.00
SHORT_CLOSE_NEAR_LOW_MAX = 0.25

SHORT_SMALL_GREEN_MAX_ATR = 0.20

# Entry buffer
BUFFER_ABS = 0.05
BUFFER_PCT = 0.0002

# AVWAP rejection
SHORT_AVWAP_REJ_ENABLED = True
SHORT_AVWAP_REJ_TOUCH = True
SHORT_AVWAP_REJ_CONSEC_CLOSES = 2
SHORT_AVWAP_REJ_DIST_ATR_MULT = 0.25
SHORT_AVWAP_REJ_MODE = "any"

SHORT_CAP_PER_TICKER_PER_DAY = 1

# =============================================================================
# V11 LONG PARAMETERS — refined from avwap_common.default_long_config()
# =============================================================================
LONG_STOP_PCT = 0.0075           # tighter SL: 0.75% (was 1.0%)
LONG_TARGET_PCT = 0.0150         # better TGT: 1.5% (was 0.65%)

LONG_ADX_MIN = 25.0              # stricter (was 20)
LONG_ADX_SLOPE_MIN = 0.80        # relaxed vs short (was 1.0)

LONG_RSI_MIN = 45.0              # stricter (was 40)
LONG_STOCHK_MIN = 25.0           # stricter (was 20)

LONG_REQUIRE_EMA_TREND = True
LONG_REQUIRE_AVWAP_ABOVE = True

LONG_USE_TIME_WINDOWS = True
LONG_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(11, 30, 0)),
    (dtime(13, 0, 0), dtime(14, 30, 0)),
]

# Impulse thresholds
LONG_MOD_GREEN_MIN_ATR = 0.30    # relaxed for LONG (was 0.40)
LONG_MOD_GREEN_MAX_ATR = 1.00
LONG_HUGE_GREEN_MIN_ATR = 1.60   # slightly tighter (was 1.50)
LONG_HUGE_GREEN_MIN_RANGE_ATR = 2.00
LONG_CLOSE_NEAR_HIGH_MAX = 0.25

LONG_SMALL_RED_MAX_ATR = 0.20

# AVWAP rejection
LONG_AVWAP_REJ_ENABLED = True
LONG_AVWAP_REJ_TOUCH = True
LONG_AVWAP_REJ_CONSEC_CLOSES = 2
LONG_AVWAP_REJ_DIST_ATR_MULT = 0.25
LONG_AVWAP_REJ_MODE = "any"

LONG_CAP_PER_TICKER_PER_DAY = 1

# =============================================================================
# NEW: QUALITY FILTERS (from avwap_common refactored config)
# =============================================================================
# Volume filter: impulse bar volume must be >= ratio * SMA(period) of volume
USE_VOLUME_FILTER = True
VOLUME_SMA_PERIOD = 20
VOLUME_MIN_RATIO = 1.2

# ATR% volatility filter: ATR/close must be >= threshold
USE_ATR_PCT_FILTER = True
ATR_PCT_MIN = 0.0020  # 0.20%

# Close-confirm: entry candle close must confirm the breakout direction
REQUIRE_CLOSE_CONFIRM = True

# Top-N filter (optional; usually off in live)
USE_TOPN_PER_RUN = False
TOPN_PER_RUN = 30


# =============================================================================
# PYARROW REQUIREMENT
# =============================================================================
def _require_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except Exception as e:
        raise RuntimeError("Parquet support requires 'pyarrow' (pip install pyarrow).") from e


# =============================================================================
# UTILITIES
# =============================================================================
def now_ist() -> datetime:
    return datetime.now(IST)

def latest_completed_15m_slot_end(buffer_sec: int = LATEST_SLOT_BUFFER_SEC) -> pd.Timestamp:
    """Return the latest COMPLETED 15m candle end timestamp in IST.
    Example: if now=13:16 and buffer=60s => slot_end=13:15.
    """
    dt = now_ist() - timedelta(seconds=int(buffer_sec))
    minute = (dt.minute // 15) * 15
    dt_floor = dt.replace(minute=minute, second=0, microsecond=0)
    ts = pd.Timestamp(dt_floor)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts



def _buffer(price: float) -> float:
    return max(float(BUFFER_ABS), float(price) * float(BUFFER_PCT))


def in_session(ts: pd.Timestamp) -> bool:
    t = ts.tz_convert(IST).time()
    return (t >= SESSION_START) and (t <= SESSION_END)


def _in_windows(ts: pd.Timestamp, windows: List[Tuple[dtime, dtime]], enabled: bool) -> bool:
    if not enabled:
        return True
    t = ts.tz_convert(IST).time()
    for a, b in windows:
        if a <= t <= b:
            return True
    return False


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _twice_increasing(df: pd.DataFrame, idx: int, col: str) -> bool:
    if idx < 2 or col not in df.columns:
        return False
    a = _safe_float(df.at[idx, col])
    b = _safe_float(df.at[idx - 1, col])
    c = _safe_float(df.at[idx - 2, col])
    return np.isfinite(a) and np.isfinite(b) and np.isfinite(c) and (a > b > c)


def _twice_decreasing(df: pd.DataFrame, idx: int, col: str) -> bool:
    if idx < 2 or col not in df.columns:
        return False
    a = _safe_float(df.at[idx, col])
    b = _safe_float(df.at[idx - 1, col])
    c = _safe_float(df.at[idx - 2, col])
    return np.isfinite(a) and np.isfinite(b) and np.isfinite(c) and (a < b < c)


def _slope_ok(df: pd.DataFrame, idx: int, col: str, min_slope: float, direction: str = "up") -> bool:
    if idx < 2 or col not in df.columns:
        return False
    a = _safe_float(df.at[idx, col])
    c = _safe_float(df.at[idx - 2, col])
    if not (np.isfinite(a) and np.isfinite(c)):
        return False
    if direction == "up":
        return (a - c) >= float(min_slope)
    return (c - a) >= float(min_slope)


# =============================================================================
# INDICATORS (fallback computations, only if columns missing)
# =============================================================================
def ensure_ema(close: pd.Series, span: int) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    return close.ewm(span=span, adjust=False).mean()


def compute_atr14(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(14).mean()


def compute_rsi14(close: pd.Series) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_stoch_14_3(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    ll = low.rolling(14).min()
    hh = high.rolling(14).max()
    denom = (hh - ll).replace(0, np.nan)
    k = 100.0 * (close - ll) / denom
    d = k.rolling(3).mean()
    return k, d


def compute_adx14(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean().replace(0, np.nan)
    plus_di = 100.0 * (pd.Series(plus_dm, index=df.index).ewm(alpha=1 / 14, adjust=False).mean() / atr)
    minus_di = 100.0 * (pd.Series(minus_dm, index=df.index).ewm(alpha=1 / 14, adjust=False).mean() / atr)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / 14, adjust=False).mean()


def compute_day_avwap(df_day: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df_day["high"], errors="coerce")
    low = pd.to_numeric(df_day["low"], errors="coerce")
    close = pd.to_numeric(df_day["close"], errors="coerce")
    vol = pd.to_numeric(df_day.get("volume", 0.0), errors="coerce").fillna(0.0)
    tp = (high + low + close) / 3.0
    pv = tp * vol
    cum_pv = pv.cumsum()
    cum_v = vol.cumsum().replace(0, np.nan)
    return cum_pv / cum_v


# =============================================================================
# IO (fast tail read)
# =============================================================================
def read_parquet_tail(path: str, n: int = 250) -> pd.DataFrame:
    _require_pyarrow()
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa

        pf = pq.ParquetFile(path)
        num_row_groups = pf.num_row_groups
        if num_row_groups <= 1:
            df = pd.read_parquet(path, engine=PARQUET_ENGINE)
        else:
            rows = 0
            groups = []
            rg = num_row_groups - 1
            while rg >= 0 and rows < n:
                tbl = pf.read_row_group(rg)
                groups.append(tbl)
                rows += tbl.num_rows
                rg -= 1
            tbl_all = pa.concat_tables(list(reversed(groups)))
            df = tbl_all.to_pandas()
            if len(df) > n:
                df = df.tail(n).reset_index(drop=True)
        return df
    except Exception:
        df = pd.read_parquet(path, engine=PARQUET_ENGINE)
        return df.tail(n).reset_index(drop=True)


def list_tickers_15m() -> List[str]:
    pattern = os.path.join(DIR_15M, f"*{END_15M}")
    files = glob.glob(pattern)
    out: List[str] = []
    for f in files:
        base = os.path.basename(f)
        if base.endswith(END_15M):
            out.append(base[: -len(END_15M)].upper())
    return sorted(set(out))


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the 'date' column to tz-aware IST timestamps.

    IMPORTANT: Our candle-builder writes wall-clock IST times (often tz-naive) into parquet.
    So if the column is tz-naive, we localize to IST (not UTC).
    """
    if df.empty or "date" not in df.columns:
        return df

    df = df.copy()
    dt = pd.to_datetime(df["date"], errors="coerce")

    # If tz-naive, assume timestamps are already in IST wall-clock.
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(IST)
    else:
        dt = dt.dt.tz_convert(IST)

    df["date"] = dt
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


# =============================================================================
# STATE (duplicate prevention)
# =============================================================================
def _load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {"last_signal": {}}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"last_signal": {}}


def _save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(STATE_FILE)


def _state_key(ticker: str, side: str) -> str:
    return f"{ticker.upper()}|{side.upper()}"


def allow_signal_today(state: Dict[str, Any], ticker: str, side: str, today: str, cap_per_day: int) -> bool:
    cap = int(cap_per_day)
    if cap <= 0:
        return True
    state.setdefault("count", {})
    state["count"].setdefault(today, {})
    key = _state_key(ticker, side)
    n = int(state["count"][today].get(key, 0))
    return n < cap


def mark_signal(state: Dict[str, Any], ticker: str, side: str, today: str) -> None:
    state.setdefault("count", {})
    state["count"].setdefault(today, {})
    key = _state_key(ticker, side)
    state["count"][today][key] = int(state["count"][today].get(key, 0)) + 1
    state.setdefault("last_signal", {})
    state["last_signal"][key] = today


# =============================================================================
# NEW: VOLUME + ATR% QUALITY FILTERS
# =============================================================================
def _volume_filter_ok(df_day: pd.DataFrame, bar_idx: int) -> bool:
    """Check if the impulse bar's volume is >= VOLUME_MIN_RATIO * SMA(VOLUME_SMA_PERIOD)."""
    if not USE_VOLUME_FILTER:
        return True
    if "volume" not in df_day.columns:
        return True  # pass if no volume data
    vol = _safe_float(df_day.at[bar_idx, "volume"])
    if not np.isfinite(vol) or vol <= 0:
        return False
    # Compute volume SMA over the available preceding bars
    start = max(0, bar_idx - VOLUME_SMA_PERIOD + 1)
    vol_window = pd.to_numeric(df_day.loc[start:bar_idx, "volume"], errors="coerce").dropna()
    if len(vol_window) < 3:
        return True  # not enough data, pass
    avg_vol = vol_window.mean()
    if avg_vol <= 0:
        return True
    return vol >= (VOLUME_MIN_RATIO * avg_vol)


def _atr_pct_filter_ok(df_day: pd.DataFrame, bar_idx: int) -> bool:
    """Check if ATR/close >= ATR_PCT_MIN (volatility filter)."""
    if not USE_ATR_PCT_FILTER:
        return True
    atr = _safe_float(df_day.at[bar_idx, "ATR15"])
    close = _safe_float(df_day.at[bar_idx, "close"])
    if not (np.isfinite(atr) and np.isfinite(close) and close > 0):
        return False
    return (atr / close) >= ATR_PCT_MIN


# =============================================================================
# IMPULSE CLASSIFIERS
# =============================================================================
def classify_red_impulse(row: pd.Series) -> str:
    o = _safe_float(row["open"])
    c = _safe_float(row["close"])
    h = _safe_float(row["high"])
    l = _safe_float(row["low"])
    atr = _safe_float(row["ATR15"])

    if not np.isfinite(atr) or atr <= 0:
        return ""
    if not (c < o):
        return ""

    body = abs(c - o)
    rng = (h - l) if (h >= l) else np.nan
    if not np.isfinite(rng) or rng <= 0:
        return ""

    close_near_low = ((c - l) / rng) <= SHORT_CLOSE_NEAR_LOW_MAX

    if (body >= SHORT_HUGE_RED_MIN_ATR * atr) or (rng >= SHORT_HUGE_RED_MIN_RANGE_ATR * atr):
        return "HUGE"

    if (body >= SHORT_MOD_RED_MIN_ATR * atr) and (body <= SHORT_MOD_RED_MAX_ATR * atr) and close_near_low:
        return "MODERATE"

    return ""


def classify_green_impulse(row: pd.Series) -> str:
    o = _safe_float(row["open"])
    c = _safe_float(row["close"])
    h = _safe_float(row["high"])
    l = _safe_float(row["low"])
    atr = _safe_float(row["ATR15"])

    if not np.isfinite(atr) or atr <= 0:
        return ""
    if not (c > o):
        return ""

    body = abs(c - o)
    rng = (h - l) if (h >= l) else np.nan
    if not np.isfinite(rng) or rng <= 0:
        return ""

    close_near_high = ((h - c) / rng) <= LONG_CLOSE_NEAR_HIGH_MAX

    if (body >= LONG_HUGE_GREEN_MIN_ATR * atr) or (rng >= LONG_HUGE_GREEN_MIN_RANGE_ATR * atr):
        return "HUGE"

    if (body >= LONG_MOD_GREEN_MIN_ATR * atr) and (body <= LONG_MOD_GREEN_MAX_ATR * atr) and close_near_high:
        return "MODERATE"

    return ""


# =============================================================================
# AVWAP REJECTION (Option B)
# =============================================================================
def _avwap_rejection_short(df_day: pd.DataFrame, impulse_idx: int, entry_idx: int) -> Tuple[bool, Dict[str, Any]]:
    dbg: Dict[str, Any] = {"rej_ok": False, "rej_touch": False, "rej_consec": False, "rej_dist": False}
    if not SHORT_AVWAP_REJ_ENABLED:
        dbg["rej_ok"] = True
        return True, dbg

    if "AVWAP" not in df_day.columns:
        return False, dbg

    start = impulse_idx + 1
    end = entry_idx
    if start >= len(df_day) or entry_idx <= impulse_idx:
        return False, dbg

    seg = df_day.iloc[start: end + 1].copy()
    av = pd.to_numeric(seg["AVWAP"], errors="coerce")
    hi = pd.to_numeric(seg["high"], errors="coerce")
    cl = pd.to_numeric(seg["close"], errors="coerce")

    touch_ok = False
    if SHORT_AVWAP_REJ_TOUCH:
        touch_ok = bool(((hi >= av) & (cl < av)).fillna(False).any())

    consec_ok = False
    n = int(SHORT_AVWAP_REJ_CONSEC_CLOSES)
    if n > 0 and len(seg) >= n:
        consec_ok = bool((cl.tail(n) < av.tail(n)).fillna(False).all())

    mode = str(SHORT_AVWAP_REJ_MODE).lower().strip()
    if mode == "touch_only":
        rej_struct = touch_ok
    elif mode == "consec_only":
        rej_struct = consec_ok
    else:
        rej_struct = touch_ok or consec_ok

    entry_close = _safe_float(df_day.at[entry_idx, "close"])
    entry_avwap = _safe_float(df_day.at[entry_idx, "AVWAP"])
    entry_atr = _safe_float(df_day.at[entry_idx, "ATR15"])
    dist_ok = (
        np.isfinite(entry_close) and np.isfinite(entry_avwap) and np.isfinite(entry_atr)
        and entry_atr > 0 and ((entry_avwap - entry_close) >= SHORT_AVWAP_REJ_DIST_ATR_MULT * entry_atr)
    )

    dbg.update({"rej_touch": touch_ok, "rej_consec": consec_ok, "rej_dist": dist_ok})
    dbg["rej_ok"] = bool(rej_struct and dist_ok)
    return dbg["rej_ok"], dbg


def _avwap_rejection_long(df_day: pd.DataFrame, impulse_idx: int, entry_idx: int) -> Tuple[bool, Dict[str, Any]]:
    dbg: Dict[str, Any] = {"rej_ok": False, "rej_touch": False, "rej_consec": False, "rej_dist": False}
    if not LONG_AVWAP_REJ_ENABLED:
        dbg["rej_ok"] = True
        return True, dbg

    if "AVWAP" not in df_day.columns:
        return False, dbg

    start = impulse_idx + 1
    end = entry_idx
    if start >= len(df_day) or entry_idx <= impulse_idx:
        return False, dbg

    seg = df_day.iloc[start: end + 1].copy()
    av = pd.to_numeric(seg["AVWAP"], errors="coerce")
    lo = pd.to_numeric(seg["low"], errors="coerce")
    cl = pd.to_numeric(seg["close"], errors="coerce")

    touch_ok = False
    if LONG_AVWAP_REJ_TOUCH:
        touch_ok = bool(((lo <= av) & (cl > av)).fillna(False).any())

    consec_ok = False
    n = int(LONG_AVWAP_REJ_CONSEC_CLOSES)
    if n > 0 and len(seg) >= n:
        consec_ok = bool((cl.tail(n) > av.tail(n)).fillna(False).all())

    mode = str(LONG_AVWAP_REJ_MODE).lower().strip()
    if mode == "touch_only":
        rej_struct = touch_ok
    elif mode == "consec_only":
        rej_struct = consec_ok
    else:
        rej_struct = touch_ok or consec_ok

    entry_close = _safe_float(df_day.at[entry_idx, "close"])
    entry_avwap = _safe_float(df_day.at[entry_idx, "AVWAP"])
    entry_atr = _safe_float(df_day.at[entry_idx, "ATR15"])
    dist_ok = (
        np.isfinite(entry_close) and np.isfinite(entry_avwap) and np.isfinite(entry_atr)
        and entry_atr > 0 and ((entry_close - entry_avwap) >= LONG_AVWAP_REJ_DIST_ATR_MULT * entry_atr)
    )

    dbg.update({"rej_touch": touch_ok, "rej_consec": consec_ok, "rej_dist": dist_ok})
    dbg["rej_ok"] = bool(rej_struct and dist_ok)
    return dbg["rej_ok"], dbg


# =============================================================================
# LIVE ENTRY CHECKS
# =============================================================================
@dataclass
class LiveSignal:
    ticker: str
    side: str
    bar_time_ist: pd.Timestamp
    setup: str
    entry_price: float
    sl_price: float
    target_price: float
    score: float
    diagnostics: Dict[str, Any]


def _prepare_today_df(df: pd.DataFrame) -> pd.DataFrame:
    """Build today's df_day with required indicator columns."""
    df = normalize_dates(df)
    if df.empty:
        return df

    df = df[df["date"].apply(in_session)].copy()
    if df.empty:
        return df

    today = now_ist().date()
    df["day"] = df["date"].dt.tz_convert(IST).dt.date
    df_day = df[df["day"] == today].copy()
    if df_day.empty:
        return df_day

    df_day = df_day.sort_values("date").reset_index(drop=True)

    for c in ["open", "high", "low", "close"]:
        df_day[c] = pd.to_numeric(df_day[c], errors="coerce")

    # ATR
    if "ATR" in df_day.columns:
        df_day["ATR15"] = pd.to_numeric(df_day["ATR"], errors="coerce")
    else:
        df_day["ATR15"] = compute_atr14(df_day)

    # EMAs
    if "EMA_20" in df_day.columns:
        df_day["EMA20"] = pd.to_numeric(df_day["EMA_20"], errors="coerce")
    else:
        df_day["EMA20"] = ensure_ema(df_day["close"], 20)

    if "EMA_50" in df_day.columns:
        df_day["EMA50"] = pd.to_numeric(df_day["EMA_50"], errors="coerce")
    else:
        df_day["EMA50"] = ensure_ema(df_day["close"], 50)

    # RSI
    if "RSI" in df_day.columns:
        df_day["RSI15"] = pd.to_numeric(df_day["RSI"], errors="coerce")
    else:
        df_day["RSI15"] = compute_rsi14(df_day["close"])

    # Stochastic
    if "Stoch_%K" in df_day.columns:
        df_day["STOCHK15"] = pd.to_numeric(df_day["Stoch_%K"], errors="coerce")
        df_day["STOCHD15"] = pd.to_numeric(df_day.get("Stoch_%D", np.nan), errors="coerce")
    else:
        k, d = compute_stoch_14_3(df_day)
        df_day["STOCHK15"] = k
        df_day["STOCHD15"] = d

    # ADX
    if "ADX" in df_day.columns:
        df_day["ADX15"] = pd.to_numeric(df_day["ADX"], errors="coerce")
    else:
        df_day["ADX15"] = compute_adx14(df_day)

    # AVWAP
    df_day["AVWAP"] = compute_day_avwap(df_day)

    return df_day


def _score_signal_short(df_day: pd.DataFrame, impulse_idx: int, entry_idx: int) -> float:
    adx = _safe_float(df_day.at[entry_idx, "ADX15"])
    adx_prev2 = _safe_float(df_day.at[max(0, entry_idx - 2), "ADX15"])
    adx_slope = (adx - adx_prev2) if (np.isfinite(adx) and np.isfinite(adx_prev2)) else 0.0

    av = _safe_float(df_day.at[entry_idx, "AVWAP"])
    cl = _safe_float(df_day.at[entry_idx, "close"])
    atr = _safe_float(df_day.at[entry_idx, "ATR15"])
    dist_atr = ((av - cl) / atr) if (np.isfinite(av) and np.isfinite(cl) and np.isfinite(atr) and atr > 0) else 0.0

    o = _safe_float(df_day.at[impulse_idx, "open"])
    c = _safe_float(df_day.at[impulse_idx, "close"])
    body_atr = (abs(c - o) / atr) if (np.isfinite(o) and np.isfinite(c) and np.isfinite(atr) and atr > 0) else 0.0

    return float(adx_slope + 1.5 * dist_atr + 0.7 * body_atr)


def _score_signal_long(df_day: pd.DataFrame, impulse_idx: int, entry_idx: int) -> float:
    adx = _safe_float(df_day.at[entry_idx, "ADX15"])
    adx_prev2 = _safe_float(df_day.at[max(0, entry_idx - 2), "ADX15"])
    adx_slope = (adx - adx_prev2) if (np.isfinite(adx) and np.isfinite(adx_prev2)) else 0.0

    av = _safe_float(df_day.at[entry_idx, "AVWAP"])
    cl = _safe_float(df_day.at[entry_idx, "close"])
    atr = _safe_float(df_day.at[entry_idx, "ATR15"])
    dist_atr = ((cl - av) / atr) if (np.isfinite(av) and np.isfinite(cl) and np.isfinite(atr) and atr > 0) else 0.0

    o = _safe_float(df_day.at[impulse_idx, "open"])
    c = _safe_float(df_day.at[impulse_idx, "close"])
    body_atr = (abs(c - o) / atr) if (np.isfinite(o) and np.isfinite(c) and np.isfinite(atr) and atr > 0) else 0.0

    return float(adx_slope + 1.5 * dist_atr + 0.7 * body_atr)


def _check_common_filters_short(df_day: pd.DataFrame, i: int) -> Tuple[bool, Dict[str, Any]]:
    dbg: Dict[str, Any] = {}

    adx = _safe_float(df_day.at[i, "ADX15"])
    rsi = _safe_float(df_day.at[i, "RSI15"])
    k = _safe_float(df_day.at[i, "STOCHK15"])
    d = _safe_float(df_day.at[i, "STOCHD15"])

    adx_ok = (
        np.isfinite(adx) and adx >= SHORT_ADX_MIN
        and _twice_increasing(df_day, i, "ADX15")
        and _slope_ok(df_day, i, "ADX15", SHORT_ADX_SLOPE_MIN, direction="up")
    )
    rsi_ok = np.isfinite(rsi) and (rsi <= SHORT_RSI_MAX) and _twice_decreasing(df_day, i, "RSI15")
    stoch_ok = np.isfinite(k) and np.isfinite(d) and (k <= SHORT_STOCHK_MAX) and (k < d) and _twice_decreasing(df_day, i, "STOCHK15")

    # NEW: volume + ATR% quality filters on impulse bar
    vol_ok = _volume_filter_ok(df_day, i)
    atr_pct_ok = _atr_pct_filter_ok(df_day, i)

    dbg.update({
        "adx": adx, "rsi": rsi, "k": k, "d": d,
        "adx_ok": adx_ok, "rsi_ok": rsi_ok, "stoch_ok": stoch_ok,
        "vol_ok": vol_ok, "atr_pct_ok": atr_pct_ok,
    })

    if not (adx_ok and rsi_ok and stoch_ok and vol_ok and atr_pct_ok):
        return False, dbg

    close1 = _safe_float(df_day.at[i, "close"])
    ema20 = _safe_float(df_day.at[i, "EMA20"])
    ema50 = _safe_float(df_day.at[i, "EMA50"])
    av = _safe_float(df_day.at[i, "AVWAP"])

    ema_ok = True
    av_ok = True

    if SHORT_REQUIRE_EMA_TREND:
        ema_ok = np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(close1) and (ema20 < ema50) and (close1 < ema20)

    if SHORT_REQUIRE_AVWAP_BELOW:
        av_ok = np.isfinite(av) and np.isfinite(close1) and (close1 < av)

    dbg.update({"close": close1, "ema20": ema20, "ema50": ema50, "avwap": av, "ema_trend_ok": ema_ok, "avwap_ok": av_ok})

    return bool(ema_ok and av_ok), dbg


def _check_common_filters_long(df_day: pd.DataFrame, i: int) -> Tuple[bool, Dict[str, Any]]:
    dbg: Dict[str, Any] = {}

    adx = _safe_float(df_day.at[i, "ADX15"])
    rsi = _safe_float(df_day.at[i, "RSI15"])
    k = _safe_float(df_day.at[i, "STOCHK15"])
    d = _safe_float(df_day.at[i, "STOCHD15"])

    adx_ok = (
        np.isfinite(adx) and adx >= LONG_ADX_MIN
        and _twice_increasing(df_day, i, "ADX15")
        and _slope_ok(df_day, i, "ADX15", LONG_ADX_SLOPE_MIN, direction="up")
    )
    rsi_ok = np.isfinite(rsi) and (rsi >= LONG_RSI_MIN) and _twice_increasing(df_day, i, "RSI15")
    stoch_ok = np.isfinite(k) and np.isfinite(d) and (k >= LONG_STOCHK_MIN) and (k > d) and _twice_increasing(df_day, i, "STOCHK15")

    # NEW: volume + ATR% quality filters on impulse bar
    vol_ok = _volume_filter_ok(df_day, i)
    atr_pct_ok = _atr_pct_filter_ok(df_day, i)

    dbg.update({
        "adx": adx, "rsi": rsi, "k": k, "d": d,
        "adx_ok": adx_ok, "rsi_ok": rsi_ok, "stoch_ok": stoch_ok,
        "vol_ok": vol_ok, "atr_pct_ok": atr_pct_ok,
    })

    if not (adx_ok and rsi_ok and stoch_ok and vol_ok and atr_pct_ok):
        return False, dbg

    close1 = _safe_float(df_day.at[i, "close"])
    ema20 = _safe_float(df_day.at[i, "EMA20"])
    ema50 = _safe_float(df_day.at[i, "EMA50"])
    av = _safe_float(df_day.at[i, "AVWAP"])

    ema_ok = True
    av_ok = True

    if LONG_REQUIRE_EMA_TREND:
        ema_ok = np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(close1) and (ema20 > ema50) and (close1 > ema20)

    if LONG_REQUIRE_AVWAP_ABOVE:
        av_ok = np.isfinite(av) and np.isfinite(close1) and (close1 > av)

    dbg.update({"close": close1, "ema20": ema20, "ema50": ema50, "avwap": av, "ema_trend_ok": ema_ok, "avwap_ok": av_ok})

    return bool(ema_ok and av_ok), dbg


# =============================================================================
# MAIN SIGNAL DETECTION (per-ticker, both sides)
# =============================================================================

def _to_ist_ts(x: Any) -> pd.Timestamp:
    ts = pd.Timestamp(x)
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _current_15m_slot_start_ist() -> pd.Timestamp:
    """Current 15m bracket start in IST. Example: 10:05 -> 10:00."""
    dt = now_ist().astimezone(IST)
    flo = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
    return _to_ist_ts(flo)


def _latest_entry_signals_for_ticker(
    ticker: str,
    df_raw: pd.DataFrame,
    state: Dict[str, Any],
    target_slot_ist: pd.Timestamp,
) -> Tuple[List[LiveSignal], List[Dict[str, Any]]]:
    """Strict live-slot parity check using v3 row-by-row signal generation logic."""
    signals: List[LiveSignal] = []
    checks: List[Dict[str, Any]] = []

    if df_raw is None or df_raw.empty:
        return signals, checks

    df = normalize_dates(df_raw)
    if df.empty or "date" not in df.columns:
        return signals, checks

    df = df[df["date"].apply(in_session)].copy()
    if df.empty:
        return signals, checks

    df["date_ist"] = df["date"].apply(_to_ist_ts)
    df["day"] = df["date_ist"].dt.date
    df = df.sort_values("date_ist").reset_index(drop=True)

    target_slot = _to_ist_ts(target_slot_ist).floor("min")
    target_day = target_slot.date()
    target_day_str = str(target_day)
    df_day = df[df["day"] == target_day].copy().sort_values("date_ist").reset_index(drop=True)
    if df_day.empty:
        checks.append(
            {
                "ticker": ticker,
                "side": "SHORT",
                "bar_time_ist": str(target_slot),
                "signal": False,
                "no_target_day_data": True,
            }
        )
        checks.append(
            {
                "ticker": ticker,
                "side": "LONG",
                "bar_time_ist": str(target_slot),
                "signal": False,
                "no_target_day_data": True,
            }
        )
        return signals, checks

    slot_rows = df_day[pd.to_datetime(df_day["date_ist"]).dt.floor("min") == target_slot]
    if slot_rows.empty:
        last_ts = pd.Timestamp(df_day.iloc[-1]["date_ist"]).floor("min")
        checks.append(
            {
                "ticker": ticker,
                "side": "SHORT",
                "bar_time_ist": str(last_ts),
                "expected_bar_time_ist": str(target_slot),
                "signal": False,
                "stale_data": True,
            }
        )
        checks.append(
            {
                "ticker": ticker,
                "side": "LONG",
                "bar_time_ist": str(last_ts),
                "expected_bar_time_ist": str(target_slot),
                "signal": False,
                "stale_data": True,
            }
        )
        return signals, checks

    # Same parity source used by v3 row-by-row script.
    df_upto_target = df[df["day"] <= target_day].copy()
    all_sigs = _all_day_runner_parity_signals_for_ticker(ticker, df_upto_target)
    if not all_sigs:
        checks.append({"ticker": ticker, "side": "SHORT", "bar_time_ist": str(target_slot), "signal": False})
        checks.append({"ticker": ticker, "side": "LONG", "bar_time_ist": str(target_slot), "signal": False})
        return signals, checks

    slot_map: Dict[pd.Timestamp, List[Any]] = {}
    for s in all_sigs:
        s_bar_ts = _to_ist_ts(getattr(s, "bar_time_ist", pd.NaT))
        if pd.isna(s_bar_ts) or s_bar_ts.date() != target_day:
            continue
        slot = s_bar_ts.floor("min")
        slot_map.setdefault(slot, []).append((s, s_bar_ts))

    found_short = False
    found_long = False

    slot_sigs = slot_map.get(target_slot, [])
    for s, s_bar_ts in slot_sigs:
        side = str(getattr(s, "side", "")).upper()
        if side not in {"SHORT", "LONG"}:
            continue

        short_cap = int(SHORT_CAP_PER_TICKER_PER_DAY)
        long_cap = int(LONG_CAP_PER_TICKER_PER_DAY)
        cap = short_cap if side == "SHORT" else long_cap

        sig_ticker = str(getattr(s, "ticker", ticker)).upper()
        if not allow_signal_today(state, sig_ticker, side, target_day_str, cap):
            continue

        diag = dict(getattr(s, "diagnostics", {}) or {})
        entry_price = _safe_float(getattr(s, "entry_price", np.nan))
        sl_price = _safe_float(getattr(s, "sl_price", np.nan))
        target_price = _safe_float(getattr(s, "target_price", np.nan))
        score = _safe_float(getattr(s, "score", 0.0))
        if not (np.isfinite(entry_price) and np.isfinite(sl_price) and np.isfinite(target_price)):
            continue

        signal = LiveSignal(
            ticker=sig_ticker,
            side=side,
            bar_time_ist=s_bar_ts,
            setup=str(getattr(s, "setup", "")),
            entry_price=float(entry_price),
            sl_price=float(sl_price),
            target_price=float(target_price),
            score=float(score if np.isfinite(score) else 0.0),
            diagnostics=diag,
        )
        mark_signal(state, sig_ticker, side, target_day_str)
        signals.append(signal)

        if side == "SHORT":
            found_short = True
        else:
            found_long = True

        print(
            f"[SIG ] {signal.ticker} {signal.side} {pd.Timestamp(signal.bar_time_ist).strftime('%Y-%m-%d %H:%M')} "
            f"setup={signal.setup} score={signal.score:.3f}",
            flush=True,
        )

    checks.append({"ticker": ticker, "side": "SHORT", "bar_time_ist": str(target_slot), "signal": found_short})
    checks.append({"ticker": ticker, "side": "LONG", "bar_time_ist": str(target_slot), "signal": found_long})
    return signals, checks


def _all_day_runner_parity_signals_for_ticker(ticker: str, df_upto_target: pd.DataFrame):
    """Generate parity signals for the latest day present in `df_upto_target`."""
    if df_upto_target is None or df_upto_target.empty:
        return []

    df = normalize_dates(df_upto_target)
    if df.empty:
        return []

    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return []

    # Keep all available in-session bars for indicator warmup parity.
    df = df[df["date"].apply(in_session)].copy()
    if df.empty:
        return []

    df = df.sort_values("date").reset_index(drop=True)
    df = ref_prepare_indicators(df, default_short_config())

    target_day = df["day"].max()
    df_day = df[df["day"] == target_day].copy().reset_index(drop=True)
    if df_day.empty or len(df_day) < 7:
        return []

    if MAX_BARS_PER_TICKER_TODAY and len(df_day) > int(MAX_BARS_PER_TICKER_TODAY):
        df_day = df_day.iloc[-int(MAX_BARS_PER_TICKER_TODAY):].reset_index(drop=True)

    df_day["AVWAP"] = ref_compute_day_avwap(df_day)

    day_str = str(target_day)
    short_trades = scan_short_one_day(str(ticker).upper(), df_day.copy(), day_str, default_short_config())
    long_trades = scan_long_one_day(str(ticker).upper(), df_day.copy(), day_str, default_long_config())

    signals: List[LiveSignal] = []
    for tr in (short_trades + long_trades):
        diag = {
            "impulse_type": tr.impulse_type,
            "adx": tr.adx_signal,
            "rsi": tr.rsi_signal,
            "stochk": tr.stochk_signal,
            "atr_pct": tr.atr_pct_signal,
        }
        signals.append(
            LiveSignal(
                ticker=tr.ticker,
                side=tr.side,
                bar_time_ist=pd.Timestamp(tr.entry_time_ist),
                setup=tr.setup,
                entry_price=float(tr.entry_price),
                sl_price=float(tr.sl_price),
                target_price=float(tr.target_price),
                score=float(tr.quality_score),
                diagnostics=diag,
            )
        )

    signals.sort(key=lambda x: (pd.Timestamp(x.bar_time_ist), x.side, x.setup))
    return signals

def _read_holidays_safe() -> set:
    try:
        return set(core._read_holidays(core.HOLIDAYS_FILE_DEFAULT))
    except Exception:
        return set()


def is_trading_day_safe(d, holidays: set) -> bool:
    fn = getattr(core, "_is_trading_day", None)
    if fn is None:
        return d.weekday() < 5 and (d not in holidays)
    try:
        return bool(fn(d, holidays))
    except Exception:
        return d.weekday() < 5 and (d not in holidays)


# =============================================================================
# OPTIONAL UPDATER
# =============================================================================
def run_update_15m_once(holidays: set) -> None:
    core.run_mode(
        mode="15min",
        max_workers=4,
        skip_if_fresh=True,
        intraday_ts="end",
        holidays=holidays,
        refresh_tokens=False,
        report_dir=str(REPORTS_DIR),
        print_missing_rows=False,
        print_missing_rows_max=200,
    )


# =============================================================================
# SLOT SCHEDULER
# =============================================================================
def _next_slot_after(now: datetime) -> datetime:
    now = now.astimezone(IST)
    today = now.date()
    start_dt = IST.localize(datetime.combine(today, START_TIME))
    end_dt = IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt

    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // 15) * 15
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot < now:
        slot += timedelta(minutes=15)

    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _sleep_until(dt: datetime) -> None:
    now = now_ist()
    delta = (dt - now).total_seconds()
    if delta > 0:
        time.sleep(delta)


# =============================================================================
# CSV BRIDGE: write signals to live_signals/ for trade executors
# =============================================================================
def _generate_signal_id(ticker: str, side: str, signal_dt: str, setup: str = "") -> str:
    """Deterministic signal ID; include setup to avoid same-bar collisions."""
    raw = f"eqidv1|{ticker.upper()}|{side.upper()}|{signal_dt}|{setup}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _load_existing_ids(csv_path: str) -> set:
    """Read existing signal IDs from a CSV file; return empty set on any read issue."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return set()
    try:
        df_existing = pd.read_csv(
            csv_path,
            usecols=["signal_id"],
            engine="python",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
        )
        return set(df_existing["signal_id"].astype(str))
    except Exception:
        return set()


def _ensure_signal_csv_schema(csv_path: str) -> None:
    """Best-effort in-place schema migration for today's signal CSV."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return
    try:
        df_existing = pd.read_csv(
            csv_path,
            engine="python",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
        )
    except Exception:
        return

    missing = [c for c in SIGNAL_CSV_COLUMNS if c not in df_existing.columns]
    if not missing:
        return

    for c in missing:
        df_existing[c] = ""

    df_existing = df_existing[SIGNAL_CSV_COLUMNS]
    df_existing.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"[CSV ] migrated schema for {csv_path} | added={missing}", flush=True)


def _write_signals_csv(signals_df: pd.DataFrame) -> int:
    """
    Convert analyser signals_df to the CSV format expected by trade executors
    and append new signals to live_signals/signals_YYYY-MM-DD.csv.
    Returns count of new signals written.
    """
    today_str = now_ist().strftime("%Y-%m-%d")
    csv_path = str(LIVE_SIGNAL_DIR / SIGNAL_CSV_PATTERN.format(today_str))
    received_time = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    _ensure_signal_csv_schema(csv_path)

    # Read existing signal IDs from today's CSV to avoid duplicates
    existing_ids: set = _load_existing_ids(csv_path)

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
    written = 0
    scanned = 0 if signals_df is None else int(len(signals_df))

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNAL_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        if signals_df is not None and (not signals_df.empty):
            for _, row in signals_df.iterrows():
                ticker = str(row.get("ticker", "")).upper()
                side = str(row.get("side", "")).upper()
                bar_time = str(row.get("bar_time_ist", ""))
                setup = str(row.get("setup", ""))
                signal_id = _generate_signal_id(ticker, side, bar_time, setup)

                if signal_id in existing_ids:
                    continue

                entry_price = float(row.get("entry_price", 0))
                notional = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
                qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

                # Extract indicator values from diagnostics JSON if available
                atr_pct = 0.0
                rsi_val = 0.0
                adx_val = 0.0
                diag_str = row.get("diagnostics_json", "")
                if diag_str:
                    try:
                        diag = json.loads(diag_str) if isinstance(diag_str, str) else {}
                        atr_val = _safe_float(diag.get("atr", 0))
                        close_val = _safe_float(diag.get("close", entry_price))
                        if np.isfinite(atr_val) and np.isfinite(close_val) and close_val > 0:
                            atr_pct = atr_val / close_val
                        rsi_val = _safe_float(diag.get("rsi", 0))
                        adx_val = _safe_float(diag.get("adx", 0))
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Determine impulse_type from diagnostics
                impulse_type = ""
                if diag_str:
                    try:
                        diag = json.loads(diag_str) if isinstance(diag_str, str) else {}
                        impulse_type = str(diag.get("impulse_type", ""))
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Keep both names for compatibility:
                # - signal_entry_datetime_ist: explicit user-facing label
                # - signal_bar_time_ist: legacy alias
                out_row = {
                    "signal_id": signal_id,
                    "signal_datetime": bar_time,
                    "signal_entry_datetime_ist": bar_time,
                    "signal_bar_time_ist": bar_time,
                    "received_time": received_time,
                    "ticker": ticker,
                    "side": side,
                    "setup": setup,
                    "impulse_type": impulse_type,
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(float(row.get("sl_price", 0)), 2),
                    "target_price": round(float(row.get("target_price", 0)), 2),
                    "quality_score": round(float(row.get("score", 0)), 4),
                    "atr_pct": round(atr_pct, 6),
                    "rsi": round(rsi_val, 2),
                    "adx": round(adx_val, 2),
                    "quantity": qty,
                }

                writer.writerow(out_row)
                existing_ids.add(signal_id)
                written += 1
                print(
                    f"[CSV+] {ticker} {side} {bar_time} setup={setup} "
                    f"entry={entry_price:.2f} sl={float(row.get('sl_price', 0)):.2f} tgt={float(row.get('target_price', 0)):.2f}",
                    flush=True,
                )

    print(f"[CSV ] scanned={scanned} written={written} path={csv_path}", flush=True)

    return written


# =============================================================================
# RUN ONE SCAN
# =============================================================================
def run_one_scan(run_tag: str = "A") -> Tuple[pd.DataFrame, pd.DataFrame]:
    tickers = list_tickers_15m()
    if not tickers:
        return pd.DataFrame(), pd.DataFrame()

    state = _load_state()
    target_slot_ist = _current_15m_slot_start_ist()
    print(f"[SLOT] strict_target_slot={target_slot_ist.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)

    all_checks: List[Dict[str, Any]] = []
    all_signals: List[Dict[str, Any]] = []

    for idx, t in enumerate(tickers, start=1):
        path = os.path.join(DIR_15M, f"{t}{END_15M}")
        try:
            df_raw = read_parquet_tail(path, n=TAIL_ROWS)
            if df_raw is None or df_raw.empty:
                all_checks.append({"ticker": t, "side": "SHORT", "bar_time_ist": pd.NaT, "no_data": True})
                all_checks.append({"ticker": t, "side": "LONG", "bar_time_ist": pd.NaT, "no_data": True})
                continue

            signals, checks_rows = _latest_entry_signals_for_ticker(t, df_raw, state, target_slot_ist)
            all_checks.extend(checks_rows)

            for s in signals:
                all_signals.append({
                    "ticker": s.ticker, "side": s.side, "bar_time_ist": s.bar_time_ist,
                    "setup": s.setup, "entry_price": s.entry_price,
                    "sl_price": s.sl_price, "target_price": s.target_price,
                    "score": s.score,
                    "diagnostics_json": json.dumps(s.diagnostics, default=str),
                })

            if signals:
                print(f"[SCAN] {t} slot_signals={len(signals)}", flush=True)
        except Exception as e:
            all_checks.append({"ticker": t, "side": "SHORT", "bar_time_ist": pd.NaT, "error": str(e)})
            all_checks.append({"ticker": t, "side": "LONG", "bar_time_ist": pd.NaT, "error": str(e)})
            print(f"[ERR ] {t} scan_failed: {e}", flush=True)

        if idx % 100 == 0:
            print(f"  scanned {idx}/{len(tickers)} | signals_so_far={len(all_signals)}", flush=True)

    _save_state(state)

    checks_df = pd.DataFrame(all_checks)
    signals_df = pd.DataFrame(all_signals)

    if USE_TOPN_PER_RUN and (not signals_df.empty):
        keep = []
        for side in ["SHORT", "LONG"]:
            df_side = signals_df[signals_df["side"] == side].copy()
            df_side = df_side.sort_values(["score"], ascending=False).head(int(TOPN_PER_RUN))
            keep.append(df_side)
        signals_df = pd.concat(keep, ignore_index=True) if keep else signals_df

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    day_folder = now_ist().strftime("%Y%m%d")

    out_checks_day = OUT_CHECKS_DIR / day_folder
    out_signals_day = OUT_SIGNALS_DIR / day_folder
    out_checks_day.mkdir(parents=True, exist_ok=True)
    out_signals_day.mkdir(parents=True, exist_ok=True)

    checks_path = out_checks_day / f"checks_{ts}_{run_tag}.parquet"
    signals_path = out_signals_day / f"signals_{ts}_{run_tag}.parquet"

    _require_pyarrow()
    checks_df.to_parquet(checks_path, index=False, engine=PARQUET_ENGINE)
    signals_df.to_parquet(signals_path, index=False, engine=PARQUET_ENGINE)

    print(f"[SAVED] {checks_path}")
    print(f"[SAVED] {signals_path}")

    # Bridge: write signals to CSV for trade executors
    written = _write_signals_csv(signals_df)

    print(f"[RUN ] done | checks={len(checks_df)} rows | signals={len(signals_df)} rows | csv_written={written}", flush=True)

    return checks_df, signals_df


# =============================================================================
# MAIN LOOP
# =============================================================================
def main() -> None:
    print("[LIVE] eqidv1 CSV v2 (15m) | strict current 15m slot only")
    print(f"[INFO] DIR_15M={DIR_15M} | tickers={len(list_tickers_15m())}")
    print(f"[INFO] SHORT: SL={SHORT_STOP_PCT*100:.2f}%, TGT={SHORT_TARGET_PCT*100:.2f}%, ADX>={SHORT_ADX_MIN}, RSI<={SHORT_RSI_MAX}, StochK<={SHORT_STOCHK_MAX}")
    print(f"[INFO] LONG : SL={LONG_STOP_PCT*100:.2f}%, TGT={LONG_TARGET_PCT*100:.2f}%, ADX>={LONG_ADX_MIN}, RSI>={LONG_RSI_MIN}, StochK>={LONG_STOCHK_MIN}")
    print(f"[INFO] Volume filter: {USE_VOLUME_FILTER} (ratio>={VOLUME_MIN_RATIO}, SMA={VOLUME_SMA_PERIOD})")
    print(f"[INFO] ATR% filter: {USE_ATR_PCT_FILTER} (min={ATR_PCT_MIN*100:.2f}%)")
    print(f"[INFO] Close-confirm: {REQUIRE_CLOSE_CONFIRM}")
    print(f"[INFO] UPDATE_15M_BEFORE_CHECK={UPDATE_15M_BEFORE_CHECK}")
    print(f"[INFO] Timing: initial_delay={INITIAL_DELAY_SECONDS}s, scans_per_slot={NUM_SCANS_PER_SLOT}, interval={SCAN_INTERVAL_SECONDS}s")

    holidays = _read_holidays_safe()

    while True:
        now = now_ist()
        if now.time() >= HARD_STOP_TIME:
            print("[STOP] Hard-stop reached for today. Exiting.")
            return

        if not is_trading_day_safe(now.date(), holidays):
            nxt = _next_slot_after(now + timedelta(days=1))
            print(f"[SKIP] Not a trading day ({now.date()}). Sleeping until {nxt}.")
            _sleep_until(nxt)
            continue

        slot = _next_slot_after(now)
        if slot.date() != now.date():
            print(f"[WAIT] Next slot is tomorrow {slot}. Sleeping.")
            _sleep_until(slot)
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}")
            _sleep_until(slot)

        now = now_ist()
        if now.time() > END_TIME:
            nxt = _next_slot_after(now + timedelta(days=1))
            print(f"[DONE] Past END_TIME. Sleeping until {nxt}.")
            _sleep_until(nxt)
            continue

        if UPDATE_15M_BEFORE_CHECK:
            try:
                print(f"[UPD ] Running eqidv1 core 15m update at {now_ist().strftime('%Y-%m-%d %H:%M:%S%z')}")
                run_update_15m_once(holidays)
            except Exception as e:
                print(f"[WARN] Update failed: {e!r}")

        # --- Wait for data fetcher to finish (~1 min after slot boundary) ---
        delay_target = slot + timedelta(seconds=INITIAL_DELAY_SECONDS)
        now = now_ist()
        if now < delay_target:
            wait_secs = (delay_target - now).total_seconds()
            print(f"[WAIT] Delaying {wait_secs:.0f}s for data fetch to complete (until {delay_target.strftime('%H:%M:%S')})")
            time.sleep(max(0, wait_secs))

        # --- Run multiple scans per slot to catch freshly-updated data ---
        scan_labels = [chr(ord("A") + i) for i in range(NUM_SCANS_PER_SLOT)]
        for scan_idx, label in enumerate(scan_labels):
            now = now_ist()
            if now.time() >= HARD_STOP_TIME:
                print("[STOP] Hard-stop reached mid-slot. Exiting.")
                return

            print(f"[RUN ] Slot {slot.strftime('%H:%M')} | scan {label} ({scan_idx+1}/{NUM_SCANS_PER_SLOT}) at {now.strftime('%H:%M:%S')}")
            run_one_scan(run_tag=label)

            # Sleep between scans (skip after the last one)
            if scan_idx < len(scan_labels) - 1:
                time.sleep(float(SCAN_INTERVAL_SECONDS))

        time.sleep(1.0)


if __name__ == "__main__":
    main()
