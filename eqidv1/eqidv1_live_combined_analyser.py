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
LATEST_SLOT_TOLERANCE_SEC = 30  # allow small timestamp drift (seconds) in parquet timestamps

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
    if df.empty or "date" not in df.columns:
        return df
    dt = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)
    df = df.copy()
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

def _latest_entry_signals_for_ticker(
    ticker: str, df_day: pd.DataFrame, state: Dict[str, Any]
) -> Tuple[List[LiveSignal], List[Dict[str, Any]]]:
    """
    LIVE (latest 15m bar only) entry scanner that reuses the SAME v11 refactored
    entry rules used by avwap_combined_runner.py.

    How it works:
    - Prepare indicators + compute intraday AVWAP for today's df_day
    - Run refactored SHORT/LONG entry checks for *any* impulse in the day
      BUT only keep entries whose entry_idx == last bar (latest completed 15m candle).
    - Enforce per-ticker-per-day cap using the existing live state (allow_signal_today()).

    NOTE:
    This intentionally does *not* require "bars_left_after_entry" based on df length,
    because in live we are evaluating the latest candle while future candles don't
    exist yet. Use the time-window itself as your practical "bars left" constraint.
    """
    signals: List[LiveSignal] = []
    checks: List[Dict[str, Any]] = []

    if df_day.empty or len(df_day) < 7:
        return signals, checks

    # Latest completed candle (the only candle we allow entries on in live)
    entry_idx = len(df_day) - 1
    entry_ts = pd.Timestamp(df_day.at[entry_idx, "date"])
    if entry_ts.tzinfo is None:
        entry_ts = entry_ts.tz_localize(IST)
    else:
        entry_ts = entry_ts.tz_convert(IST)

    today_str = str(entry_ts.date())

    # Per-ticker/day cap (existing live behaviour)
    # IMPORTANT: cap is consumed later when signals are actually written (in generator),
    # but the analyser still uses allow_signal_today to avoid flooding checks.
    short_capped = not allow_signal_today(state, ticker, "SHORT", today_str, SHORT_CAP_PER_TICKER_PER_DAY)
    long_capped = not allow_signal_today(state, ticker, "LONG", today_str, LONG_CAP_PER_TICKER_PER_DAY)
    if short_capped and long_capped:
        checks.append(
            {
                "ticker": ticker,
                "bar_time_ist": str(entry_ts),
                "status": "SKIP_CAP",
                "reason": "per_ticker_day_cap_reached_both_sides",
            }
        )
        return signals, checks

    # -------------------------------------------------------------------------
    # Import refactored v11 strategy helpers (same as avwap_combined_runner)
    # -------------------------------------------------------------------------
    try:
        # Preferred: packaged layout
        from avwap_v11_refactored.avwap_common import (
            default_short_config,
            default_long_config,
            prepare_indicators,
            compute_day_avwap,
            in_signal_window,
            entry_buffer,
        )
        from avwap_v11_refactored import avwap_short_strategy as short_mod
        from avwap_v11_refactored import avwap_long_strategy as long_mod
    except Exception:
        # Fallback: local flat files beside this script
        from avwap_common import (
            default_short_config,
            default_long_config,
            prepare_indicators,
            compute_day_avwap,
            in_signal_window,
            entry_buffer,
        )
        import avwap_short_strategy as short_mod  # type: ignore
        import avwap_long_strategy as long_mod    # type: ignore

    cfgS = default_short_config()
    cfgL = default_long_config()

    # LIVE tweak: do not require "future bars" inside df_day
    cfgS.min_bars_left_after_entry = 0
    cfgL.min_bars_left_after_entry = 0

    # Ensure required indicator columns exist (re-uses parquet values where present)
    df_day = df_day.sort_values("date").reset_index(drop=True)
    df_day = prepare_indicators(df_day, cfgS)
    df_day["AVWAP"] = compute_day_avwap(df_day)

    # Helper: strict-ish timestamp equality on 15m grid
    def _ts_same_slot(a: pd.Timestamp, b: pd.Timestamp) -> bool:
        a2 = pd.Timestamp(a)
        b2 = pd.Timestamp(b)
        if a2.tzinfo is None:
            a2 = a2.tz_localize(IST)
        else:
            a2 = a2.tz_convert(IST)
        if b2.tzinfo is None:
            b2 = b2.tz_localize(IST)
        else:
            b2 = b2.tz_convert(IST)
        return a2.floor("min") == b2.floor("min")

    # -------------------------------------------------------------------------
    # SHORT: run the same entry rules, but only accept if entry candle is latest
    # -------------------------------------------------------------------------
    try:
        # scan all impulses i in the day, but only keep setups that trigger on entry_idx
        for i in range(2, entry_idx):
            c1 = df_day.iloc[i]
            ts1 = c1["date"]

            if not in_signal_window(ts1, cfgS):
                continue

            impulse = short_mod.classify_red_impulse(c1, cfgS)
            if impulse == "":
                continue

            # ATR + ATR% filter
            atr1 = float(c1.get("ATR15", np.nan))
            close1 = float(c1.get("close", np.nan))
            if not (np.isfinite(atr1) and atr1 > 0 and np.isfinite(close1) and close1 > 0):
                continue
            if cfgS.use_atr_pct_filter and (atr1 / close1) < cfgS.atr_pct_min:
                continue

            # Volume filter (same helper)
            if not short_mod.volume_filter_pass(c1, cfgS):
                continue

            # Trend filter
            if hasattr(short_mod, "_trend_filter_short") and not short_mod._trend_filter_short(df_day, i, c1, cfgS):
                continue

            # Diagnostics (same scoring as refactored)
            adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
            rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
            k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
            avwap1 = float(c1.get("AVWAP", 0.0)) if np.isfinite(c1.get("AVWAP", np.nan)) else 0.0
            ema20 = float(c1.get("EMA20", 0.0)) if np.isfinite(c1.get("EMA20", np.nan)) else 0.0
            atr_pct = atr1 / close1
            avwap_dist_atr = (avwap1 - close1) / atr1
            ema_gap_atr = (ema20 - close1) / atr1
            quality = short_mod.compute_quality_score_short(adx1, avwap_dist_atr, ema_gap_atr, impulse)

            low1 = float(c1["low"])
            buf1 = entry_buffer(low1, cfgS)

            def _close_confirm_ok(trigger: float) -> bool:
                if not cfgS.require_entry_close_confirm:
                    return True
                cl = float(df_day.at[entry_idx, "close"])
                return np.isfinite(cl) and cl < trigger

            # ---- Setup A: MODERATE impulse ----
            if impulse == "MODERATE":
                # A1: C2 breaks C1 low (C2 == latest candle)
                if i + 1 == entry_idx:
                    low2 = float(df_day.at[entry_idx, "low"])
                    trigger = low1 - buf1
                    if (
                        np.isfinite(low2) and low2 < trigger
                        and _close_confirm_ok(trigger)
                        and short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfgS)
                        and short_mod.avwap_distance_pass(df_day, entry_idx, cfgS)
                    ):
                        entry_price = trigger
                        atr_entry = float(df_day.at[entry_idx, "ATR15"])
                        ok_support, avwap_dist_atr_entry = long_mod.avwap_support_pass(df_day, i, entry_idx, atr_entry, cfgL)
                        if not ok_support:
                            continue

                        sid = _generate_signal_id(ticker, "SHORT", str(entry_ts))
                        signals.append(
                            LiveSignal(
                                ticker=ticker,
                                side="SHORT",
                                bar_time_ist=entry_ts,
                                setup="A_MOD_BREAK_C1_LOW",
                                entry_price=float(entry_price),
                                sl_price=float(entry_price * (1.0 + cfgS.stop_pct)),
                                target_price=float(entry_price * (1.0 - cfgS.target_pct)),
                                score=float(quality),
                                diagnostics={
                                    "impulse_type": impulse,
                                    "adx": adx1,
                                    "rsi": rsi1,
                                    "stochk": k1,
                                    "atr_pct": atr_pct,
                                    "signal_id": sid,
                                },
                            )
                        )
                        checks.append({"ticker": ticker, "side": "SHORT", "setup": "A_MOD_BREAK_C1_LOW", "status": "OK", "bar_time_ist": str(entry_ts)})
                        break  # only one per side per ticker per run

                # A2: small green pullback C2, then break C2 low on C3 (C3 == latest candle)
                if i + 2 == entry_idx:
                    c2 = df_day.iloc[i + 1]
                    c3 = df_day.iloc[i + 2]
                    if float(c2["close"]) > float(c2["open"]):  # green pullback candle
                        rng2 = float(c2["high"]) - float(c2["low"])
                        body2 = abs(float(c2["close"]) - float(c2["open"]))
                        if np.isfinite(rng2) and rng2 > 0 and (body2 / rng2) <= cfgS.pullback_body_frac_max:
                            low2 = float(c2["low"])
                            trigger = low2 - entry_buffer(low2, cfgS)
                            low3 = float(c3["low"])
                            if (
                                np.isfinite(low3) and low3 < trigger
                                and _close_confirm_ok(trigger)
                                and short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfgS)
                                and short_mod.avwap_distance_pass(df_day, entry_idx, cfgS)
                            ):
                                entry_price = trigger
                                sid = _generate_signal_id(ticker, "SHORT", str(entry_ts))
                                signals.append(
                                    LiveSignal(
                                        ticker=ticker,
                                        side="SHORT",
                                        bar_time_ist=entry_ts,
                                        setup="A_PULLBACK_C2_THEN_BREAK_C2_LOW",
                                        entry_price=float(entry_price),
                                        sl_price=float(entry_price * (1.0 + cfgS.stop_pct)),
                                        target_price=float(entry_price * (1.0 - cfgS.target_pct)),
                                        score=float(quality),
                                        diagnostics={
                                            "impulse_type": impulse,
                                            "adx": adx1,
                                            "rsi": rsi1,
                                            "stochk": k1,
                                            "atr_pct": atr_pct,
                                            "signal_id": sid,
                                        },
                                    )
                                )
                                checks.append({"ticker": ticker, "side": "SHORT", "setup": "A_PULLBACK_C2_THEN_BREAK_C2_LOW", "status": "OK", "bar_time_ist": str(entry_ts)})
                                break

            # ---- Setup B: HUGE impulse ----
            if impulse == "HUGE":
                # Same as refactored scan_one_day() HUGE logic, but we only accept
                # an entry if it triggers on the latest candle (entry_idx).
                bounce_end = min(i + 3, len(df_day) - 1)
                bounce = df_day.iloc[i + 1 : bounce_end + 1].copy()
                if bounce.empty:
                    continue

                # Bounce must contain at least one small GREEN candle
                opens = pd.to_numeric(bounce["open"], errors="coerce")
                closes = pd.to_numeric(bounce["close"], errors="coerce")
                bounce_atr = pd.to_numeric(bounce.get("ATR15", atr1), errors="coerce").fillna(atr1)
                bounce_body = (closes - opens).abs()
                bounce_green = closes > opens
                bounce_small = bounce_body <= (cfgS.small_counter_max_atr * bounce_atr)

                if not bool((bounce_green & bounce_small).fillna(False).any()):
                    continue

                # Optional: AVWAP touch-fail evidence during bounce window
                if cfgS.require_avwap_rule and cfgS.avwap_touch:
                    avwaps = pd.to_numeric(bounce["AVWAP"], errors="coerce")
                    highs = pd.to_numeric(bounce["high"], errors="coerce")
                    touch_fail = bool(((highs >= avwaps) & (closes < avwaps)).fillna(False).any())
                    if not touch_fail:
                        continue

                bounce_low = float(pd.to_numeric(bounce["low"], errors="coerce").min())
                if not np.isfinite(bounce_low):
                    continue

                trigger_b = bounce_low - entry_buffer(bounce_low, cfgS)

                # Latest candle must be AFTER bounce_end
                if entry_idx <= bounce_end:
                    continue

                # Respect signal window at entry candle
                tsE = df_day.at[entry_idx, "date"]
                if not in_signal_window(tsE, cfgS):
                    continue

                closeE = float(df_day.at[entry_idx, "close"])
                avwapE = float(df_day.at[entry_idx, "AVWAP"]) if np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
                # If price reclaims AVWAP, the breakdown attempt is invalid (same break condition)
                if np.isfinite(avwapE) and closeE >= avwapE:
                    continue

                lowE = float(df_day.at[entry_idx, "low"])
                if (
                    np.isfinite(lowE) and lowE < trigger_b
                    and _close_confirm_ok(trigger_b)
                    and short_mod.avwap_distance_pass(df_day, entry_idx, cfgS)
                    and short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfgS)
                ):
                    entry_price = trigger_b
                    sid = _generate_signal_id(ticker, "SHORT", str(entry_ts))
                    signals.append(
                        LiveSignal(
                            ticker=ticker,
                            side="SHORT",
                            bar_time_ist=entry_ts,
                            setup="B_HUGE_RED_FAILED_BOUNCE",
                            entry_price=float(entry_price),
                            sl_price=float(entry_price * (1.0 + cfgS.stop_pct)),
                            target_price=float(entry_price * (1.0 - cfgS.target_pct)),
                            score=float(quality),
                            diagnostics={
                                "impulse_type": impulse,
                                "adx": adx1,
                                "rsi": rsi1,
                                "stochk": k1,
                                "atr_pct": atr_pct,
                                "signal_id": sid,
                            },
                        )
                    )
                    checks.append({"ticker": ticker, "side": "SHORT", "setup": "B_HUGE_RED_FAILED_BOUNCE", "status": "OK", "bar_time_ist": str(entry_ts)})
                    break
    except Exception as e:
        checks.append({"ticker": ticker, "side": "SHORT", "status": "ERROR", "reason": f"short_refactored_exception: {e}"})

    # -------------------------------------------------------------------------
    # LONG: same approach for the LONG side
    # -------------------------------------------------------------------------
    try:
        for i in range(2, entry_idx):
            c1 = df_day.iloc[i]
            ts1 = c1["date"]

            if not in_signal_window(ts1, cfgL):
                continue

            impulse = long_mod.classify_green_impulse(c1, cfgL)
            if impulse == "":
                continue

            atr1 = float(c1.get("ATR15", np.nan))
            close1 = float(c1.get("close", np.nan))
            if not (np.isfinite(atr1) and atr1 > 0 and np.isfinite(close1) and close1 > 0):
                continue
            if cfgL.use_atr_pct_filter and (atr1 / close1) < cfgL.atr_pct_min:
                continue

            if not long_mod.volume_filter_pass(c1, cfgL):
                continue

            if hasattr(long_mod, "_trend_filter_long") and not long_mod._trend_filter_long(df_day, i, c1, cfgL):
                continue

            adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
            rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
            k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
            avwap1 = float(c1.get("AVWAP", 0.0)) if np.isfinite(c1.get("AVWAP", np.nan)) else 0.0
            ema20 = float(c1.get("EMA20", 0.0)) if np.isfinite(c1.get("EMA20", np.nan)) else 0.0
            atr_pct = atr1 / close1
            avwap_dist_atr = (close1 - avwap1) / atr1
            ema_gap_atr = (close1 - ema20) / atr1
            quality = long_mod.compute_quality_score_long(adx1, avwap_dist_atr, ema_gap_atr, impulse)

            high1 = float(c1["high"])
            buf1 = entry_buffer(high1, cfgL)

            def _close_confirm_ok(trigger: float) -> bool:
                if not cfgL.require_entry_close_confirm:
                    return True
                cl = float(df_day.at[entry_idx, "close"])
                return np.isfinite(cl) and cl > trigger

            if impulse == "MODERATE":
                # A1: C2 breaks C1 high (C2 == latest candle)
                if i + 1 == entry_idx:
                    highE = float(df_day.at[entry_idx, "high"])
                    trigger = high1 + buf1
                    if (
                        np.isfinite(highE) and highE > trigger
                        and _close_confirm_ok(trigger)
                    ):
                        entry_price = trigger
                        atr_entry = float(df_day.at[entry_idx, "ATR15"])
                        ok_support, avwap_dist_atr_entry = long_mod.avwap_support_pass(df_day, i, entry_idx, atr_entry, cfgL)
                        if not ok_support:
                            continue

                        sid = _generate_signal_id(ticker, "LONG", str(entry_ts))
                        signals.append(
                            LiveSignal(
                                ticker=ticker,
                                side="LONG",
                                bar_time_ist=entry_ts,
                                setup="A_MOD_BREAK_C1_HIGH",
                                entry_price=float(entry_price),
                                sl_price=float(entry_price * (1.0 - cfgL.stop_pct)),
                                target_price=float(entry_price * (1.0 + cfgL.target_pct)),
                                score=float(quality),
                                diagnostics={
                                    "impulse_type": impulse,
                                    "adx": adx1,
                                    "rsi": rsi1,
                                    "stochk": k1,
                                    "atr_pct": atr_pct,
                                    "signal_id": sid,
                                },
                            )
                        )
                        checks.append({"ticker": ticker, "side": "LONG", "setup": "A_MOD_BREAK_C1_HIGH", "status": "OK", "bar_time_ist": str(entry_ts)})
                        break

                # A2: small red pullback then break pullback high (C3 == latest)
                if i + 2 == entry_idx:
                    c2 = df_day.iloc[i + 1]
                    c3 = df_day.iloc[i + 2]
                    if float(c2["close"]) < float(c2["open"]):  # red pullback
                        rng2 = float(c2["high"]) - float(c2["low"])
                        body2 = abs(float(c2["close"]) - float(c2["open"]))
                        if np.isfinite(rng2) and rng2 > 0 and (body2 / rng2) <= cfgL.pullback_body_frac_max:
                            high2 = float(c2["high"])
                            trigger = high2 + entry_buffer(high2, cfgL)
                            high3 = float(c3["high"])
                            if (
                                np.isfinite(high3) and high3 > trigger
                                and _close_confirm_ok(trigger)
                            ):
                                entry_price = trigger
                                sid = _generate_signal_id(ticker, "LONG", str(entry_ts))
                                signals.append(
                                    LiveSignal(
                                        ticker=ticker,
                                        side="LONG",
                                        bar_time_ist=entry_ts,
                                        setup="A_PULLBACK_C2_THEN_BREAK_C2_HIGH",
                                        entry_price=float(entry_price),
                                        sl_price=float(entry_price * (1.0 - cfgL.stop_pct)),
                                        target_price=float(entry_price * (1.0 + cfgL.target_pct)),
                                        score=float(quality),
                                        diagnostics={
                                            "impulse_type": impulse,
                                            "adx": adx1,
                                            "rsi": rsi1,
                                            "stochk": k1,
                                            "atr_pct": atr_pct,
                                            "signal_id": sid,
                                        },
                                    )
                                )
                                checks.append({"ticker": ticker, "side": "LONG", "setup": "A_PULLBACK_C2_THEN_BREAK_C2_HIGH", "status": "OK", "bar_time_ist": str(entry_ts)})
                                break

            if impulse == "HUGE":
                # Same as refactored scan_one_day() HUGE logic, but only accept if
                # breakout triggers on the latest candle (entry_idx).
                pull_end = min(i + 3, len(df_day) - 1)
                pull = df_day.iloc[i + 1 : pull_end + 1].copy()
                if pull.empty:
                    continue

                mid_body = (float(c1["open"]) + float(c1["close"])) / 2.0

                pull_atr = pd.to_numeric(pull.get("ATR15", atr1), errors="coerce").fillna(atr1)
                pull_body = (pd.to_numeric(pull["close"], errors="coerce") - pd.to_numeric(pull["open"], errors="coerce")).abs()
                pull_red = pd.to_numeric(pull["close"], errors="coerce") < pd.to_numeric(pull["open"], errors="coerce")
                pull_small = pull_body <= (cfgL.small_counter_max_atr * pull_atr)

                if not bool((pull_red & pull_small).fillna(False).any()):
                    continue

                lows = pd.to_numeric(pull["low"], errors="coerce")
                closes = pd.to_numeric(pull["close"], errors="coerce")
                avwaps = pd.to_numeric(pull["AVWAP"], errors="coerce")

                hold_mid = bool((lows > mid_body).fillna(False).all())
                hold_avwap = bool((closes > avwaps).fillna(False).all())
                if not (hold_mid or hold_avwap):
                    continue

                pull_high = float(pd.to_numeric(pull["high"], errors="coerce").max())
                if not np.isfinite(pull_high):
                    continue

                trigger = pull_high + entry_buffer(pull_high, cfgL)

                if entry_idx <= pull_end:
                    continue

                tsE = df_day.at[entry_idx, "date"]
                if not in_signal_window(tsE, cfgL):
                    continue

                closeE = float(df_day.at[entry_idx, "close"])
                avwapE = float(df_day.at[entry_idx, "AVWAP"]) if np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
                if np.isfinite(avwapE) and closeE <= avwapE:
                    continue

                highE = float(df_day.at[entry_idx, "high"])
                if np.isfinite(highE) and highE > trigger and _close_confirm_ok(trigger):
                    atr_entry = float(df_day.at[entry_idx, "ATR15"])
                    ok_support, avwap_dist_atr2 = long_mod.avwap_support_pass(df_day, i, entry_idx, atr_entry, cfgL)
                    if not ok_support:
                        continue

                    entry_price = trigger
                    sid = _generate_signal_id(ticker, "LONG", str(entry_ts))
                    signals.append(
                        LiveSignal(
                            ticker=ticker,
                            side="LONG",
                            bar_time_ist=entry_ts,
                            setup="B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK",
                            entry_price=float(entry_price),
                            sl_price=float(entry_price * (1.0 - cfgL.stop_pct)),
                            target_price=float(entry_price * (1.0 + cfgL.target_pct)),
                            score=float(quality),
                            diagnostics={
                                "impulse_type": impulse,
                                "adx": adx1,
                                "rsi": rsi1,
                                "stochk": k1,
                                "atr_pct": atr_pct,
                                "avwap_dist_atr_entry": float(avwap_dist_atr2),
                                "signal_id": sid,
                            },
                        )
                    )
                    checks.append({"ticker": ticker, "side": "LONG", "setup": "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", "status": "OK", "bar_time_ist": str(entry_ts)})
                    break
    except Exception as e:
        checks.append({"ticker": ticker, "side": "LONG", "status": "ERROR", "reason": f"long_refactored_exception: {e}"})

    return signals, checks


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
def _generate_signal_id(ticker: str, side: str, signal_dt: str) -> str:
    """Deterministic signal ID from ticker + side + signal_datetime."""
    raw = f"{ticker}|{side}|{signal_dt}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _write_signals_csv(signals_df: pd.DataFrame) -> int:
    """
    Convert analyser signals_df to the CSV format expected by trade executors
    and append new signals to live_signals/signals_YYYY-MM-DD.csv.
    Returns count of new signals written.
    """
    if signals_df.empty:
        return 0

    today_str = now_ist().strftime("%Y-%m-%d")
    csv_path = str(LIVE_SIGNAL_DIR / SIGNAL_CSV_PATTERN.format(today_str))
    received_time = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    # Read existing signal IDs from today's CSV to avoid duplicates
    existing_ids: set = set()
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            df_existing = pd.read_csv(csv_path, usecols=["signal_id"], engine="python",
                                      quotechar='"', quoting=csv.QUOTE_ALL,
                                      on_bad_lines="warn")
            existing_ids = set(df_existing["signal_id"].astype(str))
        except Exception:
            existing_ids = set()

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
    written = 0

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNAL_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        for _, row in signals_df.iterrows():
            ticker = str(row.get("ticker", ""))
            side = str(row.get("side", ""))
            bar_time = str(row.get("bar_time_ist", ""))
            signal_id = _generate_signal_id(ticker, side, bar_time)

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

            writer.writerow({
                "signal_id": signal_id,
                "signal_datetime": bar_time,
                "received_time": received_time,
                "ticker": ticker,
                "side": side,
                "setup": str(row.get("setup", "")),
                "impulse_type": impulse_type,
                "entry_price": round(entry_price, 2),
                "stop_price": round(float(row.get("sl_price", 0)), 2),
                "target_price": round(float(row.get("target_price", 0)), 2),
                "quality_score": round(float(row.get("score", 0)), 4),
                "atr_pct": round(atr_pct, 6),
                "rsi": round(rsi_val, 2),
                "adx": round(adx_val, 2),
                "quantity": qty,
            })
            existing_ids.add(signal_id)
            written += 1

    if written > 0:
        print(f"[CSV ] Wrote {written} new signal(s) to {csv_path}")

    return written


# =============================================================================
# RUN ONE SCAN
# =============================================================================
def run_one_scan(run_tag: str = "A") -> Tuple[pd.DataFrame, pd.DataFrame]:
    tickers = list_tickers_15m()
    if not tickers:
        return pd.DataFrame(), pd.DataFrame()

    state = _load_state()

    # Target slot end to scan (latest completed 15m candle end)
    target_slot_end = latest_completed_15m_slot_end(LATEST_SLOT_BUFFER_SEC) if LATEST_SLOT_ONLY else None
    all_checks: List[Dict[str, Any]] = []
    all_signals: List[Dict[str, Any]] = []

    for idx, t in enumerate(tickers, start=1):
        path = os.path.join(DIR_15M, f"{t}{END_15M}")
        try:
            df_raw = read_parquet_tail(path, n=TAIL_ROWS)
            df_raw = normalize_dates(df_raw)
            df_day = _prepare_today_df(df_raw)
            if df_day.empty:
                all_checks.append({"ticker": t, "side": "SHORT", "bar_time_ist": pd.NaT, "no_data": True})
                all_checks.append({"ticker": t, "side": "LONG", "bar_time_ist": pd.NaT, "no_data": True})
                continue

            # Keep only last N bars for speed (still enough for v11 impulse checks)
            if MAX_BARS_PER_TICKER_TODAY and len(df_day) > int(MAX_BARS_PER_TICKER_TODAY):
                df_day = df_day.iloc[-int(MAX_BARS_PER_TICKER_TODAY):].reset_index(drop=True)

            # If enabled: only scan when today's last bar matches the latest completed 15m slot
            if LATEST_SLOT_ONLY and (target_slot_end is not None):
                last_ts = pd.Timestamp(df_day["date"].iloc[-1])
                if last_ts.tzinfo is None:
                    last_ts = last_ts.tz_localize(IST)
                else:
                    last_ts = last_ts.tz_convert(IST)

                expected_ts = pd.Timestamp(target_slot_end).tz_convert(IST)
                drift = abs((last_ts - expected_ts).total_seconds())
                if drift > float(LATEST_SLOT_TOLERANCE_SEC):
                    all_checks.append({
                        "ticker": t, "side": "SHORT", "bar_time_ist": last_ts,
                        "expected_bar_time_ist": expected_ts, "stale_data": True
                    })
                    all_checks.append({
                        "ticker": t, "side": "LONG", "bar_time_ist": last_ts,
                        "expected_bar_time_ist": expected_ts, "stale_data": True
                    })
                    continue

            signals, checks_rows = _latest_entry_signals_for_ticker(t, df_day, state)
            all_checks.extend(checks_rows)

            for s in signals:
                all_signals.append({
                    "ticker": s.ticker, "side": s.side, "bar_time_ist": s.bar_time_ist,
                    "setup": s.setup, "entry_price": s.entry_price,
                    "sl_price": s.sl_price, "target_price": s.target_price,
                    "score": s.score,
                    "diagnostics_json": json.dumps(s.diagnostics, default=str),
                })
        except Exception as e:
            all_checks.append({"ticker": t, "side": "SHORT", "bar_time_ist": pd.NaT, "error": str(e)})
            all_checks.append({"ticker": t, "side": "LONG", "bar_time_ist": pd.NaT, "error": str(e)})

        if idx % 100 == 0:
            print(f"  scanned {idx}/{len(tickers)} | signals_so_far={len(all_signals)}")

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
    _write_signals_csv(signals_df)

    print(f"[RUN ] done | checks={len(checks_df)} rows | signals={len(signals_df)} rows")

    return checks_df, signals_df


# =============================================================================
# MAIN LOOP
# =============================================================================
def main() -> None:
    print("[LIVE] EQIDV1 AVWAP v11 combined LIVE scanner (15m)")
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