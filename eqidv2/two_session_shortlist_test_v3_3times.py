# -*- coding: utf-8 -*-
"""
two_session_shortlist_test_v3_3times.py
Three-Window Intraday Stock Shortlisting Framework -- Manual Test Runner

Implements: shortlist v3_3times

Changes from v2:
- Three trade windows instead of two (1.5 hr each, no W4)
- W1 MORNING  : obs 09:15-09:30 (15 min, 3 bars)  ->  trade 09:30-11:00
- W2 MID      : obs 10:30-11:15 (45 min, 9 bars)  ->  trade 11:15-12:45  (NEW)
- W3 AFTERNOON: obs 12:30-13:00 (30 min, 6 bars)  ->  trade 13:00-14:30
- MID session thresholds midway between morning and afternoon
- Per-window shortlist isolation (stocks must re-qualify each window)

Run anytime during market hours to get shortlisted stocks.
Session is auto-detected from current IST time, or override with --session.

Usage:
    python two_session_shortlist_test_v3_3times.py
    python two_session_shortlist_test_v3_3times.py --session morning
    python two_session_shortlist_test_v3_3times.py --session mid
    python two_session_shortlist_test_v3_3times.py --session afternoon
    python two_session_shortlist_test_v3_3times.py --session all
    python two_session_shortlist_test_v3_3times.py --date 2026-03-13
"""

from __future__ import annotations

import argparse
import sys
import warnings
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent

_RUNTIME_ROOT    = Path(r"C:\TradingData\eqidv2")
_RUNTIME_5M_LIVE = _RUNTIME_ROOT / "stocks_indicators_5min_eq_live"
_RUNTIME_DY_DIR  = _RUNTIME_ROOT / "stocks_indicators_daily_eq"

DATA_5M_DIR    = (_RUNTIME_5M_LIVE if _RUNTIME_5M_LIVE.is_dir()
                  else SCRIPT_DIR / "stocks_indicators_5min_eq")
DATA_DAILY_DIR = _RUNTIME_DY_DIR if _RUNTIME_DY_DIR.is_dir() else SCRIPT_DIR / "stocks_indicators_daily_eq"
SHORTLIST_EXPORT_DIR = SCRIPT_DIR

IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Candle-end timestamps for each obs window
# W1 MORNING  09:15-09:30 -> 09:20, 09:25, 09:30
MORNING_OBS_TIMES   = {time(9, 20), time(9, 25), time(9, 30)}
# W2 MID      10:30-11:15 -> 10:35, 10:40, 10:45, 10:50, 10:55, 11:00, 11:05, 11:10, 11:15  (45 min, 9 bars)
MID_OBS_TIMES       = {time(10, 35), time(10, 40), time(10, 45), time(10, 50), time(10, 55),
                       time(11,  0), time(11,  5), time(11, 10), time(11, 15)}
# W3 AFTERNOON 12:30-13:00 -> 12:35, 12:40, 12:45, 12:50, 12:55, 13:00  (30 min, 6 bars)
AFTERNOON_OBS_TIMES = {time(12, 35), time(12, 40), time(12, 45),
                       time(12, 50), time(12, 55), time(13,  0)}

# ── MORNING hard filters ──────────────────────────────────────────────────────
MORNING_RVOL_MIN        = 0.70
MORNING_ORP_ATR_MIN     = 0.07
MORNING_CLV_MIN         = 0.55
MORNING_TV_MIN_LONG     = 2_000_000    # Rs.20L
MORNING_TV_MIN_SHORT    = 2_500_000    # Rs.25L
MORNING_TV_MIN          = MORNING_TV_MIN_LONG

# ── MID hard filters (midway between morning and afternoon) ───────────────────
MID_RVOL_MIN            = 0.65
MID_ORP_ATR_MIN         = 0.05
MID_CLV_MIN             = 0.50
MID_TV_MIN_LONG         = 1_500_000    # Rs.15L
MID_TV_MIN_SHORT        = 2_000_000    # Rs.20L
MID_TV_MIN              = MID_TV_MIN_LONG

# ── AFTERNOON hard filters ────────────────────────────────────────────────────
AFTERNOON_RVOL_MIN      = 0.60
AFTERNOON_ORP_ATR_MIN   = 0.04
AFTERNOON_CLV_MIN       = 0.45
AFTERNOON_TV_MIN_LONG   = 1_500_000    # Rs.15L
AFTERNOON_TV_MIN_SHORT  = 2_000_000    # Rs.20L
AFTERNOON_TV_MIN        = AFTERNOON_TV_MIN_LONG

# ── Shared indicator lookbacks ────────────────────────────────────────────────
RVOL_ACCEL_LOOKBACK       = 3
GAP_FILL_DETECT_PCT       = 0.50
GAP_THRESHOLD_PCT         = 0.2
RVOL_LOOKBACK_SESSIONS    = 20
TURNOVER_LOOKBACK_SESSIONS = 20
MARKET_PROXY_TICKER       = "NIFTYBEES"

# ── Top-N caps (None = no cap) ────────────────────────────────────────────────
MORNING_TOP_N:   Optional[int] = None
MID_TOP_N:       Optional[int] = None
AFTERNOON_TOP_N: Optional[int] = None

# ── Scoring weights ───────────────────────────────────────────────────────────
MORNING_WEIGHTS = dict(
    rvol=0.10,
    tv_ratio=0.09,
    orp_atr=0.13,
    clv=0.14,
    gap_hold=0.07,
    rs=0.15,
    vwap_edge=0.12,
    ema20_edge=0.05,
    body_eff=0.09,
    rvol_accel=0.06,   # reserved — not yet in score calc
)
MID_WEIGHTS = dict(
    rvol=0.12,
    tv_ratio=0.11,
    orp_atr=0.11,
    clv=0.13,
    rs=0.17,
    vwap_edge=0.15,
    ema20_edge=0.07,
    body_eff=0.09,
    rvol_accel=0.05,   # reserved — not yet in score calc
)
AFTERNOON_WEIGHTS = dict(
    rvol=0.10,
    tv_ratio=0.10,
    orp_atr=0.10,
    clv=0.12,
    rs=0.16,
    vwap_edge=0.14,
    ema20_edge=0.06,
    body_eff=0.07,
    rvol_accel=0.05,   # reserved
    morning_rs=0.10,   # reserved
)

MORNING_EXHAUSTION_PENALTY   = 0.10
MID_EXHAUSTION_PENALTY       = 0.11
AFTERNOON_EXHAUSTION_PENALTY = 0.12


# ─────────────────────────────────────────────────────────────────────────────
# STOCK UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────

def load_universe() -> List[str]:
    sys.path.insert(0, str(SCRIPT_DIR))
    from filtered_stocks_MIS import selected_stocks
    return sorted(selected_stocks)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def _load_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        df["date"] = pd.to_datetime(df["date"], utc=False)
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize(IST)
        else:
            df["date"] = df["date"].dt.tz_convert(IST)
        return df.sort_values("date").reset_index(drop=True)
    except Exception:
        return None


def load_5min(ticker: str) -> Optional[pd.DataFrame]:
    return _load_parquet(DATA_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet")


def load_daily(ticker: str) -> Optional[pd.DataFrame]:
    return _load_parquet(DATA_DAILY_DIR / f"{ticker}_stocks_indicators_daily.parquet")


def build_atr_cache(tickers: List[str]) -> Dict[str, float]:
    cache: Dict[str, float] = {}
    for ticker in tickers:
        df = load_daily(ticker)
        if df is None or df.empty or "ATR" not in df.columns:
            continue
        series = df["ATR"].dropna()
        if series.empty:
            continue
        val = float(series.iloc[-1])
        if val > 0:
            cache[ticker] = val
    return cache


_MARKET_5M_CACHE: Optional[pd.DataFrame] = None


def load_market_5min() -> Optional[pd.DataFrame]:
    global _MARKET_5M_CACHE
    if _MARKET_5M_CACHE is None:
        _MARKET_5M_CACHE = load_5min(MARKET_PROXY_TICKER)
        if _MARKET_5M_CACHE is not None:
            _MARKET_5M_CACHE = _enrich_time_date(_MARKET_5M_CACHE)
    return _MARKET_5M_CACHE


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _pct_rank(pool: List[float], target: float) -> float:
    if not pool:
        return 0.5
    arr = np.array(pool, dtype=float)
    return float(np.mean(arr <= target))


def _enrich_time_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_time"] = df["date"].dt.time
    df["_date"] = df["date"].dt.date
    return df


def _prev_close(df5: pd.DataFrame, today) -> Optional[float]:
    past = df5[df5["_date"] < today]
    if not past.empty:
        return float(past.iloc[-1]["close"])
    today_rows = df5[df5["_date"] == today]
    if not today_rows.empty and "Prev_Day_Close" in today_rows.columns:
        val = today_rows["Prev_Day_Close"].dropna()
        if not val.empty:
            return float(val.iloc[0])
    return None


def _rvol(df5: pd.DataFrame, today, obs_times: set) -> float:
    today_w = df5[(df5["_date"] == today) & (df5["_time"].isin(obs_times))]
    today_vol = today_w["volume"].sum()
    if today_vol == 0:
        return 0.0
    past = df5[df5["_date"] < today]
    past_dates = sorted(past["_date"].unique())[-RVOL_LOOKBACK_SESSIONS:]
    if not past_dates:
        return 1.0
    hist = [
        past[(past["_date"] == d) & (past["_time"].isin(obs_times))]["volume"].sum()
        for d in past_dates
    ]
    hist = [v for v in hist if v > 0]
    if not hist:
        return 1.0
    return float(today_vol / np.mean(hist))


def _turnover_ratio(df5: pd.DataFrame, today, obs_times: set) -> float:
    today_w = df5[(df5["_date"] == today) & (df5["_time"].isin(obs_times))]
    today_tv = float((today_w["close"] * today_w["volume"]).sum())
    if today_tv <= 0:
        return 0.0
    past = df5[df5["_date"] < today]
    past_dates = sorted(past["_date"].unique())[-TURNOVER_LOOKBACK_SESSIONS:]
    if not past_dates:
        return 1.0
    hist = [
        float((past[(past["_date"] == d) & (past["_time"].isin(obs_times))]["close"]
               * past[(past["_date"] == d) & (past["_time"].isin(obs_times))]["volume"]).sum())
        for d in past_dates
    ]
    hist = [v for v in hist if v > 0]
    if not hist:
        return 1.0
    return float(today_tv / np.mean(hist))


def _market_window_return(today, obs_times: set) -> float:
    market_df = load_market_5min()
    if market_df is None or market_df.empty:
        return 0.0
    w = market_df[(market_df["_date"] == today) & (market_df["_time"].isin(obs_times))]
    if w.empty:
        return 0.0
    w = w.sort_values("_time")
    first_open = float(w.iloc[0]["open"])
    last_close = float(w.iloc[-1]["close"])
    if first_open <= 0:
        return 0.0
    return float(((last_close - first_open) / first_open) * 100.0)


def _safe_num(value, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if np.isfinite(out):
        return out
    return default


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, value)))


# ─────────────────────────────────────────────────────────────────────────────
# MORNING FACTOR COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def morning_factors(ticker: str, df5: pd.DataFrame, today, atr14: float) -> Optional[dict]:
    w = df5[(df5["_date"] == today) & (df5["_time"].isin(MORNING_OBS_TIMES))]
    if w.empty:
        return None
    w = w.sort_values("_time")
    h = float(w["high"].max())
    l = float(w["low"].min())
    if h <= l:
        return None

    close_end  = float(w.iloc[-1]["close"])
    total_vol  = float(w["volume"].sum())
    tv         = float((w["close"] * w["volume"]).sum())
    rvol       = _rvol(df5, today, MORNING_OBS_TIMES)
    tv_ratio   = _turnover_ratio(df5, today, MORNING_OBS_TIMES)
    orp_atr    = (h - l) / atr14
    clv_long   = (close_end - l) / (h - l)
    clv_short  = (h - close_end) / (h - l)

    prev_close = _prev_close(df5, today)
    if prev_close is None or prev_close <= 0:
        return None

    first_open = float(w.iloc[0]["open"])
    gap_pct    = (first_open - prev_close) / prev_close * 100
    market_ret = _market_window_return(today, MORNING_OBS_TIMES)
    stock_ret  = ((close_end - first_open) / first_open) * 100 if first_open > 0 else 0.0
    rs_long    = stock_ret - market_ret
    rs_short   = -rs_long

    last_row   = w.iloc[-1]
    vwap_last  = _safe_num(last_row.get("VWAP", np.nan), np.nan)
    ema20_last = _safe_num(last_row.get("EMA_20", np.nan), np.nan)
    upper_band = _safe_num(last_row.get("Upper_Band", np.nan), np.nan)
    lower_band = _safe_num(last_row.get("Lower_Band", np.nan), np.nan)

    vwap_edge_long  = _clip(((close_end - vwap_last) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    vwap_edge_short = _clip(((vwap_last - close_end) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    ema20_edge_long  = _clip(((close_end - ema20_last) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    ema20_edge_short = _clip(((ema20_last - close_end) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    body_eff_long  = _clip(max(close_end - first_open, 0.0) / (h - l), 0.0, 1.0)
    body_eff_short = _clip(max(first_open - close_end, 0.0) / (h - l), 0.0, 1.0)

    gap_hold_long = gap_hold_short = 0
    if gap_pct > GAP_THRESHOLD_PCT:
        if l > prev_close and clv_long >= 0.60:
            gap_hold_long = 1
    elif gap_pct < -GAP_THRESHOLD_PCT:
        if h < prev_close and clv_short >= 0.60:
            gap_hold_short = 1
    else:
        if clv_long  >= 0.65 and orp_atr >= MORNING_ORP_ATR_MIN:
            gap_hold_long = 1
        if clv_short >= 0.65 and orp_atr >= MORNING_ORP_ATR_MIN:
            gap_hold_short = 1

    exhaust_long = int(
        (rvol >= 3.0 and clv_long >= 0.92 and vwap_edge_long >= 0.80)
        or (np.isfinite(upper_band) and close_end >= upper_band and rvol >= 2.0)
    )
    exhaust_short = int(
        (rvol >= 3.0 and clv_short >= 0.92 and vwap_edge_short >= 0.80)
        or (np.isfinite(lower_band) and close_end <= lower_band and rvol >= 2.0)
    )

    return dict(
        ticker=ticker,
        rvol=rvol, orp_atr=orp_atr,
        clv_long=clv_long, clv_short=clv_short,
        gap_hold_long=gap_hold_long, gap_hold_short=gap_hold_short,
        tv_ratio=tv_ratio,
        rs_long=rs_long, rs_short=rs_short,
        vwap_edge_long=vwap_edge_long, vwap_edge_short=vwap_edge_short,
        ema20_edge_long=ema20_edge_long, ema20_edge_short=ema20_edge_short,
        body_eff_long=body_eff_long, body_eff_short=body_eff_short,
        exhaust_long=exhaust_long, exhaust_short=exhaust_short,
        tv=tv, total_vol=total_vol,
        h15=h, l15=l, close_end=close_end,
        prev_close=prev_close, gap_pct=gap_pct, atr14=atr14,
    )


def _morning_pass_long(f: dict) -> bool:
    return (
        f["rvol"]     >= MORNING_RVOL_MIN and
        f["orp_atr"]  >= MORNING_ORP_ATR_MIN and
        f["clv_long"] >= MORNING_CLV_MIN and
        f["tv"]       >= MORNING_TV_MIN_LONG
    )


def _morning_pass_short(f: dict) -> bool:
    return (
        f["rvol"]      >= MORNING_RVOL_MIN and
        f["orp_atr"]   >= MORNING_ORP_ATR_MIN and
        f["clv_short"] >= MORNING_CLV_MIN and
        f["tv"]        >= MORNING_TV_MIN_SHORT
    )


def _score_morning_long(f: dict, pool: List[dict]) -> float:
    w = MORNING_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]           for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]       for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]        for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_long"]       for x in pool], f["clv_long"])
        + w["gap_hold"]  * _pct_rank([x["gap_hold_long"]  for x in pool], f["gap_hold_long"])
        + w["rs"]        * _pct_rank([x["rs_long"]        for x in pool], f["rs_long"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_long"] for x in pool], f["vwap_edge_long"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_long"]for x in pool], f["ema20_edge_long"])
        + w["body_eff"]  * _pct_rank([x["body_eff_long"]  for x in pool], f["body_eff_long"])
    )
    return _clip(score - MORNING_EXHAUSTION_PENALTY * float(f.get("exhaust_long", 0.0)), 0.0, 1.0)


def _score_morning_short(f: dict, pool: List[dict]) -> float:
    w = MORNING_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]            for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]        for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]         for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_short"]       for x in pool], f["clv_short"])
        + w["gap_hold"]  * _pct_rank([x["gap_hold_short"]  for x in pool], f["gap_hold_short"])
        + w["rs"]        * _pct_rank([x["rs_short"]        for x in pool], f["rs_short"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_short"] for x in pool], f["vwap_edge_short"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_short"]for x in pool], f["ema20_edge_short"])
        + w["body_eff"]  * _pct_rank([x["body_eff_short"]  for x in pool], f["body_eff_short"])
    )
    return _clip(score - MORNING_EXHAUSTION_PENALTY * float(f.get("exhaust_short", 0.0)), 0.0, 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# MID FACTOR COMPUTATION  (W2: obs 11:00-11:15, trade 11:15-12:45)
# ─────────────────────────────────────────────────────────────────────────────

def mid_factors(ticker: str, df5: pd.DataFrame, today, atr14: float) -> Optional[dict]:
    w = df5[(df5["_date"] == today) & (df5["_time"].isin(MID_OBS_TIMES))]
    if w.empty:
        return None
    w = w.sort_values("_time")
    h = float(w["high"].max())
    l = float(w["low"].min())
    if h <= l:
        return None

    close_end  = float(w.iloc[-1]["close"])
    tv         = float((w["close"] * w["volume"]).sum())
    rvol       = _rvol(df5, today, MID_OBS_TIMES)
    tv_ratio   = _turnover_ratio(df5, today, MID_OBS_TIMES)
    orp_atr    = (h - l) / atr14
    clv_long   = (close_end - l) / (h - l)
    clv_short  = (h - close_end) / (h - l)
    first_open = float(w.iloc[0]["open"])
    market_ret = _market_window_return(today, MID_OBS_TIMES)
    stock_ret  = ((close_end - first_open) / first_open) * 100 if first_open > 0 else 0.0
    rs_long    = stock_ret - market_ret
    rs_short   = -rs_long

    last_row   = w.iloc[-1]
    vwap_last  = _safe_num(last_row.get("VWAP", np.nan), np.nan)
    ema20_last = _safe_num(last_row.get("EMA_20", np.nan), np.nan)
    upper_band = _safe_num(last_row.get("Upper_Band", np.nan), np.nan)
    lower_band = _safe_num(last_row.get("Lower_Band", np.nan), np.nan)

    vwap_edge_long   = _clip(((close_end - vwap_last) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    vwap_edge_short  = _clip(((vwap_last - close_end) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    ema20_edge_long  = _clip(((close_end - ema20_last) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    ema20_edge_short = _clip(((ema20_last - close_end) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    body_eff_long    = _clip(max(close_end - first_open, 0.0) / (h - l), 0.0, 1.0)
    body_eff_short   = _clip(max(first_open - close_end, 0.0) / (h - l), 0.0, 1.0)

    # Mid exhaustion thresholds: between morning (3.0/0.80) and afternoon (2.6/0.70)
    exhaust_long = int(
        (rvol >= 2.8 and clv_long >= 0.92 and vwap_edge_long >= 0.75)
        or (np.isfinite(upper_band) and close_end >= upper_band and rvol >= 1.9)
    )
    exhaust_short = int(
        (rvol >= 2.8 and clv_short >= 0.92 and vwap_edge_short >= 0.75)
        or (np.isfinite(lower_band) and close_end <= lower_band and rvol >= 1.9)
    )

    return dict(
        ticker=ticker,
        rvol=rvol, orp_atr=orp_atr,
        clv_long=clv_long, clv_short=clv_short,
        tv_ratio=tv_ratio,
        rs_long=rs_long, rs_short=rs_short,
        vwap_edge_long=vwap_edge_long, vwap_edge_short=vwap_edge_short,
        ema20_edge_long=ema20_edge_long, ema20_edge_short=ema20_edge_short,
        body_eff_long=body_eff_long, body_eff_short=body_eff_short,
        exhaust_long=exhaust_long, exhaust_short=exhaust_short,
        tv=tv,
        h15=h, l15=l, close_end=close_end,
        atr14=atr14,
    )


def _mid_pass_long(f: dict) -> bool:
    return (
        f["rvol"]     >= MID_RVOL_MIN and
        f["orp_atr"]  >= MID_ORP_ATR_MIN and
        f["clv_long"] >= MID_CLV_MIN and
        f["tv"]       >= MID_TV_MIN_LONG
    )


def _mid_pass_short(f: dict) -> bool:
    return (
        f["rvol"]      >= MID_RVOL_MIN and
        f["orp_atr"]   >= MID_ORP_ATR_MIN and
        f["clv_short"] >= MID_CLV_MIN and
        f["tv"]        >= MID_TV_MIN_SHORT
    )


def _score_mid_long(f: dict, pool: List[dict]) -> float:
    w = MID_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]           for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]       for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]        for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_long"]       for x in pool], f["clv_long"])
        + w["rs"]        * _pct_rank([x["rs_long"]        for x in pool], f["rs_long"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_long"] for x in pool], f["vwap_edge_long"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_long"]for x in pool], f["ema20_edge_long"])
        + w["body_eff"]  * _pct_rank([x["body_eff_long"]  for x in pool], f["body_eff_long"])
    )
    return _clip(score - MID_EXHAUSTION_PENALTY * float(f.get("exhaust_long", 0.0)), 0.0, 1.0)


def _score_mid_short(f: dict, pool: List[dict]) -> float:
    w = MID_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]            for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]        for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]         for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_short"]       for x in pool], f["clv_short"])
        + w["rs"]        * _pct_rank([x["rs_short"]        for x in pool], f["rs_short"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_short"] for x in pool], f["vwap_edge_short"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_short"]for x in pool], f["ema20_edge_short"])
        + w["body_eff"]  * _pct_rank([x["body_eff_short"]  for x in pool], f["body_eff_short"])
    )
    return _clip(score - MID_EXHAUSTION_PENALTY * float(f.get("exhaust_short", 0.0)), 0.0, 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# AFTERNOON FACTOR COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def afternoon_factors(ticker: str, df5: pd.DataFrame, today, atr14: float) -> Optional[dict]:
    w = df5[(df5["_date"] == today) & (df5["_time"].isin(AFTERNOON_OBS_TIMES))]
    if w.empty:
        return None
    w = w.sort_values("_time")
    h = float(w["high"].max())
    l = float(w["low"].min())
    if h <= l:
        return None

    close_end  = float(w.iloc[-1]["close"])
    tv         = float((w["close"] * w["volume"]).sum())
    rvol       = _rvol(df5, today, AFTERNOON_OBS_TIMES)
    tv_ratio   = _turnover_ratio(df5, today, AFTERNOON_OBS_TIMES)
    orp_atr    = (h - l) / atr14
    clv_long   = (close_end - l) / (h - l)
    clv_short  = (h - close_end) / (h - l)
    first_open = float(w.iloc[0]["open"])
    market_ret = _market_window_return(today, AFTERNOON_OBS_TIMES)
    stock_ret  = ((close_end - first_open) / first_open) * 100 if first_open > 0 else 0.0
    rs_long    = stock_ret - market_ret
    rs_short   = -rs_long

    last_row   = w.iloc[-1]
    vwap_last  = _safe_num(last_row.get("VWAP", np.nan), np.nan)
    ema20_last = _safe_num(last_row.get("EMA_20", np.nan), np.nan)
    upper_band = _safe_num(last_row.get("Upper_Band", np.nan), np.nan)
    lower_band = _safe_num(last_row.get("Lower_Band", np.nan), np.nan)

    vwap_edge_long   = _clip(((close_end - vwap_last) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    vwap_edge_short  = _clip(((vwap_last - close_end) / atr14) if np.isfinite(vwap_last) else 0.0, -2.0, 2.0)
    ema20_edge_long  = _clip(((close_end - ema20_last) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    ema20_edge_short = _clip(((ema20_last - close_end) / atr14) if np.isfinite(ema20_last) else 0.0, -2.0, 2.0)
    body_eff_long    = _clip(max(close_end - first_open, 0.0) / (h - l), 0.0, 1.0)
    body_eff_short   = _clip(max(first_open - close_end, 0.0) / (h - l), 0.0, 1.0)

    exhaust_long = int(
        (rvol >= 2.6 and clv_long >= 0.92 and vwap_edge_long >= 0.70)
        or (np.isfinite(upper_band) and close_end >= upper_band and rvol >= 1.8)
    )
    exhaust_short = int(
        (rvol >= 2.6 and clv_short >= 0.92 and vwap_edge_short >= 0.70)
        or (np.isfinite(lower_band) and close_end <= lower_band and rvol >= 1.8)
    )

    return dict(
        ticker=ticker,
        rvol=rvol, orp_atr=orp_atr,
        clv_long=clv_long, clv_short=clv_short,
        tv_ratio=tv_ratio,
        rs_long=rs_long, rs_short=rs_short,
        vwap_edge_long=vwap_edge_long, vwap_edge_short=vwap_edge_short,
        ema20_edge_long=ema20_edge_long, ema20_edge_short=ema20_edge_short,
        body_eff_long=body_eff_long, body_eff_short=body_eff_short,
        exhaust_long=exhaust_long, exhaust_short=exhaust_short,
        tv=tv,
        h15=h, l15=l, close_end=close_end,
        atr14=atr14,
    )


def _afternoon_pass_long(f: dict) -> bool:
    return (
        f["rvol"]     >= AFTERNOON_RVOL_MIN and
        f["orp_atr"]  >= AFTERNOON_ORP_ATR_MIN and
        f["clv_long"] >= AFTERNOON_CLV_MIN and
        f["tv"]       >= AFTERNOON_TV_MIN_LONG
    )


def _afternoon_pass_short(f: dict) -> bool:
    return (
        f["rvol"]      >= AFTERNOON_RVOL_MIN and
        f["orp_atr"]   >= AFTERNOON_ORP_ATR_MIN and
        f["clv_short"] >= AFTERNOON_CLV_MIN and
        f["tv"]        >= AFTERNOON_TV_MIN_SHORT
    )


def _score_afternoon_long(f: dict, pool: List[dict]) -> float:
    w = AFTERNOON_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]           for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]       for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]        for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_long"]       for x in pool], f["clv_long"])
        + w["rs"]        * _pct_rank([x["rs_long"]        for x in pool], f["rs_long"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_long"] for x in pool], f["vwap_edge_long"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_long"]for x in pool], f["ema20_edge_long"])
        + w["body_eff"]  * _pct_rank([x["body_eff_long"]  for x in pool], f["body_eff_long"])
    )
    return _clip(score - AFTERNOON_EXHAUSTION_PENALTY * float(f.get("exhaust_long", 0.0)), 0.0, 1.0)


def _score_afternoon_short(f: dict, pool: List[dict]) -> float:
    w = AFTERNOON_WEIGHTS
    score = (
        w["rvol"]      * _pct_rank([x["rvol"]            for x in pool], f["rvol"])
        + w["tv_ratio"]  * _pct_rank([x["tv_ratio"]        for x in pool], f["tv_ratio"])
        + w["orp_atr"]   * _pct_rank([x["orp_atr"]         for x in pool], f["orp_atr"])
        + w["clv"]       * _pct_rank([x["clv_short"]       for x in pool], f["clv_short"])
        + w["rs"]        * _pct_rank([x["rs_short"]        for x in pool], f["rs_short"])
        + w["vwap_edge"] * _pct_rank([x["vwap_edge_short"] for x in pool], f["vwap_edge_short"])
        + w["ema20_edge"]* _pct_rank([x["ema20_edge_short"]for x in pool], f["ema20_edge_short"])
        + w["body_eff"]  * _pct_rank([x["body_eff_short"]  for x in pool], f["body_eff_short"])
    )
    return _clip(score - AFTERNOON_EXHAUSTION_PENALTY * float(f.get("exhaust_short", 0.0)), 0.0, 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# SESSION RUNNERS
# ─────────────────────────────────────────────────────────────────────────────

def run_morning(tickers: List[str], today, atr_cache: Dict[str, float]):
    print(f"\n[W1 MORNING]  obs 09:15-09:30  |  trade 09:30-11:00")
    print(f"  Scanning {len(tickers)} stocks...")
    raw, skipped = [], 0
    for ticker in tickers:
        atr14 = atr_cache.get(ticker)
        if atr14 is None:
            skipped += 1; continue
        df5 = load_5min(ticker)
        if df5 is None:
            skipped += 1; continue
        df5 = _enrich_time_date(df5)
        f = morning_factors(ticker, df5, today, atr14)
        if f is None:
            skipped += 1; continue
        raw.append(f)
    print(f"  Factors computed: {len(raw)}  |  skip: {skipped}")
    long_pool = [f for f in raw if _morning_pass_long(f)]
    for f in long_pool:
        f["score"] = _score_morning_long(f, long_pool)
    long_pool.sort(key=lambda x: x["score"], reverse=True)
    long_out = long_pool if MORNING_TOP_N is None or MORNING_TOP_N <= 0 else long_pool[:MORNING_TOP_N]
    short_pool = [f for f in raw if _morning_pass_short(f)]
    for f in short_pool:
        f["score"] = _score_morning_short(f, short_pool)
    short_pool.sort(key=lambda x: x["score"], reverse=True)
    short_out = short_pool if MORNING_TOP_N is None or MORNING_TOP_N <= 0 else short_pool[:MORNING_TOP_N]
    print(f"  Long  -> passed: {len(long_pool):3d}  |  shortlist: {len(long_out)}")
    print(f"  Short -> passed: {len(short_pool):3d}  |  shortlist: {len(short_out)}")
    return long_out, short_out


def run_mid(tickers: List[str], today, atr_cache: Dict[str, float]):
    print(f"\n[W2 MID]  obs 10:30-11:15 (45 min, 9 bars)  |  trade 11:15-12:45")
    print(f"  Scanning {len(tickers)} stocks...")
    raw, skipped = [], 0
    for ticker in tickers:
        atr14 = atr_cache.get(ticker)
        if atr14 is None:
            skipped += 1; continue
        df5 = load_5min(ticker)
        if df5 is None:
            skipped += 1; continue
        df5 = _enrich_time_date(df5)
        f = mid_factors(ticker, df5, today, atr14)
        if f is None:
            skipped += 1; continue
        raw.append(f)
    print(f"  Factors computed: {len(raw)}  |  skip: {skipped}")
    long_pool = [f for f in raw if _mid_pass_long(f)]
    for f in long_pool:
        f["score"] = _score_mid_long(f, long_pool)
    long_pool.sort(key=lambda x: x["score"], reverse=True)
    long_out = long_pool if MID_TOP_N is None or MID_TOP_N <= 0 else long_pool[:MID_TOP_N]
    short_pool = [f for f in raw if _mid_pass_short(f)]
    for f in short_pool:
        f["score"] = _score_mid_short(f, short_pool)
    short_pool.sort(key=lambda x: x["score"], reverse=True)
    short_out = short_pool if MID_TOP_N is None or MID_TOP_N <= 0 else short_pool[:MID_TOP_N]
    print(f"  Long  -> passed: {len(long_pool):3d}  |  shortlist: {len(long_out)}")
    print(f"  Short -> passed: {len(short_pool):3d}  |  shortlist: {len(short_out)}")
    return long_out, short_out


def run_afternoon(tickers: List[str], today, atr_cache: Dict[str, float]):
    print(f"\n[W3 AFTERNOON]  obs 12:30-13:00 (30 min, 6 bars)  |  trade 13:00-14:30")
    print(f"  Scanning {len(tickers)} stocks...")
    raw, skipped = [], 0
    for ticker in tickers:
        atr14 = atr_cache.get(ticker)
        if atr14 is None:
            skipped += 1; continue
        df5 = load_5min(ticker)
        if df5 is None:
            skipped += 1; continue
        df5 = _enrich_time_date(df5)
        f = afternoon_factors(ticker, df5, today, atr14)
        if f is None:
            skipped += 1; continue
        raw.append(f)
    print(f"  Factors computed: {len(raw)}  |  skip: {skipped}")
    long_pool = [f for f in raw if _afternoon_pass_long(f)]
    for f in long_pool:
        f["score"] = _score_afternoon_long(f, long_pool)
    long_pool.sort(key=lambda x: x["score"], reverse=True)
    long_out = long_pool if AFTERNOON_TOP_N is None or AFTERNOON_TOP_N <= 0 else long_pool[:AFTERNOON_TOP_N]
    short_pool = [f for f in raw if _afternoon_pass_short(f)]
    for f in short_pool:
        f["score"] = _score_afternoon_short(f, short_pool)
    short_pool.sort(key=lambda x: x["score"], reverse=True)
    short_out = short_pool if AFTERNOON_TOP_N is None or AFTERNOON_TOP_N <= 0 else short_pool[:AFTERNOON_TOP_N]
    print(f"  Long  -> passed: {len(long_pool):3d}  |  shortlist: {len(long_out)}")
    print(f"  Short -> passed: {len(short_pool):3d}  |  shortlist: {len(short_out)}")
    return long_out, short_out


# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def _display(title: str, rows: List[dict], display_rows: List[dict]) -> None:
    sep = "=" * 72
    if not rows:
        print(f"\n{sep}\n  {title}\n  No candidates passed all filters.\n{sep}")
        return
    df = pd.DataFrame(display_rows)
    print(f"\n{sep}")
    print(f"  {title}  [{len(rows)} stocks]")
    print(f"{sep}")
    print(df.to_string(index=False))
    print(sep)


def display_morning(long_list: List[dict], short_list: List[dict]) -> None:
    def fmt_long(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_l=round(r["clv_long"],2),
                     rs=round(r["rs_long"],2), vwap_e=round(r["vwap_edge_long"],2),
                     gap_hold=r["gap_hold_long"], gap_pct=round(r["gap_pct"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    def fmt_short(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_s=round(r["clv_short"],2),
                     rs=round(r["rs_short"],2), vwap_e=round(r["vwap_edge_short"],2),
                     gap_hold=r["gap_hold_short"], gap_pct=round(r["gap_pct"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    _display("W1 MORNING LONG  (valid 09:30-11:00)", long_list,  fmt_long(long_list))
    _display("W1 MORNING SHORT (valid 09:30-11:00)", short_list, fmt_short(short_list))


def display_mid(long_list: List[dict], short_list: List[dict]) -> None:
    def fmt_long(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_l=round(r["clv_long"],2),
                     rs=round(r["rs_long"],2), vwap_e=round(r["vwap_edge_long"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    def fmt_short(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_s=round(r["clv_short"],2),
                     rs=round(r["rs_short"],2), vwap_e=round(r["vwap_edge_short"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    _display("W2 MID LONG  obs 10:30-11:15  (valid 11:15-12:45)", long_list,  fmt_long(long_list))
    _display("W2 MID SHORT obs 10:30-11:15  (valid 11:15-12:45)", short_list, fmt_short(short_list))


def display_afternoon(long_list: List[dict], short_list: List[dict]) -> None:
    def fmt_long(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_l=round(r["clv_long"],2),
                     rs=round(r["rs_long"],2), vwap_e=round(r["vwap_edge_long"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    def fmt_short(rows):
        return [dict(ticker=r["ticker"], rvol=round(r["rvol"],2), tvr=round(r["tv_ratio"],2),
                     orp_atr=round(r["orp_atr"],3), clv_s=round(r["clv_short"],2),
                     rs=round(r["rs_short"],2), vwap_e=round(r["vwap_edge_short"],2),
                     tv_L=round(r["tv"]/100_000,1), atr14=round(r["atr14"],2), score=round(r["score"],3)) for r in rows]
    _display("W3 AFTERNOON LONG  obs 12:30-13:00  (valid 13:00-14:30)", long_list,  fmt_long(long_list))
    _display("W3 AFTERNOON SHORT obs 12:30-13:00  (valid 13:00-14:30)", short_list, fmt_short(short_list))


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def _py_set_literal(stocks: List[str]) -> str:
    names = sorted({str(s) for s in stocks if str(s).strip()})
    if not names:
        return "set()"
    return "{" + ", ".join(repr(name) for name in names) + "}"


def export_shortlist_module(session: str, scan_date, long_list: List[dict], short_list: List[dict]) -> List[Path]:
    long_tickers  = sorted({str(r["ticker"]) for r in long_list})
    short_tickers = sorted({str(r["ticker"]) for r in short_list})
    all_tickers   = sorted(set(long_tickers) | set(short_tickers))
    body = (
        "# Auto-generated by two_session_shortlist_test_v3_3times.py\n"
        f"scan_date = {scan_date.isoformat()!r}\n"
        f"session = {session.upper()!r}\n"
        "version = 'v3_3times'\n"
        f"selected_stocks_long = {_py_set_literal(long_tickers)}\n"
        f"selected_stocks_short = {_py_set_literal(short_tickers)}\n"
        f"selected_stocks = {_py_set_literal(all_tickers)}\n"
    )
    paths = [
        SHORTLIST_EXPORT_DIR / f"filtered_stocks_two_session_{session.lower()}_v3_3times.py",
        SHORTLIST_EXPORT_DIR / "filtered_stocks_two_session_latest_v3_3times.py",
    ]
    for path in paths:
        path.write_text(body, encoding="utf-8")
    return paths


# ─────────────────────────────────────────────────────────────────────────────
# DATE RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_scan_date(requested_date, tickers: List[str]):
    import datetime as _dt
    sample = [t for t in tickers if (DATA_5M_DIR / f"{t}_stocks_indicators_5min.parquet").exists()][:5]
    if not sample:
        return requested_date
    latest_available = None
    for ticker in sample:
        df = load_5min(ticker)
        if df is None or df.empty:
            continue
        d = df["date"].dt.date.max()
        if latest_available is None or d > latest_available:
            latest_available = d
    if latest_available is None:
        return requested_date
    if latest_available < requested_date:
        print(f"\n  WARNING: No 5min data for {requested_date}. Falling back to {latest_available}\n")
        return latest_available
    return requested_date


# ─────────────────────────────────────────────────────────────────────────────
# SESSION AUTO-DETECT
# ─────────────────────────────────────────────────────────────────────────────

def detect_session() -> str:
    t = datetime.now(IST).time()
    if time(9, 15) <= t < time(11, 0):
        return "morning"
    elif time(11, 0) <= t < time(11, 15):
        return "mid"        # obs window for W2
    elif time(11, 15) <= t < time(12, 45):
        return "mid"
    elif time(12, 45) <= t < time(13, 0):
        return "afternoon"  # obs window for W3
    elif time(13, 0) <= t <= time(15, 30):
        return "afternoon"
    else:
        return "all"        # outside market hours — run all for testing


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Three-Window Stock Shortlister v3_3times -- Manual Test Runner"
    )
    parser.add_argument(
        "--session", choices=["morning", "mid", "afternoon", "all", "auto"],
        default="auto",
        help="Session to run (default: auto-detect from IST time)"
    )
    parser.add_argument(
        "--date", default=None,
        help="Date override YYYY-MM-DD (default: today IST)"
    )
    args = parser.parse_args()

    now_ist = datetime.now(IST)
    today   = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else now_ist.date()
    session = detect_session() if args.session == "auto" else args.session

    print(f"\n{'='*72}")
    print(f"  THREE-WINDOW SHORTLISTING FRAMEWORK  --  v3_3times TEST RUNNER")
    print(f"  Run time : {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Date     : {today}")
    print(f"  Session  : {session.upper()}")
    print(f"  W1 MORNING   obs 09:15-09:30 (15 min, 3 bars)  trade 09:30-11:00")
    print(f"  W2 MID       obs 10:30-11:15 (45 min, 9 bars)  trade 11:15-12:45")
    print(f"  W3 AFTERNOON obs 12:30-13:00 (30 min, 6 bars)  trade 13:00-14:30")
    print(f"{'='*72}")

    tickers = load_universe()
    print(f"\n  Universe  : {len(tickers)} stocks (filtered_stocks_MIS.py)")
    print(f"\n  Pre-loading daily ATR_14d...")
    atr_cache = build_atr_cache(tickers)
    print(f"  ATR cache : {len(atr_cache)} / {len(tickers)} loaded")
    print(f"  5min data : {DATA_5M_DIR}")

    today = _resolve_scan_date(today, tickers)

    if session in ("morning", "all"):
        long_l, short_l = run_morning(tickers, today, atr_cache)
        display_morning(long_l, short_l)
        for path in export_shortlist_module("morning", today, long_l, short_l):
            print(f"  Exported: {path}")

    if session in ("mid", "all"):
        long_l, short_l = run_mid(tickers, today, atr_cache)
        display_mid(long_l, short_l)
        for path in export_shortlist_module("mid", today, long_l, short_l):
            print(f"  Exported: {path}")

    if session in ("afternoon", "all"):
        long_l, short_l = run_afternoon(tickers, today, atr_cache)
        display_afternoon(long_l, short_l)
        for path in export_shortlist_module("afternoon", today, long_l, short_l):
            print(f"  Exported: {path}")

    print(f"\n  Column guide:")
    print(f"    rvol     = Relative volume vs same-time 20-session avg")
    print(f"    tvr      = Turnover ratio vs same-window 20-session avg")
    print(f"    orp_atr  = (H-L of window) / ATR_14d")
    print(f"    clv_l/s  = Close location value for long/short")
    print(f"    rs       = Window return minus NIFTYBEES window return")
    print(f"    vwap_e   = (close - VWAP) / ATR_14d in direction of trade")
    print(f"    gap_hold = 1 if gap is holding directionally (morning only)")
    print(f"    tv_L     = Traded value in Lakhs for the obs window")
    print(f"    atr14    = 14-day daily ATR (from daily parquet)")
    print(f"    score    = Weighted multi-factor percentile score with exhaustion penalty")
    print()


if __name__ == "__main__":
    main()
