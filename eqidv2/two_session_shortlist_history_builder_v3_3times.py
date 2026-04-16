from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from pprint import pformat
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import two_session_shortlist_test_v3_3times as shortlist_base

IST               = shortlist_base.IST
# Keep shortlist-history builds aligned with the v19a runner's 5-minute source of truth.
HIST_5M_DIR       = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DAILY_DIR         = Path(r"C:\TradingData\eqidv2\stocks_indicators_daily_eq")
DEFAULT_OUTPUT_PATH = SCRIPT_DIR / "filtered_stocks_two_session_history_v3_3times.py"
DEFAULT_START_DATE  = date(2025, 6, 1)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_date(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _safe_rank(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return series.rank(method="average", pct=True)


def _hybrid_rank(df: pd.DataFrame, col: str, cross_weight: float = 0.6) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    cross = df.groupby("scan_date")[col].transform(_safe_rank)
    hist  = df.groupby("ticker")[col].transform(_safe_rank)
    hist  = hist.fillna(cross)
    return (cross_weight * cross) + ((1.0 - cross_weight) * hist)


def _clip_series(series: pd.Series, lo: float = 0.0, hi: float = 1.0) -> pd.Series:
    return series.clip(lower=lo, upper=hi)


def _combine_lists(primary: Sequence[str], secondary: Sequence[str]) -> List[str]:
    seen: set = set()
    combined: List[str] = []
    for name in list(primary) + list(secondary):
        if name not in seen:
            seen.add(name)
            combined.append(name)
    return combined


def _load_intraday(ticker: str) -> Optional[pd.DataFrame]:
    return shortlist_base._load_parquet(HIST_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet")


def _load_daily(ticker: str) -> Optional[pd.DataFrame]:
    return shortlist_base._load_parquet(DAILY_DIR / f"{ticker}_stocks_indicators_daily.parquet")


def _build_atr_by_date(df_daily: Optional[pd.DataFrame]) -> Dict[date, float]:
    if df_daily is None or df_daily.empty or "ATR" not in df_daily.columns:
        return {}
    daily = df_daily.copy()
    daily["_date"] = daily["date"].dt.date
    atr_series = (
        daily.groupby("_date", sort=True)["ATR"]
        .last()
        .dropna()
        .sort_index()
        .shift(1)
    )
    atr_series = atr_series[atr_series > 0]
    return {scan_date: float(value) for scan_date, value in atr_series.items()}


def _build_market_return_map(obs_times: set, start_date: date, end_date: date) -> Dict[date, float]:
    market_df = shortlist_base.load_5min(shortlist_base.MARKET_PROXY_TICKER)
    if market_df is None or market_df.empty:
        return {}
    market_df = shortlist_base._enrich_time_date(market_df)
    market_df = market_df.sort_values("date").reset_index(drop=True)
    market_df = market_df[
        (market_df["_date"] >= start_date)
        & (market_df["_date"] <= end_date)
        & (market_df["_time"].isin(obs_times))
    ].copy()
    if market_df.empty:
        return {}
    out: Dict[date, float] = {}
    for scan_date, group in market_df.groupby("_date", sort=True):
        group = group.sort_values("_time")
        first_open = float(group.iloc[0]["open"])
        last_close = float(group.iloc[-1]["close"])
        if first_open > 0:
            out[scan_date] = float(((last_close - first_open) / first_open) * 100.0)
    return out


def _session_summary(
    ticker: str,
    df5: pd.DataFrame,
    atr_by_date: Dict[date, float],
    market_return_map: Dict[date, float],
    obs_times: set,
    session_name: str,   # "morning" | "mid" | "afternoon"
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    if df5.empty:
        return pd.DataFrame()

    df5 = shortlist_base._enrich_time_date(df5)
    df5 = df5.sort_values("date").reset_index(drop=True)
    date_mask = (df5["_date"] >= start_date) & (df5["_date"] <= end_date)
    if not date_mask.any():
        return pd.DataFrame()

    window = df5[date_mask & df5["_time"].isin(obs_times)].copy()
    if window.empty:
        return pd.DataFrame()

    window["turnover"] = window["close"] * window["volume"]
    grouped = window.groupby("_date", sort=True)

    agg_spec: dict = {
        "high":       ("high",   "max"),
        "low":        ("low",    "min"),
        "close_end":  ("close",  "last"),
        "total_vol":  ("volume", "sum"),
        "tv":         ("turnover","sum"),
        "first_open": ("open",   "first"),
    }
    _OPT_COLS = {
        "vwap_last":       "VWAP",
        "ema20_last":      "EMA_20",
        "upper_band_last": "Upper_Band",
        "lower_band_last": "Lower_Band",
    }
    _present_opt = {alias: (src, "last") for alias, src in _OPT_COLS.items() if src in window.columns}
    agg_spec.update(_present_opt)

    summary = grouped.agg(**agg_spec)
    for alias in _OPT_COLS:
        if alias not in summary.columns:
            summary[alias] = np.nan

    daily_close = df5.groupby("_date", sort=True)["close"].last().sort_index()
    prev_close  = daily_close.shift(1).reindex(summary.index)
    if "Prev_Day_Close" in window.columns:
        prev_day_close = grouped["Prev_Day_Close"].first()
        prev_close = prev_close.combine_first(prev_day_close)

    vol_series   = summary["total_vol"].where(summary["total_vol"] > 0)
    hist_mean    = vol_series.shift(1).rolling(shortlist_base.RVOL_LOOKBACK_SESSIONS, min_periods=1).mean()
    tv_hist_mean = summary["tv"].where(summary["tv"] > 0).shift(1).rolling(
        shortlist_base.TURNOVER_LOOKBACK_SESSIONS, min_periods=1
    ).mean()

    summary["scan_date"] = summary.index
    summary["ticker"]    = ticker
    summary["atr14"]     = summary.index.to_series().map(atr_by_date)
    summary["prev_close"] = prev_close
    summary["rvol"] = np.where(
        summary["total_vol"] > 0,
        np.where(hist_mean > 0, summary["total_vol"] / hist_mean, 1.0),
        0.0,
    )
    summary["tv_ratio"] = np.where(
        summary["tv"] > 0,
        np.where(tv_hist_mean > 0, summary["tv"] / tv_hist_mean, 1.0),
        0.0,
    )

    span    = summary["high"] - summary["low"]
    summary = summary[(summary["atr14"].notna()) & (summary["atr14"] > 0) & (span > 0)].copy()
    if summary.empty:
        return pd.DataFrame()

    span = summary["high"] - summary["low"]
    summary["orp_atr"]   = span / summary["atr14"]
    summary["clv_long"]  = (summary["close_end"] - summary["low"]) / span
    summary["clv_short"] = (summary["high"] - summary["close_end"]) / span
    stock_ret  = np.where(
        summary["first_open"] > 0,
        ((summary["close_end"] - summary["first_open"]) / summary["first_open"]) * 100.0,
        0.0,
    )
    market_ret = summary.index.to_series().map(market_return_map).fillna(0.0)
    summary["rs_long"]  = stock_ret - market_ret
    summary["rs_short"] = -summary["rs_long"]
    summary["vwap_edge_long"] = np.where(
        pd.notna(summary["vwap_last"]),
        ((summary["close_end"] - summary["vwap_last"]) / summary["atr14"]).clip(-2.0, 2.0),
        0.0,
    )
    summary["vwap_edge_short"] = np.where(
        pd.notna(summary["vwap_last"]),
        ((summary["vwap_last"] - summary["close_end"]) / summary["atr14"]).clip(-2.0, 2.0),
        0.0,
    )
    summary["ema20_edge_long"] = np.where(
        pd.notna(summary["ema20_last"]),
        ((summary["close_end"] - summary["ema20_last"]) / summary["atr14"]).clip(-2.0, 2.0),
        0.0,
    )
    summary["ema20_edge_short"] = np.where(
        pd.notna(summary["ema20_last"]),
        ((summary["ema20_last"] - summary["close_end"]) / summary["atr14"]).clip(-2.0, 2.0),
        0.0,
    )
    summary["body_eff_long"]  = ((summary["close_end"] - summary["first_open"]).clip(lower=0.0) / span).clip(0.0, 1.0)
    summary["body_eff_short"] = ((summary["first_open"] - summary["close_end"]).clip(lower=0.0) / span).clip(0.0, 1.0)

    # ── Gap logic (morning only) ──────────────────────────────────────────────
    if session_name == "morning":
        summary = summary[(summary["prev_close"].notna()) & (summary["prev_close"] > 0)].copy()
        if summary.empty:
            return pd.DataFrame()
        summary["gap_pct"]       = ((summary["first_open"] - summary["prev_close"]) / summary["prev_close"]) * 100.0
        summary["gap_hold_long"]  = 0
        summary["gap_hold_short"] = 0
        gap_up   = summary["gap_pct"] > shortlist_base.GAP_THRESHOLD_PCT
        gap_down = summary["gap_pct"] < -shortlist_base.GAP_THRESHOLD_PCT
        flat_open = ~(gap_up | gap_down)
        summary.loc[gap_up  & (summary["low"]  > summary["prev_close"]) & (summary["clv_long"]  >= 0.60), "gap_hold_long"]  = 1
        summary.loc[gap_down & (summary["high"] < summary["prev_close"]) & (summary["clv_short"] >= 0.60), "gap_hold_short"] = 1
        summary.loc[flat_open & (summary["clv_long"]  >= 0.65) & (summary["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN), "gap_hold_long"]  = 1
        summary.loc[flat_open & (summary["clv_short"] >= 0.65) & (summary["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN), "gap_hold_short"] = 1
    else:
        summary["gap_pct"]        = np.nan
        summary["gap_hold_long"]  = 0
        summary["gap_hold_short"] = 0

    # ── Exhaustion flags ──────────────────────────────────────────────────────
    _ub = pd.to_numeric(summary["upper_band_last"], errors="coerce")
    _lb = pd.to_numeric(summary["lower_band_last"], errors="coerce")
    _ub_valid = pd.notna(_ub)
    _lb_valid = pd.notna(_lb)

    if session_name == "morning":
        summary["exhaust_long"] = (
            ((summary["rvol"] >= 3.0) & (summary["clv_long"]  >= 0.92) & (summary["vwap_edge_long"]  >= 0.80))
            | (_ub_valid & (summary["close_end"] >= _ub) & (summary["rvol"] >= 2.0))
        ).astype(int)
        summary["exhaust_short"] = (
            ((summary["rvol"] >= 3.0) & (summary["clv_short"] >= 0.92) & (summary["vwap_edge_short"] >= 0.80))
            | (_lb_valid & (summary["close_end"] <= _lb) & (summary["rvol"] >= 2.0))
        ).astype(int)
    elif session_name == "mid":
        summary["exhaust_long"] = (
            ((summary["rvol"] >= 2.8) & (summary["clv_long"]  >= 0.92) & (summary["vwap_edge_long"]  >= 0.75))
            | (_ub_valid & (summary["close_end"] >= _ub) & (summary["rvol"] >= 1.9))
        ).astype(int)
        summary["exhaust_short"] = (
            ((summary["rvol"] >= 2.8) & (summary["clv_short"] >= 0.92) & (summary["vwap_edge_short"] >= 0.75))
            | (_lb_valid & (summary["close_end"] <= _lb) & (summary["rvol"] >= 1.9))
        ).astype(int)
    else:  # afternoon
        summary["exhaust_long"] = (
            ((summary["rvol"] >= 2.6) & (summary["clv_long"]  >= 0.92) & (summary["vwap_edge_long"]  >= 0.70))
            | (_ub_valid & (summary["close_end"] >= _ub) & (summary["rvol"] >= 1.8))
        ).astype(int)
        summary["exhaust_short"] = (
            ((summary["rvol"] >= 2.6) & (summary["clv_short"] >= 0.92) & (summary["vwap_edge_short"] >= 0.70))
            | (_lb_valid & (summary["close_end"] <= _lb) & (summary["rvol"] >= 1.8))
        ).astype(int)

    keep_cols = [
        "scan_date", "ticker", "rvol", "tv_ratio", "orp_atr",
        "clv_long", "clv_short", "rs_long", "rs_short",
        "vwap_edge_long", "vwap_edge_short", "ema20_edge_long", "ema20_edge_short",
        "body_eff_long", "body_eff_short", "exhaust_long", "exhaust_short",
        "gap_hold_long", "gap_hold_short", "gap_pct",
        "tv", "total_vol", "atr14", "prev_close", "high", "low", "close_end", "first_open",
    ]
    return summary.reset_index(drop=True)[keep_cols]


# ─────────────────────────────────────────────────────────────────────────────
# BUILD RAW FRAMES  (3 sessions)
# ─────────────────────────────────────────────────────────────────────────────

def _build_raw_frames(
    tickers: Sequence[str],
    start_date: date,
    end_date: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (morning_raw, mid_raw, afternoon_raw)."""
    morning_frames:   List[pd.DataFrame] = []
    mid_frames:       List[pd.DataFrame] = []
    afternoon_frames: List[pd.DataFrame] = []

    morning_market_map   = _build_market_return_map(shortlist_base.MORNING_OBS_TIMES,   start_date, end_date)
    mid_market_map       = _build_market_return_map(shortlist_base.MID_OBS_TIMES,       start_date, end_date)
    afternoon_market_map = _build_market_return_map(shortlist_base.AFTERNOON_OBS_TIMES, start_date, end_date)

    total = len(tickers)
    for idx, ticker in enumerate(tickers, start=1):
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(f"  [BUILD] {idx}/{total} tickers processed...", flush=True)

        df5 = _load_intraday(ticker)
        if df5 is None or df5.empty:
            continue
        df_daily    = _load_daily(ticker)
        atr_by_date = _build_atr_by_date(df_daily)
        if not atr_by_date:
            continue

        for frames, obs, sname, mmap in [
            (morning_frames,   shortlist_base.MORNING_OBS_TIMES,   "morning",   morning_market_map),
            (mid_frames,       shortlist_base.MID_OBS_TIMES,       "mid",       mid_market_map),
            (afternoon_frames, shortlist_base.AFTERNOON_OBS_TIMES, "afternoon", afternoon_market_map),
        ]:
            df_sess = _session_summary(
                ticker=ticker, df5=df5, atr_by_date=atr_by_date,
                market_return_map=mmap, obs_times=obs,
                session_name=sname, start_date=start_date, end_date=end_date,
            )
            if not df_sess.empty:
                frames.append(df_sess)

    morning   = pd.concat(morning_frames,   ignore_index=True) if morning_frames   else pd.DataFrame()
    mid       = pd.concat(mid_frames,       ignore_index=True) if mid_frames       else pd.DataFrame()
    afternoon = pd.concat(afternoon_frames, ignore_index=True) if afternoon_frames else pd.DataFrame()
    return morning, mid, afternoon


# ─────────────────────────────────────────────────────────────────────────────
# APPLY SHORTLIST FILTERS
# ─────────────────────────────────────────────────────────────────────────────

def _score_components(df: pd.DataFrame, side: str, session: str) -> pd.DataFrame:
    """Add score, score_mod, score_cont, score_huge columns in-place and return df."""
    if df.empty:
        return df

    clv_col       = "clv_long"  if side == "long"  else "clv_short"
    rs_col        = "rs_long"   if side == "long"  else "rs_short"
    vwap_col      = "vwap_edge_long"  if side == "long" else "vwap_edge_short"
    ema20_col     = "ema20_edge_long" if side == "long" else "ema20_edge_short"
    body_col      = "body_eff_long"   if side == "long" else "body_eff_short"
    exhaust_col   = "exhaust_long"    if side == "long" else "exhaust_short"
    gap_hold_col  = "gap_hold_long"   if side == "long" else "gap_hold_short"

    if session == "morning":
        weights      = shortlist_base.MORNING_WEIGHTS
        exhaust_pen  = shortlist_base.MORNING_EXHAUSTION_PENALTY
    elif session == "mid":
        weights      = shortlist_base.MID_WEIGHTS
        exhaust_pen  = shortlist_base.MID_EXHAUSTION_PENALTY
    else:
        weights      = shortlist_base.AFTERNOON_WEIGHTS
        exhaust_pen  = shortlist_base.AFTERNOON_EXHAUSTION_PENALTY

    df["score_rvol"]      = _hybrid_rank(df, "rvol")
    df["score_tv_ratio"]  = _hybrid_rank(df, "tv_ratio")
    df["score_orp_atr"]   = _hybrid_rank(df, "orp_atr")
    df["score_clv"]       = _hybrid_rank(df, clv_col)
    df["score_rs"]        = _hybrid_rank(df, rs_col)
    df["score_vwap_edge"] = _hybrid_rank(df, vwap_col)
    df["score_ema20_edge"]= _hybrid_rank(df, ema20_col)
    df["score_body_eff"]  = _hybrid_rank(df, body_col)

    if session == "morning":
        df["score_gap_hold"] = df.groupby("scan_date")[gap_hold_col].transform(_safe_rank)
        df["score"] = _clip_series(
            weights["rvol"]      * df["score_rvol"]
            + weights["tv_ratio"]  * df["score_tv_ratio"]
            + weights["orp_atr"]   * df["score_orp_atr"]
            + weights["clv"]       * df["score_clv"]
            + weights["gap_hold"]  * df["score_gap_hold"]
            + weights["rs"]        * df["score_rs"]
            + weights["vwap_edge"] * df["score_vwap_edge"]
            + weights["ema20_edge"]* df["score_ema20_edge"]
            + weights["body_eff"]  * df["score_body_eff"]
            - exhaust_pen * df[exhaust_col].astype(float)
        )
        df["score_mod"] = _clip_series(
            0.24 * df["score_orp_atr"] + 0.18 * df["score_clv"]  + 0.18 * df["score_rs"]
            + 0.14 * df["score_vwap_edge"] + 0.10 * df["score_gap_hold"]
            + 0.08 * df["score_body_eff"]  + 0.08 * df["score_tv_ratio"]
            - 0.08 * df[exhaust_col].astype(float)
        )
        df["score_cont"] = _clip_series(
            0.24 * df["score_rs"]      + 0.18 * df["score_vwap_edge"]
            + 0.12 * df["score_ema20_edge"] + 0.12 * df["score_body_eff"]
            + 0.12 * df["score_clv"]   + 0.12 * df["score_rvol"]
            + 0.10 * df["score_tv_ratio"]
            - 0.10 * df[exhaust_col].astype(float)
        )
        df["score_huge"] = _clip_series(
            0.24 * df["score_rvol"]    + 0.18 * df["score_tv_ratio"]
            + 0.18 * df["score_orp_atr"] + 0.16 * df["score_rs"]
            + 0.12 * df["score_clv"]   + 0.12 * df["score_gap_hold"]
            - 0.04 * df[exhaust_col].astype(float)
        )
    else:  # mid or afternoon — no gap_hold component
        df["score"] = _clip_series(
            weights["rvol"]      * df["score_rvol"]
            + weights["tv_ratio"]  * df["score_tv_ratio"]
            + weights["orp_atr"]   * df["score_orp_atr"]
            + weights["clv"]       * df["score_clv"]
            + weights["rs"]        * df["score_rs"]
            + weights["vwap_edge"] * df["score_vwap_edge"]
            + weights["ema20_edge"]* df["score_ema20_edge"]
            + weights["body_eff"]  * df["score_body_eff"]
            - exhaust_pen * df[exhaust_col].astype(float)
        )
        df["score_mod"] = _clip_series(
            0.22 * df["score_orp_atr"] + 0.18 * df["score_clv"]
            + 0.20 * df["score_rs"]    + 0.16 * df["score_vwap_edge"]
            + 0.10 * df["score_body_eff"] + 0.14 * df["score_tv_ratio"]
            - 0.08 * df[exhaust_col].astype(float)
        )
        df["score_cont"] = _clip_series(
            0.26 * df["score_rs"]      + 0.20 * df["score_vwap_edge"]
            + 0.14 * df["score_ema20_edge"] + 0.12 * df["score_body_eff"]
            + 0.12 * df["score_clv"]   + 0.08 * df["score_rvol"]
            + 0.08 * df["score_tv_ratio"]
            - 0.10 * df[exhaust_col].astype(float)
        )
        df["score_huge"] = _clip_series(
            0.24 * df["score_rvol"]    + 0.20 * df["score_tv_ratio"]
            + 0.18 * df["score_orp_atr"] + 0.18 * df["score_rs"]
            + 0.10 * df["score_clv"]   + 0.10 * df["score_vwap_edge"]
            - 0.04 * df[exhaust_col].astype(float)
        )
    return df


def _apply_session_shortlist(
    raw_df: pd.DataFrame,
    session: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter, score and rank raw_df for the given session. Returns (long_df, short_df)."""
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    if session == "morning":
        rvol_min = shortlist_base.MORNING_RVOL_MIN
        orp_min  = shortlist_base.MORNING_ORP_ATR_MIN
        clv_min  = shortlist_base.MORNING_CLV_MIN
        tv_long  = shortlist_base.MORNING_TV_MIN_LONG
        tv_short = shortlist_base.MORNING_TV_MIN_SHORT
        top_n    = shortlist_base.MORNING_TOP_N
    elif session == "mid":
        rvol_min = shortlist_base.MID_RVOL_MIN
        orp_min  = shortlist_base.MID_ORP_ATR_MIN
        clv_min  = shortlist_base.MID_CLV_MIN
        tv_long  = shortlist_base.MID_TV_MIN_LONG
        tv_short = shortlist_base.MID_TV_MIN_SHORT
        top_n    = shortlist_base.MID_TOP_N
    else:  # afternoon
        rvol_min = shortlist_base.AFTERNOON_RVOL_MIN
        orp_min  = shortlist_base.AFTERNOON_ORP_ATR_MIN
        clv_min  = shortlist_base.AFTERNOON_CLV_MIN
        tv_long  = shortlist_base.AFTERNOON_TV_MIN_LONG
        tv_short = shortlist_base.AFTERNOON_TV_MIN_SHORT
        top_n    = shortlist_base.AFTERNOON_TOP_N

    long_mask = (
        (raw_df["rvol"]      >= rvol_min) &
        (raw_df["orp_atr"]   >= orp_min)  &
        (raw_df["clv_long"]  >= clv_min)  &
        (raw_df["tv"]        >= tv_long)
    )
    short_mask = (
        (raw_df["rvol"]      >= rvol_min) &
        (raw_df["orp_atr"]   >= orp_min)  &
        (raw_df["clv_short"] >= clv_min)  &
        (raw_df["tv"]        >= tv_short)
    )

    long_df  = raw_df.loc[long_mask].copy()
    short_df = raw_df.loc[short_mask].copy()

    if not long_df.empty:
        long_df  = _score_components(long_df,  "long",  session)
        long_df  = long_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if top_n:
            long_df = long_df.groupby("scan_date", sort=False).head(top_n)
    if not short_df.empty:
        short_df = _score_components(short_df, "short", session)
        short_df = short_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if top_n:
            short_df = short_df.groupby("scan_date", sort=False).head(top_n)

    return long_df, short_df


# ─────────────────────────────────────────────────────────────────────────────
# PAYLOAD HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _rows_to_ranked_lists(df: pd.DataFrame) -> Dict[date, List[str]]:
    if df.empty:
        return {}
    out: Dict[date, List[str]] = {}
    for scan_date, group in df.groupby("scan_date", sort=True):
        out[scan_date] = group["ticker"].astype(str).tolist()
    return out


def _rows_to_score_maps(df: pd.DataFrame, score_col: str = "score") -> Dict[date, Dict[str, float]]:
    if df.empty or score_col not in df.columns:
        return {}
    out: Dict[date, Dict[str, float]] = {}
    for scan_date, group in df.groupby("scan_date", sort=True):
        score_map: Dict[str, float] = {}
        for row in group.itertuples(index=False):
            ticker = str(getattr(row, "ticker", ""))
            if not ticker:
                continue
            score_map[ticker] = float(getattr(row, score_col, 0.0))
        out[scan_date] = score_map
    return out


def _combine_score_maps(
    primary: Dict[date, Dict[str, float]],
    secondary: Dict[date, Dict[str, float]],
) -> Dict[date, Dict[str, float]]:
    out: Dict[date, Dict[str, float]] = {}
    for scan_date in sorted(set(primary) | set(secondary)):
        merged: Dict[str, float] = {}
        for source in (primary.get(scan_date, {}), secondary.get(scan_date, {})):
            for ticker, score in source.items():
                best = merged.get(ticker)
                if best is None or float(score) > float(best):
                    merged[ticker] = float(score)
        out[scan_date] = dict(sorted(merged.items(), key=lambda kv: (-kv[1], kv[0])))
    return out


def _counts_by_date(df: pd.DataFrame) -> Dict[date, int]:
    if df.empty:
        return {}
    return {scan_date: int(count) for scan_date, count in df.groupby("scan_date", sort=True).size().items()}


def _build_session_block(
    long_df: pd.DataFrame,
    short_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    universe_size: int,
    scan_date,
) -> dict:
    long_list  = _rows_to_ranked_lists(long_df).get(scan_date, [])
    short_list = _rows_to_ranked_lists(short_df).get(scan_date, [])
    long_scores       = _rows_to_score_maps(long_df).get(scan_date, {})
    short_scores      = _rows_to_score_maps(short_df).get(scan_date, {})
    long_score_mod    = _rows_to_score_maps(long_df,  "score_mod").get(scan_date, {})
    short_score_mod   = _rows_to_score_maps(short_df, "score_mod").get(scan_date, {})
    long_score_cont   = _rows_to_score_maps(long_df,  "score_cont").get(scan_date, {})
    short_score_cont  = _rows_to_score_maps(short_df, "score_cont").get(scan_date, {})
    long_score_huge   = _rows_to_score_maps(long_df,  "score_huge").get(scan_date, {})
    short_score_huge  = _rows_to_score_maps(short_df, "score_huge").get(scan_date, {})
    combined_scores   = _combine_score_maps(
        {scan_date: long_scores}, {scan_date: short_scores}
    ).get(scan_date, {})
    combined_list     = list(combined_scores.keys())
    factors_computed  = _counts_by_date(raw_df).get(scan_date, 0)

    return {
        "long":     long_list,
        "short":    short_list,
        "combined": combined_list,
        "scores": {
            "long":     long_scores,
            "short":    short_scores,
            "combined": combined_scores,
        },
        "component_scores": {
            "score_mod":  {"long": long_score_mod,  "short": short_score_mod},
            "score_cont": {"long": long_score_cont, "short": short_score_cont},
            "score_huge": {"long": long_score_huge, "short": short_score_huge},
        },
        "counts": {
            "universe":         universe_size,
            "factors_computed": factors_computed,
            "skipped":          universe_size - factors_computed,
            "long":             len(long_list),
            "short":            len(short_list),
            "combined":         len(combined_list),
        },
    }


def _build_history_payload(
    tickers: Sequence[str],
    morning_raw: pd.DataFrame,
    mid_raw: pd.DataFrame,
    afternoon_raw: pd.DataFrame,
    morning_long: pd.DataFrame,
    morning_short: pd.DataFrame,
    mid_long: pd.DataFrame,
    mid_short: pd.DataFrame,
    afternoon_long: pd.DataFrame,
    afternoon_short: pd.DataFrame,
    requested_start: date,
    requested_end: date,
) -> Dict[str, object]:
    all_dates = sorted(
        set(morning_raw.get("scan_date", pd.Series(dtype=object)).tolist())
        | set(mid_raw.get("scan_date", pd.Series(dtype=object)).tolist())
        | set(afternoon_raw.get("scan_date", pd.Series(dtype=object)).tolist())
    )
    actual_start = min(all_dates).isoformat() if all_dates else None
    actual_end   = max(all_dates).isoformat() if all_dates else None
    universe_size = len(tickers)

    shortlist_by_date: Dict[str, Dict[str, object]] = {}
    for scan_date in all_dates:
        shortlist_by_date[scan_date.isoformat()] = {
            "morning":   _build_session_block(morning_long,   morning_short,   morning_raw,   universe_size, scan_date),
            "mid":       _build_session_block(mid_long,       mid_short,       mid_raw,       universe_size, scan_date),
            "afternoon": _build_session_block(afternoon_long, afternoon_short, afternoon_raw, universe_size, scan_date),
        }

    return {
        "generated_at":        datetime.now(IST).isoformat(),
        "requested_start_date": requested_start.isoformat(),
        "requested_end_date":   requested_end.isoformat(),
        "actual_start_date":    actual_start,
        "actual_end_date":      actual_end,
        "data_5m_dir":          str(HIST_5M_DIR),
        "data_daily_dir":       str(DAILY_DIR),
        "universe_size":        universe_size,
        "config": {
            "version": "v3_3times",
            "windows": "W1=morning(09:30-11:00) W2=mid(11:15-12:45) W3=afternoon(13:00-14:30)",
            "morning":   {"rvol_min": shortlist_base.MORNING_RVOL_MIN,   "orp_atr_min": shortlist_base.MORNING_ORP_ATR_MIN,   "clv_min": shortlist_base.MORNING_CLV_MIN,   "tv_min_long": shortlist_base.MORNING_TV_MIN_LONG,   "tv_min_short": shortlist_base.MORNING_TV_MIN_SHORT,   "top_n": shortlist_base.MORNING_TOP_N},
            "mid":       {"rvol_min": shortlist_base.MID_RVOL_MIN,       "orp_atr_min": shortlist_base.MID_ORP_ATR_MIN,       "clv_min": shortlist_base.MID_CLV_MIN,       "tv_min_long": shortlist_base.MID_TV_MIN_LONG,       "tv_min_short": shortlist_base.MID_TV_MIN_SHORT,       "top_n": shortlist_base.MID_TOP_N},
            "afternoon": {"rvol_min": shortlist_base.AFTERNOON_RVOL_MIN, "orp_atr_min": shortlist_base.AFTERNOON_ORP_ATR_MIN, "clv_min": shortlist_base.AFTERNOON_CLV_MIN, "tv_min_long": shortlist_base.AFTERNOON_TV_MIN_LONG, "tv_min_short": shortlist_base.AFTERNOON_TV_MIN_SHORT, "top_n": shortlist_base.AFTERNOON_TOP_N},
        },
        "shortlists": shortlist_by_date,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RENDER MODULE
# ─────────────────────────────────────────────────────────────────────────────

def _render_module(payload: Dict[str, object]) -> str:
    shortlists = pformat(payload["shortlists"], width=120, sort_dicts=False)
    config     = pformat(payload["config"],     width=120, sort_dicts=False)
    return (
        "# Auto-generated by two_session_shortlist_history_builder_v3_3times.py\n"
        "# THREE-WINDOW shortlist: morning / mid / afternoon\n"
        "from __future__ import annotations\n\n"
        f"GENERATED_AT = {payload['generated_at']!r}\n"
        f"REQUESTED_START_DATE = {payload['requested_start_date']!r}\n"
        f"REQUESTED_END_DATE = {payload['requested_end_date']!r}\n"
        f"ACTUAL_START_DATE = {payload['actual_start_date']!r}\n"
        f"ACTUAL_END_DATE = {payload['actual_end_date']!r}\n"
        f"DATA_5M_DIR = {payload['data_5m_dir']!r}\n"
        f"DATA_DAILY_DIR = {payload['data_daily_dir']!r}\n"
        f"UNIVERSE_SIZE = {payload['universe_size']!r}\n"
        f"CONFIG = {config}\n\n"
        f"SHORTLISTS = {shortlists}\n\n"
        "def available_dates():\n"
        "    return tuple(SHORTLISTS.keys())\n\n"
        "def get_day_payload(scan_date):\n"
        "    return SHORTLISTS.get(str(scan_date))\n\n"
        "def get_session_payload(scan_date, session):\n"
        "    day = SHORTLISTS.get(str(scan_date))\n"
        "    if day is None:\n"
        "        return None\n"
        "    return day.get(str(session).lower())\n\n"
        "def get_shortlist(scan_date, session, side='combined', as_set=False):\n"
        "    sp = get_session_payload(scan_date, session)\n"
        "    if sp is None:\n"
        "        return set() if as_set else []\n"
        "    values = list(sp.get(str(side).lower(), []))\n"
        "    return set(values) if as_set else values\n\n"
        "def get_score_map(scan_date, session, side='combined'):\n"
        "    sp = get_session_payload(scan_date, session)\n"
        "    if sp is None:\n"
        "        return {}\n"
        "    return dict(sp.get('scores', {}).get(str(side).lower(), {}))\n\n"
        "def get_shortlist_entries(scan_date, session, side='combined'):\n"
        "    values = list(get_shortlist(scan_date, session, side=side, as_set=False))\n"
        "    score_map = get_score_map(scan_date, session, side=side)\n"
        "    total = len(values)\n"
        "    return [\n"
        "        {'ticker': t, 'score': float(score_map.get(t, 0.0)),\n"
        "         'rank': i, 'rank_pct': float(i / total) if total else 1.0}\n"
        "        for i, t in enumerate(values, start=1)\n"
        "    ]\n\n"
        "def get_shortlist_score(scan_date, session, side, ticker, default=None):\n"
        "    return get_score_map(scan_date, session, side=side).get(str(ticker), default)\n\n"
        "def get_counts(scan_date, session):\n"
        "    sp = get_session_payload(scan_date, session)\n"
        "    return dict(sp.get('counts', {})) if sp else {}\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def build_history(start_date: date, end_date: date) -> Dict[str, object]:
    tickers = shortlist_base.load_universe()
    print("=" * 72)
    print("  THREE-WINDOW SHORTLIST HISTORY BUILDER  v3_3times")
    print(f"  Range     : {start_date} -> {end_date}")
    print(f"  Universe  : {len(tickers)} stocks")
    print(f"  5min data : {HIST_5M_DIR}")
    print(f"  Daily data: {DAILY_DIR}")
    print(f"  W1 MORNING   obs 09:15-09:30 (15 min, 3 bars)  -> trade 09:30-11:00")
    print(f"  W2 MID       obs 10:30-11:15 (45 min, 9 bars)  -> trade 11:15-12:45")
    print(f"  W3 AFTERNOON obs 12:30-13:00 (30 min, 6 bars)  -> trade 13:00-14:30")
    print("=" * 72)

    morning_raw, mid_raw, afternoon_raw = _build_raw_frames(tickers, start_date, end_date)
    morning_long,   morning_short   = _apply_session_shortlist(morning_raw,   "morning")
    mid_long,       mid_short       = _apply_session_shortlist(mid_raw,       "mid")
    afternoon_long, afternoon_short = _apply_session_shortlist(afternoon_raw, "afternoon")

    payload = _build_history_payload(
        tickers=tickers,
        morning_raw=morning_raw,     mid_raw=mid_raw,           afternoon_raw=afternoon_raw,
        morning_long=morning_long,   morning_short=morning_short,
        mid_long=mid_long,           mid_short=mid_short,
        afternoon_long=afternoon_long, afternoon_short=afternoon_short,
        requested_start=start_date,  requested_end=end_date,
    )
    return payload


def main():
    parser = argparse.ArgumentParser(description="Three-Window Shortlist History Builder v3_3times")
    parser.add_argument("--start", default=str(DEFAULT_START_DATE), help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default=None,                    help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output .py path")
    args = parser.parse_args()

    start_date = _parse_date(args.start)
    end_date   = _parse_date(args.end) if args.end else datetime.now(IST).date()
    out_path   = Path(args.output)

    payload = build_history(start_date, end_date)
    module_text = _render_module(payload)
    out_path.write_text(module_text, encoding="utf-8")

    shortlists = payload["shortlists"]
    n_dates    = len(shortlists)
    print(f"\n  Written {n_dates} dates to {out_path}")
    for session in ("morning", "mid", "afternoon"):
        total_long  = sum(d[session]["counts"]["long"]  for d in shortlists.values() if session in d)
        total_short = sum(d[session]["counts"]["short"] for d in shortlists.values() if session in d)
        avg_long  = total_long  / n_dates if n_dates else 0
        avg_short = total_short / n_dates if n_dates else 0
        print(f"  {session.upper():12s}  avg long/day={avg_long:.1f}  avg short/day={avg_short:.1f}")
    print()


if __name__ == "__main__":
    main()
