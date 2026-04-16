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

import two_session_shortlist_test_v2 as shortlist_base

IST = shortlist_base.IST
HIST_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq")
DAILY_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_daily_eq")
DEFAULT_OUTPUT_PATH = SCRIPT_DIR / "filtered_stocks_two_session_history_v2.py"
DEFAULT_START_DATE = date(2025, 6, 1)


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
    hist = df.groupby("ticker")[col].transform(_safe_rank)
    hist = hist.fillna(cross)
    return (cross_weight * cross) + ((1.0 - cross_weight) * hist)


def _clip_series(series: pd.Series, lo: float = 0.0, hi: float = 1.0) -> pd.Series:
    return series.clip(lower=lo, upper=hi)


def _combine_lists(primary: Sequence[str], secondary: Sequence[str]) -> List[str]:
    seen = set()
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
    session_name: str,
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

    # Base agg — always-present columns
    agg_spec: dict = {
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close_end": ("close", "last"),
        "total_vol": ("volume", "sum"),
        "tv": ("turnover", "sum"),
        "first_open": ("open", "first"),
    }
    # Optional indicator columns — only include if they exist in the window
    _OPT_COLS = {
        "vwap_last": "VWAP",
        "ema20_last": "EMA_20",
        "upper_band_last": "Upper_Band",
        "lower_band_last": "Lower_Band",
    }
    _present_opt = {alias: (src, "last") for alias, src in _OPT_COLS.items() if src in window.columns}
    agg_spec.update(_present_opt)

    summary = grouped.agg(**agg_spec)

    # Fill missing optional columns with NaN so downstream code is uniform
    for alias in _OPT_COLS:
        if alias not in summary.columns:
            summary[alias] = np.nan

    daily_close = df5.groupby("_date", sort=True)["close"].last().sort_index()
    prev_close = daily_close.shift(1).reindex(summary.index)
    if "Prev_Day_Close" in window.columns:
        prev_day_close = grouped["Prev_Day_Close"].first()
        prev_close = prev_close.combine_first(prev_day_close)

    vol_series = summary["total_vol"].where(summary["total_vol"] > 0)
    hist_mean = vol_series.shift(1).rolling(shortlist_base.RVOL_LOOKBACK_SESSIONS, min_periods=1).mean()
    tv_hist_mean = summary["tv"].where(summary["tv"] > 0).shift(1).rolling(
        shortlist_base.TURNOVER_LOOKBACK_SESSIONS,
        min_periods=1,
    ).mean()

    summary["scan_date"] = summary.index
    summary["ticker"] = ticker
    summary["atr14"] = summary.index.to_series().map(atr_by_date)
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

    span = summary["high"] - summary["low"]
    summary = summary[(summary["atr14"].notna()) & (summary["atr14"] > 0) & (span > 0)].copy()
    if summary.empty:
        return pd.DataFrame()

    span = summary["high"] - summary["low"]
    summary["orp_atr"] = span / summary["atr14"]
    summary["clv_long"] = (summary["close_end"] - summary["low"]) / span
    summary["clv_short"] = (summary["high"] - summary["close_end"]) / span
    stock_ret = np.where(
        summary["first_open"] > 0,
        ((summary["close_end"] - summary["first_open"]) / summary["first_open"]) * 100.0,
        0.0,
    )
    market_ret = summary.index.to_series().map(market_return_map).fillna(0.0)
    summary["rs_long"] = stock_ret - market_ret
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
    summary["body_eff_long"] = ((summary["close_end"] - summary["first_open"]).clip(lower=0.0) / span).clip(0.0, 1.0)
    summary["body_eff_short"] = ((summary["first_open"] - summary["close_end"]).clip(lower=0.0) / span).clip(0.0, 1.0)

    if session_name == "morning":
        summary = summary[(summary["prev_close"].notna()) & (summary["prev_close"] > 0)].copy()
        if summary.empty:
            return pd.DataFrame()

        summary["gap_pct"] = ((summary["first_open"] - summary["prev_close"]) / summary["prev_close"]) * 100.0
        summary["gap_hold_long"] = 0
        summary["gap_hold_short"] = 0

        gap_up = summary["gap_pct"] > shortlist_base.GAP_THRESHOLD_PCT
        gap_down = summary["gap_pct"] < -shortlist_base.GAP_THRESHOLD_PCT
        flat_open = ~(gap_up | gap_down)

        summary.loc[
            gap_up & (summary["low"] > summary["prev_close"]) & (summary["clv_long"] >= 0.60),
            "gap_hold_long",
        ] = 1
        summary.loc[
            gap_down & (summary["high"] < summary["prev_close"]) & (summary["clv_short"] >= 0.60),
            "gap_hold_short",
        ] = 1
        summary.loc[
            flat_open
            & (summary["clv_long"] >= 0.65)
            & (summary["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN),
            "gap_hold_long",
        ] = 1
        summary.loc[
            flat_open
            & (summary["clv_short"] >= 0.65)
            & (summary["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN),
            "gap_hold_short",
        ] = 1
    else:
        summary["gap_pct"] = np.nan
        summary["gap_hold_long"] = 0
        summary["gap_hold_short"] = 0

    # Upper/Lower Band exhaustion clauses — only fire when band data is available
    _ub = pd.to_numeric(summary["upper_band_last"], errors="coerce")
    _lb = pd.to_numeric(summary["lower_band_last"], errors="coerce")
    _ub_valid = pd.notna(_ub)
    _lb_valid = pd.notna(_lb)

    if session_name == "morning":
        summary["exhaust_long"] = (
            ((summary["rvol"] >= 3.0) & (summary["clv_long"] >= 0.92) & (summary["vwap_edge_long"] >= 0.80))
            | (_ub_valid & (summary["close_end"] >= _ub) & (summary["rvol"] >= 2.0))
        ).astype(int)
        summary["exhaust_short"] = (
            ((summary["rvol"] >= 3.0) & (summary["clv_short"] >= 0.92) & (summary["vwap_edge_short"] >= 0.80))
            | (_lb_valid & (summary["close_end"] <= _lb) & (summary["rvol"] >= 2.0))
        ).astype(int)
    else:
        summary["exhaust_long"] = (
            ((summary["rvol"] >= 2.6) & (summary["clv_long"] >= 0.92) & (summary["vwap_edge_long"] >= 0.70))
            | (_ub_valid & (summary["close_end"] >= _ub) & (summary["rvol"] >= 1.8))
        ).astype(int)
        summary["exhaust_short"] = (
            ((summary["rvol"] >= 2.6) & (summary["clv_short"] >= 0.92) & (summary["vwap_edge_short"] >= 0.70))
            | (_lb_valid & (summary["close_end"] <= _lb) & (summary["rvol"] >= 1.8))
        ).astype(int)

    keep_cols = [
        "scan_date",
        "ticker",
        "rvol",
        "tv_ratio",
        "orp_atr",
        "clv_long",
        "clv_short",
        "rs_long",
        "rs_short",
        "vwap_edge_long",
        "vwap_edge_short",
        "ema20_edge_long",
        "ema20_edge_short",
        "body_eff_long",
        "body_eff_short",
        "exhaust_long",
        "exhaust_short",
        "gap_hold_long",
        "gap_hold_short",
        "gap_pct",
        "tv",
        "total_vol",
        "atr14",
        "prev_close",
        "high",
        "low",
        "close_end",
        "first_open",
    ]
    return summary.reset_index(drop=True)[keep_cols]


def _build_raw_frames(
    tickers: Sequence[str],
    start_date: date,
    end_date: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    morning_frames: List[pd.DataFrame] = []
    afternoon_frames: List[pd.DataFrame] = []
    morning_market_map = _build_market_return_map(shortlist_base.MORNING_OBS_TIMES, start_date, end_date)
    afternoon_market_map = _build_market_return_map(shortlist_base.AFTERNOON_OBS_TIMES, start_date, end_date)

    total = len(tickers)
    for idx, ticker in enumerate(tickers, start=1):
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(f"  [BUILD] {idx}/{total} tickers processed...", flush=True)

        df5 = _load_intraday(ticker)
        if df5 is None or df5.empty:
            continue

        df_daily = _load_daily(ticker)
        atr_by_date = _build_atr_by_date(df_daily)
        if not atr_by_date:
            continue

        morning_df = _session_summary(
            ticker=ticker,
            df5=df5,
            atr_by_date=atr_by_date,
            market_return_map=morning_market_map,
            obs_times=shortlist_base.MORNING_OBS_TIMES,
            session_name="morning",
            start_date=start_date,
            end_date=end_date,
        )
        if not morning_df.empty:
            morning_frames.append(morning_df)

        afternoon_df = _session_summary(
            ticker=ticker,
            df5=df5,
            atr_by_date=atr_by_date,
            market_return_map=afternoon_market_map,
            obs_times=shortlist_base.AFTERNOON_OBS_TIMES,
            session_name="afternoon",
            start_date=start_date,
            end_date=end_date,
        )
        if not afternoon_df.empty:
            afternoon_frames.append(afternoon_df)

    morning = pd.concat(morning_frames, ignore_index=True) if morning_frames else pd.DataFrame()
    afternoon = pd.concat(afternoon_frames, ignore_index=True) if afternoon_frames else pd.DataFrame()
    return morning, afternoon


def _apply_morning_shortlist(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    long_mask = (
        (raw_df["rvol"] >= shortlist_base.MORNING_RVOL_MIN)
        & (raw_df["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN)
        & (raw_df["clv_long"] >= shortlist_base.MORNING_CLV_MIN)
        & (raw_df["tv"] >= shortlist_base.MORNING_TV_MIN)
    )
    short_mask = (
        (raw_df["rvol"] >= shortlist_base.MORNING_RVOL_MIN)
        & (raw_df["orp_atr"] >= shortlist_base.MORNING_ORP_ATR_MIN)
        & (raw_df["clv_short"] >= shortlist_base.MORNING_CLV_MIN)
        & (raw_df["tv"] >= shortlist_base.MORNING_TV_MIN)
    )

    long_df = raw_df.loc[long_mask].copy()
    short_df = raw_df.loc[short_mask].copy()

    if not long_df.empty:
        long_df["score_rvol"] = _hybrid_rank(long_df, "rvol")
        long_df["score_tv_ratio"] = _hybrid_rank(long_df, "tv_ratio")
        long_df["score_orp_atr"] = _hybrid_rank(long_df, "orp_atr")
        long_df["score_clv"] = _hybrid_rank(long_df, "clv_long")
        long_df["score_gap_hold"] = long_df.groupby("scan_date")["gap_hold_long"].transform(_safe_rank)
        long_df["score_rs"] = _hybrid_rank(long_df, "rs_long")
        long_df["score_vwap_edge"] = _hybrid_rank(long_df, "vwap_edge_long")
        long_df["score_ema20_edge"] = _hybrid_rank(long_df, "ema20_edge_long")
        long_df["score_body_eff"] = _hybrid_rank(long_df, "body_eff_long")
        long_df["score"] = _clip_series(
            shortlist_base.MORNING_WEIGHTS["rvol"] * long_df["score_rvol"]
            + shortlist_base.MORNING_WEIGHTS["tv_ratio"] * long_df["score_tv_ratio"]
            + shortlist_base.MORNING_WEIGHTS["orp_atr"] * long_df["score_orp_atr"]
            + shortlist_base.MORNING_WEIGHTS["clv"] * long_df["score_clv"]
            + shortlist_base.MORNING_WEIGHTS["gap_hold"] * long_df["score_gap_hold"]
            + shortlist_base.MORNING_WEIGHTS["rs"] * long_df["score_rs"]
            + shortlist_base.MORNING_WEIGHTS["vwap_edge"] * long_df["score_vwap_edge"]
            + shortlist_base.MORNING_WEIGHTS["ema20_edge"] * long_df["score_ema20_edge"]
            + shortlist_base.MORNING_WEIGHTS["body_eff"] * long_df["score_body_eff"]
            - shortlist_base.MORNING_EXHAUSTION_PENALTY * long_df["exhaust_long"].astype(float)
        )
        long_df["score_mod"] = _clip_series(
            0.24 * long_df["score_orp_atr"]
            + 0.18 * long_df["score_clv"]
            + 0.18 * long_df["score_rs"]
            + 0.14 * long_df["score_vwap_edge"]
            + 0.10 * long_df["score_gap_hold"]
            + 0.08 * long_df["score_body_eff"]
            + 0.08 * long_df["score_tv_ratio"]
            - 0.08 * long_df["exhaust_long"].astype(float)
        )
        long_df["score_cont"] = _clip_series(
            0.24 * long_df["score_rs"]
            + 0.18 * long_df["score_vwap_edge"]
            + 0.12 * long_df["score_ema20_edge"]
            + 0.12 * long_df["score_body_eff"]
            + 0.12 * long_df["score_clv"]
            + 0.12 * long_df["score_rvol"]
            + 0.10 * long_df["score_tv_ratio"]
            - 0.10 * long_df["exhaust_long"].astype(float)
        )
        long_df["score_huge"] = _clip_series(
            0.24 * long_df["score_rvol"]
            + 0.18 * long_df["score_tv_ratio"]
            + 0.18 * long_df["score_orp_atr"]
            + 0.16 * long_df["score_rs"]
            + 0.12 * long_df["score_clv"]
            + 0.12 * long_df["score_gap_hold"]
            - 0.04 * long_df["exhaust_long"].astype(float)
        )
        long_df = long_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if shortlist_base.MORNING_TOP_N:
            long_df = long_df.groupby("scan_date", sort=False).head(shortlist_base.MORNING_TOP_N)

    if not short_df.empty:
        short_df["score_rvol"] = _hybrid_rank(short_df, "rvol")
        short_df["score_tv_ratio"] = _hybrid_rank(short_df, "tv_ratio")
        short_df["score_orp_atr"] = _hybrid_rank(short_df, "orp_atr")
        short_df["score_clv"] = _hybrid_rank(short_df, "clv_short")
        short_df["score_gap_hold"] = short_df.groupby("scan_date")["gap_hold_short"].transform(_safe_rank)
        short_df["score_rs"] = _hybrid_rank(short_df, "rs_short")
        short_df["score_vwap_edge"] = _hybrid_rank(short_df, "vwap_edge_short")
        short_df["score_ema20_edge"] = _hybrid_rank(short_df, "ema20_edge_short")
        short_df["score_body_eff"] = _hybrid_rank(short_df, "body_eff_short")
        short_df["score"] = _clip_series(
            shortlist_base.MORNING_WEIGHTS["rvol"] * short_df["score_rvol"]
            + shortlist_base.MORNING_WEIGHTS["tv_ratio"] * short_df["score_tv_ratio"]
            + shortlist_base.MORNING_WEIGHTS["orp_atr"] * short_df["score_orp_atr"]
            + shortlist_base.MORNING_WEIGHTS["clv"] * short_df["score_clv"]
            + shortlist_base.MORNING_WEIGHTS["gap_hold"] * short_df["score_gap_hold"]
            + shortlist_base.MORNING_WEIGHTS["rs"] * short_df["score_rs"]
            + shortlist_base.MORNING_WEIGHTS["vwap_edge"] * short_df["score_vwap_edge"]
            + shortlist_base.MORNING_WEIGHTS["ema20_edge"] * short_df["score_ema20_edge"]
            + shortlist_base.MORNING_WEIGHTS["body_eff"] * short_df["score_body_eff"]
            - shortlist_base.MORNING_EXHAUSTION_PENALTY * short_df["exhaust_short"].astype(float)
        )
        short_df["score_mod"] = _clip_series(
            0.24 * short_df["score_orp_atr"]
            + 0.18 * short_df["score_clv"]
            + 0.18 * short_df["score_rs"]
            + 0.14 * short_df["score_vwap_edge"]
            + 0.10 * short_df["score_gap_hold"]
            + 0.08 * short_df["score_body_eff"]
            + 0.08 * short_df["score_tv_ratio"]
            - 0.08 * short_df["exhaust_short"].astype(float)
        )
        short_df["score_cont"] = _clip_series(
            0.24 * short_df["score_rs"]
            + 0.18 * short_df["score_vwap_edge"]
            + 0.12 * short_df["score_ema20_edge"]
            + 0.12 * short_df["score_body_eff"]
            + 0.12 * short_df["score_clv"]
            + 0.12 * short_df["score_rvol"]
            + 0.10 * short_df["score_tv_ratio"]
            - 0.10 * short_df["exhaust_short"].astype(float)
        )
        short_df["score_huge"] = _clip_series(
            0.24 * short_df["score_rvol"]
            + 0.18 * short_df["score_tv_ratio"]
            + 0.18 * short_df["score_orp_atr"]
            + 0.16 * short_df["score_rs"]
            + 0.12 * short_df["score_clv"]
            + 0.12 * short_df["score_gap_hold"]
            - 0.04 * short_df["exhaust_short"].astype(float)
        )
        short_df = short_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if shortlist_base.MORNING_TOP_N:
            short_df = short_df.groupby("scan_date", sort=False).head(shortlist_base.MORNING_TOP_N)

    return long_df, short_df


def _apply_afternoon_shortlist(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    long_mask = (
        (raw_df["rvol"] >= shortlist_base.AFTERNOON_RVOL_MIN)
        & (raw_df["orp_atr"] >= shortlist_base.AFTERNOON_ORP_ATR_MIN)
        & (raw_df["clv_long"] >= shortlist_base.AFTERNOON_CLV_MIN)
        & (raw_df["tv"] >= shortlist_base.AFTERNOON_TV_MIN)
    )
    short_mask = (
        (raw_df["rvol"] >= shortlist_base.AFTERNOON_RVOL_MIN)
        & (raw_df["orp_atr"] >= shortlist_base.AFTERNOON_ORP_ATR_MIN)
        & (raw_df["clv_short"] >= shortlist_base.AFTERNOON_CLV_MIN)
        & (raw_df["tv"] >= shortlist_base.AFTERNOON_TV_MIN)
    )

    long_df = raw_df.loc[long_mask].copy()
    short_df = raw_df.loc[short_mask].copy()

    if not long_df.empty:
        long_df["score_rvol"] = _hybrid_rank(long_df, "rvol")
        long_df["score_tv_ratio"] = _hybrid_rank(long_df, "tv_ratio")
        long_df["score_orp_atr"] = _hybrid_rank(long_df, "orp_atr")
        long_df["score_clv"] = _hybrid_rank(long_df, "clv_long")
        long_df["score_rs"] = _hybrid_rank(long_df, "rs_long")
        long_df["score_vwap_edge"] = _hybrid_rank(long_df, "vwap_edge_long")
        long_df["score_ema20_edge"] = _hybrid_rank(long_df, "ema20_edge_long")
        long_df["score_body_eff"] = _hybrid_rank(long_df, "body_eff_long")
        long_df["score"] = _clip_series(
            shortlist_base.AFTERNOON_WEIGHTS["rvol"] * long_df["score_rvol"]
            + shortlist_base.AFTERNOON_WEIGHTS["tv_ratio"] * long_df["score_tv_ratio"]
            + shortlist_base.AFTERNOON_WEIGHTS["orp_atr"] * long_df["score_orp_atr"]
            + shortlist_base.AFTERNOON_WEIGHTS["clv"] * long_df["score_clv"]
            + shortlist_base.AFTERNOON_WEIGHTS["rs"] * long_df["score_rs"]
            + shortlist_base.AFTERNOON_WEIGHTS["vwap_edge"] * long_df["score_vwap_edge"]
            + shortlist_base.AFTERNOON_WEIGHTS["ema20_edge"] * long_df["score_ema20_edge"]
            + shortlist_base.AFTERNOON_WEIGHTS["body_eff"] * long_df["score_body_eff"]
            - shortlist_base.AFTERNOON_EXHAUSTION_PENALTY * long_df["exhaust_long"].astype(float)
        )
        long_df["score_mod"] = _clip_series(
            0.22 * long_df["score_orp_atr"]
            + 0.18 * long_df["score_clv"]
            + 0.20 * long_df["score_rs"]
            + 0.16 * long_df["score_vwap_edge"]
            + 0.10 * long_df["score_body_eff"]
            + 0.14 * long_df["score_tv_ratio"]
            - 0.08 * long_df["exhaust_long"].astype(float)
        )
        long_df["score_cont"] = _clip_series(
            0.26 * long_df["score_rs"]
            + 0.20 * long_df["score_vwap_edge"]
            + 0.14 * long_df["score_ema20_edge"]
            + 0.12 * long_df["score_body_eff"]
            + 0.12 * long_df["score_clv"]
            + 0.08 * long_df["score_rvol"]
            + 0.08 * long_df["score_tv_ratio"]
            - 0.10 * long_df["exhaust_long"].astype(float)
        )
        long_df["score_huge"] = _clip_series(
            0.24 * long_df["score_rvol"]
            + 0.20 * long_df["score_tv_ratio"]
            + 0.18 * long_df["score_orp_atr"]
            + 0.18 * long_df["score_rs"]
            + 0.10 * long_df["score_clv"]
            + 0.10 * long_df["score_vwap_edge"]
            - 0.04 * long_df["exhaust_long"].astype(float)
        )
        long_df = long_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if shortlist_base.AFTERNOON_TOP_N:
            long_df = long_df.groupby("scan_date", sort=False).head(shortlist_base.AFTERNOON_TOP_N)

    if not short_df.empty:
        short_df["score_rvol"] = _hybrid_rank(short_df, "rvol")
        short_df["score_tv_ratio"] = _hybrid_rank(short_df, "tv_ratio")
        short_df["score_orp_atr"] = _hybrid_rank(short_df, "orp_atr")
        short_df["score_clv"] = _hybrid_rank(short_df, "clv_short")
        short_df["score_rs"] = _hybrid_rank(short_df, "rs_short")
        short_df["score_vwap_edge"] = _hybrid_rank(short_df, "vwap_edge_short")
        short_df["score_ema20_edge"] = _hybrid_rank(short_df, "ema20_edge_short")
        short_df["score_body_eff"] = _hybrid_rank(short_df, "body_eff_short")
        short_df["score"] = _clip_series(
            shortlist_base.AFTERNOON_WEIGHTS["rvol"] * short_df["score_rvol"]
            + shortlist_base.AFTERNOON_WEIGHTS["tv_ratio"] * short_df["score_tv_ratio"]
            + shortlist_base.AFTERNOON_WEIGHTS["orp_atr"] * short_df["score_orp_atr"]
            + shortlist_base.AFTERNOON_WEIGHTS["clv"] * short_df["score_clv"]
            + shortlist_base.AFTERNOON_WEIGHTS["rs"] * short_df["score_rs"]
            + shortlist_base.AFTERNOON_WEIGHTS["vwap_edge"] * short_df["score_vwap_edge"]
            + shortlist_base.AFTERNOON_WEIGHTS["ema20_edge"] * short_df["score_ema20_edge"]
            + shortlist_base.AFTERNOON_WEIGHTS["body_eff"] * short_df["score_body_eff"]
            - shortlist_base.AFTERNOON_EXHAUSTION_PENALTY * short_df["exhaust_short"].astype(float)
        )
        short_df["score_mod"] = _clip_series(
            0.22 * short_df["score_orp_atr"]
            + 0.18 * short_df["score_clv"]
            + 0.20 * short_df["score_rs"]
            + 0.16 * short_df["score_vwap_edge"]
            + 0.10 * short_df["score_body_eff"]
            + 0.14 * short_df["score_tv_ratio"]
            - 0.08 * short_df["exhaust_short"].astype(float)
        )
        short_df["score_cont"] = _clip_series(
            0.26 * short_df["score_rs"]
            + 0.20 * short_df["score_vwap_edge"]
            + 0.14 * short_df["score_ema20_edge"]
            + 0.12 * short_df["score_body_eff"]
            + 0.12 * short_df["score_clv"]
            + 0.08 * short_df["score_rvol"]
            + 0.08 * short_df["score_tv_ratio"]
            - 0.10 * short_df["exhaust_short"].astype(float)
        )
        short_df["score_huge"] = _clip_series(
            0.24 * short_df["score_rvol"]
            + 0.20 * short_df["score_tv_ratio"]
            + 0.18 * short_df["score_orp_atr"]
            + 0.18 * short_df["score_rs"]
            + 0.10 * short_df["score_clv"]
            + 0.10 * short_df["score_vwap_edge"]
            - 0.04 * short_df["exhaust_short"].astype(float)
        )
        short_df = short_df.sort_values(["scan_date", "score", "ticker"], ascending=[True, False, True])
        if shortlist_base.AFTERNOON_TOP_N:
            short_df = short_df.groupby("scan_date", sort=False).head(shortlist_base.AFTERNOON_TOP_N)

    return long_df, short_df


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
    all_dates = sorted(set(primary) | set(secondary))
    for scan_date in all_dates:
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


def _build_history_payload(
    tickers: Sequence[str],
    morning_raw: pd.DataFrame,
    afternoon_raw: pd.DataFrame,
    morning_long: pd.DataFrame,
    morning_short: pd.DataFrame,
    afternoon_long: pd.DataFrame,
    afternoon_short: pd.DataFrame,
    requested_start: date,
    requested_end: date,
) -> Dict[str, object]:
    all_dates = sorted(
        set(morning_raw.get("scan_date", pd.Series(dtype=object)).tolist())
        | set(afternoon_raw.get("scan_date", pd.Series(dtype=object)).tolist())
    )
    actual_start = min(all_dates).isoformat() if all_dates else None
    actual_end = max(all_dates).isoformat() if all_dates else None

    morning_factors = _counts_by_date(morning_raw)
    afternoon_factors = _counts_by_date(afternoon_raw)
    morning_long_map = _rows_to_ranked_lists(morning_long)
    morning_short_map = _rows_to_ranked_lists(morning_short)
    afternoon_long_map = _rows_to_ranked_lists(afternoon_long)
    afternoon_short_map = _rows_to_ranked_lists(afternoon_short)
    morning_long_scores = _rows_to_score_maps(morning_long)
    morning_short_scores = _rows_to_score_maps(morning_short)
    afternoon_long_scores = _rows_to_score_maps(afternoon_long)
    afternoon_short_scores = _rows_to_score_maps(afternoon_short)
    morning_long_score_mod = _rows_to_score_maps(morning_long, "score_mod")
    morning_short_score_mod = _rows_to_score_maps(morning_short, "score_mod")
    afternoon_long_score_mod = _rows_to_score_maps(afternoon_long, "score_mod")
    afternoon_short_score_mod = _rows_to_score_maps(afternoon_short, "score_mod")
    morning_long_score_cont = _rows_to_score_maps(morning_long, "score_cont")
    morning_short_score_cont = _rows_to_score_maps(morning_short, "score_cont")
    afternoon_long_score_cont = _rows_to_score_maps(afternoon_long, "score_cont")
    afternoon_short_score_cont = _rows_to_score_maps(afternoon_short, "score_cont")
    morning_long_score_huge = _rows_to_score_maps(morning_long, "score_huge")
    morning_short_score_huge = _rows_to_score_maps(morning_short, "score_huge")
    afternoon_long_score_huge = _rows_to_score_maps(afternoon_long, "score_huge")
    afternoon_short_score_huge = _rows_to_score_maps(afternoon_short, "score_huge")
    morning_combined_scores = _combine_score_maps(morning_long_scores, morning_short_scores)
    afternoon_combined_scores = _combine_score_maps(afternoon_long_scores, afternoon_short_scores)

    shortlist_by_date: Dict[str, Dict[str, object]] = {}
    universe_size = len(tickers)

    for scan_date in all_dates:
        morning_long_list = morning_long_map.get(scan_date, [])
        morning_short_list = morning_short_map.get(scan_date, [])
        afternoon_long_list = afternoon_long_map.get(scan_date, [])
        afternoon_short_list = afternoon_short_map.get(scan_date, [])
        morning_long_score_map = morning_long_scores.get(scan_date, {})
        morning_short_score_map = morning_short_scores.get(scan_date, {})
        afternoon_long_score_map = afternoon_long_scores.get(scan_date, {})
        afternoon_short_score_map = afternoon_short_scores.get(scan_date, {})
        morning_long_score_mod_map = morning_long_score_mod.get(scan_date, {})
        morning_short_score_mod_map = morning_short_score_mod.get(scan_date, {})
        afternoon_long_score_mod_map = afternoon_long_score_mod.get(scan_date, {})
        afternoon_short_score_mod_map = afternoon_short_score_mod.get(scan_date, {})
        morning_long_score_cont_map = morning_long_score_cont.get(scan_date, {})
        morning_short_score_cont_map = morning_short_score_cont.get(scan_date, {})
        afternoon_long_score_cont_map = afternoon_long_score_cont.get(scan_date, {})
        afternoon_short_score_cont_map = afternoon_short_score_cont.get(scan_date, {})
        morning_long_score_huge_map = morning_long_score_huge.get(scan_date, {})
        morning_short_score_huge_map = morning_short_score_huge.get(scan_date, {})
        afternoon_long_score_huge_map = afternoon_long_score_huge.get(scan_date, {})
        afternoon_short_score_huge_map = afternoon_short_score_huge.get(scan_date, {})
        morning_combined_score_map = morning_combined_scores.get(scan_date, {})
        afternoon_combined_score_map = afternoon_combined_scores.get(scan_date, {})
        morning_combined_list = list(morning_combined_score_map.keys())
        afternoon_combined_list = list(afternoon_combined_score_map.keys())

        shortlist_by_date[scan_date.isoformat()] = {
            "morning": {
                "long": morning_long_list,
                "short": morning_short_list,
                "combined": morning_combined_list,
                "scores": {
                    "long": morning_long_score_map,
                    "short": morning_short_score_map,
                    "combined": morning_combined_score_map,
                },
                "component_scores": {
                    "score_mod": {
                        "long": morning_long_score_mod_map,
                        "short": morning_short_score_mod_map,
                    },
                    "score_cont": {
                        "long": morning_long_score_cont_map,
                        "short": morning_short_score_cont_map,
                    },
                    "score_huge": {
                        "long": morning_long_score_huge_map,
                        "short": morning_short_score_huge_map,
                    },
                },
                "counts": {
                    "universe": universe_size,
                    "factors_computed": morning_factors.get(scan_date, 0),
                    "skipped": universe_size - morning_factors.get(scan_date, 0),
                    "long": len(morning_long_list),
                    "short": len(morning_short_list),
                    "combined": len(morning_combined_list),
                },
            },
            "afternoon": {
                "long": afternoon_long_list,
                "short": afternoon_short_list,
                "combined": afternoon_combined_list,
                "scores": {
                    "long": afternoon_long_score_map,
                    "short": afternoon_short_score_map,
                    "combined": afternoon_combined_score_map,
                },
                "component_scores": {
                    "score_mod": {
                        "long": afternoon_long_score_mod_map,
                        "short": afternoon_short_score_mod_map,
                    },
                    "score_cont": {
                        "long": afternoon_long_score_cont_map,
                        "short": afternoon_short_score_cont_map,
                    },
                    "score_huge": {
                        "long": afternoon_long_score_huge_map,
                        "short": afternoon_short_score_huge_map,
                    },
                },
                "counts": {
                    "universe": universe_size,
                    "factors_computed": afternoon_factors.get(scan_date, 0),
                    "skipped": universe_size - afternoon_factors.get(scan_date, 0),
                    "long": len(afternoon_long_list),
                    "short": len(afternoon_short_list),
                    "combined": len(afternoon_combined_list),
                },
            },
        }

    return {
        "generated_at": datetime.now(IST).isoformat(),
        "requested_start_date": requested_start.isoformat(),
        "requested_end_date": requested_end.isoformat(),
        "actual_start_date": actual_start,
        "actual_end_date": actual_end,
        "data_5m_dir": str(HIST_5M_DIR),
        "data_daily_dir": str(DAILY_DIR),
        "universe_size": len(tickers),
        "config": {
            "version": "v1",
            "morning": {
                "rvol_min": shortlist_base.MORNING_RVOL_MIN,
                "orp_atr_min": shortlist_base.MORNING_ORP_ATR_MIN,
                "clv_min": shortlist_base.MORNING_CLV_MIN,
                "tv_min_rs": shortlist_base.MORNING_TV_MIN,
                "top_n": shortlist_base.MORNING_TOP_N,
            },
            "afternoon": {
                "rvol_min": shortlist_base.AFTERNOON_RVOL_MIN,
                "orp_atr_min": shortlist_base.AFTERNOON_ORP_ATR_MIN,
                "clv_min": shortlist_base.AFTERNOON_CLV_MIN,
                "tv_min_rs": shortlist_base.AFTERNOON_TV_MIN,
                "top_n": shortlist_base.AFTERNOON_TOP_N,
            },
        },
        "shortlists": shortlist_by_date,
    }


def _render_module(payload: Dict[str, object]) -> str:
    shortlists = pformat(payload["shortlists"], width=120, sort_dicts=False)
    config = pformat(payload["config"], width=120, sort_dicts=False)
    return (
        "# Auto-generated by two_session_shortlist_history_builder_v1.py\n"
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
        "    day_payload = SHORTLISTS.get(str(scan_date))\n"
        "    if day_payload is None:\n"
        "        return None\n"
        "    return day_payload.get(str(session).lower())\n\n"
        "def get_shortlist(scan_date, session, side='combined', as_set=False):\n"
        "    session_payload = get_session_payload(scan_date, session)\n"
        "    if session_payload is None:\n"
        "        return set() if as_set else []\n"
        "    values = list(session_payload.get(str(side).lower(), []))\n"
        "    return set(values) if as_set else values\n\n"
        "def get_score_map(scan_date, session, side='combined'):\n"
        "    session_payload = get_session_payload(scan_date, session)\n"
        "    if session_payload is None:\n"
        "        return {}\n"
        "    scores_payload = session_payload.get('scores', {})\n"
        "    return dict(scores_payload.get(str(side).lower(), {}))\n\n"
        "def get_shortlist_entries(scan_date, session, side='combined'):\n"
        "    values = list(get_shortlist(scan_date, session, side=side, as_set=False))\n"
        "    score_map = get_score_map(scan_date, session, side=side)\n"
        "    total = len(values)\n"
        "    out = []\n"
        "    for idx, ticker in enumerate(values, start=1):\n"
        "        rank_pct = (idx / total) if total else 1.0\n"
        "        out.append({\n"
        "            'ticker': ticker,\n"
        "            'score': float(score_map.get(ticker, 0.0)),\n"
        "            'rank': idx,\n"
        "            'rank_pct': float(rank_pct),\n"
        "        })\n"
        "    return out\n\n"
        "def get_shortlist_score(scan_date, session, side, ticker, default=None):\n"
        "    score_map = get_score_map(scan_date, session, side=side)\n"
        "    return score_map.get(str(ticker), default)\n\n"
        "def get_counts(scan_date, session):\n"
        "    session_payload = get_session_payload(scan_date, session)\n"
        "    if session_payload is None:\n"
        "        return {}\n"
        "    return dict(session_payload.get('counts', {}))\n"
    )


def build_history(start_date: date, end_date: date) -> Dict[str, object]:
    tickers = shortlist_base.load_universe()
    print("=" * 72)
    print("  TWO-SESSION SHORTLIST HISTORY BUILDER")
    print(f"  Requested range : {start_date} -> {end_date}")
    print(f"  Universe        : {len(tickers)} stocks")
    print(f"  5min data       : {HIST_5M_DIR}")
    print(f"  Daily data      : {DAILY_DIR}")
    print("=" * 72)

    morning_raw, afternoon_raw = _build_raw_frames(tickers, start_date, end_date)
    morning_long, morning_short = _apply_morning_shortlist(morning_raw)
    afternoon_long, afternoon_short = _apply_afternoon_shortlist(afternoon_raw)

    payload = _build_history_payload(
        tickers=tickers,
        morning_raw=morning_raw,
        afternoon_raw=afternoon_raw,
        morning_long=morning_long,
        morning_short=morning_short,
        afternoon_long=afternoon_long,
        afternoon_short=afternoon_short,
        requested_start=start_date,
        requested_end=end_date,
    )

    print("\n[SUMMARY]")
    print(f"  Actual range    : {payload['actual_start_date']} -> {payload['actual_end_date']}")
    print(f"  Morning dates   : {morning_raw['scan_date'].nunique() if not morning_raw.empty else 0}")
    print(f"  Afternoon dates : {afternoon_raw['scan_date'].nunique() if not afternoon_raw.empty else 0}")
    print(f"  Morning longs   : {len(morning_long)} total shortlisted rows")
    print(f"  Morning shorts  : {len(morning_short)} total shortlisted rows")
    print(f"  Afternoon longs : {len(afternoon_long)} total shortlisted rows")
    print(f"  Afternoon shorts: {len(afternoon_short)} total shortlisted rows")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a date-wise/session-wise historical shortlist module for backtesting."
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="Inclusive start date in YYYY-MM-DD.")
    parser.add_argument(
        "--end-date",
        default=datetime.now(IST).date().isoformat(),
        help="Inclusive end date in YYYY-MM-DD. The builder will clamp to the last available 5-min date.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output Python module path.",
    )
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    requested_end = _parse_date(args.end_date)
    output_path = Path(args.output).resolve()

    sample_ticker = "NIFTYBEES"
    sample_df = _load_intraday(sample_ticker)
    if sample_df is None or sample_df.empty:
        raise SystemExit(f"Unable to read historical 5-min data for {sample_ticker} from {HIST_5M_DIR}")

    latest_available = sample_df["date"].dt.date.max()
    end_date = min(requested_end, latest_available)
    if end_date < start_date:
        raise SystemExit(
            f"Requested range has no overlapping 5-min history. Start={start_date}, latest available={latest_available}"
        )

    payload = build_history(start_date=start_date, end_date=end_date)
    payload["requested_end_date"] = requested_end.isoformat()
    module_text = _render_module(payload)
    output_path.write_text(module_text, encoding="utf-8")

    print(f"\n[FILE SAVED] {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
