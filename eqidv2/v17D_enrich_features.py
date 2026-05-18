"""v17D feature enrichment — add di_plus / di_minus / atr_pct_rank_60d.

The refined Cand-E4 trades CSV has adx_signal but is missing the DI components
and the 60-day ATR rank that the Pareto search wants. This script:

  1. For each unique (ticker, signal_time_ist) in the trades CSV:
     - Loads the 5min indicator parquet
     - Computes DI+ / DI- via Wilder's (14-period) smoothing
     - Looks up the value at signal_time_ist (-1 bar safety)
  2. For each unique (ticker, trade_date):
     - Loads the daily parquet
     - Computes (ATR / close) per day
     - Computes 60-day rolling rank (0..1) at trade_date

Writes a new trades CSV with the three new columns appended.

Usage:
    python -m eqidv2.v17D_enrich_features \
        --trades outputs_v17D_phase0/trades_candE4_refined_alladv.csv \
        --indicator-dir C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2 \
        --daily-dir     C:/TradingData/eqidv2/stocks_indicators_daily_eq \
        --output  outputs_v17D_phase0/trades_candE4_refined_enriched.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _wilder_rma(values: np.ndarray, period: int) -> np.ndarray:
    """Wilder's smoothed moving average (used for DI+/-/ADX/ATR)."""
    n = len(values)
    out = np.full(n, np.nan, dtype=float)
    if n < period:
        return out
    # Seed with simple average of first period values
    seed = np.nanmean(values[:period])
    out[period - 1] = seed
    for i in range(period, n):
        prev = out[i - 1]
        v = values[i]
        if np.isnan(prev) or np.isnan(v):
            out[i] = prev
        else:
            out[i] = (prev * (period - 1) + v) / period
    return out


def _compute_di_pm(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Add di_plus / di_minus columns to a 5min OHLC DataFrame.

    Standard Wilder formulas:
        +DM = max(high - prev_high, 0) if (high - prev_high) > (prev_low - low) else 0
        -DM = max(prev_low - low,  0) if (prev_low - low)   > (high - prev_high)  else 0
        TR  = max(high - low, abs(high - prev_close), abs(low - prev_close))
        +DI = 100 * Wilder(+DM, period) / Wilder(TR, period)
        -DI = 100 * Wilder(-DM, period) / Wilder(TR, period)
    """
    out = df.copy()
    high = out["high"].to_numpy(dtype=float)
    low = out["low"].to_numpy(dtype=float)
    close = out["close"].to_numpy(dtype=float)

    prev_high = np.concatenate([[np.nan], high[:-1]])
    prev_low = np.concatenate([[np.nan], low[:-1]])
    prev_close = np.concatenate([[np.nan], close[:-1]])

    up_move = high - prev_high
    down_move = prev_low - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = np.maximum.reduce([
        high - low,
        np.abs(high - prev_close),
        np.abs(low - prev_close),
    ])

    pdm_smooth = _wilder_rma(plus_dm, period)
    mdm_smooth = _wilder_rma(minus_dm, period)
    tr_smooth = _wilder_rma(tr, period)

    with np.errstate(divide="ignore", invalid="ignore"):
        di_plus = 100.0 * np.where(tr_smooth > 0, pdm_smooth / tr_smooth, np.nan)
        di_minus = 100.0 * np.where(tr_smooth > 0, mdm_smooth / tr_smooth, np.nan)

    out["di_plus"] = di_plus
    out["di_minus"] = di_minus
    return out


def _normalize_5min(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.set_index("date").sort_index()
    # Some parquets ship duplicate timestamps (rebroadcast slots, etc.)
    # — keep the last occurrence so the index is unique for get_indexer().
    df = df[~df.index.duplicated(keep="last")]
    return df


def _compute_atr_rank(daily_df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """Add atr_pct, atr_pct_rank_60d to a daily DataFrame indexed by date."""
    out = daily_df.copy()
    out["date_only"] = pd.to_datetime(out["date"]).dt.date
    out["atr_pct"] = pd.to_numeric(out["ATR"], errors="coerce") / \
                     pd.to_numeric(out["close"], errors="coerce")
    # Rank in [0,1] across last `window` rows
    out["atr_pct_rank_60d"] = (
        out["atr_pct"]
        .rolling(window=window, min_periods=max(20, window // 2))
        .rank(pct=True)
    )
    return out


def enrich(
    trades: pd.DataFrame, indicator_dir: Path, daily_dir: Path,
    period_di: int = 14, rank_window: int = 60,
) -> pd.DataFrame:
    out = trades.copy()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"])
    if out["signal_time_ist"].dt.tz is None:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_localize("Asia/Kolkata")
    else:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_convert("Asia/Kolkata")
    out["trade_date_d"] = pd.to_datetime(out["trade_date"]).dt.date

    cache_5m: dict = {}
    cache_d: dict = {}

    di_plus_col, di_minus_col, atr_rank_col = [], [], []
    n_miss_5m, n_miss_d = 0, 0

    for _, t in out.iterrows():
        tk = str(t["ticker"])

        # ----- 5min DI+/-
        if tk not in cache_5m:
            p = indicator_dir / f"{tk}_stocks_indicators_5min.parquet"
            if not p.exists():
                cache_5m[tk] = None
            else:
                df5 = pd.read_parquet(
                    p, columns=["date", "high", "low", "close"]
                )
                df5 = _normalize_5min(df5)
                df5 = _compute_di_pm(df5, period=period_di)
                cache_5m[tk] = df5
        df5 = cache_5m[tk]
        if df5 is None:
            n_miss_5m += 1
            di_plus_col.append(np.nan)
            di_minus_col.append(np.nan)
        else:
            try:
                # Use the bar at or just before signal_time_ist
                ts = t["signal_time_ist"]
                pos = df5.index.get_indexer([ts], method="pad")[0]
                if pos < 0:
                    di_plus_col.append(np.nan)
                    di_minus_col.append(np.nan)
                else:
                    di_plus_col.append(float(df5["di_plus"].iloc[pos]))
                    di_minus_col.append(float(df5["di_minus"].iloc[pos]))
            except Exception:
                di_plus_col.append(np.nan)
                di_minus_col.append(np.nan)

        # ----- daily ATR rank
        if tk not in cache_d:
            p = daily_dir / f"{tk}_stocks_indicators_daily.parquet"
            if not p.exists():
                cache_d[tk] = None
            else:
                dfd = pd.read_parquet(p, columns=["date", "close", "ATR"])
                dfd = _compute_atr_rank(dfd, window=rank_window)
                cache_d[tk] = dfd.set_index("date_only").sort_index()
        dfd = cache_d[tk]
        if dfd is None:
            n_miss_d += 1
            atr_rank_col.append(np.nan)
        else:
            try:
                d = t["trade_date_d"]
                if d in dfd.index:
                    atr_rank_col.append(float(dfd.loc[d, "atr_pct_rank_60d"]))
                else:
                    # Use last value on or before d
                    sub = dfd[dfd.index <= d]
                    if sub.empty:
                        atr_rank_col.append(np.nan)
                    else:
                        atr_rank_col.append(float(sub["atr_pct_rank_60d"].iloc[-1]))
            except Exception:
                atr_rank_col.append(np.nan)

    out["di_plus"] = di_plus_col
    out["di_minus"] = di_minus_col
    out["atr_pct_rank_60d"] = atr_rank_col
    out = out.drop(columns=["trade_date_d"])

    print(f"  enriched: 5min-miss={n_miss_5m}, daily-miss={n_miss_d}")
    di_valid = pd.Series(di_plus_col).notna().sum()
    rank_valid = pd.Series(atr_rank_col).notna().sum()
    print(f"  DI+/- valid: {di_valid}/{len(out)}; ATR-rank valid: {rank_valid}/{len(out)}")
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--indicator-dir", required=True)
    p.add_argument("--daily-dir", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()

    trades = pd.read_csv(args.trades)
    ind = Path(args.indicator_dir)
    d = Path(args.daily_dir)
    if not ind.is_dir() or not d.is_dir():
        raise SystemExit("indicator-dir or daily-dir missing")

    print(f"v17D enrich features — {len(trades)} input trades")
    out = enrich(trades, ind, d)
    out.to_csv(args.output, index=False)
    print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()
