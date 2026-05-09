"""v17D Step 2.3 — multi-timeframe trend stack feature precompute.

For each ticker, computes three trend-context features at every 5-min bar:

    htf_15m_ema20_slope_5bar    sign+magnitude of 15-min EMA20 5-bar slope
    htf_60m_ema_stack_bullish   bool: 60-min EMA50 > EMA200
    daily_above_ema20           bool: yesterday's daily close > 20-day EMA

Used in v17D Pareto search as a candidate filter (Step 2.3 in roadmap).
Most count-expensive filter; Pareto search will likely apply only to the
weakest-PF setups.

Public API:
    aggregate_to_15min(df_5min) -> df_15min
    aggregate_to_60min(df_5min) -> df_60min
    aggregate_to_daily(df_5min) -> df_daily
    htf_features_at(stock_df_5min, ts) -> dict
    attach_htf_features(rows, stock_df_5min) -> rows with new cols
"""
from __future__ import annotations

import pandas as pd


def aggregate_to_15min(df_5min: pd.DataFrame) -> pd.DataFrame:
    """5-min OHLCV → 15-min via standard OHLC resample."""
    if df_5min.empty:
        return df_5min
    agg = df_5min.resample("15min", label="right", closed="right").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna(subset=["close"])
    if "EMA_20" not in agg.columns:
        agg["EMA_20"] = agg["close"].ewm(span=20, adjust=False).mean()
    return agg


def aggregate_to_60min(df_5min: pd.DataFrame) -> pd.DataFrame:
    if df_5min.empty:
        return df_5min
    agg = df_5min.resample("60min", label="right", closed="right").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna(subset=["close"])
    agg["EMA_50"] = agg["close"].ewm(span=50, adjust=False).mean()
    agg["EMA_200"] = agg["close"].ewm(span=200, adjust=False).mean()
    return agg


def aggregate_to_daily(df_5min: pd.DataFrame) -> pd.DataFrame:
    """Daily bars from 5-min: group by calendar date in IST."""
    if df_5min.empty:
        return df_5min
    grp = df_5min.groupby(df_5min.index.date)
    daily = pd.DataFrame({
        "open": grp["open"].first(),
        "high": grp["high"].max(),
        "low": grp["low"].min(),
        "close": grp["close"].last(),
        "volume": grp["volume"].sum(),
    })
    daily.index = pd.to_datetime(daily.index)
    daily["EMA_20"] = daily["close"].ewm(span=20, adjust=False).mean()
    return daily


def htf_features_at(
    stock_5min: pd.DataFrame, ts: pd.Timestamp,
) -> dict:
    """Three HTF trend features at timestamp ts (causal: only past bars used)."""
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata")

    df_5 = stock_5min[stock_5min.index <= ts]
    if df_5.empty:
        return {
            "htf_15m_ema20_slope_5bar": float("nan"),
            "htf_60m_ema_stack_bullish": None,
            "daily_above_ema20": None,
        }

    df_15 = aggregate_to_15min(df_5)
    df_60 = aggregate_to_60min(df_5)
    df_d = aggregate_to_daily(df_5)

    # 15-min EMA20 5-bar slope (positive = uptrend)
    slope = float("nan")
    if "EMA_20" in df_15.columns and len(df_15) >= 6:
        e_now = df_15["EMA_20"].iloc[-1]
        e_back = df_15["EMA_20"].iloc[-6]
        if pd.notna(e_now) and pd.notna(e_back):
            slope = float(e_now - e_back)

    # 60-min EMA stack
    ema_stack = None
    if {"EMA_50", "EMA_200"}.issubset(df_60.columns) and not df_60.empty:
        e50 = df_60["EMA_50"].iloc[-1]
        e200 = df_60["EMA_200"].iloc[-1]
        if pd.notna(e50) and pd.notna(e200):
            ema_stack = bool(e50 > e200)

    # Daily: yesterday's close > yesterday's 20-day EMA
    daily_above = None
    today_naive = pd.Timestamp(ts.date())   # tz-naive midnight matching df_d's index
    df_d_prior = df_d[df_d.index < today_naive]
    if not df_d_prior.empty and "EMA_20" in df_d_prior.columns:
        last = df_d_prior.iloc[-1]
        if pd.notna(last["close"]) and pd.notna(last["EMA_20"]):
            daily_above = bool(last["close"] > last["EMA_20"])

    return {
        "htf_15m_ema20_slope_5bar": slope,
        "htf_60m_ema_stack_bullish": ema_stack,
        "daily_above_ema20": daily_above,
    }


def attach_htf_features(rows: list, stock_5min: pd.DataFrame) -> list:
    """Attach HTF features to each signal-row dict in `rows`."""
    out = []
    for row in rows:
        feats = htf_features_at(stock_5min, row["signal_time_ist"])
        new = dict(row)
        new.update(feats)
        # Convenience: count of HTF agreements with LONG direction
        n_bull = sum([
            (feats["htf_15m_ema20_slope_5bar"] or 0) > 0,
            bool(feats["htf_60m_ema_stack_bullish"]),
            bool(feats["daily_above_ema20"]),
        ])
        new["htf_bull_alignment_count"] = n_bull
        out.append(new)
    return out


if __name__ == "__main__":
    import numpy as np

    # 30 trading days of synthetic 5-min bars (uptrend)
    n_days = 30
    bars_per_day = 75
    n = n_days * bars_per_day
    base = pd.Timestamp("2026-04-01 09:15", tz="Asia/Kolkata")
    idx = []
    for d in range(n_days):
        for b in range(bars_per_day):
            idx.append(base + pd.Timedelta(days=d, minutes=5 * b))
    idx = pd.DatetimeIndex(idx)
    closes = np.linspace(500, 600, n)
    df = pd.DataFrame({
        "open": closes - 0.2,
        "high": closes + 0.5,
        "low": closes - 0.5,
        "close": closes,
        "volume": np.full(n, 100_000.0),
    }, index=idx)

    feats = htf_features_at(df, idx[-50])
    print(f"HTF features near end of trend: {feats}")
    assert feats["htf_15m_ema20_slope_5bar"] > 0, "uptrend should give positive slope"
    print("Multi-TF smoke test passed.")
