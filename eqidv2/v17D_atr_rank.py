"""v17D Step 2.4 — ATR percentile rank precompute.

Per-stock relative volatility regime feature. Replaces fixed thresholds
like `atr_pct_signal >= 0.0040` with `atr_pct_rank_60d >= 0.30`, where
the rank is the percentile position (0.0–1.0) of today's ATR%
within the last 60 trading days.

Why: a fixed 0.40% ATR threshold systematically excludes low-priced /
low-vol stocks even when they are *relatively* volatile for themselves.
Rank-based gating is regime-agnostic.

Public API:
    compute_atr_rank(df_5min, lookback_days=60) -> df with new column
        atr_pct_rank_60d (0.0–1.0)
    atr_rank_at(df_5min, ts, lookback_days=60) -> float in [0.0, 1.0]
    attach_atr_rank(rows, stock_df_5min) -> rows with atr_pct_rank_60d
"""
from __future__ import annotations

import pandas as pd


def _daily_atr_pct_series(df_5min: pd.DataFrame) -> pd.Series:
    """One ATR% value per trading day = ATR(14) at last bar of day / close."""
    if df_5min.empty:
        return pd.Series(dtype=float)
    grp = df_5min.groupby(df_5min.index.date)
    out = {}
    for day, idx in grp.indices.items():
        bars = df_5min.iloc[idx]
        if "ATR" not in bars.columns:
            continue
        last_atr = bars["ATR"].iloc[-1]
        last_close = bars["close"].iloc[-1]
        if pd.isna(last_atr) or pd.isna(last_close) or last_close <= 0:
            continue
        out[pd.Timestamp(day)] = float(last_atr) / float(last_close)
    return pd.Series(out).sort_index()


def compute_atr_rank(
    df_5min: pd.DataFrame, lookback_days: int = 60,
) -> pd.DataFrame:
    """Add atr_pct_rank_60d column to a 5-min df.

    Rank for each bar = percentile position of today's daily ATR% within
    the last `lookback_days` trading days (causal: today included).
    """
    if df_5min.empty:
        return df_5min
    out = df_5min.copy()
    daily_atr_pct = _daily_atr_pct_series(out)
    if daily_atr_pct.empty:
        out["atr_pct_rank_60d"] = float("nan")
        return out

    daily_rank = pd.Series(
        index=daily_atr_pct.index, dtype=float,
    )
    vals = daily_atr_pct.to_numpy()
    for i in range(len(daily_atr_pct)):
        start = max(0, i - lookback_days + 1)
        window = vals[start:i + 1]
        if len(window) < 2:
            continue
        rank = float((window <= vals[i]).sum() - 1) / max(len(window) - 1, 1)
        daily_rank.iloc[i] = rank

    # Map daily rank back to each 5-min bar
    bar_dates = out.index.normalize().date
    bar_dates = pd.Series(bar_dates, index=out.index)
    rank_map = {pd.Timestamp(d).date(): r for d, r in daily_rank.items()}
    out["atr_pct_rank_60d"] = bar_dates.map(rank_map)
    return out


def atr_rank_at(
    df_5min: pd.DataFrame, ts: pd.Timestamp,
    lookback_days: int = 60,
) -> float:
    """Causal rank lookup for a single timestamp."""
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata")
    sub = df_5min[df_5min.index <= ts]
    if sub.empty:
        return float("nan")
    enriched = compute_atr_rank(sub, lookback_days)
    if "atr_pct_rank_60d" not in enriched.columns or enriched.empty:
        return float("nan")
    return float(enriched["atr_pct_rank_60d"].iloc[-1])


def attach_atr_rank(rows: list, stock_df_5min: pd.DataFrame,
                    lookback_days: int = 60) -> list:
    """Attach atr_pct_rank_60d to each row in `rows`."""
    out = []
    for row in rows:
        rank = atr_rank_at(stock_df_5min, row["signal_time_ist"], lookback_days)
        new = dict(row)
        new["atr_pct_rank_60d"] = rank
        out.append(new)
    return out


if __name__ == "__main__":
    import numpy as np

    n_days = 80
    bars_per_day = 75
    n = n_days * bars_per_day
    base = pd.Timestamp("2026-02-01 09:15", tz="Asia/Kolkata")
    idx = []
    for d in range(n_days):
        for b in range(bars_per_day):
            idx.append(base + pd.Timedelta(days=d, minutes=5 * b))
    idx = pd.DatetimeIndex(idx)

    closes = np.full(n, 500.0)
    # Most days low ATR (5), recent days high ATR (15)
    atr = np.where(np.arange(n) > n - bars_per_day * 5, 15.0, 5.0)
    df = pd.DataFrame({
        "open": closes, "high": closes + 1, "low": closes - 1,
        "close": closes, "volume": np.full(n, 100_000.0),
        "ATR": atr,
    }, index=idx)

    enriched = compute_atr_rank(df)
    last_rank = float(enriched["atr_pct_rank_60d"].iloc[-1])
    print(f"Last bar ATR rank (high vol regime): {last_rank:.3f}")
    assert last_rank > 0.8, "high vol days should rank near top"

    early_rank = float(enriched["atr_pct_rank_60d"].iloc[bars_per_day * 30])
    print(f"Mid-history ATR rank (low vol regime): {early_rank:.3f}")
    print("ATR rank smoke test passed.")
