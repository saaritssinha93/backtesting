"""Shared V7/V11 position-sizing primitives."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class RiskSizingConfig:
    enabled: bool
    fallback_notional_rs: float
    equity_rs: float
    risk_pct_per_trade: float
    min_notional_rs: float
    max_notional_rs: float


def risk_based_quantity(
    entry_price: float,
    stop_price: float,
    config: RiskSizingConfig,
) -> int:
    """Return the V7 share quantity for an entry/initial-stop pair."""
    entry = float(entry_price)
    stop = float(stop_price)
    if not config.enabled or entry <= 0 or stop <= 0:
        return max(1, int(config.fallback_notional_rs / entry)) if entry > 0 else 1

    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return max(1, int(config.fallback_notional_rs / entry))

    risk_rs = config.equity_rs * config.risk_pct_per_trade / 100.0
    raw_quantity = risk_rs / stop_distance
    min_quantity = config.min_notional_rs / entry
    max_quantity = config.max_notional_rs / entry
    return max(1, int(max(min_quantity, min(max_quantity, raw_quantity))))


def nifty_regime_short_multiplier(
    parquet_path: str | Path,
    *,
    trade_day: str,
    enabled: bool,
    ma_days: int,
    bullish_multiplier: float,
) -> float:
    """Return V7's short-size multiplier using data through trade_day."""
    if not enabled:
        return 1.0

    try:
        frame = pd.read_parquet(parquet_path, columns=["date", "close"])
        timestamps = pd.to_datetime(frame["date"], errors="coerce")
        day = pd.Timestamp(trade_day)
        if timestamps.dt.tz is not None:
            day = day.tz_localize(timestamps.dt.tz)
        frame = frame.loc[timestamps.dt.normalize() <= day.normalize()].copy()
        if frame.empty:
            return 1.0

        frame["date"] = timestamps.loc[frame.index]
        daily = (
            frame.assign(trade_date=frame["date"].dt.normalize())
            .groupby("trade_date", sort=True)["close"]
            .last()
            .reset_index()
        )
        daily.columns = ["trade_date", "close"]
        daily = daily.sort_values("trade_date").tail(int(ma_days) + 3).reset_index(drop=True)
        if len(daily) < int(ma_days) + 1:
            return 1.0

        daily["ma"] = daily["close"].rolling(int(ma_days)).mean()
        last, previous = daily.iloc[-1], daily.iloc[-2]
        bullish = (
            pd.notna(last["ma"])
            and pd.notna(previous["ma"])
            and last["close"] > last["ma"]
            and last["ma"] > previous["ma"]
        )
        return float(bullish_multiplier) if bullish else 1.0
    except Exception:
        return 1.0
