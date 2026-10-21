"""Causal one-minute EMA20 and ATR14 for G3 confirmation candles."""
from __future__ import annotations

import math
from typing import Any

import pandas as pd


def last_features(history: pd.DataFrame, bar: dict[str, Any]) -> tuple[float, float]:
    """Use completed prior bars and the completed confirmation bar only.

    EMA20 uses adjust=False; ATR14 is the simple mean of the last fourteen
    true ranges. These match the stored equity one-minute indicators.
    """
    if not {"open", "high", "low", "close"}.issubset(history.columns):
        return math.nan, math.nan
    frame = pd.concat([history[["open", "high", "low", "close"]],
                       pd.DataFrame([{name: bar.get(name) for name in ("open", "high", "low", "close")}])],
                      ignore_index=True)
    for name in ("open", "high", "low", "close"):
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    if len(frame) < 20 or not frame[["open", "high", "low", "close"]].notna().all().all():
        return math.nan, math.nan
    close = frame["close"]
    previous = close.shift()
    true_range = pd.concat([frame.high - frame.low,
                            (frame.high - previous).abs(),
                            (frame.low - previous).abs()], axis=1).max(axis=1)
    ema = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
    atr = float(true_range.rolling(14, min_periods=14).mean().iloc[-1])
    return (ema, atr) if math.isfinite(ema) and math.isfinite(atr) and atr > 0 else (math.nan, math.nan)


def extension_atr(close: Any, ema20: Any, atr14: Any, side: str) -> float:
    try:
        values = [float(close), float(ema20), float(atr14)]
    except (TypeError, ValueError):
        return math.nan
    if not all(math.isfinite(value) for value in values) or values[2] <= 0:
        return math.nan
    direction = 1 if side == "LONG" else -1 if side == "SHORT" else 0
    return direction * (values[0] - values[1]) / values[2] if direction else math.nan
