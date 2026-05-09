"""v17D setup library — common types, helpers, and registry.

Every setup detector follows the same signature:

    detect(
        df: pd.DataFrame,           # 5-min OHLCV + indicator parquet for one ticker
        ticker: str,
        cfg: dict | None = None,    # optional threshold overrides
    ) -> list[Signal]

Required df columns (5-min bars, tz-aware IST DatetimeIndex):
    open, high, low, close, volume
    EMA_20, EMA_50          (and EMA_200 for some setups)
    ATR                     (price units)
    RSI, ADX, Stoch_K
    Upper_Band, Lower_Band  (Bollinger 20,2)
    MFI, OBV, CCI, MACD_Hist
    VWAP
    Volume_SMA20            (or computed on the fly)
    Prev_Day_Close

Detector responsibilities:
    - emit Signal at the *signal bar* (the bar where the trigger completed)
    - entry_price = close of signal bar (matches v17C F12 entry-bar exits)
    - never look at bars after the signal bar (causality)
    - populate diag features so downstream filters can gate

Bars-from-open and entry-hour are populated automatically by `make_signal`.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Callable, Literal

import numpy as np
import pandas as pd

Side = Literal["LONG", "SHORT"]


@dataclass
class Signal:
    ticker: str
    setup: str
    side: Side
    signal_time_ist: pd.Timestamp
    entry_price: float

    # Standard diagnostic columns (every setup populates these)
    bars_from_open: int = 0
    entry_hour: float = 0.0
    rsi_signal: float = float("nan")
    atr_pct_signal: float = float("nan")
    adx_signal: float = float("nan")
    stochk_signal: float = float("nan")
    ema20_gap_atr_signal: float = float("nan")
    avwap_dist_atr_signal: float = float("nan")
    entry_bar_vol_ratio: float = float("nan")
    quality_score: float = float("nan")

    # Suggested risk parameters (each setup can override)
    suggested_sl_pct: float = 0.75
    suggested_tgt_pct: float = 0.80

    # Setup-specific extras
    extras: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("extras", None)
        d.update(self.extras)
        d["signal_time_ist"] = pd.Timestamp(self.signal_time_ist).isoformat()
        return d


def _bars_from_open(ts: pd.Timestamp) -> int:
    """5-min bars elapsed since 09:15 IST on that calendar date."""
    market_open = ts.normalize() + pd.Timedelta(hours=9, minutes=15)
    minutes = (ts - market_open).total_seconds() / 60.0
    return max(0, int(minutes // 5))


def _entry_hour(ts: pd.Timestamp) -> float:
    return ts.hour + ts.minute / 60.0


def make_signal(
    ticker: str, setup: str, side: Side,
    bar_ts: pd.Timestamp, bar: pd.Series,
    *,
    suggested_sl_pct: float = 0.75,
    suggested_tgt_pct: float = 0.80,
    extras: dict | None = None,
) -> Signal:
    """Build a Signal with standard diag columns populated from `bar`."""
    close = float(bar["close"])
    atr = float(bar.get("ATR", float("nan")))
    ema20 = float(bar.get("EMA_20", float("nan")))
    vwap = float(bar.get("VWAP", float("nan")))
    vol = float(bar.get("volume", 0.0))
    vol_sma = float(bar.get("Volume_SMA20", 0.0))

    atr_pct = (atr / close) if (atr == atr and close > 0) else float("nan")
    ema20_gap = ((close - ema20) / atr) if (atr == atr and atr > 0) else float("nan")
    vwap_gap = ((close - vwap) / atr) if (atr == atr and atr > 0) else float("nan")
    vol_ratio = (vol / vol_sma) if vol_sma > 0 else float("nan")

    return Signal(
        ticker=ticker, setup=setup, side=side,
        signal_time_ist=bar_ts, entry_price=close,
        bars_from_open=_bars_from_open(bar_ts),
        entry_hour=_entry_hour(bar_ts),
        rsi_signal=float(bar.get("RSI", float("nan"))),
        atr_pct_signal=atr_pct,
        adx_signal=float(bar.get("ADX", float("nan"))),
        stochk_signal=float(bar.get("Stoch_K", float("nan"))),
        ema20_gap_atr_signal=ema20_gap,
        avwap_dist_atr_signal=vwap_gap,
        entry_bar_vol_ratio=vol_ratio,
        suggested_sl_pct=suggested_sl_pct,
        suggested_tgt_pct=suggested_tgt_pct,
        extras=dict(extras or {}),
    )


def ensure_volume_sma(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Compute Volume_SMA20 if the parquet didn't pre-compute it."""
    if "Volume_SMA20" in df.columns:
        return df
    out = df.copy()
    out["Volume_SMA20"] = out["volume"].rolling(window, min_periods=window).mean()
    return out


def session_open_high(df: pd.DataFrame, end_minute: int = 30) -> pd.DataFrame:
    """Return the OR (opening range) high/low up to `end_minute` past 09:15 IST.

    Adds two columns OR_HIGH, OR_LOW (per-day forward-filled).
    """
    out = df.copy()
    by_day = out.groupby(out.index.date)
    or_high_col = pd.Series(index=out.index, dtype=float)
    or_low_col = pd.Series(index=out.index, dtype=float)
    for day, idx in by_day.indices.items():
        day_df = out.iloc[idx]
        day_open = day_df.index[0].normalize() + pd.Timedelta(hours=9, minutes=15)
        cutoff = day_open + pd.Timedelta(minutes=end_minute)
        or_window = day_df[day_df.index < cutoff]
        if len(or_window) == 0:
            continue
        or_h = float(or_window["high"].max())
        or_l = float(or_window["low"].min())
        or_high_col.iloc[idx] = or_h
        or_low_col.iloc[idx] = or_l
    out["OR_HIGH"] = or_high_col
    out["OR_LOW"] = or_low_col
    return out


def cfg_get(cfg: dict | None, key: str, default):
    if cfg is None:
        return default
    return cfg.get(key, default)
