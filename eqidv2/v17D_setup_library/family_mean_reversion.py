"""v17D mean reversion detectors (4).

MR_1  Bollinger band touch + RSI extreme
MR_2  VWAP fade (price >=2 ATR from VWAP)
MR_3  Overextended EMA reversion (price >=3 ATR from EMA20)
MR_4  Three-bar drive + reversal candle
"""
from __future__ import annotations

import pandas as pd

from .common import Signal, make_signal, cfg_get, ensure_volume_sma


def detect_mr_1_bb_rsi_extreme(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Lower-band touch + RSI<=20 (LONG) or upper-band + RSI>=80 (SHORT).

    Daily ADX <= 25 gate (mean-rev prefers chop)."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    rsi_long_max = cfg_get(cfg, "rsi_long_max", 20.0)
    rsi_short_min = cfg_get(cfg, "rsi_short_min", 80.0)
    adx_max = cfg_get(cfg, "adx_max", 25.0)

    for i in range(1, len(df)):
        bar = df.iloc[i]
        if any(pd.isna(bar.get(c)) for c in ("Lower_Band", "Upper_Band", "RSI", "ADX")):
            continue
        if bar["ADX"] > adx_max:
            continue

        # LONG: low touched lower band, RSI extreme low, bullish reversal
        if (bar["low"] <= bar["Lower_Band"]
                and bar["RSI"] <= rsi_long_max
                and bar["close"] > bar["open"]):
            out.append(make_signal(
                ticker, "MR_1_BB_RSI_EXTREME", "LONG", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=0.85,
            ))
        # SHORT: high touched upper band, RSI extreme high, bearish reversal
        elif (bar["high"] >= bar["Upper_Band"]
                and bar["RSI"] >= rsi_short_min
                and bar["close"] < bar["open"]):
            out.append(make_signal(
                ticker, "MR_1_BB_RSI_EXTREME", "SHORT", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=0.85,
            ))
    return out


def detect_mr_2_vwap_fade(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Price >= 2 ATR from VWAP, reversal candle toward VWAP."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    atr_threshold = cfg_get(cfg, "atr_threshold", 2.0)
    adx_max = cfg_get(cfg, "adx_max", 28.0)

    for i in range(1, len(df)):
        bar = df.iloc[i]
        atr = bar.get("ATR")
        vwap = bar.get("VWAP")
        if pd.isna(atr) or pd.isna(vwap) or atr <= 0:
            continue
        if pd.notna(bar.get("ADX")) and bar["ADX"] > adx_max:
            continue

        gap = (bar["close"] - vwap) / atr

        # LONG: price <= -2 ATR below VWAP, bullish bar
        if gap <= -atr_threshold and bar["close"] > bar["open"]:
            body = bar["close"] - bar["open"]
            rng = bar["high"] - bar["low"]
            if rng > 0 and body / rng >= 0.4:
                out.append(make_signal(
                    ticker, "MR_2_VWAP_FADE", "LONG", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=0.90,
                ))
        # SHORT: price >= +2 ATR above VWAP, bearish bar
        elif gap >= atr_threshold and bar["close"] < bar["open"]:
            body = bar["open"] - bar["close"]
            rng = bar["high"] - bar["low"]
            if rng > 0 and body / rng >= 0.4:
                out.append(make_signal(
                    ticker, "MR_2_VWAP_FADE", "SHORT", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=0.90,
                ))
    return out


def detect_mr_3_overext_ema_reversion(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Price >= 3 ATR from EMA20, first counter-direction bar with body>=50%."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    atr_threshold = cfg_get(cfg, "atr_threshold", 3.0)

    for i in range(1, len(df)):
        bar = df.iloc[i]
        prev = df.iloc[i - 1]
        atr = bar.get("ATR")
        ema20 = bar.get("EMA_20")
        if pd.isna(atr) or pd.isna(ema20) or atr <= 0:
            continue
        prev_gap = (prev["close"] - ema20) / atr
        cur_gap = (bar["close"] - ema20) / atr
        rng = bar["high"] - bar["low"]
        body = abs(bar["close"] - bar["open"])
        body_pct = (body / rng) if rng > 0 else 0

        # LONG: was <= -3 ATR, now bullish counter-bar
        if prev_gap <= -atr_threshold and bar["close"] > bar["open"] and body_pct >= 0.5:
            out.append(make_signal(
                ticker, "MR_3_OVEREXT_EMA_REVERSION", "LONG", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
        # SHORT: was >= +3 ATR, now bearish counter-bar
        elif prev_gap >= atr_threshold and bar["close"] < bar["open"] and body_pct >= 0.5:
            out.append(make_signal(
                ticker, "MR_3_OVEREXT_EMA_REVERSION", "SHORT", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
    return out


def detect_mr_4_three_bar_drive(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """3 consecutive same-direction bars + climactic volume + reversal close."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.2)

    for i in range(3, len(df)):
        b1 = df.iloc[i - 3]
        b2 = df.iloc[i - 2]
        b3 = df.iloc[i - 1]
        b4 = df.iloc[i]
        vol_sma = b3.get("Volume_SMA20", 0)
        if vol_sma <= 0:
            continue
        vol_ratio = b3["volume"] / vol_sma
        if vol_ratio < vol_ratio_min:
            continue

        all_red = (b1["close"] < b1["open"] and b2["close"] < b2["open"]
                   and b3["close"] < b3["open"])
        all_green = (b1["close"] > b1["open"] and b2["close"] > b2["open"]
                     and b3["close"] > b3["open"])
        rng = b4["high"] - b4["low"]
        body4 = abs(b4["close"] - b4["open"])
        body_pct = (body4 / rng) if rng > 0 else 0

        # LONG: 3 red drive + bullish reversal
        if all_red and b4["close"] > b4["open"] and body_pct >= 0.4:
            out.append(make_signal(
                ticker, "MR_4_THREE_BAR_DRIVE", "LONG", b4.name, b4,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
            ))
        # SHORT: 3 green drive + bearish reversal
        elif all_green and b4["close"] < b4["open"] and body_pct >= 0.4:
            out.append(make_signal(
                ticker, "MR_4_THREE_BAR_DRIVE", "SHORT", b4.name, b4,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
            ))
    return out
