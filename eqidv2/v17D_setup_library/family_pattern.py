"""v17D pattern-based detectors (3).

PT_1  Gap-and-go (gap >=1.5%, first 5-min hold)
PT_2  Gap-fill fade (gap >=2%, 50% fill within 60min, reversal)
PT_3  Inside-bar breakout
"""
from __future__ import annotations

import pandas as pd

from .common import Signal, make_signal, cfg_get, ensure_volume_sma


def _first_bar_each_day(df: pd.DataFrame) -> pd.DataFrame:
    by_day = df.groupby(df.index.date)
    return by_day.first()


def detect_pt_1_gap_and_go(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Open gap >=1.5% + first 5-min closes in gap direction with vol>=2x."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    gap_min_pct = cfg_get(cfg, "gap_min_pct", 1.5)
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 2.0)
    if "Prev_Day_Close" not in df.columns:
        return out

    by_day = df.groupby(df.index.date)
    for day, idx in by_day.indices.items():
        if len(idx) == 0:
            continue
        bar = df.iloc[idx[0]]
        prev_close = bar.get("Prev_Day_Close")
        if pd.isna(prev_close) or prev_close <= 0:
            continue
        gap_pct = (bar["open"] - prev_close) / prev_close * 100
        vol_sma = bar.get("Volume_SMA20", 0)
        vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
        if vol_ratio < vol_ratio_min:
            continue
        # LONG gap up
        if gap_pct >= gap_min_pct and bar["close"] >= bar["open"]:
            out.append(make_signal(
                ticker, "PT_1_GAP_AND_GO", "LONG", bar.name, bar,
                suggested_sl_pct=0.85, suggested_tgt_pct=1.50,
                extras={"gap_pct": gap_pct},
            ))
        elif gap_pct <= -gap_min_pct and bar["close"] <= bar["open"]:
            out.append(make_signal(
                ticker, "PT_1_GAP_AND_GO", "SHORT", bar.name, bar,
                suggested_sl_pct=0.85, suggested_tgt_pct=1.50,
                extras={"gap_pct": gap_pct},
            ))
    return out


def detect_pt_2_gap_fill_fade(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Gap >=2%, 50% fill within 60min, reversal candle at fill point."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    gap_min_pct = cfg_get(cfg, "gap_min_pct", 2.0)
    fill_window_min = int(cfg_get(cfg, "fill_window_min", 60))
    adx_max = cfg_get(cfg, "adx_max", 25.0)
    if "Prev_Day_Close" not in df.columns:
        return out

    by_day = df.groupby(df.index.date)
    for day, idx in by_day.indices.items():
        if len(idx) == 0:
            continue
        first_bar = df.iloc[idx[0]]
        prev_close = first_bar.get("Prev_Day_Close")
        if pd.isna(prev_close) or prev_close <= 0:
            continue
        gap_pct = (first_bar["open"] - prev_close) / prev_close * 100
        if abs(gap_pct) < gap_min_pct:
            continue
        # 50% fill price
        fill_price = (first_bar["open"] + prev_close) / 2.0
        cutoff = first_bar.name + pd.Timedelta(minutes=fill_window_min)
        for i in idx[1:]:
            bar = df.iloc[i]
            if bar.name > cutoff:
                break
            if pd.notna(bar.get("ADX")) and bar["ADX"] > adx_max:
                continue
            # SHORT: gap up + fill from below + bearish reversal at fill
            if gap_pct >= gap_min_pct:
                if bar["low"] <= fill_price and bar["close"] < bar["open"]:
                    out.append(make_signal(
                        ticker, "PT_2_GAP_FILL_FADE", "SHORT", bar.name, bar,
                        suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
                        extras={"gap_pct": gap_pct},
                    ))
                    break
            # LONG: gap down + fill from above + bullish reversal at fill
            else:
                if bar["high"] >= fill_price and bar["close"] > bar["open"]:
                    out.append(make_signal(
                        ticker, "PT_2_GAP_FILL_FADE", "LONG", bar.name, bar,
                        suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
                        extras={"gap_pct": gap_pct},
                    ))
                    break
    return out


def detect_pt_3_inside_bar_break(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Bar fully inside prior, then directional break with body >=50%."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.3)

    for i in range(2, len(df)):
        prev2 = df.iloc[i - 2]   # parent bar
        inside = df.iloc[i - 1]  # inside bar
        bar = df.iloc[i]         # break bar

        is_inside = (inside["high"] <= prev2["high"]
                     and inside["low"] >= prev2["low"])
        if not is_inside:
            continue
        rng = bar["high"] - bar["low"]
        body = abs(bar["close"] - bar["open"])
        body_pct = (body / rng) if rng > 0 else 0
        if body_pct < 0.5:
            continue
        vol_sma = bar.get("Volume_SMA20", 0)
        vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
        if vol_ratio < vol_ratio_min:
            continue

        if bar["close"] > inside["high"]:
            out.append(make_signal(
                ticker, "PT_3_INSIDE_BAR_BREAK", "LONG", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=0.90,
            ))
        elif bar["close"] < inside["low"]:
            out.append(make_signal(
                ticker, "PT_3_INSIDE_BAR_BREAK", "SHORT", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=0.90,
            ))
    return out
