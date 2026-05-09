"""v17D time-of-day detectors (2).

TD_1  Opening drive (first 5-min > 1.5x ATR + continuation on second bar)
TD_2  Late-day reversal (after 14:00 IST, reversal of intraday trend)
"""
from __future__ import annotations

import pandas as pd

from .common import Signal, make_signal, cfg_get, ensure_volume_sma


def detect_td_1_opening_drive(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """First 5-min bar range >= 1.5x daily ATR + continuation on second bar."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    range_atr_min = cfg_get(cfg, "range_atr_min", 1.5)

    by_day = df.groupby(df.index.date)
    for day, idx in by_day.indices.items():
        if len(idx) < 2:
            continue
        b1 = df.iloc[idx[0]]
        b2 = df.iloc[idx[1]]
        atr = b1.get("ATR")
        if pd.isna(atr) or atr <= 0:
            continue
        b1_range = b1["high"] - b1["low"]
        if b1_range / atr < range_atr_min:
            continue
        b1_close_dir = b1["close"] - b1["open"]
        b2_range = b2["high"] - b2["low"]
        b2_body = abs(b2["close"] - b2["open"])
        b2_body_pct = (b2_body / b2_range) if b2_range > 0 else 0
        if b2_body_pct < 0.5:
            continue

        # LONG: bar1 closed up + bar2 also closes up
        if b1_close_dir > 0 and b2["close"] > b2["open"] and b2["close"] > b1["close"]:
            out.append(make_signal(
                ticker, "TD_1_OPENING_DRIVE", "LONG", b2.name, b2,
                suggested_sl_pct=1.00, suggested_tgt_pct=1.50,
            ))
        elif b1_close_dir < 0 and b2["close"] < b2["open"] and b2["close"] < b1["close"]:
            out.append(make_signal(
                ticker, "TD_1_OPENING_DRIVE", "SHORT", b2.name, b2,
                suggested_sl_pct=1.00, suggested_tgt_pct=1.50,
            ))
    return out


def detect_td_2_late_day_reversal(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """After 14:00 IST: intraday trend established >=1%; reversal bar with vol."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    move_pct_min = cfg_get(cfg, "move_pct_min", 1.0)
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.3)
    after_hr = cfg_get(cfg, "after_hr", 14.0)

    by_day = df.groupby(df.index.date)
    for day, idx in by_day.indices.items():
        if len(idx) < 5:
            continue
        day_df = df.iloc[idx]
        day_open = float(day_df.iloc[0]["open"])
        triggered = False
        for j, i in enumerate(idx):
            if triggered:
                break
            bar = df.iloc[i]
            et_hr = bar.name.hour + bar.name.minute / 60.0
            if et_hr < after_hr:
                continue
            move_pct = (bar["close"] - day_open) / day_open * 100
            vol_sma = bar.get("Volume_SMA20", 0)
            vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
            if vol_ratio < vol_ratio_min:
                continue
            rng = bar["high"] - bar["low"]
            body = abs(bar["close"] - bar["open"])
            body_pct = (body / rng) if rng > 0 else 0
            if body_pct < 0.5:
                continue

            # Reverse a downtrend: was -1%+ down, bar closes up
            if move_pct <= -move_pct_min and bar["close"] > bar["open"]:
                out.append(make_signal(
                    ticker, "TD_2_LATE_DAY_REVERSAL", "LONG", bar.name, bar,
                    suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
                ))
                triggered = True
            elif move_pct >= move_pct_min and bar["close"] < bar["open"]:
                out.append(make_signal(
                    ticker, "TD_2_LATE_DAY_REVERSAL", "SHORT", bar.name, bar,
                    suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
                ))
                triggered = True
    return out
