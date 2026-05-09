"""v17D breakout detectors (4).

BO_1  Donchian 20-bar high/low breakout
BO_2  Squeeze release (BB inside Keltner)
BO_3  Higher-TF level break (yesterday high/low, 5-day high/low)
BO_4  Opening-range 15-min breakout (restored C_OR_BREAKOUT)
"""
from __future__ import annotations

import pandas as pd

from .common import (
    Signal, make_signal, cfg_get, ensure_volume_sma, session_open_high,
)


def detect_bo_1_donchian_break(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """20-bar Donchian channel breakout with volume confirmation."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    window = int(cfg_get(cfg, "donchian_window", 20))
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.5)

    if len(df) <= window:
        return out
    rolling_high = df["high"].rolling(window, min_periods=window).max().shift(1)
    rolling_low = df["low"].rolling(window, min_periods=window).min().shift(1)

    for i in range(window, len(df)):
        bar = df.iloc[i]
        rh = rolling_high.iloc[i]
        rl = rolling_low.iloc[i]
        if pd.isna(rh) or pd.isna(rl):
            continue
        et_hr = bar.name.hour + bar.name.minute / 60.0
        if not (9.5 <= et_hr <= 13.0):
            continue
        vol_sma = bar.get("Volume_SMA20", 0)
        vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
        if vol_ratio < vol_ratio_min:
            continue

        if bar["high"] > rh and bar["close"] > rh:
            out.append(make_signal(
                ticker, "BO_1_DONCHIAN_BREAK", "LONG", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
        elif bar["low"] < rl and bar["close"] < rl:
            out.append(make_signal(
                ticker, "BO_1_DONCHIAN_BREAK", "SHORT", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
    return out


def _keltner(df: pd.DataFrame, period: int = 20, mult: float = 1.5):
    """Compute Keltner channels (EMA20 +/- mult*ATR)."""
    if "EMA_20" in df.columns and "ATR" in df.columns:
        ema = df["EMA_20"]
        atr = df["ATR"]
    else:
        ema = df["close"].ewm(span=period, adjust=False).mean()
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(period, min_periods=period).mean()
    return ema + mult * atr, ema - mult * atr


def detect_bo_2_squeeze_release(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """BB inside Keltner for >=N bars, then close outside Keltner."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    if "Upper_Band" not in df.columns or "Lower_Band" not in df.columns:
        return out
    squeeze_min_bars = int(cfg_get(cfg, "squeeze_min_bars", 10))
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.3)

    kc_up, kc_dn = _keltner(df)
    inside_kc = (df["Upper_Band"] < kc_up) & (df["Lower_Band"] > kc_dn)
    streak = inside_kc.astype(int).groupby((~inside_kc).cumsum()).cumsum()

    for i in range(squeeze_min_bars, len(df)):
        prev_streak = streak.iloc[i - 1]
        if prev_streak < squeeze_min_bars:
            continue
        bar = df.iloc[i]
        kc_u = kc_up.iloc[i]
        kc_d = kc_dn.iloc[i]
        vol_sma = bar.get("Volume_SMA20", 0)
        vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
        if vol_ratio < vol_ratio_min:
            continue
        if bar["close"] > kc_u:
            out.append(make_signal(
                ticker, "BO_2_SQUEEZE_RELEASE", "LONG", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.20,
            ))
        elif bar["close"] < kc_d:
            out.append(make_signal(
                ticker, "BO_2_SQUEEZE_RELEASE", "SHORT", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.20,
            ))
    return out


def detect_bo_3_htf_level_break(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Break of yesterday's high/low or 5-day high/low with volume."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.5)
    atr_buffer = cfg_get(cfg, "atr_buffer", 0.0)  # optional false-break buffer

    by_day = df.groupby(df.index.date)
    daily_highs = by_day["high"].max()
    daily_lows = by_day["low"].min()

    days_seen = list(daily_highs.index)
    for day_idx, (day, idx_arr) in enumerate(by_day.indices.items()):
        if day_idx == 0:
            continue
        # yesterday
        prev_day = days_seen[day_idx - 1]
        y_high = float(daily_highs.loc[prev_day])
        y_low = float(daily_lows.loc[prev_day])
        # 5-day high/low (excluding today)
        lookback = days_seen[max(0, day_idx - 5):day_idx]
        if len(lookback) < 1:
            continue
        five_high = float(daily_highs.loc[lookback].max())
        five_low = float(daily_lows.loc[lookback].min())

        first_break_long = False
        first_break_short = False
        for i in idx_arr:
            bar = df.iloc[i]
            atr = bar.get("ATR", 0) or 0
            buf = atr_buffer * atr
            vol_sma = bar.get("Volume_SMA20", 0)
            vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
            if vol_ratio < vol_ratio_min:
                continue
            if not first_break_long and bar["close"] > max(y_high, five_high) + buf:
                first_break_long = True
                out.append(make_signal(
                    ticker, "BO_3_HTF_LEVEL_BREAK", "LONG", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                ))
            if not first_break_short and bar["close"] < min(y_low, five_low) - buf:
                first_break_short = True
                out.append(make_signal(
                    ticker, "BO_3_HTF_LEVEL_BREAK", "SHORT", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                ))
    return out


def detect_bo_4_or15_break(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """15-min OR breakout post-09:30 with volume + ADX gate."""
    df = ensure_volume_sma(df)
    df = session_open_high(df, end_minute=15)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.5)
    adx_min = cfg_get(cfg, "adx_min", 22.0)

    by_day = df.groupby(df.index.date)
    for day, idx_arr in by_day.indices.items():
        first_long = False
        first_short = False
        for i in idx_arr:
            bar = df.iloc[i]
            et_hr = bar.name.hour + bar.name.minute / 60.0
            if et_hr < 9.50:  # only post-09:30
                continue
            or_h = bar.get("OR_HIGH")
            or_l = bar.get("OR_LOW")
            if pd.isna(or_h) or pd.isna(or_l):
                continue
            if pd.notna(bar.get("ADX")) and bar["ADX"] < adx_min:
                continue
            vol_sma = bar.get("Volume_SMA20", 0)
            vol_ratio = (bar["volume"] / vol_sma) if vol_sma > 0 else 0
            if vol_ratio < vol_ratio_min:
                continue
            rng = bar["high"] - bar["low"]
            body = abs(bar["close"] - bar["open"])
            body_pct = (body / rng) if rng > 0 else 0
            if body_pct < 0.5:
                continue
            if not first_long and bar["close"] > or_h:
                first_long = True
                out.append(make_signal(
                    ticker, "BO_4_OR15_BREAK", "LONG", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                ))
            if not first_short and bar["close"] < or_l:
                first_short = True
                out.append(make_signal(
                    ticker, "BO_4_OR15_BREAK", "SHORT", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                ))
    return out
