"""v17D trend continuation detectors (4).

TC_1  Pullback to EMA20 bounce
TC_2  Pullback to EMA50 bounce (deeper, better R:R)
TC_3  Higher-high higher-low momentum break
TC_4  Trend-day first-pullback to VWAP (ADX>=30 days only)
"""
from __future__ import annotations

import pandas as pd

from .common import (
    Signal, Side, make_signal, cfg_get, ensure_volume_sma,
)


def detect_tc_1_pullback_ema20(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Pullback to EMA20 + bullish/bearish reversal candle in trend direction."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    adx_min = cfg_get(cfg, "adx_min", 22.0)
    slope_lookback = int(cfg_get(cfg, "slope_lookback", 5))

    for i in range(slope_lookback + 1, len(df)):
        bar = df.iloc[i]
        prev = df.iloc[i - 1]
        ema20_now = bar["EMA_20"]
        ema50_now = bar["EMA_50"]
        ema20_back = df.iloc[i - slope_lookback]["EMA_20"]
        if pd.isna(ema20_now) or pd.isna(ema50_now) or pd.isna(ema20_back):
            continue
        if pd.isna(bar.get("ADX")) or bar["ADX"] < adx_min:
            continue

        slope_up = ema20_now > ema20_back and bar["close"] > ema50_now
        slope_dn = ema20_now < ema20_back and bar["close"] < ema50_now

        if slope_up:
            tagged = (prev["low"] <= ema20_now) or (bar["low"] <= ema20_now)
            reversal = bar["close"] > bar["open"] and bar["close"] > ema20_now
            if tagged and reversal:
                out.append(make_signal(
                    ticker, "TC_1_PULLBACK_EMA20", "LONG",
                    bar.name, bar,
                    suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
                ))
        elif slope_dn:
            tagged = (prev["high"] >= ema20_now) or (bar["high"] >= ema20_now)
            reversal = bar["close"] < bar["open"] and bar["close"] < ema20_now
            if tagged and reversal:
                out.append(make_signal(
                    ticker, "TC_1_PULLBACK_EMA20", "SHORT",
                    bar.name, bar,
                    suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
                ))
    return out


def detect_tc_2_pullback_ema50(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Deeper pullback to EMA50; requires stronger ADX."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    adx_min = cfg_get(cfg, "adx_min", 25.0)

    for i in range(2, len(df)):
        bar = df.iloc[i]
        ema50 = bar["EMA_50"]
        ema200 = bar.get("EMA_200")
        if pd.isna(ema50):
            continue
        if pd.isna(bar.get("ADX")) or bar["ADX"] < adx_min:
            continue
        long_trend = pd.notna(ema200) and ema50 > ema200
        short_trend = pd.notna(ema200) and ema50 < ema200

        # tagged EMA50 in last 2 bars
        prev = df.iloc[i - 1]
        if long_trend:
            tagged = (prev["low"] <= ema50) or (bar["low"] <= ema50)
            reversal = bar["close"] > bar["open"] and bar["close"] > ema50
            if tagged and reversal:
                out.append(make_signal(
                    ticker, "TC_2_PULLBACK_EMA50", "LONG", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.20,
                ))
        elif short_trend:
            tagged = (prev["high"] >= ema50) or (bar["high"] >= ema50)
            reversal = bar["close"] < bar["open"] and bar["close"] < ema50
            if tagged and reversal:
                out.append(make_signal(
                    ticker, "TC_2_PULLBACK_EMA50", "SHORT", bar.name, bar,
                    suggested_sl_pct=0.80, suggested_tgt_pct=1.20,
                ))
    return out


def _swing_points(highs: pd.Series, lows: pd.Series, lookback: int = 5):
    """Last 3 swing highs / lows.

    A swing high is a bar whose high is STRICTLY greater than every other
    bar's high in the +/- lookback window (i.e. a clean fractal). Same for
    swing lows in the strict-less-than sense. The strict inequality avoids
    flat-noise plateaus polluting the swing list.
    """
    sh, sl = [], []
    for i in range(lookback, len(highs) - lookback):
        h_now = highs.iloc[i]
        l_now = lows.iloc[i]
        h_neighbors = pd.concat([
            highs.iloc[i - lookback:i],
            highs.iloc[i + 1:i + lookback + 1],
        ])
        l_neighbors = pd.concat([
            lows.iloc[i - lookback:i],
            lows.iloc[i + 1:i + lookback + 1],
        ])
        if pd.notna(h_now) and h_now > h_neighbors.max():
            sh.append((i, float(h_now)))
        if pd.notna(l_now) and l_now < l_neighbors.min():
            sl.append((i, float(l_now)))
    return sh[-3:], sl[-3:]


def detect_tc_3_hh_hl_momentum(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Confirmed HH+HL pattern then break of latest HH (mirror SHORT)."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 1.5)
    swing_lookback = int(cfg_get(cfg, "swing_lookback", 5))

    if len(df) < swing_lookback * 4:
        return out

    sh, sl = _swing_points(df["high"], df["low"], swing_lookback)
    if len(sh) < 2 or len(sl) < 2:
        return out

    # Need: HH (sh[-1] > sh[-2]) and HL (sl[-1] > sl[-2])
    hh = sh[-1][1] > sh[-2][1]
    hl = sl[-1][1] > sl[-2][1]
    ll = sl[-1][1] < sl[-2][1]
    lh = sh[-1][1] < sh[-2][1]

    last_hh_idx = sh[-1][0]
    last_ll_idx = sl[-1][0]

    for i in range(max(last_hh_idx, last_ll_idx) + 1, len(df)):
        bar = df.iloc[i]
        vol_ratio = (bar["volume"] / bar["Volume_SMA20"]) if bar.get("Volume_SMA20", 0) > 0 else 0
        if vol_ratio < vol_ratio_min:
            continue
        et_hr = bar.name.hour + bar.name.minute / 60.0
        if not (9.5 <= et_hr <= 13.0):
            continue
        if hh and hl and bar["close"] > sh[-1][1]:
            out.append(make_signal(
                ticker, "TC_3_HH_HL_MOMENTUM", "LONG", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
            return out
        if ll and lh and bar["close"] < sl[-1][1]:
            out.append(make_signal(
                ticker, "TC_3_HH_HL_MOMENTUM", "SHORT", bar.name, bar,
                suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
            ))
            return out
    return out


def detect_tc_4_trendday_vwap_pullback(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """ADX>=30 trend day; first pullback to VWAP +- 0.3 ATR; reversal candle."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    adx_min = cfg_get(cfg, "adx_min", 30.0)
    move_pct_min = cfg_get(cfg, "move_pct_min", 1.5)
    vwap_band_atr = cfg_get(cfg, "vwap_band_atr", 0.3)

    if "VWAP" not in df.columns or "ATR" not in df.columns:
        return out

    by_day = df.groupby(df.index.date)
    for day, idx in by_day.indices.items():
        day_df = df.iloc[idx]
        if len(day_df) < 5:
            continue
        day_open = float(day_df.iloc[0]["open"])
        adx_today = day_df["ADX"].iloc[-1] if pd.notna(day_df["ADX"].iloc[-1]) else 0
        if adx_today < adx_min:
            continue
        triggered = False
        for i in range(2, len(day_df)):
            if triggered:
                break
            bar = day_df.iloc[i]
            move_pct = (bar["close"] - day_open) / day_open * 100
            atr = bar.get("ATR", float("nan"))
            vwap = bar.get("VWAP", float("nan"))
            if pd.isna(atr) or pd.isna(vwap) or atr <= 0:
                continue
            band = vwap_band_atr * atr
            if move_pct >= move_pct_min:
                # LONG-day: look for pullback into VWAP band
                if abs(bar["low"] - vwap) <= band and bar["close"] > bar["open"] and bar["close"] > vwap:
                    out.append(make_signal(
                        ticker, "TC_4_TRENDDAY_VWAP_PULLBACK", "LONG",
                        bar.name, bar,
                        suggested_sl_pct=0.85, suggested_tgt_pct=1.50,
                    ))
                    triggered = True
            elif move_pct <= -move_pct_min:
                if abs(bar["high"] - vwap) <= band and bar["close"] < bar["open"] and bar["close"] < vwap:
                    out.append(make_signal(
                        ticker, "TC_4_TRENDDAY_VWAP_PULLBACK", "SHORT",
                        bar.name, bar,
                        suggested_sl_pct=0.85, suggested_tgt_pct=1.50,
                    ))
                    triggered = True
    return out
