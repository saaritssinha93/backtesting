"""v17D volume / order-flow detectors (3).

VO_1  Climax volume reversal (vol >=3x with opposite-direction close)
VO_2  Low-volume drift then acceleration
VO_3  OBV divergence (price HH + OBV LL, or mirror)
"""
from __future__ import annotations

import pandas as pd

from .common import Signal, make_signal, cfg_get, ensure_volume_sma


def detect_vo_1_climax_reversal(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Volume >= 3x SMA20 + bar opposite recent 5-bar slope + long wick."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    vol_ratio_min = cfg_get(cfg, "vol_ratio_min", 3.0)
    slope_lookback = int(cfg_get(cfg, "slope_lookback", 5))
    adx_min = cfg_get(cfg, "adx_min", 20.0)
    adx_max = cfg_get(cfg, "adx_max", 28.0)

    for i in range(slope_lookback, len(df)):
        bar = df.iloc[i]
        vol_sma = bar.get("Volume_SMA20", 0)
        if vol_sma <= 0:
            continue
        vol_ratio = bar["volume"] / vol_sma
        if vol_ratio < vol_ratio_min:
            continue
        adx = bar.get("ADX")
        if pd.notna(adx) and not (adx_min <= adx <= adx_max):
            continue

        prior_close = df.iloc[i - slope_lookback]["close"]
        slope_up = bar["close"] > prior_close
        slope_dn = bar["close"] < prior_close
        rng = bar["high"] - bar["low"]
        upper_wick = bar["high"] - max(bar["open"], bar["close"])
        lower_wick = min(bar["open"], bar["close"]) - bar["low"]
        upper_wick_pct = (upper_wick / rng) if rng > 0 else 0
        lower_wick_pct = (lower_wick / rng) if rng > 0 else 0

        # SHORT after buy climax: was rising, big-vol bar with long upper wick
        if slope_up and upper_wick_pct >= 0.5 and bar["close"] < bar["open"]:
            out.append(make_signal(
                ticker, "VO_1_CLIMAX_REVERSAL", "SHORT", bar.name, bar,
                suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
            ))
        # LONG after sell climax: was falling, big-vol bar with long lower wick
        elif slope_dn and lower_wick_pct >= 0.5 and bar["close"] > bar["open"]:
            out.append(make_signal(
                ticker, "VO_1_CLIMAX_REVERSAL", "LONG", bar.name, bar,
                suggested_sl_pct=0.85, suggested_tgt_pct=1.00,
            ))
    return out


def detect_vo_2_low_drift_accel(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """5-bar drift at low vol (<=0.7x), latest bar >=1.5x with 60% body."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    drift_lookback = int(cfg_get(cfg, "drift_lookback", 5))
    drift_vol_max = cfg_get(cfg, "drift_vol_max", 0.7)
    accel_vol_min = cfg_get(cfg, "accel_vol_min", 1.5)

    for i in range(drift_lookback, len(df)):
        bar = df.iloc[i]
        vol_sma = bar.get("Volume_SMA20", 0)
        if vol_sma <= 0:
            continue
        accel_ratio = bar["volume"] / vol_sma
        if accel_ratio < accel_vol_min:
            continue
        # 5-bar prior-volume avg <= 0.7x
        prior = df.iloc[i - drift_lookback:i]
        prior_avg_ratio = prior["volume"].mean() / vol_sma
        if prior_avg_ratio > drift_vol_max:
            continue

        rng = bar["high"] - bar["low"]
        body = abs(bar["close"] - bar["open"])
        body_pct = (body / rng) if rng > 0 else 0
        if body_pct < 0.6:
            continue

        # Direction: drift direction matches bar direction
        prior_close_first = prior.iloc[0]["close"]
        drift_up = bar["close"] > prior_close_first
        drift_dn = bar["close"] < prior_close_first

        if drift_up and bar["close"] > bar["open"]:
            out.append(make_signal(
                ticker, "VO_2_LOW_DRIFT_ACCEL", "LONG", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
            ))
        elif drift_dn and bar["close"] < bar["open"]:
            out.append(make_signal(
                ticker, "VO_2_LOW_DRIFT_ACCEL", "SHORT", bar.name, bar,
                suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
            ))
    return out


def detect_vo_3_obv_divergence(
    df: pd.DataFrame, ticker: str, cfg: dict | None = None,
) -> list[Signal]:
    """Price makes 10-bar new low/high but OBV does NOT confirm. Reversal bar."""
    df = ensure_volume_sma(df)
    out: list[Signal] = []
    if "OBV" not in df.columns:
        return out
    window = int(cfg_get(cfg, "window", 10))
    adx_max = cfg_get(cfg, "adx_max", 28.0)

    rolling_low = df["low"].rolling(window, min_periods=window).min()
    rolling_high = df["high"].rolling(window, min_periods=window).max()
    obv_rolling_low = df["OBV"].rolling(window, min_periods=window).min()
    obv_rolling_high = df["OBV"].rolling(window, min_periods=window).max()

    for i in range(window, len(df)):
        bar = df.iloc[i]
        if pd.notna(bar.get("ADX")) and bar["ADX"] > adx_max:
            continue
        # LONG divergence: price makes new low, OBV does not
        if bar["low"] <= rolling_low.iloc[i - 1]:
            obv_now = bar["OBV"]
            obv_low_window = obv_rolling_low.iloc[i - 1]
            if pd.notna(obv_now) and pd.notna(obv_low_window) and obv_now > obv_low_window:
                if bar["close"] > bar["open"]:
                    out.append(make_signal(
                        ticker, "VO_3_OBV_DIVERGENCE", "LONG", bar.name, bar,
                        suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                    ))
                    continue
        # SHORT divergence: price makes new high, OBV does not
        if bar["high"] >= rolling_high.iloc[i - 1]:
            obv_now = bar["OBV"]
            obv_high_window = obv_rolling_high.iloc[i - 1]
            if pd.notna(obv_now) and pd.notna(obv_high_window) and obv_now < obv_high_window:
                if bar["close"] < bar["open"]:
                    out.append(make_signal(
                        ticker, "VO_3_OBV_DIVERGENCE", "SHORT", bar.name, bar,
                        suggested_sl_pct=0.80, suggested_tgt_pct=1.00,
                    ))
    return out
