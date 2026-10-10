"""Uncalibrated, observational G-3 fixed-rule pullback monitoring for V14.

This is the original deterministic cash OHLCV monitor, not the learned selective
V2 classifier from the G-3 daywise report. Its thresholds are copied unchanged;
no model is trained and no warning changes entry, exit, capital or trade P&L.
Outcome columns are hindsight labels and never feed warning features.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
MONITOR_VERSION = "G3_FIXED_RULES_UNCALIBRATED_ON_NON_NFO"
SOURCE_PATH = "research/g3_pullback_monitor.py"
SOURCE_SHA256 = "74a820e80b58f32c8c8a378c27cb18d395652e060a6904a28ce4efc112ea424e"
SPECS = {5: .30, 30: .50}


# Exact source function: research/g3_pullback_monitor.py:31
def features(minute: pd.DataFrame) -> pd.DataFrame:
    """Every value in a row depends only on that row and earlier candles."""
    m = minute.copy().sort_values("ts").reset_index(drop=True)
    if m.ts.duplicated().any():
        raise ValueError("Duplicate context minute")
    previous_close = m.close.shift(1)
    tr = pd.concat([m.high-m.low, (m.high-previous_close).abs(),
                    (m.low-previous_close).abs()], axis=1).max(axis=1)
    m["atr14"] = tr.rolling(14, min_periods=14).mean()
    m["sma5"] = m.close.rolling(5, min_periods=5).mean()
    m["sma13"] = m.close.rolling(13, min_periods=13).mean()
    m["prior3_low"] = m.low.shift(1).rolling(3, min_periods=3).min()
    m["prior3_high"] = m.high.shift(1).rolling(3, min_periods=3).max()
    denom = m.volume.shift(1).rolling(20, min_periods=20).mean()
    m["volume_ratio20"] = m.volume / denom.where(denom.gt(0))
    m["momentum3"] = m.close-m.close.shift(3)
    m["momentum5"] = m.close-m.close.shift(5)
    return m


# Exact source function: research/g3_pullback_monitor.py:51
def warning_features(row, sign: int, favorable_close: float) -> dict:
    candle_range = float(row.high-row.low)
    atr = float(row.atr14)
    required = [row.close, row.open, row.sma5, row.sma13, row.prior3_low,
                row.prior3_high, row.momentum3, row.momentum5, atr]
    ready = bool(np.isfinite(required).all() and atr > 0 and candle_range >= 0)
    if not ready:
        return dict(feature_ready=False, fast_warning=False, slow_warning=False,
                    baseline_warning=False)
    adverse_body = -sign*float(row.close-row.open)/candle_range if candle_range > 0 else 0.
    momentum3 = sign*float(row.momentum3)/atr
    momentum5 = sign*float(row.momentum5)/atr
    micro_break = bool(row.close < row.prior3_low if sign == 1 else row.close > row.prior3_high)
    trend_damage = bool(sign*(row.close-row.sma5) < 0 and sign*(row.sma5-row.sma13) < 0)
    giveback = sign*(favorable_close-float(row.close))/atr
    volume = float(row.volume_ratio20)
    fast = momentum3 <= -.5 and (micro_break or (adverse_body >= .5 and np.isfinite(volume) and volume >= 1.2))
    slow = trend_damage and momentum5 <= -.5 and giveback >= 1.
    return dict(feature_ready=True, fast_warning=bool(fast), slow_warning=bool(slow),
                baseline_warning=bool(sign*(row.close-row.open) < 0),
                adverse_body_fraction=adverse_body, momentum3_atr=momentum3,
                momentum5_atr=momentum5, micro_break=micro_break,
                trend_damage=trend_damage, giveback_atr=giveback,
                volume_ratio20=volume, atr14=atr, sma5=float(row.sma5), sma13=float(row.sma13))


# Exact source function: research/g3_pullback_monitor.py:77
def label_outcome(minute: pd.DataFrame, decision_ts: pd.Timestamp,
                  decision_close: float, sign: int, horizon: int, threshold: float,
                  exit_ts: pd.Timestamp, exit_bar_end: pd.Timestamp,
                  exit_price: float, exit_event: str = "INTRABAR") -> dict:
    """Use known temporal order; unresolved intrabar extremes stay unknown."""
    horizon_end = decision_ts + pd.Timedelta(minutes=horizon)
    end = min(horizon_end, exit_ts)
    bar_allowed = minute.ts.le(exit_bar_end) if exit_event == "CLOSE" else minute.ts.lt(exit_bar_end)
    future = minute.loc[minute.ts.gt(decision_ts) & minute.ts.le(end)
                        & bar_allowed]
    extreme = future.low if sign == 1 else future.high
    adverse = -sign*(extreme/decision_close-1)*100
    hit = future.loc[adverse.ge(threshold-1e-10)]
    event_ts = hit.ts.iloc[0] if len(hit) else pd.NaT
    event_source = "COMPLETED_BAR" if len(hit) else ""
    terminal_adverse = -sign*(exit_price/decision_close-1)*100
    exit_in_window = decision_ts < exit_ts <= horizon_end
    threshold_hit = lambda value: value >= threshold-1e-10
    exit_bar = minute.loc[minute.ts.eq(exit_bar_end)]
    opening_adverse = np.nan
    if exit_in_window and exit_event == "INTRABAR":
        if len(exit_bar) != 1:
            raise ValueError("Missing/duplicate intrabar exit candle")
        opening_ts = exit_bar_end-pd.Timedelta(minutes=1)
        opening_adverse = -sign*(float(exit_bar.open.iloc[0])/decision_close-1)*100
        if opening_ts >= decision_ts and threshold_hit(opening_adverse) and (pd.isna(event_ts) or opening_ts < event_ts):
            event_ts, event_source = opening_ts, "EXIT_CANDLE_OPEN"
    if exit_in_window and threshold_hit(terminal_adverse) and (pd.isna(event_ts) or exit_ts < event_ts):
        event_ts, event_source = exit_ts, "ACTUAL_EXIT_FILL"
    event = bool(pd.notna(event_ts))
    unknown = False
    if not event and exit_in_window and exit_event == "INTRABAR":
        adverse_extreme = float(exit_bar.low.iloc[0] if sign == 1 else exit_bar.high.iloc[0])
        unknown = bool(threshold_hit(-sign*(adverse_extreme/decision_close-1)*100))
    censored = bool(exit_ts < horizon_end and not event)
    recovered = False
    if event:
        after = future.loc[future.ts.gt(event_ts)]
        recovered = bool((sign*(after.close/decision_close-1)).ge(0).any())
        if event_ts < exit_ts <= horizon_end and sign*(exit_price/decision_close-1) >= 0:
            recovered = True
    all_adverse = [float(adverse.max())] if len(adverse) else []
    if exit_in_window:
        all_adverse.append(float(terminal_adverse))
    if np.isfinite(opening_adverse):
        all_adverse.append(float(opening_adverse))
    return dict(event=event, event_ts=event_ts, event_source=event_source,
        lead_minutes=float((event_ts-decision_ts).total_seconds()/60) if event else np.nan,
        observed_exposure_minutes=float((end-decision_ts).total_seconds()/60),
        max_future_adverse_pct=max([0., *all_adverse]),
        early_exit=bool(exit_ts < horizon_end), censored=censored,
        outcome_available=not unknown, unknown_reason="UNKNOWN_INTRABAR_ORDER" if unknown else "",
        full_horizon_eligible=not censored and not unknown, recovered_by_later_close=recovered)
