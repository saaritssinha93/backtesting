"""Causal, fixed VWAP-band and RSI entry-rejection features for G4 research.

The equity input contract is END-labelled: 09:16 represents 09:15--09:16,
as enforced by ``fno_v13_replay_source._coverage``. Session VWAP includes
09:16 through the completed confirmation candle, using each candle's HLC3
and incremental (not session-cumulative) ``volume``. Its band is 2.5 times
the volume-weighted population standard deviation of those HLC3 prices.

RSI restarts each session, seeds average gain/loss with the first 14 close
changes, then uses Wilder's recurrence. The comparison is against exactly
three minutes earlier, not three arbitrary available observations. A long
is rejected only above the upper band with falling RSI, and a short only
below the lower band with rising RSI. No post-confirmation bar is inspected.

This module neither changes configuration nor selects or executes trades.
Invalid observed data raises, so missing bars cannot silently masquerade as
a profitable filter. A complete early session with fewer than 30 bars is
NOT_APPLICABLE and preserves the unfiltered candidate. A zero-dispersion
session is likewise NOT_APPLICABLE because its z-score is undefined.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
BAND_SIGMAS = 2.5
RSI_PERIOD = 14
RSI_LAG = 3
MIN_SESSION_BARS = 30
FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale")


def wilder_rsi(closes: np.ndarray, period: int = RSI_PERIOD) -> np.ndarray:
    """RSI with arithmetic seeding; NaN until ``period`` price changes exist.

    Entirely flat observations give RSI 50; all gains give 100 and all
    losses give 0. These conventions avoid undefined 0/0 at a flat open.
    """
    values = np.asarray(closes, dtype=float)
    if values.ndim != 1 or not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("RSI requires finite positive one-dimensional closes")
    if not isinstance(period, int) or isinstance(period, bool) or period < 1:
        raise ValueError("RSI period must be a positive integer")
    result = np.full(len(values), np.nan)
    if len(values) <= period:
        return result
    changes = np.diff(values)
    gains, losses = np.maximum(changes, 0), np.maximum(-changes, 0)
    average_gain = float(np.mean(gains[:period]))
    average_loss = float(np.mean(losses[:period]))

    def value():
        if average_loss == 0:
            return 100.0 if average_gain > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + average_gain / average_loss)

    result[period] = value()
    for index in range(period + 1, len(values)):
        average_gain = (average_gain * (period - 1) + gains[index - 1]) / period
        average_loss = (average_loss * (period - 1) + losses[index - 1]) / period
        result[index] = value()
    return result


def _timestamp(value) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise ValueError("Invalid confirmation timestamp")
    return result.tz_localize(IST) if result.tzinfo is None else result.tz_convert(IST)


def _check_flags(window: pd.DataFrame) -> None:
    for field in FLAGS:
        if field not in window:
            continue
        values = window[field]
        words = values.astype(str).str.strip().str.lower()
        numeric = pd.to_numeric(values, errors="coerce")
        affirmative = words.isin(("true", "yes", "on")) | (numeric.notna() & numeric.ne(0))
        recognized = values.isna() | numeric.notna() | words.isin(("true", "yes", "on", "false", "no", "off", ""))
        if affirmative.any() or not recognized.all():
            raise ValueError("Flagged or unrecognized session input: " + field)


def vwap_rsi_features(raw: pd.DataFrame, row: Mapping | pd.Series) -> dict:
    """Return audit fields for one candidate, or raise on corrupt source data.

    ``raw`` contains normalized ``ts, open, high, low, close, volume``.
    ``row`` supplies ``confirmation_ts``, ``confirmation_<OHLC>`` and
    ``side`` (LONG/SHORT). Other sessions and future candles are excluded.
    """
    required = {"ts", "open", "high", "low", "close", "volume"}
    if required.difference(raw.columns):
        raise ValueError("Missing session fields: " + ", ".join(sorted(required.difference(raw.columns))))
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown side")
    stamp = _timestamp(row["confirmation_ts"])
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    end = stamp.normalize() + pd.Timedelta(hours=15, minutes=30)
    if stamp != stamp.floor("min") or not start <= stamp <= end:
        raise ValueError("Confirmation is not a regular completed session minute")
    timestamps = pd.DatetimeIndex(pd.to_datetime(raw["ts"], errors="raise"))
    if timestamps.hasnans:
        raise ValueError("Invalid source timestamp")
    timestamps = timestamps.tz_localize(IST) if timestamps.tz is None else timestamps.tz_convert(IST)
    window = raw.loc[(timestamps >= start) & (timestamps <= stamp)].copy()
    window["ts"] = timestamps[(timestamps >= start) & (timestamps <= stamp)]
    window = window.sort_values("ts", kind="stable")
    expected = pd.date_range(start, stamp, freq="min")
    if not pd.DatetimeIndex(window.ts).equals(expected):
        raise ValueError("Missing or duplicate session minute through confirmation")
    numeric = window[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    if (not np.isfinite(numeric).all() or (numeric[:, :4] <= 0).any() or (numeric[:, 4] < 0).any()
            or (numeric[:, 1] < np.maximum(numeric[:, 0], numeric[:, 3])).any()
            or (numeric[:, 2] > np.minimum(numeric[:, 0], numeric[:, 3])).any()):
        raise ValueError("Invalid session OHLCV")
    _check_flags(window)
    for column, name in enumerate(("open", "high", "low", "close")):
        if not np.isclose(numeric[-1, column], float(row["confirmation_" + name]), atol=.001, rtol=0):
            raise ValueError("Confirmation OHLC drift")

    volume = numeric[:, 4]
    total_volume = float(volume.sum())
    if total_volume <= 0 or not np.isfinite(total_volume):
        raise ValueError("Session cumulative volume must be finite and positive")
    typical = numeric[:, [1, 2, 3]].mean(axis=1)
    vwap = float(np.average(typical, weights=volume))
    sigma = float(np.sqrt(np.average(np.square(typical - vwap), weights=volume)))
    close = float(numeric[-1, 3])
    rsi = wilder_rsi(numeric[:, 3])
    current_rsi = float(rsi[-1]) if np.isfinite(rsi[-1]) else None
    lag_rsi = float(rsi[-1 - RSI_LAG]) if len(rsi) > RSI_LAG and np.isfinite(rsi[-1 - RSI_LAG]) else None
    result = dict(vwap_rsi_status="APPLICABLE", vwap_rsi_reason="",
        vwap_rsi_session_bar_count=len(window), session_vwap=vwap,
        session_volume_weighted_sigma=sigma, session_vwap_z=(close - vwap) / sigma if sigma > 0 else None,
        session_vwap_upper_band=vwap + BAND_SIGMAS * sigma,
        session_vwap_lower_band=vwap - BAND_SIGMAS * sigma,
        rsi14=current_rsi, rsi14_lag3=lag_rsi,
        rsi14_delta3=current_rsi - lag_rsi if current_rsi is not None and lag_rsi is not None else None,
        vwap_rsi_extreme=False, vwap_rsi_momentum_weakening=False, vwap_rsi_flag=False)
    if len(window) < MIN_SESSION_BARS:
        result.update(vwap_rsi_status="NOT_APPLICABLE", vwap_rsi_reason="INSUFFICIENT_SESSION_WARMUP")
        return result
    if sigma == 0:
        result.update(vwap_rsi_status="NOT_APPLICABLE", vwap_rsi_reason="ZERO_VARIANCE")
        return result
    if current_rsi is None or lag_rsi is None:
        raise ValueError("RSI unavailable after minimum session warmup")
    extreme = close > result["session_vwap_upper_band"] if side == "LONG" else close < result["session_vwap_lower_band"]
    weakening = current_rsi < lag_rsi if side == "LONG" else current_rsi > lag_rsi
    result.update(vwap_rsi_extreme=bool(extreme), vwap_rsi_momentum_weakening=bool(weakening),
        vwap_rsi_flag=bool(extreme and weakening))
    return result
