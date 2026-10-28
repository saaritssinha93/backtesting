"""Compact causal confirmation features for isolated G7 V4 research.

The normalized equity timestamps label candle ends. Every calculation uses
09:16 through the original completed confirmation, never a prospective fill
or an exit. This module defines no rejection thresholds and reads no outcomes.
Volume is incremental bar volume. Unavailable warmups/weights are explicit;
they are not themselves evidence against an entry.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from research.g7_v4_extremes import FIELDS, IST, checked_session, timestamp


def _window(raw: pd.DataFrame, stamp: pd.Timestamp) -> pd.DataFrame:
    missing = set(["ts", *FIELDS]).difference(raw.columns)
    if missing:
        raise ValueError("Missing session fields: " + ", ".join(sorted(missing)))
    times = pd.DatetimeIndex(pd.to_datetime(raw.ts, errors="raise"))
    if times.hasnans:
        raise ValueError("Invalid source timestamp")
    times = times.tz_localize(IST) if times.tz is None else times.tz_convert(IST)
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    mask = (times >= start) & (times <= stamp)
    prefix = raw.loc[mask].copy()
    prefix["ts"] = times[mask]
    prefix[FIELDS] = prefix[FIELDS].apply(pd.to_numeric, errors="coerce")
    volumes = prefix.volume.to_numpy(float)
    if not np.isfinite(volumes).all() or (volumes < 0).any():
        raise ValueError("Invalid volume")
    if volumes.sum() == 0:
        # The shared integrity checker requires positive cumulative weights.
        # Unit weights here validate only timestamps/OHLC/flags, after actual
        # volumes have been checked. Calculations below retain actual zeros.
        checked_session(prefix.assign(volume=1.0), stamp)
        return prefix.sort_values("ts", kind="stable")
    return checked_session(prefix, stamp)


def feature_row(raw: pd.DataFrame, row: Mapping | pd.Series) -> dict:
    """Return original-confirmation features, raising on invalid source data.

    Required row fields: sid, day, side, confirmation_ts, confirmation_open,
    confirmation_high, confirmation_low, confirmation_close, trigger.
    CMF20 needs 20 completed session bars and positive volume in those bars.
    Session VWAP slope compares the current cumulative HLC3 VWAP against its
    value exactly five minutes earlier, after at least ten session bars.
    Failed-breakout status uses the preceding five bars, excluding current.
    All signed measurements are positive in the proposed trade direction.
    """
    stamp = timestamp(row["confirmation_ts"])
    if (stamp != stamp.floor("min")
            or not "09:16" <= stamp.strftime("%H:%M") <= "15:15"):
        raise ValueError("Invalid confirmation clock")
    if str(row["day"]) != str(stamp.date()):
        raise ValueError("Candidate day differs from confirmation day")
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Invalid side")
    sign = 1.0 if side == "LONG" else -1.0
    trigger = float(row["trigger"])
    if not np.isfinite(trigger) or trigger <= 0:
        raise ValueError("Invalid trigger")
    window = _window(raw, stamp)
    current = window.iloc[-1]
    for field in FIELDS[:4]:
        expected = float(row["confirmation_" + field])
        if not np.isfinite(expected) or not np.isclose(current[field], expected, atol=.001, rtol=0):
            raise ValueError("Confirmation OHLC differs from archived order")
    open_, high, low, close = (float(current[field]) for field in FIELDS[:4])
    spread = high - low
    long_location = (close - low) / spread if spread > 0 else .5
    wick = high - max(open_, close) if sign == 1 else min(open_, close) - low
    result = dict(
        sid=int(row["sid"]), day=str(stamp.date()), side=side,
        confirmation_ts=stamp.isoformat(), feature_available_at=stamp.isoformat(),
        sel_session_bars=len(window), sel_candle_ready=True, sel_candle_status="APPLICABLE",
        sel_directional_close_location=float(long_location if sign == 1 else 1 - long_location),
        sel_directional_body_return_pct=sign * (close / open_ - 1) * 100,
        sel_directional_wick_fraction=float(wick / spread) if spread > 0 else 0.0,
        sel_trigger_vs_confirmation_close_pct=sign * (trigger / close - 1) * 100,
        sel_cmf20_ready=False, sel_cmf20_status="NOT_APPLICABLE_WARMUP", sel_signed_cmf20=None,
        sel_vwap_slope5_ready=False, sel_vwap_slope5_status="NOT_APPLICABLE_WARMUP",
        sel_signed_vwap_slope5_pct=None,
        sel_prior5_ready=False, sel_prior5_status="NOT_APPLICABLE_WARMUP",
        sel_prior5_breaks=False, sel_prior5_failed_breakout=False, sel_prior5_edge=None,
    )
    if len(window) >= 20:
        last = window.tail(20)
        weights = last.volume.to_numpy(float)
        total = float(weights.sum())
        if not np.isfinite(total):
            raise ValueError("Nonfinite CMF weights")
        result["sel_cmf20_status"] = "NOT_APPLICABLE_ZERO_VOLUME"
        if total > 0:
            ranges = (last.high - last.low).to_numpy(float)
            numerators = (2 * last.close - last.high - last.low).to_numpy(float)
            multiplier = np.divide(numerators, ranges, out=np.zeros(len(last)), where=ranges > 0)
            result.update(sel_cmf20_ready=True, sel_cmf20_status="APPLICABLE",
                          sel_signed_cmf20=sign * float(np.dot(multiplier, weights) / total))
    if len(window) >= 10:
        volume = window.volume.to_numpy(float)
        cumulative_volume = np.cumsum(volume)
        result["sel_vwap_slope5_status"] = "NOT_APPLICABLE_ZERO_VOLUME"
        if cumulative_volume[-1] > 0 and cumulative_volume[-6] > 0:
            typical = window[["high", "low", "close"]].mean(axis=1).to_numpy(float)
            cumulative_value = np.cumsum(typical * volume)
            now = cumulative_value[-1] / cumulative_volume[-1]
            lag = cumulative_value[-6] / cumulative_volume[-6]
            result.update(sel_vwap_slope5_ready=True, sel_vwap_slope5_status="APPLICABLE",
                          sel_signed_vwap_slope5_pct=sign * float(now / lag - 1) * 100)
    if len(window) >= 6:
        prior = window.iloc[-6:-1]
        edge = float(prior.high.max() if sign == 1 else prior.low.min())
        broke = sign * ((high if sign == 1 else low) - edge) > 0
        failed = broke and sign * (close - edge) <= 0
        result.update(sel_prior5_ready=True, sel_prior5_status="APPLICABLE",
                      sel_prior5_breaks=bool(broke), sel_prior5_failed_breakout=bool(failed),
                      sel_prior5_edge=edge)
    numeric = [result[key] for key in ("sel_directional_close_location", "sel_directional_body_return_pct",
               "sel_directional_wick_fraction", "sel_trigger_vs_confirmation_close_pct")]
    numeric += [result[key] for ready, key in (("sel_cmf20_ready", "sel_signed_cmf20"),
                ("sel_vwap_slope5_ready", "sel_signed_vwap_slope5_pct"),
                ("sel_prior5_ready", "sel_prior5_edge")) if result[ready]]
    if not np.isfinite(numeric).all():
        raise ValueError("Nonfinite advertised-ready selection feature")
    return result
