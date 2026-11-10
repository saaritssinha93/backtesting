"""Causal opening-anchored VWAP and candle-pressure features for V14.

The anchor is the first completed 09:20 cash five-minute bar. VWAP is a
volume-weighted *bar typical-price approximation* (H+L+C)/3, not a measured
trade-level VWAP. Pressure is a close-location/volume proxy, not observed
aggressor buy-minus-sell volume. Only exact completed, contiguous bars enter
each feature. Existing V18 VWAP values are checked where present; later
expanded stock clocks can be recomputed from the same frozen OHLCV rows.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


IST = "Asia/Kolkata"
CLOCKS = ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45")
REQUIRED = frozenset((
    "sid", "day", "signal_ts", "tradingsymbol", "side",
    "open", "high", "low", "close", "volume", "source_1m_count",
))
TOL = 1e-7


def _stamps(values: pd.Series) -> pd.Series:
    for value in values.dropna():
        if pd.Timestamp(value).tzinfo is None:
            raise ValueError("V14 AVWAP signal timestamps must be timezone-aware")
    return pd.to_datetime(values, errors="raise", utc=True).dt.tz_convert(IST)


def _numeric(frame: pd.DataFrame, name: str) -> np.ndarray:
    return pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float)


def _true(values: pd.Series) -> np.ndarray:
    return values.astype(str).str.lower().isin(("true", "1", "1.0")).to_numpy(bool)


def enrich_day(candidates: pd.DataFrame, day: str) -> pd.DataFrame:
    """Preserve source row order and add per-candidate completed-bar features.

    A missing/invalid bar breaks the prefix for that stock's later clocks.
    Stored V18 values are only audit evidence; their absence does not erase
    recomputable OHLCV from the frozen complete candidate pool.
    """
    out = candidates.copy(deep=True)
    n = len(out)
    for field in ("flow_avwap", "flow_avwap_distance_signed_pct",
                  "flow_avwap_slope_signed_pct", "flow_pressure3_signed"):
        out[field] = np.nan
    for field in ("flow_avwap_ready", "flow_avwap_slope_ready", "flow_pressure3_ready"):
        out[field] = False
    out["flow_avwap_source"] = "INVALID_OR_MISSING_PREFIX"
    out["flow_anchor_clock"] = "09:20"
    out["flow_feature_ts"] = pd.NaT
    if out.empty:
        return out
    missing = REQUIRED - set(out)
    if missing:
        raise ValueError(f"V14 AVWAP candidate fields missing: {sorted(missing)}")
    if out.sid.isna().any() or out.sid.duplicated().any():
        raise ValueError("V14 AVWAP requires unique candidate SID")
    stamp = _stamps(out.signal_ts)
    if (out.day.astype(str).str[:10] != day).any() or not stamp.dt.strftime("%Y-%m-%d").eq(day).all():
        raise ValueError("V14 AVWAP requires one exact requested session")
    if out.tradingsymbol.isna().any() or out.assign(_ts=stamp).duplicated(["tradingsymbol", "_ts"]).any():
        raise ValueError("Duplicate or missing V14 AVWAP stock/clock identity")
    out["flow_feature_ts"] = stamp
    work = out.copy(deep=False)
    work["_position"] = np.arange(n)
    work["_ts"] = stamp
    work = work.sort_values(["tradingsymbol", "_ts"], kind="stable")
    avwap = np.full(n, np.nan)
    distance = np.full(n, np.nan)
    slope = np.full(n, np.nan)
    pressure = np.full(n, np.nan)
    ready = np.zeros(n, dtype=bool)
    slope_ready = np.zeros(n, dtype=bool)
    pressure_ready = np.zeros(n, dtype=bool)
    source = np.full(n, "INVALID_OR_MISSING_PREFIX", dtype=object)
    opening = pd.Timestamp(f"{day} 09:20", tz=IST)
    for _, part in work.groupby("tradingsymbol", sort=False):
        pos = part._position.to_numpy(dtype=int)
        m = len(part)
        clocks = pd.DatetimeIndex(part._ts)
        expected = pd.date_range(opening, periods=m, freq="5min")
        clock_ok = np.asarray(clocks == expected) & (np.arange(m) < len(CLOCKS))
        o, h, low, c, vol = (_numeric(part, key) for key in ("open", "high", "low", "close", "volume"))
        count = _numeric(part, "source_1m_count")
        bar_ok = (clock_ok & (count == 5)
                  & np.isfinite(o) & (o > 0)
                  & np.isfinite(h) & (h > 0)
                  & np.isfinite(low) & (low > 0)
                  & np.isfinite(c) & (c > 0)
                  & (h >= np.maximum(o, c)) & (low <= np.minimum(o, c))
                  & (h >= low) & np.isfinite(vol) & (vol > 0))
        # A synthetic or repaired candle cannot enter an opening-anchored
        # cumulative feature. A flagged prior bar breaks every later prefix.
        for flag in ("gap_filled", "opening_snapshot", "provisional_stale",
                     "is_synthetic", "synthetic"):
            if flag in part:
                bar_ok &= ~_true(part[flag])
        prefix = np.logical_and.accumulate(bar_ok)
        safe_volume = np.where(bar_ok, vol, 0.)
        typical = (h + low + c) / 3.
        safe_weighted = np.where(bar_ok, typical * safe_volume, 0.)
        cumulative_volume = np.cumsum(safe_volume)
        cumulative_weighted = np.cumsum(safe_weighted)
        current = np.divide(cumulative_weighted, cumulative_volume,
                            out=np.full(m, np.nan), where=cumulative_volume > 0)
        current[~prefix] = np.nan
        prior_avwap = np.r_[np.nan, current[:-1]]
        local_slope = 100 * (current / prior_avwap - 1)
        local_distance = 100 * (c / current - 1)
        location = np.divide(2*c-h-low, h-low, out=np.zeros(m), where=(h-low) > 0)
        weighted_location = pd.Series(location * safe_volume).rolling(3, min_periods=1).sum().to_numpy(float)
        rolling_volume = pd.Series(safe_volume).rolling(3, min_periods=1).sum().to_numpy(float)
        local_pressure = np.divide(weighted_location, rolling_volume,
                                   out=np.full(m, np.nan), where=rolling_volume > 0)
        sign = np.where(part.side.eq("LONG"), 1., np.where(part.side.eq("SHORT"), -1., np.nan))
        signed_ready = prefix & np.isfinite(sign)
        local_slope_ready = signed_ready & (np.arange(m) > 0) & np.isfinite(local_slope)
        avwap[pos] = np.where(prefix, current, np.nan)
        distance[pos] = np.where(signed_ready, sign * local_distance, np.nan)
        slope[pos] = np.where(local_slope_ready, sign * local_slope, np.nan)
        pressure[pos] = np.where(signed_ready, sign * local_pressure, np.nan)
        ready[pos] = signed_ready
        slope_ready[pos] = local_slope_ready
        pressure_ready[pos] = signed_ready & np.isfinite(local_pressure)
        if "v18_vwap" in part:
            stored = _numeric(part, "v18_vwap")
            overlap = prefix & np.isfinite(stored)
            if not np.all(np.isclose(current[overlap], stored[overlap], rtol=0, atol=TOL)):
                raise ValueError(f"Stored V18 VWAP parity failed: {day} {part.tradingsymbol.iloc[0]}")
            source[pos] = np.where(prefix, np.where(np.isfinite(stored), "V18_VALIDATED",
                                                   "FROZEN_5M_RECONSTRUCTED"),
                                    "INVALID_OR_MISSING_PREFIX")
        else:
            source[pos] = np.where(prefix, "FROZEN_5M_RECONSTRUCTED", "INVALID_OR_MISSING_PREFIX")
        if "v18_vwap_slope_pct" in part:
            stored_slope = _numeric(part, "v18_vwap_slope_pct")
            overlap_slope = local_slope_ready & np.isfinite(stored_slope)
            if not np.all(np.isclose(local_slope[overlap_slope], stored_slope[overlap_slope],
                                     rtol=0, atol=TOL)):
                raise ValueError(f"Stored V18 VWAP slope parity failed: {day} {part.tradingsymbol.iloc[0]}")
    out["flow_avwap"] = avwap
    out["flow_avwap_distance_signed_pct"] = distance
    out["flow_avwap_slope_signed_pct"] = slope
    out["flow_pressure3_signed"] = pressure
    out["flow_avwap_ready"] = ready
    out["flow_avwap_slope_ready"] = slope_ready
    out["flow_pressure3_ready"] = pressure_ready
    out["flow_avwap_source"] = source
    return out


__all__ = ["enrich_day", "CLOCKS", "TOL"]
