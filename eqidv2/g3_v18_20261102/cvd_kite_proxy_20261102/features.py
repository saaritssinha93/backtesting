"""Causal five-minute candle-volume estimates for isolated G3 research.

This is an OHLCV pressure proxy, not observed buyer/seller aggressor volume.
The primary allocation is close-location weighting:
    delta_est = volume * (2 * close - high - low) / (high - low)
with a neutral zero estimate for a flat candle. Estimated buy/sell allocations
are (volume +/- delta_est) / 2. Session sums reset at the NSE session open.

The separately named body/range model is a fixed sensitivity check. Neither
model uses subsequent candles, daily final volume or next-day information.
All inputs must label COMPLETED bar ends; Kite opening timestamps must be
shifted forward five minutes by the data adapter before entering this module.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
METHOD = "ESTIMATED_CLOSE_LOCATION_VOLUME_5M"
REQUIRED = ("symbol", "bar_end", "open", "high", "low", "close", "volume")
SOURCE_FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic", "synthetic")
BOOL_FIELDS = (
    "cvd_proxy_bar_valid", "cvd_proxy_prefix_valid", "cvd_proxy_session_ready",
    "cvd_proxy_recent15_ready", "cvd_proxy_ready",
)
FLOAT_FIELDS = (
    "cvd_proxy_delta", "cvd_proxy_buy_volume_est", "cvd_proxy_sell_volume_est",
    "cvd_proxy_session", "cvd_proxy_recent15", "cvd_proxy_session_volume",
    "cvd_proxy_recent15_volume", "cvd_proxy_session_ratio", "cvd_proxy_recent15_ratio",
    "cvd_proxy_body_delta", "cvd_proxy_body_session", "cvd_proxy_body_recent15",
    "cvd_proxy_body_session_ratio", "cvd_proxy_body_recent15_ratio",
)
FEATURE_FIELDS = (*FLOAT_FIELDS, *BOOL_FIELDS, "cvd_proxy_feature_ts", "cvd_proxy_reason", "cvd_proxy_method")


def _aware(values: pd.Series, field: str) -> pd.Series:
    """Reject ambiguous naive input instead of silently interpreting it as UTC."""
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        result = values.dt.tz_convert(IST)
    else:
        parsed = []
        for value in values:
            stamp = pd.Timestamp(value)
            if pd.isna(stamp) or stamp.tzinfo is None:
                raise ValueError(f"{field} must contain nonmissing timezone-aware timestamps")
            parsed.append(stamp)
        result = pd.Series(pd.to_datetime(parsed, utc=True), index=values.index).dt.tz_convert(IST)
    if result.isna().any():
        raise ValueError(f"{field} contains missing timestamps")
    return result.dt.as_unit("ns")


def _number(frame: pd.DataFrame, name: str) -> np.ndarray:
    return pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float)


def _ratio(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    return np.divide(numerator, denominator, out=np.full(len(numerator), np.nan), where=denominator > 0)


def calculate_features(bars: pd.DataFrame, *, as_of=None) -> pd.DataFrame:
    """Return every supplied stock/bar plus explicitly estimated CVD columns.

    Structural ambiguity (missing columns/symbols, naive timestamps, duplicate
    stock/bar identities) raises. Invalid OHLCV, flags and clock gaps remain
    visible with readiness false and unknown derived cumulative values.

    Session CVD requires an unbroken valid prefix beginning at 09:20 IST. A
    missing/bad candle disables session CVD for the rest of that date. Recent
    15-minute estimates require three consecutive valid candles and can recover
    after a gap. ``cvd_proxy_ready`` requires both, and hence cannot recover
    until the next session. A current zero-volume candle is never ready, though
    its zero contribution does not invalidate later session observations.

    If supplied, ``as_of`` is an aware capture time; any not-yet-completed input
    bar raises. The caller owns source/instrument verification and membership.
    """
    missing = set(REQUIRED) - set(bars.columns)
    if missing:
        raise ValueError(f"Missing five-minute input columns: {sorted(missing)}")
    if set(FEATURE_FIELDS) & set(bars.columns):
        raise ValueError("Input already contains CVD proxy feature columns")
    out = bars.copy()
    if out.symbol.isna().any() or out.symbol.astype(str).str.strip().eq("").any():
        raise ValueError("Stock symbol is missing")
    out["symbol"] = out.symbol.astype(str)
    out["bar_end"] = _aware(out.bar_end, "bar_end")
    if out.duplicated(["symbol", "bar_end"]).any():
        raise ValueError("Duplicate stock/bar identity")
    if as_of is not None:
        cutoff = pd.Timestamp(as_of)
        if pd.isna(cutoff) or cutoff.tzinfo is None:
            raise ValueError("as_of must be a timezone-aware capture time")
        if out.bar_end.gt(cutoff).any():
            raise ValueError("Input includes a bar ending after as_of; bar is not yet complete")
    out = out.sort_values(["symbol", "bar_end"], kind="stable").reset_index(drop=True)
    for field in FLOAT_FIELDS:
        out[field] = np.nan
    for field in BOOL_FIELDS:
        out[field] = False
    out["cvd_proxy_feature_ts"] = out.bar_end
    out["cvd_proxy_reason"] = ""
    out["cvd_proxy_method"] = METHOD
    if out.empty:
        return out

    for (_, _), part in out.groupby(["symbol", out.bar_end.dt.strftime("%Y-%m-%d")], sort=False):
        ix = part.index
        n = len(part)
        o, h, lo, c, vol = (_number(part, name) for name in ("open", "high", "low", "close", "volume"))
        ohlc_valid = np.isfinite(np.column_stack((o, h, lo, c))).all(axis=1)
        ohlc_valid &= (o > 0) & (h > 0) & (lo > 0) & (c > 0)
        ohlc_valid &= (h >= np.maximum(o, c)) & (lo <= np.minimum(o, c)) & (h >= lo)
        volume_valid = np.isfinite(vol) & (vol >= 0)
        stamps = pd.DatetimeIndex(part.bar_end)
        start = stamps[0].normalize() + pd.Timedelta(hours=9, minutes=20)
        end = start.normalize() + pd.Timedelta(hours=15, minutes=30)
        elapsed = stamps.asi8 - start.value
        grid_valid = (stamps >= start) & (stamps <= end) & (elapsed % pd.Timedelta(minutes=5).value == 0)
        flagged = np.zeros(n, dtype=bool)
        for field in SOURCE_FLAGS:
            if field in part:
                flagged |= part[field].astype(str).str.strip().str.lower().isin(("true", "1", "1.0", "yes", "on")).to_numpy()
        if "source_1m_count" in part:
            flagged |= _number(part, "source_1m_count") != 5
        valid = ohlc_valid & volume_valid & grid_valid & ~flagged
        expected = pd.date_range(start, periods=n, freq="5min")
        prefix = np.logical_and.accumulate(valid & (stamps.asi8 == expected.asi8))

        span = h - lo
        location = np.divide(2 * c - h - lo, span, out=np.zeros(n), where=span > 0)
        body = np.divide(c - o, span, out=np.zeros(n), where=span > 0)
        delta = np.where(valid, vol * np.clip(location, -1, 1), np.nan)
        body_delta = np.where(valid, vol * np.clip(body, -1, 1), np.nan)
        safe_vol = np.where(valid, vol, np.nan)
        cumulative_volume = np.cumsum(np.where(valid, vol, 0.0))
        session = np.cumsum(np.nan_to_num(delta, nan=0.0))
        body_session = np.cumsum(np.nan_to_num(body_delta, nan=0.0))
        session[~prefix] = np.nan
        body_session[~prefix] = np.nan
        cumulative_volume[~prefix] = np.nan
        recent = pd.Series(delta).rolling(3, min_periods=3).sum().to_numpy()
        body_recent = pd.Series(body_delta).rolling(3, min_periods=3).sum().to_numpy()
        recent_vol = pd.Series(safe_vol).rolling(3, min_periods=3).sum().to_numpy()
        consecutive = np.zeros(n, dtype=bool)
        if n >= 3:
            consecutive[2:] = (stamps.asi8[2:] - stamps.asi8[:-2] == pd.Timedelta(minutes=10).value)
        recent_window_valid = consecutive & np.isfinite(recent) & np.isfinite(recent_vol)
        recent[~recent_window_valid] = np.nan
        body_recent[~recent_window_valid] = np.nan
        recent_vol[~recent_window_valid] = np.nan
        session_ready = prefix & (cumulative_volume > 0) & (vol > 0)
        recent_ready = recent_window_valid & (recent_vol > 0) & (vol > 0)
        ready = session_ready & recent_ready
        reason = np.full(n, "READY", dtype=object)
        reason[~recent_window_valid] = "RECENT15_WARMUP_OR_GAP"
        reason[vol == 0] = "ZERO_CURRENT_VOLUME"
        reason[~prefix] = "INCOMPLETE_SESSION_PREFIX"
        reason[flagged] = "FLAGGED_OR_INCOMPLETE_SOURCE_BAR"
        reason[~grid_valid] = "OUTSIDE_REGULAR_5M_GRID"
        reason[~volume_valid] = "INVALID_VOLUME"
        reason[~ohlc_valid] = "INVALID_OHLC"
        values = dict(
            cvd_proxy_delta=delta,
            cvd_proxy_buy_volume_est=(safe_vol + delta) / 2,
            cvd_proxy_sell_volume_est=(safe_vol - delta) / 2,
            cvd_proxy_session=session,
            cvd_proxy_recent15=recent,
            cvd_proxy_session_volume=cumulative_volume,
            cvd_proxy_recent15_volume=recent_vol,
            cvd_proxy_session_ratio=_ratio(session, cumulative_volume),
            cvd_proxy_recent15_ratio=_ratio(recent, recent_vol),
            cvd_proxy_body_delta=body_delta,
            cvd_proxy_body_session=body_session,
            cvd_proxy_body_recent15=body_recent,
            cvd_proxy_body_session_ratio=_ratio(body_session, cumulative_volume),
            cvd_proxy_body_recent15_ratio=_ratio(body_recent, recent_vol),
            cvd_proxy_bar_valid=valid,
            cvd_proxy_prefix_valid=prefix,
            cvd_proxy_session_ready=session_ready,
            cvd_proxy_recent15_ready=recent_ready,
            cvd_proxy_ready=ready,
            cvd_proxy_reason=reason,
        )
        for field, values_for_field in values.items():
            out.loc[ix, field] = values_for_field
    return out


def attach_features(raw: pd.DataFrame, enriched: pd.DataFrame) -> pd.DataFrame:
    """Attach only exact same completed-bar features, preserving raw row order.

    Missing identities fail closed. This never searches for a nearby candle or
    substitutes a later timestamp; multiple candidates may share one signal bar.
    """
    if {"tradingsymbol", "signal_ts"} - set(raw):
        raise ValueError("G3 inputs require tradingsymbol and signal_ts")
    if {"symbol", "bar_end", *FEATURE_FIELDS} - set(enriched):
        raise ValueError("CVD input is missing calculated feature columns")
    if set(FEATURE_FIELDS) & set(raw):
        raise ValueError("G3 input already contains CVD proxy feature columns")
    right = enriched[["symbol", "bar_end", *FEATURE_FIELDS]].copy()
    right["bar_end"] = _aware(right.bar_end, "bar_end")
    right["cvd_proxy_feature_ts"] = _aware(right.cvd_proxy_feature_ts, "cvd_proxy_feature_ts")
    if not right.bar_end.eq(right.cvd_proxy_feature_ts).all():
        raise ValueError("CVD feature timestamp does not equal its completed bar end")
    if right.duplicated(["symbol", "bar_end"]).any():
        raise ValueError("Duplicate stock/bar feature identity")
    left = pd.DataFrame({
        "symbol": raw.tradingsymbol.astype(str).to_numpy(),
        "bar_end": _aware(raw.signal_ts, "signal_ts").to_numpy(),
        "_order": np.arange(len(raw)),
    })
    left["bar_end"] = _aware(left.bar_end, "signal_ts")
    matched = left.merge(right, on=["symbol", "bar_end"], how="left", validate="many_to_one", sort=False).sort_values("_order")
    out = raw.copy()
    for field in FEATURE_FIELDS:
        if field in BOOL_FIELDS:
            out[field] = matched[field].eq(True).to_numpy(dtype=bool)
        elif field == "cvd_proxy_reason":
            out[field] = matched[field].fillna("MISSING_EXACT_BAR").to_numpy()
        elif field == "cvd_proxy_method":
            out[field] = matched[field].fillna(METHOD).to_numpy()
        else:
            out[field] = matched[field].to_numpy()
    return out
