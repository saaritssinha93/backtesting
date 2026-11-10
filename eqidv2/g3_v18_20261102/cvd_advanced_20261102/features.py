"""Causal one-minute volume estimates, aggregated at completed G3 five-minute clocks.

Minute delta is volume*(2*close-high-low)/(high-low), zero for a flat range.
It estimates pressure; it does not measure buyer/seller aggressor trades.
The acceleration compares two nonoverlapping 15-minute normalized windows.
RVOL compares the current 15-minute volume with exactly the preceding twenty
stock-observed session dates at the same clock. Missing prior clock windows
invalidate RVOL; older dates never replace missing observations.

Canonical stock identity/verified historical aliases belong to the data
adapter. This module does not infer or join stock renames.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
METHOD = "ESTIMATED_1M_CLOSE_LOCATION_DELTA_5M_ACCELERATION"
REQUIRED = ("symbol", "minute_end", "open", "high", "low", "close", "volume")
SOURCE_FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic", "synthetic")
FLOAT_FIELDS = (
    "adv_delta5", "adv_session_cvd", "adv_recent15_delta", "adv_recent15_volume",
    "adv_recent15_pressure", "adv_prior15_delta", "adv_prior15_volume", "adv_prior15_pressure",
    "adv_acceleration", "adv_acceleration_normalized", "adv_rvol20", "adv_rvol20_baseline_volume",
    "adv_activity_multiplier",
)
BOOL_FIELDS = ("adv_ready", "adv_session_ready", "adv_rvol_ready")
INTEGER_FIELDS = ("adv_rvol_history_count", "adv_rvol_prior_sessions")
FEATURE_FIELDS = (*FLOAT_FIELDS, *BOOL_FIELDS, *INTEGER_FIELDS, "adv_feature_ts", "adv_reason", "adv_rvol_reason", "adv_method")
OUTPUT_COLUMNS = ("symbol", "bar_end", "session_date", "open", "high", "low", "close", "volume",
                  "source_1m_count", "bar_contiguous", *FEATURE_FIELDS)


def _aware(values: pd.Series, field: str) -> pd.Series:
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        stamps = values.dt.tz_convert(IST)
    else:
        parsed = []
        for value in values:
            stamp = pd.Timestamp(value)
            if pd.isna(stamp) or stamp.tzinfo is None:
                raise ValueError(f"{field} requires nonmissing timezone-aware timestamps")
            parsed.append(stamp)
        stamps = pd.Series(pd.to_datetime(parsed, utc=True), index=values.index).dt.tz_convert(IST)
    if stamps.isna().any():
        raise ValueError(f"{field} contains missing timestamps")
    return stamps.dt.as_unit("ns")


def _numbers(frame: pd.DataFrame, name: str) -> np.ndarray:
    return pd.to_numeric(frame[name], errors="coerce").to_numpy(float)


def _ratio(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return np.divide(a, b, out=np.full(len(a), np.nan), where=np.isfinite(b) & (b > 0))


def _session(frame: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    n = len(frame)
    stamps = pd.DatetimeIndex(frame.minute_end)
    start = stamps[0].normalize() + pd.Timedelta(hours=9, minutes=16)
    end = start.normalize() + pd.Timedelta(hours=15, minutes=30)
    o, h, lo, c, vol = (_numbers(frame, field) for field in ("open", "high", "low", "close", "volume"))
    ohlc_ok = np.isfinite(np.column_stack((o, h, lo, c))).all(axis=1)
    ohlc_ok &= (o > 0) & (h > 0) & (lo > 0) & (c > 0)
    ohlc_ok &= (h >= np.maximum(o, c)) & (lo <= np.minimum(o, c)) & (h >= lo)
    volume_ok = np.isfinite(vol) & (vol >= 0)
    grid_ok = (stamps >= start) & (stamps <= end)
    grid_ok &= ((stamps.asi8 - start.value) % pd.Timedelta(minutes=1).value == 0)
    flagged = np.zeros(n, bool)
    for name in SOURCE_FLAGS:
        if name in frame:
            flagged |= frame[name].astype(str).str.strip().str.lower().isin(("true", "1", "1.0", "yes", "on")).to_numpy()
    valid = ohlc_ok & volume_ok & grid_ok & ~flagged
    span = h - lo
    location = np.divide(2*c-h-lo, span, out=np.zeros(n), where=span > 0)
    delta = np.where(valid, vol * np.clip(location, -1, 1), np.nan)
    volume = np.where(valid, vol, np.nan)

    def rolling(window):
        sums = pd.Series(delta).rolling(window, min_periods=window).sum().to_numpy()
        volumes = pd.Series(volume).rolling(window, min_periods=window).sum().to_numpy()
        contiguous = np.zeros(n, bool)
        if n >= window:
            contiguous[window-1:] = stamps.asi8[window-1:] - stamps.asi8[:n-window+1] == pd.Timedelta(minutes=window-1).value
        complete = contiguous & np.isfinite(sums) & np.isfinite(volumes)
        sums[~complete] = np.nan
        volumes[~complete] = np.nan
        return sums, volumes, complete

    delta5, _, valid5 = rolling(5)
    delta15, volume15, valid15 = rolling(15)
    _, _, valid30 = rolling(30)
    prior_delta = np.full(n, np.nan)
    prior_volume = np.full(n, np.nan)
    if n > 15:
        prior_delta[15:] = delta15[:-15]
        prior_volume[15:] = volume15[:-15]
    # Sparse input-row shifts are safe only after the exact thirty-minute
    # contiguity check; otherwise the earlier window may have a wrong clock.
    prior_delta[~valid30] = np.nan
    prior_volume[~valid30] = np.nan
    pressure = _ratio(delta15, volume15)
    prior_pressure = _ratio(prior_delta, prior_volume)
    acceleration = np.clip(pressure - prior_pressure, -2., 2.)
    prefix = np.logical_and.accumulate(valid & (stamps.asi8 == pd.date_range(start, periods=n, freq="min").asi8))
    session_delta = np.cumsum(np.nan_to_num(delta, nan=0.))
    session_volume = np.cumsum(np.where(valid, vol, 0.))
    session_delta[~prefix] = np.nan

    buckets = stamps.ceil("5min")
    starts = np.r_[0, np.flatnonzero(buckets.asi8[1:] != buckets.asi8[:-1]) + 1]
    stops = np.r_[starts[1:] - 1, n - 1]
    bar_ends = buckets[starts]
    counts = stops - starts + 1
    exact_end = stamps.asi8[stops] == bar_ends.asi8
    bar_valid = valid5[stops] & exact_end & (counts == 5)
    bar_volume = np.add.reduceat(vol, starts)
    ready = valid30[stops] & bar_valid & (volume15[stops] > 0) & (prior_volume[stops] > 0) & (bar_volume > 0)
    session_ready = prefix[stops] & bar_valid & (session_volume[stops] > 0) & (bar_volume > 0)

    def endpoint(values, *, require_bar=True):
        result = values[stops].copy()
        result[~(bar_valid if require_bar else exact_end)] = np.nan
        return result

    acceleration5 = endpoint(acceleration)
    acceleration5[~ready] = np.nan
    reasons = np.full(len(starts), "READY", object)
    reasons[~ready] = "THIRTY_MINUTE_WARMUP_OR_INVALID_WINDOW"
    reasons[(volume15[stops] <= 0) | (prior_volume[stops] <= 0)] = "ZERO_WINDOW_VOLUME"
    reasons[bar_volume == 0] = "ZERO_CURRENT_BAR_VOLUME"
    reasons[~bar_valid] = "INCOMPLETE_OR_INVALID_CURRENT_5M_BAR"
    result = pd.DataFrame(dict(
        symbol=str(frame.symbol.iloc[0]), bar_end=bar_ends,
        session_date=start.strftime("%Y-%m-%d"),
        open=o[starts], high=np.maximum.reduceat(h, starts), low=np.minimum.reduceat(lo, starts), close=c[stops],
        volume=bar_volume, source_1m_count=counts, bar_contiguous=bar_valid,
        adv_delta5=endpoint(delta5), adv_session_cvd=endpoint(session_delta),
        adv_recent15_delta=endpoint(delta15), adv_recent15_volume=endpoint(volume15),
        adv_recent15_pressure=endpoint(pressure), adv_prior15_delta=endpoint(prior_delta),
        adv_prior15_volume=endpoint(prior_volume), adv_prior15_pressure=endpoint(prior_pressure),
        adv_acceleration=acceleration5, adv_acceleration_normalized=acceleration5 / 2.,
        adv_ready=ready, adv_session_ready=session_ready, adv_feature_ts=bar_ends,
        adv_reason=reasons, adv_method=METHOD,
    ))
    # A capture at 14:57 cannot create a completed 15:00 feature. Historical
    # partial buckets remain visible once their scheduled end is in the past.
    return result.loc[result.bar_end.le(cutoff)].copy()


def _rvol(stock: pd.DataFrame, observed_dates: list[str]) -> pd.DataFrame:
    stock = stock.copy()
    if stock.empty:
        return stock
    clocks = stock.bar_end.dt.strftime("%H:%M")
    by_clock = stock.assign(_clock=clocks).pivot(index="session_date", columns="_clock", values="adv_recent15_volume")
    by_clock = by_clock.reindex(observed_dates)
    prior = by_clock.shift(1)
    mean = prior.rolling(20, min_periods=20).mean()
    count = prior.rolling(20, min_periods=1).count()
    row_i = pd.Index(observed_dates).get_indexer(stock.session_date)
    col_i = by_clock.columns.get_indexer(clocks)
    baseline = mean.to_numpy()[row_i, col_i]
    history_count = np.nan_to_num(count.to_numpy()[row_i, col_i], nan=0.).astype(int)
    prior_sessions = np.minimum(row_i, 20)
    value = _ratio(stock.adv_recent15_volume.to_numpy(float), baseline)
    ready = stock.adv_ready.to_numpy(bool) & (prior_sessions == 20) & (history_count == 20) & np.isfinite(value) & (value >= 0)
    reason = np.full(len(stock), "READY", object)
    reason[~stock.adv_ready.to_numpy(bool)] = "CURRENT_ACCELERATION_UNREADY"
    reason[~np.isfinite(baseline) | (baseline <= 0)] = "MISSING_OR_NONPOSITIVE_HISTORY_BASELINE"
    reason[(prior_sessions == 20) & (history_count < 20)] = "MISSING_PRIOR_EXACT_CLOCK_WINDOW"
    reason[prior_sessions < 20] = "WARMUP_LT20_OBSERVED_SESSIONS"
    stock["adv_rvol20"] = value
    stock["adv_rvol20_baseline_volume"] = baseline
    stock["adv_rvol_history_count"] = history_count
    stock["adv_rvol_prior_sessions"] = prior_sessions
    stock["adv_rvol_ready"] = ready
    stock["adv_activity_multiplier"] = np.where(ready, np.clip(value - 1., 0., 1.), np.nan)
    stock["adv_rvol_reason"] = reason
    return stock


def calculate_features(minutes: pd.DataFrame, *, as_of=None) -> pd.DataFrame:
    """Aggregate canonical completed minute bars to causal five-minute features.

    ``adv_ready`` needs thirty consecutive valid minutes in this session,
    positive volume in both fifteen-minute windows and the current 5m candle.
    Therefore the first possible ready clock is 09:45 IST. After a missing
    minute, thirty new valid minutes can restore acceleration readiness; the
    full-session CVD remains unknown for that date.

    ``adv_rvol_ready`` additionally needs exact-clock window volume for every
    one of the preceding twenty stock-observed session dates. Missing dates
    with no provider observations are absent from this disclosed date grid;
    missing clocks within an observed date are never skipped. The twenty dates
    always precede the current date. No final-day information from the current
    session is used, and no fallback replaces the initial twenty-session warmup.

    Invalid minute values remain visible as invalid derived windows. Duplicate
    identities, naive stamps, missing identities or post-capture minutes raise.
    The returned data contain source OHLCV and all ``adv_*`` fields, with source
    timestamps matching G3's completed five-minute signal clocks.
    """
    missing = set(REQUIRED) - set(minutes.columns)
    if missing:
        raise ValueError(f"Missing minute input columns: {sorted(missing)}")
    if minutes.symbol.isna().any() or minutes.symbol.astype(str).str.strip().eq("").any():
        raise ValueError("Stock symbol is missing")
    raw = minutes.copy()
    raw["symbol"] = raw.symbol.astype(str)
    raw["minute_end"] = _aware(raw.minute_end, "minute_end")
    if raw.duplicated(["symbol", "minute_end"]).any():
        raise ValueError("Duplicate stock/minute identity")
    if as_of is not None:
        cutoff = pd.Timestamp(as_of)
        if pd.isna(cutoff) or cutoff.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        cutoff = cutoff.tz_convert(IST)
        if raw.minute_end.gt(cutoff).any():
            raise ValueError("Input includes an incomplete minute after as_of")
    else:
        cutoff = raw.minute_end.max()
    if raw.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        for field in ("bar_end", "adv_feature_ts"):
            out[field] = pd.Series(pd.DatetimeIndex([], tz=IST))
        for field in (*BOOL_FIELDS, "bar_contiguous"):
            out[field] = pd.Series(dtype=bool)
        return out
    raw = raw.sort_values(["symbol", "minute_end"], kind="stable").reset_index(drop=True)
    pieces = []
    for _, stock in raw.groupby("symbol", sort=False):
        dates = stock.minute_end.dt.strftime("%Y-%m-%d")
        observed_dates = sorted(dates.unique())
        sessions = [_session(part, cutoff) for _, part in stock.groupby(dates, sort=True)]
        own = pd.concat(sessions, ignore_index=True)
        pieces.append(_rvol(own, observed_dates))
    result = pd.concat(pieces, ignore_index=True).sort_values(["symbol", "bar_end"], kind="stable").reset_index(drop=True)
    return result.reindex(columns=OUTPUT_COLUMNS)


def attach_features(raw: pd.DataFrame, enriched: pd.DataFrame) -> pd.DataFrame:
    """Attach the same stock's exact completed signal clock, preserving rows."""
    if {"tradingsymbol", "signal_ts"} - set(raw):
        raise ValueError("G3 inputs require tradingsymbol and signal_ts")
    if {"symbol", "bar_end", *FEATURE_FIELDS} - set(enriched):
        raise ValueError("Advanced input is missing calculated feature columns")
    if set(FEATURE_FIELDS) & set(raw):
        raise ValueError("G3 input already contains advanced feature columns")
    right = enriched[["symbol", "bar_end", *FEATURE_FIELDS]].copy()
    right["bar_end"] = _aware(right.bar_end, "bar_end")
    right["adv_feature_ts"] = _aware(right.adv_feature_ts, "adv_feature_ts")
    if not right.bar_end.eq(right.adv_feature_ts).all():
        raise ValueError("Advanced feature timestamp differs from completed bar end")
    if right.duplicated(["symbol", "bar_end"]).any():
        raise ValueError("Duplicate stock/advanced-bar identity")
    left = pd.DataFrame(dict(symbol=raw.tradingsymbol.astype(str).to_numpy(),
        bar_end=_aware(raw.signal_ts, "signal_ts").to_numpy(), _order=np.arange(len(raw))))
    left["bar_end"] = _aware(left.bar_end, "signal_ts")
    merged = left.merge(right, on=["symbol", "bar_end"], how="left", validate="many_to_one", sort=False).sort_values("_order")
    result = raw.copy()
    for field in FEATURE_FIELDS:
        if field in BOOL_FIELDS:
            result[field] = merged[field].eq(True).to_numpy(bool)
        elif field in ("adv_reason", "adv_rvol_reason"):
            result[field] = merged[field].fillna("MISSING_EXACT_BAR").to_numpy()
        elif field == "adv_method":
            result[field] = merged[field].fillna(METHOD).to_numpy()
        else:
            result[field] = merged[field].to_numpy()
    return result
