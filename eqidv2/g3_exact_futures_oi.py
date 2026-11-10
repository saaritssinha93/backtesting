"""Validate the two completed futures OI bars used by a regular G3 signal."""

from __future__ import annotations

from datetime import timedelta
import math

import pandas as pd


def exact_futures_oi_pair_reason(frame: pd.DataFrame, slot: pd.Timestamp) -> str | None:
    """Return a rejection reason unless the exact, adjacent five-minute pair exists.

    The shared hybrid feature builder shifts over all stored rows, which can
    otherwise use an older same-session bar or the previous trading day as OI
    baseline. This validation is called only for G3's regular signal clocks;
    its 09:20 opening has a separately verified 09:16 baseline.
    """
    if frame.empty or "ts" not in frame or "oi" not in frame:
        return "missing_futures_oi_pair_columns_or_rows"
    signal_ts = pd.Timestamp(slot)
    if signal_ts.tzinfo is None:
        return "invalid_futures_oi_signal_timestamp"
    prior_ts = signal_ts - timedelta(minutes=5)
    try:
        timestamps = pd.to_datetime(frame["ts"], errors="coerce", utc=True)
        signal_utc = signal_ts.tz_convert("UTC")
        prior_utc = prior_ts.tz_convert("UTC")
        current = frame.loc[timestamps.eq(signal_utc)]
        prior = frame.loc[timestamps.eq(prior_utc)]
    except (TypeError, ValueError):
        return "invalid_futures_oi_timestamps"
    if len(current) != 1:
        return ("missing_exact_current_futures_oi_bar" if current.empty
                else "duplicate_exact_current_futures_oi_bar")
    if len(prior) != 1:
        return ("missing_exact_prior_futures_oi_bar" if prior.empty
                else "duplicate_exact_prior_futures_oi_bar")
    # The baseline must also be the immediately preceding stored observation.
    # A stray intervening row would make the shared shift select that row.
    between = timestamps.gt(prior_utc) & timestamps.lt(signal_utc)
    if bool(between.any()):
        return "intervening_futures_oi_bar"
    for row in (prior, current):
        try:
            value = float(row["oi"].iloc[0])
        except (TypeError, ValueError):
            return "invalid_futures_oi_value"
        if not math.isfinite(value) or value <= 0:
            return "invalid_futures_oi_value"
    return None
