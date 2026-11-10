"""Causal price-action features for V14's frozen morning candidate pool.

The signal candle is the completed five-minute bar at 09:20--09:45. Its
confirmation is exactly the next completed one-minute bar. Feature values
never use the later native entry or exit path. A confirmation bar whose range
touches a level and closes favorably is only an OHLC proxy for a retest:
intrabar order of the touch and move is not observed.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


IST = "Asia/Kolkata"
CLOCKS = frozenset(("09:20", "09:25", "09:30", "09:35", "09:40", "09:45"))
REQUIRED = frozenset((
    "sid", "day", "tradingsymbol", "side", "signal_ts", "confirmation_ts",
    "open", "high", "low", "close", "signal_close", "source_1m_count",
    "confirmation_open", "confirmation_high", "confirmation_low",
    "confirmation_close",
))


def _stamps(values: pd.Series, label: str) -> pd.Series:
    if values.isna().any() or any(pd.Timestamp(value).tzinfo is None for value in values):
        raise ValueError(f"V14 price-action {label} requires aware timestamps")
    return pd.to_datetime(values, errors="raise", utc=True).dt.tz_convert(IST)


def _number(frame: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_numeric(frame[field], errors="coerce")


def _true(values: pd.Series) -> pd.Series:
    return values.astype(str).str.lower().isin(("true", "1", "1.0"))


def _ohlc_valid(o: pd.Series, h: pd.Series, low: pd.Series, c: pd.Series,
                *, require_range: bool) -> pd.Series:
    valid = pd.Series(True, index=o.index)
    for value in (o, h, low, c):
        valid &= np.isfinite(value) & value.gt(0)
    valid &= h.ge(pd.concat((o, c), axis=1).max(axis=1))
    valid &= low.le(pd.concat((o, c), axis=1).min(axis=1))
    valid &= h.gt(low) if require_range else h.ge(low)
    return valid.fillna(False).astype(bool)


def enrich_day(candidates: pd.DataFrame, day: str) -> pd.DataFrame:
    """Preserve row/index order and add exact-price-action readiness/proxies."""
    out = candidates.copy(deep=True)
    out["pa_prior_ready"] = False
    out["pa_failed_breakout"] = False
    out["pa_confirmation_ready"] = False
    out["pa_confirmation_breakout"] = False
    out["pa_touch_hold"] = False
    out["pa_signal_feature_ts"] = pd.NaT
    out["pa_confirmation_feature_ts"] = pd.NaT
    out["pa_prior_signal_ts"] = pd.NaT
    if out.empty:
        return out
    missing = REQUIRED - set(out)
    if missing:
        raise ValueError(f"V14 price-action candidate fields missing: {sorted(missing)}")
    if out.sid.isna().any() or out.sid.duplicated().any():
        raise ValueError("V14 price-action requires unique candidate SID")
    if out.tradingsymbol.isna().any() or out.tradingsymbol.astype(str).str.len().eq(0).any():
        raise ValueError("V14 price-action requires stock symbol")
    signal = _stamps(out.signal_ts, "signal")
    confirmation = _stamps(out.confirmation_ts, "confirmation")
    if (not out.day.astype(str).str[:10].eq(day).all()
            or not signal.dt.strftime("%Y-%m-%d").eq(day).all()
            or not confirmation.dt.strftime("%Y-%m-%d").eq(day).all()):
        raise ValueError("V14 price-action requires one exact requested session")
    if out.assign(_signal=signal).duplicated(["tradingsymbol", "_signal"]).any():
        raise ValueError("Duplicate stock/signal clock in V14 price-action source")
    out["pa_signal_feature_ts"] = signal
    out["pa_confirmation_feature_ts"] = confirmation

    clock = signal.dt.strftime("%H:%M")
    signal_clock = (clock.isin(CLOCKS) & signal.eq(signal.dt.floor("5min"))
                    & signal.eq(signal.dt.floor("min")))
    confirmation_clock = (confirmation.eq(signal + pd.Timedelta(minutes=1))
                          & confirmation.eq(confirmation.dt.floor("min")))
    side_valid = out.side.isin(("LONG", "SHORT"))
    opening = clock.eq("09:20")
    o, h, low, c = (_number(out, key) for key in ("open", "high", "low", "close"))
    signal_close = _number(out, "signal_close")
    count = _number(out, "source_1m_count")
    signal_valid = (_ohlc_valid(o, h, low, c, require_range=False)
                    & signal_clock & side_valid & count.eq(5)
                    & np.isfinite(signal_close)
                    & np.isclose(signal_close.to_numpy(float), c.to_numpy(float),
                                 rtol=0, atol=1e-6, equal_nan=False))
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale",
                 "is_synthetic", "synthetic"):
        if flag in out:
            signal_valid &= ~_true(out[flag])
    if "v9_5m_feature_ts" in out:
        at = _stamps(out.v9_5m_feature_ts, "five-minute source")
        signal_valid &= at.le(signal)

    co, ch, cl, cc = (_number(out, key) for key in (
        "confirmation_open", "confirmation_high", "confirmation_low",
        "confirmation_close"))
    confirmed = (_ohlc_valid(co, ch, cl, cc, require_range=True)
                 & confirmation_clock & signal_valid)
    if "confirmation_source_flagged" in out:
        confirmed &= ~_true(out.confirmation_source_flagged)
    if "v9_exact_confirmation_present" in out:
        confirmed &= _true(out.v9_exact_confirmation_present)
    if "confirmed" in out:
        confirmed &= _true(out.confirmed)
    if "v9_1m_feature_ts" in out:
        at = _stamps(out.v9_1m_feature_ts, "one-minute source")
        confirmed &= at.eq(confirmation)
    if "v9_feature_available_ts" in out:
        at = _stamps(out.v9_feature_available_ts, "availability")
        confirmed &= at.le(confirmation)
    out["pa_confirmation_ready"] = confirmed.fillna(False).astype(bool)
    long = out.side.eq("LONG")
    short = out.side.eq("SHORT")
    out["pa_confirmation_breakout"] = (confirmed & (
        (long & cc.gt(h)) | (short & cc.lt(low)))).fillna(False).astype(bool)
    out["pa_touch_hold"] = (confirmed & (
        (long & cl.le(signal_close) & cc.gt(signal_close))
        | (short & ch.ge(signal_close) & cc.lt(signal_close)))).fillna(False).astype(bool)

    # Only the immediately prior completed five-minute stock candle is a
    # valid breakout reference. A missing 09:25 cannot make 09:20 a 09:30
    # reference. The first 09:20 bar deliberately has no prior session bar.
    work = pd.DataFrame({
        "_pos": np.arange(len(out)), "symbol": out.tradingsymbol.to_numpy(),
        "ts": signal.to_numpy(), "high": h.to_numpy(float),
        "low": low.to_numpy(float), "close": c.to_numpy(float),
        "valid": signal_valid.to_numpy(bool),
    }).sort_values(["symbol", "ts"], kind="stable")
    grouped = work.groupby("symbol", sort=False)
    previous_ts = grouped.ts.shift()
    previous_high = grouped.high.shift()
    previous_low = grouped.low.shift()
    previous_valid = grouped.valid.shift().eq(True)
    exact_prior = (work.valid & previous_valid
                   & (work.ts - previous_ts).eq(pd.Timedelta(minutes=5)))
    pos = work._pos.to_numpy(dtype=int)
    prior_ready = np.zeros(len(out), dtype=bool)
    prior_ready[pos] = exact_prior.to_numpy(bool)
    out["pa_prior_ready"] = prior_ready
    previous = pd.to_datetime(previous_ts, utc=True).dt.tz_convert(IST)
    out["pa_prior_signal_ts"] = pd.Series(previous.array, index=pos).sort_index().array
    signed_long = long.to_numpy(bool)[pos]
    signed_short = short.to_numpy(bool)[pos]
    failure_sorted = exact_prior & (
        (signed_long & work.high.gt(previous_high) & work.close.le(previous_high))
        | (signed_short & work.low.lt(previous_low) & work.close.ge(previous_low)))
    failure = np.zeros(len(out), dtype=bool)
    failure[pos] = failure_sorted.fillna(False).to_numpy(bool)
    out["pa_failed_breakout"] = failure
    if (out.loc[opening, "pa_prior_ready"].any()
            or out.loc[opening, "pa_failed_breakout"].any()):
        raise ValueError("09:20 V14 signal used a prior in-session price-action bar")
    return out


__all__ = ["enrich_day", "CLOCKS"]
