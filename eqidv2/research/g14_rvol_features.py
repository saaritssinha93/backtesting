"""Prior-session, same-stock/same-clock RVOL for frozen V14 morning bars.

The reference is the median of up to 20 *strictly earlier observed valid*
sessions at the identical completed five-minute clock. No current-day value
is included in its own denominator. The first five represented sessions have
no five-session reference when the source starts on 2026-07-29.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import date

import numpy as np
import pandas as pd


IST = "Asia/Kolkata"
CLOCKS = frozenset({"09:20", "09:25", "09:30", "09:35", "09:40", "09:45"})
REQUIRED = frozenset({"sid", "day", "signal_ts", "tradingsymbol", "volume",
                      "source_1m_count", "open", "high", "low", "close"})


def _timestamps(values: pd.Series) -> pd.Series:
    if values.isna().any() or any(pd.Timestamp(value).tzinfo is None for value in values):
        raise ValueError("RVOL signal timestamps must all be timezone-aware")
    return pd.to_datetime(values, errors="raise", utc=True).dt.tz_convert(IST)


def _true(values: pd.Series) -> pd.Series:
    return values.astype(str).str.lower().isin(("true", "1", "1.0"))


class PriorClockRvol:
    """Stateful, strictly chronological same-clock RVOL feature builder."""

    def __init__(self, *, lookback: int = 20, min_periods: int = 5) -> None:
        if (isinstance(lookback, bool) or isinstance(min_periods, bool)
                or not isinstance(lookback, int) or not isinstance(min_periods, int)
                or not 1 <= min_periods <= lookback):
            raise ValueError("Require integer 1 <= min_periods <= lookback")
        self.lookback = lookback
        self.min_periods = min_periods
        self._history: dict[tuple[str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=lookback))
        self.last_day: date | None = None
        self.sessions_seen = 0

    def enrich_day(self, candidates: pd.DataFrame, day: str) -> pd.DataFrame:
        """Return same rows/order with prior-only median RVOL and readiness.

        Invalid current bars do not receive RVOL and do not enter subsequent
        reference sets. A missing stock/clock simply contributes no historical
        observation; no zero-volume row is invented. A real zero-volume bar is
        retained as observed but cannot produce a positive current RVOL.
        """
        missing = REQUIRED - set(candidates)
        if missing:
            raise ValueError(f"RVOL candidate fields missing: {sorted(missing)}")
        parsed_day = date.fromisoformat(str(day))
        if self.last_day is not None and parsed_day <= self.last_day:
            raise ValueError("RVOL days must be strictly increasing")
        out = candidates.copy(deep=True)
        out["flow_tod_rvol"] = np.nan
        out["flow_tod_prior_median_volume"] = np.nan
        out["flow_tod_history_count"] = 0
        out["flow_tod_ready"] = False
        out["flow_tod_current_valid"] = False
        out["flow_tod_source"] = "FROZEN_5M_VOLUME_PRIOR_CLOCK_MEDIAN"
        if out.empty:
            self.last_day = parsed_day
            self.sessions_seen += 1
            return out
        if out.sid.isna().any() or out.sid.duplicated().any():
            raise ValueError("RVOL requires unique candidate SID")
        if out.tradingsymbol.isna().any() or out.tradingsymbol.astype(str).str.len().eq(0).any():
            raise ValueError("RVOL requires stock symbols")
        stamp = _timestamps(out.signal_ts)
        clock = stamp.dt.strftime("%H:%M")
        exact = (stamp.eq(stamp.dt.floor("min")) & stamp.eq(stamp.dt.floor("5min"))
                 & stamp.dt.strftime("%Y-%m-%d").eq(str(day)) & clock.isin(CLOCKS))
        if not exact.all() or not out.day.astype(str).str[:10].eq(str(day)).all():
            raise ValueError("RVOL requires exact completed morning bars of the requested day")
        keys = pd.DataFrame({"symbol": out.tradingsymbol.astype(str), "clock": clock}, index=out.index)
        if keys.duplicated(["symbol", "clock"]).any():
            raise ValueError("Duplicate symbol/clock in RVOL day")
        volume = pd.to_numeric(out.volume, errors="coerce")
        count = pd.to_numeric(out.source_1m_count, errors="coerce")
        price = {name: pd.to_numeric(out[name], errors="coerce")
                 for name in ("open", "high", "low", "close")}
        valid = np.isfinite(volume) & volume.ge(0) & count.eq(5)
        for field in price.values():
            valid &= np.isfinite(field) & field.gt(0)
        valid &= price["high"].ge(price["low"])
        valid &= price["high"].ge(pd.concat([price["open"], price["close"]], axis=1).max(axis=1))
        valid &= price["low"].le(pd.concat([price["open"], price["close"]], axis=1).min(axis=1))
        for flag in ("gap_filled", "is_synthetic", "synthetic", "opening_snapshot",
                     "provisional_stale"):
            if flag in out:
                valid &= ~_true(out[flag])
        if "quality_state" in out:
            quality = out.quality_state.astype(str).str.upper()
            valid &= quality.eq("VALID")
        valid = valid.fillna(False).astype(bool)
        out["flow_tod_current_valid"] = valid

        # Compute every reference before mutating state. Even with the unique
        # key check, this makes the no-current-day lookahead contract explicit.
        for pos, key in enumerate(zip(keys.symbol.to_numpy(), keys.clock.to_numpy())):
            history = self._history[key]
            out.iat[pos, out.columns.get_loc("flow_tod_history_count")] = len(history)
            if len(history) >= self.min_periods:
                median = float(np.median(history))
                if np.isfinite(median) and median > 0:
                    out.iat[pos, out.columns.get_loc("flow_tod_prior_median_volume")] = median
                    if bool(valid.iloc[pos]):
                        ratio = float(volume.iloc[pos]) / median
                        if np.isfinite(ratio):
                            out.iat[pos, out.columns.get_loc("flow_tod_rvol")] = ratio
                            out.iat[pos, out.columns.get_loc("flow_tod_ready")] = True
        for pos, key in enumerate(zip(keys.symbol.to_numpy(), keys.clock.to_numpy())):
            if bool(valid.iloc[pos]):
                self._history[key].append(float(volume.iloc[pos]))
        self.last_day = parsed_day
        self.sessions_seen += 1
        return out


__all__ = ["PriorClockRvol", "CLOCKS"]
