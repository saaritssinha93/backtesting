"""Causal liquidity diagnostics and explicit preselection eligibility.

All turnover figures use observed cash minute close * volume, an OHLCV proxy
for traded rupees. Missing minutes remain missing. Historical metrics use the
previous scheduled sessions, including absent sessions; current metrics stop at
the confirmation minute. No execution outcome, future gap or P&L is consulted.
Apply the mask to the full strict candidate pool before native ranking.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from .features import IST, clean_minutes


@dataclass(frozen=True)
class ScreenConfig:
    """Explicit research thresholds; this module chooses no winning threshold."""

    min_prior_median_turnover_rupees: float
    min_prior_median_minute_coverage: float
    min_prior_full_session_fraction: float
    max_order_participation10: float
    min_current_active_fraction: float = 0.0
    min_history_sessions: int = 20
    require_open_prefix: bool = True

    def __post_init__(self):
        if not np.isfinite(self.min_prior_median_turnover_rupees) or self.min_prior_median_turnover_rupees < 0:
            raise ValueError("Minimum turnover must be finite and nonnegative")
        if not np.isfinite(self.max_order_participation10) or self.max_order_participation10 <= 0:
            raise ValueError("Maximum participation must be positive and finite")
        for name in ("min_prior_median_minute_coverage", "min_prior_full_session_fraction", "min_current_active_fraction"):
            value = getattr(self, name)
            if not np.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{name} must be between zero and one")
        if not isinstance(self.min_history_sessions, int) or self.min_history_sessions < 1:
            raise ValueError("Minimum history must be a positive integer")

    def to_dict(self) -> dict:
        return asdict(self)


def _times(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(IST)
    else:
        parsed = parsed.dt.tz_convert(IST)
    return parsed.dt.as_unit("ns")


def daily_liquidity(minute: pd.DataFrame, session_days: Iterable[str]) -> pd.DataFrame:
    """One record per supplied trading session, even when it has no bars.

Use the full exchange calendar, not a subset selected after inspecting paths.
An absent session has zero coverage and unavailable turnover/range. Observed
turnover on a partial day is a lower bound, never a fabricated full-day sum.
"""
    days = sorted(set(str(day) for day in session_days))
    out = pd.DataFrame(index=pd.Index(days, name="day"))
    work = clean_minutes(minute)
    if work.empty:
        out["observed_minutes"] = 0
        out["turnover_rupees"] = np.nan
        out["range_pct"] = np.nan
        out["active_minutes"] = 0
        out["range_minutes"] = 0
    else:
        work["day"] = work.ts.dt.strftime("%Y-%m-%d")
        work["turnover_rupees"] = work.close.astype(float) * work.volume.astype(float)
        work["active"] = work.volume.gt(0)
        work["has_range"] = work.high.gt(work.low)
        daily = work.groupby("day", sort=True).agg(
            observed_minutes=("ts", "size"), turnover_rupees=("turnover_rupees", "sum"),
            first_open=("open", "first"), day_high=("high", "max"), day_low=("low", "min"),
            active_minutes=("active", "sum"), range_minutes=("has_range", "sum"))
        daily["range_pct"] = (daily.day_high - daily.day_low) / daily.first_open * 100.
        out = out.join(daily[["observed_minutes", "turnover_rupees", "range_pct", "active_minutes", "range_minutes"]])
        out[["observed_minutes", "active_minutes", "range_minutes"]] = out[["observed_minutes", "active_minutes", "range_minutes"]].fillna(0)
    out["minute_coverage"] = out.observed_minutes / 375.
    out["active_fraction"] = out.active_minutes / 375.
    out["range_fraction"] = out.range_minutes / 375.
    out["full_session"] = out.observed_minutes.eq(375)
    return out.reset_index()


def add_capacity_features(candidates: pd.DataFrame, *, order_notional_rupees: float) -> pd.DataFrame:
    """Vectorized size diagnostics for an existing strict candidate pool.

The five-minute denominator deliberately preserves native ``traded_value``
(signal close * five-minute volume). The confirmation denominator uses its
completed cash close * volume. This helper sets no eligibility thresholds.
"""
    if not np.isfinite(order_notional_rupees) or order_notional_rupees <= 0:
        raise ValueError("Order notional must be positive and finite")
    out = candidates.copy()
    signal = pd.to_numeric(out["traded_value"], errors="coerce")
    close = pd.to_numeric(out["confirmation_close"], errors="coerce")
    volume = pd.to_numeric(out["confirmation_volume"], errors="coerce")
    confirmation = close * volume
    signal_valid = np.isfinite(signal) & signal.gt(0)
    confirmation_valid = np.isfinite(close) & np.isfinite(volume) & close.gt(0) & volume.gt(0) & np.isfinite(confirmation)
    out["liq_signal5_turnover_rupees"] = signal.where(signal_valid)
    out["liq_confirmation_turnover_rupees"] = confirmation.where(confirmation_valid)
    out["liq_order_participation5"] = order_notional_rupees / out.liq_signal5_turnover_rupees
    out["liq_order_participation1"] = order_notional_rupees / out.liq_confirmation_turnover_rupees
    return out


def add_liquidity_features(minute: pd.DataFrame, candidates: pd.DataFrame, *,
                           session_days: Iterable[str], order_notional_rupees: float,
                           history_sessions: int = 20) -> pd.DataFrame:
    """Attach past-only metrics for a single symbol, preserving candidate order.

The denominator of participation is the ten completed cash minutes ending at
confirmation_ts. It measures planned order size against recent traded value,
not bid/ask depth or guaranteed fill capacity. All thresholds remain external.
"""
    if history_sessions < 1 or int(history_sessions) != history_sessions:
        raise ValueError("history_sessions must be a positive integer")
    if not np.isfinite(order_notional_rupees) or order_notional_rupees <= 0:
        raise ValueError("Order notional must be positive and finite")
    if "tradingsymbol" in candidates and candidates.tradingsymbol.nunique() > 1:
        raise ValueError("Liquidity features require one symbol at a time")
    if "symbol" in minute and minute.symbol.nunique() > 1:
        raise ValueError("Cash minute input contains multiple symbols")
    out = candidates.copy()
    if out.empty:
        return out
    confirmation = _times(out["confirmation_ts"])
    if confirmation.isna().any():
        raise ValueError("Every candidate requires a valid confirmation_ts")
    candidate_days = confirmation.dt.strftime("%Y-%m-%d")
    daily = daily_liquidity(minute, session_days).set_index("day")
    if not set(candidate_days).issubset(daily.index):
        raise ValueError("Candidate day missing from the supplied exchange calendar")
    historical = pd.DataFrame(index=daily.index)
    prior = daily.shift(1)
    historical["liq_history_sessions"] = daily.observed_minutes.gt(0).shift(1).rolling(history_sessions, min_periods=1).sum()
    for source, target in (
        ("turnover_rupees", "liq_prior_median_turnover_rupees"),
        ("minute_coverage", "liq_prior_median_minute_coverage"),
        ("active_fraction", "liq_prior_median_active_fraction"),
        ("range_fraction", "liq_prior_median_range_fraction"),
        ("range_pct", "liq_prior_median_range_pct"),
    ):
        historical[target] = prior[source].rolling(history_sessions, min_periods=history_sessions).median()
    historical["liq_prior_full_session_fraction"] = daily.full_session.astype(float).shift(1).rolling(history_sessions, min_periods=history_sessions).mean()
    historical["liq_prior_minute_coverage_p10"] = prior.minute_coverage.rolling(history_sessions, min_periods=history_sessions).quantile(.1)
    historical["liq_prior_last_session"] = pd.Series(daily.index, index=daily.index).shift(1)
    for column in historical:
        out[column] = candidate_days.map(historical[column]).to_numpy()

    work = clean_minutes(minute)
    work["ts"] = _times(work.ts)
    work = work.set_index("ts")
    current = pd.DataFrame(index=work.index)
    turnover = work.close.astype(float) * work.volume.astype(float)
    current["liq_observed_confirmation_turnover_rupees"] = turnover
    observed = work.close.rolling("10min", min_periods=1).count()
    current["liq_current10_minute_coverage"] = observed / 10.
    current["liq_current10_turnover_rupees"] = turnover.rolling("10min", min_periods=1).sum().where(observed.eq(10))
    current["liq_current10_active_fraction"] = work.volume.gt(0).astype(float).rolling("10min", min_periods=1).sum() / 10.
    current["liq_current10_range_fraction"] = work.high.gt(work.low).astype(float).rolling("10min", min_periods=1).sum() / 10.
    day = work.index.normalize()
    seen = work.groupby(day).cumcount() + 1
    expected = (work.index - day).total_seconds() / 60 - 555
    current["liq_open_prefix_complete"] = seen.to_numpy() == expected
    for column in current:
        out[column] = current[column].reindex(pd.DatetimeIndex(confirmation)).to_numpy()
    out["liq_order_notional_rupees"] = float(order_notional_rupees)
    out["liq_order_participation10"] = order_notional_rupees / out.liq_current10_turnover_rupees.where(out.liq_current10_turnover_rupees.gt(0))
    out["liq_order_participation_observed1"] = order_notional_rupees / out.liq_observed_confirmation_turnover_rupees.where(out.liq_observed_confirmation_turnover_rupees.gt(0))
    out["liq_available_ts"] = confirmation.to_numpy()
    if {"traded_value", "confirmation_close", "confirmation_volume"}.issubset(out.columns):
        out = add_capacity_features(out, order_notional_rupees=order_notional_rupees)
    return out


def liquidity_decisions(rows: pd.DataFrame, config: ScreenConfig) -> pd.DataFrame:
    """Explain every rejected candidate using only confirmation-time metrics."""
    checks = {
        "INSUFFICIENT_PRIOR_SESSIONS": rows.liq_history_sessions.ge(config.min_history_sessions),
        "LOW_PRIOR_TURNOVER": rows.liq_prior_median_turnover_rupees.ge(config.min_prior_median_turnover_rupees),
        "LOW_PRIOR_MINUTE_COVERAGE": rows.liq_prior_median_minute_coverage.ge(config.min_prior_median_minute_coverage),
        "LOW_PRIOR_FULL_SESSION_FRACTION": rows.liq_prior_full_session_fraction.ge(config.min_prior_full_session_fraction),
        "INCOMPLETE_CURRENT10": rows.liq_current10_minute_coverage.eq(1.),
        "LOW_CURRENT_ACTIVE_FRACTION": rows.liq_current10_active_fraction.ge(config.min_current_active_fraction),
        "EXCESSIVE_ORDER_PARTICIPATION": rows.liq_order_participation10.le(config.max_order_participation10),
    }
    if config.require_open_prefix:
        checks["INCOMPLETE_OPEN_PREFIX"] = rows.liq_open_prefix_complete.eq(True)
    frame = pd.DataFrame(checks, index=rows.index).fillna(False)
    # Comparison with infinity must not convert an unavailable metric into a pass.
    numeric = ["liq_history_sessions", "liq_prior_median_turnover_rupees", "liq_prior_median_minute_coverage",
               "liq_prior_full_session_fraction", "liq_current10_minute_coverage", "liq_current10_active_fraction",
               "liq_order_participation10"]
    frame["NONFINITE_LIQUIDITY_METRIC"] = np.isfinite(rows[numeric].to_numpy(dtype=float)).all(axis=1)
    return pd.DataFrame({"liquidity_pass": frame.all(axis=1),
        "liquidity_reject_reason": frame.apply(lambda row: "|".join(row.index[~row]), axis=1)}, index=rows.index)


def liquidity_mask(rows: pd.DataFrame, config: ScreenConfig) -> pd.Series:
    return liquidity_decisions(rows, config).liquidity_pass
