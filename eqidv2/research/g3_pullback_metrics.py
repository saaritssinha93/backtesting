"""Descriptive metrics for predeclared, regularly sampled pullback forecasts.

Each input row is one *grid* forecast, not a dense alert. Optional
``outcome_available`` defaults to True. Unknown intrabar-order outcomes are
excluded from confusion matrices, retained in source counts, and yield
conservative precision identification bounds. The primary outcome
is whether the adverse event occurred before min(horizon, actual exit), so
early-exit negatives remain in PRIMARY.  FULL_HORIZON retains the supplied
eligibility flag (event observed OR exposure lasting the full horizon).

Rates and confidence intervals are fractions, not percentages. Undefined
ratios are NaN. Wilson intervals are descriptive observation-level intervals;
day-cluster bootstrap intervals are the dependence-aware companion. Bootstrap
differences use the same resampled days for both terms. Undefined bootstrap
ratios are omitted, and their valid replicate counts are reported explicitly.

``days`` is the ordered research-session calendar, including zero-trade days.
Its first 30 days are EARLIER30 and later days LATER16, except that explicit
input split labels take precedence for dates with observations. This permits
small synthetic fixtures without requiring a 46-day calendar.
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd


REQUIRED = (
    "day", "trade_id", "side", "horizon_minutes", "threshold_pct", "split",
    "phase", "warning", "event", "full_horizon_eligible", "lead_minutes",
    "censored", "baseline_warning",
)
PHASES = ("ENTRY_1_5", "EARLY_6_30", "LATE_31_PLUS")
SPLITS = ("ALL", "EARLIER30", "LATER16")
SIDES = ("BOTH", "LONG", "SHORT")
COUNT_COLUMNS = (
    "source_observations", "source_censored_observations", "observations",
    "excluded_observations", "censored_observations", "full_horizon_observations",
    "trades", "days", "calendar_days", "tp", "fp", "fn", "tn",
    "baseline_tp", "baseline_fp", "baseline_fn", "baseline_tn",
    "unknown_observations", "unknown_warnings",
)
METRIC_COLUMNS = (
    "precision", "recall", "fpr", "specificity", "accuracy", "prevalence",
    "warning_rate", "lift", "baseline_precision", "median_lead_minutes_tp",
    "precision_minus_prevalence", "precision_minus_baseline_precision",
    "precision_wilson_low", "precision_wilson_high", "precision_bootstrap_low",
    "precision_bootstrap_high", "precision_minus_prevalence_bootstrap_low",
    "precision_minus_prevalence_bootstrap_high",
    "precision_minus_baseline_precision_bootstrap_low",
    "precision_minus_baseline_precision_bootstrap_high",
    "bootstrap_reps", "bootstrap_valid_reps", "bootstrap_prevalence_difference_valid_reps",
    "bootstrap_baseline_difference_valid_reps",
    "precision_identification_lower", "precision_identification_upper",
)
COLUMNS = (
    "analysis", "split", "side", "horizon_minutes", "threshold_pct", "phase",
    *COUNT_COLUMNS, *METRIC_COLUMNS,
)


def _ratio(numerator, denominator):
    return float(numerator / denominator) if denominator else math.nan


def _wilson(successes: int, total: int) -> tuple[float, float]:
    if not total:
        return math.nan, math.nan
    z = 1.959963984540054
    p = successes / total
    divisor = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / divisor
    half = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / divisor
    return max(0.0, center - half), min(1.0, center + half)


def _validate(frame: pd.DataFrame, days: Iterable[str]):
    calendar = list(days)
    if len(calendar) != len(set(calendar)):
        raise ValueError("days must be a unique ordered session calendar")
    if any(not isinstance(day, str) for day in calendar):
        raise ValueError("days must contain date strings")
    if frame.empty:
        return pd.DataFrame(columns=REQUIRED), calendar, {}
    missing = set(REQUIRED).difference(frame.columns)
    if missing:
        raise ValueError(f"Missing grid forecast columns: {sorted(missing)}")
    data = frame.loc[:, REQUIRED].copy()
    data["outcome_available"] = frame.outcome_available if "outcome_available" in frame else True
    if data[["day", "trade_id", "side", "split", "phase"]].isna().any().any():
        raise ValueError("Grid identifiers and categories must not be null")
    unknown = set(data.day).difference(calendar)
    if unknown:
        raise ValueError(f"Prediction dates absent from days: {sorted(unknown)}")
    for column, allowed in (("side", SIDES[1:]), ("split", SPLITS[1:]), ("phase", PHASES)):
        if not data[column].isin(allowed).all():
            raise ValueError(f"Invalid {column} category")
    for column in ("warning", "event", "full_horizon_eligible", "censored", "baseline_warning", "outcome_available"):
        if data[column].isna().any() or not data[column].isin([True, False]).all():
            raise ValueError(f"{column} must contain non-null booleans")
        data[column] = data[column].astype(bool)
    for column in ("horizon_minutes", "threshold_pct"):
        data[column] = pd.to_numeric(data[column], errors="raise")
        if not (np.isfinite(data[column]) & (data[column] > 0)).all():
            raise ValueError(f"{column} must be finite and positive")
    if not (data.horizon_minutes % 1 == 0).all():
        raise ValueError("horizon_minutes must be integral")
    data["horizon_minutes"] = data.horizon_minutes.astype(int)
    data["lead_minutes"] = pd.to_numeric(data.lead_minutes, errors="raise")
    finite_lead = data.lead_minutes.notna()
    if not (np.isfinite(data.loc[finite_lead, "lead_minutes"]) &
            (data.loc[finite_lead, "lead_minutes"] >= 0)).all():
        raise ValueError("Observed lead_minutes must be finite and nonnegative")
    if (data.event & (~data.full_horizon_eligible | data.censored)).any():
        raise ValueError("Observed events must be eligible and cannot be censored")
    if (data.censored & data.full_horizon_eligible).any():
        raise ValueError("Censored early exits cannot be full-horizon eligible")
    assignments = data.groupby("day", sort=False).split.nunique()
    if (assignments > 1).any():
        raise ValueError("A session cannot belong to multiple splits")
    day_split = {day: ("EARLIER30" if index < 30 else "LATER16")
                 for index, day in enumerate(calendar)}
    day_split.update(data.groupby("day", sort=False).split.first().to_dict())
    return data, calendar, day_split


def _count_vectors(data: pd.DataFrame) -> np.ndarray:
    warning = data.warning.to_numpy(dtype=bool)
    event = data.event.to_numpy(dtype=bool)
    baseline = data.baseline_warning.to_numpy(dtype=bool)
    return np.column_stack((warning & event, warning & ~event, ~warning & event,
                            ~warning & ~event, baseline & event, baseline & ~event,
                            ~baseline & event, ~baseline & ~event)).astype(np.int64)


def _interval(values: np.ndarray) -> tuple[float, float, int]:
    valid = values[np.isfinite(values)]
    if not len(valid):
        return math.nan, math.nan, 0
    low, high = np.quantile(valid, [0.025, 0.975])
    return float(low), float(high), int(len(valid))


def _bootstrap(data, calendar, weights):
    # Identical weights are reused across policies, sides and both endpoints.
    empty = {
        "precision_bootstrap_low": math.nan, "precision_bootstrap_high": math.nan,
        "precision_minus_prevalence_bootstrap_low": math.nan,
        "precision_minus_prevalence_bootstrap_high": math.nan,
        "precision_minus_baseline_precision_bootstrap_low": math.nan,
        "precision_minus_baseline_precision_bootstrap_high": math.nan,
        "bootstrap_reps": int(weights.shape[0]), "bootstrap_valid_reps": 0,
        "bootstrap_prevalence_difference_valid_reps": 0, "bootstrap_baseline_difference_valid_reps": 0,
    }
    if data.empty or not len(calendar) or not weights.shape[0]:
        return empty
    counts = pd.DataFrame(_count_vectors(data), index=data.day)
    per_day = counts.groupby(level=0).sum().reindex(calendar, fill_value=0).to_numpy()
    draws = weights @ per_day
    tp, fp, fn, tn, baseline_tp, baseline_fp, _, _ = draws.T
    with np.errstate(divide="ignore", invalid="ignore"):
        precision = tp / (tp + fp)
        prevalence = (tp + fn) / (tp + fp + fn + tn)
        baseline_precision = baseline_tp / (baseline_tp + baseline_fp)
    for prefix, values, count_name in (
        ("precision", precision, "bootstrap_valid_reps"),
        ("precision_minus_prevalence", precision - prevalence, "bootstrap_prevalence_difference_valid_reps"),
        ("precision_minus_baseline_precision", precision - baseline_precision,
         "bootstrap_baseline_difference_valid_reps"),
    ):
        low, high, valid = _interval(values)
        empty[f"{prefix}_bootstrap_low"] = low
        empty[f"{prefix}_bootstrap_high"] = high
        empty[count_name] = valid
    return empty


def _row(source, data, calendar, weights):
    tp, fp, fn, tn, btp, bfp, bfn, btn = _count_vectors(data).sum(axis=0).tolist()
    n = len(data)
    precision = _ratio(tp, tp + fp)
    prevalence = _ratio(tp + fn, n)
    baseline_precision = _ratio(btp, btp + bfp)
    wilson_low, wilson_high = _wilson(tp, tp + fp)
    leads = data.loc[data.warning & data.event, "lead_minutes"].dropna()
    unknown = source.loc[~source.outcome_available]
    unknown_warnings = int(unknown.warning.sum())
    result = {
        "source_observations": len(source),
        "source_censored_observations": int(source.censored.sum()),
        "observations": n, "excluded_observations": len(source) - n,
        "censored_observations": int(data.censored.sum()),
        "full_horizon_observations": int(data.full_horizon_eligible.sum()),
        "unknown_observations": len(unknown), "unknown_warnings": unknown_warnings,
        "trades": len(data[["day", "trade_id"]].drop_duplicates()),
        "days": int(data.day.nunique()), "calendar_days": len(calendar),
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "baseline_tp": btp, "baseline_fp": bfp, "baseline_fn": bfn, "baseline_tn": btn,
        "precision": precision, "recall": _ratio(tp, tp + fn),
        "fpr": _ratio(fp, fp + tn), "specificity": _ratio(tn, fp + tn),
        "accuracy": _ratio(tp + tn, n), "prevalence": prevalence,
        "warning_rate": _ratio(tp + fp, n),
        "lift": _ratio(precision, prevalence), "baseline_precision": baseline_precision,
        "median_lead_minutes_tp": float(leads.median()) if len(leads) else math.nan,
        "precision_minus_prevalence": precision - prevalence,
        "precision_minus_baseline_precision": precision - baseline_precision,
        "precision_wilson_low": wilson_low, "precision_wilson_high": wilson_high,
        "precision_identification_lower": _ratio(tp, tp + fp + unknown_warnings),
        "precision_identification_upper": _ratio(tp + unknown_warnings, tp + fp + unknown_warnings),
    }
    result.update(_bootstrap(data, calendar, weights))
    return result


def _summarize(frame, days, bootstrap_reps, seed, phases):
    if isinstance(bootstrap_reps, bool) or int(bootstrap_reps) != bootstrap_reps or bootstrap_reps < 0:
        raise ValueError("bootstrap_reps must be a nonnegative integer")
    data, calendar, day_split = _validate(frame, days)
    if data.empty:
        return pd.DataFrame(columns=COLUMNS)
    rng = np.random.default_rng(seed)
    parameters = sorted(set(zip(data.horizon_minutes, data.threshold_pct)))
    rows = []
    for split in SPLITS:
        split_days = [day for day in calendar if split == "ALL" or day_split[day] == split]
        weights = (rng.multinomial(len(split_days), np.full(len(split_days), 1 / len(split_days)),
                                   size=int(bootstrap_reps)) if split_days else
                   np.zeros((int(bootstrap_reps), 0), dtype=np.int64))
        split_data = data if split == "ALL" else data.loc[data.split.eq(split)]
        for side in SIDES:
            side_data = split_data if side == "BOTH" else split_data.loc[split_data.side.eq(side)]
            for horizon, threshold in parameters:
                subset = side_data.loc[side_data.horizon_minutes.eq(horizon) &
                                       side_data.threshold_pct.eq(threshold)]
                for phase in phases:
                    source = subset if phase == "ALL" else subset.loc[subset.phase.eq(phase)]
                    for analysis in ("PRIMARY", "FULL_HORIZON"):
                        selected = source.loc[source.outcome_available]
                        if analysis == "FULL_HORIZON":
                            selected = selected.loc[selected.full_horizon_eligible]
                        values = _row(source, selected, split_days, weights)
                        values.update(analysis=analysis, split=split, side=side,
                                      horizon_minutes=int(horizon), threshold_pct=float(threshold), phase=phase)
                        rows.append(values)
    return pd.DataFrame(rows, columns=COLUMNS)


def summarize(frame: pd.DataFrame, days: list[str], bootstrap_reps=2000,
              seed=20261008) -> pd.DataFrame:
    """Summarize fixed-grid outcomes; never mix dense-alert rows into this API."""
    return _summarize(frame, days, bootstrap_reps, seed, ("ALL",))


def summarize_phases(frame: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    """Optional descriptive phase strata with Wilson, but no bootstrap, CIs."""
    return _summarize(frame, days, 0, 20261008, PHASES)
