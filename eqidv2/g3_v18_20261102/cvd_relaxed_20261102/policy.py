"""Pure training-only policy choice for the frozen relaxed CVD experiment.

This module reads no files and loads no trading data. The caller must supply
metrics and actual selection-change flags from completed training sessions
only, then persist the returned choice before loading the evaluation month.
Previously viewed history remains retrospective research, not an unseen test.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

RULES = (
    "relaxed_and", "either_nonnegative", "either_soft", "opposition_veto",
    "strong_opposition_veto", "pressure_tolerance",
)
REFERENCE_VARIANT = "available_control"
COSTS = (5, 9, 15)
TOLERANCE = 1e-6
VERSION = "G3-RELAXED-CVD-TRAINING-POLICY-20261102"
METRICS = (
    "trades", "net_pnl_rs", "losses", "win_rate_pct", "loss_trade_pnl_abs_rs",
    "losing_days", "loss_day_pnl_abs_rs", "daily_close_drawdown_rs",
)
COUNT_FIELDS = ("trades", "losses", "losing_days")
CRITERIA = (
    "complete_cost_rows", "valid_metrics", "minimum_trade_sample_5bps",
    "net_pnl_noninferior_5bps", "losing_trade_count_noninferior_5bps",
    "win_rate_noninferior_5bps", "losing_trade_rupees_noninferior_5bps",
    "losing_day_count_noninferior_5bps", "losing_day_rupees_noninferior_5bps",
    "drawdown_noninferior_5bps", "net_pnl_noninferior_15bps",
    "drawdown_noninferior_15bps", "actual_selection_change", "strict_improvement",
)
RANKING = (
    ("net_pnl_rs", "higher"), ("losses", "lower"), ("win_rate_pct", "higher"),
    ("loss_trade_pnl_abs_rs", "lower"), ("daily_close_drawdown_rs", "lower"),
    ("trades", "higher"),
)


def _metadata():
    return dict(
        version=VERSION, reference_variant=REFERENCE_VARIANT,
        rule_order=list(RULES), costs_bps=list(COSTS), arithmetic_tolerance=TOLERANCE,
        minimum_training_trades=40, minimum_reference_trade_fraction=.80,
        evaluation_scope="CALLER_SUPPLIED_COMPLETED_TRAINING_METRICS_ONLY",
        io_performed=False, future_data_loaded=False, independent_holdout=False,
        ranking_5bps=[dict(metric=name, preference=direction) for name, direction in RANKING],
        final_tie_break="FIXED_RULE_ORDER",
        strict_improvement="Any gated profit, loss-count, losing-rupee, win-rate or drawdown metric improves; sample size alone does not qualify",
        costs_used_for_constraints=[5, 15], nine_bps_role="REQUIRED_COMPLETE_TRAINING_SENSITIVITY_ROW_NOT_USED_TO_RANK",
        fallback="available_control; constraints are never relaxed after viewing results",
        chronology_requirement="Caller locks this result before loading the next month; this function cannot verify which sessions the caller aggregated",
    )


def _candidate(variant, reason="MISSING_TRAINING_METRICS"):
    return dict(variant=variant, qualified=False, reason=reason,
        criteria={name: False for name in CRITERIA}, strict_improvement_metrics=[],
        metrics_by_cost={}, selection_rank=None)


def _response(candidates, reference_metrics, reason, ranked=()):
    chosen = ranked[0] if ranked else REFERENCE_VARIANT
    ranks = {name: i+1 for i, name in enumerate(ranked)}
    for candidate in candidates:
        candidate["selection_rank"] = ranks.get(candidate["variant"])
        candidate["failed_criteria"] = [name for name, passed in candidate["criteria"].items() if not passed]
    return dict(selected_variant=chosen, reference_variant=REFERENCE_VARIANT,
        selection_reason=reason, fallback_used=not bool(ranked),
        ranked_qualified_variants=list(ranked), candidates=candidates,
        reference_metrics_by_cost=reference_metrics, metadata=_metadata())


def _extract(rows):
    """Return ordinary finite Python numbers, or a concise invalid-data reason."""
    values = {}
    for cost in COSTS:
        row = rows.loc[rows.cost_bps.eq(cost)]
        if len(row) != 1:
            return None, "MISSING_COST_ROW"
        record = row.iloc[0]
        own = {}
        for name in METRICS:
            if name not in record:
                return None, "MISSING_METRIC_COLUMN"
            value = record[name]
            if name == "win_rate_pct" and own.get("trades") == 0 and (value is None or pd.isna(value)):
                own[name] = None
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None, "NONNUMERIC_METRIC"
            # A zero-trade cohort has an undefined win rate. It cannot satisfy
            # the sample requirement; retain None for honest JSON reporting.
            if name == "win_rate_pct" and own.get("trades") == 0 and not math.isfinite(number):
                own[name] = None
                continue
            if not math.isfinite(number):
                return None, "NONFINITE_METRIC"
            if name in COUNT_FIELDS:
                if number < 0 or abs(number-round(number)) > TOLERANCE:
                    return None, "INVALID_COUNT_METRIC"
                own[name] = int(round(number))
            else:
                if name != "net_pnl_rs" and number < 0:
                    return None, "NEGATIVE_MAGNITUDE_METRIC"
                if name == "win_rate_pct" and number > 100:
                    return None, "INVALID_WIN_RATE"
                own[name] = number
        if own["losses"] > own["trades"]:
            return None, "LOSS_COUNT_EXCEEDS_TRADES"
        values[str(cost)] = own
    return values, None


def _rank(qualified):
    """Lexicographic ranking with only the declared arithmetic tie tolerance."""
    remaining = list(qualified)
    ranked = []
    while remaining:
        tier = remaining.copy()
        for metric, direction in RANKING:
            numbers = [item["metrics_by_cost"]["5"][metric] for item in tier]
            best = max(numbers) if direction == "higher" else min(numbers)
            tier = [item for item in tier if abs(item["metrics_by_cost"]["5"][metric] - best) <= TOLERANCE]
            if len(tier) == 1:
                break
        winner = min(tier, key=lambda item: RULES.index(item["variant"]))
        ranked.append(winner["variant"])
        remaining = [item for item in remaining if item["variant"] != winner["variant"]]
    return ranked


def choose_policy(metrics, changed):
    """Choose one rule using ONLY the supplied completed training-period rows.

    ``metrics`` is a DataFrame or record list with one row per variant and
    ``cost_bps`` (5, 9, 15), containing all ``METRICS``. Extra variants such as
    ``base_control`` are ignored. Duplicate variant/cost keys raise, preventing
    accidental mixing of training and evaluation summaries. ``changed`` maps
    rule identifiers to actual selected-stock-change booleans for TRAINING.

    Empty training data or a zero-trade reference explicitly falls back to the
    availability control. A malformed nonempty reference raises. Missing or
    invalid candidate rows cannot qualify. All constraints, tie rules and the
    six-member family are fixed; this function does not fit thresholds.
    """
    if not isinstance(changed, Mapping):
        raise ValueError("changed must map rule names to training-selection-change booleans")
    frame = metrics.copy(deep=True) if isinstance(metrics, pd.DataFrame) else pd.DataFrame(metrics)
    candidates = [_candidate(name) for name in RULES]
    if frame.empty:
        return _response(candidates, {}, "NO_COMPLETED_TRAINING_METRICS")
    if {"variant", "cost_bps"} - set(frame):
        raise ValueError("Training metrics require variant and cost_bps columns")
    frame = frame.loc[frame.variant.isin((REFERENCE_VARIANT, *RULES))].copy()
    if frame.empty:
        raise ValueError("Nonempty training input has no available_control reference")
    frame["cost_bps"] = pd.to_numeric(frame.cost_bps, errors="coerce")
    if not frame.cost_bps.isin(COSTS).all():
        raise ValueError("Training rows must use the fixed 5/9/15 bps costs")
    if frame.duplicated(["variant", "cost_bps"]).any():
        raise ValueError("Duplicate training variant/cost rows; do not mix calibration and evaluation periods")
    reference_rows = frame.loc[frame.variant.eq(REFERENCE_VARIANT)]
    reference, error = _extract(reference_rows)
    if error:
        raise ValueError(f"Invalid available_control training reference: {error}")
    if reference["5"]["trades"] == 0:
        return _response(candidates, reference, "NO_EXECUTED_REFERENCE_TRAINING_TRADES")
    if reference["15"]["trades"] == 0 or reference["5"]["win_rate_pct"] is None:
        raise ValueError("Inconsistent available_control execution sample across costs")

    ref5, ref15 = reference["5"], reference["15"]
    minimum_trades = max(40., .80*ref5["trades"])
    for candidate in candidates:
        variant = candidate["variant"]
        own_rows = frame.loc[frame.variant.eq(variant)]
        checks = candidate["criteria"]
        checks["complete_cost_rows"] = len(own_rows) == len(COSTS) and set(own_rows.cost_bps) == set(COSTS)
        own, error = _extract(own_rows)
        if error:
            candidate["reason"] = error
            continue
        checks["valid_metrics"] = True
        candidate["metrics_by_cost"] = own
        own5, own15 = own["5"], own["15"]
        checks["minimum_trade_sample_5bps"] = own5["trades"] + TOLERANCE >= minimum_trades
        checks["net_pnl_noninferior_5bps"] = own5["net_pnl_rs"] >= ref5["net_pnl_rs"] - TOLERANCE
        checks["losing_trade_count_noninferior_5bps"] = own5["losses"] <= ref5["losses"]
        checks["win_rate_noninferior_5bps"] = own5["win_rate_pct"] is not None and own5["win_rate_pct"] >= ref5["win_rate_pct"] - TOLERANCE
        checks["losing_trade_rupees_noninferior_5bps"] = own5["loss_trade_pnl_abs_rs"] <= ref5["loss_trade_pnl_abs_rs"] + TOLERANCE
        checks["losing_day_count_noninferior_5bps"] = own5["losing_days"] <= ref5["losing_days"]
        checks["losing_day_rupees_noninferior_5bps"] = own5["loss_day_pnl_abs_rs"] <= ref5["loss_day_pnl_abs_rs"] + TOLERANCE
        checks["drawdown_noninferior_5bps"] = own5["daily_close_drawdown_rs"] <= ref5["daily_close_drawdown_rs"] + TOLERANCE
        checks["net_pnl_noninferior_15bps"] = own15["net_pnl_rs"] >= ref15["net_pnl_rs"] - TOLERANCE
        checks["drawdown_noninferior_15bps"] = own15["daily_close_drawdown_rs"] <= ref15["daily_close_drawdown_rs"] + TOLERANCE
        value = changed.get(variant, False)
        if not isinstance(value, (bool, np.bool_)):
            raise ValueError(f"Training selection-change flag for {variant} must be boolean")
        checks["actual_selection_change"] = bool(value)
        comparisons = (
            (5, "net_pnl_rs", "higher"), (5, "losses", "lower"),
            (5, "win_rate_pct", "higher"), (5, "loss_trade_pnl_abs_rs", "lower"),
            (5, "losing_days", "lower"), (5, "loss_day_pnl_abs_rs", "lower"),
            (5, "daily_close_drawdown_rs", "lower"),
            (15, "net_pnl_rs", "higher"), (15, "daily_close_drawdown_rs", "lower"),
        )
        improvements = []
        for cost, name, direction in comparisons:
            a, b = own[str(cost)][name], reference[str(cost)][name]
            if a is not None and b is not None and ((a > b+TOLERANCE) if direction == "higher" else (a < b-TOLERANCE)):
                improvements.append(f"{name}@{cost}bps")
        candidate["strict_improvement_metrics"] = improvements
        checks["strict_improvement"] = bool(improvements)
        candidate["qualified"] = bool(all(checks.values()))
        candidate["reason"] = "QUALIFIED" if candidate["qualified"] else "TRAINING_CONSTRAINTS_FAILED"
        candidate["failed_criteria"] = [name for name, passed in checks.items() if not passed]
    ranked = _rank([item for item in candidates if item["qualified"]])
    return _response(candidates, reference, "BEST_QUALIFIED_TRAINING_RULE" if ranked else "NO_RULE_PASSED_FROZEN_TRAINING_CONSTRAINTS", ranked)
