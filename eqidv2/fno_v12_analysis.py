"""Pure, deterministic analysis helpers for FNO V12 staged research.

The module deliberately contains no strategy, selection, execution, file-system,
or reporting code.  It consumes aggregate frames produced by a runner and keeps
the frozen V11 variant as a comparison baseline only.

The development gate is intentionally conservative.  A challenger is
``INSUFFICIENT`` when it has fewer than 30 affected economic decisions or any
required comparison input is missing.  Complete challengers are ``PASS`` only
when every predeclared criterion succeeds; otherwise they are ``FAIL``.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


ANALYSIS_SCHEMA_VERSION = "fno_v12_pure_analysis_v1"
REFERENCE_SCENARIO = "REFERENCE_15_0"
STRESS_SCENARIO = "STRESS_20_2"
HARSH_SCENARIO = "STRESS_25_5"
REQUIRED_SCENARIOS = (
    REFERENCE_SCENARIO,
    STRESS_SCENARIO,
    HARSH_SCENARIO,
)
FULL_PERIOD = "FULL_USABLE"
FORWARD_PERIOD = "FORWARD_EXTENSION"
BOOTSTRAP_REPLICATES = 2_000
MIN_AFFECTED_DECISIONS = 30

_EPSILON = 1e-12
_NET_COLUMNS = ("net_return_points", "net_return_pct", "net_points")
_PNL_COLUMNS = ("net_pnl_rs", "pnl_rs", "net_pnl")
_MDD_COLUMNS = (
    "max_daily_drawdown_points",
    "max_drawdown_points",
    "mdd_points",
)
_AFFECTED_COUNT_COLUMNS = (
    "affected_decisions",
    "affected_decision_count",
    "changed_decisions",
    "materially_changed_decisions",
)
_AFFECTED_FLAG_COLUMNS = ("affected", "is_affected", "decision_changed")


@dataclass(frozen=True)
class AnalysisBundle:
    """All pure-analysis products returned by :func:`analyze_v12_results`."""

    pairwise_daywise_deltas: pd.DataFrame
    development_gates: pd.DataFrame
    bootstrap_and_concentration: pd.DataFrame
    best_observed: dict[str, Any] | None
    best_gate_passing: dict[str, Any] | None

    def summary(self) -> dict[str, Any]:
        """Return a small JSON-safe summary without embedding large frames."""

        return json_safe(
            {
                "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
                "best_observed": self.best_observed,
                "best_gate_passing": self.best_gate_passing,
                "pairwise_daywise_rows": len(self.pairwise_daywise_deltas),
                "development_gate_rows": len(self.development_gates),
                "bootstrap_rows": len(self.bootstrap_and_concentration),
            }
        )


def _require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _first_column(frame: pd.DataFrame, candidates: Sequence[str], label: str) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    raise ValueError(f"{label} requires one of these columns: {list(candidates)}")


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _finite(value: Any, *, allow_infinite: bool = False) -> bool:
    number = _number(value)
    if math.isnan(number):
        return False
    return allow_infinite or math.isfinite(number)


def _safe_ratio(numerator: Any, denominator: Any) -> float:
    top = _number(numerator)
    bottom = _number(denominator)
    if math.isnan(top) or math.isnan(bottom):
        return math.nan
    if abs(bottom) <= _EPSILON:
        if abs(top) <= _EPSILON:
            return 1.0
        return math.inf if top > 0 else -math.inf
    return top / bottom


def _period_filter(frame: pd.DataFrame, period: str) -> pd.DataFrame:
    """Select a named period while tolerating the runner's daily labels."""

    if "period_label" in frame.columns:
        labels = frame["period_label"].astype(str)
        if labels.eq(period).any():
            return frame.loc[labels.eq(period)].copy()
    if "period" in frame.columns:
        labels = frame["period"].astype(str)
        accepted = {period}
        if period == FULL_PERIOD:
            accepted.add("FULL")
        if labels.isin(accepted).any():
            return frame.loc[labels.isin(accepted)].copy()
        return frame.iloc[0:0].copy()
    # Side/setup aggregates are allowed to be full-history-only and omit period.
    return frame.copy() if period == FULL_PERIOD else frame.iloc[0:0].copy()


def _normalise_daywise(daywise: pd.DataFrame) -> tuple[pd.DataFrame, str, str | None]:
    _require_columns(
        daywise,
        ("variant_id", "scenario", "session_date"),
        "daywise metrics",
    )
    net_column = _first_column(daywise, _NET_COLUMNS, "daywise metrics")
    pnl_column = next((c for c in _PNL_COLUMNS if c in daywise.columns), None)
    frame = _period_filter(daywise, FULL_PERIOD)
    frame = frame.copy()
    parsed = pd.to_datetime(frame["session_date"], errors="coerce")
    if parsed.isna().any():
        examples = frame.loc[parsed.isna(), "session_date"].astype(str).head(3).tolist()
        raise ValueError(f"daywise metrics contain invalid session dates: {examples}")
    frame["session_date"] = parsed.dt.date
    frame["variant_id"] = frame["variant_id"].astype(str)
    frame["scenario"] = frame["scenario"].astype(str)
    frame[net_column] = pd.to_numeric(frame[net_column], errors="coerce")
    if pnl_column is not None:
        frame[pnl_column] = pd.to_numeric(frame[pnl_column], errors="coerce")
    duplicate = frame.duplicated(
        ["variant_id", "scenario", "session_date"], keep=False
    )
    if duplicate.any():
        sample = frame.loc[
            duplicate, ["variant_id", "scenario", "session_date"]
        ].head(5)
        raise ValueError(
            "daywise metrics must have one full-history row per "
            f"variant/scenario/session; duplicates include {sample.to_dict('records')}"
        )
    return frame, net_column, pnl_column


def pairwise_daywise_deltas(
    daywise: pd.DataFrame,
    frozen_v11_variant_id: str,
    *,
    scenarios: Sequence[str] = REQUIRED_SCENARIOS,
    variant_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Align every challenger session with frozen V11 and calculate deltas.

    An outer pairing is used intentionally.  Missing sessions remain visible via
    ``paired_session=False`` instead of silently being filled with zero.
    """

    frame, net_column, pnl_column = _normalise_daywise(daywise)
    baseline_id = str(frozen_v11_variant_id)
    required = tuple(str(value) for value in scenarios)
    baseline = frame.loc[
        frame["variant_id"].eq(baseline_id) & frame["scenario"].isin(required)
    ].copy()
    observed_baseline_scenarios = set(baseline["scenario"])
    missing_baseline = sorted(set(required) - observed_baseline_scenarios)
    if missing_baseline:
        raise ValueError(
            f"frozen V11 daywise baseline is missing scenarios: {missing_baseline}"
        )

    if variant_ids is None:
        variants = sorted(
            set(frame.loc[frame["scenario"].isin(required), "variant_id"])
            - {baseline_id}
        )
    else:
        variants = sorted({str(value) for value in variant_ids} - {baseline_id})

    result_parts: list[pd.DataFrame] = []
    for variant_id in variants:
        challenger = frame.loc[
            frame["variant_id"].eq(variant_id)
            & frame["scenario"].isin(required)
        ].copy()
        for scenario in required:
            base_scenario = baseline.loc[baseline["scenario"].eq(scenario)].copy()
            observed_scenario = challenger.loc[
                challenger["scenario"].eq(scenario)
            ].copy()
            base_columns = ["session_date", net_column]
            observed_columns = ["session_date", net_column]
            if pnl_column is not None:
                base_columns.append(pnl_column)
                observed_columns.append(pnl_column)
            base_scenario = base_scenario.loc[:, base_columns].rename(
                columns={
                    net_column: "baseline_net_return_points",
                    **(
                        {pnl_column: "baseline_net_pnl_rs"}
                        if pnl_column is not None
                        else {}
                    ),
                }
            )
            observed_scenario = observed_scenario.loc[:, observed_columns].rename(
                columns={
                    net_column: "variant_net_return_points",
                    **(
                        {pnl_column: "variant_net_pnl_rs"}
                        if pnl_column is not None
                        else {}
                    ),
                }
            )
            paired = base_scenario.merge(
                observed_scenario,
                on="session_date",
                how="outer",
                validate="one_to_one",
                indicator=True,
            )
            paired.insert(0, "scenario", scenario)
            paired.insert(0, "baseline_variant_id", baseline_id)
            paired.insert(0, "variant_id", variant_id)
            paired["baseline_session_present"] = paired["_merge"].ne("right_only")
            paired["variant_session_present"] = paired["_merge"].ne("left_only")
            paired["paired_session"] = paired["_merge"].eq("both")
            paired["delta_net_return_points"] = (
                paired["variant_net_return_points"]
                - paired["baseline_net_return_points"]
            )
            if pnl_column is not None:
                paired["delta_net_pnl_rs"] = (
                    paired["variant_net_pnl_rs"] - paired["baseline_net_pnl_rs"]
                )
            else:
                paired["baseline_net_pnl_rs"] = np.nan
                paired["variant_net_pnl_rs"] = np.nan
                paired["delta_net_pnl_rs"] = np.nan
            paired = paired.drop(columns="_merge")
            result_parts.append(paired)

    columns = [
        "variant_id",
        "baseline_variant_id",
        "scenario",
        "session_date",
        "baseline_session_present",
        "variant_session_present",
        "paired_session",
        "variant_net_return_points",
        "baseline_net_return_points",
        "delta_net_return_points",
        "variant_net_pnl_rs",
        "baseline_net_pnl_rs",
        "delta_net_pnl_rs",
    ]
    if not result_parts:
        return pd.DataFrame(columns=columns)
    result = pd.concat(result_parts, ignore_index=True)
    return result.loc[:, columns].sort_values(
        ["variant_id", "scenario", "session_date"], kind="stable"
    ).reset_index(drop=True)


def _normalise_affected_decisions(
    affected_decisions: Mapping[str, Any] | pd.DataFrame,
) -> dict[str, int | None]:
    def extract_count(raw: Any) -> int | None:
        if isinstance(raw, Mapping):
            for key in _AFFECTED_COUNT_COLUMNS:
                if key in raw:
                    return extract_count(raw[key])
            # Runner result payloads commonly nest the count by cost scenario.
            if REFERENCE_SCENARIO in raw:
                return extract_count(raw[REFERENCE_SCENARIO])
            return None
        number = _number(raw)
        return (
            int(number)
            if math.isfinite(number) and number >= 0 and number.is_integer()
            else None
        )

    if isinstance(affected_decisions, Mapping):
        result: dict[str, int | None] = {}
        for variant_id, raw in affected_decisions.items():
            result[str(variant_id)] = extract_count(raw)
        return result
    if not isinstance(affected_decisions, pd.DataFrame):
        raise TypeError("affected_decisions must be a mapping or pandas DataFrame")
    _require_columns(affected_decisions, ("variant_id",), "affected decisions")
    frame = affected_decisions.copy()
    frame["variant_id"] = frame["variant_id"].astype(str)
    count_column = next(
        (column for column in _AFFECTED_COUNT_COLUMNS if column in frame.columns),
        None,
    )
    if count_column is not None:
        values = pd.to_numeric(frame[count_column], errors="coerce")
        result = {}
        for variant_id, group in frame.assign(_count=values).groupby(
            "variant_id", sort=False
        ):
            valid = group["_count"].dropna()
            if valid.empty or (valid < 0).any():
                result[str(variant_id)] = None
            elif len(valid) == 1:
                value = float(valid.iloc[0])
                result[str(variant_id)] = int(value) if value.is_integer() else None
            else:
                value = float(valid.sum())
                result[str(variant_id)] = int(value) if value.is_integer() else None
        return result
    flag_column = next(
        (column for column in _AFFECTED_FLAG_COLUMNS if column in frame.columns), None
    )
    if flag_column is None:
        raise ValueError(
            "affected decisions requires a count column or one of these flags: "
            f"{list(_AFFECTED_FLAG_COLUMNS)}"
        )
    flags = frame[flag_column]
    if not pd.api.types.is_bool_dtype(flags.dtype):
        normalised = flags.astype(str).str.strip().str.lower()
        known = normalised.isin({"true", "false", "1", "0", "yes", "no"})
        if not known.all():
            raise ValueError("affected decision flags contain non-boolean values")
        flags = normalised.isin({"true", "1", "yes"})
    return (
        frame.assign(_affected=flags.astype(bool))
        .groupby("variant_id", sort=False)["_affected"]
        .sum()
        .astype(int)
        .to_dict()
    )


def _candidate_variants(
    full_metrics: pd.DataFrame,
    baseline_id: str,
    candidate_variant_ids: Sequence[str] | None,
) -> list[str]:
    if candidate_variant_ids is not None:
        return sorted({str(value) for value in candidate_variant_ids} - {baseline_id})
    frame = full_metrics.copy()
    for exclusion_column in ("comparison_only", "external_comparator"):
        if exclusion_column in frame.columns:
            frame = frame.loc[~frame[exclusion_column].fillna(False).astype(bool)]
    if "gate_eligible" in frame.columns:
        frame = frame.loc[frame["gate_eligible"].fillna(False).astype(bool)]
    if "post_hoc" in frame.columns:
        frame = frame.loc[~frame["post_hoc"].fillna(False).astype(bool)]
    return sorted(set(frame["variant_id"].astype(str)) - {baseline_id})


def _unique_metric_row(
    frame: pd.DataFrame, variant_id: str, scenario: str
) -> pd.Series | None:
    rows = frame.loc[
        frame["variant_id"].astype(str).eq(variant_id)
        & frame["scenario"].astype(str).eq(scenario)
    ]
    if len(rows) != 1:
        return None
    return rows.iloc[0]


def _side_totals(
    side_setup: pd.DataFrame,
    variant_id: str,
) -> tuple[dict[str, float], list[str]]:
    required = {"variant_id", "scenario"}
    missing = sorted(required - set(side_setup.columns))
    if missing:
        return {}, [f"side_setup_missing_columns:{','.join(missing)}"]
    if "side" not in side_setup.columns and "group_id" not in side_setup.columns:
        return {}, ["side_setup_missing_columns:side_or_group_id"]
    try:
        net_column = _first_column(side_setup, _NET_COLUMNS, "side/setup metrics")
    except ValueError as exc:
        return {}, [str(exc)]
    frame = _period_filter(side_setup, FULL_PERIOD)
    frame = frame.loc[
        frame["variant_id"].astype(str).eq(variant_id)
        & frame["scenario"].astype(str).eq(HARSH_SCENARIO)
    ].copy()
    if "side" not in frame.columns:
        frame["side"] = (
            frame["group_id"].astype(str).str.upper().str.removeprefix("SIDE_")
        )
    else:
        frame["side"] = frame["side"].astype(str).str.upper()
    frame = frame.loc[frame["side"].isin({"LONG", "SHORT"})]
    # Prefer explicit all-setup rows when a producer supplies both detail and totals.
    if "setup_id" in frame.columns:
        labels = frame["setup_id"].astype(str).str.upper()
        aggregate = labels.isin({"ALL", "ALL_SETUPS", "*"})
        if aggregate.any():
            frame = frame.loc[aggregate]
    frame[net_column] = pd.to_numeric(frame[net_column], errors="coerce")
    totals: dict[str, float] = {}
    reasons: list[str] = []
    for side in ("LONG", "SHORT"):
        values = frame.loc[frame["side"].eq(side), net_column]
        if values.empty or values.isna().any():
            reasons.append(f"missing_harsh_side_net:{side}")
        else:
            totals[side] = float(values.sum())
    return totals, reasons


def _daily_subset_delta(
    pairwise: pd.DataFrame,
    variant_id: str,
    scenario: str,
    *,
    session_dates: set[date] | None = None,
    exclude_month: int | None = None,
) -> tuple[float, bool, int]:
    rows = pairwise.loc[
        pairwise["variant_id"].astype(str).eq(variant_id)
        & pairwise["scenario"].astype(str).eq(scenario)
    ].copy()
    if session_dates is not None:
        rows = rows.loc[rows["session_date"].isin(session_dates)]
    if exclude_month is not None:
        rows = rows.loc[
            rows["session_date"].map(lambda value: value.month != exclude_month)
        ]
    if rows.empty:
        return math.nan, False, 0
    complete = bool(rows["paired_session"].all())
    paired = rows.loc[rows["paired_session"]]
    if paired.empty or paired["delta_net_return_points"].isna().any():
        return math.nan, False, len(paired)
    return float(paired["delta_net_return_points"].sum()), complete, len(paired)


def _aggregate_period_delta(
    metrics: pd.DataFrame,
    baseline_id: str,
    variant_id: str,
    scenario: str,
    period: str,
    net_column: str,
) -> tuple[float, bool]:
    period_rows = _period_filter(metrics, period)
    baseline = _unique_metric_row(period_rows, baseline_id, scenario)
    challenger = _unique_metric_row(period_rows, variant_id, scenario)
    if baseline is None or challenger is None:
        return math.nan, False
    if not _finite(baseline[net_column]) or not _finite(challenger[net_column]):
        return math.nan, False
    return float(challenger[net_column]) - float(baseline[net_column]), True


def isolated_development_gates(
    metrics: pd.DataFrame,
    daywise: pd.DataFrame,
    side_setup: pd.DataFrame,
    frozen_v11_variant_id: str,
    affected_decisions: Mapping[str, Any] | pd.DataFrame,
    *,
    candidate_variant_ids: Sequence[str] | None = None,
    forward_session_dates: Iterable[date | datetime | str] | None = None,
    excluded_month: int = 7,
) -> pd.DataFrame:
    """Evaluate the predeclared isolated V12 development gates.

    Ranking is deliberately independent of win rate: first maximise the minimum
    full-history net ratio across the three cost scenarios, then harsh-scenario
    net points, then minimise reference drawdown.  Forward and ex-July deltas
    are reported for every scenario; their predeclared gate checks use the
    reference-cost paired series.
    """

    _require_columns(metrics, ("variant_id", "scenario", "period"), "metrics")
    net_column = _first_column(metrics, _NET_COLUMNS, "metrics")
    mdd_column = _first_column(metrics, _MDD_COLUMNS, "metrics")
    _require_columns(metrics, ("profit_factor", "fills"), "metrics")
    full = _period_filter(metrics, FULL_PERIOD).copy()
    full["variant_id"] = full["variant_id"].astype(str)
    full["scenario"] = full["scenario"].astype(str)
    baseline_id = str(frozen_v11_variant_id)
    baseline_scope = set(
        full.loc[full["variant_id"].eq(baseline_id), "scenario"].astype(str)
    )
    if baseline_scope != set(REQUIRED_SCENARIOS):
        raise ValueError(
            "frozen V11 full-history baseline scenario scope must be exactly "
            f"{list(REQUIRED_SCENARIOS)}; observed {sorted(baseline_scope)}"
        )
    baseline_rows: dict[str, pd.Series] = {}
    for scenario in REQUIRED_SCENARIOS:
        row = _unique_metric_row(full, baseline_id, scenario)
        if row is None:
            raise ValueError(
                "frozen V11 full-history baseline must have exactly one row for "
                f"{scenario}"
            )
        required_values = (
            row[net_column],
            row["profit_factor"],
            row["fills"],
            row[mdd_column],
        )
        if not all(_finite(value, allow_infinite=index == 1) for index, value in enumerate(required_values)):
            raise ValueError(f"frozen V11 baseline has invalid numeric data for {scenario}")
        baseline_rows[scenario] = row

    variants = _candidate_variants(full, baseline_id, candidate_variant_ids)
    pairwise = pairwise_daywise_deltas(
        daywise,
        baseline_id,
        scenarios=REQUIRED_SCENARIOS,
        variant_ids=variants,
    )
    affected = _normalise_affected_decisions(affected_decisions)
    parsed_forward_dates: set[date] | None = None
    if forward_session_dates is not None:
        parsed = pd.to_datetime(list(forward_session_dates), errors="coerce")
        if pd.isna(parsed).any():
            raise ValueError("forward_session_dates contains an invalid date")
        parsed_forward_dates = {value.date() for value in parsed}
        if not parsed_forward_dates:
            raise ValueError("forward_session_dates cannot be empty")

    rows: list[dict[str, Any]] = []
    for variant_id in variants:
        variant_full = {
            scenario: _unique_metric_row(full, variant_id, scenario)
            for scenario in REQUIRED_SCENARIOS
        }
        first_row = next((row for row in variant_full.values() if row is not None), None)
        output: dict[str, Any] = {
            "variant_id": variant_id,
            "comparison_baseline_variant_id": baseline_id,
            "stage_id": (
                str(first_row.get("stage_id", "")) if first_row is not None else ""
            ),
            "family": (
                str(first_row.get("family", "")) if first_row is not None else ""
            ),
            "post_hoc": (
                bool(first_row.get("post_hoc", False))
                if first_row is not None
                else False
            ),
            "affected_decisions": affected.get(variant_id),
            "minimum_affected_decisions": MIN_AFFECTED_DECISIONS,
        }
        insufficient: list[str] = []
        checks: dict[str, bool | None] = {}
        observed_scope = set(
            full.loc[full["variant_id"].eq(variant_id), "scenario"].astype(str)
        )
        if observed_scope != set(REQUIRED_SCENARIOS):
            insufficient.append(
                "full_history_scenario_scope_not_exact:"
                + ",".join(sorted(observed_scope))
            )
        affected_count = affected.get(variant_id)
        if affected_count is None:
            insufficient.append("affected_decisions_missing_or_invalid")
            checks["affected_decisions_at_least_30"] = None
        elif affected_count < MIN_AFFECTED_DECISIONS:
            insufficient.append(
                f"affected_decisions_below_{MIN_AFFECTED_DECISIONS}:{affected_count}"
            )
            checks["affected_decisions_at_least_30"] = False
        else:
            checks["affected_decisions_at_least_30"] = True

        ratios: list[float] = []
        valid_full = True
        for scenario in REQUIRED_SCENARIOS:
            observed = variant_full[scenario]
            baseline = baseline_rows[scenario]
            key = scenario.lower()
            if observed is None:
                insufficient.append(f"full_history_metric_missing_or_duplicate:{scenario}")
                valid_full = False
                output[f"net_ratio_{key}"] = math.nan
                output[f"pf_delta_{key}"] = math.nan
                checks[f"net_at_least_baseline_{key}"] = None
                checks[f"pf_delta_at_least_minus_005_{key}"] = None
                continue
            numeric_values = (
                observed[net_column],
                observed["profit_factor"],
                observed["fills"],
                observed[mdd_column],
            )
            if not all(
                _finite(value, allow_infinite=index == 1)
                for index, value in enumerate(numeric_values)
            ):
                insufficient.append(f"full_history_numeric_invalid:{scenario}")
                valid_full = False
                output[f"net_ratio_{key}"] = math.nan
                output[f"pf_delta_{key}"] = math.nan
                checks[f"net_at_least_baseline_{key}"] = None
                checks[f"pf_delta_at_least_minus_005_{key}"] = None
                continue
            observed_net = float(observed[net_column])
            baseline_net = float(baseline[net_column])
            ratio = _safe_ratio(observed_net, baseline_net)
            ratios.append(ratio)
            pf_delta = float(observed["profit_factor"]) - float(
                baseline["profit_factor"]
            )
            output[f"net_ratio_{key}"] = ratio
            output[f"pf_delta_{key}"] = pf_delta
            checks[f"net_at_least_baseline_{key}"] = (
                observed_net >= baseline_net - _EPSILON
            )
            checks[f"pf_delta_at_least_minus_005_{key}"] = (
                pf_delta >= -0.05 - _EPSILON
            )

        harsh = variant_full[HARSH_SCENARIO]
        if harsh is not None and _finite(harsh[net_column]):
            harsh_net = float(harsh[net_column])
            harsh_baseline_net = float(baseline_rows[HARSH_SCENARIO][net_column])
            output["harsh_net_return_points"] = harsh_net
            output["harsh_net_required_105pct_baseline"] = 1.05 * harsh_baseline_net
            checks["harsh_net_at_least_105pct_baseline"] = (
                harsh_net >= 1.05 * harsh_baseline_net - _EPSILON
            )
        else:
            output["harsh_net_return_points"] = math.nan
            output["harsh_net_required_105pct_baseline"] = math.nan
            checks["harsh_net_at_least_105pct_baseline"] = None

        reference = variant_full[REFERENCE_SCENARIO]
        if reference is not None and _finite(reference[mdd_column]) and _finite(reference["fills"]):
            reference_mdd = abs(float(reference[mdd_column]))
            baseline_reference_mdd = abs(
                float(baseline_rows[REFERENCE_SCENARIO][mdd_column])
            )
            fill_retention = _safe_ratio(
                reference["fills"], baseline_rows[REFERENCE_SCENARIO]["fills"]
            )
            output["reference_mdd_points"] = reference_mdd
            output["reference_mdd_ratio_vs_baseline"] = _safe_ratio(
                reference_mdd, baseline_reference_mdd
            )
            output["reference_fill_retention"] = fill_retention
            checks["reference_mdd_within_105pct_baseline"] = (
                reference_mdd <= 1.05 * baseline_reference_mdd + _EPSILON
            )
            checks["reference_fill_retention_at_least_070"] = (
                fill_retention >= 0.70 - _EPSILON
            )
        else:
            output["reference_mdd_points"] = math.nan
            output["reference_mdd_ratio_vs_baseline"] = math.nan
            output["reference_fill_retention"] = math.nan
            checks["reference_mdd_within_105pct_baseline"] = None
            checks["reference_fill_retention_at_least_070"] = None

        for scenario in REQUIRED_SCENARIOS:
            key = scenario.lower()
            ex_july_delta, ex_july_complete, ex_july_sessions = _daily_subset_delta(
                pairwise,
                variant_id,
                scenario,
                exclude_month=excluded_month,
            )
            output[f"ex_july_paired_delta_{key}"] = ex_july_delta
            output[f"ex_july_paired_sessions_{key}"] = ex_july_sessions
            output[f"ex_july_pairing_complete_{key}"] = ex_july_complete
            if scenario == REFERENCE_SCENARIO and (
                not ex_july_complete or math.isnan(ex_july_delta)
            ):
                insufficient.append(f"ex_july_pairing_incomplete:{scenario}")
                checks[f"ex_july_delta_nonnegative_{key}"] = None
            elif scenario == REFERENCE_SCENARIO:
                checks[f"ex_july_delta_nonnegative_{key}"] = (
                    ex_july_delta >= -_EPSILON
                )

            if parsed_forward_dates is not None:
                forward_delta, forward_complete, forward_sessions = _daily_subset_delta(
                    pairwise,
                    variant_id,
                    scenario,
                    session_dates=parsed_forward_dates,
                )
                source = "PAIRWISE_DAYWISE_EXPLICIT_DATES"
            else:
                forward_delta, forward_complete = _aggregate_period_delta(
                    metrics,
                    baseline_id,
                    variant_id,
                    scenario,
                    FORWARD_PERIOD,
                    net_column,
                )
                forward_sessions = 0
                source = "AGGREGATE_MATCHED_FORWARD_PERIOD"
            output[f"forward_extension_paired_delta_{key}"] = forward_delta
            output[f"forward_extension_paired_sessions_{key}"] = forward_sessions
            output[f"forward_extension_pairing_complete_{key}"] = forward_complete
            output[f"forward_extension_delta_source_{key}"] = source
            if scenario == REFERENCE_SCENARIO and (
                not forward_complete or math.isnan(forward_delta)
            ):
                insufficient.append(f"forward_extension_pairing_incomplete:{scenario}")
                checks[f"forward_extension_delta_nonnegative_{key}"] = None
            elif scenario == REFERENCE_SCENARIO:
                checks[f"forward_extension_delta_nonnegative_{key}"] = (
                    forward_delta >= -_EPSILON
                )

        side_totals, side_reasons = _side_totals(side_setup, variant_id)
        insufficient.extend(side_reasons)
        for side in ("LONG", "SHORT"):
            value = side_totals.get(side, math.nan)
            output[f"harsh_{side.lower()}_net_return_points"] = value
            checks[f"harsh_{side.lower()}_net_positive"] = (
                value > _EPSILON if not math.isnan(value) else None
            )

        output["minimum_scenario_net_ratio"] = (
            float(min(ratios)) if valid_full and len(ratios) == 3 else math.nan
        )
        # Stable compatibility names make this pure module easy to adopt in the
        # staged runner while retaining the more explicit scenario-keyed fields.
        output["worst_scenario_net_ratio"] = output[
            "minimum_scenario_net_ratio"
        ]
        output["reference_net_ratio"] = output.get(
            f"net_ratio_{REFERENCE_SCENARIO.lower()}", math.nan
        )
        output["stress20_net_ratio"] = output.get(
            f"net_ratio_{STRESS_SCENARIO.lower()}", math.nan
        )
        output["harsh_net_ratio"] = output.get(
            f"net_ratio_{HARSH_SCENARIO.lower()}", math.nan
        )
        output["reference_mdd_ratio"] = output["reference_mdd_ratio_vs_baseline"]
        output["forward_extension_delta_points"] = output.get(
            f"forward_extension_paired_delta_{REFERENCE_SCENARIO.lower()}",
            math.nan,
        )
        output["ex_july_delta_points"] = output.get(
            f"ex_july_paired_delta_{REFERENCE_SCENARIO.lower()}", math.nan
        )
        output["both_sides_harsh_positive"] = bool(
            checks.get("harsh_long_net_positive") is True
            and checks.get("harsh_short_net_positive") is True
        )
        for name, result in checks.items():
            output[f"check_{name}"] = result
        failed = sorted(name for name, result in checks.items() if result is False)
        unknown = sorted(name for name, result in checks.items() if result is None)
        if unknown:
            insufficient.extend(f"check_not_evaluable:{name}" for name in unknown)
        insufficient = sorted(set(insufficient))
        if insufficient:
            gate_status = "INSUFFICIENT"
        elif failed:
            gate_status = "FAIL"
        else:
            gate_status = "PASS"
        output.update(
            {
                "gate_status": gate_status,
                "gate_passed": gate_status == "PASS",
                "development_gate_passed": gate_status == "PASS",
                "gate_input_sufficient": not insufficient,
                "failed_check_count": len(failed),
                "failed_checks": ";".join(failed),
                "insufficient_reason_count": len(insufficient),
                "insufficient_reasons": ";".join(insufficient),
                "gate_reasons": ";".join(
                    [*(f"FAILED:{value}" for value in failed), *insufficient]
                ),
            }
        )
        output["gate_reason"] = (
            "PASS" if gate_status == "PASS" else output["gate_reasons"]
        )
        rows.append(output)

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["observed_rank"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    rankable = result.loc[
        result["minimum_scenario_net_ratio"].notna()
        & result["harsh_net_return_points"].notna()
        & result["reference_mdd_points"].notna()
    ].sort_values(
        [
            "minimum_scenario_net_ratio",
            "harsh_net_return_points",
            "reference_mdd_points",
            "variant_id",
        ],
        ascending=[False, False, True, True],
        kind="stable",
    )
    for rank, index in enumerate(rankable.index, start=1):
        result.at[index, "observed_rank"] = rank
    result["gate_passing_rank"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    passing = rankable.loc[rankable["gate_status"].eq("PASS")]
    for rank, index in enumerate(passing.index, start=1):
        result.at[index, "gate_passing_rank"] = rank
    return result.sort_values(
        ["observed_rank", "variant_id"], na_position="last", kind="stable"
    ).reset_index(drop=True)


def paired_bootstrap_and_concentration(
    pairwise_deltas: pd.DataFrame,
) -> pd.DataFrame:
    """Return deterministic 2,000-replicate paired-session diagnostics."""

    _require_columns(
        pairwise_deltas,
        (
            "variant_id",
            "baseline_variant_id",
            "scenario",
            "session_date",
            "paired_session",
            "delta_net_return_points",
        ),
        "pairwise daywise deltas",
    )
    rows: list[dict[str, Any]] = []
    grouped = pairwise_deltas.groupby(
        ["variant_id", "baseline_variant_id", "scenario"], sort=True, dropna=False
    )
    for (variant_id, baseline_id, scenario), group in grouped:
        ordered = group.sort_values("session_date", kind="stable").copy()
        paired = ordered.loc[ordered["paired_session"].astype(bool)].copy()
        delta = pd.to_numeric(
            paired["delta_net_return_points"], errors="coerce"
        ).to_numpy(dtype=float)
        valid = np.isfinite(delta)
        delta = delta[valid]
        paired_dates = pd.DatetimeIndex(
            pd.to_datetime(paired.loc[valid, "session_date"], errors="coerce")
        )
        complete = bool(
            len(ordered) > 0
            and ordered["paired_session"].astype(bool).all()
            and len(delta) == len(ordered)
        )
        base: dict[str, Any] = {
            "variant_id": str(variant_id),
            "baseline_variant_id": str(baseline_id),
            "scenario": str(scenario),
            "bootstrap_replicates": BOOTSTRAP_REPLICATES,
            "bootstrap_unit": "PAIRED_SESSION",
            "paired_sessions": len(delta),
            "pairing_complete": complete,
        }
        if len(delta) == 0:
            base.update(
                {
                    "observed_delta_net_points": math.nan,
                    "bootstrap_delta_sum_p025": math.nan,
                    "bootstrap_delta_sum_median": math.nan,
                    "bootstrap_delta_sum_p975": math.nan,
                    "bootstrap_mean_daily_delta_p025": math.nan,
                    "bootstrap_mean_daily_delta_median": math.nan,
                    "bootstrap_mean_daily_delta_p975": math.nan,
                    "bootstrap_probability_delta_positive": math.nan,
                    "positive_delta_sessions": 0,
                    "negative_delta_sessions": 0,
                    "zero_delta_sessions": 0,
                    "best_5_positive_sessions_share_pct": math.nan,
                    "best_10_positive_sessions_share_pct": math.nan,
                    "top_5_absolute_sessions_share_pct": math.nan,
                    "absolute_delta_hhi": math.nan,
                    "best_month_share_pct": math.nan,
                    "positive_months": 0,
                    "negative_months": 0,
                    "max_cumulative_delta_drawdown_points": math.nan,
                }
            )
            rows.append(base)
            continue
        seed_material = (
            f"{ANALYSIS_SCHEMA_VERSION}|PAIRED_BOOTSTRAP|{variant_id}|"
            f"{baseline_id}|{scenario}|{BOOTSTRAP_REPLICATES}"
        ).encode("utf-8")
        seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
        rng = np.random.default_rng(seed)
        sampled_indices = rng.integers(
            0, len(delta), size=(BOOTSTRAP_REPLICATES, len(delta))
        )
        sampled_sums = delta[sampled_indices].sum(axis=1)
        sampled_means = delta[sampled_indices].mean(axis=1)
        total = float(delta.sum())
        positive = np.sort(delta[delta > 0])[::-1]
        absolute = np.abs(delta)
        absolute_total = float(absolute.sum())
        cumulative = np.cumsum(delta)
        peaks = np.maximum.accumulate(np.insert(cumulative, 0, 0.0))[1:]
        max_drawdown = float(np.max(peaks - cumulative))
        monthly_series = pd.Series(delta, index=paired_dates).groupby(
            paired_dates.strftime("%Y-%m")
        ).sum()

        def positive_share(count: int) -> float:
            return (
                float(positive[:count].sum() / total * 100.0)
                if total > _EPSILON and len(positive)
                else math.nan
            )

        base.update(
            {
                "observed_delta_net_points": total,
                "bootstrap_delta_sum_p025": float(np.quantile(sampled_sums, 0.025)),
                "bootstrap_delta_sum_median": float(np.quantile(sampled_sums, 0.50)),
                "bootstrap_delta_sum_p975": float(np.quantile(sampled_sums, 0.975)),
                "bootstrap_mean_daily_delta_p025": float(
                    np.quantile(sampled_means, 0.025)
                ),
                "bootstrap_mean_daily_delta_median": float(
                    np.quantile(sampled_means, 0.50)
                ),
                "bootstrap_mean_daily_delta_p975": float(
                    np.quantile(sampled_means, 0.975)
                ),
                "bootstrap_probability_delta_positive": float(
                    np.mean(sampled_sums > 0)
                ),
                "positive_delta_sessions": int(np.sum(delta > _EPSILON)),
                "negative_delta_sessions": int(np.sum(delta < -_EPSILON)),
                "zero_delta_sessions": int(np.sum(np.abs(delta) <= _EPSILON)),
                "best_5_positive_sessions_share_pct": positive_share(5),
                "best_10_positive_sessions_share_pct": positive_share(10),
                "top_5_absolute_sessions_share_pct": (
                    float(np.sort(absolute)[::-1][:5].sum() / absolute_total * 100.0)
                    if absolute_total > _EPSILON
                    else math.nan
                ),
                "absolute_delta_hhi": (
                    float(np.square(absolute / absolute_total).sum())
                    if absolute_total > _EPSILON
                    else math.nan
                ),
                "best_month_share_pct": (
                    float(monthly_series.max() / total * 100.0)
                    if total > _EPSILON and not monthly_series.empty
                    else math.nan
                ),
                "positive_months": int((monthly_series > _EPSILON).sum()),
                "negative_months": int((monthly_series < -_EPSILON).sum()),
                "max_cumulative_delta_drawdown_points": max_drawdown,
            }
        )
        if "delta_net_pnl_rs" in paired.columns:
            pnl = pd.to_numeric(paired.loc[valid, "delta_net_pnl_rs"], errors="coerce")
            base["observed_delta_net_pnl_rs"] = (
                float(pnl.sum()) if pnl.notna().all() else math.nan
            )
        rows.append(base)
    return pd.DataFrame(rows).sort_values(
        ["variant_id", "scenario"], kind="stable"
    ).reset_index(drop=True) if rows else pd.DataFrame()


def select_best_variants(
    development_gates: pd.DataFrame,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return JSON-safe best-observed and best-gate-passing records."""

    if development_gates.empty:
        return None, None
    _require_columns(
        development_gates,
        (
            "variant_id",
            "gate_status",
            "minimum_scenario_net_ratio",
            "harsh_net_return_points",
            "reference_mdd_points",
        ),
        "development gates",
    )
    rankable = development_gates.loc[
        development_gates["minimum_scenario_net_ratio"].notna()
        & development_gates["harsh_net_return_points"].notna()
        & development_gates["reference_mdd_points"].notna()
    ].sort_values(
        [
            "minimum_scenario_net_ratio",
            "harsh_net_return_points",
            "reference_mdd_points",
            "variant_id",
        ],
        ascending=[False, False, True, True],
        kind="stable",
    )
    if rankable.empty:
        return None, None
    output_columns = [
        "variant_id",
        "stage_id",
        "family",
        "gate_status",
        "minimum_scenario_net_ratio",
        "harsh_net_return_points",
        "reference_mdd_points",
        "failed_checks",
        "insufficient_reasons",
    ]

    def record(row: pd.Series) -> dict[str, Any]:
        return json_safe(
            {column: row[column] for column in output_columns if column in row.index}
        )

    best_observed = record(rankable.iloc[0])
    passing = rankable.loc[rankable["gate_status"].eq("PASS")]
    best_passing = record(passing.iloc[0]) if not passing.empty else None
    return best_observed, best_passing


def analyze_v12_results(
    metrics: pd.DataFrame,
    daywise: pd.DataFrame,
    side_setup: pd.DataFrame,
    frozen_v11_variant_id: str,
    affected_decisions: Mapping[str, Any] | pd.DataFrame,
    *,
    candidate_variant_ids: Sequence[str] | None = None,
    forward_session_dates: Iterable[date | datetime | str] | None = None,
    excluded_month: int = 7,
) -> AnalysisBundle:
    """Compute every V12 pure-analysis artifact in one call."""

    gates = isolated_development_gates(
        metrics,
        daywise,
        side_setup,
        frozen_v11_variant_id,
        affected_decisions,
        candidate_variant_ids=candidate_variant_ids,
        forward_session_dates=forward_session_dates,
        excluded_month=excluded_month,
    )
    variant_ids = gates["variant_id"].astype(str).tolist() if not gates.empty else []
    deltas = pairwise_daywise_deltas(
        daywise,
        frozen_v11_variant_id,
        scenarios=REQUIRED_SCENARIOS,
        variant_ids=variant_ids,
    )
    bootstrap = paired_bootstrap_and_concentration(deltas)
    best_observed, best_passing = select_best_variants(gates)
    return AnalysisBundle(
        pairwise_daywise_deltas=deltas,
        development_gates=gates,
        bootstrap_and_concentration=bootstrap,
        best_observed=best_observed,
        best_gate_passing=best_passing,
    )


def json_safe(value: Any) -> Any:
    """Recursively convert numpy/pandas values and non-finite floats for JSON."""

    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


# Short aliases for runners that prefer noun-based API names.
compute_pairwise_daywise_deltas = pairwise_daywise_deltas
compute_development_gates = isolated_development_gates
compute_bootstrap_and_concentration = paired_bootstrap_and_concentration
development_gates = isolated_development_gates
bootstrap_and_concentration = paired_bootstrap_and_concentration
analyze = analyze_v12_results


__all__ = [
    "ANALYSIS_SCHEMA_VERSION",
    "AnalysisBundle",
    "BOOTSTRAP_REPLICATES",
    "FORWARD_PERIOD",
    "FULL_PERIOD",
    "HARSH_SCENARIO",
    "MIN_AFFECTED_DECISIONS",
    "REFERENCE_SCENARIO",
    "REQUIRED_SCENARIOS",
    "STRESS_SCENARIO",
    "analyze",
    "analyze_v12_results",
    "compute_bootstrap_and_concentration",
    "compute_development_gates",
    "compute_pairwise_daywise_deltas",
    "development_gates",
    "bootstrap_and_concentration",
    "isolated_development_gates",
    "json_safe",
    "paired_bootstrap_and_concentration",
    "pairwise_daywise_deltas",
    "select_best_variants",
]
