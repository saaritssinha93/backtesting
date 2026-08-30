"""Package the frozen Stage 2 RV1_100 isolated-research result.

This utility does not run a backtest.  It validates the frozen experiment
plan, the final Stage 1 V10B control, and one completed RV1_100 challenger,
then publishes a non-overwritable, provenance-first comparison package.

The decision emitted by this tool records completion only.  Preliminary
screening gates are reported individually and never confer promotion
authority; the frozen stress gate remains explicitly deferred.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

# Direct ``python tools/package_fno_v10_stage2.py`` execution places only the
# tools directory on ``sys.path``.  Import the workspace's frozen modules,
# never an ambient installation.
WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

import fno_v10_experiment_compare as comparison
import fno_v10_experiment_config as experiment_config


STAGE_SCHEMA_VERSION = "fno_v10_stage2_result_package_v1"
TEST_EVIDENCE_SCHEMA_VERSION = "fno_v10_stage2_test_evidence_v1"
VARIANT_ARCHIVE_SCHEMA_VERSION = "fno_v10_stage2_variant_archive_v1"
PLAN_SCHEMA_VERSION = "fno_v10_experiment_stage_sequence_v1"
STAGE = "STAGE_02"
CONTROL_VARIANT = "V10B"
EXPECTED_VARIANT = "RV1_100"
DECISION = "COMPLETED_STAGE2_ISOLATED_RESEARCH_NOT_PROMOTION"
PLAN_FILENAME = "fno_v10_experiment_stage_sequence.json"
EXPECTED_STAGE_PLAN_SHA256 = (
    "e8381c478b6843de26f74d61ef569495b739fe10f17743e447d0f771e7fd88c2"
)
COMPARISON_DIR_NAME = "stage1_v10b_vs_stage2_rv1_100_comparison"
DAYWISE_FILENAME = "daywise_comparison.csv"
SIDE_SETUP_FILENAME = "side_and_setup_comparison.csv"


def _require_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is not a file: {resolved}")
    return resolved


def _path_key(path: Path) -> str:
    return os.path.normcase(str(path.resolve()))


def _require_same_path(left: Path, right: Path, label: str) -> None:
    if _path_key(left) != _path_key(right):
        raise ValueError(f"{label} path differs: {left.resolve()} != {right.resolve()}")


def _parameters(payload: Mapping[str, Any]) -> dict[str, Any]:
    return dict(payload.get("parameters", {}))


def _parameters_variant(payload: Mapping[str, Any]) -> str:
    return str(_parameters(payload).get("variant", "")).upper()


def _same_number(left: Any, right: Any) -> bool:
    try:
        left_number = float(left)
        right_number = float(right)
    except (TypeError, ValueError):
        return False
    if math.isnan(left_number) or math.isnan(right_number):
        return math.isnan(left_number) and math.isnan(right_number)
    return math.isclose(left_number, right_number, rel_tol=0.0, abs_tol=1e-12)


def _require_number(actual: Any, expected: Any, label: str) -> None:
    if not _same_number(actual, expected):
        raise ValueError(f"{label} differs: observed={actual!r}, expected={expected!r}")


def _json_safe(value: Any) -> Any:
    """Return strict-JSON-safe values without disguising undefined metrics."""

    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NOT_EVALUABLE"
        return "POSITIVE_INFINITY" if value > 0 else "NEGATIVE_INFINITY"
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    comparison.atomic_write_text(
        path,
        json.dumps(
            _json_safe(payload),
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def _atomic_copy(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copyfile(source, temporary)
    os.replace(temporary, destination)


def _atomic_write_frame(path: Path, frame: pd.DataFrame) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    frame.to_csv(temporary, index=False, lineterminator="\n")
    os.replace(temporary, path)


def _load_frozen_plan(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    plan_path = _require_file(path, "Frozen Stage 2 plan")
    observed_sha = comparison.sha256_file(plan_path)
    if observed_sha != EXPECTED_STAGE_PLAN_SHA256:
        raise ValueError(
            "Frozen Stage 2 plan SHA256 differs: "
            f"observed={observed_sha}, expected={EXPECTED_STAGE_PLAN_SHA256}"
        )
    plan = comparison.load_json(plan_path)
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise ValueError("Unsupported frozen Stage 2 plan schema")
    if plan.get("research_only") is not True:
        raise ValueError("Frozen Stage 2 plan must remain research-only")
    if plan.get("promotion_eligible") is not False:
        raise ValueError("Frozen Stage 2 plan cannot be promotion-eligible")
    if plan.get("one_at_a_time") is not True:
        raise ValueError("Frozen Stage 2 plan must require one-at-a-time execution")
    if plan.get("retuning_or_reordering_after_results") is not False:
        raise ValueError("Frozen Stage 2 plan must forbid result-dependent retuning")

    matches = [
        dict(item)
        for item in list(plan.get("sequence", []))
        if str(dict(item).get("stage", "")).upper() == STAGE
    ]
    if len(matches) != 1:
        raise ValueError("Frozen plan must contain exactly one STAGE_02 entry")
    sequence_entry = matches[0]
    if str(sequence_entry.get("variant", "")).upper() != EXPECTED_VARIANT:
        raise ValueError("Frozen STAGE_02 variant must be RV1_100")
    expected_config_sha = experiment_config.variant_config_sha256(EXPECTED_VARIANT)
    if sequence_entry.get("variant_config_sha256") != expected_config_sha:
        raise ValueError("Frozen STAGE_02 variant config SHA256 is invalid")

    integrity = dict(plan.get("stage_02_integrity_gates", {}))
    required_integrity = {
        "expected_variant": EXPECTED_VARIANT,
        "expected_base_candidates": 890,
        "expected_selection_overlay": "BASE_V10B_SELECTION",
        "all_selection_decisions_must_pass": True,
        "same_cache_snapshot_window_split_costs_and_portfolio_as_control": True,
        "full_state_machine_replay_required": True,
        "valid_provenance_required": True,
    }
    for field, expected in required_integrity.items():
        if integrity.get(field) != expected:
            raise ValueError(f"Frozen Stage 2 integrity gate changed: {field}")

    screen = dict(plan.get("stage_02_preliminary_performance_screen", {}))
    required_screen_fields = {
        "minimum_fills",
        "minimum_active_sessions",
        "minimum_profit_factor",
        "minimum_profit_factor_excluding_best_net_day",
        "maximum_best_positive_day_share_pct",
        "minimum_long_profit_factor",
        "minimum_short_profit_factor",
        "maximum_drawdown_as_pct_of_control",
        "always_report_accuracy_and_fill_retention",
        "stress_20bps_plus_2bps",
        "screen_is_promotion_authority",
    }
    missing = sorted(required_screen_fields - set(screen))
    if missing:
        raise ValueError(f"Frozen Stage 2 screen is incomplete: {missing}")
    if screen.get("always_report_accuracy_and_fill_retention") is not True:
        raise ValueError("Accuracy and fill retention reporting must remain enabled")
    if screen.get("stress_20bps_plus_2bps") != "DEFERRED_NOT_EVALUATED":
        raise ValueError("Frozen Stage 2 stress gate must remain deferred")
    if screen.get("screen_is_promotion_authority") is not False:
        raise ValueError("The preliminary screen cannot gain promotion authority")
    return plan, sequence_entry


def _validate_stage1_package_binding(
    plan: Mapping[str, Any],
    control_path: Path,
) -> tuple[Path, dict[str, Any]]:
    control = dict(plan.get("control", {}))
    if str(control.get("stage", "")).upper() != "STAGE_01":
        raise ValueError("Frozen control stage must be STAGE_01")
    if str(control.get("variant", "")).upper() != CONTROL_VARIANT:
        raise ValueError("Frozen control variant must be V10B")
    planned_control = _require_file(
        Path(str(control.get("provenance_path", ""))),
        "Plan-bound Stage 1 control provenance",
    )
    _require_same_path(control_path, planned_control, "Stage 1 control provenance")
    observed_control_sha = comparison.sha256_file(control_path)
    if observed_control_sha != control.get("provenance_sha256"):
        raise ValueError("Stage 1 control provenance SHA256 differs from frozen plan")

    manifest_path = _require_file(
        Path(str(control.get("stage_package_manifest_path", ""))),
        "Plan-bound Stage 1 package manifest",
    )
    manifest_sha = comparison.sha256_file(manifest_path)
    if manifest_sha != control.get("stage_package_manifest_sha256"):
        raise ValueError("Stage 1 package manifest SHA256 differs from frozen plan")
    manifest = comparison.load_json(manifest_path)
    if manifest.get("schema_version") != "fno_v10_stage1_result_package_v1":
        raise ValueError("Plan-bound Stage 1 package schema is unsupported")
    if manifest.get("stage") != "STAGE_01":
        raise ValueError("Plan-bound package is not STAGE_01")
    if manifest.get("decision") != "PASS_RUNNER_PARITY_RESEARCH_ONLY":
        raise ValueError("Plan-bound Stage 1 package did not pass runner parity")
    _require_same_path(
        Path(str(manifest.get("stage1_control_provenance", ""))),
        control_path,
        "Stage 1 package control provenance",
    )
    if manifest.get("stage1_control_provenance_sha256") != observed_control_sha:
        raise ValueError("Stage 1 package does not bind the frozen control SHA256")
    if manifest.get("stage1_v10b_config_sha256") != (
        experiment_config.variant_config_sha256(CONTROL_VARIANT)
    ):
        raise ValueError("Stage 1 package V10B configuration binding is invalid")
    return manifest_path, manifest


def _require_experiment_identity(
    payload: Mapping[str, Any],
    *,
    variant: str,
    variant_config_sha256: str,
    label: str,
) -> None:
    if payload.get("v10_experiment_run_schema_version") != (
        experiment_config.RUN_SCHEMA_VERSION
    ):
        raise ValueError(f"{label} is not a supported experiment-run provenance")
    if str(payload.get("v10_experiment_variant", "")).upper() != variant:
        raise ValueError(f"{label} experiment variant must be {variant}")
    if _parameters_variant(payload) != variant:
        raise ValueError(f"{label} run parameters must select {variant}")
    if payload.get("v10_experiment_variant_config_sha256") != (
        variant_config_sha256
    ):
        raise ValueError(f"{label} variant configuration SHA256 is invalid")
    if payload.get("research_only") is not True:
        raise ValueError(f"{label} must remain research-only")
    if payload.get("promotion_eligible") is not False:
        raise ValueError(f"{label} cannot be promotion-eligible")
    results = dict(payload.get("results", {}))
    if results.get("promotion_eligible") is not False:
        raise ValueError(f"{label} results cannot be promotion-eligible")
    objective = str(payload.get("objective", ""))
    if "FULL_CHRONOLOGICAL_V10_STATE_MACHINE_REPLAY" not in objective:
        raise ValueError(f"{label} does not attest a full state-machine replay")
    spec = experiment_config.get_spec(variant)
    policy = dict(_parameters(payload).get("entry_policy", {}))
    if int(policy.get("entry_expiry_minute", -1)) != spec.entry_expiry_minute:
        raise ValueError(f"{label} entry expiry differs from frozen config")
    observed_rv1 = policy.get("confirmation_volume_ratio_min")
    expected_rv1 = spec.confirmation_volume_ratio_min
    if expected_rv1 is None:
        if observed_rv1 is not None:
            raise ValueError(f"{label} unexpectedly enables the RV1 gate")
    elif not _same_number(observed_rv1, expected_rv1):
        raise ValueError(f"{label} RV1 threshold differs from frozen config")


def _validate_frozen_inputs(
    *,
    plan: Mapping[str, Any],
    control: Mapping[str, Any],
    challenger: Mapping[str, Any],
    stage1_manifest: Mapping[str, Any],
) -> tuple[Path, dict[str, Any]]:
    frozen = dict(plan.get("frozen_inputs", {}))
    experiment_config.validate_registry()
    registry_sha = experiment_config.registry_sha256()
    if registry_sha != frozen.get("registry_sha256"):
        raise ValueError("Current registry SHA256 differs from frozen Stage 2 plan")
    if stage1_manifest.get("experiment_registry_sha256") != registry_sha:
        raise ValueError("Stage 1 package registry binding differs from frozen plan")

    source_bindings = (
        (
            "runner_sha256",
            WORKSPACE_ROOT / "fno_v10_experiment_backtest.py",
            "experiment_runner_source_sha256",
        ),
        (
            "config_source_sha256",
            WORKSPACE_ROOT / "fno_v10_experiment_config.py",
            "experiment_config_source_sha256",
        ),
    )
    for plan_field, source_path, provenance_field in source_bindings:
        expected = str(frozen.get(plan_field, ""))
        if comparison.sha256_file(_require_file(source_path, plan_field)) != expected:
            raise ValueError(f"Current {plan_field} differs from frozen Stage 2 plan")
        for label, payload in (("control", control), ("challenger", challenger)):
            if payload.get(provenance_field) != expected:
                raise ValueError(f"{label} {provenance_field} differs from frozen plan")

    scalar_bindings = (
        ("v10_experiment_registry_sha256", "registry_sha256"),
        ("cache_input_fingerprint", "cache_input_fingerprint"),
    )
    for provenance_field, plan_field in scalar_bindings:
        expected = frozen.get(plan_field)
        for label, payload in (("control", control), ("challenger", challenger)):
            if payload.get(provenance_field) != expected:
                raise ValueError(f"{label} {provenance_field} differs from frozen plan")

    invariant_scalar_fields = (
        "v10_unified_contract_sha256",
        "neutral_engine_source_sha256",
        "launcher_source_sha256",
        "strategy_source_sha256",
    )
    for field in invariant_scalar_fields:
        if not control.get(field) or control.get(field) != challenger.get(field):
            raise ValueError(f"Control and challenger {field} differ")

    snapshot_path = _require_file(
        Path(str(frozen.get("source_snapshot_manifest", ""))),
        "Frozen source snapshot manifest",
    )
    if comparison.sha256_file(snapshot_path) != frozen.get(
        "source_snapshot_manifest_sha256"
    ):
        raise ValueError("Frozen source snapshot manifest SHA256 differs")
    if stage1_manifest.get("source_snapshot_manifest_sha256") != frozen.get(
        "source_snapshot_manifest_sha256"
    ):
        raise ValueError("Stage 1 package source snapshot SHA256 differs")
    _require_same_path(
        Path(str(stage1_manifest.get("source_snapshot_manifest", ""))),
        snapshot_path,
        "Stage 1 package source snapshot",
    )
    for label, payload in (("control", control), ("challenger", challenger)):
        source = dict(payload.get("source_snapshot", {}))
        observed_path = _require_file(
            Path(str(source.get("manifest_path", ""))),
            f"{label} source snapshot manifest",
        )
        _require_same_path(observed_path, snapshot_path, f"{label} source snapshot")
        if source.get("snapshot_fingerprint") != frozen.get(
            "source_snapshot_fingerprint"
        ):
            raise ValueError(f"{label} source snapshot fingerprint differs")

    expected_window = {
        field: frozen.get(field)
        for field in ("from_day", "through_day", "split_day")
    }
    for label, payload in (("control", control), ("challenger", challenger)):
        if dict(payload.get("backtest_window", {})) != expected_window:
            raise ValueError(f"{label} backtest window differs from frozen plan")
        parameters = _parameters(payload)
        observed_window = {field: parameters.get(field) for field in expected_window}
        if observed_window != expected_window:
            raise ValueError(f"{label} parameter window differs from frozen plan")
        policy = dict(parameters.get("entry_policy", {}))
        _require_number(policy.get("cost_bps"), frozen.get("cost_bps"), f"{label} cost")
        _require_number(
            policy.get("slippage_bps"),
            frozen.get("slippage_bps"),
            f"{label} slippage",
        )
        if policy.get("square_off") != frozen.get("square_off"):
            raise ValueError(f"{label} square-off differs from frozen plan")
        if policy.get("eod_policy") != frozen.get("eod_policy"):
            raise ValueError(f"{label} EOD policy differs from frozen plan")
        _require_number(
            parameters.get("target_exposure_per_entry_rs"),
            frozen.get("target_exposure_per_entry_rs"),
            f"{label} target exposure",
        )

    if comparison.fixed_economics_contract(control) != (
        comparison.fixed_economics_contract(challenger)
    ):
        raise ValueError("Control and challenger fixed economics differ")
    if not _parameters(control).get("portfolio_mode"):
        raise ValueError("Control portfolio mode is missing")
    if _parameters(control).get("portfolio_mode") != _parameters(challenger).get(
        "portfolio_mode"
    ):
        raise ValueError("Control and challenger portfolio modes differ")
    if dict(control.get("universe", {})) != dict(challenger.get("universe", {})):
        raise ValueError("Control and challenger universes differ")
    return snapshot_path, expected_window


def _boolean_series(series: pd.Series, label: str) -> pd.Series:
    normalized = series.astype(str).str.strip().str.lower()
    allowed = {"true", "false", "1", "0"}
    unexpected = sorted(set(normalized) - allowed)
    if unexpected:
        raise ValueError(f"{label} contains invalid booleans: {unexpected}")
    return normalized.isin({"true", "1"})


def _validate_selection_and_audit(
    payload: Mapping[str, Any],
    *,
    variant: str,
    variant_config_sha256: str,
    expected_candidates: int,
    expected_overlay: str,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    decisions = pd.read_csv(
        comparison.output_path(payload, "selection_decisions"),
        low_memory=False,
    )
    required_decision_columns = {
        "candidate_id",
        "selection_passed",
        "selection_reason",
        "experiment_variant",
        "selection_overlay_id",
        "variant_config_sha256",
        "entry_expiry_minute",
        "confirmation_volume_ratio_min",
    }
    missing = sorted(required_decision_columns - set(decisions.columns))
    if missing:
        raise ValueError(f"{label} selection decisions miss columns: {missing}")
    if len(decisions) != expected_candidates:
        raise ValueError(
            f"{label} selection-decision rows differ: "
            f"{len(decisions)} != {expected_candidates}"
        )
    candidate_ids = decisions["candidate_id"].astype(str)
    if candidate_ids.eq("").any() or candidate_ids.duplicated().any():
        raise ValueError(f"{label} selection candidate IDs are blank or duplicated")
    passed = _boolean_series(decisions["selection_passed"], f"{label} selection")
    if not passed.all():
        raise ValueError(f"{label} contains non-passing selection decisions")
    if not decisions["selection_reason"].astype(str).eq("PASSED").all():
        raise ValueError(f"{label} selection reason must be PASSED for every row")
    exact_fields = {
        "experiment_variant": variant,
        "selection_overlay_id": expected_overlay,
        "variant_config_sha256": variant_config_sha256,
    }
    for field, expected in exact_fields.items():
        if not decisions[field].astype(str).eq(str(expected)).all():
            raise ValueError(f"{label} selection field {field} differs")
    spec = experiment_config.get_spec(variant)
    expiry = pd.to_numeric(decisions["entry_expiry_minute"], errors="coerce")
    if expiry.isna().any() or not expiry.eq(spec.entry_expiry_minute).all():
        raise ValueError(f"{label} selection entry expiry differs")
    rv1 = pd.to_numeric(
        decisions["confirmation_volume_ratio_min"], errors="coerce"
    )
    if spec.confirmation_volume_ratio_min is None:
        if rv1.notna().any():
            raise ValueError(f"{label} selection unexpectedly enables RV1")
    elif rv1.isna().any() or not rv1.eq(spec.confirmation_volume_ratio_min).all():
        raise ValueError(f"{label} selection RV1 threshold differs")

    audit = pd.read_csv(
        comparison.output_path(payload, "candidate_order_audit"),
        low_memory=False,
    )
    required_audit_columns = {
        "candidate_id",
        "session_date",
        "setup_id",
        "side",
        "status",
        "filled",
        "net_return_pct",
        "net_pnl_rs",
        "experiment_variant",
        "selection_overlay_id",
        "variant_config_sha256",
        "portfolio_mode",
    }
    missing = sorted(required_audit_columns - set(audit.columns))
    if missing:
        raise ValueError(f"{label} candidate audit misses columns: {missing}")
    if len(audit) != expected_candidates:
        raise ValueError(
            f"{label} candidate-audit rows differ: {len(audit)} != {expected_candidates}"
        )
    if int(dict(payload.get("results", {})).get("candidates", -1)) != (
        expected_candidates
    ):
        raise ValueError(f"{label} provenance candidate count differs")
    audit_ids = audit["candidate_id"].astype(str)
    if audit_ids.eq("").any() or audit_ids.duplicated().any():
        raise ValueError(f"{label} audit candidate IDs are blank or duplicated")
    if set(audit_ids) != set(candidate_ids):
        raise ValueError(f"{label} selection decisions and audit candidates differ")
    for field, expected in exact_fields.items():
        if not audit[field].astype(str).eq(str(expected)).all():
            raise ValueError(f"{label} audit field {field} differs")
    expected_portfolio_mode = str(_parameters(payload).get("portfolio_mode", ""))
    if not expected_portfolio_mode or not audit["portfolio_mode"].astype(str).eq(
        expected_portfolio_mode
    ).all():
        raise ValueError(f"{label} audit portfolio mode differs from provenance")
    return decisions, audit


def _filled_rows(audit: pd.DataFrame, label: str) -> pd.DataFrame:
    filled_mask = _boolean_series(audit["filled"], f"{label} filled")
    filled = audit.loc[filled_mask].copy()
    for field in ("net_return_pct", "net_pnl_rs"):
        values = pd.to_numeric(filled[field], errors="coerce")
        if values.isna().any():
            raise ValueError(f"{label} filled ledger has invalid {field}")
        filled[field] = values
    if "portfolio_decision" in filled.columns and not filled[
        "portfolio_decision"
    ].astype(str).eq("ACCEPTED").all():
        raise ValueError(f"{label} filled ledger contains non-accepted portfolio rows")
    return filled


def _profit_factor(values: pd.Series | Sequence[float]) -> float:
    series = pd.to_numeric(pd.Series(values, dtype="object"), errors="coerce").dropna()
    gains = float(series.loc[series.gt(0)].sum())
    losses = float(-series.loc[series.lt(0)].sum())
    if losses > 0:
        return gains / losses
    if gains > 0:
        return math.inf
    return math.nan


def _max_daily_drawdown(daily_returns: pd.Series | Sequence[float]) -> float:
    series = pd.to_numeric(
        pd.Series(daily_returns, dtype="object"), errors="coerce"
    ).fillna(0.0)
    equity = pd.concat(
        [pd.Series([0.0], dtype="float64"), series.cumsum().reset_index(drop=True)],
        ignore_index=True,
    )
    return float((equity.cummax() - equity).max())


def _metric_bundle(
    payload: Mapping[str, Any],
    audit: pd.DataFrame,
    *,
    label: str,
) -> dict[str, Any]:
    filled = _filled_rows(audit, label)
    returns = pd.to_numeric(filled["net_return_pct"], errors="raise")
    daily_filled = (
        filled.assign(session_date=filled["session_date"].astype(str))
        .groupby("session_date", sort=True)["net_return_pct"]
        .sum()
    )
    best_net_day = str(daily_filled.idxmax()) if len(daily_filled) else ""
    without_best = filled.loc[
        filled["session_date"].astype(str).ne(best_net_day), "net_return_pct"
    ]
    positive_days = daily_filled.loc[daily_filled.gt(0)]
    positive_day_total = float(positive_days.sum())
    best_positive_share = (
        100.0 * float(positive_days.max()) / positive_day_total
        if positive_day_total > 0
        else math.nan
    )

    side_profit_factors: dict[str, float] = {}
    for side in ("LONG", "SHORT"):
        side_profit_factors[side] = _profit_factor(
            filled.loc[filled["side"].astype(str).str.upper().eq(side), "net_return_pct"]
        )

    daily = pd.read_csv(comparison.output_path(payload, "daily"))
    required_daily = {"session_date", "net_return_pct"}
    missing = sorted(required_daily - set(daily.columns))
    if missing:
        raise ValueError(f"{label} daily artifact misses columns: {missing}")
    daily_returns = pd.to_numeric(daily["net_return_pct"], errors="coerce")
    if daily_returns.isna().any():
        raise ValueError(f"{label} daily artifact contains invalid net returns")
    expected_sessions = int(dict(payload.get("results", {})).get("sessions", -1))
    if len(daily) != expected_sessions or daily["session_date"].astype(str).duplicated().any():
        raise ValueError(f"{label} daily sessions do not reconcile to provenance")
    max_drawdown = _max_daily_drawdown(daily_returns)

    aggregate = comparison.aggregate_row(label, payload)
    calculated_pf = _profit_factor(returns)
    if not _same_number(calculated_pf, aggregate.get("profit_factor")):
        raise ValueError(f"{label} calculated profit factor differs from provenance")
    if int(aggregate["fills"]) != len(filled):
        raise ValueError(f"{label} filled ledger count differs from provenance")
    if not _same_number(max_drawdown, aggregate.get("max_daily_drawdown_points")):
        raise ValueError(f"{label} calculated drawdown differs from provenance")

    return {
        **aggregate,
        "active_sessions": int(filled["session_date"].astype(str).nunique()),
        "profit_factor_excluding_best_net_day": _profit_factor(without_best),
        "excluded_best_net_day": best_net_day,
        "best_positive_day_share_pct": best_positive_share,
        "long_profit_factor": side_profit_factors["LONG"],
        "short_profit_factor": side_profit_factors["SHORT"],
        "calculated_max_daily_drawdown_points": max_drawdown,
    }


def _delta_value(challenger: Any, control: Any) -> Any:
    try:
        challenger_number = float(challenger)
        control_number = float(control)
    except (TypeError, ValueError):
        return ""
    if math.isnan(challenger_number) or math.isnan(control_number):
        return math.nan
    return challenger_number - control_number


def _summary_rows(
    control: Mapping[str, Any], challenger: Mapping[str, Any]
) -> list[dict[str, Any]]:
    control_dd = float(control["calculated_max_daily_drawdown_points"])
    challenger_dd = float(challenger["calculated_max_daily_drawdown_points"])
    if control_dd > 0:
        challenger_dd_ratio = 100.0 * challenger_dd / control_dd
    elif challenger_dd == 0:
        challenger_dd_ratio = 100.0
    else:
        challenger_dd_ratio = math.inf
    control_fills = int(control["fills"])
    fill_retention = (
        100.0 * int(challenger["fills"]) / control_fills
        if control_fills > 0
        else math.nan
    )

    metric_fields = (
        "sessions",
        "candidates",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "active_sessions",
        "profit_factor",
        "profit_factor_excluding_best_net_day",
        "best_positive_day_share_pct",
        "long_profit_factor",
        "short_profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "calculated_max_daily_drawdown_points",
    )

    def row(label: str, metrics: Mapping[str, Any], dd_ratio: float, retention: float) -> dict[str, Any]:
        return {
            "stage": STAGE,
            "label": label,
            "variant": metrics.get("variant"),
            "from_day": metrics.get("from_day"),
            "through_day": metrics.get("through_day"),
            **{field: metrics.get(field) for field in metric_fields},
            "excluded_best_net_day": metrics.get("excluded_best_net_day"),
            "drawdown_as_pct_of_control": dd_ratio,
            "fill_retention_pct": retention,
            "research_only": True,
            "promotion_eligible": False,
        }

    control_row = row("CONTROL", control, 100.0, 100.0)
    challenger_row = row(
        "CHALLENGER", challenger, challenger_dd_ratio, fill_retention
    )
    delta_row = {
        "stage": STAGE,
        "label": "DELTA_CHALLENGER_MINUS_CONTROL",
        "variant": challenger.get("variant"),
        "from_day": challenger.get("from_day"),
        "through_day": challenger.get("through_day"),
        **{
            field: _delta_value(challenger.get(field), control.get(field))
            for field in metric_fields
        },
        "excluded_best_net_day": "",
        "drawdown_as_pct_of_control": challenger_dd_ratio - 100.0,
        "fill_retention_pct": fill_retention - 100.0,
        "research_only": True,
        "promotion_eligible": False,
    }
    return [control_row, challenger_row, delta_row]


def _is_evaluable(value: Any) -> bool:
    try:
        return not math.isnan(float(value))
    except (TypeError, ValueError):
        return False


def _gate_status(observed: Any, comparator_name: str, threshold: Any) -> str:
    if not _is_evaluable(observed) or not _is_evaluable(threshold):
        return "FAIL"
    observed_number = float(observed)
    threshold_number = float(threshold)
    if comparator_name == "GE":
        return "PASS" if observed_number >= threshold_number else "FAIL"
    if comparator_name == "LE":
        return "PASS" if observed_number <= threshold_number else "FAIL"
    if comparator_name == "EQ":
        return "PASS" if _same_number(observed_number, threshold_number) else "FAIL"
    raise ValueError(f"Unknown gate comparator: {comparator_name}")


def _preliminary_gate_rows(
    *,
    plan: Mapping[str, Any],
    control: Mapping[str, Any],
    challenger: Mapping[str, Any],
    selection_rows: int,
) -> list[dict[str, Any]]:
    integrity = dict(plan["stage_02_integrity_gates"])
    screen = dict(plan["stage_02_preliminary_performance_screen"])
    dd_ratio = float(
        _summary_rows(control, challenger)[1]["drawdown_as_pct_of_control"]
    )
    fill_retention = float(_summary_rows(control, challenger)[1]["fill_retention_pct"])

    rows: list[dict[str, Any]] = []

    def add(
        category: str,
        gate: str,
        comparator_name: str,
        threshold: Any,
        observed: Any,
        status: str,
        note: str,
    ) -> None:
        rows.append(
            {
                "stage": STAGE,
                "variant": EXPECTED_VARIANT,
                "category": category,
                "gate": gate,
                "comparator": comparator_name,
                "threshold": threshold,
                "control_value": "",
                "challenger_value": observed,
                "status": status,
                "note": note,
            }
        )

    add("INTEGRITY", "valid_provenance_required", "EQ", True, True, "PASS", "Both provenances validated")
    add("INTEGRITY", "expected_variant", "EQ", integrity["expected_variant"], EXPECTED_VARIANT, "PASS", "Frozen sequence identity")
    add(
        "INTEGRITY",
        "expected_base_candidates",
        "EQ",
        integrity["expected_base_candidates"],
        int(challenger["candidates"]),
        _gate_status(challenger["candidates"], "EQ", integrity["expected_base_candidates"]),
        "Entry-only variant retains the V10B selection set",
    )
    add(
        "INTEGRITY",
        "all_selection_decisions_must_pass",
        "EQ",
        integrity["expected_base_candidates"],
        selection_rows,
        _gate_status(selection_rows, "EQ", integrity["expected_base_candidates"]),
        "All decisions were PASSED",
    )
    add("INTEGRITY", "expected_selection_overlay", "EQ", integrity["expected_selection_overlay"], integrity["expected_selection_overlay"], "PASS", "BASE_V10B_SELECTION retained")
    add("INTEGRITY", "same_frozen_inputs_as_control", "EQ", True, True, "PASS", "Cache, snapshot, window, split, costs, universe and portfolio validated")
    add("INTEGRITY", "full_state_machine_replay_required", "EQ", True, True, "PASS", "Full candidate audit is present and bound")

    performance_gates = (
        ("minimum_fills", "GE", screen["minimum_fills"], challenger["fills"]),
        (
            "minimum_active_sessions",
            "GE",
            screen["minimum_active_sessions"],
            challenger["active_sessions"],
        ),
        (
            "minimum_profit_factor",
            "GE",
            screen["minimum_profit_factor"],
            challenger["profit_factor"],
        ),
        (
            "minimum_profit_factor_excluding_best_net_day",
            "GE",
            screen["minimum_profit_factor_excluding_best_net_day"],
            challenger["profit_factor_excluding_best_net_day"],
        ),
        (
            "maximum_best_positive_day_share_pct",
            "LE",
            screen["maximum_best_positive_day_share_pct"],
            challenger["best_positive_day_share_pct"],
        ),
        (
            "minimum_long_profit_factor",
            "GE",
            screen["minimum_long_profit_factor"],
            challenger["long_profit_factor"],
        ),
        (
            "minimum_short_profit_factor",
            "GE",
            screen["minimum_short_profit_factor"],
            challenger["short_profit_factor"],
        ),
        (
            "maximum_drawdown_as_pct_of_control",
            "LE",
            screen["maximum_drawdown_as_pct_of_control"],
            dd_ratio,
        ),
    )
    for gate, comparator_name, threshold, observed in performance_gates:
        add(
            "PRELIMINARY_PERFORMANCE",
            gate,
            comparator_name,
            threshold,
            observed,
            _gate_status(observed, comparator_name, threshold),
            "Research screen only; not promotion authority",
        )
    add("REQUIRED_REPORTING", "accuracy_pct_reported", "PRESENT", True, challenger["win_rate_pct"], "PASS" if _is_evaluable(challenger["win_rate_pct"]) else "FAIL", "Win-rate accuracy is reported in summary.csv")
    add("REQUIRED_REPORTING", "fill_retention_pct_reported", "PRESENT", True, fill_retention, "PASS" if _is_evaluable(fill_retention) else "FAIL", "Fill retention versus V10B is reported in summary.csv")
    add("STRESS", "stress_20bps_plus_2bps", "DEFERRED", screen["stress_20bps_plus_2bps"], "DEFERRED_NOT_EVALUATED", "DEFERRED", "No stressed backtest was authorized in Stage 2")
    add("AUTHORITY", "screen_is_promotion_authority", "EQ", False, False, "PASS", "Gate results cannot promote this research variant")
    return rows


def _funnel_rows(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    filled: pd.DataFrame,
) -> list[dict[str, Any]]:
    base = int(len(decisions))
    passed = int(_boolean_series(decisions["selection_passed"], "funnel selection").sum())
    replayed = int(len(audit))
    fills = int(len(filled))
    return [
        {
            "stage": STAGE,
            "variant": EXPECTED_VARIANT,
            "step_order": order,
            "step": step,
            "reason": reason,
            "count": count,
            "pct_of_base_candidates": 100.0 * count / base if base else math.nan,
        }
        for order, step, reason, count in (
            (1, "BASE_CANDIDATES", "ALL", base),
            (2, "SELECTION_PASSED", "PASSED", passed),
            (3, "STATE_MACHINE_REPLAYED", "ALL", replayed),
            (4, "FILLED", "FILLED_TRUE", fills),
        )
    ]


def _test_results_payload(
    *,
    tests_passed: int | None,
    tests_failed: int | None,
    test_commands: Sequence[str],
) -> dict[str, Any]:
    if (tests_passed is None) != (tests_failed is None):
        raise ValueError("--tests-passed and --tests-failed must be supplied together")
    if tests_passed is not None and tests_passed < 0:
        raise ValueError("--tests-passed cannot be negative")
    if tests_failed is not None and tests_failed < 0:
        raise ValueError("--tests-failed cannot be negative")
    commands = [str(command).strip() for command in test_commands]
    if any(not command for command in commands):
        raise ValueError("Recorded test commands cannot be empty")
    if tests_passed is None:
        status = "COUNTS_NOT_RECORDED"
        total = None
    else:
        status = "PASS" if int(tests_failed or 0) == 0 else "FAIL"
        total = tests_passed + int(tests_failed or 0)
    return {
        "schema_version": TEST_EVIDENCE_SCHEMA_VERSION,
        "status": status,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "tests_total": total,
        "commands": commands,
    }


def _recursive_artifacts(root: Path) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for path in sorted(
        (candidate for candidate in root.rglob("*") if candidate.is_file()),
        key=lambda candidate: candidate.relative_to(root).as_posix(),
    ):
        relative = path.relative_to(root).as_posix()
        if relative == "stage_manifest.json":
            continue
        artifacts[relative] = {
            "sha256": comparison.sha256_file(path),
            "bytes": int(path.stat().st_size),
        }
    return artifacts


def build_stage2_package(
    *,
    stage_plan: Path,
    stage1_control_provenance: Path,
    stage2_challenger_provenance: Path,
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"Stage 2 output directory already exists: {target}")
    plan_path = _require_file(stage_plan, "Frozen Stage 2 plan")
    control_path = _require_file(
        stage1_control_provenance, "Stage 1 V10B control provenance"
    )
    challenger_path = _require_file(
        stage2_challenger_provenance, "Stage 2 RV1_100 challenger provenance"
    )
    if _path_key(control_path) == _path_key(challenger_path):
        raise ValueError("Control and challenger provenance paths must differ")

    plan, sequence_entry = _load_frozen_plan(plan_path)
    stage1_manifest_path, stage1_manifest = _validate_stage1_package_binding(
        plan, control_path
    )
    control_payload = comparison.validate_provenance(control_path)
    challenger_payload = comparison.validate_provenance(challenger_path)
    control_config_sha = experiment_config.variant_config_sha256(CONTROL_VARIANT)
    challenger_config_sha = str(sequence_entry["variant_config_sha256"])
    _require_experiment_identity(
        control_payload,
        variant=CONTROL_VARIANT,
        variant_config_sha256=control_config_sha,
        label="Stage 1 control",
    )
    _require_experiment_identity(
        challenger_payload,
        variant=EXPECTED_VARIANT,
        variant_config_sha256=challenger_config_sha,
        label="Stage 2 challenger",
    )
    snapshot_path, expected_window = _validate_frozen_inputs(
        plan=plan,
        control=control_payload,
        challenger=challenger_payload,
        stage1_manifest=stage1_manifest,
    )

    integrity = dict(plan["stage_02_integrity_gates"])
    expected_candidates = int(integrity["expected_base_candidates"])
    expected_overlay = str(integrity["expected_selection_overlay"])
    control_decisions, control_audit = _validate_selection_and_audit(
        control_payload,
        variant=CONTROL_VARIANT,
        variant_config_sha256=control_config_sha,
        expected_candidates=expected_candidates,
        expected_overlay=expected_overlay,
        label="Stage 1 control",
    )
    challenger_decisions, challenger_audit = _validate_selection_and_audit(
        challenger_payload,
        variant=EXPECTED_VARIANT,
        variant_config_sha256=challenger_config_sha,
        expected_candidates=expected_candidates,
        expected_overlay=expected_overlay,
        label="Stage 2 challenger",
    )
    if set(control_decisions["candidate_id"].astype(str)) != set(
        challenger_decisions["candidate_id"].astype(str)
    ):
        raise ValueError("RV1_100 did not retain the exact V10B candidate set")

    control_metrics = _metric_bundle(
        control_payload, control_audit, label="CONTROL"
    )
    challenger_metrics = _metric_bundle(
        challenger_payload, challenger_audit, label="CHALLENGER"
    )
    summary_rows = _summary_rows(control_metrics, challenger_metrics)
    challenger_filled = _filled_rows(challenger_audit, "Stage 2 challenger")
    funnel_rows = _funnel_rows(
        challenger_decisions, challenger_audit, challenger_filled
    )
    gate_rows = _preliminary_gate_rows(
        plan=plan,
        control=control_metrics,
        challenger=challenger_metrics,
        selection_rows=len(challenger_decisions),
    )
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )

    comparison_dir = target / COMPARISON_DIR_NAME
    comparison.build_comparison(
        control_provenance=control_path,
        challenger_provenance=challenger_path,
        output_dir=comparison_dir,
        require_control_parity=False,
    )

    plan_copy_path = target / PLAN_FILENAME
    variant_path = target / "variant_config.json"
    summary_path = target / "summary.csv"
    daywise_path = target / DAYWISE_FILENAME
    side_setup_path = target / SIDE_SETUP_FILENAME
    funnel_path = target / "funnel.csv"
    ledger_path = target / "filled_trade_ledger.csv"
    gates_path = target / "preliminary_gates.csv"
    tests_path = target / "test_results.json"
    decision_path = target / "decision.md"

    _atomic_copy(plan_path, plan_copy_path)
    if comparison.sha256_file(plan_copy_path) != EXPECTED_STAGE_PLAN_SHA256:
        raise AssertionError("Frozen Stage 2 plan copy is not byte-identical")
    variant_archive = {
        "schema_version": VARIANT_ARCHIVE_SCHEMA_VERSION,
        "stage": STAGE,
        "variant": EXPECTED_VARIANT,
        "mechanism": sequence_entry.get("mechanism"),
        "frozen_plan_sha256": EXPECTED_STAGE_PLAN_SHA256,
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": challenger_config_sha,
        "variant_config": experiment_config.variant_config_payload(EXPECTED_VARIANT),
        "control_provenance": str(control_path),
        "control_provenance_sha256": comparison.sha256_file(control_path),
        "challenger_provenance": str(challenger_path),
        "challenger_provenance_sha256": comparison.sha256_file(challenger_path),
        "resolved_entry_policy": _parameters(challenger_payload).get("entry_policy"),
        "research_only": True,
        "promotion_eligible": False,
    }
    _write_json(variant_path, variant_archive)
    comparison.atomic_write_csv(summary_path, [_json_safe(row) for row in summary_rows])
    _atomic_copy(comparison_dir / DAYWISE_FILENAME, daywise_path)
    _atomic_copy(
        comparison_dir / "side_and_leg_comparison.csv", side_setup_path
    )
    comparison.atomic_write_csv(funnel_path, [_json_safe(row) for row in funnel_rows])
    _atomic_write_frame(ledger_path, challenger_filled)
    comparison.atomic_write_csv(gates_path, [_json_safe(row) for row in gate_rows])
    _write_json(tests_path, test_results)

    gate_counts = Counter(str(row["status"]) for row in gate_rows)
    challenger_summary = summary_rows[1]
    tests_line = (
        "not recorded"
        if test_results["tests_total"] is None
        else (
            f"{test_results['tests_passed']} passed / "
            f"{test_results['tests_failed']} failed"
        )
    )
    decision_text = f"""# FNO V10 Stage 2 - RV1_100 isolated experiment

- Decision: **{DECISION}**
- Control: `{control_path}`
- Challenger: `{challenger_path}`
- Frozen plan SHA256: `{EXPECTED_STAGE_PLAN_SHA256}`
- Variant config SHA256: `{challenger_config_sha}`
- Window: {expected_window['from_day']} through {expected_window['through_day']}
- Candidates retained: {int(challenger_summary['candidates'])} / {expected_candidates}
- Fills: {int(challenger_summary['fills'])}
- Active sessions: {int(challenger_summary['active_sessions'])}
- Accuracy: {float(challenger_summary['win_rate_pct']):.6f}%
- Fill retention versus V10B: {float(challenger_summary['fill_retention_pct']):.6f}%
- Profit factor: {float(challenger_summary['profit_factor']):.12f}
- Profit factor excluding best net day: {float(challenger_summary['profit_factor_excluding_best_net_day']):.12f}
- Best positive day share: {float(challenger_summary['best_positive_day_share_pct']):.6f}%
- LONG / SHORT profit factor: {float(challenger_summary['long_profit_factor']):.12f} / {float(challenger_summary['short_profit_factor']):.12f}
- Drawdown versus V10B: {float(challenger_summary['drawdown_as_pct_of_control']):.6f}%
- Preliminary gates: {gate_counts.get('PASS', 0)} PASS / {gate_counts.get('FAIL', 0)} FAIL / {gate_counts.get('DEFERRED', 0)} DEFERRED
- Stress 20bps + 2bps: **DEFERRED_NOT_EVALUATED**
- Test evidence: {tests_line}

This package records completion of the predeclared Stage 2 isolated research
run. Individual screening outcomes are preserved in `preliminary_gates.csv`;
they do not alter this completion-only decision and do not authorize live,
paper, or strategy promotion. The stress screen remains unevaluated.
"""
    comparison.atomic_write_text(decision_path, decision_text)

    artifacts = _recursive_artifacts(target)
    required_top_level = {
        PLAN_FILENAME,
        "variant_config.json",
        "summary.csv",
        DAYWISE_FILENAME,
        SIDE_SETUP_FILENAME,
        "funnel.csv",
        "filled_trade_ledger.csv",
        "preliminary_gates.csv",
        "test_results.json",
        "decision.md",
    }
    missing_top_level = sorted(required_top_level - set(artifacts))
    if missing_top_level:
        raise AssertionError(
            f"Stage 2 package is missing top-level artifacts: {missing_top_level}"
        )
    manifest = {
        "schema_version": STAGE_SCHEMA_VERSION,
        "stage": STAGE,
        "variant": EXPECTED_VARIANT,
        "decision": DECISION,
        "research_only": True,
        "promotion_eligible": False,
        "frozen_plan_source": str(plan_path),
        "frozen_plan_copy": PLAN_FILENAME,
        "frozen_plan_sha256": EXPECTED_STAGE_PLAN_SHA256,
        "stage1_control_provenance": str(control_path),
        "stage1_control_provenance_sha256": comparison.sha256_file(control_path),
        "stage2_challenger_provenance": str(challenger_path),
        "stage2_challenger_provenance_sha256": comparison.sha256_file(
            challenger_path
        ),
        "stage1_package_manifest": str(stage1_manifest_path),
        "stage1_package_manifest_sha256": comparison.sha256_file(
            stage1_manifest_path
        ),
        "source_snapshot_manifest": str(snapshot_path),
        "source_snapshot_manifest_sha256": comparison.sha256_file(snapshot_path),
        "source_snapshot_fingerprint": dict(
            challenger_payload.get("source_snapshot", {})
        ).get("snapshot_fingerprint"),
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": challenger_config_sha,
        "experiment_runner_source_sha256": challenger_payload.get(
            "experiment_runner_source_sha256"
        ),
        "experiment_config_source_sha256": challenger_payload.get(
            "experiment_config_source_sha256"
        ),
        "cache_input_fingerprint": challenger_payload.get(
            "cache_input_fingerprint"
        ),
        "backtest_window": expected_window,
        "fixed_economics_contract": comparison.fixed_economics_contract(
            challenger_payload
        ),
        "control_backtest_input_fingerprint": control_payload.get(
            "backtest_input_fingerprint"
        ),
        "challenger_backtest_input_fingerprint": challenger_payload.get(
            "backtest_input_fingerprint"
        ),
        "comparison_directory": COMPARISON_DIR_NAME,
        "summary": summary_rows,
        "preliminary_gate_status_counts": dict(sorted(gate_counts.items())),
        "stress_status": "DEFERRED_NOT_EVALUATED",
        "test_evidence": test_results,
        "artifact_hash_scope": (
            "ALL_PACKAGE_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_SELF"
        ),
        "artifacts": artifacts,
    }
    _write_json(target / "stage_manifest.json", manifest)
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage-plan",
        type=Path,
        default=WORKSPACE_ROOT / PLAN_FILENAME,
        help=f"Frozen sequence plan (default: workspace {PLAN_FILENAME}).",
    )
    parser.add_argument(
        "--stage1-control-provenance",
        "--control-provenance",
        dest="stage1_control_provenance",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--stage2-challenger-provenance",
        "--challenger-provenance",
        dest="stage2_challenger_provenance",
        type=Path,
        required=True,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument(
        "--test-command",
        action="append",
        default=[],
        help="Repeat to record each executed validation/test command.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_stage2_package(
        stage_plan=args.stage_plan,
        stage1_control_provenance=args.stage1_control_provenance,
        stage2_challenger_provenance=args.stage2_challenger_provenance,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
