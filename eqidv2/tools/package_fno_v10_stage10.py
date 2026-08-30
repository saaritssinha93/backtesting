"""Package the frozen Stage 10 locked-variant cost-stress matrix.

The utility consumes exactly nine already-completed V10 experiment
provenances.  It never launches a backtest and cannot authorize promotion.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = TOOLS_ROOT.parent
for import_root in (str(TOOLS_ROOT), str(WORKSPACE_ROOT)):
    if import_root not in sys.path:
        sys.path.insert(0, import_root)

import fno_v10_experiment_compare as comparison
import fno_v10_experiment_config as experiment_config
import package_fno_v10_stage2 as stage2_package
import package_fno_v10_stage3 as stage3_package
import package_fno_v10_stage5_to_stage8 as selection_package


STAGE = "STAGE_10"
PLAN_FILENAME = "fno_v10_stage10_cost_stress_plan.json"
EXPECTED_PLAN_SHA256 = "bf05b088e96d0e5259012a6d76cc837b5ed0941075f76ac31ccccb518191a928"
PACKAGE_SCHEMA_VERSION = "fno_v10_stage10_cost_stress_package_v1"
TEST_SCHEMA_VERSION = "fno_v10_stage10_cost_stress_test_evidence_v1"
DECISION = "COMPLETED_STAGE10_LOCKED_COST_STRESS_RESEARCH_NOT_PROMOTION"

VARIANT_KEYS = {
    "V10B": {
        "source_stage": "STAGE_01",
        "overlay": "BASE_V10B_SELECTION",
        "config_sha256": "b61ddb7a084000c9318112300938c67ddbe9cd47170951ed51711160d82476a4",
        "expected_candidates": 890,
        "selection_stage": None,
    },
    "0940_LONG_MOVE_030": {
        "source_stage": "STAGE_06",
        "overlay": "0940_LONG_MOVE_030",
        "config_sha256": "0848a01c04c69b33facd90d2bf9d6739f6356ef40478b10c6035d37fee8a2b12",
        "expected_candidates": 878,
        "selection_stage": "STAGE_06",
    },
    "0940_LONG_MOVE_040": {
        "source_stage": "STAGE_07",
        "overlay": "0940_LONG_MOVE_040",
        "config_sha256": "f3a54e5fddbfd8445923f9df52a68207f47b57bc43ccbd7eb83b2aad10a9bc18",
        "expected_candidates": 868,
        "selection_stage": "STAGE_07",
    },
}

SCENARIOS = {
    "REFERENCE_15_0": {
        "classification": "REFERENCE",
        "cost_bps": 15.0,
        "slippage_bps": 0.0,
    },
    "STRESS_20_2": {
        "classification": "STRESS",
        "cost_bps": 20.0,
        "slippage_bps": 2.0,
    },
    "STRESS_25_5": {
        "classification": "STRESS",
        "cost_bps": 25.0,
        "slippage_bps": 5.0,
    },
}

RUN_ORDER = (
    "V10B__REFERENCE_15_0",
    "V10B__STRESS_20_2",
    "V10B__STRESS_25_5",
    "0940_LONG_MOVE_030__REFERENCE_15_0",
    "0940_LONG_MOVE_030__STRESS_20_2",
    "0940_LONG_MOVE_030__STRESS_25_5",
    "0940_LONG_MOVE_040__REFERENCE_15_0",
    "0940_LONG_MOVE_040__STRESS_20_2",
    "0940_LONG_MOVE_040__STRESS_25_5",
)

RUN_ARGUMENTS = {
    "V10B__REFERENCE_15_0": "v10b_reference_provenance",
    "V10B__STRESS_20_2": "v10b_stress_20_2_provenance",
    "V10B__STRESS_25_5": "v10b_stress_25_5_provenance",
    "0940_LONG_MOVE_030__REFERENCE_15_0": "move030_reference_provenance",
    "0940_LONG_MOVE_030__STRESS_20_2": "move030_stress_20_2_provenance",
    "0940_LONG_MOVE_030__STRESS_25_5": "move030_stress_25_5_provenance",
    "0940_LONG_MOVE_040__REFERENCE_15_0": "move040_reference_provenance",
    "0940_LONG_MOVE_040__STRESS_20_2": "move040_stress_20_2_provenance",
    "0940_LONG_MOVE_040__STRESS_25_5": "move040_stress_25_5_provenance",
}

SUMMARY_FIELDS = stage3_package.SUMMARY_METRIC_FIELDS
PERIOD_FIELDS = stage3_package.PERIOD_METRIC_FIELDS
DIAGNOSTIC_FIELDS = selection_package.DIAGNOSTIC_FIELDS


def _require_file(path: Path, label: str) -> Path:
    return stage2_package._require_file(path, label)


def _path_key(path: Path) -> str:
    return stage2_package._path_key(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    stage2_package._write_json(path, payload)


def _split_run_id(run_id: str) -> tuple[str, str]:
    variant, scenario = run_id.split("__", 1)
    if variant not in VARIANT_KEYS or scenario not in SCENARIOS:
        raise ValueError(f"Unknown Stage 10 run identity: {run_id}")
    return variant, scenario


def _same_number(left: Any, right: Any) -> bool:
    return stage2_package._same_number(left, right)


def _load_plan(path: Path) -> dict[str, Any]:
    plan_path = _require_file(path, "Stage 10 frozen plan")
    if comparison.sha256_file(plan_path) != EXPECTED_PLAN_SHA256:
        raise ValueError("Stage 10 plan SHA256 differs from the frozen contract")
    plan = comparison.load_json(plan_path)
    if plan.get("schema_version") != "fno_v10_stage10_cost_stress_plan_v1":
        raise ValueError("Unsupported Stage 10 plan schema")
    if plan.get("stage") != STAGE or plan.get("objective") != (
        "LOCKED_VARIANT_FULL_STATE_MACHINE_COST_AND_SLIPPAGE_STRESS"
    ):
        raise ValueError("Stage 10 plan identity differs")
    if (
        plan.get("research_only") is not True
        or plan.get("promotion_eligible") is not False
        or plan.get("strategy_or_selection_retuning_allowed") is not False
        or plan.get("decision") != DECISION
    ):
        raise ValueError("Stage 10 authority flags differ")

    source = dict(plan.get("source_stage_plan", {}))
    source_path = Path(str(source.get("path", "")))
    if not source_path.is_absolute():
        source_path = WORKSPACE_ROOT / source_path
    source_path = _require_file(source_path, "Frozen Stage 1-9 plan")
    if (
        source.get("sha256")
        != "e8381c478b6843de26f74d61ef569495b739fe10f17743e447d0f771e7fd88c2"
        or comparison.sha256_file(source_path) != source.get("sha256")
    ):
        raise ValueError("Stage 10 source plan binding differs")

    frozen = dict(plan.get("frozen_inputs", {}))
    experiment_config.validate_registry()
    source_bindings = (
        (
            "runner_sha256",
            WORKSPACE_ROOT / "fno_v10_experiment_backtest.py",
        ),
        (
            "config_source_sha256",
            WORKSPACE_ROOT / "fno_v10_experiment_config.py",
        ),
    )
    for field, source_file in source_bindings:
        if comparison.sha256_file(_require_file(source_file, field)) != frozen.get(
            field
        ):
            raise ValueError(f"Current {field} differs from Stage 10 plan")
    if experiment_config.registry_sha256() != frozen.get("registry_sha256"):
        raise ValueError("Current registry differs from Stage 10 plan")
    snapshot = _require_file(
        Path(str(frozen.get("source_snapshot_manifest", ""))),
        "Stage 10 frozen source snapshot",
    )
    if comparison.sha256_file(snapshot) != frozen.get(
        "source_snapshot_manifest_sha256"
    ):
        raise ValueError("Stage 10 source snapshot SHA256 differs")

    observed_variants = [
        (
            int(item.get("matrix_order", -1)),
            str(item.get("variant", "")),
            str(item.get("source_stage", "")),
            str(item.get("variant_config_sha256", "")),
        )
        for item in list(plan.get("variants", []))
    ]
    expected_variants = [
        (
            index,
            variant,
            str(VARIANT_KEYS[variant]["source_stage"]),
            str(VARIANT_KEYS[variant]["config_sha256"]),
        )
        for index, variant in enumerate(VARIANT_KEYS, start=1)
    ]
    if observed_variants != expected_variants:
        raise ValueError("Stage 10 variant matrix differs")
    observed_scenarios = [
        (
            int(item.get("scenario_order", -1)),
            str(item.get("scenario", "")),
            float(item.get("cost_bps", math.nan)),
            float(item.get("slippage_bps", math.nan)),
        )
        for item in list(plan.get("scenarios", []))
    ]
    expected_scenarios = [
        (
            index,
            scenario,
            float(SCENARIOS[scenario]["cost_bps"]),
            float(SCENARIOS[scenario]["slippage_bps"]),
        )
        for index, scenario in enumerate(SCENARIOS, start=1)
    ]
    if observed_scenarios != expected_scenarios:
        raise ValueError("Stage 10 cost/slippage scenarios differ")
    observed_runs = [
        str(item.get("run_id", "")) for item in list(plan.get("run_matrix", []))
    ]
    if observed_runs != list(RUN_ORDER):
        raise ValueError("Stage 10 nine-run order differs")
    if list(plan.get("only_authorized_input_differences_within_variant", [])) != [
        "parameters.entry_policy.cost_bps",
        "parameters.entry_policy.slippage_bps",
    ]:
        raise ValueError("Stage 10 authorized input differences changed")
    return plan


def _variant_plan(plan: Mapping[str, Any], variant: str) -> dict[str, Any]:
    matches = [
        dict(item)
        for item in list(plan.get("variants", []))
        if item.get("variant") == variant
    ]
    if len(matches) != 1:
        raise ValueError(f"Stage 10 plan must contain one {variant} entry")
    return matches[0]


def _normalized_parameters(payload: Mapping[str, Any]) -> dict[str, Any]:
    parameters = copy.deepcopy(dict(payload.get("parameters", {})))
    policy = dict(parameters.get("entry_policy", {}))
    policy.pop("cost_bps", None)
    policy.pop("slippage_bps", None)
    parameters["entry_policy"] = policy
    return parameters


def _validate_fixed_context(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    variant: str,
    scenario: str,
    plan: Mapping[str, Any],
) -> None:
    frozen = dict(plan["frozen_inputs"])
    variant_spec = dict(VARIANT_KEYS[variant])
    stage2_package._require_experiment_identity(
        payload,
        variant=variant,
        variant_config_sha256=str(variant_spec["config_sha256"]),
        label=run_id,
    )
    direct_bindings = {
        "v10_experiment_registry_sha256": frozen["registry_sha256"],
        "experiment_runner_source_sha256": frozen["runner_sha256"],
        "experiment_config_source_sha256": frozen["config_source_sha256"],
        "cache_input_fingerprint": frozen["cache_input_fingerprint"],
    }
    for field, expected in direct_bindings.items():
        if payload.get(field) != expected:
            raise ValueError(f"{run_id} {field} differs from Stage 10 plan")
    source = dict(payload.get("source_snapshot", {}))
    observed_snapshot = _require_file(
        Path(str(source.get("manifest_path", ""))),
        f"{run_id} source snapshot",
    )
    frozen_snapshot = Path(str(frozen["source_snapshot_manifest"])).resolve()
    if _path_key(observed_snapshot) != _path_key(frozen_snapshot):
        raise ValueError(f"{run_id} source snapshot path differs")
    if source.get("snapshot_fingerprint") != frozen["source_snapshot_fingerprint"]:
        raise ValueError(f"{run_id} source snapshot fingerprint differs")
    if comparison.sha256_file(observed_snapshot) != frozen[
        "source_snapshot_manifest_sha256"
    ]:
        raise ValueError(f"{run_id} source snapshot SHA256 differs")

    expected_window = {
        field: frozen[field] for field in ("from_day", "through_day", "split_day")
    }
    if dict(payload.get("backtest_window", {})) != expected_window:
        raise ValueError(f"{run_id} backtest window differs")
    parameters = stage2_package._parameters(payload)
    observed_window = {field: parameters.get(field) for field in expected_window}
    if observed_window != expected_window:
        raise ValueError(f"{run_id} parameter window differs")
    if parameters.get("portfolio_mode") != frozen["portfolio_mode"]:
        raise ValueError(f"{run_id} portfolio mode differs")
    if not _same_number(
        parameters.get("target_exposure_per_entry_rs"),
        frozen["target_exposure_per_entry_rs"],
    ):
        raise ValueError(f"{run_id} target exposure differs")
    policy = dict(parameters.get("entry_policy", {}))
    scenario_spec = SCENARIOS[scenario]
    required_policy = {
        "cost_bps": scenario_spec["cost_bps"],
        "slippage_bps": scenario_spec["slippage_bps"],
        "square_off": frozen["square_off"],
        "eod_policy": frozen["eod_policy"],
        "same_bar_policy": frozen["same_bar_policy"],
    }
    for field, expected in required_policy.items():
        observed = policy.get(field)
        if isinstance(expected, (int, float)):
            if not _same_number(observed, expected):
                raise ValueError(f"{run_id} entry policy {field} differs")
        elif observed != expected:
            raise ValueError(f"{run_id} entry policy {field} differs")
    if payload.get("research_only") is not True:
        raise ValueError(f"{run_id} is not research-only")
    if payload.get("promotion_eligible") is not False:
        raise ValueError(f"{run_id} is promotion-eligible")
    results = dict(payload.get("results", {}))
    if results.get("promotion_eligible") is not False:
        raise ValueError(f"{run_id} results are promotion-eligible")
    if "FULL_CHRONOLOGICAL_V10_STATE_MACHINE_REPLAY" not in str(
        payload.get("objective", "")
    ):
        raise ValueError(f"{run_id} does not attest full state-machine replay")


def _validate_reference_binding(
    *,
    path: Path,
    payload: Mapping[str, Any],
    variant_plan: Mapping[str, Any],
    run_id: str,
) -> None:
    expected_path = _require_file(
        Path(str(variant_plan["reference_provenance_path"])),
        f"{run_id} frozen reference provenance",
    )
    if _path_key(path) != _path_key(expected_path):
        raise ValueError(f"{run_id} is not the frozen reference path")
    observed_sha = comparison.sha256_file(path)
    if observed_sha != variant_plan["reference_provenance_sha256"]:
        raise ValueError(f"{run_id} reference provenance SHA256 differs")
    if payload.get("backtest_input_fingerprint") != variant_plan[
        "reference_backtest_input_fingerprint"
    ]:
        raise ValueError(f"{run_id} reference input fingerprint differs")
    outputs = dict(payload.get("outputs", {}))
    if dict(outputs.get("selection_decisions", {})).get("sha256") != variant_plan[
        "reference_selection_decisions_sha256"
    ]:
        raise ValueError(f"{run_id} reference selection artifact differs")
    if dict(outputs.get("candidate_order_audit", {})).get("sha256") != variant_plan[
        "reference_candidate_order_audit_sha256"
    ]:
        raise ValueError(f"{run_id} reference audit artifact differs")


def _validate_audit_costs(
    audit: pd.DataFrame, *, run_id: str, scenario: str
) -> None:
    required = {"cost_bps", "slippage_bps", "candidate_id", "filled"}
    missing = sorted(required - set(audit.columns))
    if missing:
        raise ValueError(f"{run_id} audit misses cost-stress columns: {missing}")
    scenario_spec = SCENARIOS[scenario]
    for field in ("cost_bps", "slippage_bps"):
        values = pd.to_numeric(audit[field], errors="coerce")
        expected = float(scenario_spec[field])
        if values.isna().any() or not values.map(
            lambda value: _same_number(value, expected)
        ).all():
            raise ValueError(f"{run_id} audit {field} differs from scenario")


def _load_and_validate_runs(
    *, plan: Mapping[str, Any], paths: Mapping[str, Path]
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, pd.DataFrame],
    dict[str, pd.DataFrame],
]:
    if set(paths) != set(RUN_ORDER):
        raise ValueError("Stage 10 provenance mapping is incomplete")
    resolved_paths = {
        run_id: _require_file(Path(paths[run_id]), f"{run_id} provenance")
        for run_id in RUN_ORDER
    }
    if len({_path_key(path) for path in resolved_paths.values()}) != len(RUN_ORDER):
        raise ValueError("Stage 10 requires nine distinct provenance paths")
    payloads: dict[str, dict[str, Any]] = {}
    for run_id in RUN_ORDER:
        variant, scenario = _split_run_id(run_id)
        payload = comparison.validate_provenance(resolved_paths[run_id])
        _validate_fixed_context(
            payload,
            run_id=run_id,
            variant=variant,
            scenario=scenario,
            plan=plan,
        )
        if scenario == "REFERENCE_15_0":
            _validate_reference_binding(
                path=resolved_paths[run_id],
                payload=payload,
                variant_plan=_variant_plan(plan, variant),
                run_id=run_id,
            )
        payloads[run_id] = payload

    base_decisions, _ = stage2_package._validate_selection_and_audit(
        payloads["V10B__REFERENCE_15_0"],
        variant="V10B",
        variant_config_sha256=str(VARIANT_KEYS["V10B"]["config_sha256"]),
        expected_candidates=890,
        expected_overlay="BASE_V10B_SELECTION",
        label="V10B__REFERENCE_15_0",
    )
    base_ids = base_decisions["candidate_id"].astype(str).tolist()
    decisions: dict[str, pd.DataFrame] = {}
    audits: dict[str, pd.DataFrame] = {}
    reference_parameters: dict[str, dict[str, Any]] = {}
    reference_strategy_payloads: dict[str, Any] = {}
    reference_audit_ids: dict[str, list[str]] = {}
    for run_id in RUN_ORDER:
        variant, scenario = _split_run_id(run_id)
        payload = payloads[run_id]
        variant_spec = VARIANT_KEYS[variant]
        if variant == "V10B":
            decisions[run_id], audits[run_id] = (
                stage2_package._validate_selection_and_audit(
                    payload,
                    variant=variant,
                    variant_config_sha256=str(variant_spec["config_sha256"]),
                    expected_candidates=890,
                    expected_overlay=str(variant_spec["overlay"]),
                    label=run_id,
                )
            )
        else:
            decisions[run_id], audits[run_id], selection = (
                selection_package._validate_selection_variant(
                    payload,
                    stage=str(variant_spec["selection_stage"]),
                    base_candidate_ids=base_ids,
                )
            )
            if int(selection["selection_passed"]) != int(
                variant_spec["expected_candidates"]
            ):
                raise ValueError(f"{run_id} retained-candidate count differs")
        expected_selection_sha = _variant_plan(plan, variant)[
            "reference_selection_decisions_sha256"
        ]
        observed_selection_sha = dict(payload.get("outputs", {})).get(
            "selection_decisions", {}
        ).get("sha256")
        if observed_selection_sha != expected_selection_sha:
            raise ValueError(
                f"{run_id} selection decisions are not byte-identical to reference"
            )
        _validate_audit_costs(audits[run_id], run_id=run_id, scenario=scenario)
        audit_ids = audits[run_id]["candidate_id"].astype(str).tolist()
        if scenario == "REFERENCE_15_0":
            reference_parameters[variant] = _normalized_parameters(payload)
            reference_strategy_payloads[variant] = payload.get("strategy_payload")
            reference_audit_ids[variant] = audit_ids
        else:
            if _normalized_parameters(payload) != reference_parameters[variant]:
                raise ValueError(
                    f"{run_id} changes inputs beyond cost/slippage"
                )
            if payload.get("strategy_payload") != reference_strategy_payloads[variant]:
                raise ValueError(f"{run_id} strategy payload differs from reference")
            if audit_ids != reference_audit_ids[variant]:
                raise ValueError(f"{run_id} candidate audit order differs")
    return payloads, decisions, audits


def _scenario_rows(
    *,
    paths: Mapping[str, Path],
    payloads: Mapping[str, Mapping[str, Any]],
    decisions: Mapping[str, pd.DataFrame],
    audits: Mapping[str, pd.DataFrame],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    metrics = {
        run_id: stage2_package._metric_bundle(
            payloads[run_id], audits[run_id], label=run_id
        )
        for run_id in RUN_ORDER
    }
    rows: list[dict[str, Any]] = []
    for run_id in RUN_ORDER:
        variant, scenario = _split_run_id(run_id)
        scenario_spec = SCENARIOS[scenario]
        passed = stage2_package._boolean_series(
            decisions[run_id]["selection_passed"], f"{run_id} selection"
        )
        row = {
            "stage": STAGE,
            "run_id": run_id,
            "source_stage": VARIANT_KEYS[variant]["source_stage"],
            "variant": variant,
            "scenario": scenario,
            "classification": scenario_spec["classification"],
            "cost_bps": scenario_spec["cost_bps"],
            "slippage_bps": scenario_spec["slippage_bps"],
            **{field: metrics[run_id].get(field) for field in SUMMARY_FIELDS},
            "excluded_best_net_day": metrics[run_id].get("excluded_best_net_day"),
            "base_selection_decisions": int(len(decisions[run_id])),
            "selection_passed": int(passed.sum()),
            "selection_rejected": int((~passed).sum()),
            "provenance": str(paths[run_id]),
            "provenance_sha256": comparison.sha256_file(paths[run_id]),
            "backtest_input_fingerprint": payloads[run_id].get(
                "backtest_input_fingerprint"
            ),
            "research_only": True,
            "promotion_eligible": False,
        }
        rows.append(row)
    return rows, metrics


def _paired_candidate_rows(
    *, audits: Mapping[str, pd.DataFrame]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    aggregate_inputs: dict[str, dict[str, Any]] = {}
    numeric_fields = (
        "entry_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "net_pnl_rs",
    )
    for variant in VARIANT_KEYS:
        reference_id = f"{variant}__REFERENCE_15_0"
        reference = audits[reference_id].reset_index(drop=True)
        reference_filled = stage2_package._boolean_series(
            reference["filled"], f"{reference_id} filled"
        )
        for scenario in ("STRESS_20_2", "STRESS_25_5"):
            run_id = f"{variant}__{scenario}"
            stress = audits[run_id].reset_index(drop=True)
            stress_filled = stage2_package._boolean_series(
                stress["filled"], f"{run_id} filled"
            )
            if reference["candidate_id"].astype(str).tolist() != stress[
                "candidate_id"
            ].astype(str).tolist():
                raise ValueError(f"{run_id} cannot be paired to reference")
            pair_rows: list[dict[str, Any]] = []
            for index in range(len(reference)):
                ref = reference.iloc[index]
                observed = stress.iloc[index]
                for identity_field in (
                    "session_date",
                    "signal_time",
                    "signal_end",
                    "setup_id",
                    "side",
                    "symbol",
                    "futures_symbol",
                ):
                    if str(ref.get(identity_field, "")) != str(
                        observed.get(identity_field, "")
                    ):
                        raise ValueError(
                            f"{run_id} paired candidate {ref['candidate_id']} "
                            f"changes {identity_field}"
                        )
                row: dict[str, Any] = {
                    "stage": STAGE,
                    "variant": variant,
                    "reference_scenario": "REFERENCE_15_0",
                    "stress_scenario": scenario,
                    "stress_cost_bps": SCENARIOS[scenario]["cost_bps"],
                    "stress_slippage_bps": SCENARIOS[scenario]["slippage_bps"],
                    "candidate_id": str(ref["candidate_id"]),
                    "session_date": str(ref["session_date"]),
                    "setup_id": str(ref["setup_id"]),
                    "side": str(ref["side"]),
                    "symbol": str(ref.get("symbol", "")),
                    "reference_status": str(ref["status"]),
                    "stress_status": str(observed["status"]),
                    "status_changed": str(ref["status"]) != str(observed["status"]),
                    "reference_filled": bool(reference_filled.iloc[index]),
                    "stress_filled": bool(stress_filled.iloc[index]),
                    "reference_exit_reason": str(ref.get("exit_reason", "")),
                    "stress_exit_reason": str(observed.get("exit_reason", "")),
                }
                for field in numeric_fields:
                    ref_value = pd.to_numeric(pd.Series([ref.get(field)]), errors="coerce").iloc[0]
                    stress_value = pd.to_numeric(
                        pd.Series([observed.get(field)]), errors="coerce"
                    ).iloc[0]
                    row[f"reference_{field}"] = ref_value
                    row[f"stress_{field}"] = stress_value
                    row[f"delta_{field}"] = (
                        float(stress_value) - float(ref_value)
                        if pd.notna(ref_value) and pd.notna(stress_value)
                        else math.nan
                    )
                pair_rows.append(row)
            rows.extend(pair_rows)
            aggregate_inputs[run_id] = {
                "paired_rows": len(pair_rows),
                "status_changed": sum(bool(row["status_changed"]) for row in pair_rows),
                "reference_filled": int(reference_filled.sum()),
                "stress_filled": int(stress_filled.sum()),
                "both_filled": int((reference_filled & stress_filled).sum()),
                "reference_only_filled": int(
                    (reference_filled & ~stress_filled).sum()
                ),
                "stress_only_filled": int((~reference_filled & stress_filled).sum()),
            }
    return rows, aggregate_inputs


def _paired_summary_rows(
    *,
    metrics: Mapping[str, Mapping[str, Any]],
    aggregate_inputs: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in VARIANT_KEYS:
        reference_id = f"{variant}__REFERENCE_15_0"
        reference = metrics[reference_id]
        for scenario in ("STRESS_20_2", "STRESS_25_5"):
            run_id = f"{variant}__{scenario}"
            stress = metrics[run_id]
            dd_ratio, fill_retention = stage3_package._dd_and_fill_ratios(
                stress, reference
            )
            rows.append(
                {
                    "stage": STAGE,
                    "variant": variant,
                    "source_stage": VARIANT_KEYS[variant]["source_stage"],
                    "reference_scenario": "REFERENCE_15_0",
                    "stress_scenario": scenario,
                    "reference_cost_bps": 15.0,
                    "reference_slippage_bps": 0.0,
                    "stress_cost_bps": SCENARIOS[scenario]["cost_bps"],
                    "stress_slippage_bps": SCENARIOS[scenario]["slippage_bps"],
                    **dict(aggregate_inputs[run_id]),
                    **{
                        f"reference_{field}": reference.get(field)
                        for field in SUMMARY_FIELDS
                    },
                    **{
                        f"stress_{field}": stress.get(field)
                        for field in SUMMARY_FIELDS
                    },
                    **{
                        f"delta_{field}": stage2_package._delta_value(
                            stress.get(field), reference.get(field)
                        )
                        for field in SUMMARY_FIELDS
                    },
                    "fill_retention_pct": fill_retention,
                    "drawdown_as_pct_of_reference": dd_ratio,
                    "research_only": True,
                    "promotion_eligible": False,
                }
            )
    return rows


def _daywise_rows(
    payloads: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fields = ("candidates", "fills", "net_return_pct", "net_pnl_rs")
    for variant in VARIANT_KEYS:
        reference_id = f"{variant}__REFERENCE_15_0"
        reference = stage3_package._read_daily(payloads[reference_id], reference_id)
        ref_dates = reference["session_date"].astype(str).tolist()
        ref_periods = reference["period"].astype(str).tolist()
        for scenario in SCENARIOS:
            run_id = f"{variant}__{scenario}"
            observed = stage3_package._read_daily(payloads[run_id], run_id)
            if observed["session_date"].astype(str).tolist() != ref_dates:
                raise ValueError(f"{run_id} daily sessions differ from reference")
            if observed["period"].astype(str).tolist() != ref_periods:
                raise ValueError(f"{run_id} daily TRAIN/TEST labels differ")
            for index in range(len(observed)):
                row = {
                    "stage": STAGE,
                    "variant": variant,
                    "source_stage": VARIANT_KEYS[variant]["source_stage"],
                    "scenario": scenario,
                    "classification": SCENARIOS[scenario]["classification"],
                    "cost_bps": SCENARIOS[scenario]["cost_bps"],
                    "slippage_bps": SCENARIOS[scenario]["slippage_bps"],
                    "session_date": str(observed.iloc[index]["session_date"]),
                    "period": str(observed.iloc[index]["period"]),
                }
                for field in fields:
                    current = observed.iloc[index][field]
                    baseline = reference.iloc[index][field]
                    row[field] = current
                    row[f"reference_{field}"] = baseline
                    row[f"delta_vs_reference_{field}"] = stage2_package._delta_value(
                        current, baseline
                    )
                rows.append(row)
    return rows


def _train_test_rows(
    payloads: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in VARIANT_KEYS:
        reference_id = f"{variant}__REFERENCE_15_0"
        reference = stage3_package._period_metrics(
            payloads[reference_id], reference_id
        )
        for scenario in SCENARIOS:
            run_id = f"{variant}__{scenario}"
            observed = stage3_package._period_metrics(payloads[run_id], run_id)
            for period in ("TRAIN", "TEST"):
                row = {
                    "stage": STAGE,
                    "variant": variant,
                    "source_stage": VARIANT_KEYS[variant]["source_stage"],
                    "scenario": scenario,
                    "classification": SCENARIOS[scenario]["classification"],
                    "cost_bps": SCENARIOS[scenario]["cost_bps"],
                    "slippage_bps": SCENARIOS[scenario]["slippage_bps"],
                    "reference_scenario": "REFERENCE_15_0",
                    "period": period,
                }
                for field in PERIOD_FIELDS:
                    current = observed[period].get(field)
                    baseline = reference[period].get(field)
                    row[field] = current
                    row[f"reference_{field}"] = baseline
                    row[f"delta_vs_reference_{field}"] = (
                        stage2_package._delta_value(current, baseline)
                    )
                rows.append(row)
    return rows


def _side_setup_rows(
    payloads: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    additive = set(DIAGNOSTIC_FIELDS) - {"profit_factor"}
    for variant in VARIANT_KEYS:
        reference_id = f"{variant}__REFERENCE_15_0"
        reference_frame = selection_package._diagnostic_frame(
            payloads[reference_id], reference_id
        )
        reference_map = {
            (str(row["dimension"]), str(row["bucket"])): row
            for row in reference_frame.to_dict("records")
        }
        for scenario in SCENARIOS:
            run_id = f"{variant}__{scenario}"
            observed_frame = selection_package._diagnostic_frame(
                payloads[run_id], run_id
            )
            observed_map = {
                (str(row["dimension"]), str(row["bucket"])): row
                for row in observed_frame.to_dict("records")
            }
            for dimension, bucket in sorted(set(reference_map) | set(observed_map)):
                baseline = reference_map.get((dimension, bucket), {})
                observed = observed_map.get((dimension, bucket), {})
                row: dict[str, Any] = {
                    "stage": STAGE,
                    "variant": variant,
                    "source_stage": VARIANT_KEYS[variant]["source_stage"],
                    "scenario": scenario,
                    "classification": SCENARIOS[scenario]["classification"],
                    "cost_bps": SCENARIOS[scenario]["cost_bps"],
                    "slippage_bps": SCENARIOS[scenario]["slippage_bps"],
                    "reference_scenario": "REFERENCE_15_0",
                    "dimension": dimension,
                    "bucket": bucket,
                }
                for field in DIAGNOSTIC_FIELDS:
                    if field in additive:
                        current = observed.get(field, 0.0)
                        reference_value = baseline.get(field, 0.0)
                    else:
                        current = observed.get(field, "")
                        reference_value = baseline.get(field, "")
                    row[field] = current
                    row[f"reference_{field}"] = reference_value
                    row[f"delta_vs_reference_{field}"] = (
                        stage2_package._delta_value(current, reference_value)
                        if current != "" and reference_value != ""
                        else ""
                    )
                rows.append(row)
    return rows


def _gate_rows(
    *,
    plan_path: Path,
    paths: Mapping[str, Path],
    decisions: Mapping[str, pd.DataFrame],
    audits: Mapping[str, pd.DataFrame],
    scenario_rows: Sequence[Mapping[str, Any]],
    paired_rows: Sequence[Mapping[str, Any]],
    train_test_rows: Sequence[Mapping[str, Any]],
    daywise_rows: Sequence[Mapping[str, Any]],
    side_setup_rows: Sequence[Mapping[str, Any]],
    test_results: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(
        category: str,
        gate: str,
        comparator: str,
        threshold: Any,
        observed: Any,
        status: str,
        note: str,
    ) -> None:
        rows.append(
            {
                "stage": STAGE,
                "category": category,
                "gate": gate,
                "comparator": comparator,
                "threshold": threshold,
                "observed": observed,
                "status": status,
                "note": note,
            }
        )

    add(
        "FROZEN_CONTRACT",
        "stage10_plan_sha256",
        "EQ",
        EXPECTED_PLAN_SHA256,
        comparison.sha256_file(plan_path),
        "PASS",
        "The package uses the exact frozen Stage 10 plan.",
    )
    add(
        "RUN_MATRIX",
        "nine_distinct_provenances",
        "EQ",
        9,
        len({_path_key(path) for path in paths.values()}),
        "PASS",
        "One immutable provenance was supplied for every frozen matrix cell.",
    )
    add(
        "PROVENANCE",
        "complete_hash_valid_provenances",
        "EQ",
        9,
        len(paths),
        "PASS",
        "All provenance self-validations and referenced output hashes passed.",
    )
    add(
        "REFERENCE_BINDING",
        "exact_frozen_reference_runs",
        "EQ",
        3,
        3,
        "PASS",
        "Reference paths, provenance hashes, input fingerprints, and selection/audit hashes match the frozen plan.",
    )
    add(
        "FIXED_CONTEXT",
        "runner_config_registry_cache_snapshot_window_portfolio",
        "EQ",
        "FROZEN",
        "FROZEN",
        "PASS",
        "Every run matches the frozen runner, config, registry, cache, snapshot, window, and portfolio contract.",
    )
    for variant in VARIANT_KEYS:
        add(
            "AUTHORIZED_DIFFERENCES",
            f"{variant.lower()}_cost_slippage_only",
            "EQ",
            "parameters.entry_policy.cost_bps|parameters.entry_policy.slippage_bps",
            "parameters.entry_policy.cost_bps|parameters.entry_policy.slippage_bps",
            "PASS",
            "Normalized parameters, strategy payload, universe, and candidate order are unchanged within the variant.",
        )
        selection_hashes = {
            comparison.sha256_file(
                comparison.output_path(
                    comparison.load_json(paths[f"{variant}__{scenario}"]),
                    "selection_decisions",
                )
            )
            for scenario in SCENARIOS
        }
        add(
            "SELECTION_IDENTITY",
            f"{variant.lower()}_selection_byte_identity",
            "EQ",
            1,
            len(selection_hashes),
            "PASS",
            "Selection decisions are byte-identical across all three economics scenarios.",
        )
        expected_replayed = int(VARIANT_KEYS[variant]["expected_candidates"])
        observed_replayed = {
            len(audits[f"{variant}__{scenario}"]) for scenario in SCENARIOS
        }
        add(
            "REPLAY_RECONCILIATION",
            f"{variant.lower()}_replayed_candidates",
            "EQ",
            expected_replayed,
            next(iter(observed_replayed)),
            "PASS" if observed_replayed == {expected_replayed} else "FAIL",
            "Every retained candidate was replayed through the full state machine.",
        )
    add(
        "SCENARIO_BINDING",
        "audit_cost_slippage_exact",
        "EQ",
        9,
        len(audits),
        "PASS",
        "Every audit row carries the exact frozen scenario cost and slippage.",
    )
    expected_pairs = 2 * sum(
        int(VARIANT_KEYS[variant]["expected_candidates"])
        for variant in VARIANT_KEYS
    )
    add(
        "PAIRING",
        "candidate_pairs_complete",
        "EQ",
        expected_pairs,
        len(paired_rows),
        "PASS" if len(paired_rows) == expected_pairs else "FAIL",
        "Each stress audit is paired one-to-one with its locked reference audit.",
    )
    add(
        "REPORTING",
        "scenario_rows",
        "EQ",
        9,
        len(scenario_rows),
        "PASS" if len(scenario_rows) == 9 else "FAIL",
        "One summary row exists for every matrix cell.",
    )
    add(
        "REPORTING",
        "train_test_rows",
        "EQ",
        18,
        len(train_test_rows),
        "PASS" if len(train_test_rows) == 18 else "FAIL",
        "TRAIN and TEST metrics exist for every matrix cell.",
    )
    add(
        "REPORTING",
        "daywise_and_side_setup_present",
        "GT",
        0,
        f"daywise={len(daywise_rows)};side_setup={len(side_setup_rows)}",
        "PASS" if daywise_rows and side_setup_rows else "FAIL",
        "Daywise and side/setup scenario comparisons are populated.",
    )
    test_status = str(test_results.get("status", "COUNTS_NOT_RECORDED"))
    add(
        "TEST_EVIDENCE",
        "recorded_test_commands",
        "OPTIONAL",
        "NO_FAILURES_IF_RECORDED",
        test_status,
        (
            "PASS"
            if test_status == "PASS"
            else "FAIL"
            if test_status == "FAIL"
            else "DEFERRED"
        ),
        "Test counts are optional; any supplied failures remain visible.",
    )
    add(
        "AUTHORITY",
        "package_is_promotion_authority",
        "EQ",
        False,
        False,
        "PASS",
        "Stage 10 is descriptive locked-variant research only.",
    )
    return rows


def _test_results_payload(
    *,
    tests_passed: int | None,
    tests_failed: int | None,
    test_commands: Sequence[str],
) -> dict[str, Any]:
    payload = stage2_package._test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )
    payload["schema_version"] = TEST_SCHEMA_VERSION
    return payload


def _display_metric(value: Any, *, digits: int = 6) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isnan(number):
        return "NOT_EVALUABLE"
    if math.isinf(number):
        return "POSITIVE_INFINITY" if number > 0 else "NEGATIVE_INFINITY"
    return f"{number:.{digits}f}"


def _decision_text(
    *,
    paths: Mapping[str, Path],
    metrics: Mapping[str, Mapping[str, Any]],
    paired_summary: Sequence[Mapping[str, Any]],
    gate_rows: Sequence[Mapping[str, Any]],
    test_results: Mapping[str, Any],
) -> str:
    lines = [
        "# FNO V10 Stage 10 - Locked Cost/Slippage Stress",
        "",
        f"- Decision: **{DECISION}**",
        f"- Frozen plan SHA256: `{EXPECTED_PLAN_SHA256}`",
        "- Matrix: 3 locked variants x 3 economics scenarios = 9 full replays",
        "- Reference economics: 15 bps cost + 0 bps slippage",
        "- Stress economics: 20 + 2 bps and 25 + 5 bps",
        "",
        "## Scenario results",
        "",
        "| Variant | Scenario | Fills | Win rate | Profit factor | Net return points | Max daily drawdown |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for run_id in RUN_ORDER:
        variant, scenario = _split_run_id(run_id)
        item = metrics[run_id]
        lines.append(
            "| "
            + " | ".join(
                (
                    variant,
                    scenario,
                    str(int(item["fills"])),
                    f"{_display_metric(item['win_rate_pct'])}%",
                    _display_metric(item["profit_factor"], digits=12),
                    _display_metric(item["net_return_points"], digits=12),
                    _display_metric(
                        item["calculated_max_daily_drawdown_points"], digits=12
                    ),
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Paired stress checks",
            "",
            "| Variant | Stress | Status changes | Reference-only fills | Stress-only fills | Fill retention | PF delta | Net-return delta |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in paired_summary:
        lines.append(
            "| "
            + " | ".join(
                (
                    str(row["variant"]),
                    str(row["stress_scenario"]),
                    str(row["status_changed"]),
                    str(row["reference_only_filled"]),
                    str(row["stress_only_filled"]),
                    f"{_display_metric(row['fill_retention_pct'])}%",
                    _display_metric(row["delta_profit_factor"], digits=12),
                    _display_metric(row["delta_net_return_points"], digits=12),
                )
            )
            + " |"
        )
    gate_counts = Counter(str(row["status"]) for row in gate_rows)
    tests_line = (
        "not recorded"
        if test_results.get("tests_total") is None
        else (
            f"{test_results.get('tests_passed')} passed / "
            f"{test_results.get('tests_failed')} failed"
        )
    )
    lines.extend(
        [
            "",
            "## Validation and authority",
            "",
            f"- Validation gates: {gate_counts.get('PASS', 0)} PASS / {gate_counts.get('FAIL', 0)} FAIL / {gate_counts.get('DEFERRED', 0)} DEFERRED",
            f"- Test evidence: {tests_line}",
            "- All nine original provenance artifacts remain unchanged; their paths and SHA256 hashes are bound in the manifest.",
            "- Stress outcomes are descriptive sensitivity evidence only. They do not retune selection, choose a winner, or authorize live, paper, or strategy promotion.",
            "",
            "## Provenance matrix",
            "",
        ]
    )
    lines.extend(f"- `{run_id}`: `{paths[run_id]}`" for run_id in RUN_ORDER)
    return "\n".join(lines) + "\n"


def build_stage10_package(
    *,
    stage_plan: Path,
    provenance_by_run: Mapping[str, Path],
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"Stage 10 output directory already exists: {target}")
    if not target.parent.is_dir():
        raise FileNotFoundError(
            f"Stage 10 output parent directory does not exist: {target.parent}"
        )
    plan_path = _require_file(stage_plan, "Stage 10 frozen plan")
    if set(provenance_by_run) != set(RUN_ORDER):
        missing = sorted(set(RUN_ORDER) - set(provenance_by_run))
        extra = sorted(set(provenance_by_run) - set(RUN_ORDER))
        raise ValueError(
            f"Stage 10 provenance mapping differs; missing={missing}, extra={extra}"
        )
    paths = {
        run_id: _require_file(
            Path(provenance_by_run[run_id]), f"{run_id} provenance"
        )
        for run_id in RUN_ORDER
    }
    plan = _load_plan(plan_path)
    payloads, decisions, audits = _load_and_validate_runs(plan=plan, paths=paths)

    # The universe is a frozen common input even though the three selection
    # variants intentionally have different retained candidate sets.
    reference_universe = dict(payloads[RUN_ORDER[0]].get("universe", {}))
    if not reference_universe:
        raise ValueError("Stage 10 frozen universe is missing")
    for run_id in RUN_ORDER[1:]:
        if dict(payloads[run_id].get("universe", {})) != reference_universe:
            raise ValueError(f"{run_id} universe differs from the frozen matrix")

    scenario_rows, metrics = _scenario_rows(
        paths=paths,
        payloads=payloads,
        decisions=decisions,
        audits=audits,
    )
    paired_candidate_rows, aggregate_inputs = _paired_candidate_rows(audits=audits)
    paired_summary_rows = _paired_summary_rows(
        metrics=metrics, aggregate_inputs=aggregate_inputs
    )
    daywise_rows = _daywise_rows(payloads)
    train_test_rows = _train_test_rows(payloads)
    side_setup_rows = _side_setup_rows(payloads)
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )
    gate_rows = _gate_rows(
        plan_path=plan_path,
        paths=paths,
        decisions=decisions,
        audits=audits,
        scenario_rows=scenario_rows,
        paired_rows=paired_candidate_rows,
        train_test_rows=train_test_rows,
        daywise_rows=daywise_rows,
        side_setup_rows=side_setup_rows,
        test_results=test_results,
    )
    failed_contract_gates = [
        row
        for row in gate_rows
        if row["status"] == "FAIL" and row["category"] != "TEST_EVIDENCE"
    ]
    if failed_contract_gates:
        raise ValueError(
            "Stage 10 validation contract failed: "
            + ", ".join(str(row["gate"]) for row in failed_contract_gates)
        )

    staging = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.stage10-", dir=target.parent)
    )
    published = False
    try:
        stage2_package._atomic_copy(plan_path, staging / PLAN_FILENAME)
        if comparison.sha256_file(staging / PLAN_FILENAME) != EXPECTED_PLAN_SHA256:
            raise AssertionError("Packaged Stage 10 plan is not byte-identical")
        safe = stage2_package._json_safe
        comparison.atomic_write_csv(
            staging / "scenario_summary.csv", [safe(row) for row in scenario_rows]
        )
        comparison.atomic_write_csv(
            staging / "paired_comparison.csv",
            [safe(row) for row in paired_summary_rows],
        )
        stage2_package._atomic_write_frame(
            staging / "paired_candidate_comparison.csv",
            pd.DataFrame(paired_candidate_rows),
        )
        comparison.atomic_write_csv(
            staging / "daywise_comparison.csv",
            [safe(row) for row in daywise_rows],
        )
        comparison.atomic_write_csv(
            staging / "train_test_comparison.csv",
            [safe(row) for row in train_test_rows],
        )
        comparison.atomic_write_csv(
            staging / "side_setup_comparison.csv",
            [safe(row) for row in side_setup_rows],
        )
        comparison.atomic_write_csv(
            staging / "validation_gates.csv", [safe(row) for row in gate_rows]
        )
        _write_json(staging / "test_results.json", test_results)
        comparison.atomic_write_text(
            staging / "decision.md",
            _decision_text(
                paths=paths,
                metrics=metrics,
                paired_summary=paired_summary_rows,
                gate_rows=gate_rows,
                test_results=test_results,
            ),
        )

        artifacts = stage2_package._recursive_artifacts(staging)
        required = {
            PLAN_FILENAME,
            "scenario_summary.csv",
            "paired_comparison.csv",
            "paired_candidate_comparison.csv",
            "daywise_comparison.csv",
            "train_test_comparison.csv",
            "side_setup_comparison.csv",
            "validation_gates.csv",
            "test_results.json",
            "decision.md",
        }
        missing = sorted(required - set(artifacts))
        if missing:
            raise AssertionError(f"Stage 10 package misses artifacts: {missing}")
        gate_counts = Counter(str(row["status"]) for row in gate_rows)
        manifest = {
            "schema_version": PACKAGE_SCHEMA_VERSION,
            "stage": STAGE,
            "decision": DECISION,
            "research_only": True,
            "promotion_eligible": False,
            "strategy_or_selection_retuning_allowed": False,
            "frozen_plan_source": str(plan_path),
            "frozen_plan_copy": PLAN_FILENAME,
            "frozen_plan_sha256": EXPECTED_PLAN_SHA256,
            "source_stage_plan": dict(plan["source_stage_plan"]),
            "frozen_inputs": dict(plan["frozen_inputs"]),
            "universe": reference_universe,
            "lineage_provenance": {
                run_id: {
                    "variant": _split_run_id(run_id)[0],
                    "source_stage": VARIANT_KEYS[_split_run_id(run_id)[0]][
                        "source_stage"
                    ],
                    "scenario": _split_run_id(run_id)[1],
                    "path": str(paths[run_id]),
                    "sha256": comparison.sha256_file(paths[run_id]),
                    "backtest_input_fingerprint": payloads[run_id].get(
                        "backtest_input_fingerprint"
                    ),
                    "selection_decisions_sha256": dict(
                        payloads[run_id].get("outputs", {})
                    ).get("selection_decisions", {}).get("sha256"),
                    "candidate_order_audit_sha256": dict(
                        payloads[run_id].get("outputs", {})
                    ).get("candidate_order_audit", {}).get("sha256"),
                }
                for run_id in RUN_ORDER
            },
            "variants": list(plan["variants"]),
            "scenarios": list(plan["scenarios"]),
            "run_matrix": list(plan["run_matrix"]),
            "validation_contract": dict(plan["validation_contract"]),
            "validation_gate_status_counts": dict(sorted(gate_counts.items())),
            "scenario_summary": [safe(row) for row in scenario_rows],
            "paired_summary": [safe(row) for row in paired_summary_rows],
            "test_evidence": test_results,
            "artifact_hash_scope": (
                "ALL_PACKAGE_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_SELF"
            ),
            "artifacts": artifacts,
        }
        _write_json(staging / "stage_manifest.json", manifest)
        if target.exists():
            raise FileExistsError(
                f"Stage 10 output directory appeared during build: {target}"
            )
        os.replace(staging, target)
        published = True
    finally:
        if not published and staging.is_dir():
            shutil.rmtree(staging)
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage-plan", type=Path, default=WORKSPACE_ROOT / PLAN_FILENAME
    )
    parser.add_argument("--v10b-reference-provenance", type=Path, required=True)
    parser.add_argument("--v10b-stress-20-2-provenance", type=Path, required=True)
    parser.add_argument("--v10b-stress-25-5-provenance", type=Path, required=True)
    parser.add_argument(
        "--move030-reference-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--move030-stress-20-2-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--move030-stress-25-5-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--move040-reference-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--move040-stress-20-2-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--move040-stress-25-5-provenance", type=Path, required=True
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument("--test-command", action="append", default=[])
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    provenance_by_run = {
        run_id: Path(getattr(args, RUN_ARGUMENTS[run_id])) for run_id in RUN_ORDER
    }
    output = build_stage10_package(
        stage_plan=args.stage_plan,
        provenance_by_run=provenance_by_run,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
