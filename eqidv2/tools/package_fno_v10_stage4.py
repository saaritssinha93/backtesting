"""Package the frozen Stage 4 RV1_100_S4 isolated-research result.

This utility consumes five completed provenance artifacts (the frozen
V8/V10B parity baseline and Stages 1 through 4), validates the complete frozen
lineage, and writes an isolated Stage 4 package plus cumulative results.  It
does not execute a backtest and never grants promotion authority.
"""

from __future__ import annotations

import argparse
import math
import sys
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


STAGE_SCHEMA_VERSION = "fno_v10_stage4_result_package_v1"
TEST_EVIDENCE_SCHEMA_VERSION = "fno_v10_stage4_test_evidence_v1"
VARIANT_ARCHIVE_SCHEMA_VERSION = "fno_v10_stage4_variant_archive_v1"
STAGE = "STAGE_04"
V8_STAGE = "STAGE_00"
STAGE1 = "STAGE_01"
STAGE2 = "STAGE_02"
STAGE3 = "STAGE_03"
V8_VARIANT = "V10B"
STAGE1_VARIANT = "V10B"
STAGE2_VARIANT = "RV1_100"
STAGE3_VARIANT = "EXPIRY_S4"
EXPECTED_VARIANT = "RV1_100_S4"
DECISION = "COMPLETED_STAGE4_ISOLATED_RESEARCH_NOT_PROMOTION"
PLAN_FILENAME = stage2_package.PLAN_FILENAME
EXPECTED_STAGE_PLAN_SHA256 = stage2_package.EXPECTED_STAGE_PLAN_SHA256
COMPARISON_DIR_NAME = "stage1_v10b_vs_stage4_rv1_100_s4_comparison"
DAYWISE_FILENAME = "daywise_comparison.csv"
SIDE_SETUP_FILENAME = "side_and_setup_comparison.csv"
VERSION_KEYS = ("V8", "STAGE1", "STAGE2", "STAGE3", "STAGE4")


def _require_file(path: Path, label: str) -> Path:
    return stage2_package._require_file(path, label)


def _path_key(path: Path) -> str:
    return stage2_package._path_key(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    stage2_package._write_json(path, payload)


def _load_stage4_plan(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    plan, stage3_entry = stage3_package._load_stage3_plan(path)
    sequence = [dict(item) for item in list(plan.get("sequence", []))]
    matches = [
        item
        for item in sequence
        if str(item.get("stage", "")).upper() == STAGE
    ]
    if len(matches) != 1:
        raise ValueError("Frozen plan must contain exactly one STAGE_04 entry")
    stage4_entry = matches[0]
    if str(stage4_entry.get("variant", "")).upper() != EXPECTED_VARIANT:
        raise ValueError("Frozen STAGE_04 variant must be RV1_100_S4")
    expected_sha = experiment_config.variant_config_sha256(EXPECTED_VARIANT)
    if stage4_entry.get("variant_config_sha256") != expected_sha:
        raise ValueError("Frozen STAGE_04 variant config SHA256 is invalid")
    if sequence.index(stage4_entry) != sequence.index(stage3_entry) + 1:
        raise ValueError("STAGE_04 must immediately follow STAGE_03")
    spec = experiment_config.get_spec(EXPECTED_VARIANT)
    if spec.confirmation_volume_ratio_min != 1.0 or spec.entry_expiry_minute != 4:
        raise ValueError("Frozen RV1_100_S4 mechanisms changed")
    return plan, stage4_entry


def _version_records(
    *,
    paths: Mapping[str, Path],
    metrics: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    identities = {
        "V8": (V8_STAGE, V8_VARIANT),
        "STAGE1": (STAGE1, STAGE1_VARIANT),
        "STAGE2": (STAGE2, STAGE2_VARIANT),
        "STAGE3": (STAGE3, STAGE3_VARIANT),
        "STAGE4": (STAGE, EXPECTED_VARIANT),
    }
    return [
        {
            "key": key,
            "stage": identities[key][0],
            "variant": identities[key][1],
            "provenance": paths[key],
            "metrics": metrics[key],
        }
        for key in VERSION_KEYS
    ]


def _all_versions_summary_rows(
    records: Sequence[Mapping[str, Any]],
    stage1_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    absolute = [
        stage3_package._absolute_summary_row(record, stage1_metrics)
        for record in records
    ]
    deltas = [
        stage3_package._delta_summary_row(record, stage1_metrics)
        for record in records
        if record["key"] != "STAGE1"
    ]
    return [*absolute, *deltas]


def _isolated_summary_rows(
    stage1_metrics: Mapping[str, Any], stage4_metrics: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rows = stage2_package._summary_rows(stage1_metrics, stage4_metrics)
    for row in rows:
        row["stage"] = STAGE
    return rows


def _cumulative_train_test_rows(
    records: Sequence[Mapping[str, Any]],
    payload_by_key: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    periods_by_key = {
        str(record["key"]): stage3_package._period_metrics(
            payload_by_key[str(record["key"])], str(record["key"])
        )
        for record in records
    }
    rows: list[dict[str, Any]] = []
    for record in records:
        key = str(record["key"])
        for period in ("TRAIN", "TEST"):
            observed = periods_by_key[key][period]
            rows.append(
                {
                    "row_type": "ABSOLUTE",
                    "version": key,
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": "STAGE1",
                    "period": period,
                    **{
                        field: observed.get(field)
                        for field in stage3_package.PERIOD_METRIC_FIELDS
                    },
                }
            )
    baseline_periods = periods_by_key["STAGE1"]
    for record in records:
        key = str(record["key"])
        if key == "STAGE1":
            continue
        for period in ("TRAIN", "TEST"):
            observed = periods_by_key[key][period]
            baseline = baseline_periods[period]
            rows.append(
                {
                    "row_type": "DELTA_VS_STAGE1",
                    "version": f"{key}_MINUS_STAGE1",
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": "STAGE1",
                    "period": period,
                    **{
                        field: stage2_package._delta_value(
                            observed.get(field), baseline.get(field)
                        )
                        for field in stage3_package.PERIOD_METRIC_FIELDS
                    },
                }
            )
    return rows


def _cumulative_daywise_frame(
    payload_by_key: Mapping[str, Mapping[str, Any]]
) -> pd.DataFrame:
    daily_by_key = {
        key: stage3_package._read_daily(payload_by_key[key], key)
        for key in VERSION_KEYS
    }
    baseline = daily_by_key["STAGE1"]
    baseline_dates = baseline["session_date"].astype(str).tolist()
    baseline_periods = baseline["period"].astype(str).tolist()
    out = baseline[["session_date", "period"]].copy()
    value_fields = ("candidates", "fills", "net_return_pct", "net_pnl_rs")
    for key in VERSION_KEYS:
        daily = daily_by_key[key]
        if daily["session_date"].astype(str).tolist() != baseline_dates:
            raise ValueError(f"{key} daily sessions differ from Stage 1")
        if daily["period"].astype(str).tolist() != baseline_periods:
            raise ValueError(f"{key} daily TRAIN/TEST labels differ from Stage 1")
        prefix = key.lower()
        for field in value_fields:
            out[f"{prefix}_{field}"] = daily[field].to_numpy()
        out[f"{prefix}_cumulative_net_return_points"] = daily[
            "net_return_pct"
        ].cumsum()
    for key in VERSION_KEYS:
        if key == "STAGE1":
            continue
        prefix = key.lower()
        for field in value_fields:
            out[f"delta_{prefix}_vs_stage1_{field}"] = (
                out[f"{prefix}_{field}"] - out[f"stage1_{field}"]
            )
        out[f"delta_{prefix}_vs_stage1_cumulative_net_return_points"] = (
            out[f"{prefix}_cumulative_net_return_points"]
            - out["stage1_cumulative_net_return_points"]
        )
    return out


def _stage4_gate_rows(
    *,
    plan: Mapping[str, Any],
    stage1_metrics: Mapping[str, Any],
    stage4_metrics: Mapping[str, Any],
    selection_rows: int,
) -> list[dict[str, Any]]:
    integrity = dict(plan["stage_02_integrity_gates"])
    screen = dict(plan["stage_02_preliminary_performance_screen"])
    dd_ratio, fill_retention = stage3_package._dd_and_fill_ratios(
        stage4_metrics, stage1_metrics
    )
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

    add("INTEGRITY", "valid_provenance_required", "EQ", True, True, "PASS", "All five provenances validated")
    add("INTEGRITY", "expected_variant", "EQ", EXPECTED_VARIANT, EXPECTED_VARIANT, "PASS", "Frozen sequence identity")
    add("INTEGRITY", "expected_base_candidates", "EQ", integrity["expected_base_candidates"], int(stage4_metrics["candidates"]), stage2_package._gate_status(stage4_metrics["candidates"], "EQ", integrity["expected_base_candidates"]), "Entry-only combination retains V10B selection")
    add("INTEGRITY", "all_selection_decisions_must_pass", "EQ", integrity["expected_base_candidates"], selection_rows, stage2_package._gate_status(selection_rows, "EQ", integrity["expected_base_candidates"]), "All decisions were PASSED")
    add("INTEGRITY", "expected_selection_overlay", "EQ", integrity["expected_selection_overlay"], integrity["expected_selection_overlay"], "PASS", "BASE_V10B_SELECTION retained")
    add("INTEGRITY", "expected_confirmation_volume_ratio_min", "EQ", 1.0, 1.0, "PASS", "Frozen RV1_100 mechanism")
    add("INTEGRITY", "expected_entry_expiry_minute", "EQ", 4, 4, "PASS", "Frozen S+4 mechanism")
    add("INTEGRITY", "same_frozen_inputs_as_control", "EQ", True, True, "PASS", "Runner, cache, snapshot, window, costs, universe and portfolio validated")
    add("INTEGRITY", "full_state_machine_replay_required", "EQ", True, True, "PASS", "Full candidate audit is bound")

    performance = (
        ("minimum_fills", "GE", screen["minimum_fills"], stage4_metrics["fills"]),
        ("minimum_active_sessions", "GE", screen["minimum_active_sessions"], stage4_metrics["active_sessions"]),
        ("minimum_profit_factor", "GE", screen["minimum_profit_factor"], stage4_metrics["profit_factor"]),
        ("minimum_profit_factor_excluding_best_net_day", "GE", screen["minimum_profit_factor_excluding_best_net_day"], stage4_metrics["profit_factor_excluding_best_net_day"]),
        ("maximum_best_positive_day_share_pct", "LE", screen["maximum_best_positive_day_share_pct"], stage4_metrics["best_positive_day_share_pct"]),
        ("minimum_long_profit_factor", "GE", screen["minimum_long_profit_factor"], stage4_metrics["long_profit_factor"]),
        ("minimum_short_profit_factor", "GE", screen["minimum_short_profit_factor"], stage4_metrics["short_profit_factor"]),
        ("maximum_drawdown_as_pct_of_control", "LE", screen["maximum_drawdown_as_pct_of_control"], dd_ratio),
    )
    for gate, comparator_name, threshold, observed in performance:
        add("PRELIMINARY_PERFORMANCE", gate, comparator_name, threshold, observed, stage2_package._gate_status(observed, comparator_name, threshold), "Research screen only; not promotion authority")
    add("REQUIRED_REPORTING", "accuracy_pct_reported", "PRESENT", True, stage4_metrics["win_rate_pct"], "PASS" if stage2_package._is_evaluable(stage4_metrics["win_rate_pct"]) else "FAIL", "Accuracy is reported")
    add("REQUIRED_REPORTING", "fill_retention_pct_reported", "PRESENT", True, fill_retention, "PASS" if stage2_package._is_evaluable(fill_retention) else "FAIL", "Fill retention is reported")
    add("STRESS", "stress_20bps_plus_2bps", "DEFERRED", screen["stress_20bps_plus_2bps"], "DEFERRED_NOT_EVALUATED", "DEFERRED", "No stressed backtest was authorized in Stage 4")
    add("AUTHORITY", "screen_is_promotion_authority", "EQ", False, False, "PASS", "Gate results cannot promote this research variant")
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
    payload["schema_version"] = TEST_EVIDENCE_SCHEMA_VERSION
    return payload


def build_stage4_package(
    *,
    stage_plan: Path,
    v8_provenance: Path,
    stage1_control_provenance: Path,
    stage2_rv1_provenance: Path,
    stage3_expiry_provenance: Path,
    stage4_challenger_provenance: Path,
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"Stage 4 output directory already exists: {target}")
    plan_path = _require_file(stage_plan, "Frozen experiment plan")
    paths = {
        "V8": _require_file(v8_provenance, "Frozen V8/V10B provenance"),
        "STAGE1": _require_file(stage1_control_provenance, "Stage 1 V10B provenance"),
        "STAGE2": _require_file(stage2_rv1_provenance, "Stage 2 RV1_100 provenance"),
        "STAGE3": _require_file(stage3_expiry_provenance, "Stage 3 EXPIRY_S4 provenance"),
        "STAGE4": _require_file(stage4_challenger_provenance, "Stage 4 RV1_100_S4 provenance"),
    }
    if len({_path_key(path) for path in paths.values()}) != len(paths):
        raise ValueError("All five provenance paths must be different")

    plan, stage4_entry = _load_stage4_plan(plan_path)
    stage1_manifest_path, stage1_manifest = stage3_package._validate_stage1_v8_binding(
        plan=plan, v8_path=paths["V8"], stage1_path=paths["STAGE1"]
    )
    payloads = {
        key: comparison.validate_provenance(path) for key, path in paths.items()
    }
    stage3_package._require_v8_identity(payloads["V8"], "V8")
    identities = {
        "STAGE1": STAGE1_VARIANT,
        "STAGE2": STAGE2_VARIANT,
        "STAGE3": STAGE3_VARIANT,
        "STAGE4": EXPECTED_VARIANT,
    }
    config_hashes = {
        key: experiment_config.variant_config_sha256(variant)
        for key, variant in identities.items()
    }
    if config_hashes["STAGE4"] != stage4_entry["variant_config_sha256"]:
        raise ValueError("Stage 4 config hash differs from frozen plan")
    for key, variant in identities.items():
        stage2_package._require_experiment_identity(
            payloads[key],
            variant=variant,
            variant_config_sha256=config_hashes[key],
            label=key,
        )
    stage3_package._validate_v8_against_stage1(
        plan=plan, v8=payloads["V8"], stage1=payloads["STAGE1"]
    )

    snapshot_path: Path | None = None
    expected_window: dict[str, Any] | None = None
    for key in ("STAGE2", "STAGE3", "STAGE4"):
        observed_snapshot, observed_window = stage2_package._validate_frozen_inputs(
            plan=plan,
            control=payloads["STAGE1"],
            challenger=payloads[key],
            stage1_manifest=stage1_manifest,
        )
        comparison.challenger_invariant_rows(payloads["STAGE1"], payloads[key])
        if snapshot_path is None:
            snapshot_path = observed_snapshot
            expected_window = observed_window
        elif _path_key(snapshot_path) != _path_key(observed_snapshot):
            raise ValueError(f"{key} source snapshot differs from prior stages")
        elif expected_window != observed_window:
            raise ValueError(f"{key} backtest window differs from prior stages")
    if snapshot_path is None or expected_window is None:
        raise AssertionError("Stage 4 frozen-input validation did not execute")

    integrity = dict(plan["stage_02_integrity_gates"])
    expected_candidates = int(integrity["expected_base_candidates"])
    expected_overlay = str(integrity["expected_selection_overlay"])
    audits: dict[str, pd.DataFrame] = {
        "V8": stage3_package._load_v8_audit(
            payloads["V8"], expected_candidates
        )
    }
    decisions: dict[str, pd.DataFrame] = {}
    for key in ("STAGE1", "STAGE2", "STAGE3", "STAGE4"):
        decisions[key], audits[key] = stage2_package._validate_selection_and_audit(
            payloads[key],
            variant=identities[key],
            variant_config_sha256=config_hashes[key],
            expected_candidates=expected_candidates,
            expected_overlay=expected_overlay,
            label=key,
        )
    base_ids = decisions["STAGE1"]["candidate_id"].astype(str).tolist()
    if audits["V8"]["candidate_id"].astype(str).tolist() != base_ids:
        raise ValueError("V8 and Stage 1 ordered candidate sets differ")
    for key in ("STAGE2", "STAGE3", "STAGE4"):
        if decisions[key]["candidate_id"].astype(str).tolist() != base_ids:
            raise ValueError(f"{key} did not retain ordered Stage 1 candidates")
    stage3_package._require_chronology(
        tuple((key, payloads[key]) for key in VERSION_KEYS)
    )

    metrics = {
        key: stage2_package._metric_bundle(payloads[key], audits[key], label=key)
        for key in VERSION_KEYS
    }
    records = _version_records(paths=paths, metrics=metrics)
    isolated_summary = _isolated_summary_rows(metrics["STAGE1"], metrics["STAGE4"])
    all_versions_summary = _all_versions_summary_rows(records, metrics["STAGE1"])
    train_test_rows = _cumulative_train_test_rows(records, payloads)
    cumulative_daywise = _cumulative_daywise_frame(payloads)
    stage4_filled = stage2_package._filled_rows(audits["STAGE4"], "Stage 4")
    funnel_rows = stage2_package._funnel_rows(
        decisions["STAGE4"], audits["STAGE4"], stage4_filled
    )
    for row in funnel_rows:
        row["stage"] = STAGE
        row["variant"] = EXPECTED_VARIANT
    gate_rows = _stage4_gate_rows(
        plan=plan,
        stage1_metrics=metrics["STAGE1"],
        stage4_metrics=metrics["STAGE4"],
        selection_rows=len(decisions["STAGE4"]),
    )
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )

    comparison_dir = target / COMPARISON_DIR_NAME
    comparison.build_comparison(
        control_provenance=paths["STAGE1"],
        challenger_provenance=paths["STAGE4"],
        output_dir=comparison_dir,
        require_control_parity=False,
    )

    plan_copy_path = target / PLAN_FILENAME
    stage2_package._atomic_copy(plan_path, plan_copy_path)
    if comparison.sha256_file(plan_copy_path) != EXPECTED_STAGE_PLAN_SHA256:
        raise AssertionError("Frozen experiment plan copy is not byte-identical")
    variant_archive = {
        "schema_version": VARIANT_ARCHIVE_SCHEMA_VERSION,
        "stage": STAGE,
        "variant": EXPECTED_VARIANT,
        "mechanism": stage4_entry.get("mechanism"),
        "frozen_plan_sha256": EXPECTED_STAGE_PLAN_SHA256,
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": config_hashes["STAGE4"],
        "variant_config": experiment_config.variant_config_payload(EXPECTED_VARIANT),
        "resolved_entry_policy": stage2_package._parameters(payloads["STAGE4"]).get("entry_policy"),
        "lineage_provenance": {
            key: {
                "path": str(paths[key]),
                "sha256": comparison.sha256_file(paths[key]),
            }
            for key in VERSION_KEYS
        },
        "research_only": True,
        "promotion_eligible": False,
    }
    _write_json(target / "variant_config.json", variant_archive)
    comparison.atomic_write_csv(target / "summary.csv", [stage2_package._json_safe(row) for row in isolated_summary])
    comparison.atomic_write_csv(target / "all_versions_summary.csv", [stage2_package._json_safe(row) for row in all_versions_summary])
    comparison.atomic_write_csv(target / "cumulative_train_test.csv", [stage2_package._json_safe(row) for row in train_test_rows])
    stage2_package._atomic_write_frame(target / "cumulative_daywise.csv", cumulative_daywise)
    stage2_package._atomic_copy(comparison_dir / DAYWISE_FILENAME, target / DAYWISE_FILENAME)
    stage2_package._atomic_copy(comparison_dir / "side_and_leg_comparison.csv", target / SIDE_SETUP_FILENAME)
    comparison.atomic_write_csv(target / "funnel.csv", [stage2_package._json_safe(row) for row in funnel_rows])
    stage2_package._atomic_write_frame(target / "filled_trade_ledger.csv", stage4_filled)
    comparison.atomic_write_csv(target / "preliminary_gates.csv", [stage2_package._json_safe(row) for row in gate_rows])
    _write_json(target / "test_results.json", test_results)

    gate_counts = Counter(str(row["status"]) for row in gate_rows)
    dd_ratio, fill_retention = stage3_package._dd_and_fill_ratios(
        metrics["STAGE4"], metrics["STAGE1"]
    )
    tests_line = (
        "not recorded"
        if test_results["tests_total"] is None
        else f"{test_results['tests_passed']} passed / {test_results['tests_failed']} failed"
    )
    decision_text = f"""# FNO V10 Stage 4 - RV1_100_S4 isolated experiment

- Decision: **{DECISION}**
- V8 baseline: `{paths['V8']}`
- Stage 1 V10B: `{paths['STAGE1']}`
- Stage 2 RV1_100: `{paths['STAGE2']}`
- Stage 3 EXPIRY_S4: `{paths['STAGE3']}`
- Stage 4 RV1_100_S4: `{paths['STAGE4']}`
- Frozen plan SHA256: `{EXPECTED_STAGE_PLAN_SHA256}`
- Variant config SHA256: `{config_hashes['STAGE4']}`
- Window: {expected_window['from_day']} through {expected_window['through_day']}
- Candidates retained: {int(metrics['STAGE4']['candidates'])} / {expected_candidates}
- Fills / active sessions: {int(metrics['STAGE4']['fills'])} / {int(metrics['STAGE4']['active_sessions'])}
- Accuracy: {float(metrics['STAGE4']['win_rate_pct']):.6f}%
- Fill retention versus Stage 1: {fill_retention:.6f}%
- Profit factor: {float(metrics['STAGE4']['profit_factor']):.12f}
- Profit factor excluding best net day: {float(metrics['STAGE4']['profit_factor_excluding_best_net_day']):.12f}
- Best positive day share: {float(metrics['STAGE4']['best_positive_day_share_pct']):.6f}%
- LONG / SHORT profit factor: {float(metrics['STAGE4']['long_profit_factor']):.12f} / {float(metrics['STAGE4']['short_profit_factor']):.12f}
- Drawdown versus Stage 1: {dd_ratio:.6f}%
- Preliminary gates: {gate_counts.get('PASS', 0)} PASS / {gate_counts.get('FAIL', 0)} FAIL / {gate_counts.get('DEFERRED', 0)} DEFERRED
- Stress 20bps + 2bps: **DEFERRED_NOT_EVALUATED**
- Test evidence: {tests_line}

This package records completion of the frozen Stage 4 combination and keeps
the full V8-through-Stage-4 comparison. Gate outcomes are research screens,
do not alter this completion-only decision, and do not authorize promotion.
"""
    comparison.atomic_write_text(target / "decision.md", decision_text)

    artifacts = stage2_package._recursive_artifacts(target)
    required_top_level = {
        PLAN_FILENAME,
        "variant_config.json",
        "summary.csv",
        "all_versions_summary.csv",
        "cumulative_train_test.csv",
        "cumulative_daywise.csv",
        DAYWISE_FILENAME,
        SIDE_SETUP_FILENAME,
        "funnel.csv",
        "filled_trade_ledger.csv",
        "preliminary_gates.csv",
        "test_results.json",
        "decision.md",
    }
    missing = sorted(required_top_level - set(artifacts))
    if missing:
        raise AssertionError(f"Stage 4 package misses top-level artifacts: {missing}")
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
        "stage1_package_manifest": str(stage1_manifest_path),
        "stage1_package_manifest_sha256": comparison.sha256_file(stage1_manifest_path),
        "lineage_provenance": {
            key: {
                "stage": next(record["stage"] for record in records if record["key"] == key),
                "variant": next(record["variant"] for record in records if record["key"] == key),
                "path": str(paths[key]),
                "sha256": comparison.sha256_file(paths[key]),
                "backtest_input_fingerprint": payloads[key].get("backtest_input_fingerprint"),
            }
            for key in VERSION_KEYS
        },
        "source_snapshot_manifest": str(snapshot_path),
        "source_snapshot_manifest_sha256": comparison.sha256_file(snapshot_path),
        "source_snapshot_fingerprint": dict(payloads["STAGE4"].get("source_snapshot", {})).get("snapshot_fingerprint"),
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": config_hashes["STAGE4"],
        "experiment_runner_source_sha256": payloads["STAGE4"].get("experiment_runner_source_sha256"),
        "experiment_config_source_sha256": payloads["STAGE4"].get("experiment_config_source_sha256"),
        "cache_input_fingerprint": payloads["STAGE4"].get("cache_input_fingerprint"),
        "backtest_window": expected_window,
        "fixed_economics_contract": comparison.fixed_economics_contract(payloads["STAGE4"]),
        "comparison_directory": COMPARISON_DIR_NAME,
        "isolated_summary": isolated_summary,
        "all_versions_summary": all_versions_summary,
        "preliminary_gate_status_counts": dict(sorted(gate_counts.items())),
        "stress_status": "DEFERRED_NOT_EVALUATED",
        "test_evidence": test_results,
        "artifact_hash_scope": "ALL_PACKAGE_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_SELF",
        "artifacts": artifacts,
    }
    _write_json(target / "stage_manifest.json", manifest)
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-plan", type=Path, default=WORKSPACE_ROOT / PLAN_FILENAME)
    parser.add_argument("--v8-provenance", "--v8-control-provenance", dest="v8_provenance", type=Path, required=True)
    parser.add_argument("--stage1-control-provenance", type=Path, required=True)
    parser.add_argument("--stage2-rv1-provenance", "--stage2-provenance", dest="stage2_rv1_provenance", type=Path, required=True)
    parser.add_argument("--stage3-expiry-provenance", "--stage3-provenance", dest="stage3_expiry_provenance", type=Path, required=True)
    parser.add_argument("--stage4-challenger-provenance", "--challenger-provenance", dest="stage4_challenger_provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument("--test-command", action="append", default=[], help="Repeat to record each validation/test command.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_stage4_package(
        stage_plan=args.stage_plan,
        v8_provenance=args.v8_provenance,
        stage1_control_provenance=args.stage1_control_provenance,
        stage2_rv1_provenance=args.stage2_rv1_provenance,
        stage3_expiry_provenance=args.stage3_expiry_provenance,
        stage4_challenger_provenance=args.stage4_challenger_provenance,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
