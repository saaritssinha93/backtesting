"""Package the frozen Stage 3 EXPIRY_S4 isolated-research result.

The utility consumes four already-completed provenance artifacts: the frozen
V8/V10B baseline, the accepted Stage 1 experiment-runner V10B control, the
Stage 2 RV1_100 run, and the Stage 3 EXPIRY_S4 challenger.  It does not run a
backtest.  It validates the frozen sequence and complete lineage before
publishing isolated Stage 3 and cumulative V8-through-Stage-3 artifacts.

All decisions are research-only completion records.  Preliminary gates are
reported individually and have no promotion authority; the frozen stress gate
remains explicitly deferred.
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


STAGE_SCHEMA_VERSION = "fno_v10_stage3_result_package_v1"
TEST_EVIDENCE_SCHEMA_VERSION = "fno_v10_stage3_test_evidence_v1"
VARIANT_ARCHIVE_SCHEMA_VERSION = "fno_v10_stage3_variant_archive_v1"
STAGE = "STAGE_03"
V8_STAGE = "STAGE_00"
STAGE1 = "STAGE_01"
STAGE2 = "STAGE_02"
V8_VARIANT = "V10B"
STAGE1_VARIANT = "V10B"
STAGE2_VARIANT = "RV1_100"
EXPECTED_VARIANT = "EXPIRY_S4"
DECISION = "COMPLETED_STAGE3_ISOLATED_RESEARCH_NOT_PROMOTION"
PLAN_FILENAME = stage2_package.PLAN_FILENAME
EXPECTED_STAGE_PLAN_SHA256 = stage2_package.EXPECTED_STAGE_PLAN_SHA256
COMPARISON_DIR_NAME = "stage1_v10b_vs_stage3_expiry_s4_comparison"
DAYWISE_FILENAME = "daywise_comparison.csv"
SIDE_SETUP_FILENAME = "side_and_setup_comparison.csv"

SUMMARY_METRIC_FIELDS = (
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
PERIOD_METRIC_FIELDS = (
    "sessions",
    "fills",
    "positive_days",
    "profit_factor",
    "net_return_percentage_points",
)


def _require_file(path: Path, label: str) -> Path:
    return stage2_package._require_file(path, label)


def _path_key(path: Path) -> str:
    return stage2_package._path_key(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    stage2_package._write_json(path, payload)


def _load_stage3_plan(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    plan, stage2_entry = stage2_package._load_frozen_plan(path)
    sequence = [dict(item) for item in list(plan.get("sequence", []))]
    matches = [
        item
        for item in sequence
        if str(item.get("stage", "")).upper() == STAGE
    ]
    if len(matches) != 1:
        raise ValueError("Frozen plan must contain exactly one STAGE_03 entry")
    stage3_entry = matches[0]
    if str(stage3_entry.get("variant", "")).upper() != EXPECTED_VARIANT:
        raise ValueError("Frozen STAGE_03 variant must be EXPIRY_S4")
    expected_sha = experiment_config.variant_config_sha256(EXPECTED_VARIANT)
    if stage3_entry.get("variant_config_sha256") != expected_sha:
        raise ValueError("Frozen STAGE_03 variant config SHA256 is invalid")
    stage2_index = sequence.index(stage2_entry)
    stage3_index = sequence.index(stage3_entry)
    if stage3_index != stage2_index + 1:
        raise ValueError("STAGE_03 must immediately follow STAGE_02 in frozen plan")
    return plan, stage3_entry


def _validate_stage1_v8_binding(
    *,
    plan: Mapping[str, Any],
    v8_path: Path,
    stage1_path: Path,
) -> tuple[Path, dict[str, Any]]:
    manifest_path, manifest = stage2_package._validate_stage1_package_binding(
        plan, stage1_path
    )
    planned_v8 = _require_file(
        Path(str(manifest.get("stage0_control_provenance", ""))),
        "Stage 1 package V8 provenance",
    )
    stage2_package._require_same_path(
        v8_path, planned_v8, "V8 provenance bound by Stage 1 package"
    )
    v8_sha = comparison.sha256_file(v8_path)
    if manifest.get("stage0_control_provenance_sha256") != v8_sha:
        raise ValueError("V8 provenance SHA256 differs from Stage 1 package")
    return manifest_path, manifest


def _require_v8_identity(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("v10_experiment_run_schema_version"):
        raise ValueError(f"{label} cannot be an experiment-run provenance")
    if payload.get("v10_run_schema_version") != comparison.v10.V10_RUN_SCHEMA_VERSION:
        raise ValueError(f"{label} is not a supported V10 unified provenance")
    if stage2_package._parameters_variant(payload) != V8_VARIANT:
        raise ValueError(f"{label} variant must be V10B")
    if payload.get("backtest_input_fingerprint") != (
        comparison.STAGE0_V10B_INPUT_FINGERPRINT
    ):
        raise ValueError(f"{label} is not the frozen V8/V10B run")
    if payload.get("research_only") is not True:
        raise ValueError(f"{label} must remain research-only")
    if payload.get("promotion_eligible") is not False:
        raise ValueError(f"{label} cannot be promotion-eligible")
    if dict(payload.get("results", {})).get("promotion_eligible") is not False:
        raise ValueError(f"{label} results cannot be promotion-eligible")
    if "V8_COMBINED_ECONOMIC_PARITY_BASELINE" not in str(
        payload.get("objective", "")
    ):
        raise ValueError(f"{label} objective is not the V8 parity baseline")
    comparison.assert_anchor_metrics(
        comparison.aggregate_row(label, payload), label=label
    )


def _validate_v8_against_stage1(
    *,
    plan: Mapping[str, Any],
    v8: Mapping[str, Any],
    stage1: Mapping[str, Any],
) -> None:
    frozen = dict(plan.get("frozen_inputs", {}))
    source_path = _require_file(
        Path(str(frozen.get("source_snapshot_manifest", ""))),
        "Frozen source snapshot manifest",
    )
    for label, payload in (("V8", v8), ("Stage 1", stage1)):
        source = dict(payload.get("source_snapshot", {}))
        observed = _require_file(
            Path(str(source.get("manifest_path", ""))),
            f"{label} source snapshot",
        )
        stage2_package._require_same_path(
            observed, source_path, f"{label} source snapshot"
        )
        if source.get("snapshot_fingerprint") != frozen.get(
            "source_snapshot_fingerprint"
        ):
            raise ValueError(f"{label} source snapshot fingerprint differs")
    if comparison.sha256_file(source_path) != frozen.get(
        "source_snapshot_manifest_sha256"
    ):
        raise ValueError("Frozen source snapshot manifest SHA256 differs")
    if comparison.execution_contract(v8) != comparison.execution_contract(stage1):
        raise ValueError("V8 and Stage 1 execution contracts differ")
    if dict(v8.get("backtest_window", {})) != dict(
        stage1.get("backtest_window", {})
    ):
        raise ValueError("V8 and Stage 1 windows differ")
    if dict(v8.get("universe", {})) != dict(stage1.get("universe", {})):
        raise ValueError("V8 and Stage 1 universes differ")
    for field in (
        "v10_unified_contract_sha256",
        "neutral_engine_source_sha256",
        "launcher_source_sha256",
        "strategy_source_sha256",
    ):
        if not v8.get(field) or v8.get(field) != stage1.get(field):
            raise ValueError(f"V8 and Stage 1 {field} differ")
    parity = comparison.core_audit_parity(v8, stage1)
    if parity.get("parity") is not True:
        raise ValueError(f"V8 and Stage 1 audit parity failed: {parity}")
    changed = [
        row["artifact"]
        for row in comparison.artifact_parity_rows(v8, stage1)
        if not row["byte_identical"]
    ]
    if changed:
        raise ValueError(f"V8 and Stage 1 parity artifacts differ: {changed}")


def _load_v8_audit(
    payload: Mapping[str, Any], expected_candidates: int
) -> pd.DataFrame:
    audit = pd.read_csv(
        comparison.output_path(payload, "candidate_order_audit"),
        low_memory=False,
    )
    required = {
        "candidate_id",
        "session_date",
        "setup_id",
        "side",
        "status",
        "filled",
        "net_return_pct",
        "net_pnl_rs",
        "portfolio_mode",
    }
    missing = sorted(required - set(audit.columns))
    if missing:
        raise ValueError(f"V8 audit misses columns: {missing}")
    if len(audit) != expected_candidates:
        raise ValueError(
            f"V8 audit rows differ: {len(audit)} != {expected_candidates}"
        )
    if int(dict(payload.get("results", {})).get("candidates", -1)) != (
        expected_candidates
    ):
        raise ValueError("V8 provenance candidate count differs")
    ids = audit["candidate_id"].astype(str)
    if ids.eq("").any() or ids.duplicated().any():
        raise ValueError("V8 audit candidate IDs are blank or duplicated")
    expected_portfolio = str(
        stage2_package._parameters(payload).get("portfolio_mode", "")
    )
    if not expected_portfolio or not audit["portfolio_mode"].astype(str).eq(
        expected_portfolio
    ).all():
        raise ValueError("V8 audit portfolio mode differs from provenance")
    return audit


def _require_chronology(
    payloads: Sequence[tuple[str, Mapping[str, Any]]]
) -> None:
    observed: list[tuple[str, pd.Timestamp]] = []
    for label, payload in payloads:
        raw = str(payload.get("generated_at_ist", ""))
        timestamp = pd.to_datetime(raw, errors="coerce", utc=True)
        if pd.isna(timestamp):
            raise ValueError(f"{label} generated_at_ist is invalid")
        observed.append((label, timestamp))
    for (left_label, left), (right_label, right) in zip(observed, observed[1:]):
        if left > right:
            raise ValueError(
                f"Frozen stage chronology is reversed: {left_label} > {right_label}"
            )


def _version_records(
    *,
    v8_path: Path,
    stage1_path: Path,
    stage2_path: Path,
    stage3_path: Path,
    v8_metrics: Mapping[str, Any],
    stage1_metrics: Mapping[str, Any],
    stage2_metrics: Mapping[str, Any],
    stage3_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "key": "V8",
            "stage": V8_STAGE,
            "variant": V8_VARIANT,
            "provenance": v8_path,
            "metrics": v8_metrics,
        },
        {
            "key": "STAGE1",
            "stage": STAGE1,
            "variant": STAGE1_VARIANT,
            "provenance": stage1_path,
            "metrics": stage1_metrics,
        },
        {
            "key": "STAGE2",
            "stage": STAGE2,
            "variant": STAGE2_VARIANT,
            "provenance": stage2_path,
            "metrics": stage2_metrics,
        },
        {
            "key": "STAGE3",
            "stage": STAGE,
            "variant": EXPECTED_VARIANT,
            "provenance": stage3_path,
            "metrics": stage3_metrics,
        },
    ]


def _dd_and_fill_ratios(
    metrics: Mapping[str, Any], stage1_metrics: Mapping[str, Any]
) -> tuple[float, float]:
    baseline_dd = float(stage1_metrics["calculated_max_daily_drawdown_points"])
    observed_dd = float(metrics["calculated_max_daily_drawdown_points"])
    if baseline_dd > 0:
        dd_ratio = 100.0 * observed_dd / baseline_dd
    elif observed_dd == 0:
        dd_ratio = 100.0
    else:
        dd_ratio = math.inf
    baseline_fills = int(stage1_metrics["fills"])
    fill_retention = (
        100.0 * int(metrics["fills"]) / baseline_fills
        if baseline_fills > 0
        else math.nan
    )
    return dd_ratio, fill_retention


def _absolute_summary_row(
    record: Mapping[str, Any], stage1_metrics: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = dict(record["metrics"])
    dd_ratio, fill_retention = _dd_and_fill_ratios(metrics, stage1_metrics)
    provenance = Path(record["provenance"])
    return {
        "row_type": "ABSOLUTE",
        "version": record["key"],
        "stage": record["stage"],
        "variant": record["variant"],
        "baseline": "STAGE1",
        "from_day": metrics.get("from_day"),
        "through_day": metrics.get("through_day"),
        **{field: metrics.get(field) for field in SUMMARY_METRIC_FIELDS},
        "excluded_best_net_day": metrics.get("excluded_best_net_day"),
        "drawdown_as_pct_of_stage1": dd_ratio,
        "fill_retention_pct_vs_stage1": fill_retention,
        "provenance": str(provenance),
        "provenance_sha256": comparison.sha256_file(provenance),
        "research_only": True,
        "promotion_eligible": False,
    }


def _delta_summary_row(
    record: Mapping[str, Any], stage1_metrics: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = dict(record["metrics"])
    dd_ratio, fill_retention = _dd_and_fill_ratios(metrics, stage1_metrics)
    return {
        "row_type": "DELTA_VS_STAGE1",
        "version": f"{record['key']}_MINUS_STAGE1",
        "stage": record["stage"],
        "variant": record["variant"],
        "baseline": "STAGE1",
        "from_day": metrics.get("from_day"),
        "through_day": metrics.get("through_day"),
        **{
            field: stage2_package._delta_value(
                metrics.get(field), stage1_metrics.get(field)
            )
            for field in SUMMARY_METRIC_FIELDS
        },
        "excluded_best_net_day": "",
        "drawdown_as_pct_of_stage1": dd_ratio - 100.0,
        "fill_retention_pct_vs_stage1": fill_retention - 100.0,
        "provenance": "",
        "provenance_sha256": "",
        "research_only": True,
        "promotion_eligible": False,
    }


def _all_versions_summary_rows(
    records: Sequence[Mapping[str, Any]],
    stage1_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    absolute = [
        _absolute_summary_row(record, stage1_metrics) for record in records
    ]
    deltas = [
        _delta_summary_row(record, stage1_metrics)
        for record in records
        if record["key"] != "STAGE1"
    ]
    return [*absolute, *deltas]


def _isolated_summary_rows(
    stage1_metrics: Mapping[str, Any], stage3_metrics: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rows = stage2_package._summary_rows(stage1_metrics, stage3_metrics)
    for row in rows:
        row["stage"] = STAGE
    return rows


def _period_metrics(
    payload: Mapping[str, Any], label: str
) -> dict[str, dict[str, Any]]:
    periods = dict(dict(payload.get("results", {})).get("period_metrics", {}))
    if set(periods) != {"TRAIN", "TEST"}:
        raise ValueError(f"{label} period metrics must contain TRAIN and TEST")
    normalized: dict[str, dict[str, Any]] = {}
    for period in ("TRAIN", "TEST"):
        row = dict(periods[period])
        missing = sorted(set(PERIOD_METRIC_FIELDS) - set(row))
        if missing:
            raise ValueError(f"{label} {period} metrics miss fields: {missing}")
        normalized[period] = row
    return normalized


def _cumulative_train_test_rows(
    records: Sequence[Mapping[str, Any]],
    payload_by_key: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    periods_by_key = {
        record["key"]: _period_metrics(
            payload_by_key[str(record["key"])], str(record["key"])
        )
        for record in records
    }
    rows: list[dict[str, Any]] = []
    for record in records:
        for period in ("TRAIN", "TEST"):
            metrics = periods_by_key[str(record["key"])][period]
            rows.append(
                {
                    "row_type": "ABSOLUTE",
                    "version": record["key"],
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": "STAGE1",
                    "period": period,
                    **{field: metrics.get(field) for field in PERIOD_METRIC_FIELDS},
                }
            )
    stage1_periods = periods_by_key["STAGE1"]
    for record in records:
        if record["key"] == "STAGE1":
            continue
        for period in ("TRAIN", "TEST"):
            metrics = periods_by_key[str(record["key"])][period]
            baseline = stage1_periods[period]
            rows.append(
                {
                    "row_type": "DELTA_VS_STAGE1",
                    "version": f"{record['key']}_MINUS_STAGE1",
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": "STAGE1",
                    "period": period,
                    **{
                        field: stage2_package._delta_value(
                            metrics.get(field), baseline.get(field)
                        )
                        for field in PERIOD_METRIC_FIELDS
                    },
                }
            )
    return rows


def _read_daily(payload: Mapping[str, Any], label: str) -> pd.DataFrame:
    daily = pd.read_csv(comparison.output_path(payload, "daily"))
    required = {
        "session_date",
        "candidates",
        "net_return_pct",
        "net_pnl_rs",
        "fills",
        "period",
    }
    missing = sorted(required - set(daily.columns))
    if missing:
        raise ValueError(f"{label} daily artifact misses columns: {missing}")
    if daily["session_date"].astype(str).duplicated().any():
        raise ValueError(f"{label} daily artifact has duplicate sessions")
    for field in ("candidates", "net_return_pct", "net_pnl_rs", "fills"):
        daily[field] = pd.to_numeric(daily[field], errors="coerce")
        if daily[field].isna().any():
            raise ValueError(f"{label} daily artifact has invalid {field}")
    return daily.sort_values("session_date", kind="stable").reset_index(drop=True)


def _cumulative_daywise_frame(
    payload_by_key: Mapping[str, Mapping[str, Any]]
) -> pd.DataFrame:
    daily_by_key = {
        key: _read_daily(payload, key) for key, payload in payload_by_key.items()
    }
    baseline = daily_by_key["STAGE1"]
    baseline_dates = baseline["session_date"].astype(str).tolist()
    baseline_periods = baseline["period"].astype(str).tolist()
    out = baseline[["session_date", "period"]].copy()
    value_fields = ("candidates", "fills", "net_return_pct", "net_pnl_rs")
    for key in ("V8", "STAGE1", "STAGE2", "STAGE3"):
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
    for key in ("V8", "STAGE2", "STAGE3"):
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


def _stage3_gate_rows(
    *,
    plan: Mapping[str, Any],
    stage1_metrics: Mapping[str, Any],
    stage3_metrics: Mapping[str, Any],
    selection_rows: int,
) -> list[dict[str, Any]]:
    integrity = dict(plan["stage_02_integrity_gates"])
    screen = dict(plan["stage_02_preliminary_performance_screen"])
    dd_ratio, fill_retention = _dd_and_fill_ratios(stage3_metrics, stage1_metrics)
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

    add("INTEGRITY", "valid_provenance_required", "EQ", True, True, "PASS", "All four provenances validated")
    add("INTEGRITY", "expected_variant", "EQ", EXPECTED_VARIANT, EXPECTED_VARIANT, "PASS", "Frozen sequence identity")
    add("INTEGRITY", "expected_base_candidates", "EQ", integrity["expected_base_candidates"], int(stage3_metrics["candidates"]), stage2_package._gate_status(stage3_metrics["candidates"], "EQ", integrity["expected_base_candidates"]), "Entry-only variant retains V10B selection")
    add("INTEGRITY", "all_selection_decisions_must_pass", "EQ", integrity["expected_base_candidates"], selection_rows, stage2_package._gate_status(selection_rows, "EQ", integrity["expected_base_candidates"]), "All decisions were PASSED")
    add("INTEGRITY", "expected_selection_overlay", "EQ", integrity["expected_selection_overlay"], integrity["expected_selection_overlay"], "PASS", "BASE_V10B_SELECTION retained")
    add("INTEGRITY", "expected_entry_expiry_minute", "EQ", 4, 4, "PASS", "Frozen EXPIRY_S4 policy")
    add("INTEGRITY", "same_frozen_inputs_as_control", "EQ", True, True, "PASS", "Cache, snapshot, window, costs, universe and portfolio validated")
    add("INTEGRITY", "full_state_machine_replay_required", "EQ", True, True, "PASS", "Full candidate audit is bound")

    gates = (
        ("minimum_fills", "GE", screen["minimum_fills"], stage3_metrics["fills"]),
        ("minimum_active_sessions", "GE", screen["minimum_active_sessions"], stage3_metrics["active_sessions"]),
        ("minimum_profit_factor", "GE", screen["minimum_profit_factor"], stage3_metrics["profit_factor"]),
        ("minimum_profit_factor_excluding_best_net_day", "GE", screen["minimum_profit_factor_excluding_best_net_day"], stage3_metrics["profit_factor_excluding_best_net_day"]),
        ("maximum_best_positive_day_share_pct", "LE", screen["maximum_best_positive_day_share_pct"], stage3_metrics["best_positive_day_share_pct"]),
        ("minimum_long_profit_factor", "GE", screen["minimum_long_profit_factor"], stage3_metrics["long_profit_factor"]),
        ("minimum_short_profit_factor", "GE", screen["minimum_short_profit_factor"], stage3_metrics["short_profit_factor"]),
        ("maximum_drawdown_as_pct_of_control", "LE", screen["maximum_drawdown_as_pct_of_control"], dd_ratio),
    )
    for gate, comparator_name, threshold, observed in gates:
        add("PRELIMINARY_PERFORMANCE", gate, comparator_name, threshold, observed, stage2_package._gate_status(observed, comparator_name, threshold), "Research screen only; not promotion authority")
    add("REQUIRED_REPORTING", "accuracy_pct_reported", "PRESENT", True, stage3_metrics["win_rate_pct"], "PASS" if stage2_package._is_evaluable(stage3_metrics["win_rate_pct"]) else "FAIL", "Accuracy is in summary outputs")
    add("REQUIRED_REPORTING", "fill_retention_pct_reported", "PRESENT", True, fill_retention, "PASS" if stage2_package._is_evaluable(fill_retention) else "FAIL", "Fill retention is in summary outputs")
    add("STRESS", "stress_20bps_plus_2bps", "DEFERRED", screen["stress_20bps_plus_2bps"], "DEFERRED_NOT_EVALUATED", "DEFERRED", "No stressed backtest was authorized in Stage 3")
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


def _recursive_artifacts(root: Path) -> dict[str, dict[str, Any]]:
    return stage2_package._recursive_artifacts(root)


def build_stage3_package(
    *,
    stage_plan: Path,
    v8_provenance: Path,
    stage1_control_provenance: Path,
    stage2_rv1_provenance: Path,
    stage3_challenger_provenance: Path,
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"Stage 3 output directory already exists: {target}")
    plan_path = _require_file(stage_plan, "Frozen experiment plan")
    v8_path = _require_file(v8_provenance, "Frozen V8 provenance")
    stage1_path = _require_file(
        stage1_control_provenance, "Stage 1 V10B provenance"
    )
    stage2_path = _require_file(stage2_rv1_provenance, "Stage 2 RV1_100 provenance")
    stage3_path = _require_file(
        stage3_challenger_provenance, "Stage 3 EXPIRY_S4 provenance"
    )
    path_keys = {
        _path_key(path) for path in (v8_path, stage1_path, stage2_path, stage3_path)
    }
    if len(path_keys) != 4:
        raise ValueError("All four provenance paths must be different")

    plan, stage3_entry = _load_stage3_plan(plan_path)
    stage1_manifest_path, stage1_manifest = _validate_stage1_v8_binding(
        plan=plan, v8_path=v8_path, stage1_path=stage1_path
    )
    v8_payload = comparison.validate_provenance(v8_path)
    stage1_payload = comparison.validate_provenance(stage1_path)
    stage2_payload = comparison.validate_provenance(stage2_path)
    stage3_payload = comparison.validate_provenance(stage3_path)

    _require_v8_identity(v8_payload, "V8")
    stage1_config_sha = experiment_config.variant_config_sha256(STAGE1_VARIANT)
    stage2_config_sha = experiment_config.variant_config_sha256(STAGE2_VARIANT)
    stage3_config_sha = str(stage3_entry["variant_config_sha256"])
    stage2_package._require_experiment_identity(
        stage1_payload,
        variant=STAGE1_VARIANT,
        variant_config_sha256=stage1_config_sha,
        label="Stage 1",
    )
    stage2_package._require_experiment_identity(
        stage2_payload,
        variant=STAGE2_VARIANT,
        variant_config_sha256=stage2_config_sha,
        label="Stage 2",
    )
    stage2_package._require_experiment_identity(
        stage3_payload,
        variant=EXPECTED_VARIANT,
        variant_config_sha256=stage3_config_sha,
        label="Stage 3",
    )
    _validate_v8_against_stage1(
        plan=plan, v8=v8_payload, stage1=stage1_payload
    )
    snapshot_path, expected_window = stage2_package._validate_frozen_inputs(
        plan=plan,
        control=stage1_payload,
        challenger=stage2_payload,
        stage1_manifest=stage1_manifest,
    )
    stage3_snapshot, stage3_window = stage2_package._validate_frozen_inputs(
        plan=plan,
        control=stage1_payload,
        challenger=stage3_payload,
        stage1_manifest=stage1_manifest,
    )
    if _path_key(snapshot_path) != _path_key(stage3_snapshot):
        raise ValueError("Stage 2 and Stage 3 source snapshots differ")
    if expected_window != stage3_window:
        raise ValueError("Stage 2 and Stage 3 windows differ")

    integrity = dict(plan["stage_02_integrity_gates"])
    expected_candidates = int(integrity["expected_base_candidates"])
    expected_overlay = str(integrity["expected_selection_overlay"])
    v8_audit = _load_v8_audit(v8_payload, expected_candidates)
    stage1_decisions, stage1_audit = stage2_package._validate_selection_and_audit(
        stage1_payload,
        variant=STAGE1_VARIANT,
        variant_config_sha256=stage1_config_sha,
        expected_candidates=expected_candidates,
        expected_overlay=expected_overlay,
        label="Stage 1",
    )
    stage2_decisions, stage2_audit = stage2_package._validate_selection_and_audit(
        stage2_payload,
        variant=STAGE2_VARIANT,
        variant_config_sha256=stage2_config_sha,
        expected_candidates=expected_candidates,
        expected_overlay=expected_overlay,
        label="Stage 2",
    )
    stage3_decisions, stage3_audit = stage2_package._validate_selection_and_audit(
        stage3_payload,
        variant=EXPECTED_VARIANT,
        variant_config_sha256=stage3_config_sha,
        expected_candidates=expected_candidates,
        expected_overlay=expected_overlay,
        label="Stage 3",
    )
    base_ids = stage1_decisions["candidate_id"].astype(str).tolist()
    if stage2_decisions["candidate_id"].astype(str).tolist() != base_ids:
        raise ValueError("Stage 2 did not retain ordered Stage 1 candidates")
    if stage3_decisions["candidate_id"].astype(str).tolist() != base_ids:
        raise ValueError("Stage 3 did not retain ordered Stage 1 candidates")
    if v8_audit["candidate_id"].astype(str).tolist() != base_ids:
        raise ValueError("V8 and Stage 1 ordered candidate sets differ")

    _require_chronology(
        (
            ("V8", v8_payload),
            ("Stage 1", stage1_payload),
            ("Stage 2", stage2_payload),
            ("Stage 3", stage3_payload),
        )
    )

    v8_metrics = stage2_package._metric_bundle(v8_payload, v8_audit, label="V8")
    stage1_metrics = stage2_package._metric_bundle(
        stage1_payload, stage1_audit, label="STAGE1"
    )
    stage2_metrics = stage2_package._metric_bundle(
        stage2_payload, stage2_audit, label="STAGE2"
    )
    stage3_metrics = stage2_package._metric_bundle(
        stage3_payload, stage3_audit, label="STAGE3"
    )
    records = _version_records(
        v8_path=v8_path,
        stage1_path=stage1_path,
        stage2_path=stage2_path,
        stage3_path=stage3_path,
        v8_metrics=v8_metrics,
        stage1_metrics=stage1_metrics,
        stage2_metrics=stage2_metrics,
        stage3_metrics=stage3_metrics,
    )
    payload_by_key = {
        "V8": v8_payload,
        "STAGE1": stage1_payload,
        "STAGE2": stage2_payload,
        "STAGE3": stage3_payload,
    }
    isolated_summary = _isolated_summary_rows(stage1_metrics, stage3_metrics)
    all_versions_summary = _all_versions_summary_rows(records, stage1_metrics)
    train_test_rows = _cumulative_train_test_rows(records, payload_by_key)
    cumulative_daywise = _cumulative_daywise_frame(payload_by_key)
    stage3_filled = stage2_package._filled_rows(stage3_audit, "Stage 3")
    funnel_rows = stage2_package._funnel_rows(
        stage3_decisions, stage3_audit, stage3_filled
    )
    for row in funnel_rows:
        row["stage"] = STAGE
        row["variant"] = EXPECTED_VARIANT
    gate_rows = _stage3_gate_rows(
        plan=plan,
        stage1_metrics=stage1_metrics,
        stage3_metrics=stage3_metrics,
        selection_rows=len(stage3_decisions),
    )
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )

    comparison_dir = target / COMPARISON_DIR_NAME
    comparison.build_comparison(
        control_provenance=stage1_path,
        challenger_provenance=stage3_path,
        output_dir=comparison_dir,
        require_control_parity=False,
    )

    plan_copy_path = target / PLAN_FILENAME
    variant_path = target / "variant_config.json"
    summary_path = target / "summary.csv"
    all_versions_path = target / "all_versions_summary.csv"
    train_test_path = target / "cumulative_train_test.csv"
    cumulative_daywise_path = target / "cumulative_daywise.csv"
    daywise_path = target / DAYWISE_FILENAME
    side_setup_path = target / SIDE_SETUP_FILENAME
    funnel_path = target / "funnel.csv"
    ledger_path = target / "filled_trade_ledger.csv"
    gates_path = target / "preliminary_gates.csv"
    tests_path = target / "test_results.json"
    decision_path = target / "decision.md"

    stage2_package._atomic_copy(plan_path, plan_copy_path)
    if comparison.sha256_file(plan_copy_path) != EXPECTED_STAGE_PLAN_SHA256:
        raise AssertionError("Frozen experiment plan copy is not byte-identical")
    variant_archive = {
        "schema_version": VARIANT_ARCHIVE_SCHEMA_VERSION,
        "stage": STAGE,
        "variant": EXPECTED_VARIANT,
        "mechanism": stage3_entry.get("mechanism"),
        "frozen_plan_sha256": EXPECTED_STAGE_PLAN_SHA256,
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": stage3_config_sha,
        "variant_config": experiment_config.variant_config_payload(
            EXPECTED_VARIANT
        ),
        "resolved_entry_policy": stage2_package._parameters(stage3_payload).get(
            "entry_policy"
        ),
        "lineage_provenance": {
            record["key"]: {
                "path": str(record["provenance"]),
                "sha256": comparison.sha256_file(Path(record["provenance"])),
            }
            for record in records
        },
        "research_only": True,
        "promotion_eligible": False,
    }
    _write_json(variant_path, variant_archive)
    comparison.atomic_write_csv(
        summary_path,
        [stage2_package._json_safe(row) for row in isolated_summary],
    )
    comparison.atomic_write_csv(
        all_versions_path,
        [stage2_package._json_safe(row) for row in all_versions_summary],
    )
    comparison.atomic_write_csv(
        train_test_path,
        [stage2_package._json_safe(row) for row in train_test_rows],
    )
    stage2_package._atomic_write_frame(cumulative_daywise_path, cumulative_daywise)
    stage2_package._atomic_copy(comparison_dir / DAYWISE_FILENAME, daywise_path)
    stage2_package._atomic_copy(
        comparison_dir / "side_and_leg_comparison.csv", side_setup_path
    )
    comparison.atomic_write_csv(
        funnel_path, [stage2_package._json_safe(row) for row in funnel_rows]
    )
    stage2_package._atomic_write_frame(ledger_path, stage3_filled)
    comparison.atomic_write_csv(
        gates_path, [stage2_package._json_safe(row) for row in gate_rows]
    )
    _write_json(tests_path, test_results)

    gate_counts = Counter(str(row["status"]) for row in gate_rows)
    dd_ratio, fill_retention = _dd_and_fill_ratios(stage3_metrics, stage1_metrics)
    tests_line = (
        "not recorded"
        if test_results["tests_total"] is None
        else (
            f"{test_results['tests_passed']} passed / "
            f"{test_results['tests_failed']} failed"
        )
    )
    decision_text = f"""# FNO V10 Stage 3 - EXPIRY_S4 isolated experiment

- Decision: **{DECISION}**
- V8 baseline: `{v8_path}`
- Stage 1 V10B control: `{stage1_path}`
- Stage 2 RV1_100: `{stage2_path}`
- Stage 3 EXPIRY_S4: `{stage3_path}`
- Frozen plan SHA256: `{EXPECTED_STAGE_PLAN_SHA256}`
- Variant config SHA256: `{stage3_config_sha}`
- Window: {expected_window['from_day']} through {expected_window['through_day']}
- Candidates retained: {int(stage3_metrics['candidates'])} / {expected_candidates}
- Fills / active sessions: {int(stage3_metrics['fills'])} / {int(stage3_metrics['active_sessions'])}
- Accuracy: {float(stage3_metrics['win_rate_pct']):.6f}%
- Fill retention versus Stage 1: {fill_retention:.6f}%
- Profit factor: {float(stage3_metrics['profit_factor']):.12f}
- Profit factor excluding best net day: {float(stage3_metrics['profit_factor_excluding_best_net_day']):.12f}
- Best positive day share: {float(stage3_metrics['best_positive_day_share_pct']):.6f}%
- LONG / SHORT profit factor: {float(stage3_metrics['long_profit_factor']):.12f} / {float(stage3_metrics['short_profit_factor']):.12f}
- Drawdown versus Stage 1: {dd_ratio:.6f}%
- Preliminary gates: {gate_counts.get('PASS', 0)} PASS / {gate_counts.get('FAIL', 0)} FAIL / {gate_counts.get('DEFERRED', 0)} DEFERRED
- Stress 20bps + 2bps: **DEFERRED_NOT_EVALUATED**
- Test evidence: {tests_line}

This package records completion of the predeclared Stage 3 isolated research
run and preserves cumulative V8-through-Stage-3 results. Gate outcomes do not
alter this completion-only decision and do not authorize live, paper, or
strategy promotion. The stress screen remains unevaluated.
"""
    comparison.atomic_write_text(decision_path, decision_text)

    artifacts = _recursive_artifacts(target)
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
    missing_top_level = sorted(required_top_level - set(artifacts))
    if missing_top_level:
        raise AssertionError(
            f"Stage 3 package is missing top-level artifacts: {missing_top_level}"
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
        "stage1_package_manifest": str(stage1_manifest_path),
        "stage1_package_manifest_sha256": comparison.sha256_file(
            stage1_manifest_path
        ),
        "lineage_provenance": {
            record["key"]: {
                "stage": record["stage"],
                "variant": record["variant"],
                "path": str(record["provenance"]),
                "sha256": comparison.sha256_file(Path(record["provenance"])),
                "backtest_input_fingerprint": payload_by_key[
                    str(record["key"])
                ].get("backtest_input_fingerprint"),
            }
            for record in records
        },
        "source_snapshot_manifest": str(snapshot_path),
        "source_snapshot_manifest_sha256": comparison.sha256_file(snapshot_path),
        "source_snapshot_fingerprint": dict(
            stage3_payload.get("source_snapshot", {})
        ).get("snapshot_fingerprint"),
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": stage3_config_sha,
        "experiment_runner_source_sha256": stage3_payload.get(
            "experiment_runner_source_sha256"
        ),
        "experiment_config_source_sha256": stage3_payload.get(
            "experiment_config_source_sha256"
        ),
        "cache_input_fingerprint": stage3_payload.get("cache_input_fingerprint"),
        "backtest_window": expected_window,
        "fixed_economics_contract": comparison.fixed_economics_contract(
            stage3_payload
        ),
        "comparison_directory": COMPARISON_DIR_NAME,
        "isolated_summary": isolated_summary,
        "all_versions_summary": all_versions_summary,
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
        "--v8-provenance",
        "--v8-control-provenance",
        dest="v8_provenance",
        type=Path,
        required=True,
    )
    parser.add_argument("--stage1-control-provenance", type=Path, required=True)
    parser.add_argument(
        "--stage2-rv1-provenance",
        "--stage2-provenance",
        dest="stage2_rv1_provenance",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--stage3-challenger-provenance",
        "--challenger-provenance",
        dest="stage3_challenger_provenance",
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
    output = build_stage3_package(
        stage_plan=args.stage_plan,
        v8_provenance=args.v8_provenance,
        stage1_control_provenance=args.stage1_control_provenance,
        stage2_rv1_provenance=args.stage2_rv1_provenance,
        stage3_challenger_provenance=args.stage3_challenger_provenance,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
