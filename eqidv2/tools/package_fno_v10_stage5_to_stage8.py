"""Package frozen selection experiments for FNO V10 Stages 5 through 9.

One invocation publishes exactly one non-overwritable stage package.  Every
package contains an isolated Stage-1-control comparison and cumulative
Stage-0-through-target results.  Selection-changing variants are validated
against their frozen mechanism before the replay audit is accepted.  RVOL
stages also require and archive their causal same-slot RVOL sidecar.

This utility never executes a backtest and never grants promotion authority.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
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
import package_fno_v10_stage4 as stage4_package


PACKAGE_SCHEMA_VERSION = "fno_v10_stage5_to_stage8_result_package_v1"
TEST_EVIDENCE_SCHEMA_VERSION = "fno_v10_stage5_to_stage8_test_evidence_v1"
VARIANT_ARCHIVE_SCHEMA_VERSION = "fno_v10_stage5_to_stage8_variant_archive_v1"
STAGE9_PACKAGE_SCHEMA_VERSION = "fno_v10_stage5_to_stage9_result_package_v1"
STAGE9_TEST_EVIDENCE_SCHEMA_VERSION = "fno_v10_stage5_to_stage9_test_evidence_v1"
STAGE9_VARIANT_ARCHIVE_SCHEMA_VERSION = "fno_v10_stage5_to_stage9_variant_archive_v1"
RVOL_SIZE_DEFECT_CODE = "FROZEN_RUNNER_ARTIFACT_SIZE_KEY_BYTES_VS_SIZE"
PLAN_FILENAME = stage2_package.PLAN_FILENAME
EXPECTED_STAGE_PLAN_SHA256 = stage2_package.EXPECTED_STAGE_PLAN_SHA256
BASE_DECISION_COUNT = 890
BASELINE_KEY = "STAGE1"

STAGE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "STAGE_05": {
        "key": "STAGE5",
        "variant": "NO_0935_LONG",
        "allowed_rejections": ("SETUP_DISABLED",),
        "decision": "COMPLETED_STAGE5_ISOLATED_RESEARCH_NOT_PROMOTION",
        "comparison_dir": "stage1_v10b_vs_stage5_no_0935_long_comparison",
    },
    "STAGE_06": {
        "key": "STAGE6",
        "variant": "0940_LONG_MOVE_030",
        "allowed_rejections": ("PRICE_CHANGE_BELOW_VARIANT_MINIMUM",),
        "decision": "COMPLETED_STAGE6_ISOLATED_RESEARCH_NOT_PROMOTION",
        "comparison_dir": "stage1_v10b_vs_stage6_0940_long_move_030_comparison",
    },
    "STAGE_07": {
        "key": "STAGE7",
        "variant": "0940_LONG_MOVE_040",
        "allowed_rejections": ("PRICE_CHANGE_BELOW_VARIANT_MINIMUM",),
        "decision": "COMPLETED_STAGE7_ISOLATED_RESEARCH_NOT_PROMOTION",
        "comparison_dir": "stage1_v10b_vs_stage7_0940_long_move_040_comparison",
    },
    "STAGE_08": {
        "key": "STAGE8",
        "variant": "SLOT_RVOL_150",
        "allowed_rejections": (
            "SLOT_RVOL_HISTORY_INSUFFICIENT",
            "SLOT_RVOL_BASELINE_INVALID",
            "SLOT_RVOL_BELOW_MINIMUM",
        ),
        "decision": (
            "COMPLETED_STAGE8_ISOLATED_RESEARCH_WITH_RVOL_SIZE_ATTESTATION_"
            "NOT_PROMOTION"
        ),
        "comparison_dir": "stage1_v10b_vs_stage8_slot_rvol_150_comparison",
    },
    "STAGE_09": {
        "key": "STAGE9",
        "variant": "SLOT_RVOL_200",
        "allowed_rejections": (
            "SLOT_RVOL_HISTORY_INSUFFICIENT",
            "SLOT_RVOL_BASELINE_INVALID",
            "SLOT_RVOL_BELOW_MINIMUM",
        ),
        "decision": (
            "COMPLETED_STAGE9_ISOLATED_RESEARCH_WITH_RVOL_SIZE_ATTESTATION_"
            "NOT_PROMOTION"
        ),
        "comparison_dir": "stage1_v10b_vs_stage9_slot_rvol_200_comparison",
    },
}

FIXED_IDENTITIES: dict[str, tuple[str, str]] = {
    "STAGE0": ("STAGE_00", "V10B"),
    "STAGE1": ("STAGE_01", "V10B"),
    "STAGE2": ("STAGE_02", "RV1_100"),
    "STAGE3": ("STAGE_03", "EXPIRY_S4"),
    "STAGE4": ("STAGE_04", "RV1_100_S4"),
    "STAGE5": ("STAGE_05", "NO_0935_LONG"),
    "STAGE6": ("STAGE_06", "0940_LONG_MOVE_030"),
    "STAGE7": ("STAGE_07", "0940_LONG_MOVE_040"),
    "STAGE8": ("STAGE_08", "SLOT_RVOL_150"),
    "STAGE9": ("STAGE_09", "SLOT_RVOL_200"),
}

SUMMARY_FIELDS = (
    *stage3_package.SUMMARY_METRIC_FIELDS,
    "base_selection_decisions",
    "selection_passed",
    "selection_rejected",
    "selection_retention_pct",
)

DIAGNOSTIC_FIELDS = (
    "candidates",
    "confirmed",
    "fills",
    "closed_fills",
    "wins",
    "losses",
    "flat_trades",
    "net_return_percentage_points",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
    "profit_factor",
)


def _require_file(path: Path, label: str) -> Path:
    return stage2_package._require_file(path, label)


def _path_key(path: Path) -> str:
    return stage2_package._path_key(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    stage2_package._write_json(path, payload)


def _stage_number(stage: str) -> int:
    normalized = str(stage).upper().strip()
    if normalized not in STAGE_DEFINITIONS:
        raise ValueError(
            f"Target stage must be one of {sorted(STAGE_DEFINITIONS)}"
        )
    return int(normalized.rsplit("_", 1)[1])


def _required_keys(target_stage: str) -> tuple[str, ...]:
    target_number = _stage_number(target_stage)
    return tuple(f"STAGE{number}" for number in range(0, target_number + 1))


def _target_definition(target_stage: str) -> dict[str, Any]:
    return dict(STAGE_DEFINITIONS[str(target_stage).upper().strip()])


def _rvol_attestation_filename(target_stage: str) -> str:
    stage_number = _stage_number(target_stage)
    return f"stage{stage_number}_rvol_size_attestation.json"


def _rvol_attestation_schema(target_stage: str) -> str:
    stage_number = _stage_number(target_stage)
    return f"fno_v10_stage{stage_number}_rvol_size_attestation_v1"


def _package_schema_versions(target_stage: str) -> tuple[str, str, str]:
    if str(target_stage).upper().strip() == "STAGE_09":
        return (
            STAGE9_PACKAGE_SCHEMA_VERSION,
            STAGE9_TEST_EVIDENCE_SCHEMA_VERSION,
            STAGE9_VARIANT_ARCHIVE_SCHEMA_VERSION,
        )
    return (
        PACKAGE_SCHEMA_VERSION,
        TEST_EVIDENCE_SCHEMA_VERSION,
        VARIANT_ARCHIVE_SCHEMA_VERSION,
    )


def _load_target_plan(
    path: Path, target_stage: str
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    plan, stage4_entry = stage4_package._load_stage4_plan(path)
    sequence = [dict(item) for item in list(plan.get("sequence", []))]
    previous_index = sequence.index(stage4_entry)
    entries: dict[str, dict[str, Any]] = {}
    for stage_number in range(5, _stage_number(target_stage) + 1):
        stage = f"STAGE_{stage_number:02d}"
        definition = _target_definition(stage)
        matches = [
            item for item in sequence if str(item.get("stage", "")).upper() == stage
        ]
        if len(matches) != 1:
            raise ValueError(f"Frozen plan must contain exactly one {stage} entry")
        entry = matches[0]
        if str(entry.get("variant", "")).upper() != definition["variant"]:
            raise ValueError(f"Frozen {stage} variant identity changed")
        expected_config_sha = experiment_config.variant_config_sha256(
            definition["variant"]
        )
        if entry.get("variant_config_sha256") != expected_config_sha:
            raise ValueError(f"Frozen {stage} variant config SHA256 is invalid")
        current_index = sequence.index(entry)
        if current_index != previous_index + 1:
            raise ValueError(f"Frozen {stage} is out of sequence")
        entries[stage] = entry
        previous_index = current_index
    return plan, entries


def _selection_metadata(
    *,
    base_count: int,
    passed_count: int,
) -> dict[str, Any]:
    return {
        "base_selection_decisions": int(base_count),
        "selection_passed": int(passed_count),
        "selection_rejected": int(base_count - passed_count),
        "selection_retention_pct": (
            100.0 * passed_count / base_count if base_count else math.nan
        ),
    }


def _expected_reasons(
    decisions: pd.DataFrame,
    spec: experiment_config.ExperimentSpec,
) -> pd.Series:
    reasons = pd.Series("PASSED", index=decisions.index, dtype=object)
    if spec.disabled_setup_ids:
        disabled = decisions["setup_id"].astype(str).isin(spec.disabled_setup_ids)
        reasons.loc[disabled] = "SETUP_DISABLED"
    for setup_id, threshold in spec.price_threshold_overrides:
        affected = (
            reasons.eq("PASSED")
            & decisions["setup_id"].astype(str).eq(setup_id)
            & pd.to_numeric(decisions["price_change_pct"], errors="coerce")
            .add(1e-12)
            .lt(float(threshold))
        )
        reasons.loc[affected] = "PRICE_CHANGE_BELOW_VARIANT_MINIMUM"
    if spec.slot_rvol20_min is not None:
        required = {
            "prior_slot_observation_count_20",
            "prior_slot_volume_median",
            "slot_rvol20",
            "feature_available",
        }
        missing = sorted(required - set(decisions.columns))
        if missing:
            raise ValueError(f"RVOL decisions miss sidecar columns: {missing}")
        count = pd.to_numeric(
            decisions["prior_slot_observation_count_20"], errors="coerce"
        )
        median = pd.to_numeric(
            decisions["prior_slot_volume_median"], errors="coerce"
        )
        ratio = pd.to_numeric(decisions["slot_rvol20"], errors="coerce")
        insufficient = reasons.eq("PASSED") & (count.isna() | count.lt(10))
        reasons.loc[insufficient] = "SLOT_RVOL_HISTORY_INSUFFICIENT"
        invalid = reasons.eq("PASSED") & (
            median.isna()
            | ~np.isfinite(median)
            | median.le(0)
            | ratio.isna()
            | ~np.isfinite(ratio)
        )
        reasons.loc[invalid] = "SLOT_RVOL_BASELINE_INVALID"
        below = (
            reasons.eq("PASSED")
            & ratio.add(1e-12).lt(float(spec.slot_rvol20_min))
        )
        reasons.loc[below] = "SLOT_RVOL_BELOW_MINIMUM"
    return reasons


def _validate_selection_variant(
    payload: Mapping[str, Any],
    *,
    stage: str,
    base_candidate_ids: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    definition = _target_definition(stage)
    variant = str(definition["variant"])
    spec = experiment_config.get_spec(variant)
    config_sha = experiment_config.variant_config_sha256(spec)
    decisions = pd.read_csv(
        comparison.output_path(payload, "selection_decisions"), low_memory=False
    )
    required_decisions = {
        "candidate_id",
        "session_date",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "picker_value",
        "traded_value",
        "selection_passed",
        "selection_reason",
        "experiment_variant",
        "selection_overlay_id",
        "slot_rvol20_min",
        "confirmation_volume_ratio_min",
        "entry_expiry_minute",
        "variant_config_sha256",
    }
    missing = sorted(required_decisions - set(decisions.columns))
    if missing:
        raise ValueError(f"{stage} selection decisions miss columns: {missing}")
    if len(decisions) != BASE_DECISION_COUNT:
        raise ValueError(
            f"{stage} must retain {BASE_DECISION_COUNT} base decisions; "
            f"observed={len(decisions)}"
        )
    ids = decisions["candidate_id"].astype(str).tolist()
    if ids != list(base_candidate_ids):
        raise ValueError(f"{stage} base decision order differs from Stage 1")
    if len(set(ids)) != len(ids) or any(not value for value in ids):
        raise ValueError(f"{stage} decision candidate IDs are blank or duplicated")
    exact_fields = {
        "experiment_variant": variant,
        "selection_overlay_id": spec.selection_overlay_id,
        "variant_config_sha256": config_sha,
    }
    for field, expected in exact_fields.items():
        if not decisions[field].astype(str).eq(str(expected)).all():
            raise ValueError(f"{stage} selection field {field} differs")
    expiry = pd.to_numeric(decisions["entry_expiry_minute"], errors="coerce")
    if expiry.isna().any() or not expiry.eq(spec.entry_expiry_minute).all():
        raise ValueError(f"{stage} selection expiry differs from frozen config")
    for field, expected in (
        ("confirmation_volume_ratio_min", spec.confirmation_volume_ratio_min),
        ("slot_rvol20_min", spec.slot_rvol20_min),
    ):
        observed = pd.to_numeric(decisions[field], errors="coerce")
        if expected is None:
            if observed.notna().any():
                raise ValueError(f"{stage} selection unexpectedly sets {field}")
        elif observed.isna().any() or not observed.eq(float(expected)).all():
            raise ValueError(f"{stage} selection {field} differs")

    expected_reasons = _expected_reasons(decisions, spec)
    observed_reasons = decisions["selection_reason"].astype(str)
    if not observed_reasons.equals(expected_reasons.astype(str)):
        mismatch_count = int(observed_reasons.ne(expected_reasons).sum())
        raise ValueError(
            f"{stage} selection reasons do not match frozen mechanism: "
            f"{mismatch_count} rows"
        )
    allowed = {"PASSED", *definition["allowed_rejections"]}
    unexpected = sorted(set(observed_reasons) - allowed)
    if unexpected:
        raise ValueError(f"{stage} has unexpected rejection reasons: {unexpected}")
    passed = stage2_package._boolean_series(
        decisions["selection_passed"], f"{stage} selection"
    )
    if not passed.equals(expected_reasons.eq("PASSED")):
        raise ValueError(f"{stage} selection booleans and reasons disagree")
    if int((~passed).sum()) <= 0:
        raise ValueError(f"{stage} selection-changing mechanism rejected no rows")

    audit = pd.read_csv(
        comparison.output_path(payload, "candidate_order_audit"), low_memory=False
    )
    required_audit = {
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
    missing_audit = sorted(required_audit - set(audit.columns))
    if missing_audit:
        raise ValueError(f"{stage} audit misses columns: {missing_audit}")
    retained_ids = decisions.loc[passed, "candidate_id"].astype(str).tolist()
    audit_ids = audit["candidate_id"].astype(str).tolist()
    if len(audit_ids) != len(retained_ids) or set(audit_ids) != set(retained_ids):
        raise ValueError(f"{stage} passed decisions do not reconcile to audit")
    if len(set(audit_ids)) != len(audit_ids):
        raise ValueError(f"{stage} audit candidate IDs are duplicated")
    results_candidates = int(dict(payload.get("results", {})).get("candidates", -1))
    if results_candidates != len(audit) or results_candidates != int(passed.sum()):
        raise ValueError(f"{stage} provenance candidate count does not reconcile")
    for field, expected in exact_fields.items():
        if not audit[field].astype(str).eq(str(expected)).all():
            raise ValueError(f"{stage} audit field {field} differs")
    expected_portfolio = str(
        stage2_package._parameters(payload).get("portfolio_mode", "")
    )
    if not expected_portfolio or not audit["portfolio_mode"].astype(str).eq(
        expected_portfolio
    ).all():
        raise ValueError(f"{stage} audit portfolio mode differs")
    return decisions, audit, _selection_metadata(
        base_count=len(decisions), passed_count=int(passed.sum())
    )


def _validate_artifact_record(
    record: Mapping[str, Any], *, label: str
) -> tuple[Path, int, str]:
    required = {"path", "size", "sha256"}
    missing = sorted(required - set(record))
    if missing:
        raise ValueError(f"{label} artifact record misses fields: {missing}")
    path = _require_file(Path(str(record["path"])), label)
    try:
        recorded_size = int(record["size"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} artifact size is invalid") from exc
    actual_size = int(path.stat().st_size)
    if recorded_size <= 0 or recorded_size != actual_size:
        raise ValueError(f"{label} artifact size differs from the file")
    recorded_sha = str(record["sha256"]).lower()
    if len(recorded_sha) != 64 or any(
        character not in "0123456789abcdef" for character in recorded_sha
    ):
        raise ValueError(f"{label} artifact SHA256 is invalid")
    actual_sha = comparison.sha256_file(path)
    if recorded_sha != actual_sha:
        raise ValueError(f"{label} artifact SHA256 differs from the file")
    return path.resolve(), actual_size, actual_sha


def _validate_rvol_sidecar(
    payload: Mapping[str, Any],
    provenance_path: Path,
    plan: Mapping[str, Any],
    target_stage: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_stage = str(target_stage).upper().strip()
    definition = _target_definition(normalized_stage)
    if not experiment_config.get_spec(definition["variant"]).uses_slot_rvol20:
        raise ValueError(f"{normalized_stage} is not an RVOL experiment")
    binding = payload.get("v10_experiment_feature_input_binding")
    if not isinstance(binding, Mapping):
        raise ValueError(f"{normalized_stage} RVOL feature input binding is missing")
    resolved = dict(binding)
    required = {
        "schema_version",
        "input_fingerprint",
        "source_manifest_sha256",
        "table_sha256",
        "table_bytes",
        "row_count",
    }
    missing = sorted(required - set(resolved))
    if missing:
        raise ValueError(f"{normalized_stage} RVOL binding misses fields: {missing}")
    if resolved["schema_version"] != experiment_config.SLOT_RVOL_SCHEMA_VERSION:
        raise ValueError(f"{normalized_stage} RVOL schema differs from frozen config")
    for field in ("input_fingerprint", "source_manifest_sha256", "table_sha256"):
        value = str(resolved.get(field, "")).lower()
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError(f"{normalized_stage} RVOL binding {field} is invalid")
        resolved[field] = value
    try:
        original_table_bytes = int(resolved["table_bytes"])
        bound_row_count = int(resolved["row_count"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{normalized_stage} RVOL size/row binding is invalid") from exc
    if original_table_bytes != -1:
        raise ValueError(
            f"{normalized_stage} RVOL run does not have the exact frozen table_bytes=-1 "
            "defect signature"
        )
    if bound_row_count <= 0:
        raise ValueError(f"{normalized_stage} RVOL sidecar is empty")

    outputs = dict(payload.get("outputs", {}))
    manifest_record = dict(outputs.get("slot_rvol20_manifest_archive", {}))
    table_record = dict(outputs.get("slot_rvol20_table_archive", {}))
    runner_record = dict(outputs.get("experiment_runner_source_archive", {}))
    manifest_path, manifest_size, manifest_sha = _validate_artifact_record(
        manifest_record, label=f"{normalized_stage} RVOL manifest archive"
    )
    table_path, table_size, table_sha = _validate_artifact_record(
        table_record, label=f"{normalized_stage} RVOL table archive"
    )
    runner_path, runner_size, runner_sha = _validate_artifact_record(
        runner_record, label=f"{normalized_stage} runner source archive"
    )
    run_dir = provenance_path.resolve().parent
    if (
        manifest_path.parent != run_dir
        or table_path.parent != run_dir
        or runner_path.parent != run_dir
    ):
        raise ValueError(f"{normalized_stage} RVOL archives are not run-local")
    if "bytes" in table_record or int(table_record.get("size", -1)) != table_size:
        raise ValueError(
            f"{normalized_stage} output record does not match the known size-key defect"
        )
    if table_sha != resolved["table_sha256"]:
        raise ValueError(f"{normalized_stage} RVOL table SHA256 differs from binding")

    frozen_runner_sha = str(dict(plan.get("frozen_inputs", {})).get("runner_sha256", ""))
    if (
        len(frozen_runner_sha) != 64
        or payload.get("experiment_runner_source_sha256") != frozen_runner_sha
        or runner_sha != frozen_runner_sha
    ):
        raise ValueError(f"{normalized_stage} runner does not match the frozen plan")

    manifest = comparison.load_json(manifest_path)
    if manifest.get("schema_version") != resolved["schema_version"]:
        raise ValueError(f"{normalized_stage} archived RVOL manifest schema differs")
    if manifest.get("complete") is not True:
        raise ValueError(f"{normalized_stage} archived RVOL manifest is incomplete")
    contract = dict(manifest.get("input_contract", {}))
    canonical_fingerprint = experiment_config.canonical_json_sha256(contract)
    if (
        manifest.get("input_fingerprint") != canonical_fingerprint
        or resolved["input_fingerprint"] != canonical_fingerprint
    ):
        raise ValueError(f"{normalized_stage} RVOL canonical input fingerprint differs")
    frozen_contract = {
        "runner_source_sha256": payload.get("experiment_runner_source_sha256"),
        "registry_sha256": payload.get("v10_experiment_registry_sha256"),
        "snapshot_fingerprint": dict(payload.get("source_snapshot", {})).get(
            "snapshot_fingerprint"
        ),
        "from_day": dict(payload.get("backtest_window", {})).get("from_day"),
        "through_day": dict(payload.get("backtest_window", {})).get("through_day"),
        "lookback_sessions": 20,
        "minimum_prior_observations": 10,
        "invalid_or_insufficient_policy": "FAIL_CLOSED",
    }
    for field, expected in frozen_contract.items():
        if contract.get(field) != expected:
            raise ValueError(f"{normalized_stage} RVOL input contract changed: {field}")

    archived_table_record = dict(
        dict(manifest.get("artifacts", {})).get("slot_rvol20", {})
    )
    archived_table_path, archived_table_size, archived_table_sha = (
        _validate_artifact_record(
            archived_table_record,
            label=f"{normalized_stage} archived-manifest RVOL table",
        )
    )
    if "bytes" in archived_table_record or archived_table_path != table_path:
        raise ValueError(
            f"{normalized_stage} archived table does not match the known size-key defect"
        )
    if archived_table_size != table_size or archived_table_sha != table_sha:
        raise ValueError(f"{normalized_stage} archived table records disagree")

    source_record = dict(manifest.get("source_cache_manifest", {}))
    source_manifest_path = _require_file(
        Path(str(source_record.get("path", ""))),
        f"{normalized_stage} RVOL source cache manifest",
    )
    source_manifest_sha = comparison.sha256_file(source_manifest_path)
    if (
        str(source_record.get("sha256", "")).lower() != source_manifest_sha
        or resolved["source_manifest_sha256"] != source_manifest_sha
    ):
        raise ValueError(f"{normalized_stage} RVOL source manifest SHA256 differs")
    source_manifest = comparison.load_json(source_manifest_path)
    source_contract = dict(source_manifest.get("input_contract", {}))
    if (
        source_manifest.get("schema_version") != resolved["schema_version"]
        or source_manifest.get("complete") is not True
        or source_contract != contract
        or source_manifest.get("input_fingerprint") != canonical_fingerprint
        or int(source_manifest.get("row_count", -1)) != bound_row_count
    ):
        raise ValueError(
            f"{normalized_stage} RVOL source and archived manifests disagree"
        )
    source_table_record = dict(
        dict(source_manifest.get("artifacts", {})).get("slot_rvol20", {})
    )
    source_table_path, source_table_size, source_table_sha = (
        _validate_artifact_record(
            source_table_record,
            label=f"{normalized_stage} source-cache RVOL table",
        )
    )
    if source_table_size != table_size or source_table_sha != table_sha:
        raise ValueError(
            f"{normalized_stage} source and run-local RVOL tables disagree"
        )

    if int(manifest.get("row_count", -1)) != bound_row_count:
        raise ValueError(f"{normalized_stage} RVOL archived manifest row count differs")
    try:
        import pyarrow.parquet as parquet
    except ImportError as exc:
        raise RuntimeError(
            f"pyarrow is required for metadata-only {normalized_stage} row attestation"
        ) from exc
    parquet_file = parquet.ParquetFile(table_path)
    parquet_row_count = int(parquet_file.metadata.num_rows)
    if parquet_row_count != bound_row_count:
        raise ValueError(f"{normalized_stage} RVOL parquet metadata row count differs")

    provenance_sha = comparison.sha256_file(provenance_path)
    validated_binding = {
        **resolved,
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "table_path": str(table_path),
    }
    attestation = {
        "schema_version": _rvol_attestation_schema(normalized_stage),
        "stage": normalized_stage,
        "variant": definition["variant"],
        "attestation_type": "DETERMINISTIC_METADATA_ONLY_SIZE_REPAIR",
        "defect_code": RVOL_SIZE_DEFECT_CODE,
        "source_provenance_unchanged": True,
        "source_provenance": {
            "path": str(provenance_path.resolve()),
            "sha256": provenance_sha,
        },
        "frozen_runner": {
            "sha256": frozen_runner_sha,
            "archive_path": str(runner_path),
            "archive_size": runner_size,
        },
        "original_binding": resolved,
        "defect_signature": {
            "binding_table_bytes": original_table_bytes,
            "output_record_size_key": "size",
            "output_record_has_bytes_key": False,
            "archived_manifest_record_size_key": "size",
            "archived_manifest_record_has_bytes_key": False,
        },
        "attested_binding": {
            **resolved,
            "table_bytes": table_size,
        },
        "validated_artifacts": {
            "run_manifest": {
                "path": str(manifest_path),
                "sha256": manifest_sha,
                "size": manifest_size,
            },
            "run_table": {
                "path": str(table_path),
                "sha256": table_sha,
                "size": table_size,
                "parquet_metadata_row_count": parquet_row_count,
            },
            "source_manifest": {
                "path": str(source_manifest_path.resolve()),
                "sha256": source_manifest_sha,
            },
            "source_table": {
                "path": str(source_table_path),
                "sha256": source_table_sha,
                "size": source_table_size,
            },
        },
        "canonical_input_fingerprint": canonical_fingerprint,
        "research_only": True,
        "promotion_eligible": False,
    }
    return validated_binding, attestation


def _augment_metrics(
    metrics: Mapping[str, Any], selection: Mapping[str, Any]
) -> dict[str, Any]:
    return {**dict(metrics), **dict(selection)}


def _version_records(
    *,
    keys: Sequence[str],
    paths: Mapping[str, Path],
    metrics: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "key": key,
            "stage": FIXED_IDENTITIES[key][0],
            "variant": FIXED_IDENTITIES[key][1],
            "provenance": paths[key],
            "metrics": metrics[key],
        }
        for key in keys
    ]


def _ratio_metrics(
    metrics: Mapping[str, Any], baseline: Mapping[str, Any]
) -> tuple[float, float]:
    return stage3_package._dd_and_fill_ratios(metrics, baseline)


def _absolute_summary_row(
    record: Mapping[str, Any], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = dict(record["metrics"])
    dd_ratio, fill_retention = _ratio_metrics(metrics, baseline)
    provenance = Path(record["provenance"])
    return {
        "row_type": "ABSOLUTE",
        "version": record["key"],
        "stage": record["stage"],
        "variant": record["variant"],
        "baseline": BASELINE_KEY,
        "from_day": metrics.get("from_day"),
        "through_day": metrics.get("through_day"),
        **{field: metrics.get(field) for field in SUMMARY_FIELDS},
        "excluded_best_net_day": metrics.get("excluded_best_net_day"),
        "drawdown_as_pct_of_stage1": dd_ratio,
        "fill_retention_pct_vs_stage1": fill_retention,
        "provenance": str(provenance),
        "provenance_sha256": comparison.sha256_file(provenance),
        "research_only": True,
        "promotion_eligible": False,
    }


def _delta_summary_row(
    record: Mapping[str, Any], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = dict(record["metrics"])
    dd_ratio, fill_retention = _ratio_metrics(metrics, baseline)
    return {
        "row_type": "DELTA_VS_STAGE1",
        "version": f"{record['key']}_MINUS_STAGE1",
        "stage": record["stage"],
        "variant": record["variant"],
        "baseline": BASELINE_KEY,
        "from_day": metrics.get("from_day"),
        "through_day": metrics.get("through_day"),
        **{
            field: stage2_package._delta_value(
                metrics.get(field), baseline.get(field)
            )
            for field in SUMMARY_FIELDS
        },
        "excluded_best_net_day": "",
        "drawdown_as_pct_of_stage1": dd_ratio - 100.0,
        "fill_retention_pct_vs_stage1": fill_retention - 100.0,
        "provenance": "",
        "provenance_sha256": "",
        "research_only": True,
        "promotion_eligible": False,
    }


def _summary_rows(
    records: Sequence[Mapping[str, Any]], baseline: Mapping[str, Any]
) -> list[dict[str, Any]]:
    absolute = [_absolute_summary_row(record, baseline) for record in records]
    deltas = [
        _delta_summary_row(record, baseline)
        for record in records
        if record["key"] != BASELINE_KEY
    ]
    return [*absolute, *deltas]


def _isolated_summary_rows(
    baseline: Mapping[str, Any], target: Mapping[str, Any], target_stage: str
) -> list[dict[str, Any]]:
    rows = stage2_package._summary_rows(baseline, target)
    for row in rows:
        row["stage"] = target_stage
        row["base_selection_decisions"] = target.get("base_selection_decisions")
        row["selection_passed"] = target.get("selection_passed")
        row["selection_rejected"] = target.get("selection_rejected")
        row["selection_retention_pct"] = target.get("selection_retention_pct")
    return rows


def _train_test_rows(
    records: Sequence[Mapping[str, Any]],
    payloads: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    periods = {
        str(record["key"]): stage3_package._period_metrics(
            payloads[str(record["key"])], str(record["key"])
        )
        for record in records
    }
    rows: list[dict[str, Any]] = []
    for record in records:
        key = str(record["key"])
        for period in ("TRAIN", "TEST"):
            observed = periods[key][period]
            rows.append(
                {
                    "row_type": "ABSOLUTE",
                    "version": key,
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": BASELINE_KEY,
                    "period": period,
                    **{
                        field: observed.get(field)
                        for field in stage3_package.PERIOD_METRIC_FIELDS
                    },
                }
            )
    baseline_periods = periods[BASELINE_KEY]
    for record in records:
        key = str(record["key"])
        if key == BASELINE_KEY:
            continue
        for period in ("TRAIN", "TEST"):
            observed = periods[key][period]
            baseline = baseline_periods[period]
            rows.append(
                {
                    "row_type": "DELTA_VS_STAGE1",
                    "version": f"{key}_MINUS_STAGE1",
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": BASELINE_KEY,
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


def _daywise_frame(
    keys: Sequence[str], payloads: Mapping[str, Mapping[str, Any]]
) -> pd.DataFrame:
    daily = {key: stage3_package._read_daily(payloads[key], key) for key in keys}
    baseline = daily[BASELINE_KEY]
    dates = baseline["session_date"].astype(str).tolist()
    periods = baseline["period"].astype(str).tolist()
    out = baseline[["session_date", "period"]].copy()
    fields = ("candidates", "fills", "net_return_pct", "net_pnl_rs")
    for key in keys:
        current = daily[key]
        if current["session_date"].astype(str).tolist() != dates:
            raise ValueError(f"{key} daily sessions differ from Stage 1")
        if current["period"].astype(str).tolist() != periods:
            raise ValueError(f"{key} daily TRAIN/TEST labels differ")
        prefix = key.lower()
        for field in fields:
            out[f"{prefix}_{field}"] = current[field].to_numpy()
        out[f"{prefix}_cumulative_net_return_points"] = current[
            "net_return_pct"
        ].cumsum()
    for key in keys:
        if key == BASELINE_KEY:
            continue
        prefix = key.lower()
        for field in fields:
            out[f"delta_{prefix}_vs_stage1_{field}"] = (
                out[f"{prefix}_{field}"] - out[f"stage1_{field}"]
            )
        out[f"delta_{prefix}_vs_stage1_cumulative_net_return_points"] = (
            out[f"{prefix}_cumulative_net_return_points"]
            - out["stage1_cumulative_net_return_points"]
        )
    return out


def _diagnostic_frame(payload: Mapping[str, Any], label: str) -> pd.DataFrame:
    frame = pd.read_csv(comparison.output_path(payload, "diagnostic_breakdowns"))
    required = {"dimension", "bucket", *DIAGNOSTIC_FIELDS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} diagnostic artifact misses columns: {missing}")
    selected = frame.loc[frame["dimension"].astype(str).isin(["side", "setup_id"])].copy()
    if selected.duplicated(["dimension", "bucket"]).any():
        raise ValueError(f"{label} diagnostic side/setup buckets are duplicated")
    return selected


def _side_setup_rows(
    records: Sequence[Mapping[str, Any]],
    payloads: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    frames = {
        str(record["key"]): _diagnostic_frame(
            payloads[str(record["key"])], str(record["key"])
        )
        for record in records
    }
    rows: list[dict[str, Any]] = []
    for record in records:
        key = str(record["key"])
        for raw in frames[key].to_dict("records"):
            rows.append(
                {
                    "row_type": "ABSOLUTE",
                    "version": key,
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": BASELINE_KEY,
                    "dimension": raw["dimension"],
                    "bucket": raw["bucket"],
                    **{field: raw.get(field) for field in DIAGNOSTIC_FIELDS},
                }
            )
    baseline_map = {
        (str(row["dimension"]), str(row["bucket"])): row
        for row in frames[BASELINE_KEY].to_dict("records")
    }
    additive = set(DIAGNOSTIC_FIELDS) - {"profit_factor"}
    for record in records:
        key = str(record["key"])
        if key == BASELINE_KEY:
            continue
        current_map = {
            (str(row["dimension"]), str(row["bucket"])): row
            for row in frames[key].to_dict("records")
        }
        for dimension, bucket in sorted(set(baseline_map) | set(current_map)):
            baseline = baseline_map.get((dimension, bucket), {})
            current = current_map.get((dimension, bucket), {})
            values: dict[str, Any] = {}
            for field in DIAGNOSTIC_FIELDS:
                if field in additive:
                    values[field] = stage2_package._delta_value(
                        current.get(field, 0.0), baseline.get(field, 0.0)
                    )
                elif field in current and field in baseline:
                    values[field] = stage2_package._delta_value(
                        current.get(field), baseline.get(field)
                    )
                else:
                    values[field] = ""
            rows.append(
                {
                    "row_type": "DELTA_VS_STAGE1",
                    "version": f"{key}_MINUS_STAGE1",
                    "stage": record["stage"],
                    "variant": record["variant"],
                    "baseline": BASELINE_KEY,
                    "dimension": dimension,
                    "bucket": bucket,
                    **values,
                }
            )
    return rows


def _funnel_rows(
    *,
    stage: str,
    variant: str,
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    filled: pd.DataFrame,
) -> list[dict[str, Any]]:
    reasons = decisions["selection_reason"].astype(str)
    passed = int(reasons.eq("PASSED").sum())
    base = int(len(decisions))
    raw_rows = [
        (1, "BASE_SELECTION_DECISIONS", "ALL", base),
        (2, "SELECTION_PASSED", "PASSED", passed),
    ]
    order = 3
    for reason, count in sorted(Counter(reasons.loc[reasons.ne("PASSED")]).items()):
        raw_rows.append((order, "SELECTION_REJECTED", reason, int(count)))
        order += 1
    raw_rows.extend(
        [
            (order, "STATE_MACHINE_REPLAYED", "ALL", int(len(audit))),
            (order + 1, "FILLED", "FILLED_TRUE", int(len(filled))),
        ]
    )
    return [
        {
            "stage": stage,
            "variant": variant,
            "step_order": step_order,
            "step": step,
            "reason": reason,
            "count": count,
            "pct_of_base_candidates": 100.0 * count / base if base else math.nan,
        }
        for step_order, step, reason, count in raw_rows
    ]


def _gate_rows(
    *,
    plan: Mapping[str, Any],
    target_stage: str,
    baseline_metrics: Mapping[str, Any],
    target_metrics: Mapping[str, Any],
    decisions: pd.DataFrame,
    rvol_binding: Mapping[str, Any] | None,
    rvol_size_attestation: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    definition = _target_definition(target_stage)
    screen = dict(plan["stage_02_preliminary_performance_screen"])
    dd_ratio, fill_retention = _ratio_metrics(target_metrics, baseline_metrics)
    reasons = decisions["selection_reason"].astype(str)
    rejected = int(reasons.ne("PASSED").sum())
    allowed = {"PASSED", *definition["allowed_rejections"]}
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
                "stage": target_stage,
                "variant": definition["variant"],
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

    add("INTEGRITY", "valid_provenance_required", "EQ", True, True, "PASS", "Complete frozen lineage validated")
    add("INTEGRITY", "expected_variant", "EQ", definition["variant"], definition["variant"], "PASS", "Frozen sequence identity")
    add("INTEGRITY", "base_selection_decisions", "EQ", BASE_DECISION_COUNT, len(decisions), stage2_package._gate_status(len(decisions), "EQ", BASE_DECISION_COUNT), "Frozen V10B candidate superset")
    add("INTEGRITY", "passed_decisions_reconcile_to_replay", "EQ", target_metrics["candidates"], target_metrics["selection_passed"], stage2_package._gate_status(target_metrics["selection_passed"], "EQ", target_metrics["candidates"]), "Filtered candidates were replayed")
    add("INTEGRITY", "selection_changed", "GE", 1, rejected, stage2_package._gate_status(rejected, "GE", 1), "Selection-changing mechanism rejected candidates")
    add("INTEGRITY", "rejection_reason_whitelist", "SUBSET", sorted(allowed), sorted(set(reasons)), "PASS", "Every rejection matches the frozen mechanism")
    add("INTEGRITY", "same_frozen_inputs_as_control", "EQ", True, True, "PASS", "Runner, cache, snapshot, window, costs, universe and portfolio validated")
    add("INTEGRITY", "full_state_machine_replay_required", "EQ", True, True, "PASS", "Filtered candidate audit is bound")
    if experiment_config.get_spec(definition["variant"]).uses_slot_rvol20:
        add("INTEGRITY", "rvol_sidecar_provenance", "PRESENT", True, bool(rvol_binding), "PASS" if rvol_binding else "FAIL", "Causal run-local sidecar is hash-bound")
        exact_defect = bool(
            rvol_size_attestation
            and dict(rvol_size_attestation.get("defect_signature", {})).get(
                "binding_table_bytes"
            )
            == -1
            and rvol_size_attestation.get("defect_code")
            == RVOL_SIZE_DEFECT_CODE
        )
        attested_bytes = dict(
            dict(rvol_size_attestation or {}).get("attested_binding", {})
        ).get("table_bytes")
        add(
            "INTEGRITY",
            "rvol_frozen_size_defect_signature",
            "EQ",
            True,
            exact_defect,
            "PASS" if exact_defect else "FAIL",
            "Exact frozen table_bytes=-1 bytes/size key defect is preserved",
        )
        add(
            "INTEGRITY",
            "rvol_metadata_size_attestation",
            "GE",
            1,
            attested_bytes,
            stage2_package._gate_status(attested_bytes, "GE", 1),
            "Actual table size and parquet row count were independently attested",
        )

    performance = (
        ("minimum_fills", "GE", screen["minimum_fills"], target_metrics["fills"]),
        ("minimum_active_sessions", "GE", screen["minimum_active_sessions"], target_metrics["active_sessions"]),
        ("minimum_profit_factor", "GE", screen["minimum_profit_factor"], target_metrics["profit_factor"]),
        ("minimum_profit_factor_excluding_best_net_day", "GE", screen["minimum_profit_factor_excluding_best_net_day"], target_metrics["profit_factor_excluding_best_net_day"]),
        ("maximum_best_positive_day_share_pct", "LE", screen["maximum_best_positive_day_share_pct"], target_metrics["best_positive_day_share_pct"]),
        ("minimum_long_profit_factor", "GE", screen["minimum_long_profit_factor"], target_metrics["long_profit_factor"]),
        ("minimum_short_profit_factor", "GE", screen["minimum_short_profit_factor"], target_metrics["short_profit_factor"]),
        ("maximum_drawdown_as_pct_of_control", "LE", screen["maximum_drawdown_as_pct_of_control"], dd_ratio),
    )
    for gate, comparator_name, threshold, observed in performance:
        add("PRELIMINARY_PERFORMANCE", gate, comparator_name, threshold, observed, stage2_package._gate_status(observed, comparator_name, threshold), "Research screen only; not promotion authority")
    add("REQUIRED_REPORTING", "accuracy_pct_reported", "PRESENT", True, target_metrics["win_rate_pct"], "PASS" if stage2_package._is_evaluable(target_metrics["win_rate_pct"]) else "FAIL", "Accuracy is reported")
    add("REQUIRED_REPORTING", "fill_retention_pct_reported", "PRESENT", True, fill_retention, "PASS" if stage2_package._is_evaluable(fill_retention) else "FAIL", "Fill retention is reported")
    add("REQUIRED_REPORTING", "selection_retention_pct_reported", "PRESENT", True, target_metrics["selection_retention_pct"], "PASS" if stage2_package._is_evaluable(target_metrics["selection_retention_pct"]) else "FAIL", "Selection retention is reported")
    add("STRESS", "stress_20bps_plus_2bps", "DEFERRED", screen["stress_20bps_plus_2bps"], "DEFERRED_NOT_EVALUATED", "DEFERRED", "No stressed backtest was authorized")
    add("AUTHORITY", "screen_is_promotion_authority", "EQ", False, False, "PASS", "Gate results cannot promote this variant")
    return rows


def _test_results_payload(
    *,
    tests_passed: int | None,
    tests_failed: int | None,
    test_commands: Sequence[str],
    schema_version: str,
) -> dict[str, Any]:
    payload = stage2_package._test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )
    payload["schema_version"] = schema_version
    return payload


def build_stage_package(
    *,
    target_stage: str,
    stage_plan: Path,
    provenance_by_stage: Mapping[str, Path],
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    normalized_stage = str(target_stage).upper().strip()
    definition = _target_definition(normalized_stage)
    (
        package_schema_version,
        test_evidence_schema_version,
        variant_archive_schema_version,
    ) = _package_schema_versions(normalized_stage)
    keys = _required_keys(normalized_stage)
    target_key = str(definition["key"])
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(
            f"{normalized_stage} output directory already exists: {target}"
        )
    plan_path = _require_file(stage_plan, "Frozen experiment plan")
    missing_paths = sorted(set(keys) - set(provenance_by_stage))
    if missing_paths:
        raise ValueError(f"Missing lineage provenance paths: {missing_paths}")
    paths = {
        key: _require_file(Path(provenance_by_stage[key]), f"{key} provenance")
        for key in keys
    }
    if len({_path_key(path) for path in paths.values()}) != len(paths):
        raise ValueError("Every lineage provenance path must be distinct")

    plan, plan_entries = _load_target_plan(plan_path, normalized_stage)
    stage1_manifest_path, stage1_manifest = stage3_package._validate_stage1_v8_binding(
        plan=plan, v8_path=paths["STAGE0"], stage1_path=paths["STAGE1"]
    )
    payloads = {
        key: comparison.validate_provenance(path) for key, path in paths.items()
    }
    stage3_package._require_v8_identity(payloads["STAGE0"], "STAGE0")
    config_hashes: dict[str, str] = {}
    for key in keys:
        if key == "STAGE0":
            continue
        variant = FIXED_IDENTITIES[key][1]
        config_sha = experiment_config.variant_config_sha256(variant)
        config_hashes[key] = config_sha
        stage2_package._require_experiment_identity(
            payloads[key],
            variant=variant,
            variant_config_sha256=config_sha,
            label=key,
        )
        stage_number = int(key.removeprefix("STAGE"))
        if stage_number >= 5:
            entry = plan_entries[f"STAGE_{stage_number:02d}"]
            if entry["variant_config_sha256"] != config_sha:
                raise ValueError(f"{key} config hash differs from frozen plan")
    stage3_package._validate_v8_against_stage1(
        plan=plan, v8=payloads["STAGE0"], stage1=payloads["STAGE1"]
    )

    snapshot_path: Path | None = None
    expected_window: dict[str, Any] | None = None
    for key in keys:
        if key in {"STAGE0", "STAGE1"}:
            continue
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
        raise AssertionError("Frozen-input lineage validation did not execute")

    stage1_decisions, stage1_audit = stage2_package._validate_selection_and_audit(
        payloads["STAGE1"],
        variant="V10B",
        variant_config_sha256=config_hashes["STAGE1"],
        expected_candidates=BASE_DECISION_COUNT,
        expected_overlay="BASE_V10B_SELECTION",
        label="STAGE1",
    )
    base_ids = stage1_decisions["candidate_id"].astype(str).tolist()
    audits: dict[str, pd.DataFrame] = {
        "STAGE0": stage3_package._load_v8_audit(
            payloads["STAGE0"], BASE_DECISION_COUNT
        ),
        "STAGE1": stage1_audit,
    }
    decisions: dict[str, pd.DataFrame] = {"STAGE1": stage1_decisions}
    selection: dict[str, dict[str, Any]] = {
        "STAGE0": _selection_metadata(
            base_count=BASE_DECISION_COUNT, passed_count=BASE_DECISION_COUNT
        ),
        "STAGE1": _selection_metadata(
            base_count=BASE_DECISION_COUNT, passed_count=BASE_DECISION_COUNT
        ),
    }
    if audits["STAGE0"]["candidate_id"].astype(str).tolist() != base_ids:
        raise ValueError("Stage 0 and Stage 1 ordered candidate sets differ")
    for key in keys:
        if key in {"STAGE0", "STAGE1"}:
            continue
        stage_number = int(key.removeprefix("STAGE"))
        if stage_number <= 4:
            variant = FIXED_IDENTITIES[key][1]
            decisions[key], audits[key] = stage2_package._validate_selection_and_audit(
                payloads[key],
                variant=variant,
                variant_config_sha256=config_hashes[key],
                expected_candidates=BASE_DECISION_COUNT,
                expected_overlay="BASE_V10B_SELECTION",
                label=key,
            )
            if decisions[key]["candidate_id"].astype(str).tolist() != base_ids:
                raise ValueError(f"{key} base candidate order differs")
            selection[key] = _selection_metadata(
                base_count=BASE_DECISION_COUNT, passed_count=BASE_DECISION_COUNT
            )
        else:
            decisions[key], audits[key], selection[key] = _validate_selection_variant(
                payloads[key],
                stage=f"STAGE_{stage_number:02d}",
                base_candidate_ids=base_ids,
            )
    stage3_package._require_chronology(
        tuple((key, payloads[key]) for key in keys)
    )

    rvol_lineage: dict[
        str, tuple[dict[str, Any], dict[str, Any]]
    ] = {}
    for key in keys:
        uses_rvol = experiment_config.get_spec(FIXED_IDENTITIES[key][1]).uses_slot_rvol20 if key != "STAGE0" else False
        binding = payloads[key].get("v10_experiment_feature_input_binding")
        if uses_rvol != isinstance(binding, Mapping):
            raise ValueError(f"{key} RVOL feature-binding presence is inconsistent")
        if uses_rvol:
            rvol_lineage[key] = _validate_rvol_sidecar(
                payloads[key],
                paths[key],
                plan,
                FIXED_IDENTITIES[key][0],
            )
    rvol_binding: dict[str, Any] | None = None
    rvol_size_attestation: dict[str, Any] | None = None
    if target_key in rvol_lineage:
        rvol_binding, rvol_size_attestation = rvol_lineage[target_key]
    if len(rvol_lineage) > 1:
        first_key = next(iter(rvol_lineage))
        first_binding, first_attestation = rvol_lineage[first_key]
        for key, (binding, attestation) in rvol_lineage.items():
            for field in (
                "schema_version",
                "input_fingerprint",
                "source_manifest_sha256",
                "table_sha256",
                "row_count",
            ):
                if binding[field] != first_binding[field]:
                    raise ValueError(
                        f"{key} RVOL {field} differs from {first_key}"
                    )
            observed_bytes = dict(attestation["attested_binding"])["table_bytes"]
            expected_bytes = dict(first_attestation["attested_binding"])[
                "table_bytes"
            ]
            if observed_bytes != expected_bytes:
                raise ValueError(
                    f"{key} RVOL attested size differs from {first_key}"
                )
    rvol_attestation_filename = (
        _rvol_attestation_filename(normalized_stage)
        if rvol_size_attestation is not None
        else None
    )
    rvol_lineage_summary = {
        key: {
            "stage": FIXED_IDENTITIES[key][0],
            "variant": FIXED_IDENTITIES[key][1],
            "provenance_path": dict(attestation["source_provenance"])["path"],
            "provenance_sha256": dict(attestation["source_provenance"])["sha256"],
            "input_fingerprint": binding["input_fingerprint"],
            "source_manifest_sha256": binding["source_manifest_sha256"],
            "table_sha256": binding["table_sha256"],
            "original_table_bytes": binding["table_bytes"],
            "attested_table_bytes": dict(attestation["attested_binding"])[
                "table_bytes"
            ],
            "parquet_metadata_row_count": dict(
                dict(attestation["validated_artifacts"])["run_table"]
            )["parquet_metadata_row_count"],
            "defect_code": attestation["defect_code"],
        }
        for key, (binding, attestation) in rvol_lineage.items()
    }

    metrics = {
        key: _augment_metrics(
            stage2_package._metric_bundle(payloads[key], audits[key], label=key),
            selection[key],
        )
        for key in keys
    }
    records = _version_records(keys=keys, paths=paths, metrics=metrics)
    all_summary = _summary_rows(records, metrics[BASELINE_KEY])
    isolated_summary = _isolated_summary_rows(
        metrics[BASELINE_KEY], metrics[target_key], normalized_stage
    )
    train_test_rows = _train_test_rows(records, payloads)
    daywise = _daywise_frame(keys, payloads)
    side_setup_rows = _side_setup_rows(records, payloads)
    target_filled = stage2_package._filled_rows(audits[target_key], target_key)
    funnel_rows = _funnel_rows(
        stage=normalized_stage,
        variant=definition["variant"],
        decisions=decisions[target_key],
        audit=audits[target_key],
        filled=target_filled,
    )
    gate_rows = _gate_rows(
        plan=plan,
        target_stage=normalized_stage,
        baseline_metrics=metrics[BASELINE_KEY],
        target_metrics=metrics[target_key],
        decisions=decisions[target_key],
        rvol_binding=rvol_binding,
        rvol_size_attestation=rvol_size_attestation,
    )
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
        schema_version=test_evidence_schema_version,
    )

    comparison_dir = target / str(definition["comparison_dir"])
    comparison.build_comparison(
        control_provenance=paths[BASELINE_KEY],
        challenger_provenance=paths[target_key],
        output_dir=comparison_dir,
        require_control_parity=False,
    )
    stage2_package._atomic_copy(plan_path, target / PLAN_FILENAME)
    if comparison.sha256_file(target / PLAN_FILENAME) != EXPECTED_STAGE_PLAN_SHA256:
        raise AssertionError("Frozen experiment plan copy is not byte-identical")

    variant_archive = {
        "schema_version": variant_archive_schema_version,
        "package_schema_version": package_schema_version,
        "stage": normalized_stage,
        "variant": definition["variant"],
        "mechanism": plan_entries[normalized_stage].get("mechanism"),
        "frozen_plan_sha256": EXPECTED_STAGE_PLAN_SHA256,
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": config_hashes[target_key],
        "variant_config": experiment_config.variant_config_payload(
            definition["variant"]
        ),
        "resolved_entry_policy": stage2_package._parameters(
            payloads[target_key]
        ).get("entry_policy"),
        "selection_rejection_reasons": dict(
            sorted(Counter(decisions[target_key]["selection_reason"].astype(str)).items())
        ),
        "rvol_feature_input_binding": rvol_binding,
        "rvol_size_attestation": rvol_size_attestation,
        **(
            {"rvol_lineage_attestations": rvol_lineage_summary}
            if normalized_stage == "STAGE_09"
            else {}
        ),
        "lineage_provenance": {
            key: {
                "path": str(paths[key]),
                "sha256": comparison.sha256_file(paths[key]),
            }
            for key in keys
        },
        "research_only": True,
        "promotion_eligible": False,
    }
    _write_json(target / "variant_config.json", variant_archive)
    comparison.atomic_write_csv(target / "summary.csv", [stage2_package._json_safe(row) for row in isolated_summary])
    comparison.atomic_write_csv(target / "all_versions_summary.csv", [stage2_package._json_safe(row) for row in all_summary])
    comparison.atomic_write_csv(target / "cumulative_train_test.csv", [stage2_package._json_safe(row) for row in train_test_rows])
    stage2_package._atomic_write_frame(target / "cumulative_daywise.csv", daywise)
    comparison.atomic_write_csv(target / "cumulative_side_and_setup.csv", [stage2_package._json_safe(row) for row in side_setup_rows])
    stage2_package._atomic_copy(comparison_dir / "daywise_comparison.csv", target / "daywise_comparison.csv")
    stage2_package._atomic_copy(comparison_dir / "side_and_leg_comparison.csv", target / "side_and_setup_comparison.csv")
    comparison.atomic_write_csv(target / "funnel.csv", [stage2_package._json_safe(row) for row in funnel_rows])
    stage2_package._atomic_write_frame(target / "filled_trade_ledger.csv", target_filled)
    rejected = decisions[target_key].loc[
        decisions[target_key]["selection_reason"].astype(str).ne("PASSED")
    ].copy()
    stage2_package._atomic_write_frame(target / "selection_rejections.csv", rejected)
    comparison.atomic_write_csv(target / "preliminary_gates.csv", [stage2_package._json_safe(row) for row in gate_rows])
    _write_json(target / "test_results.json", test_results)
    if rvol_binding is not None:
        sidecar_dir = target / "rvol_sidecar"
        sidecar_dir.mkdir(parents=False, exist_ok=False)
        stage2_package._atomic_copy(
            Path(str(rvol_binding["manifest_path"])),
            sidecar_dir / "slot_rvol20_manifest.json",
        )
        stage2_package._atomic_copy(
            Path(str(rvol_binding["table_path"])),
            sidecar_dir / "slot_rvol20.parquet",
        )
        if comparison.sha256_file(sidecar_dir / "slot_rvol20.parquet") != (
            rvol_binding["table_sha256"]
        ):
            raise AssertionError("Packaged RVOL table differs from run binding")
        if rvol_size_attestation is None:
            raise AssertionError(f"{normalized_stage} RVOL size attestation is missing")
        attested_table_bytes = int(
            dict(rvol_size_attestation["attested_binding"])["table_bytes"]
        )
        if int((sidecar_dir / "slot_rvol20.parquet").stat().st_size) != (
            attested_table_bytes
        ):
            raise AssertionError("Packaged RVOL table size differs from attestation")
        if comparison.sha256_file(sidecar_dir / "slot_rvol20_manifest.json") != (
            rvol_binding["manifest_sha256"]
        ):
            raise AssertionError("Packaged RVOL manifest differs from run binding")
        _write_json(
            target / str(rvol_attestation_filename),
            rvol_size_attestation,
        )

    gate_counts = Counter(str(row["status"]) for row in gate_rows)
    dd_ratio, fill_retention = _ratio_metrics(
        metrics[target_key], metrics[BASELINE_KEY]
    )
    tests_line = (
        "not recorded"
        if test_results["tests_total"] is None
        else f"{test_results['tests_passed']} passed / {test_results['tests_failed']} failed"
    )
    rvol_attestation_line = ""
    if rvol_size_attestation is not None:
        original_bytes = int(
            dict(rvol_size_attestation["original_binding"])["table_bytes"]
        )
        attested_bytes = int(
            dict(rvol_size_attestation["attested_binding"])["table_bytes"]
        )
        rvol_attestation_line = (
            "- Native RVOL binding: **DEFECTIVE table_bytes=-1; original run "
            "unchanged**\n"
            "- RVOL metadata size attestation: **PASS** "
            f"(immutable binding {original_bytes}; metadata-attested "
            f"{attested_bytes} bytes)\n"
        )
    decision_text = f"""# FNO V10 {normalized_stage} - {definition['variant']}

- Decision: **{definition['decision']}**
- Frozen plan SHA256: `{EXPECTED_STAGE_PLAN_SHA256}`
- Target provenance: `{paths[target_key]}`
- Variant config SHA256: `{config_hashes[target_key]}`
- Window: {expected_window['from_day']} through {expected_window['through_day']}
- Base decisions / retained / rejected: {metrics[target_key]['base_selection_decisions']} / {metrics[target_key]['selection_passed']} / {metrics[target_key]['selection_rejected']}
- Selection retention: {float(metrics[target_key]['selection_retention_pct']):.6f}%
- Fills / active sessions: {int(metrics[target_key]['fills'])} / {int(metrics[target_key]['active_sessions'])}
- Accuracy: {float(metrics[target_key]['win_rate_pct']):.6f}%
- Fill retention versus Stage 1: {fill_retention:.6f}%
- Profit factor: {float(metrics[target_key]['profit_factor']):.12f}
- Profit factor excluding best net day: {float(metrics[target_key]['profit_factor_excluding_best_net_day']):.12f}
- Best positive day share: {float(metrics[target_key]['best_positive_day_share_pct']):.6f}%
- LONG / SHORT profit factor: {float(metrics[target_key]['long_profit_factor']):.12f} / {float(metrics[target_key]['short_profit_factor']):.12f}
- Drawdown versus Stage 1: {dd_ratio:.6f}%
- Preliminary gates: {gate_counts.get('PASS', 0)} PASS / {gate_counts.get('FAIL', 0)} FAIL / {gate_counts.get('DEFERRED', 0)} DEFERRED
- Stress 20bps + 2bps: **DEFERRED_NOT_EVALUATED**
- Test evidence: {tests_line}
{rvol_attestation_line}

This immutable package records completion of one frozen isolated research
stage. Selection changes were replayed through the full state machine. Gate
outcomes do not authorize live, paper, or strategy promotion.
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
        "cumulative_side_and_setup.csv",
        "daywise_comparison.csv",
        "side_and_setup_comparison.csv",
        "funnel.csv",
        "filled_trade_ledger.csv",
        "selection_rejections.csv",
        "preliminary_gates.csv",
        "test_results.json",
        "decision.md",
    }
    missing_artifacts = sorted(required_top_level - set(artifacts))
    if missing_artifacts:
        raise AssertionError(
            f"{normalized_stage} package misses artifacts: {missing_artifacts}"
        )
    if rvol_binding is not None:
        required_sidecar = {
            "rvol_sidecar/slot_rvol20_manifest.json",
            "rvol_sidecar/slot_rvol20.parquet",
            str(rvol_attestation_filename),
        }
        missing_sidecar = sorted(required_sidecar - set(artifacts))
        if missing_sidecar:
            raise AssertionError(
                f"{normalized_stage} package misses sidecar: {missing_sidecar}"
            )
    manifest = {
        "schema_version": package_schema_version,
        "stage": normalized_stage,
        "variant": definition["variant"],
        "decision": definition["decision"],
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
            key: {
                "stage": FIXED_IDENTITIES[key][0],
                "variant": FIXED_IDENTITIES[key][1],
                "path": str(paths[key]),
                "sha256": comparison.sha256_file(paths[key]),
                "backtest_input_fingerprint": payloads[key].get(
                    "backtest_input_fingerprint"
                ),
            }
            for key in keys
        },
        "source_snapshot_manifest": str(snapshot_path),
        "source_snapshot_manifest_sha256": comparison.sha256_file(snapshot_path),
        "source_snapshot_fingerprint": dict(
            payloads[target_key].get("source_snapshot", {})
        ).get("snapshot_fingerprint"),
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "variant_config_sha256": config_hashes[target_key],
        "experiment_runner_source_sha256": payloads[target_key].get(
            "experiment_runner_source_sha256"
        ),
        "experiment_config_source_sha256": payloads[target_key].get(
            "experiment_config_source_sha256"
        ),
        "cache_input_fingerprint": payloads[target_key].get(
            "cache_input_fingerprint"
        ),
        "backtest_window": expected_window,
        "fixed_economics_contract": comparison.fixed_economics_contract(
            payloads[target_key]
        ),
        "selection": selection[target_key],
        "selection_reason_counts": dict(sorted(Counter(decisions[target_key]["selection_reason"].astype(str)).items())),
        "rvol_feature_input_binding": rvol_binding,
        **(
            {"rvol_lineage_attestations": rvol_lineage_summary}
            if normalized_stage == "STAGE_09"
            else {}
        ),
        "rvol_size_attestation": (
            {
                "path": str(rvol_attestation_filename),
                "sha256": artifacts[str(rvol_attestation_filename)]["sha256"],
                "original_table_bytes": int(
                    dict(rvol_size_attestation["original_binding"])["table_bytes"]
                ),
                "attested_table_bytes": int(
                    dict(rvol_size_attestation["attested_binding"])["table_bytes"]
                ),
                "parquet_metadata_row_count": int(
                    dict(
                        dict(rvol_size_attestation["validated_artifacts"])[
                            "run_table"
                        ]
                    )["parquet_metadata_row_count"]
                ),
                "defect_code": rvol_size_attestation["defect_code"],
            }
            if rvol_size_attestation is not None
            else None
        ),
        "comparison_directory": definition["comparison_dir"],
        "isolated_summary": isolated_summary,
        "all_versions_summary": all_summary,
        "preliminary_gate_status_counts": dict(sorted(gate_counts.items())),
        "stress_status": "DEFERRED_NOT_EVALUATED",
        "test_evidence": test_results,
        "artifact_hash_scope": "ALL_PACKAGE_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_SELF",
        "artifacts": artifacts,
    }
    _write_json(target / "stage_manifest.json", manifest)
    return target


def _arg_name_for_key(key: str) -> str:
    return {
        "STAGE0": "stage0_provenance",
        "STAGE1": "stage1_control_provenance",
        "STAGE2": "stage2_rv1_provenance",
        "STAGE3": "stage3_expiry_provenance",
        "STAGE4": "stage4_combination_provenance",
        "STAGE5": "stage5_no_0935_provenance",
        "STAGE6": "stage6_move_030_provenance",
        "STAGE7": "stage7_move_040_provenance",
        "STAGE8": "stage8_rvol_150_provenance",
        "STAGE9": "stage9_rvol_200_provenance",
    }[key]


def parse_args_for_stage(
    target_stage: str, argv: Sequence[str] | None = None
) -> argparse.Namespace:
    normalized_stage = str(target_stage).upper().strip()
    keys = _required_keys(normalized_stage)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-plan", type=Path, default=WORKSPACE_ROOT / PLAN_FILENAME)
    parser.add_argument("--stage0-provenance", "--v8-provenance", dest="stage0_provenance", type=Path, required=True)
    parser.add_argument("--stage1-control-provenance", type=Path, required=True)
    parser.add_argument("--stage2-rv1-provenance", dest="stage2_rv1_provenance", type=Path, required=True)
    parser.add_argument("--stage3-expiry-provenance", dest="stage3_expiry_provenance", type=Path, required=True)
    parser.add_argument("--stage4-combination-provenance", dest="stage4_combination_provenance", type=Path, required=True)
    optional_arguments = {
        "STAGE5": ("--stage5-no-0935-provenance", "stage5_no_0935_provenance"),
        "STAGE6": ("--stage6-move-030-provenance", "stage6_move_030_provenance"),
        "STAGE7": ("--stage7-move-040-provenance", "stage7_move_040_provenance"),
        "STAGE8": ("--stage8-rvol-150-provenance", "stage8_rvol_150_provenance"),
        "STAGE9": ("--stage9-rvol-200-provenance", "stage9_rvol_200_provenance"),
    }
    for key, (flag, destination) in optional_arguments.items():
        if key in keys:
            aliases = [flag]
            if key == _target_definition(normalized_stage)["key"]:
                aliases.append("--challenger-provenance")
            parser.add_argument(*aliases, dest=destination, type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument("--test-command", action="append", default=[])
    return parser.parse_args(argv)


def main_for_stage(
    target_stage: str, argv: Sequence[str] | None = None
) -> int:
    args = parse_args_for_stage(target_stage, argv)
    provenance_by_stage = {
        key: getattr(args, _arg_name_for_key(key)) for key in _required_keys(target_stage)
    }
    output = build_stage_package(
        target_stage=target_stage,
        stage_plan=args.stage_plan,
        provenance_by_stage=provenance_by_stage,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-stage", choices=sorted(STAGE_DEFINITIONS), required=True)
    known, remaining = parser.parse_known_args(argv)
    return parse_args_for_stage(known.target_stage, remaining)


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--target-stage", choices=sorted(STAGE_DEFINITIONS), required=True)
    known, remaining = parser.parse_known_args(raw)
    return main_for_stage(known.target_stage, remaining)


if __name__ == "__main__":
    raise SystemExit(main())
