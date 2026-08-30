"""Package the immutable-style Stage 1 V10B runner-parity result.

The utility accepts one validated Stage 0 V10B provenance artifact and one
validated Stage 1 experiment-runner V10B provenance artifact.  It delegates
the economic/state comparison to :mod:`fno_v10_experiment_compare`, requires
exact control parity, then publishes a small provenance-first result package.

It does not run a backtest and does not modify either source run.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

# Direct ``python tools/package_fno_v10_stage1.py`` execution places only the
# tools directory on ``sys.path``.  Add the repository root explicitly so the
# frozen V10 modules are imported from the workspace, not from an installed or
# ambient package.
WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

import fno_v10_experiment_compare as comparison
import fno_v10_experiment_config as experiment_config
import fno_v10_unified_5m_1m_backtest as v10


STAGE_SCHEMA_VERSION = "fno_v10_stage1_result_package_v1"
STAGE = "STAGE_01"
DECISION = "PASS_RUNNER_PARITY_RESEARCH_ONLY"
COMPARISON_DIR_NAME = "stage0_vs_stage1_v10b_comparison"


def _require_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is not a file: {resolved}")
    return resolved


def _parameters_variant(payload: Mapping[str, Any]) -> str:
    return str(dict(payload.get("parameters", {})).get("variant", "")).upper()


def _require_stage0_v10b(payload: Mapping[str, Any]) -> None:
    if payload.get("v10_experiment_run_schema_version"):
        raise ValueError("Stage 0 control cannot be a Stage 1 experiment run")
    if payload.get("v10_run_schema_version") != v10.V10_RUN_SCHEMA_VERSION:
        raise ValueError("Stage 0 control is not a supported V10 unified run")
    if _parameters_variant(payload) != "V10B":
        raise ValueError("Stage 0 control provenance must select V10B")


def _require_stage1_v10b(payload: Mapping[str, Any]) -> None:
    if payload.get("v10_experiment_run_schema_version") != (
        experiment_config.RUN_SCHEMA_VERSION
    ):
        raise ValueError("Stage 1 control is not a supported experiment run")
    if str(payload.get("v10_experiment_variant", "")).upper() != "V10B":
        raise ValueError("Stage 1 control provenance must select V10B")
    if _parameters_variant(payload) != "V10B":
        raise ValueError("Stage 1 run parameters must select V10B")
    if payload.get("v10_experiment_variant_config_sha256") != (
        experiment_config.variant_config_sha256("V10B")
    ):
        raise ValueError("Stage 1 V10B configuration hash is invalid")
    if payload.get("research_only") is not True:
        raise ValueError("Stage 1 control must remain research-only")
    if payload.get("promotion_eligible") is not False:
        raise ValueError("Stage 1 control cannot be promotion-eligible")


def _source_snapshot_manifest(payload: Mapping[str, Any], label: str) -> Path:
    raw = str(dict(payload.get("source_snapshot", {})).get("manifest_path", ""))
    if not raw:
        raise ValueError(f"{label} has no source-snapshot manifest path")
    return _require_file(Path(raw), f"{label} source-snapshot manifest")


def _same_value(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        left_number = float(left)
        right_number = float(right)
    except (TypeError, ValueError):
        return left == right
    if math.isnan(left_number) or math.isnan(right_number):
        return math.isnan(left_number) and math.isnan(right_number)
    return math.isclose(left_number, right_number, rel_tol=1e-12, abs_tol=1e-12)


def _require_aggregate_parity(
    stage0: Mapping[str, Any],
    stage1: Mapping[str, Any],
) -> None:
    fields = (
        "from_day",
        "through_day",
        "sessions",
        "candidates",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "headline_valid",
        "promotion_eligible",
    )
    mismatches = [
        field
        for field in fields
        if not _same_value(stage0.get(field), stage1.get(field))
    ]
    if mismatches:
        raise AssertionError(
            "Stage 1 V10B aggregate metrics differ from Stage 0: "
            f"{mismatches}"
        )


def _test_results_payload(
    *,
    tests_passed: int | None,
    tests_failed: int | None,
    test_commands: Sequence[str],
) -> dict[str, Any]:
    if (tests_passed is None) != (tests_failed is None):
        raise ValueError(
            "--tests-passed and --tests-failed must be supplied together"
        )
    if tests_passed is not None and tests_passed < 0:
        raise ValueError("--tests-passed cannot be negative")
    if tests_failed is not None and tests_failed < 0:
        raise ValueError("--tests-failed cannot be negative")
    if tests_failed:
        raise AssertionError("Cannot publish a PASS package with failing tests")
    commands = [str(command).strip() for command in test_commands]
    if any(not command for command in commands):
        raise ValueError("Recorded test commands cannot be empty")
    if tests_passed is None:
        status = "COUNTS_NOT_RECORDED"
        total = None
    else:
        status = "PASS"
        total = tests_passed + int(tests_failed or 0)
    return {
        "schema_version": "fno_v10_stage1_test_evidence_v1",
        "status": status,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "tests_total": total,
        "commands": commands,
    }


def _registry_payload() -> dict[str, Any]:
    return {
        **experiment_config.registry_payload(),
        "registry_sha256": experiment_config.registry_sha256(),
        "expected_registry_sha256": (
            experiment_config.EXPECTED_EXPERIMENT_REGISTRY_SHA256
        ),
        "variant_config_sha256": {
            spec.variant: experiment_config.variant_config_sha256(spec)
            for spec in experiment_config.EXPERIMENT_SPECS
        },
    }


def _parity_row(
    *,
    stage0_provenance: Path,
    stage1_provenance: Path,
    comparison_manifest: Mapping[str, Any],
    stage0_metrics: Mapping[str, Any],
    stage1_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    parity = dict(comparison_manifest.get("core_audit_parity", {}))
    return {
        "stage": STAGE,
        "decision": DECISION,
        "stage0_variant": stage0_metrics.get("variant"),
        "stage1_variant": stage1_metrics.get("variant"),
        "from_day": stage0_metrics.get("from_day"),
        "through_day": stage0_metrics.get("through_day"),
        "sessions": stage0_metrics.get("sessions"),
        "candidates": stage0_metrics.get("candidates"),
        "fills": stage0_metrics.get("fills"),
        "wins": stage0_metrics.get("wins"),
        "losses": stage0_metrics.get("losses"),
        "win_rate_pct": stage0_metrics.get("win_rate_pct"),
        "profit_factor": stage0_metrics.get("profit_factor"),
        "net_return_points": stage0_metrics.get("net_return_points"),
        "net_pnl_rs": stage0_metrics.get("net_pnl_rs"),
        "max_daily_drawdown_points": stage0_metrics.get(
            "max_daily_drawdown_points"
        ),
        "stage0_audit_rows": parity.get("control_rows"),
        "stage1_audit_rows": parity.get("challenger_rows"),
        "common_audit_columns": parity.get("common_columns"),
        "differing_audit_cells": parity.get("differing_cells"),
        "exact_core_audit_parity": parity.get("parity"),
        "stage0_provenance": str(stage0_provenance),
        "stage1_provenance": str(stage1_provenance),
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


def build_stage1_package(
    *,
    stage0_control_provenance: Path,
    stage1_control_provenance: Path,
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = output_dir.resolve()
    if target.exists():
        raise FileExistsError(f"Stage 1 output directory already exists: {target}")
    stage0_path = _require_file(
        stage0_control_provenance, "Stage 0 control provenance"
    )
    stage1_path = _require_file(
        stage1_control_provenance, "Stage 1 control provenance"
    )
    if stage0_path == stage1_path:
        raise ValueError("Stage 0 and Stage 1 provenance paths must be different")

    experiment_config.validate_registry()
    stage0_payload = comparison.validate_provenance(stage0_path)
    stage1_payload = comparison.validate_provenance(stage1_path)
    _require_stage0_v10b(stage0_payload)
    _require_stage1_v10b(stage1_payload)
    stage0_snapshot = _source_snapshot_manifest(stage0_payload, "Stage 0")
    stage1_snapshot = _source_snapshot_manifest(stage1_payload, "Stage 1")
    if stage0_snapshot != stage1_snapshot:
        raise ValueError(
            "Stage 0 and Stage 1 must reference the same source-snapshot path"
        )

    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )
    comparison_dir = target / COMPARISON_DIR_NAME
    comparison.build_comparison(
        control_provenance=stage0_path,
        challenger_provenance=stage1_path,
        output_dir=comparison_dir,
        require_control_parity=True,
    )

    comparison_manifest_path = comparison_dir / "comparison_manifest.json"
    comparison_manifest = comparison.load_json(comparison_manifest_path)
    parity = dict(comparison_manifest.get("core_audit_parity", {}))
    if parity.get("parity") is not True:
        raise AssertionError("Stage 1 package requires exact V10B audit parity")

    stage0_metrics = comparison.aggregate_row("STAGE0_CONTROL", stage0_payload)
    stage1_metrics = comparison.aggregate_row("STAGE1_CONTROL", stage1_payload)
    _require_aggregate_parity(stage0_metrics, stage1_metrics)
    parity_row = _parity_row(
        stage0_provenance=stage0_path,
        stage1_provenance=stage1_path,
        comparison_manifest=comparison_manifest,
        stage0_metrics=stage0_metrics,
        stage1_metrics=stage1_metrics,
    )

    registry_path = target / "variant_registry.json"
    tests_path = target / "test_results.json"
    parity_path = target / "v10b_parity.csv"
    decision_path = target / "decision.md"
    comparison.atomic_write_text(
        registry_path,
        json.dumps(_registry_payload(), indent=2, sort_keys=True) + "\n",
    )
    comparison.atomic_write_text(
        tests_path,
        json.dumps(test_results, indent=2, sort_keys=True) + "\n",
    )
    comparison.atomic_write_csv(parity_path, [parity_row])

    tests_line = (
        "not recorded"
        if test_results["tests_total"] is None
        else (
            f"{test_results['tests_passed']} passed / "
            f"{test_results['tests_failed']} failed"
        )
    )
    decision = f"""# FNO V10 Stage 1 - isolated experiment runner

- Decision: **{DECISION}**
- Stage 0 control: `{stage0_path}`
- Stage 1 control: `{stage1_path}`
- Source snapshot: `{stage0_snapshot}`
- Window: {parity_row['from_day']} through {parity_row['through_day']}
- Sessions: {parity_row['sessions']}
- Candidates / fills: {parity_row['candidates']} / {parity_row['fills']}
- Wins / losses: {parity_row['wins']} / {parity_row['losses']}
- Win rate: {float(parity_row['win_rate_pct']):.6f}%
- Profit factor: {float(parity_row['profit_factor']):.12f}
- Net return: {float(parity_row['net_return_points']):.12f} points
- Net P&L proxy: Rs {float(parity_row['net_pnl_rs']):.2f}
- Exact common-audit parity: {parity_row['exact_core_audit_parity']}
- Differing common-audit cells: {parity_row['differing_audit_cells']}
- Experiment registry SHA256: `{experiment_config.registry_sha256()}`
- Test evidence: {tests_line}

The Stage 1 runner reproduced the frozen Stage 0 V10B control and is ready for
isolated research challengers. This decision does not authorize live or paper
promotion; all Stage 1 variants remain research-only and promotion-ineligible.
"""
    comparison.atomic_write_text(decision_path, decision)

    artifacts = _recursive_artifacts(target)
    required_top_level = {
        "decision.md",
        "test_results.json",
        "v10b_parity.csv",
        "variant_registry.json",
    }
    missing_top_level = sorted(required_top_level - set(artifacts))
    if missing_top_level:
        raise AssertionError(
            f"Stage 1 package is missing top-level artifacts: {missing_top_level}"
        )
    manifest = {
        "schema_version": STAGE_SCHEMA_VERSION,
        "stage": STAGE,
        "decision": DECISION,
        "research_only": True,
        "promotion_eligible": False,
        "stage0_control_provenance": str(stage0_path),
        "stage0_control_provenance_sha256": comparison.sha256_file(stage0_path),
        "stage1_control_provenance": str(stage1_path),
        "stage1_control_provenance_sha256": comparison.sha256_file(stage1_path),
        "source_snapshot_manifest": str(stage0_snapshot),
        "source_snapshot_manifest_sha256": comparison.sha256_file(
            stage0_snapshot
        ),
        "stage0_input_fingerprint": stage0_payload.get(
            "backtest_input_fingerprint"
        ),
        "stage1_input_fingerprint": stage1_payload.get(
            "backtest_input_fingerprint"
        ),
        "experiment_registry_sha256": experiment_config.registry_sha256(),
        "stage1_v10b_config_sha256": (
            experiment_config.variant_config_sha256("V10B")
        ),
        "test_evidence": test_results,
        "parity": parity_row,
        "comparison_directory": COMPARISON_DIR_NAME,
        "artifact_hash_scope": (
            "ALL_PACKAGE_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_SELF"
        ),
        "artifacts": artifacts,
    }
    comparison.atomic_write_text(
        target / "stage_manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage0-control-provenance", type=Path, required=True
    )
    parser.add_argument(
        "--stage1-control-provenance", type=Path, required=True
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument(
        "--test-command",
        action="append",
        default=[],
        help="Repeat to record each executed test command.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_stage1_package(
        stage0_control_provenance=args.stage0_control_provenance,
        stage1_control_provenance=args.stage1_control_provenance,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
