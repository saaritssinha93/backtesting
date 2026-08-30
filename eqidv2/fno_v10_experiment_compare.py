"""Create separate provenance-first comparison packs for V10 experiments."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

import fno_v10_experiment_backtest as experiment
import fno_v10_experiment_config as experiment_config
import fno_v10_unified_5m_1m_backtest as v10


COMPARISON_SCHEMA_VERSION = "fno_v10_experiment_comparison_v1"
STAGE0_V10B_INPUT_FINGERPRINT = (
    "683bf5b48781d8292be6f6ef82d3ca30f28bcb46b2b1d0a977271b75867aeca3"
)
STAGE0_V10B_ANCHORS = {
    "sessions": 40,
    "candidates": 890,
    "fills": 184,
    "wins": 93,
    "losses": 91,
    "win_rate_pct": 50.54347826086956,
    "profit_factor": 1.8915541214588851,
    "net_return_points": 60.162175365936484,
    "net_pnl_rs": 29890.12232498285,
    "max_daily_drawdown_points": 6.00117548673969,
}
EXACT_PARITY_ARTIFACTS = (
    "daily",
    "diagnostic_breakdowns",
    "coverage",
    "setups",
)
CHALLENGER_INVARIANT_ARTIFACTS = (
    "coverage",
    "setups",
    "cache_manifest_archive",
    "experiment_runner_source_archive",
    "experiment_config_source_archive",
    "launcher_source_archive",
    "neutral_engine_source_archive",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def atomic_write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"Refusing to write empty comparison CSV: {path}")
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def output_path(payload: Mapping[str, Any], name: str) -> Path:
    record = dict(dict(payload.get("outputs", {})).get(name, {}))
    path = Path(str(record.get("path", "")))
    if not path.is_file():
        raise FileNotFoundError(f"Missing provenance output {name}: {path}")
    return path


def validate_provenance(path: Path) -> dict[str, Any]:
    preview = load_json(path)
    if preview.get("v10_experiment_run_schema_version"):
        variant = str(preview.get("v10_experiment_variant", ""))
        experiment.configure_engine(variant)
        return experiment.validate_experiment_run_provenance(path)
    if preview.get("v10_run_schema_version"):
        v10.configure_engine()
        return v10.validate_v10_run_provenance(path)
    raise ValueError(f"Unsupported V10 provenance artifact: {path}")


def aggregate_row(label: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    results = dict(payload.get("results", {}))
    diagnostic = dict(results.get("diagnostic_closed_trade_metrics", {}))
    audit = pd.read_csv(output_path(payload, "candidate_order_audit"))
    filled = audit.loc[
        audit["filled"].astype(str).str.lower().eq("true")
    ].copy()
    returns = pd.to_numeric(filled["net_return_pct"], errors="coerce").dropna()
    wins = int(returns.gt(0).sum())
    losses = int(returns.lt(0).sum())
    return {
        "label": label,
        "variant": str(dict(payload.get("parameters", {})).get("variant", "")),
        "from_day": dict(payload.get("backtest_window", {})).get("from_day"),
        "through_day": dict(payload.get("backtest_window", {})).get("through_day"),
        "sessions": int(results.get("sessions", 0)),
        "candidates": int(results.get("candidates", 0)),
        "fills": int(results.get("fills", 0)),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": 100.0 * wins / len(returns) if len(returns) else math.nan,
        "profit_factor": diagnostic.get("profit_factor"),
        "net_return_points": diagnostic.get("net_return_percentage_points"),
        "net_pnl_rs": diagnostic.get("net_pnl_rs"),
        "max_daily_drawdown_points": diagnostic.get(
            "max_daily_drawdown_percentage_points"
        ),
        "headline_valid": bool(results.get("headline_valid", False)),
        "promotion_eligible": bool(results.get("promotion_eligible", False)),
        "input_fingerprint": payload.get("backtest_input_fingerprint"),
    }


def execution_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    parameters = dict(payload.get("parameters", {}))
    policy = dict(parameters.get("entry_policy", {}))
    policy_fields = (
        "allow_cap_reassignment",
        "buffer_bps",
        "close_location_min",
        "cost_bps",
        "entry_expiry_minute",
        "eod_policy",
        "max_confirmation_minute",
        "midpoint_invalidation",
        "post_confirmation_cancel",
        "same_bar_policy",
        "slippage_bps",
        "square_off",
    )
    return {
        "from_day": parameters.get("from_day"),
        "through_day": parameters.get("through_day"),
        "split_day": parameters.get("split_day"),
        "target_exposure_per_entry_rs": parameters.get(
            "target_exposure_per_entry_rs"
        ),
        "portfolio_mode": parameters.get("portfolio_mode"),
        "entry_policy": {field: policy.get(field) for field in policy_fields},
    }


def fixed_economics_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return invariant economics while excluding declared experiment knobs."""

    contract = execution_contract(payload)
    policy = dict(contract["entry_policy"])
    policy.pop("entry_expiry_minute", None)
    contract["entry_policy"] = policy
    return contract


def assert_comparison_identity(
    control: Mapping[str, Any],
    challenger: Mapping[str, Any],
    *,
    require_control_parity: bool,
) -> None:
    control_variant = str(dict(control.get("parameters", {})).get("variant", ""))
    challenger_variant = str(
        dict(challenger.get("parameters", {})).get("variant", "")
    )
    control_is_experiment = bool(
        control.get("v10_experiment_run_schema_version")
    )
    if not control.get("v10_run_schema_version"):
        raise AssertionError("Control must be a validated V10 run")
    if control_variant != "V10B":
        raise AssertionError("Control variant must be V10B")
    if control_is_experiment and str(
        control.get("v10_experiment_variant", "")
    ) != "V10B":
        raise AssertionError("Experiment control identity must be V10B")
    if not challenger.get("v10_experiment_run_schema_version"):
        raise AssertionError("Challenger must be a Stage 1 experiment run")
    if require_control_parity:
        if execution_contract(control) != execution_contract(challenger):
            raise AssertionError("Control and challenger execution economics differ")
        if control_is_experiment:
            raise AssertionError(
                "Runner acceptance requires the frozen non-experiment Stage 0 control"
            )
        if challenger_variant != "V10B":
            raise AssertionError("Runner acceptance challenger must be V10B")
        if control.get("backtest_input_fingerprint") != (
            STAGE0_V10B_INPUT_FINGERPRINT
        ):
            raise AssertionError("Control is not the frozen Stage 0 V10B run")
    elif fixed_economics_contract(control) != fixed_economics_contract(
        challenger
    ):
        raise AssertionError("Control and challenger fixed economics differ")


def assert_anchor_metrics(row: Mapping[str, Any], *, label: str) -> None:
    integer_fields = ("sessions", "candidates", "fills", "wins", "losses")
    for field in integer_fields:
        if int(row[field]) != int(STAGE0_V10B_ANCHORS[field]):
            raise AssertionError(
                f"{label} anchor mismatch for {field}: {row[field]}"
            )
    float_fields = (
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    )
    for field in float_fields:
        if not math.isclose(
            float(row[field]),
            float(STAGE0_V10B_ANCHORS[field]),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise AssertionError(
                f"{label} anchor mismatch for {field}: {row[field]}"
            )


def artifact_parity_rows(
    control: Mapping[str, Any], challenger: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in EXACT_PARITY_ARTIFACTS:
        control_hash = sha256_file(output_path(control, name))
        challenger_hash = sha256_file(output_path(challenger, name))
        rows.append(
            {
                "artifact": name,
                "control_sha256": control_hash,
                "challenger_sha256": challenger_hash,
                "byte_identical": control_hash == challenger_hash,
            }
        )
    return rows


def challenger_invariant_rows(
    control: Mapping[str, Any], challenger: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Fail closed if an isolated challenger changes non-experiment inputs."""

    challenger_variant = str(
        dict(challenger.get("parameters", {})).get("variant", "")
    )
    spec = experiment_config.get_spec(challenger_variant)
    scalar_fields = (
        "v10_experiment_registry_sha256",
        "experiment_runner_source_sha256",
        "experiment_config_source_sha256",
        "cache_input_fingerprint",
        "v10_unified_contract_sha256",
        "neutral_engine_source_sha256",
    )
    mismatched = [
        field
        for field in scalar_fields
        if control.get(field) != challenger.get(field)
    ]
    if mismatched:
        raise AssertionError(
            f"Challenger changed frozen experiment inputs: {mismatched}"
        )
    if challenger.get("v10_experiment_variant_config_sha256") != (
        experiment_config.variant_config_sha256(spec)
    ):
        raise AssertionError("Challenger variant configuration hash is invalid")
    control_snapshot = dict(control.get("source_snapshot", {})).get(
        "snapshot_fingerprint"
    )
    challenger_snapshot = dict(challenger.get("source_snapshot", {})).get(
        "snapshot_fingerprint"
    )
    if not control_snapshot or control_snapshot != challenger_snapshot:
        raise AssertionError("Challenger source snapshot differs from control")
    policy = dict(dict(challenger.get("parameters", {})).get("entry_policy", {}))
    if int(policy.get("entry_expiry_minute", -1)) != spec.entry_expiry_minute:
        raise AssertionError("Challenger expiry does not match its frozen config")
    observed_rv1 = policy.get("confirmation_volume_ratio_min")
    expected_rv1 = spec.confirmation_volume_ratio_min
    if observed_rv1 is None and expected_rv1 is not None:
        raise AssertionError("Challenger RV1 threshold is missing")
    if observed_rv1 is not None and (
        expected_rv1 is None
        or not math.isclose(
            float(observed_rv1), float(expected_rv1), rel_tol=0.0, abs_tol=1e-12
        )
    ):
        raise AssertionError("Challenger RV1 threshold changed")

    rows: list[dict[str, Any]] = []
    for name in CHALLENGER_INVARIANT_ARTIFACTS:
        control_hash = sha256_file(output_path(control, name))
        challenger_hash = sha256_file(output_path(challenger, name))
        match = control_hash == challenger_hash
        rows.append(
            {
                "artifact": name,
                "control_sha256": control_hash,
                "challenger_sha256": challenger_hash,
                "byte_identical": match,
            }
        )
    changed = [row["artifact"] for row in rows if not row["byte_identical"]]
    if changed:
        raise AssertionError(
            f"Challenger changed invariant artifacts: {changed}"
        )
    return rows


def core_audit_parity(
    control_payload: Mapping[str, Any],
    challenger_payload: Mapping[str, Any],
) -> dict[str, Any]:
    control = pd.read_csv(
        output_path(control_payload, "candidate_order_audit"),
        keep_default_na=False,
        dtype=str,
    )
    challenger = pd.read_csv(
        output_path(challenger_payload, "candidate_order_audit"),
        keep_default_na=False,
        dtype=str,
    )
    missing = sorted(set(control.columns) - set(challenger.columns))
    if missing:
        return {
            "parity": False,
            "control_rows": len(control),
            "challenger_rows": len(challenger),
            "common_columns": 0,
            "differing_cells": None,
            "missing_control_columns": missing,
        }
    common = list(control.columns)
    same_shape = len(control) == len(challenger)
    differing = (
        int((control[common] != challenger[common]).to_numpy().sum())
        if same_shape
        else None
    )
    return {
        "parity": bool(same_shape and differing == 0),
        "control_rows": int(len(control)),
        "challenger_rows": int(len(challenger)),
        "common_columns": int(len(common)),
        "differing_cells": differing,
        "missing_control_columns": missing,
    }


def daywise_rows(
    control_payload: Mapping[str, Any],
    challenger_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    control = pd.read_csv(output_path(control_payload, "daily"))
    challenger = pd.read_csv(output_path(challenger_payload, "daily"))
    merged = control.merge(
        challenger,
        on="session_date",
        how="outer",
        suffixes=("_control", "_challenger"),
        validate="one_to_one",
    ).sort_values("session_date", kind="stable")
    for column in (
        "candidates_control",
        "fills_control",
        "net_return_pct_control",
        "net_pnl_rs_control",
        "candidates_challenger",
        "fills_challenger",
        "net_return_pct_challenger",
        "net_pnl_rs_challenger",
    ):
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0)
    merged["delta_candidates"] = (
        merged["candidates_challenger"] - merged["candidates_control"]
    )
    merged["delta_fills"] = merged["fills_challenger"] - merged["fills_control"]
    merged["delta_net_points"] = (
        merged["net_return_pct_challenger"] - merged["net_return_pct_control"]
    )
    merged["delta_net_pnl_rs"] = (
        merged["net_pnl_rs_challenger"] - merged["net_pnl_rs_control"]
    )
    merged["cumulative_delta_points"] = merged["delta_net_points"].cumsum()
    return merged.to_dict("records")


def leg_rows(
    label: str,
    payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    diagnostics = pd.read_csv(output_path(payload, "diagnostic_breakdowns"))
    selected = diagnostics.loc[
        diagnostics["dimension"].astype(str).isin(["side", "setup_id"])
    ].copy()
    selected.insert(0, "label", label)
    return selected.to_dict("records")


def build_comparison(
    *,
    control_provenance: Path,
    challenger_provenance: Path,
    output_dir: Path,
    require_control_parity: bool,
) -> Path:
    control = validate_provenance(control_provenance)
    challenger = validate_provenance(challenger_provenance)
    assert_comparison_identity(
        control,
        challenger,
        require_control_parity=require_control_parity,
    )
    control_window = dict(control.get("backtest_window", {}))
    challenger_window = dict(challenger.get("backtest_window", {}))
    if control_window != challenger_window:
        raise ValueError("Comparison runs must have the same backtest window")
    control_cache = str(control.get("cache_input_fingerprint", ""))
    challenger_cache = str(challenger.get("cache_input_fingerprint", ""))
    if control_cache != challenger_cache:
        # Stage 0 and Stage 1 intentionally have different cache schema hashes,
        # so source identity is checked separately below.
        control_manifest = load_json(output_path(control, "cache_manifest_archive"))
        challenger_manifest = load_json(
            output_path(challenger, "cache_manifest_archive")
        )
        control_source = dict(control_manifest.get("source_snapshot", {})).get(
            "snapshot_fingerprint"
        )
        challenger_source = dict(
            challenger_manifest.get("source_snapshot", {})
        ).get("snapshot_fingerprint")
        if not control_source or control_source != challenger_source:
            raise ValueError("Comparison runs do not use the same source snapshot")

    challenger_invariants: list[dict[str, Any]] = []
    if (
        not require_control_parity
        and control.get("v10_experiment_run_schema_version")
    ):
        challenger_invariants = challenger_invariant_rows(control, challenger)

    parity = core_audit_parity(control, challenger)
    if require_control_parity and not parity["parity"]:
        raise AssertionError(f"Required V10B control parity failed: {parity}")
    aggregates = [
        aggregate_row("CONTROL", control),
        aggregate_row("CHALLENGER", challenger),
    ]
    control_metrics, challenger_metrics = aggregates
    artifact_parity = artifact_parity_rows(control, challenger)
    if require_control_parity:
        assert_anchor_metrics(control_metrics, label="Stage 0 control")
        assert_anchor_metrics(challenger_metrics, label="Stage 1 V10B")
        mismatched_artifacts = [
            row["artifact"]
            for row in artifact_parity
            if not row["byte_identical"]
        ]
        if mismatched_artifacts:
            raise AssertionError(
                "Required V10B artifact parity failed: "
                f"{mismatched_artifacts}"
            )
    output_dir.mkdir(parents=True, exist_ok=False)
    delta = {
        "label": "DELTA_CHALLENGER_MINUS_CONTROL",
        "variant": challenger_metrics["variant"],
        "from_day": challenger_metrics["from_day"],
        "through_day": challenger_metrics["through_day"],
        "sessions": challenger_metrics["sessions"] - control_metrics["sessions"],
        "candidates": challenger_metrics["candidates"] - control_metrics["candidates"],
        "fills": challenger_metrics["fills"] - control_metrics["fills"],
        "wins": challenger_metrics["wins"] - control_metrics["wins"],
        "losses": challenger_metrics["losses"] - control_metrics["losses"],
        "win_rate_pct": (
            challenger_metrics["win_rate_pct"] - control_metrics["win_rate_pct"]
        ),
        "profit_factor": (
            float(challenger_metrics["profit_factor"])
            - float(control_metrics["profit_factor"])
        ),
        "net_return_points": (
            float(challenger_metrics["net_return_points"])
            - float(control_metrics["net_return_points"])
        ),
        "net_pnl_rs": (
            float(challenger_metrics["net_pnl_rs"])
            - float(control_metrics["net_pnl_rs"])
        ),
        "max_daily_drawdown_points": (
            float(challenger_metrics["max_daily_drawdown_points"])
            - float(control_metrics["max_daily_drawdown_points"])
        ),
        "headline_valid": False,
        "promotion_eligible": False,
        "input_fingerprint": "",
    }
    aggregate_path = output_dir / "aggregate_comparison.csv"
    daywise_path = output_dir / "daywise_comparison.csv"
    legs_path = output_dir / "side_and_leg_comparison.csv"
    artifact_parity_path = output_dir / "artifact_parity.csv"
    invariant_path = output_dir / "challenger_invariant_artifacts.csv"
    decision_path = output_dir / "decision.md"
    atomic_write_csv(aggregate_path, [*aggregates, delta])
    atomic_write_csv(daywise_path, daywise_rows(control, challenger))
    atomic_write_csv(
        legs_path,
        [*leg_rows("CONTROL", control), *leg_rows("CHALLENGER", challenger)],
    )
    atomic_write_csv(artifact_parity_path, artifact_parity)
    if challenger_invariants:
        atomic_write_csv(invariant_path, challenger_invariants)
    decision = f"""# V10 experiment comparison

- Control: `{control_metrics['variant']}`
- Challenger: `{challenger_metrics['variant']}`
- Window: {control_metrics['from_day']} through {control_metrics['through_day']}
- Required parity: {require_control_parity}
- Core audit parity: {parity['parity']}
- Control fills / PF / net: {control_metrics['fills']} / {control_metrics['profit_factor']:.6f} / {control_metrics['net_return_points']:.6f}
- Challenger fills / PF / net: {challenger_metrics['fills']} / {challenger_metrics['profit_factor']:.6f} / {challenger_metrics['net_return_points']:.6f}
- Accuracy delta: {delta['win_rate_pct']:.6f} percentage points
- Net delta: {delta['net_return_points']:.6f} points

Decision: **COMPARISON_ONLY_RESEARCH_NOT_PROMOTION**
"""
    atomic_write_text(decision_path, decision)
    artifact_paths = [
        aggregate_path,
        daywise_path,
        legs_path,
        artifact_parity_path,
        decision_path,
    ]
    if challenger_invariants:
        artifact_paths.append(invariant_path)
    artifacts = {
        path.name: {"sha256": sha256_file(path), "bytes": path.stat().st_size}
        for path in artifact_paths
    }
    manifest = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "control_provenance": str(control_provenance.resolve()),
        "control_provenance_sha256": sha256_file(control_provenance),
        "challenger_provenance": str(challenger_provenance.resolve()),
        "challenger_provenance_sha256": sha256_file(challenger_provenance),
        "required_control_parity": require_control_parity,
        "core_audit_parity": parity,
        "artifact_parity": artifact_parity,
        "challenger_invariant_artifacts": challenger_invariants,
        "execution_contract": execution_contract(control),
        "fixed_economics_contract": fixed_economics_contract(control),
        "research_only": True,
        "promotion_eligible": False,
        "artifacts": artifacts,
    }
    atomic_write_text(
        output_dir / "comparison_manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    return output_dir.resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-provenance", type=Path, required=True)
    parser.add_argument("--challenger-provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--require-control-parity", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = build_comparison(
        control_provenance=args.control_provenance,
        challenger_provenance=args.challenger_provenance,
        output_dir=args.output_dir,
        require_control_parity=args.require_control_parity,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
