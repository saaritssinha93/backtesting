"""Create an immutable-style Stage 0 V10B parity result package.

This is a report-only utility.  It never changes strategy, cache, run, or
provenance artifacts; it compares an existing V8 control with a completed
V10B run and writes a small, separately versioned comparison package.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


EXACT_ARTIFACTS = (
    "daily.csv",
    "diagnostic_breakdowns.csv",
    "coverage.csv",
    "setups.csv",
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
        raise ValueError(f"Refusing to write an empty CSV: {path}")
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compare_audits(v8_path: Path, v10_path: Path) -> dict[str, Any]:
    v8 = pd.read_csv(v8_path, keep_default_na=False, dtype=str)
    v10 = pd.read_csv(v10_path, keep_default_na=False, dtype=str)
    same_columns = list(v8.columns) == list(v10.columns)
    if not same_columns or "variant" not in v8.columns:
        raise AssertionError("Candidate audit schemas do not match as expected")
    economic_columns = [column for column in v8.columns if column != "variant"]
    economic_match = v8[economic_columns].equals(v10[economic_columns])
    variant_v8 = sorted(v8["variant"].unique().tolist())
    variant_v10 = sorted(v10["variant"].unique().tolist())
    expected_label_difference = variant_v8 == ["VC"] and variant_v10 == ["V10B"]
    if not economic_match or not expected_label_difference:
        raise AssertionError("V10B is not trade-for-trade equal to the V8 control")
    return {
        "v8_rows": int(len(v8)),
        "v10_rows": int(len(v10)),
        "columns": int(len(v8.columns)),
        "economic_and_state_columns_compared": int(len(economic_columns)),
        "differing_cells_excluding_variant": 0,
        "variant_v8": variant_v8,
        "variant_v10": variant_v10,
        "parity": True,
    }


def trade_counts(audit_path: Path) -> dict[str, Any]:
    audit = pd.read_csv(audit_path)
    filled = audit.loc[audit["filled"].eq(True)].copy()  # noqa: E712
    returns = pd.to_numeric(filled["net_return_pct"], errors="coerce").dropna()
    wins = int(returns.gt(0).sum())
    losses = int(returns.lt(0).sum())
    flats = int(returns.eq(0).sum())
    return {
        "wins": wins,
        "losses": losses,
        "flat_trades": flats,
        "win_rate_pct": float(100.0 * wins / len(returns)) if len(returns) else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v8-control-run", type=Path, required=True)
    parser.add_argument("--v10-run", type=Path, required=True)
    parser.add_argument("--snapshot-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--contract-tests-passed", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    required = (
        "candidate_order_audit.csv",
        "daily.csv",
        "diagnostic_breakdowns.csv",
        "coverage.csv",
        "setups.csv",
        "report.md",
        "provenance.json",
    )
    for run in (args.v8_control_run, args.v10_run):
        missing = [name for name in required if not (run / name).is_file()]
        if missing:
            raise FileNotFoundError(f"Missing artifacts in {run}: {missing}")
    if not args.snapshot_manifest.is_file():
        raise FileNotFoundError(args.snapshot_manifest)
    args.output_dir.mkdir(parents=True, exist_ok=False)

    v8_provenance = load_json(args.v8_control_run / "provenance.json")
    v10_provenance = load_json(args.v10_run / "provenance.json")
    snapshot = load_json(args.snapshot_manifest)
    audit_comparison = compare_audits(
        args.v8_control_run / "candidate_order_audit.csv",
        args.v10_run / "candidate_order_audit.csv",
    )

    comparison_rows: list[dict[str, Any]] = []
    audit_v8_hash = sha256_file(args.v8_control_run / "candidate_order_audit.csv")
    audit_v10_hash = sha256_file(args.v10_run / "candidate_order_audit.csv")
    comparison_rows.append(
        {
            "artifact": "candidate_order_audit.csv",
            "comparison": "EXACT_EXCEPT_INTENTIONAL_VARIANT_LABEL",
            "match": True,
            "v8_sha256": audit_v8_hash,
            "v10_sha256": audit_v10_hash,
        }
    )
    for name in EXACT_ARTIFACTS:
        v8_hash = sha256_file(args.v8_control_run / name)
        v10_hash = sha256_file(args.v10_run / name)
        if v8_hash != v10_hash:
            raise AssertionError(f"Expected byte-identical artifact differs: {name}")
        comparison_rows.append(
            {
                "artifact": name,
                "comparison": "BYTE_IDENTICAL_SHA256",
                "match": True,
                "v8_sha256": v8_hash,
                "v10_sha256": v10_hash,
            }
        )

    results = dict(v10_provenance["results"])
    diagnostics = dict(results["diagnostic_closed_trade_metrics"])
    counts = trade_counts(args.v10_run / "candidate_order_audit.csv")
    aggregate = {
        "stage": "STAGE_00",
        "variant": "V10B",
        "from_day": v10_provenance["backtest_window"]["from_day"],
        "through_day": v10_provenance["backtest_window"]["through_day"],
        "sessions": results["sessions"],
        "candidates": results["candidates"],
        "fills": results["fills"],
        **counts,
        "profit_factor": diagnostics["profit_factor"],
        "net_return_points": diagnostics["net_return_percentage_points"],
        "net_pnl_rs": diagnostics["net_pnl_rs"],
        "max_daily_drawdown_points": diagnostics[
            "max_daily_drawdown_percentage_points"
        ],
        "headline_valid": results["headline_valid"],
        "promotion_eligible": results["promotion_eligible"],
        "parity_with_v8": True,
        "decision": "PASS_PARITY_RESEARCH_ONLY",
    }

    aggregate_path = args.output_dir / "aggregate.csv"
    comparison_path = args.output_dir / "artifact_comparison.csv"
    daywise_path = args.output_dir / "daywise_results.csv"
    diagnostics_path = args.output_dir / "diagnostic_breakdowns.csv"
    decision_path = args.output_dir / "decision.md"
    atomic_write_csv(aggregate_path, [aggregate])
    atomic_write_csv(comparison_path, comparison_rows)
    shutil.copy2(args.v10_run / "daily.csv", daywise_path)
    shutil.copy2(args.v10_run / "diagnostic_breakdowns.csv", diagnostics_path)

    blockers = [str(item) for item in results.get("promotion_blockers", [])]
    blocker_lines = "\n".join(f"- `{item}`" for item in blockers)
    decision = f"""# FNO V10 Stage 0 — Frozen V10B control

- Decision: **PASS_PARITY_RESEARCH_ONLY**
- Window: {aggregate['from_day']} through {aggregate['through_day']}
- Sessions: {aggregate['sessions']}
- Candidates / fills: {aggregate['candidates']} / {aggregate['fills']}
- Wins / losses: {aggregate['wins']} / {aggregate['losses']}
- Win rate: {aggregate['win_rate_pct']:.6f}%
- Profit factor: {aggregate['profit_factor']:.12f}
- Net return: {aggregate['net_return_points']:.12f} points
- Net P&L proxy: Rs {aggregate['net_pnl_rs']:.2f}
- Maximum daily drawdown: {aggregate['max_daily_drawdown_points']:.12f} points
- V8 parity: exact across all economic/state cells; only `VC` versus `V10B` differs
- Headline valid: {aggregate['headline_valid']}
- Promotion eligible: {aggregate['promotion_eligible']}

## Control locations

- V8 control: `{args.v8_control_run}`
- V10B run: `{args.v10_run}`
- Source snapshot: `{args.snapshot_manifest}`
- V10 input fingerprint: `{v10_provenance['backtest_input_fingerprint']}`

## Promotion blockers retained from provenance

{blocker_lines}

This package is the immutable comparison control for subsequent V10 stages.
It does not authorize live or paper promotion.
"""
    atomic_write_text(decision_path, decision)

    package_artifacts = {
        path.name: {"sha256": sha256_file(path), "bytes": path.stat().st_size}
        for path in (
            aggregate_path,
            comparison_path,
            daywise_path,
            diagnostics_path,
            decision_path,
        )
    }
    manifest = {
        "schema_version": "fno_v10_stage_result_package_v1",
        "stage": "STAGE_00",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "PASS_PARITY_RESEARCH_ONLY",
        "v8_control_run": str(args.v8_control_run.resolve()),
        "v10_run": str(args.v10_run.resolve()),
        "snapshot_manifest": str(args.snapshot_manifest.resolve()),
        "snapshot_manifest_sha256": sha256_file(args.snapshot_manifest),
        "snapshot_fingerprint": snapshot.get("snapshot_fingerprint"),
        "v10_provenance_sha256": sha256_file(args.v10_run / "provenance.json"),
        "v10_input_fingerprint": v10_provenance["backtest_input_fingerprint"],
        "contract_tests_passed": args.contract_tests_passed,
        "audit_comparison": audit_comparison,
        "aggregate": aggregate,
        "promotion_blockers": blockers,
        "package_artifacts": package_artifacts,
    }
    atomic_write_text(
        args.output_dir / "stage_manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    print(args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
