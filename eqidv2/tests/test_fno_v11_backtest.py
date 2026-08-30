from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v11_backtest as backtest


def test_locked_stage10_profile_is_exact_and_research_only() -> None:
    assert backtest.profile_sha256() == backtest.LOCKED_PROFILE_SHA256
    assert backtest.LOCKED_PROFILE_SHA256 == (
        "8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1"
    )
    payload = backtest._profile_payload()
    assert payload["component_variant_ids"] == [
        "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
        "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
    ]
    assert payload["runtime_spec"] == {
        "entry_setup_id": "09:30_SHORT",
        "entry_not_before_minute": 3,
        "exit_rule": None,
        "exit_activation_r": None,
        "same_side_symbol_limit": 2,
    }
    assert payload["gap_guard"]["variant"] == "MAX_2_BPS"
    assert payload["gap_guard"]["identity_policy"] == (
        "STRONG_REFERENCE_AND_IS_CHECK"
    )
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False
    assert payload["live_or_paper_authority"] is False


def test_fixed_contract_validates_without_touching_historical_files() -> None:
    result = backtest.validate_fixed_contract(require_files=False)
    assert result["validated"] is True
    assert result["profile_sha256"] == backtest.LOCKED_PROFILE_SHA256
    assert result["gap_identity_policy"] == "STRONG_REFERENCE_AND_IS_CHECK"


def test_composite_requires_explicit_composite_authority() -> None:
    with pytest.raises(ValueError, match="only permitted post-hoc composite"):
        backtest.FIXED_RUNTIME_SPEC.validate()
    backtest.FIXED_RUNTIME_SPEC.validate(allow_composite=True)


def test_run_cli_has_no_strategy_tuning_flags() -> None:
    parser = backtest._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])
    args = parser.parse_args(["run", "--all-usable-history"])
    assert args.all_usable_history is True
    assert args.reference_only is False
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["run", "--all-usable-history", "--same-side-symbol-limit", "1"]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["run", "--all-usable-history", "--entry-not-before-minute", "2"]
        )


def test_runner_does_not_import_staged_discovery_or_use_legacy_gap_context() -> None:
    source = Path(backtest.__file__).read_text(encoding="utf-8")
    assert "import fno_v11_staged_backtest" not in source
    assert "with gaps.installed_gap_guard" not in source
    runtime_marker = "with execution_runtime.installed_runtime_hooks("
    strong_gap_marker = "with gap_runtime.installed_gap_guard(gap):"
    assert runtime_marker in source
    assert strong_gap_marker in source
    assert source.index(runtime_marker) < source.index(strong_gap_marker)


@pytest.mark.parametrize("scenario", [item[0] for item in backtest.EXPECTED_SCENARIOS])
def test_all_golden_scenario_benchmarks_validate(scenario: str) -> None:
    expected = dict(backtest.EXPECTED_FULL_USABLE[scenario])
    result = backtest.validate_full_usable_benchmark(expected, scenario)
    assert result["verified"] is True
    drifted = dict(expected)
    drifted["net_return_points"] = float(drifted["net_return_points"]) + 0.01
    with pytest.raises(AssertionError, match="benchmark drifted"):
        backtest.validate_full_usable_benchmark(drifted, scenario)


def _artifact_record(path: Path, root: Path) -> dict[str, object]:
    return {
        "relative_path": str(path.relative_to(root)).replace("\\", "/"),
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _minimal_complete_run(tmp_path: Path) -> tuple[Path, str]:
    artifact = tmp_path / "report.md"
    artifact.write_text("sealed\n", encoding="utf-8")
    scenario_dir = tmp_path / "scenarios" / "reference_15_0"
    scenario_dir.mkdir(parents=True)
    closed = scenario_dir / "closed_trades.csv"
    numeric = {
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
    }
    trade = {
        column: (1.0 if column in numeric else "TEST")
        for column in backtest._CLOSED_TRADE_FINGERPRINT_COLUMNS
    }
    pd.DataFrame([trade]).to_csv(closed, index=False)
    fingerprint = backtest._closed_trade_economic_fingerprint(pd.read_csv(closed))
    summary = scenario_dir / "summary.json"
    summary.write_text(
        json.dumps(backtest.EXPECTED_FULL_USABLE["REFERENCE_15_0"]),
        encoding="utf-8",
    )
    inventory = tmp_path / "artifact_inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "schema_version": backtest.SCHEMA_VERSION,
                "artifacts": [
                    _artifact_record(artifact, tmp_path),
                    _artifact_record(closed, tmp_path),
                    _artifact_record(summary, tmp_path),
                ],
            }
        ),
        encoding="utf-8",
    )
    provenance = tmp_path / "provenance.json"
    provenance.write_text(
        json.dumps(
            {
                "schema_version": backtest.SCHEMA_VERSION,
                "complete": True,
                "run_dir": str(tmp_path.resolve()),
                "profile_id": backtest.PROFILE_ID,
                "profile_sha256": backtest.LOCKED_PROFILE_SHA256,
                "profile": backtest._profile_payload(),
                "input_binding_sha256": backtest.EXPECTED_INPUT_BINDING_SHA256,
                "research_only": True,
                "promotion_eligible": False,
                "live_or_paper_authority": False,
                "artifact_inventory": {
                    "path": str(inventory.resolve()),
                    "sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
                },
                "executed_scenarios": ["REFERENCE_15_0"],
                "benchmark_verification": {"REFERENCE_15_0": {"verified": True}},
                "closed_trade_economic_fingerprints": {
                    "REFERENCE_15_0": fingerprint
                },
                "scenario_artifacts": {
                    "REFERENCE_15_0": {
                        "closed_trades": str(closed.resolve()),
                        "summary": str(summary.resolve()),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return provenance, fingerprint


def test_completed_run_validator_detects_artifact_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provenance, fingerprint = _minimal_complete_run(tmp_path)
    monkeypatch.setitem(
        backtest.EXPECTED_CLOSED_TRADE_FINGERPRINTS,
        "REFERENCE_15_0",
        fingerprint,
    )
    result = backtest.validate_run_provenance(provenance)
    assert result["validated"] is True
    (tmp_path / "report.md").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="size drifted|hash drifted"):
        backtest.validate_run_provenance(provenance)


def test_completed_run_validator_rejects_incomplete_provenance(
    tmp_path: Path,
) -> None:
    provenance, _ = _minimal_complete_run(tmp_path)
    payload = json.loads(provenance.read_text(encoding="utf-8"))
    payload["complete"] = False
    provenance.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="incomplete"):
        backtest.validate_run_provenance(provenance)
