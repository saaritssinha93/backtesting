from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v12_backtest as backtest


def test_locked_v12_profile_and_hashes_are_exact() -> None:
    assert backtest.profile_sha256() == backtest.EXPECTED_PROFILE_SHA256
    assert backtest.EXPECTED_PROFILE_SHA256 == (
        "067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe"
    )
    assert backtest.EXPECTED_REGISTRY_SHA256 == (
        "4948ba186095a5baea6b538a64255bc7304e96720ba98da512d6d21490328c35"
    )
    assert backtest.EXPECTED_RESOLVED_CONFIG_SHA256 == (
        "660ab5d2d06290d23e6b39593ddbb5afe03f51e3b6bb714099134eff7481ca4f"
    )
    assert backtest.EXPECTED_INPUT_BINDING_SHA256 == (
        "78c4d7088f7cf500ec8da587a200314c43cf669a56e2df2aca52b74ec025e62c"
    )


def test_profile_is_fixed_isolated_and_research_only() -> None:
    payload = backtest._profile_payload()
    assert payload["profile_id"] == "V12_S06_LATE_SHORT_VOLUME_MIN_150"
    assert payload["selection_origin"] == {
        "isolated_predeclared_variant": True,
        "gate_passing_observed": True,
        "winner_selected_after_v12_research": True,
        "stage12_combination": False,
    }
    assert payload["selection_contract"]["volume_0940_short_min"] == 1.50
    assert payload["selection_contract"]["volume_0945_short_min"] == 1.50
    assert payload["historical_contract"]["selected_candidates"] == 1017
    assert payload["headline_valid"] is False
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False
    assert payload["live_or_paper_authority"] is False


def test_fixed_contract_validates_without_historical_file_load() -> None:
    result = backtest.validate_fixed_contract(require_files=False)
    assert result["validated"] is True
    assert result["profile_sha256"] == backtest.EXPECTED_PROFILE_SHA256
    assert result["resolved_config_sha256"] == (
        backtest.EXPECTED_RESOLVED_CONFIG_SHA256
    )
    assert result["gap_identity_policy"] == "STRONG_REFERENCE_AND_IS_CHECK"
    assert backtest._runtime_spec().is_neutral is True


def test_run_cli_is_fixed_and_has_no_strategy_tuning_flags() -> None:
    parser = backtest._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])
    args = parser.parse_args(["run", "--all-usable-history"])
    assert args.all_usable_history is True
    assert args.reference_only is False
    for tuning_args in (
        ["--volume-0940-short-min", "1.25"],
        ["--volume-0945-short-min", "1.25"],
        ["--gap-bps", "3"],
        ["--variant", "V11_STAGE0_FROZEN_CONTROL"],
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "--all-usable-history", *tuning_args])


def test_runner_is_independent_and_context_order_is_fail_closed() -> None:
    source = Path(backtest.__file__).read_text(encoding="utf-8")
    assert "import fno_v12_staged_backtest" not in source
    parent_marker = "with v11_execution.installed_runtime_hooks("
    v12_marker = "with v12_execution.installed_runtime_hooks(runtime_spec):"
    gap_marker = "with v11_gap.installed_gap_guard(gap):"
    assert parent_marker in source
    assert v12_marker in source
    assert gap_marker in source
    assert source.index(parent_marker) < source.index(v12_marker) < source.index(
        gap_marker
    )


@pytest.mark.parametrize("scenario", [item[0] for item in backtest.EXPECTED_SCENARIOS])
def test_all_golden_scenario_benchmarks_validate(scenario: str) -> None:
    expected = dict(backtest.EXPECTED_FULL_USABLE[scenario])
    result = backtest.validate_full_usable_benchmark(expected, scenario)
    assert result["verified"] is True
    drifted = dict(expected)
    drifted["net_return_points"] = float(drifted["net_return_points"]) + 0.01
    with pytest.raises(AssertionError, match="benchmark drifted"):
        backtest.validate_full_usable_benchmark(drifted, scenario)


def test_closed_trade_fingerprint_is_order_independent() -> None:
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
    rows = []
    for candidate_id in ("2026-08-28|09:40_SHORT|B", "2026-08-28|09:40_SHORT|A"):
        rows.append(
            {
                column: (
                    candidate_id
                    if column == "candidate_id"
                    else 1.0
                    if column in numeric
                    else "TEST"
                )
                for column in backtest._CLOSED_TRADE_FINGERPRINT_COLUMNS
            }
        )
    frame = pd.DataFrame(rows)
    assert backtest._closed_trade_economic_fingerprint(frame) == (
        backtest._closed_trade_economic_fingerprint(frame.iloc[::-1])
    )


def _artifact_record(path: Path, root: Path) -> dict[str, object]:
    return {
        "relative_path": str(path.relative_to(root)).replace("\\", "/"),
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _minimal_complete_run(tmp_path: Path) -> tuple[Path, str]:
    report = tmp_path / "report.md"
    report.write_text("sealed\n", encoding="utf-8")
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
                    _artifact_record(report, tmp_path),
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
                "profile_sha256": backtest.EXPECTED_PROFILE_SHA256,
                "profile": backtest._profile_payload(),
                "registry_sha256": backtest.EXPECTED_REGISTRY_SHA256,
                "resolved_config_sha256": (
                    backtest.EXPECTED_RESOLVED_CONFIG_SHA256
                ),
                "input_binding_sha256": backtest.EXPECTED_INPUT_BINDING_SHA256,
                "selected_candidate_count": backtest.EXPECTED_SELECTED_CANDIDATES,
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
    (tmp_path / "report.md").write_text("tampered content\n", encoding="utf-8")
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
