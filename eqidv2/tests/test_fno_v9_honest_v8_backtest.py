from __future__ import annotations

import ast
import importlib
from dataclasses import asdict
from pathlib import Path

import pytest

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine
import fno_v9_honest_v8_backtest as v9


def test_active_book_is_literal_hash_pinned_v8_combined_book() -> None:
    payload = [asdict(setup) for setup in v9.ACTIVE_SETUPS]
    assert common.canonical_json_sha256(payload) == v9.ACTIVE_SETUP_BOOK_SHA256
    assert v9.ACTIVE_SETUP_BOOK_SHA256 == (
        "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
    )
    assert len(v9.ACTIVE_SETUPS) == 10
    assert {setup.signal_end for setup in v9.ACTIVE_SETUPS} == {
        "09:25",
        "09:30",
        "09:35",
        "09:40",
        "09:45",
    }
    assert len({setup.setup_id for setup in v9.ACTIVE_SETUPS}) == 10


def test_later_legs_are_literal_disabled_train_records_not_setups() -> None:
    expected = {
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    }
    assert set(v9.DISABLED_SHADOW_LEGS) == expected
    assert expected.isdisjoint({setup.setup_id for setup in v9.ACTIVE_SETUPS})
    for leg_id, record in v9.DISABLED_SHADOW_LEGS.items():
        assert record["status"] == "DISABLED_SHADOW"
        assert record["stage"] == "TRAIN"
        assert record["reason"] == "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS"
        assert record["attempted_configurations"] == 48
        assert record["permanently_disabled_for_search_run"] is True
        assert record["validation_outcomes_accessed"] is False
        assert record["test_outcomes_accessed"] is False
        assert record["selection_run_fingerprint"] == (
            v9.OPTIMIZER_SEARCH_RUN_FINGERPRINT
        )
        assert record["selection_artifact_path"] == str(
            v9.OPTIMIZER_SELECTION_ARTIFACT_PATH
        )
        assert record["selection_artifact_fingerprint"] == (
            v9.OPTIMIZER_SELECTION_ARTIFACT_SHA256
        )
        assert record["selection_artifact_sha256"] == (
            v9.OPTIMIZER_SELECTION_ARTIFACT_SHA256
        )
        assert leg_id == f"{record['signal_end']}_{record['side']}"


def test_optimizer_negative_selection_lineage_is_fully_bound() -> None:
    lineage = v9.selection_lineage_payload()
    assert lineage["optimizer_source_sha256"].upper().startswith("BAAC019")
    assert lineage["optimizer_source_sha256"] == (
        "baac01902807ffd6beb465cd890c2a1cae50213719f49421372d56600a5f99e2"
    )
    assert lineage["search_run_fingerprint"] == (
        "335057a1588c66762c1d20fceb8690ae132db7308aa100174e8ad0554f02b0c7"
    )
    assert lineage["selection_artifact_sha256"] == (
        "b67b28ac2eaf223b762dbeaac83b579821169359a22a5850465da383d0c8181b"
    )
    assert lineage["provenance_artifact_sha256"] == (
        "a1f091dd0de6f0f4332209f15be52e187de1958dfd3c6c6ba11998c205eb4ed9"
    )
    assert lineage["search_output_sha256"] == v9.OPTIMIZER_SEARCH_ARTIFACT_SHA256
    assert set(lineage["search_output_sha256"]) == {
        "optimizer_source_archive",
        "report",
        "selection",
        "trials_09:50_LONG",
        "trials_09:50_SHORT",
        "trials_09:55_LONG",
        "trials_09:55_SHORT",
        "v8_source_archive",
    }
    assert lineage["selection_status"] == "NO_QUALIFYING_TRAIN_LEGS"
    assert lineage["outcomes_accessed"] == ["TRAIN"]
    assert lineage["coverage_mode"] == "RECTANGULAR_PANEL"
    assert lineage["diagnostic_only"] is True
    assert lineage["lineage_certified"] is False
    assert lineage["pf_claim_eligible"] is False
    assert lineage["eligible_for_validation"] is False
    assert lineage["promotion_eligible"] is False


def test_launcher_imports_only_common_and_neutral_v8_strategy_modules() -> None:
    source = Path(v9.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert "fno_oi_common" in imports
    assert "fno_v8_windowed_1m_entry_backtest" in imports
    forbidden = {
        "fno_v8_combined_best_per_leg_backtest",
        "fno_v8_strict_v6_logic_backtest",
        "fno_v9_0950_0955_honest_optimize",
        "fno_v9_honest_data_repair",
    }
    assert imports.isdisjoint(forbidden)
    assert not any(name.startswith("fno_oi_ema_confirm") for name in imports)


def test_configured_engine_isolated_and_preserves_policy_cost_defaults() -> None:
    original_builder = engine.provenance.build_run_provenance
    try:
        v9.configure_engine()
        assert engine.ACTIVE_SETUPS == v9.ACTIVE_SETUPS
        assert engine.V8_SETUP_BOOK_SHA256 == v9.ACTIVE_SETUP_BOOK_SHA256
        assert engine.RUN_SCHEMA_VERSION == v9.V9_RUN_SCHEMA_VERSION
        assert set(engine.VARIANT_REGISTRY) == {"VH"}
        assert engine.CACHE_DIR.is_relative_to(v9.ROOT)
        assert engine.RUN_ROOT.is_relative_to(v9.ROOT)
        assert engine.PROVENANCE_ROOT.is_relative_to(v9.ROOT)
        assert "v8_combined_best_per_leg_v1" not in str(engine.CACHE_DIR)
        assert "v8_windowed_strict_v1" not in str(engine.CACHE_DIR)
        assert engine.REPORT_PATH.name == "latest_fno_v9_honest_v8_combined.md"

        args = engine.parse_args(
            v9._inject_v9_variant(
                [
                    "run",
                    "--source-snapshot",
                    "snapshot.json",
                    "--from-day",
                    "2026-05-27",
                    "--through-day",
                    "2026-07-31",
                ]
            )
        )
        assert args.variant == "VH"
        assert args.cost_bps == 15.0
        assert args.slippage_bps == 0.0
        assert args.square_off == "15:30"
        assert args.eod_policy == "EXACT_SQUARE_OFF"
        policy = engine.entry_policy_for_variant(
            "VH",
            cost_bps=args.cost_bps,
            slippage_bps=args.slippage_bps,
            square_off=args.square_off,
            eod_policy=args.eod_policy,
        )
        assert policy.max_confirmation_minute == 1
        assert policy.buffer_bps == 0.0
        assert policy.midpoint_invalidation is False
    finally:
        engine.provenance.build_run_provenance = original_builder
        importlib.reload(engine)


def test_cli_injects_v9_variant_only_for_replay_commands() -> None:
    assert v9._inject_v9_variant(["run", "--from-day", "2026-05-27"]) == [
        "run",
        "--from-day",
        "2026-05-27",
        "--variant",
        "VH",
    ]
    assert v9._inject_v9_variant(["smoke", "--variant", "VH"]) == [
        "smoke",
        "--variant",
        "VH",
    ]
    assert v9._inject_v9_variant(["validate", "--provenance", "x.json"]) == [
        "validate",
        "--provenance",
        "x.json",
    ]


def test_provenance_builder_archives_and_hashes_launcher_and_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine_archive = tmp_path / "fno_v8_windowed_1m_entry_backtest.py"
    engine_archive.write_bytes(Path(engine.__file__).read_bytes())

    def fake_builder(**kwargs: object) -> dict[str, object]:
        output_paths = dict(kwargs["output_paths"])  # type: ignore[arg-type]
        return {
            "outputs": {
                name: engine.provenance.artifact_record(path)
                for name, path in output_paths.items()
            },
            "strategy_source_sha256": engine.provenance.sha256_file(engine_archive),
        }

    monkeypatch.setattr(v9, "_ORIGINAL_PROVENANCE_BUILDER", fake_builder)
    payload = v9._build_v9_run_provenance(
        output_paths={"strategy_source_archive": engine_archive}
    )
    outputs = dict(payload["outputs"])
    assert set(outputs) == {
        "strategy_source_archive",
        "launcher_source_archive",
        "neutral_engine_source_archive",
    }
    launcher_record = dict(outputs["launcher_source_archive"])
    neutral_record = dict(outputs["neutral_engine_source_archive"])
    assert Path(str(launcher_record["path"])).name == Path(v9.__file__).name
    assert launcher_record["sha256"] == v9.launcher_sha256()
    assert neutral_record["sha256"] == engine.provenance.sha256_file(engine_archive)
    assert payload["launcher_source_sha256"] == launcher_record["sha256"]
    assert payload["neutral_engine_source_sha256"] == neutral_record["sha256"]
    assert payload["optimizer_selection_lineage"] == v9.selection_lineage_payload()
    assert payload["disabled_shadow_legs"] == v9.disabled_shadow_payload()
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False


def _synthetic_v9_provenance(
    tmp_path: Path,
) -> tuple[dict[str, object], Path, Path]:
    launcher_archive = tmp_path / "fno_v9_honest_v8_backtest.py"
    neutral_archive = tmp_path / "fno_v8_windowed_1m_entry_backtest.py"
    launcher_archive.write_bytes(Path(v9.__file__).read_bytes())
    neutral_archive.write_bytes(Path(engine.__file__).read_bytes())
    launcher_record = engine.provenance.artifact_record(launcher_archive)
    neutral_record = engine.provenance.artifact_record(neutral_archive)
    payload: dict[str, object] = {
        "v9_honest_run_schema_version": v9.V9_RUN_SCHEMA_VERSION,
        "research_only": True,
        "promotion_eligible": False,
        "outputs": {
            "launcher_source_archive": launcher_record,
            "neutral_engine_source_archive": neutral_record,
        },
        "launcher_source_sha256": launcher_record["sha256"],
        "neutral_engine_source_sha256": neutral_record["sha256"],
        "strategy_source_sha256": neutral_record["sha256"],
        "optimizer_selection_lineage": v9.selection_lineage_payload(),
        "disabled_shadow_legs": v9.disabled_shadow_payload(),
        "strategy_payload": {
            "v9_honest_launcher": {
                "selection_lineage": v9.selection_lineage_payload(),
                "launcher_source_sha256": launcher_record["sha256"],
                "neutral_engine_source_sha256": neutral_record["sha256"],
                "active_setup_book_sha256": v9.ACTIVE_SETUP_BOOK_SHA256,
                "active_later_legs": [],
                "disabled_shadow_legs": v9.disabled_shadow_payload(),
            }
        },
    }
    return payload, launcher_archive, neutral_archive


def test_v9_provenance_validator_verifies_both_source_archives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload, _, _ = _synthetic_v9_provenance(tmp_path)
    monkeypatch.setattr(
        v9,
        "_ORIGINAL_ENGINE_PROVENANCE_VALIDATOR",
        lambda _path: dict(payload),
    )
    validated = v9.validate_v9_run_provenance(tmp_path / "provenance.json")
    assert validated["current_launcher_source_matches_archive"] is True
    assert validated["current_neutral_engine_source_matches_archive"] is True


@pytest.mark.parametrize("archive_name", ["launcher", "engine"])
def test_v9_provenance_validator_rejects_either_tampered_source_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    archive_name: str,
) -> None:
    payload, launcher_archive, neutral_archive = _synthetic_v9_provenance(tmp_path)
    monkeypatch.setattr(
        v9,
        "_ORIGINAL_ENGINE_PROVENANCE_VALIDATOR",
        lambda _path: dict(payload),
    )
    target = launcher_archive if archive_name == "launcher" else neutral_archive
    target.write_bytes(target.read_bytes() + b"\n# tampered\n")
    with pytest.raises(AssertionError, match="source artifact changed"):
        v9.validate_v9_run_provenance(tmp_path / "provenance.json")


def test_v9_provenance_validator_rejects_rebound_selection_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload, _, _ = _synthetic_v9_provenance(tmp_path)
    lineage = dict(payload["optimizer_selection_lineage"])  # type: ignore[arg-type]
    lineage["search_run_fingerprint"] = "0" * 64
    payload["optimizer_selection_lineage"] = lineage
    monkeypatch.setattr(
        v9,
        "_ORIGINAL_ENGINE_PROVENANCE_VALIDATOR",
        lambda _path: dict(payload),
    )
    with pytest.raises(AssertionError, match="selection lineage changed"):
        v9.validate_v9_run_provenance(tmp_path / "provenance.json")
