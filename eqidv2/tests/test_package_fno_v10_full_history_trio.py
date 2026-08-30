from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from tools import package_fno_v10_full_history_trio as package


def test_frozen_59_session_contract_and_locked_sources() -> None:
    assert package.FROM_DAY == "2026-05-27"
    assert package.THROUGH_DAY == "2026-08-19"
    assert package.SPLIT_DAY == "2026-08-06"
    assert package.EXPECTED_SESSIONS == 59
    assert package.EXPECTED_TRAIN_SESSIONS == 49
    assert package.EXPECTED_TEST_SESSIONS == 10
    assert package.LOCKED_PROFILE_SHA256 == (
        "f2b3291903dfb1f2c95f1d24b63285d527dc7a9a6aa3d6334caed03d0834e59c"
    )
    assert package.LOCKED_V10_LAUNCHER_SHA256 == (
        "7b5e5fc6c606039b8c2e91648dbffa8f245844f0a557acedcc382d4201bac0b5"
    )
    json_roundtrip_profile = json.loads(
        json.dumps(package.locked_config.locked_profile_payload())
    )
    assert package.common.canonical_json_sha256(json_roundtrip_profile) == (
        package.LOCKED_PROFILE_SHA256
    )

    bindings = package._source_bindings()
    assert bindings["v10_locked_launcher"]["sha256"] == (
        package.LOCKED_V10_LAUNCHER_SHA256
    )
    assert bindings["v10_locked_profile_config"]["sha256"] == (
        package.LOCKED_V10_CONFIG_FILE_SHA256
    )


def test_frozen_snapshot_binding_is_still_valid() -> None:
    snapshot = package._validate_snapshot_manifest()
    assert snapshot["snapshot_fingerprint"] == package.SNAPSHOT_FINGERPRINT
    assert len(snapshot["captures"]) == 416


def test_test_evidence_fails_closed() -> None:
    evidence = package._test_results_payload(
        tests_passed=101,
        tests_failed=0,
        test_commands=["python -m pytest -q tests/a.py"],
    )
    assert evidence["status"] == "PASS"
    assert evidence["tests_total"] == 101

    with pytest.raises(AssertionError, match="failing tests"):
        package._test_results_payload(
            tests_passed=100,
            tests_failed=1,
            test_commands=["python -m pytest -q"],
        )
    with pytest.raises(ValueError, match="supplied together"):
        package._test_results_payload(
            tests_passed=101,
            tests_failed=None,
            test_commands=[],
        )
    with pytest.raises(ValueError, match="positive"):
        package._test_results_payload(
            tests_passed=0,
            tests_failed=0,
            test_commands=["python -m pytest -q"],
        )
    with pytest.raises(ValueError, match="test command"):
        package._test_results_payload(
            tests_passed=101,
            tests_failed=0,
            test_commands=[],
        )


def test_existing_output_directory_is_rejected_before_reading_provenances(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileExistsError, match="already exists"):
        package.build_package(
            v6_provenance=tmp_path / "missing-v6.json",
            v8_provenance=tmp_path / "missing-v8.json",
            stage7_provenance=tmp_path / "missing-stage7.json",
            output_dir=tmp_path,
        )


def test_stage7_filter_and_pairing_are_candidate_id_based(tmp_path: Path) -> None:
    parent = pd.DataFrame(
        [
            {
                "candidate_id": "keep",
                "session_date": "2026-08-06",
                "signal_time": "2026-08-06 09:40:00+05:30",
                "setup_id": "09:40_LONG",
                "side": "LONG",
                "symbol": "AAA",
                "futures_symbol": "AAA26AUGFUT",
                "frozen_rank": 1,
                "picker": "max_move",
                "picker_value": 0.40,
                "price_change_pct": 0.40,
                "traded_value": 10_000_000.0,
                "status": "TARGETED",
                "filled_bool": True,
                "entry_time": "2026-08-06 09:42:00+05:30",
                "exit_time": "2026-08-06 10:00:00+05:30",
                "exit_reason": "TARGETED",
                "net_return_pct": 1.0,
                "net_pnl_rs": 500.0,
            },
            {
                "candidate_id": "drop",
                "session_date": "2026-08-06",
                "signal_time": "2026-08-06 09:40:00+05:30",
                "setup_id": "09:40_LONG",
                "side": "LONG",
                "symbol": "BBB",
                "futures_symbol": "BBB26AUGFUT",
                "frozen_rank": 2,
                "picker": "max_move",
                "picker_value": 0.39,
                "price_change_pct": 0.39,
                "traded_value": 9_000_000.0,
                "status": "NO_CONFIRMATION",
                "filled_bool": False,
                "entry_time": None,
                "exit_time": None,
                "exit_reason": None,
                "net_return_pct": 0.0,
                "net_pnl_rs": 0.0,
            },
        ]
    )
    child = parent.loc[parent["candidate_id"].eq("keep")].copy()
    decisions_path = tmp_path / "selection_decisions.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "keep",
                "session_date": "2026-08-06",
                "signal_time": "2026-08-06 09:40:00+05:30",
                "setup_id": "09:40_LONG",
                "side": "LONG",
                "symbol": "AAA",
                "price_change_pct": 0.40,
                "picker": "max_move",
                "picker_value": 0.40,
                "traded_value": 10_000_000.0,
                "original_frozen_rank": 1,
                "recalculated_frozen_rank": 1,
                "selection_passed": True,
                "selection_reason": "PASSED",
                "experiment_variant": "0940_LONG_MOVE_040",
                "selection_overlay_id": "0940_LONG_MOVE_040",
                "entry_expiry_minute": 5,
                "variant_config_sha256": package.STAGE7_VARIANT_CONFIG_SHA256,
                "schema_version": "fno_v10_selection_decision_v1",
            },
            {
                "candidate_id": "drop",
                "session_date": "2026-08-06",
                "signal_time": "2026-08-06 09:40:00+05:30",
                "setup_id": "09:40_LONG",
                "side": "LONG",
                "symbol": "BBB",
                "price_change_pct": 0.39,
                "picker": "max_move",
                "picker_value": 0.39,
                "traded_value": 9_000_000.0,
                "original_frozen_rank": 2,
                "recalculated_frozen_rank": None,
                "selection_passed": False,
                "selection_reason": "PRICE_CHANGE_BELOW_VARIANT_MINIMUM",
                "experiment_variant": "0940_LONG_MOVE_040",
                "selection_overlay_id": "0940_LONG_MOVE_040",
                "entry_expiry_minute": 5,
                "variant_config_sha256": package.STAGE7_VARIANT_CONFIG_SHA256,
                "schema_version": "fno_v10_selection_decision_v1",
            },
        ]
    ).to_csv(decisions_path, index=False)

    decisions = package._validate_selection_decisions(
        decisions_path,
        parent,
        child,
    )
    paired = package._paired_rows(parent, child, decisions)
    assert [row["candidate_id"] for row in paired] == ["keep", "drop"]
    assert paired[0]["pair_class"] == "COMMON_CANDIDATE"
    assert paired[1]["pair_class"] == "DROPPED_BY_STAGE7_FILTER"

    wrong_rank = child.copy()
    wrong_rank["frozen_rank"] = 2
    with pytest.raises(AssertionError, match="recalculated selection ranks"):
        package._validate_selection_decisions(
            decisions_path,
            parent,
            wrong_rank,
        )


def test_audit_rejects_non_official_calendar_date(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "weekend",
                "session_date": "2026-08-08",
                "signal_time": "2026-08-08 09:40:00+05:30",
                "setup_id": "09:40_LONG",
                "side": "LONG",
                "symbol": "AAA",
                "futures_symbol": "AAA26AUGFUT",
                "frozen_rank": 1,
                "picker": "max_move",
                "picker_value": 0.4,
                "price_change_pct": 0.4,
                "traded_value": 1_000_000.0,
                "status": "NO_CONFIRMATION",
                "filled": False,
                "entry_time": None,
                "exit_time": None,
                "exit_reason": None,
                "net_return_pct": 0.0,
                "net_pnl_rs": 0.0,
                "schema_version": "fno_v8_windowed_1m_trade_v3",
            }
        ]
    ).to_csv(path, index=False)

    with pytest.raises(AssertionError, match="non-official session dates"):
        package._load_audit(path, "calendar-test")


def test_recursive_manifest_inventory_excludes_only_manifest(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (tmp_path / "a.txt").write_text("a\n", encoding="utf-8")
    (nested / "b.txt").write_text("b\n", encoding="utf-8")
    (nested / "stage_manifest.json").write_text(
        json.dumps({"self": True}), encoding="utf-8"
    )
    (tmp_path / "stage_manifest.json").write_text(
        json.dumps({"root_self": True}), encoding="utf-8"
    )

    rows = package._artifact_inventory(tmp_path)
    assert [row["path"] for row in rows] == [
        "a.txt",
        "nested/b.txt",
        "nested/stage_manifest.json",
    ]
    assert all(len(row["sha256"]) == 64 for row in rows)


def test_provenance_copy_is_hash_verified(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    destination = tmp_path / "archive.json"
    source.write_text('{"locked":true}\n', encoding="utf-8")
    expected = package.sha256_file(source)

    package.atomic_copy_verified(source, destination, expected)
    assert destination.read_bytes() == source.read_bytes()
    assert package.sha256_file(destination) == expected

    with pytest.raises(AssertionError, match="source copy"):
        package.atomic_copy_verified(source, tmp_path / "bad.json", "0" * 64)


def test_native_output_records_use_size_not_package_bytes(tmp_path: Path) -> None:
    artifact = tmp_path / "audit.csv"
    artifact.write_text("candidate_id\nA\n", encoding="utf-8")
    payload = {
        "outputs": {
            "candidate_order_audit": {
                "path": str(artifact),
                "size": artifact.stat().st_size,
                "sha256": package.sha256_file(artifact),
            }
        }
    }
    observed, record = package._output_record(payload, "candidate_order_audit")
    assert observed == artifact.resolve()
    assert record["size"] == artifact.stat().st_size

    payload["outputs"]["candidate_order_audit"].pop("size")
    payload["outputs"]["candidate_order_audit"]["bytes"] = artifact.stat().st_size
    with pytest.raises(AssertionError, match="size"):
        package._output_record(payload, "candidate_order_audit")
