from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v10_backtest as backtest
import fno_v10_backtest_config as config
import fno_v10_experiment_backtest as experiment
import fno_v10_unified_5m_1m_backtest as v10b
import fno_v8_windowed_1m_entry_backtest as engine


def _candidate(
    candidate_id: str,
    *,
    setup_id: str,
    move: float,
    rank: int,
) -> dict[str, object]:
    signal_time = setup_id.split("_", 1)[0]
    side = setup_id.split("_", 1)[1]
    return {
        "candidate_id": candidate_id,
        "session_date": "2026-08-01",
        "signal_time": signal_time,
        "setup_id": setup_id,
        "side": side,
        "symbol": candidate_id,
        "price_change_pct": move,
        "five_min_volume": 10_000.0,
        "picker": "max_liquidity",
        "picker_value": 1_000_000.0 - rank,
        "traded_value": 10_000_000.0 - rank,
        "frozen_rank": rank,
    }


def test_locked_profile_is_exact_and_hash_pinned() -> None:
    config.validate_locked_profile()
    payload = config.locked_profile_payload()
    assert config.profile_sha256() == config.EXPECTED_PROFILE_SHA256
    assert payload["authority"] == "BACKTEST_ONLY"
    assert payload["active_variant"] == "0940_LONG_MOVE_040"
    assert payload["selection_contract"] == {
        "changed_setup_id": "09:40_LONG",
        "price_change_pct_min": 0.40,
        "comparison_base": "V10B",
        "other_selection_and_entry_parameters_changed": False,
    }
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False
    assert payload["live_or_paper_authority"] is False


def test_locked_profile_comparison_accepts_its_json_round_trip() -> None:
    serialized = json.loads(json.dumps(config.locked_profile_payload()))
    assert serialized != config.locked_profile_payload()
    backtest._require_locked_profile_payload(serialized, "round-trip profile")
    serialized["selection_contract"]["price_change_pct_min"] = 0.41
    with pytest.raises(AssertionError, match="round-trip profile"):
        backtest._require_locked_profile_payload(serialized, "round-trip profile")


def test_locked_profile_fails_if_expected_variant_hash_is_tampered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "EXPECTED_VARIANT_CONFIG_SHA256", "0" * 64)
    with pytest.raises(AssertionError, match="variant config changed"):
        config.validate_locked_profile(require_pinned_hash=False)


def test_front_door_injects_only_stage7_and_rejects_other_variants() -> None:
    assert backtest._inject_locked_variant(["run", "--from-day", "2026-05-27"]) == [
        "run",
        "--from-day",
        "2026-05-27",
        "--variant",
        config.ACTIVE_VARIANT,
    ]
    explicit = ["smoke", f"--variant={config.ACTIVE_VARIANT}"]
    assert backtest._inject_locked_variant(explicit) == explicit
    with pytest.raises(ValueError, match="locked to"):
        backtest._inject_locked_variant(["run", "--variant", "V10B"])
    with pytest.raises(ValueError, match="one --variant"):
        backtest._inject_locked_variant(
            [
                "run",
                "--variant",
                config.ACTIVE_VARIANT,
                f"--variant={config.ACTIVE_VARIANT}",
            ]
        )
    assert backtest._inject_locked_variant(["snapshot"]) == ["snapshot"]
    assert backtest._inject_locked_variant(["run", "--help"]) == [
        "run",
        "--help",
    ]


def test_front_door_rejects_wrong_variant_before_delegation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delegated = False

    def fake_main(_args: object) -> int:
        nonlocal delegated
        delegated = True
        return 0

    monkeypatch.setattr(experiment, "main", fake_main)
    with pytest.raises(ValueError, match="locked to"):
        backtest.main(["run", "--variant", "0940_LONG_MOVE_030"])
    assert delegated is False


def test_full_history_contract_is_injected_and_conflicts_fail_closed() -> None:
    args = backtest._inject_locked_run_contract(["run"])
    values = {
        option: backtest._option_values(args, option)
        for option in (
            "--variant",
            "--source-snapshot",
            "--from-day",
            "--through-day",
            "--split-day",
            "--cost-bps",
            "--slippage-bps",
            "--square-off",
            "--eod-policy",
        )
    }
    contract = config.locked_profile_payload()["extended_stored_history_replay"]
    assert values["--variant"] == [config.ACTIVE_VARIANT]
    assert Path(values["--source-snapshot"][0]).resolve() == Path(
        contract["source_snapshot_manifest"]
    ).resolve()
    assert values["--from-day"] == [contract["from_day"]]
    assert values["--through-day"] == [contract["through_day"]]
    assert values["--split-day"] == [contract["split_day"]]
    assert values["--cost-bps"] == [str(contract["cost_bps"])]
    assert values["--slippage-bps"] == [str(contract["slippage_bps"])]
    assert values["--square-off"] == [contract["square_off"]]
    assert values["--eod-policy"] == [contract["eod_policy"]]

    with pytest.raises(ValueError, match="requires --from-day"):
        backtest._inject_locked_run_contract(
            ["run", "--from-day", "2026-06-24"]
        )
    with pytest.raises(ValueError, match="requires --cost-bps"):
        backtest._inject_locked_run_contract(["run", "--cost-bps", "99"])
    with pytest.raises(ValueError, match="full-universe"):
        backtest._inject_locked_run_contract(["run", "--symbols", "RELIANCE"])
    with pytest.raises(ValueError, match="full-universe"):
        backtest._inject_locked_run_contract(["run", "--no-write"])


def test_main_delegates_the_complete_locked_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def fake_main(args: object) -> int:
        observed.extend(list(args))
        return 0

    monkeypatch.setattr(experiment, "main", fake_main)
    monkeypatch.setattr(backtest, "_install_locked_provenance_adapters", lambda: None)
    assert backtest.main(["run"]) == 0
    assert backtest._option_values(observed, "--variant") == [
        config.ACTIVE_VARIANT
    ]
    assert backtest._option_values(observed, "--from-day") == ["2026-05-27"]
    assert backtest._option_values(observed, "--through-day") == ["2026-08-19"]
    assert backtest._option_values(observed, "--split-day") == ["2026-08-06"]


def test_stage7_boundary_and_scope_are_exact() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("BELOW", setup_id="09:40_LONG", move=0.399999, rank=1),
            _candidate("BOUNDARY", setup_id="09:40_LONG", move=0.40, rank=2),
            _candidate("ABOVE", setup_id="09:40_LONG", move=0.41, rank=3),
            _candidate("OTHER", setup_id="09:35_LONG", move=0.01, rank=1),
        ]
    )
    selected, decisions = experiment.apply_selection_overlay(
        candidates,
        experiment.experiment_config.get_spec(config.ACTIVE_VARIANT),
    )
    assert set(selected["candidate_id"]) == {"BOUNDARY", "ABOVE", "OTHER"}
    rejected = decisions.loc[~decisions["selection_passed"]]
    assert rejected[["candidate_id", "selection_reason"]].to_dict("records") == [
        {
            "candidate_id": "BELOW",
            "selection_reason": "PRICE_CHANGE_BELOW_VARIANT_MINIMUM",
        }
    ]
    stage7_ranks = selected.loc[
        selected["setup_id"].eq("09:40_LONG"), "frozen_rank"
    ].tolist()
    assert stage7_ranks == [1, 2]


def test_locked_stage7_preserves_v10b_engine_and_entry_policy() -> None:
    original_builder = engine.provenance.build_run_provenance
    try:
        experiment.configure_engine(config.ACTIVE_VARIANT)
        assert engine.ACTIVE_SETUPS == v10b.ACTIVE_SETUPS
        assert engine.V8_SETUP_BOOK_SHA256 == v10b.ACTIVE_SETUP_BOOK_SHA256
        assert engine.CACHE_DIR.is_relative_to(experiment.ROOT)
        policy = engine.entry_policy_for_variant(
            config.ACTIVE_VARIANT,
            cost_bps=15.0,
            slippage_bps=0.0,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        assert policy.confirmation_volume_ratio_min is None
        assert policy.entry_expiry_minute == 5
    finally:
        engine.provenance.build_run_provenance = original_builder
        importlib.reload(engine)


def test_locked_validator_rejects_non_stage7_provenance(tmp_path: Path) -> None:
    path = tmp_path / "provenance.json"
    path.write_text(
        json.dumps(
            {
                "v10_experiment_variant": "V10B",
                "v10_experiment_variant_config_sha256": "not-stage7",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="locked Stage 7"):
        backtest._validate_locked_provenance_target(["--provenance", str(path)])


def test_locked_validator_rejects_unbound_direct_stage7_provenance(
    tmp_path: Path,
) -> None:
    path = tmp_path / "provenance.json"
    path.write_text(
        json.dumps(
            {
                "v10_experiment_variant": config.ACTIVE_VARIANT,
                "v10_experiment_variant_config_sha256": (
                    config.EXPECTED_VARIANT_CONFIG_SHA256
                ),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="not bound"):
        backtest._validate_locked_provenance_target(["--provenance", str(path)])


def test_profile_command_is_machine_readable(capsys: pytest.CaptureFixture[str]) -> None:
    assert backtest.main(["profile"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile_sha256"] == config.EXPECTED_PROFILE_SHA256
    assert payload["profile"]["active_variant"] == config.ACTIVE_VARIANT


def test_max050_gap2_profile_is_exact() -> None:
    backtest.validate_max050_gap2_contract(require_files=False)
    payload = backtest.max050_gap2_profile_payload()
    assert payload["profile_id"] == "V10_STAGE7_0935_LONG_MAX_050_GAP2"
    assert payload["five_minute_selection"]["stage7_0940_long_move_min_pct"] == 0.40
    assert payload["five_minute_selection"]["selection"]["move_0935_long_max"] == 0.50
    assert payload["one_minute_entry"]["max_adverse_gap_bps"] == 2.0
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False
    assert payload["live_or_paper_authority"] is False


def test_max050_gap2_usable_segments_are_nonoverlapping_and_report_aug26_gap() -> None:
    sessions: set[object] = set()
    for segment in backtest.MAX050_GAP2_USABLE_SEGMENTS:
        selected = set(
            engine.expected_regular_session_dates(segment.from_day, segment.through_day)
        )
        assert not sessions.intersection(selected)
        sessions.update(selected)
    assert len(sessions) == 65
    expected = set(
        engine.expected_regular_session_dates(min(sessions), max(sessions))
    )
    assert expected - sessions == {backtest.date(2026, 8, 26)}


def test_max050_gap2_selection_boundaries_are_applied_together() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("S7_BELOW", setup_id="09:40_LONG", move=0.399999, rank=1),
            _candidate("S7_PASS", setup_id="09:40_LONG", move=0.40, rank=2),
            _candidate("M50_PASS", setup_id="09:35_LONG", move=0.50, rank=1),
            _candidate("M50_ABOVE", setup_id="09:35_LONG", move=0.500001, rank=2),
        ]
    )
    selected, decisions = backtest.filters.selection_overlay(
        candidates,
        backtest.filters.SPEC_BY_NAME[backtest.MAX050_GAP2_SELECTION_VARIANT],
    )
    assert set(selected["candidate_id"]) == {"S7_PASS", "M50_PASS"}
    rejected = decisions.loc[~decisions["selection_passed"]]
    assert set(rejected["selection_reason"]) == {
        "STAGE7_0940_LONG_MOVE_BELOW_040",
        "0935_LONG_MOVE_ABOVE_CHALLENGER_MAX",
    }


def test_max050_gap2_command_dispatches_without_stage7_delegation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def fake_run(args: object) -> Path:
        observed.extend(list(args))
        return Path("unused")

    monkeypatch.setattr(backtest, "run_max050_gap2", fake_run)
    assert backtest.main(["max050-gap2", "--all-usable-history"]) == 0
    assert observed == ["--all-usable-history"]


def test_max050_gap2_pinned_manifests_use_expected_setup_book() -> None:
    for segment in backtest.MAX050_GAP2_USABLE_SEGMENTS:
        payload = json.loads(segment.cache_manifest.read_text(encoding="utf-8"))
        contract = dict(payload.get("input_contract", {}))
        observed = payload.get("setup_book_sha256") or contract.get(
            "setup_book_sha256"
        )
        assert observed == backtest.EXPECTED_V10_SETUP_BOOK_SHA256


def test_uniform_max_entry_profiles_change_only_the_cap() -> None:
    original = tuple(v10b.ACTIVE_SETUPS)
    for value in backtest.MAX050_GAP2_CAP_SWEEP_VALUES:
        changed = backtest._uniform_max_entry_setups(original, value)
        assert all(setup.max_entries == value for setup in changed)
        for before, after in zip(original, changed, strict=True):
            before_payload = before.__dict__.copy()
            after_payload = after.__dict__.copy()
            before_payload.pop("max_entries")
            after_payload.pop("max_entries")
            assert after_payload == before_payload
    with pytest.raises(ValueError, match="max_entries"):
        backtest._uniform_max_entry_setups(original, 6)


def test_current_mixed_benchmark_is_exact_and_fails_closed_on_drift() -> None:
    expected = dict(backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK)
    verification = backtest.validate_current_mixed_benchmark(expected)
    assert verification["verified"] is True
    assert verification["expected"] == verification["observed"]
    assert "232 fills" in verification["display"]
    assert "PF 1.8327" in verification["display"]

    changed = dict(expected)
    changed["fills"] = 231
    with pytest.raises(AssertionError, match="current-mixed benchmark changed"):
        backtest.validate_current_mixed_benchmark(changed)

    missing = dict(expected)
    missing.pop("profit_factor")
    with pytest.raises(AssertionError, match="profit_factor=MISSING"):
        backtest.validate_current_mixed_benchmark(missing)


def test_max050_gap2_cap_sweep_command_dispatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def fake_run(args: object) -> Path:
        observed.extend(list(args))
        return Path("unused")

    monkeypatch.setattr(backtest, "run_max050_gap2_cap_sweep", fake_run)
    assert backtest.main(
        ["max050-gap2-cap-sweep", "--all-usable-history"]
    ) == 0
    assert observed == ["--all-usable-history"]
