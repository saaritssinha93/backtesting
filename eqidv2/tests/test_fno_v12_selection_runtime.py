from __future__ import annotations

from dataclasses import asdict

import pandas as pd
import pytest

import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as legacy_filters
import fno_v11_backtest as v11_backtest
import fno_v12_execution_runtime as execution_runtime
import fno_v12_selection_runtime as selection
import fno_v12_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


def _row(
    candidate_id: str,
    setup_id: str,
    *,
    move: float,
    volume: float = 2.0,
    traded_value: float = 100_000_000.0,
    session_date: str = "2026-08-28",
    ema9: float = 99.0,
    ema20: float = 100.0,
    ema50: float = 101.0,
    close: float = 98.0,
) -> dict[str, object]:
    side = setup_id.rsplit("_", 1)[1]
    signed_move = abs(move) if side == "LONG" else -abs(move)
    picker = registry.BASE_SETUP_PICKERS[setup_id]
    picker_value = {
        "max_move": abs(signed_move),
        "max_volume": volume,
        "max_liquidity": traded_value,
        "max_oi": 2.0,
    }[picker]
    return {
        "candidate_id": candidate_id,
        "session_date": session_date,
        "setup_id": setup_id,
        "side": side,
        "symbol": candidate_id,
        "price_change_pct": signed_move,
        "volume_ratio": volume,
        "traded_value": traded_value,
        "oi_change_pct": 2.0,
        "picker": picker,
        "picker_value": picker_value,
        "frozen_rank": 99,
        "ema9": ema9,
        "ema20": ema20,
        "ema50": ema50,
        "five_min_close": close,
    }


def _frame(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_stage0_and_relaxed_overlays_start_from_all_candidates() -> None:
    source = _frame(
        _row("L050", "09:35_LONG", move=0.50),
        _row("L060", "09:35_LONG", move=0.60),
        _row("L075", "09:35_LONG", move=0.75),
        _row("L080", "09:35_LONG", move=0.80),
        _row("F030", "09:40_LONG", move=0.30),
        _row("F040", "09:40_LONG", move=0.40),
        _row("F050", "09:40_LONG", move=0.50),
    )
    pristine = source.copy(deep=True)

    control, _, _ = selection.apply_variant_to_all_candidates(
        source, registry.CONTROL_VARIANT_ID
    )
    assert set(control["candidate_id"]) == {"L050", "F040", "F050"}

    max060, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S03A_0935_LONG_MOVE_MAX_060"
    )
    assert set(max060["candidate_id"]) == {"L050", "L060", "F040", "F050"}
    no_max, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S03A_0935_LONG_MOVE_NO_MAX"
    )
    assert set(no_max["candidate_id"]) == {
        "L050",
        "L060",
        "L075",
        "L080",
        "F040",
        "F050",
    }
    floor030, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S03B_0940_LONG_MOVE_MIN_030"
    )
    assert set(floor030["candidate_id"]) == {"L050", "F030", "F040", "F050"}
    floor050, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S03B_0940_LONG_MOVE_MIN_050"
    )
    assert set(floor050["candidate_id"]) == {"L050", "F050"}
    pd.testing.assert_frame_equal(source, pristine)


def test_stage5_attribution_is_side_specific_and_inclusive() -> None:
    source = _frame(
        _row("LONG_BOUND", "09:25_LONG", move=1.25, volume=3.0),
        _row("LONG_HIGH", "09:25_LONG", move=1.26, volume=3.0),
        _row("SHORT_BOUND", "09:25_SHORT", move=1.25),
        _row("SHORT_HIGH", "09:25_SHORT", move=1.26),
    )
    long_only, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S05_0925_LONG_ONLY_MOVE_MAX_125"
    )
    assert set(long_only["candidate_id"]) == {
        "LONG_BOUND",
        "SHORT_BOUND",
        "SHORT_HIGH",
    }
    short_only, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S05_0925_SHORT_ONLY_MOVE_MAX_125"
    )
    assert set(short_only["candidate_id"]) == {
        "LONG_BOUND",
        "LONG_HIGH",
        "SHORT_BOUND",
    }
    both, decisions, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S05_0925_BOTH_MOVE_MAX_125"
    )
    assert set(both["candidate_id"]) == {"LONG_BOUND", "SHORT_BOUND"}
    rejected = decisions.loc[~decisions["kept"]]
    assert rejected["reason"].str.contains("MOVE_0925_").all()


@pytest.mark.parametrize(
    ("variant", "rejected_id", "retained_id"),
    [
        ("V12_S06_0935_LONG_VOLUME_MIN_125", "L", "S40"),
        ("V12_S06_0940_SHORT_VOLUME_MIN_125", "S40", "S45"),
        ("V12_S06_0945_SHORT_VOLUME_MIN_125", "S45", "S40"),
    ],
)
def test_stage6_single_leg_attribution(
    variant: str, rejected_id: str, retained_id: str
) -> None:
    source = _frame(
        _row("L", "09:35_LONG", move=0.3, volume=1.24),
        _row("S40", "09:40_SHORT", move=0.3, volume=1.24),
        _row("S45", "09:45_SHORT", move=0.3, volume=1.24),
    )
    selected, _, _ = selection.apply_variant_to_all_candidates(source, variant)
    ids = set(selected["candidate_id"])
    assert rejected_id not in ids
    assert retained_id in ids


def test_late_short_common_volume_floor_filters_both_and_keeps_boundary() -> None:
    source = _frame(
        _row("S40_LOW", "09:40_SHORT", move=0.3, volume=1.24),
        _row("S40_BOUND", "09:40_SHORT", move=0.3, volume=1.25),
        _row("S45_LOW", "09:45_SHORT", move=0.3, volume=1.24),
        _row("S45_BOUND", "09:45_SHORT", move=0.3, volume=1.25),
    )
    selected, _, _ = selection.apply_variant_to_all_candidates(
        source, "V12_S06_LATE_SHORT_VOLUME_MIN_125"
    )
    assert set(selected["candidate_id"]) == {"S40_BOUND", "S45_BOUND"}


def _ema_from_prior(prior: float, close: float, span: int) -> float:
    alpha = 2.0 / (span + 1.0)
    return alpha * close + (1.0 - alpha) * prior


def test_ema_recurrence_persistence_emits_lossless_diagnostics() -> None:
    prior = (101.0, 102.0, 104.0)
    pass_close = 95.0
    fail_close = 110.0
    passing_emas = tuple(
        _ema_from_prior(value, pass_close, span)
        for value, span in zip(prior, (9, 20, 50), strict=True)
    )
    failing_emas = tuple(
        _ema_from_prior(value, fail_close, span)
        for value, span in zip(prior, (9, 20, 50), strict=True)
    )
    source = _frame(
        _row(
            "PASS",
            "09:25_SHORT",
            move=0.5,
            ema9=passing_emas[0],
            ema20=passing_emas[1],
            ema50=passing_emas[2],
            close=pass_close,
        ),
        _row(
            "FAIL",
            "09:25_SHORT",
            move=0.5,
            ema9=failing_emas[0],
            ema20=failing_emas[1],
            ema50=failing_emas[2],
            close=fail_close,
        ),
    )
    selected, decisions, metadata = selection.apply_variant_to_all_candidates(
        source, "V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_095"
    )
    assert selected["candidate_id"].tolist() == ["PASS"]
    by_id = decisions.set_index("candidate_id")
    for column, expected in zip(
        ("ema9_prior", "ema20_prior", "ema50_prior"), prior, strict=True
    ):
        assert by_id.at["PASS", column] == pytest.approx(expected)
    assert by_id.at["PASS", "ema_gap_fast_persistence_ratio"] > 0.95
    assert by_id.at["PASS", "ema_gap_slow_persistence_ratio"] > 0.95
    assert "EMA_GAP_0925_SHORT_PERSISTENCE_MIN" in by_id.at[
        "FAIL", "failed_rules"
    ]
    assert metadata.ema_gap_algorithm_sha256 == (
        registry.EMA_GAP_PERSISTENCE_ALGORITHM_SHA256
    )
    assert by_id.at["PASS", "ema_gap_algorithm_sha256"] == (
        registry.EMA_GAP_PERSISTENCE_ALGORITHM_SHA256
    )


def test_equal_rank_picker_scores_and_stable_order_are_explicit() -> None:
    source = _frame(
        _row(
            "2026-08-28|09:40_SHORT|A",
            "09:40_SHORT",
            move=3.0,
            volume=1.0,
            traded_value=100.0,
        ),
        _row(
            "2026-08-28|09:40_SHORT|B",
            "09:40_SHORT",
            move=2.0,
            volume=3.0,
            traded_value=200.0,
        ),
        _row(
            "2026-08-28|09:40_SHORT|C",
            "09:40_SHORT",
            move=1.0,
            volume=2.0,
            traded_value=300.0,
        ),
    )
    selected, decisions, metadata = selection.apply_variant_to_all_candidates(
        source, "V12_S08_0940_SHORT_EQUAL_RANK_PICKER"
    )
    ids = {
        letter: f"2026-08-28|09:40_SHORT|{letter}" for letter in ("A", "B", "C")
    }
    assert selected["candidate_id"].tolist() == [ids["B"], ids["C"], ids["A"]]
    assert selected["picker"].eq(selection.EQUAL_RANK_PICKER).all()
    scores = dict(metadata.equal_rank_picker_scores)
    assert scores == pytest.approx(
        {ids["A"]: -7 / 3, ids["B"]: -5 / 3, ids["C"]: -2.0}
    )
    assert decisions["resolved_frozen_rank"].notna().all()
    assert metadata.equal_rank_algorithm_sha256 == (
        selection.EQUAL_RANK_ALGORITHM_SHA256
    )
    runtime = execution_runtime.runtime_spec_from_rule(
        registry.resolve_variant("V12_S08_0940_SHORT_EQUAL_RANK_PICKER").runtime,
        equal_rank_picker_scores=metadata.equal_rank_picker_scores,
    )
    assert runtime.validated_picker_scores() == pytest.approx(scores)


def test_setup_authority_patches_only_resolved_changed_fields() -> None:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    base = tuple(engine.ACTIVE_SETUPS)
    baseline, baseline_metadata = selection.derive_patched_engine_setups(
        base, registry.CONTROL_VARIANT_ID
    )
    assert baseline == base
    assert baseline_metadata.field_overrides == ()

    patched, metadata = selection.derive_patched_engine_setups(
        base, "V12_S03B_0940_LONG_MOVE_MIN_030"
    )
    by_id = {setup.setup_id: setup for setup in patched}
    assert by_id["09:40_LONG"].price_change_pct == 0.30
    assert len(metadata.field_overrides) == 1
    untouched = {setup.setup_id: asdict(setup) for setup in base if setup.setup_id != "09:40_LONG"}
    observed = {setup.setup_id: asdict(setup) for setup in patched if setup.setup_id != "09:40_LONG"}
    assert observed == untouched

    equal_rank, equal_metadata = selection.derive_patched_engine_setups(
        base, "V12_S08_0940_SHORT_EQUAL_RANK_PICKER"
    )
    assert {setup.setup_id: setup for setup in equal_rank}[
        "09:40_SHORT"
    ].picker == selection.EQUAL_RANK_PICKER
    assert equal_metadata.requires_equal_rank_picker_hook


def test_merged_config_applies_each_disjoint_selection_factor() -> None:
    merged = registry.merge_resolved_configs(
        (
            "V12_S05_0925_LONG_ONLY_MOVE_MAX_125",
            "V12_S06_0945_SHORT_VOLUME_MIN_125",
        )
    )
    source = _frame(
        _row("LONG_HIGH", "09:25_LONG", move=1.3, volume=3.0),
        _row("SHORT_LOW_VOLUME", "09:45_SHORT", move=0.3, volume=1.2),
        _row("PASS", "09:35_SHORT", move=0.6, volume=2.0),
    )
    selected, _, metadata = selection.apply_variant_to_all_candidates(source, merged)
    assert selected["candidate_id"].tolist() == ["PASS"]
    assert metadata.variant_id == merged.variant_id


def test_full_corpus_stage0_matches_v11_frame_and_input_binding() -> None:
    (
        all_candidates,
        minute_paths,
        segments,
        sessions,
        _,
        _,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    observed, _, metadata = selection.apply_variant_to_all_candidates(
        all_candidates, registry.CONTROL_VARIANT_ID
    )
    expected, _ = legacy_filters.selection_overlay(
        all_candidates,
        legacy_filters.SPEC_BY_NAME[v11_backtest.SELECTION_VARIANT],
    )
    assert len(all_candidates) == 1241
    assert len(observed) == len(expected) == 1134
    assert metadata.input_candidate_count == 1241
    assert metadata.selected_candidate_count == 1134
    pd.testing.assert_frame_equal(observed, expected, check_exact=True)
    assert tuple(observed.columns) == tuple(all_candidates.columns)
    assert observed.dtypes.astype(str).tolist() == expected.dtypes.astype(str).tolist()
    assert v11_backtest._input_binding_sha256(
        observed, minute_paths, sessions, segments
    ) == v11_backtest.EXPECTED_INPUT_BINDING_SHA256
