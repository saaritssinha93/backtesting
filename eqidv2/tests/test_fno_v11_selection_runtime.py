from __future__ import annotations

from dataclasses import asdict, replace

import pandas as pd
import pandas.testing as pdt
import pytest

import fno_v10_unified_5m_1m_backtest as v10
import fno_v11_selection_runtime as runtime
import fno_v11_variant_registry as registry


def _row(
    candidate_id: str,
    setup_id: str,
    *,
    day: str = "2026-08-01",
    move: float | None = None,
    volume_ratio: float = 2.0,
    high: float = 100.5,
    low: float = 99.5,
    close: float = 100.0,
    oi_change_pct: float = 1.0,
    traded_value: float = 1_000_000.0,
    frozen_rank: int = 1,
) -> dict[str, object]:
    side = setup_id.rsplit("_", 1)[1]
    if move is None:
        move = 0.40 if side == "LONG" else -0.40
    return {
        "candidate_id": candidate_id,
        "session_date": day,
        "setup_id": setup_id,
        "side": side,
        "price_change_pct": move,
        "oi_change_pct": oi_change_pct,
        "volume_ratio": volume_ratio,
        "traded_value": traded_value,
        "five_min_high": high,
        "five_min_low": low,
        "five_min_close": close,
        "symbol": candidate_id,
        "picker": registry.BASE_SETUP_PICKERS[setup_id],
        "picker_value": 0.0,
        "frozen_rank": frozen_rank,
    }


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _setup(setups: tuple[object, ...], setup_id: str) -> object:
    return next(setup for setup in setups if setup.setup_id == setup_id)


def test_control_is_lossless_and_does_not_mutate_input() -> None:
    candidates = _frame(
        [
            _row("A", "09:25_LONG"),
            _row("B", "09:40_SHORT"),
        ]
    )
    before = candidates.copy(deep=True)
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        registry.CONTROL_VARIANT_ID,
    )
    pdt.assert_frame_equal(candidates, before)
    pdt.assert_frame_equal(selected, before.reset_index(drop=True))
    assert decisions["kept"].tolist() == [True, True]
    assert set(decisions["reason"]) == {"V11_CONTROL_PASSTHROUGH"}
    assert tuple(decisions.columns) == runtime.DECISION_COLUMNS


def test_breadth_is_counted_before_any_challenger_row_is_removed() -> None:
    rows = [
        _row(f"D1_{i}", "09:30_SHORT", day="2026-08-01")
        for i in range(3)
    ]
    rows += [
        _row(f"D2_{i}", "09:30_SHORT", day="2026-08-02")
        for i in range(4)
    ]
    rows.append(_row("OTHER", "09:30_LONG", day="2026-08-01"))
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        _frame(rows),
        "V11_S1_0930_SHORT_BREADTH_MIN_4",
    )

    assert selected["candidate_id"].tolist() == [
        "D2_0",
        "D2_1",
        "D2_2",
        "D2_3",
        "OTHER",
    ]
    by_id = decisions.set_index("candidate_id")
    assert by_id.loc[["D1_0", "D1_1", "D1_2"], "measured_value"].tolist() == [
        3.0,
        3.0,
        3.0,
    ]
    assert by_id.loc[["D2_0", "D2_1", "D2_2", "D2_3"], "measured_value"].tolist() == [
        4.0,
        4.0,
        4.0,
        4.0,
    ]
    assert bool(by_id.loc["OTHER", "kept"]) is True
    assert by_id.loc["OTHER", "reason"] == "V11_NOT_TARGET_SETUP"


def test_directional_short_move_uses_absolute_value_and_max_is_exclusive() -> None:
    candidates = _frame(
        [
            _row("PASS", "09:40_SHORT", move=-0.499),
            _row("BOUNDARY", "09:40_SHORT", move=-0.500),
            _row("OTHER", "09:40_LONG", move=0.700),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S1_0940_SHORT_MOVE_MAX_050",
    )
    assert selected["candidate_id"].tolist() == ["PASS", "OTHER"]
    by_id = decisions.set_index("candidate_id")
    assert by_id.loc["PASS", "measured_value"] == pytest.approx(0.499)
    assert by_id.loc["BOUNDARY", "measured_value"] == pytest.approx(0.500)
    assert by_id.loc["BOUNDARY", "comparator"] == "LT"
    assert bool(by_id.loc["BOUNDARY", "kept"]) is False


def test_range_formula_and_exclusive_max_boundary_are_exact() -> None:
    candidates = _frame(
        [
            _row(
                "PASS",
                "09:25_SHORT",
                high=100.49,
                low=99.51,
                close=100.0,
            ),
            _row(
                "BOUNDARY",
                "09:25_SHORT",
                high=100.50,
                low=99.50,
                close=100.0,
            ),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S2_0925_SHORT_RANGE_MAX_100",
    )
    assert selected["candidate_id"].tolist() == ["PASS"]
    by_id = decisions.set_index("candidate_id")
    assert by_id.loc["PASS", "measured_value"] == pytest.approx(0.98)
    assert by_id.loc["BOUNDARY", "measured_value"] == pytest.approx(1.0)
    assert bool(by_id.loc["BOUNDARY", "kept"]) is False


def test_volume_min_is_inclusive_and_is_also_patched_into_setup_book() -> None:
    candidates = _frame(
        [
            _row("LOW", "09:35_LONG", volume_ratio=1.499),
            _row("BOUNDARY", "09:35_LONG", volume_ratio=1.500),
            _row("OTHER", "09:35_SHORT", volume_ratio=1.0),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S1_0935_LONG_VOLUME_MIN_150",
    )
    assert selected["candidate_id"].tolist() == ["BOUNDARY", "OTHER"]
    by_id = decisions.set_index("candidate_id")
    assert by_id.loc["BOUNDARY", "comparator"] == "GE"
    assert bool(by_id.loc["BOUNDARY", "kept"]) is True

    setups, metadata = runtime.derive_patched_engine_setups(
        v10.ACTIVE_SETUPS,
        "V11_S1_0935_LONG_VOLUME_MIN_150",
    )
    assert _setup(setups, "09:35_LONG").volume_ratio == pytest.approx(1.5)
    assert metadata.external_selection_required is True
    assert [asdict(value) for value in metadata.field_overrides] == [
        {
            "setup_id": "09:35_LONG",
            "field_name": "volume_ratio",
            "old_value": 1.0,
            "new_value": 1.5,
        }
    ]


def test_disabled_setup_filters_exact_id_and_removes_engine_leg() -> None:
    candidates = _frame(
        [
            _row("SHORT", "09:45_SHORT"),
            _row("LONG", "09:45_LONG"),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S2_DISABLE_0945_SHORT",
    )
    assert selected["candidate_id"].tolist() == ["LONG"]
    by_id = decisions.set_index("candidate_id")
    assert by_id.loc["SHORT", "reason"] == "V11_DISABLED_SETUP_REJECTED"
    assert by_id.loc["SHORT", "measured_value"] == pytest.approx(0.0)

    setups, metadata = runtime.derive_patched_engine_setups(
        v10.ACTIVE_SETUPS,
        "V11_S2_DISABLE_0945_SHORT",
    )
    assert len(setups) == 9
    assert {setup.setup_id for setup in setups} == (
        registry.VALID_SETUP_IDS - {"09:45_SHORT"}
    )
    assert metadata.disabled_setup_ids == ("09:45_SHORT",)


def test_native_picker_patch_does_not_pretruncate_candidates() -> None:
    candidates = _frame(
        [
            _row("A", "09:30_SHORT", traded_value=100.0, frozen_rank=1),
            _row("B", "09:30_SHORT", traded_value=300.0, frozen_rank=2),
            _row("C", "09:25_LONG", traded_value=200.0, frozen_rank=7),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S3_0930_SHORT_PICK_MAX_LIQUIDITY",
    )
    assert selected["candidate_id"].tolist() == ["A", "B", "C"]
    assert decisions["kept"].all()
    selected_by_id = selected.set_index("candidate_id")
    assert selected_by_id.loc["A", "picker"] == "max_liquidity"
    assert selected_by_id.loc["A", "picker_value"] == pytest.approx(100.0)
    assert selected_by_id.loc["B", "picker_value"] == pytest.approx(300.0)
    assert selected_by_id.loc["B", "frozen_rank"] == 1
    assert selected_by_id.loc["A", "frozen_rank"] == 2
    # A different setup is bit-for-bit untouched by the local rerank.
    assert selected_by_id.loc["C", "picker"] == "max_move"
    assert selected_by_id.loc["C", "frozen_rank"] == 7
    decision_by_id = decisions.set_index("candidate_id")
    assert decision_by_id.loc["A", "reason"] == (
        "V11_PICKER_OVERRIDE_PASSTHROUGH"
    )
    assert decision_by_id.loc["C", "reason"] == "V11_NOT_TARGET_SETUP"
    assert decision_by_id.loc["B", "measured_value"] == pytest.approx(300.0)

    setups, metadata = runtime.derive_patched_engine_setups(
        v10.ACTIVE_SETUPS,
        "V11_S3_0930_SHORT_PICK_MAX_LIQUIDITY",
    )
    assert _setup(setups, "09:30_SHORT").picker == "max_liquidity"
    assert metadata.requires_runner_picker_hook is False
    assert metadata.field_overrides[0].field_name == "picker"


def test_min_volume_returns_mandatory_hook_without_invalid_engine_picker() -> None:
    candidates = _frame(
        [
            _row(
                "HIGH_VOLUME",
                "09:30_SHORT",
                volume_ratio=3.0,
                traded_value=500.0,
                frozen_rank=1,
            ),
            _row(
                "LOW_VOLUME",
                "09:30_SHORT",
                volume_ratio=1.2,
                traded_value=100.0,
                frozen_rank=2,
            ),
            _row(
                "LOW_VOLUME_LIQUID",
                "09:30_SHORT",
                volume_ratio=1.2,
                traded_value=300.0,
                frozen_rank=3,
            ),
        ]
    )
    selected, decisions = runtime.apply_variant_to_selected_candidates(
        candidates,
        "V11_S3_0930_SHORT_PICK_MIN_VOLUME",
    )
    assert len(selected) == len(candidates)
    by_id = selected.set_index("candidate_id")
    assert by_id.loc["LOW_VOLUME_LIQUID", "picker_value"] == pytest.approx(-1.2)
    assert by_id.loc["LOW_VOLUME_LIQUID", "frozen_rank"] == 1
    assert by_id.loc["LOW_VOLUME", "frozen_rank"] == 2
    assert by_id.loc["HIGH_VOLUME", "picker_value"] == pytest.approx(-3.0)
    assert by_id.loc["HIGH_VOLUME", "frozen_rank"] == 3
    assert decisions["kept"].all()

    setups, metadata = runtime.derive_patched_engine_setups(
        v10.ACTIVE_SETUPS,
        "V11_S3_0930_SHORT_PICK_MIN_VOLUME",
    )
    # The setup carries the dispatch value; the mandatory V11 runner hook
    # teaches the otherwise-neutral engine how to evaluate it.
    assert _setup(setups, "09:30_SHORT").picker == "min_volume"
    assert metadata.requires_runner_picker_hook is True
    assert metadata.picker_hook == runtime.PickerHook(
        setup_id="09:30_SHORT",
        picker="min_volume",
        value_field="volume_ratio",
        value_multiplier=-1.0,
        descending=True,
        secondary_field="traded_value",
        secondary_descending=True,
        final_tiebreaker_field="symbol",
        final_tiebreaker_descending=False,
    )
    assert metadata.field_overrides == (
        runtime.SetupFieldOverride(
            "09:30_SHORT",
            "picker",
            "max_volume",
            "min_volume",
        ),
    )


def test_cap_patch_changes_only_targeted_setup() -> None:
    setups, metadata = runtime.derive_patched_engine_setups(
        v10.ACTIVE_SETUPS,
        "V11_S4_0940_LONG_CAP_2",
    )
    before = {setup.setup_id: setup.max_entries for setup in v10.ACTIVE_SETUPS}
    after = {setup.setup_id: setup.max_entries for setup in setups}
    assert after["09:40_LONG"] == 2
    assert {
        key: value for key, value in after.items() if key != "09:40_LONG"
    } == {key: value for key, value in before.items() if key != "09:40_LONG"}
    assert metadata.field_overrides == (
        runtime.SetupFieldOverride("09:40_LONG", "max_entries", 1, 2),
    )


def test_invalid_data_and_ad_hoc_specs_fail_closed() -> None:
    missing = pd.DataFrame({"candidate_id": ["A"], "setup_id": ["09:40_SHORT"]})
    with pytest.raises(ValueError, match="missing columns"):
        runtime.apply_variant_to_selected_candidates(
            missing,
            "V11_S1_0940_SHORT_MOVE_MAX_050",
        )

    duplicate = _frame(
        [
            _row("A", "09:25_LONG"),
            _row("A", "09:25_SHORT"),
        ]
    )
    with pytest.raises(ValueError, match="must be unique"):
        runtime.apply_variant_to_selected_candidates(
            duplicate,
            registry.CONTROL_VARIANT_ID,
        )

    registered = registry.get_spec("V11_S1_0940_SHORT_MOVE_MAX_050")
    ad_hoc = registry.VariantSpec(
        registered.variant_id,
        registered.stage_id,
        "silently changed description",
        selection_rule=registered.selection_rule,
    )
    with pytest.raises(ValueError, match="differs from the pinned registry"):
        runtime.apply_variant_to_selected_candidates(_frame([]), ad_hoc)


def test_patched_setup_derivation_rejects_nonbaseline_books() -> None:
    already_changed = tuple(
        replace_setup
        if replace_setup.setup_id != "09:40_LONG"
        else replace(replace_setup, max_entries=2)
        for replace_setup in v10.ACTIVE_SETUPS
    )
    with pytest.raises(ValueError, match="mixed caps differ"):
        runtime.derive_patched_engine_setups(
            already_changed,
            registry.CONTROL_VARIANT_ID,
        )
