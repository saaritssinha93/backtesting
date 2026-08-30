from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

import fno_v11_variant_registry as registry


def test_registry_and_parent_control_are_hash_pinned() -> None:
    registry.validate_registry()
    assert registry.registry_sha256() == registry.EXPECTED_REGISTRY_SHA256
    assert registry.BASELINE_BINDING.profile_id == (
        "V10_STAGE7_0935_LONG_MAX_050_GAP2"
    )
    assert registry.BASELINE_BINDING.benchmark_contract == (
        "PINNED_65_SESSION_REFERENCE_15_0"
    )


def test_stage0_is_first_and_contains_no_experiment() -> None:
    control = registry.VARIANT_SPECS[0]
    assert control.variant_id == registry.CONTROL_VARIANT_ID
    assert control.stage_id == "STAGE_00"
    assert control.mechanism_count == 0
    assert control.mechanism_type == "CONTROL"


def test_every_challenger_is_setup_specific_and_one_factor() -> None:
    for spec in registry.VARIANT_SPECS[1:]:
        assert spec.mechanism_count == 1
        registry.validate_variant_spec(spec)
        if spec.selection_rule is not None:
            assert spec.selection_rule.setup_id in registry.VALID_SETUP_IDS
            assert len(spec.selection_rule.active_thresholds()) == 1
        if spec.disabled_setup_id is not None:
            assert spec.disabled_setup_id in registry.VALID_SETUP_IDS
        if spec.picker_override is not None:
            assert spec.picker_override.setup_id in registry.VALID_SETUP_IDS
        if spec.cap_override is not None:
            assert spec.cap_override.setup_id in registry.VALID_SETUP_IDS


def test_priority_selection_values_are_exact() -> None:
    breadth = registry.get_spec("v11_s1_0930_short_breadth_min_4")
    assert breadth.selection_rule == registry.SelectionRule(
        "09:30_SHORT", min_setup_breadth_inclusive=4
    )

    move = registry.get_spec("V11_S1_0940_SHORT_MOVE_MAX_050")
    assert move.selection_rule == registry.SelectionRule(
        "09:40_SHORT", price_move_max_pct_exclusive=0.50
    )

    volume = registry.get_spec("V11_S1_0935_LONG_VOLUME_MIN_150")
    assert volume.selection_rule == registry.SelectionRule(
        "09:35_LONG", volume_ratio_min_inclusive=1.50
    )


def test_picker_and_cap_variants_do_not_repeat_the_control_value() -> None:
    for spec in registry.VARIANT_SPECS:
        if spec.picker_override is not None:
            override = spec.picker_override
            assert override.picker != registry.BASE_SETUP_PICKERS[override.setup_id]
        if spec.cap_override is not None:
            override = spec.cap_override
            assert override.max_entries != registry.BASE_SETUP_CAPS[override.setup_id]


def test_registry_is_immutable_and_unknown_variants_fail_closed() -> None:
    with pytest.raises(TypeError):
        registry.VARIANT_REGISTRY["UNDECLARED"] = registry.VARIANT_SPECS[0]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        registry.VARIANT_SPECS[0].stage_id = "CHANGED"  # type: ignore[misc]
    with pytest.raises(ValueError, match="Unknown FNO V11 variant"):
        registry.get_spec("UNDECLARED")


def test_multi_mechanism_and_invalid_bounds_fail_closed() -> None:
    combined = registry.VariantSpec(
        "V11_INVALID_COMBINED",
        "STAGE_01_99",
        "invalid combined test",
        selection_rule=registry.SelectionRule(
            "09:30_SHORT", min_setup_breadth_inclusive=4
        ),
        picker_override=registry.PickerOverride(
            "09:30_SHORT", "max_liquidity"
        ),
    )
    with pytest.raises(AssertionError, match="exactly one mechanism"):
        registry.validate_variant_spec(combined)

    invalid_bound = registry.VariantSpec(
        "V11_INVALID_BOUND",
        "STAGE_01_99",
        "invalid threshold test",
        selection_rule=registry.SelectionRule(
            "09:40_SHORT", price_move_max_pct_exclusive=float("nan")
        ),
    )
    with pytest.raises(AssertionError, match="finite and positive"):
        registry.validate_variant_spec(invalid_bound)


def test_pinned_hash_tampering_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "EXPECTED_REGISTRY_SHA256", "0" * 64)
    with pytest.raises(AssertionError, match="registry hash changed"):
        registry.validate_registry(require_parent_contract=False)


def test_already_tested_global_mechanisms_are_explicitly_excluded() -> None:
    payload = registry.registry_payload()
    excluded = set(
        payload["execution_contract"]["excluded_already_tested_mechanisms"]
    )
    assert excluded == {
        "GLOBAL_RV1",
        "EXPIRY_S4",
        "GLOBAL_RV1_PLUS_S4",
        "HISTORICAL_SAME_SLOT_RVOL",
        "UNIFORM_MAX_ENTRIES",
    }
    assert payload["execution_contract"]["one_factor_per_challenger"] is True
    assert payload["execution_contract"]["free_form_thresholds_allowed"] is False
    assert payload["research_only"] is True
    assert payload["promotion_eligible"] is False
    assert payload["live_or_paper_authority"] is False
