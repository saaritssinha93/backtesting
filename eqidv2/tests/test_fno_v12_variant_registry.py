from __future__ import annotations

from collections import Counter
from dataclasses import FrozenInstanceError

import pytest

import fno_v12_execution_runtime as execution_runtime
import fno_v12_variant_registry as registry


def test_registry_is_hash_pinned_to_frozen_v11() -> None:
    registry.validate_registry()
    assert registry.registry_sha256() == registry.EXPECTED_REGISTRY_SHA256
    assert len(registry.VARIANT_SPECS) == 40
    assert registry.VARIANT_SPECS[0].variant_id == registry.CONTROL_VARIANT_ID
    assert registry.PARENT_BINDING.profile_id == "V11_S10_POST_HOC_TOP2_1436C7D363"
    assert registry.PARENT_BINDING.profile_sha256 == (
        "8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1"
    )


def test_control_resolves_the_complete_v11_baseline() -> None:
    control = registry.resolve_variant(registry.CONTROL_VARIANT_ID)
    assert control.selection == registry.SelectionConfig()
    assert control.runtime == registry.RuntimeConfig()
    assert control.gap == registry.GapConfig(2.0)
    assert control.changed_fields == ()
    assert not control.post_hoc
    execution_runtime.runtime_spec_from_rule(control.runtime).validate()


def test_attribution_catalog_is_complete_and_exact() -> None:
    stage4 = [spec for spec in registry.VARIANT_SPECS if spec.variant_id.startswith("V12_S04_")]
    assert len(stage4) == 12
    observed = Counter(
        (
            registry.resolve_variant(spec).runtime.m2_short_mode,
            registry.resolve_variant(spec).runtime.m2_short_setup_ids,
        )
        for spec in stage4
    )
    for mode in registry.M2_SHORT_MODES:
        assert observed[(mode, ("09:25_SHORT",))] == 1
        assert observed[(mode, ("09:30_SHORT",))] == 1
        assert observed[(mode, ("09:25_SHORT", "09:30_SHORT"))] == 1

    stage5 = [spec for spec in registry.VARIANT_SPECS if spec.variant_id.startswith("V12_S05_")]
    stage6 = [spec for spec in registry.VARIANT_SPECS if spec.variant_id.startswith("V12_S06_")]
    assert len(stage5) == 6
    assert len(stage6) == 8
    assert {spec.stage_id for spec in stage5} == {
        "STAGE_05A_0925_LONG_STRETCH",
        "STAGE_05B_0925_SHORT_STRETCH",
        "STAGE_05C_0925_BOTH_STRETCH",
    }
    assert {spec.stage_id for spec in stage6} == {
        "STAGE_06A_0935_LONG_VOLUME",
        "STAGE_06B_0940_SHORT_VOLUME",
        "STAGE_06C_0945_SHORT_VOLUME",
        "STAGE_06D_LATE_SHORT_VOLUME",
    }


def test_ema_recurrence_is_executable_and_algorithm_hash_is_bound() -> None:
    assert registry.EMA_GAP_PERSISTENCE_ALGORITHM_SHA256 == (
        registry.canonical_json_sha256(registry.EMA_GAP_PERSISTENCE_ALGORITHM)
    )
    assert not any(
        item.test_id == "EMA_GAP_PERSISTENCE_PRIOR_5M"
        for item in registry.BLOCKED_TESTS
    )
    assert any(
        item.test_id == "FUTURES_OI_PERSISTENCE"
        for item in registry.BLOCKED_TESTS
    )
    for suffix, expected in (("095", 0.95), ("100", 1.0)):
        resolved = registry.resolve_variant(
            f"V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_{suffix}"
        )
        assert resolved.selection.ema_gap_0925_short_persistence_min_ratio == expected


def test_disjoint_stage12_merge_is_canonical_and_overlap_fails() -> None:
    first = "V12_S05_0925_LONG_ONLY_MOVE_MAX_125"
    second = "V12_S07_LONG_ENTRY_EXPIRY_4"
    assert registry.compatible_for_merge(first, second)
    left = registry.merge_resolved_configs((first, second))
    right = registry.merge_resolved_configs((second, first))
    assert left == right
    assert left.post_hoc
    assert left.selection.move_0925_long_max_pct == 1.25
    assert left.runtime.long_entry_expiry_minute == 4
    assert left.gap.max_adverse_gap_bps == 2.0

    competing = "V12_S05_0925_LONG_ONLY_MOVE_MAX_100"
    assert not registry.compatible_for_merge(first, competing)
    with pytest.raises(ValueError, match="overlap fields"):
        registry.merge_resolved_configs((first, competing))
    with pytest.raises(ValueError, match="Stage0"):
        registry.merge_resolved_configs((registry.CONTROL_VARIANT_ID, first))


def test_registry_is_immutable_and_hash_tampering_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TypeError):
        registry.VARIANT_REGISTRY["UNDECLARED"] = registry.VARIANT_SPECS[0]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        registry.VARIANT_SPECS[0].stage_id = "CHANGED"  # type: ignore[misc]
    with pytest.raises(ValueError, match="Unknown FNO V12 variant"):
        registry.get_spec("UNDECLARED")
    monkeypatch.setattr(registry, "EXPECTED_REGISTRY_SHA256", "0" * 64)
    with pytest.raises(AssertionError, match="registry hash changed"):
        registry.validate_registry(
            require_pinned_hash=True,
            require_parent_contract=False,
        )


def test_blocked_validity_stages_are_explicit_and_unique() -> None:
    identities = [(item.stage_id, item.test_id) for item in registry.BLOCKED_TESTS]
    assert len(identities) == len(set(identities))
    assert {item.status for item in registry.BLOCKED_TESTS} == {"BLOCKED_VALIDITY"}
    assert {item.stage_id for item in registry.BLOCKED_TESTS} == {
        "STAGE_01_DATA_VALIDITY",
        "STAGE_02_FUTURES_EXECUTION",
        "STAGE_08_STRUCTURAL_FILTERS",
        "STAGE_09_MARKET_CONTEXT",
        "STAGE_10_PORTFOLIO_RISK",
        "STAGE_11_EXIT_RESEARCH",
    }
