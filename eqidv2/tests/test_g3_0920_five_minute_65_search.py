"""Guardrails for the backtest-only 09:20 five-minute filter sweep."""
from __future__ import annotations

import fno_v13_v10_g_3_contract as contract
from research.g3_0920_five_minute_65_search import (
    FIXED_LONG_1M,
    FIXED_SHORT_1M,
    STAGES,
    profile_for_stage,
)
from research.g3_0920_one_minute_65_search import STAGES as ONE_MINUTE_STAGES
from research.g3_0920_strategy import reference_profile, setup_specs


def test_every_stage_keeps_the_selected_one_minute_confirmation_fixed():
    prior_choice = next(stage for stage in ONE_MINUTE_STAGES
                        if stage.name == "volume_160_170")
    assert FIXED_LONG_1M == prior_choice.long
    assert FIXED_SHORT_1M == prior_choice.short
    for stage in STAGES:
        profile = profile_for_stage(stage)
        assert profile.long.one_minute == FIXED_LONG_1M
        assert profile.short.one_minute == FIXED_SHORT_1M
        assert profile.long.five_minute == stage.long
        assert profile.short.five_minute == stage.short
        # Constructing the copied G-3 setup also exercises profile validation.
        assert set(setup_specs(profile)) == {"LONG", "SHORT"}


def test_reference_stage_reproduces_the_previous_five_minute_gates():
    reference = reference_profile()
    stage = STAGES[0]
    assert stage.name == "reference_5m"
    assert stage.long == reference.long.five_minute
    assert stage.short == reference.short.five_minute


def test_every_stage_respects_native_directional_and_volume_floors():
    for stage in STAGES:
        for side in ("LONG", "SHORT"):
            gate = getattr(stage, side.lower())
            assert contract.BASE_PRICE_CHANGE_PCT <= gate.active_price_pct <= gate.core_price_pct
            assert contract.BASE_OI_CHANGE_PCT <= gate.oi_change_pct <= contract.MAX_OI_CHANGE_PCT
            assert gate.volume_ratio >= contract.BASE_VOLUME_RATIO


def test_cumulative_five_minute_ladder_is_unique_and_only_relaxes():
    assert len({stage.name for stage in STAGES}) == len(STAGES)
    cumulative = [stage for stage in STAGES if stage.branch == "cumulative"]
    assert cumulative[0].name == "reference_5m"
    assert cumulative[-1].name == "oi_base"
    for side in ("LONG", "SHORT"):
        gates = [getattr(stage, side.lower()) for stage in cumulative]
        for previous, following in zip(gates, gates[1:]):
            assert following.active_price_pct <= previous.active_price_pct
            assert following.core_price_pct <= previous.core_price_pct
            assert following.oi_change_pct <= previous.oi_change_pct
            assert following.volume_ratio <= previous.volume_ratio
    assert all((previous.long, previous.short) != (following.long, following.short)
               for previous, following in zip(cumulative, cumulative[1:]))
    assert len({(stage.long, stage.short) for stage in STAGES}) == len(STAGES)
