"""Fast checks for the isolated 09:20 one-minute relaxation search."""
from __future__ import annotations

import pytest

import fno_v13_v10_g_3_contract as contract
from research.g3_0920_one_minute_65_search import (
    MIN_EXPLORATORY_FILLS,
    STAGES,
    TARGET_WIN_RATE_PCT,
    choose_largest_65,
    profile_for_stage,
)
from research.g3_0920_strategy import reference_profile


def test_stages_hold_five_minute_rules_fixed_and_respect_native_volume_floors():
    reference = reference_profile()
    assert STAGES[0].name == "strict"
    assert len({stage.name for stage in STAGES}) == len(STAGES)
    for stage in STAGES:
        profile = profile_for_stage(stage)
        assert profile.long.five_minute == reference.long.five_minute
        assert profile.short.five_minute == reference.short.five_minute
        assert profile.long.one_minute == stage.long
        assert profile.short.one_minute == stage.short
        for side in ("LONG", "SHORT"):
            minimum = contract.confirmation_volume_minimum(side, setup_id=f"0921_{side}")
            assert profile.for_side(side).one_minute.confirmation_volume_ratio >= minimum


def test_cumulative_stages_only_relax_one_minute_gates():
    cumulative = [stage for stage in STAGES if stage.branch == "cumulative"]
    assert cumulative[0].name == "strict"
    assert cumulative[-1].name == "reference_1m"
    for side in ("LONG", "SHORT"):
        rules = [getattr(stage, side.lower()) for stage in cumulative]
        for earlier, later in zip(rules, rules[1:]):
            assert later.body_ratio <= earlier.body_ratio
            assert later.max_wick_ratio >= earlier.max_wick_ratio
            assert later.confirmation_volume_ratio <= earlier.confirmation_volume_ratio
    assert all((later.long, later.short) != (earlier.long, earlier.short)
               for earlier, later in zip(cumulative, cumulative[1:]))
    # A directional wick can never exceed 1 - body ratio. A wick-only step
    # above that physical limit would change a number without changing a gate.
    def effective_gates(rule):
        return (rule.body_ratio,
                min(rule.max_wick_ratio, 1 - rule.body_ratio),
                rule.confirmation_volume_ratio)

    assert all(
        (effective_gates(later.long), effective_gates(later.short)) !=
        (effective_gates(earlier.long), effective_gates(earlier.short))
        for earlier, later in zip(cumulative, cumulative[1:])
    )


def _trial(step: int, *, fills: int = 10, added_rate: float = 65.0,
           combined_rate: float = 65.0, pnl: float = 1.0,
           holdout_rate: float = 0.0) -> dict:
    return {
        "step": step,
        "periods": {
            "all": {
                "added_0920": {"executed_trades": fills, "win_rate_pct": added_rate},
                "combined": {"win_rate_pct": combined_rate},
                "incremental_pnl_rs": pnl,
            },
            "holdout": {
                "added_0920": {"executed_trades": 100, "win_rate_pct": holdout_rate},
                "combined": {"win_rate_pct": holdout_rate},
                "incremental_pnl_rs": holdout_rate,
            },
        },
    }


@pytest.mark.parametrize("changes", [
    {"fills": MIN_EXPLORATORY_FILLS - 1},
    {"added_rate": TARGET_WIN_RATE_PCT - 0.01},
    {"added_rate": None},
    {"combined_rate": TARGET_WIN_RATE_PCT - 0.01},
    {"pnl": 0.0},
    {"pnl": -1.0},
])
def test_choose_largest_65_requires_sample_both_win_rates_and_positive_pnl(changes):
    assert choose_largest_65([_trial(1, **changes)], "all") is None
    boundary = _trial(2)
    assert choose_largest_65([boundary], "all") is boundary


def test_choose_largest_65_uses_fill_count_then_pnl_then_earlier_stage():
    smaller = _trial(1, fills=10, pnl=1000.0, holdout_rate=100.0)
    larger_low_pnl = _trial(2, fills=12, pnl=100.0, holdout_rate=100.0)
    larger_high_pnl_late = _trial(4, fills=12, pnl=200.0, holdout_rate=0.0)
    larger_high_pnl_early = _trial(3, fills=12, pnl=200.0, holdout_rate=0.0)
    candidates = [smaller, larger_low_pnl, larger_high_pnl_late,
                  larger_high_pnl_early]
    assert choose_largest_65(candidates, "all") is larger_high_pnl_early
    # The specified period governs the choice; excellent future results cannot
    # rescue a profile with fewer historical fills.
    smaller["periods"]["holdout"]["added_0920"]["win_rate_pct"] = 0.0
    larger_high_pnl_early["periods"]["holdout"]["added_0920"]["win_rate_pct"] = 100.0
    assert choose_largest_65(candidates, "all") is larger_high_pnl_early
