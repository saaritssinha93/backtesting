"""Fast structural checks for the isolated 09:20 profile search."""
from __future__ import annotations

from research.g3_0920_parameter_search import (
    FIVE_TIERS,
    ONE_TIERS,
    choose_on_discovery,
    profile_for,
    search_grid,
)
from research.g3_0920_strategy import reference_profile


def test_search_grid_loosen_five_minutes_before_one_minute():
    grid = search_grid()
    five_order = list(FIVE_TIERS)
    one_order = list(ONE_TIERS)
    assert len(grid) == len(five_order) * len(one_order)
    assert [(five, one) for five, one, _ in grid] == [
        (five, one) for one in one_order for five in five_order
    ]
    assert len({(five, one) for five, one, _ in grid}) == len(grid)
    assert grid[0][:2] == ("strict", "strict")
    assert [one for _, one, _ in grid[:len(five_order)]] == ["strict"] * len(five_order)
    assert all(profile == profile_for(five, one) for five, one, profile in grid)


def test_reference_profile_parity_and_monotonic_relaxation():
    assert profile_for("reference", "reference") == reference_profile()
    for side in ("LONG", "SHORT"):
        five = [FIVE_TIERS[tier][side] for tier in FIVE_TIERS]
        one = [ONE_TIERS[tier][side] for tier in ONE_TIERS]
        for field in ("active_price_pct", "core_price_pct", "oi_change_pct", "volume_ratio"):
            values = [getattr(tier, field) for tier in five]
            assert values == sorted(values, reverse=True), (side, field, values)
        for field in ("body_ratio", "confirmation_volume_ratio"):
            values = [getattr(tier, field) for tier in one]
            assert values == sorted(values, reverse=True), (side, field, values)
        wick = [tier.max_wick_ratio for tier in one]
        assert wick == sorted(wick), (side, "max_wick_ratio", wick)


def _trial(step: int, *, discovery_count: int, holdout_wins: int) -> dict:
    return {
        "step": step,
        "periods": {
            "discovery": {
                "added_0920": {"executed_trades": discovery_count, "win_rate_pct": 75.0},
                "combined": {"win_rate_pct": 75.0},
                "added_long": {"executed_trades": 4, "win_rate_pct": 75.0},
                "added_short": {"executed_trades": 4, "win_rate_pct": 75.0},
                "incremental_pnl_rs": 1000.0,
            },
            "holdout": {
                "added_0920": {"executed_trades": 100, "wins": holdout_wins,
                               "win_rate_pct": float(holdout_wins)},
                "combined": {"win_rate_pct": float(holdout_wins)},
                "incremental_pnl_rs": float(holdout_wins),
            },
        },
    }


def test_choose_on_discovery_ignores_holdout_outcomes():
    stronger_discovery = _trial(1, discovery_count=20, holdout_wins=0)
    weaker_discovery = _trial(2, discovery_count=10, holdout_wins=100)
    chosen, reason = choose_on_discovery([stronger_discovery, weaker_discovery])
    assert chosen is stronger_discovery
    assert reason == "DISCOVERY_70_PCT_AND_POSITIVE_PNL"
    # Reverse the future outcomes: the profile chosen from discovery is unchanged.
    stronger_discovery["periods"]["holdout"]["added_0920"]["wins"] = 100
    weaker_discovery["periods"]["holdout"]["added_0920"]["wins"] = 0
    chosen_again, reason_again = choose_on_discovery([stronger_discovery, weaker_discovery])
    assert chosen_again is stronger_discovery
    assert reason_again == reason
