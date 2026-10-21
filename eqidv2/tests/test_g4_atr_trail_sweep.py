"""Selection discipline and parameter coverage for the G4 trailing grid."""
from __future__ import annotations

import pandas as pd

from research import g4_atr_trail_sweep as sweep


def test_catalog_is_predeclared_unique_and_has_all_floor_and_atr_combinations():
    policies = sweep.catalog()
    assert len(policies) == 54
    assert len({item["policy"] for item in policies}) == 54
    assert {item["parameters"]["activation_target_fraction"] for item in policies} == {.5, .7, .9}
    assert {item["parameters"]["atr_multiple"] for item in policies} == {1., 1.5, 2., 3., 4., 6.}
    assert {item["parameters"]["protected_profit_pct"] for item in policies} == {None, .1, .2}
    assert all(item["parameters"]["initial_stop_pct"] == 1.2 for item in policies)
    assert all(item["parameters"]["tighten_after_minutes"] == 120 for item in policies)
    assert all(item["parameters"]["tightened_stop_pct"] == 1. for item in policies)


def test_development_selection_requires_both_objectives_and_enough_changes():
    rows = pd.DataFrame([
        dict(policy="few", changed_trades=2, original_winner_profit_retention_pct=100.,
             loss_saved_rs=3000., net_change_rs=9000.),
        dict(policy="loses_profit", changed_trades=8, original_winner_profit_retention_pct=100.,
             loss_saved_rs=5000., net_change_rs=-1.),
        dict(policy="winner_sacrifice", changed_trades=8, original_winner_profit_retention_pct=97.,
             loss_saved_rs=5000., net_change_rs=100.),
        dict(policy="winner", changed_trades=3, original_winner_profit_retention_pct=98.5,
             loss_saved_rs=1000., net_change_rs=100.),
        dict(policy="net_first", changed_trades=4, original_winner_profit_retention_pct=99.,
             loss_saved_rs=900., net_change_rs=10000.),
    ])
    assert sweep.choose(rows) == "winner"
    assert sweep.choose(rows.loc[rows.policy.ne("winner") & rows.policy.ne("net_first")]) is None


def test_later_period_cannot_enter_selection_table():
    development = pd.DataFrame([
        dict(policy="a", changed_trades=3, original_winner_profit_retention_pct=99.,
             loss_saved_rs=1000., net_change_rs=100.),
        dict(policy="b", changed_trades=3, original_winner_profit_retention_pct=99.,
             loss_saved_rs=900., net_change_rs=10000.),
    ])
    assert sweep.choose(development) == "a"
    later = development.copy()
    later.loc[later.policy.eq("b"), "loss_saved_rs"] = 1000000.
    assert sweep.choose(development) == "a"
