"""Only the declared retention allowance changes the fixed G4 policy screen."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g4_retention_limit_backtest as cap


def policy(name="candidate", **changes):
    row = dict(policy=name, period="development35", original_winner_profit_retention_pct=92.,
        loss_saved_rs=200., changed_trades=4, net_change_rs=50.,
        wins=11, baseline_wins=10, losses=4, baseline_losses=5, qualifies=False)
    row.update(changes)
    return row


def test_relaxed_cap_uses_numeric_outcomes_instead_of_previous_95pct_flag():
    table = pd.DataFrame([policy(), policy("old_safe", original_winner_profit_retention_pct=97., loss_saved_rs=100.)])
    assert table.qualifies.eq(False).all()
    assert cap.choose(table, .95) == "old_safe"
    assert cap.choose(table, .90) == "candidate"
    pd.testing.assert_frame_equal(table, pd.DataFrame([policy(), policy("old_safe", original_winner_profit_retention_pct=97., loss_saved_rs=100.)]))


@pytest.mark.parametrize("retention,allowed", [(90., True), (89.99, False), (95., True), (np.nan, False)])
def test_90pct_original_winner_profit_floor_is_inclusive(retention, allowed):
    row = pd.DataFrame([policy(original_winner_profit_retention_pct=retention)])
    assert bool(cap.eligible(row, .90).iloc[0]) is allowed


@pytest.mark.parametrize("change", [
    dict(net_change_rs=-1.), dict(wins=10), dict(losses=5),
    dict(loss_saved_rs=0.), dict(changed_trades=2),
])
def test_strict_screen_retains_each_original_economic_and_evidence_requirement(change):
    assert not cap.eligible(pd.DataFrame([policy(**change)]), .90).iloc[0]


def test_loss_focused_choice_can_trade_net_and_win_rate_for_less_loss_money():
    table = pd.DataFrame([
        policy("loss_tradeoff", loss_saved_rs=300., net_change_rs=-100., wins=9, losses=6),
        policy("economic", loss_saved_rs=100.),
    ])
    assert cap.choose(table, .90) == "economic"
    assert cap.choose(table, .90, require_economic=False) == "loss_tradeoff"
    table.loc[table.policy.eq("loss_tradeoff"), "original_winner_profit_retention_pct"] = 89.9
    assert cap.choose(table, .90, require_economic=False) == "economic"


def test_sparse_economic_success_is_distinct_from_three_trade_evidence_screen():
    table = pd.DataFrame([policy("sparse", changed_trades=1, original_winner_profit_retention_pct=100.)])
    selected = cap.selections(table, .90)
    assert selected["strict_policy"] == "baseline"
    assert selected["strict_qualified_count"] == 0
    assert selected["economic_sparse_candidate"] == "sparse"
    assert selected["economic_qualified_count"] == 1
    assert selected["loss_focused_policy"] == "baseline"


def test_later_and_full_history_outcomes_never_choose_development_policy():
    table = pd.DataFrame([
        policy("first35"),
        policy("later_oracle", period="later17", loss_saved_rs=1e9, net_change_rs=1e9),
        policy("full_oracle", period="all", loss_saved_rs=1e12, net_change_rs=1e12),
    ])
    before = cap.selections(table, .90)
    later = table.period.ne("development35")
    table.loc[later, ["original_winner_profit_retention_pct", "loss_saved_rs", "net_change_rs", "wins", "losses"]] = [100., 1e15, 1e15, 1000, 0]
    assert cap.selections(table, .90) == before
    assert before["strict_policy"] == before["loss_focused_policy"] == "first35"


def test_ranking_is_loss_saved_then_net_then_stable_identifier():
    table = pd.DataFrame([
        policy("z", loss_saved_rs=300., net_change_rs=20.),
        policy("a", loss_saved_rs=300., net_change_rs=20.),
        policy("large_net", loss_saved_rs=200., net_change_rs=1e9),
    ])
    assert cap.choose(table, .90) == "a"
    table.loc[table.policy.eq("z"), "net_change_rs"] = 30.
    assert cap.choose(table, .90) == "z"


@pytest.mark.parametrize("floor", [0., -1., 1.01, np.nan, np.inf])
def test_invalid_retention_fraction_is_rejected(floor):
    with pytest.raises(ValueError, match="retention"):
        cap.eligible(pd.DataFrame([policy()]), floor)


def test_period_metrics_keep_sid_alignment_and_report_cost_covering_trades_as_flat():
    metadata = pd.DataFrame({"day": ["2026-10-15"] * 3}, index=[3, 1, 2])
    baseline = pd.Series([100., -100., -100.], index=[1, 2, 3])
    candidate = pd.Series([-1e-11, -100., 90.], index=[2, 3, 1])
    result = cap.period_metrics(baseline, candidate, metadata, ["2026-10-15"])
    assert result["trades"] == 3
    assert result["wins"] == 1 and result["losses"] == 1 and result["flat_trades"] == 1
    assert result["original_winner_profit_retention_pct"] == 90.
    assert result["original_winner_profit_after_rs"] == 90.
    assert result["net_change_rs"] == pytest.approx(90.)
