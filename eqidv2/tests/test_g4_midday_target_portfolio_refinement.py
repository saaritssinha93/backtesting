"""Capital-adjusted qualification and bounded removal of midday setups."""

import copy

import pandas as pd
import pytest

from research import g4_midday_target_portfolio_refinement as refinement


def ledger(setup_id, profits, *, executed=True):
    return pd.DataFrame({
        "setup_id": [setup_id] * len(profits),
        "portfolio_executed": [executed] * len(profits),
        "portfolio_net_profit_rupees": profits,
    })


def qualifying_additions():
    # Exactly 70% net wins is eligible; each setup has sufficient observations.
    return pd.concat([
        ledger("A", [10.] * 7 + [-5.] * 3),
        ledger("B", [20.] * 7 + [-10.] * 3),
    ], ignore_index=True)


def test_qualification_accepts_exact_win_rate_boundary_and_ignores_unexecuted_pnl():
    added = qualifying_additions()
    blocked = ledger("A", [-1e9] * 50, executed=False)
    added = pd.concat([added, blocked], ignore_index=True)
    combined = pd.concat([ledger("BASE", [1.] * 10), added], ignore_index=True)
    assert refinement.qualifies(combined, added)


@pytest.mark.parametrize("profits", [
    [10.] * 6 + [-1.] * 4,  # A profitable setup still fails below 70% wins.
    [10.] * 7 + [-100.] * 3,  # A 70% win rate cannot excuse a net loss.
    [3.] * 7 + [-7.] * 3,  # Break-even P&L is not positive P&L.
    [10.] * 4,  # Four successful executions are still too few observations.
    [10.] * 6 + [0.] + [-1.] * 3,  # Zero after costs is not a net win.
])
def test_each_selected_setup_must_qualify_even_with_strong_other_setups(profits):
    added = pd.concat([
        ledger("STRONG", [1e4] * 100),
        ledger("WEAK", profits),
    ], ignore_index=True)
    combined = pd.concat([ledger("BASE", [1e4] * 100), added], ignore_index=True)
    assert not refinement.qualifies(combined, added)


def test_selected_setup_with_no_executions_cannot_disappear_from_qualification():
    added = pd.concat([
        qualifying_additions(),
        ledger("NO_CAPACITY", [1e6] * 20, executed=False),
    ], ignore_index=True)
    assert not refinement.qualifies(added, added)


@pytest.mark.parametrize("baseline_profits", [[-1.] * 20, [1.] * 100 + [-1000.] * 5])
def test_combined_portfolio_must_meet_win_rate_and_positive_pnl(baseline_profits):
    added = qualifying_additions()
    assert refinement.qualifies(added, added)
    combined = pd.concat([ledger("BASE", baseline_profits), added], ignore_index=True)
    assert not refinement.qualifies(combined, added)


def test_minimum_execution_requirement_is_applied_to_every_setup():
    added = pd.concat([ledger("A", [10.] * 5), ledger("B", [10.] * 6)])
    assert refinement.qualifies(added, added, minimum=5)
    assert not refinement.qualifies(added, added, minimum=6)


def test_empty_additions_are_ineligible_even_with_a_profitable_baseline():
    added = ledger("A", [])
    assert not refinement.qualifies(ledger("BASE", [10.] * 20), added)


@pytest.mark.parametrize("invalid_pnl", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_executed_pnl_cannot_qualify(invalid_pnl):
    added = ledger("A", [10.] * 10)
    added.loc[0, "portfolio_net_profit_rupees"] = invalid_pnl
    assert not refinement.qualifies(added, added)


def test_missing_added_setup_id_is_rejected_even_when_not_executed():
    added = pd.concat([
        ledger("A", [10.] * 10),
        ledger(None, [0.], executed=False),
    ], ignore_index=True)
    assert not refinement.qualifies(added, added)


def evaluation(profit, *, eligible=True):
    return {
        "eligible": eligible,
        "combined_net_pnl_rs": profit,
        "added_net_pnl_rs": profit / 2.,
        "added_win_rate_pct": 80.,
        "combined_win_rate_pct": 80.,
    }


def accepted_removals(trials):
    return [trial["removed_setup"] for trial in trials if trial["accepted"]]


def test_greedy_removal_chooses_largest_eligible_gain_not_first_or_unqualified_best():
    target_map = {"A": .5, "B": .75, "C": 1.}
    seen = []
    results = {
        frozenset("ABC"): evaluation(100.),
        frozenset("BC"): evaluation(120.),
        frozenset("AC"): evaluation(150.),
        frozenset("AB"): evaluation(1000., eligible=False),
        frozenset("C"): evaluation(140.),
        frozenset("A"): evaluation(140.),
    }

    def evaluate(candidate):
        seen.append(copy.deepcopy(candidate))
        return results[frozenset(candidate)]

    final_map, trials = refinement.greedy_refine(target_map, evaluate)
    assert final_map == {"A": .5, "C": 1.}
    assert target_map == {"A": .5, "B": .75, "C": 1.}
    assert accepted_removals(trials) == ["B"]
    rejected_best = next(row for row in trials if row["removed_setup"] == "C"
                         and row["combined_net_pnl_rs"] == 1000.)
    assert not rejected_best["eligible"]
    assert not rejected_best["accepted"]
    assert all(seen)


def test_greedy_removal_recomputes_gains_after_each_accepted_deletion():
    target_map = {"A": .5, "B": .6, "C": .7, "D": .8}
    results = {
        frozenset("ABCD"): 100.,
        frozenset("BCD"): 160.,
        frozenset("ACD"): 140.,
        frozenset("ABD"): 130.,
        frozenset("ABC"): 120.,
        frozenset("CD"): 170.,
        frozenset("BD"): 155.,
        frozenset("BC"): 150.,
        frozenset("D"): 165.,
        frozenset("C"): 160.,
    }
    calls = []

    def evaluate(candidate):
        calls.append(copy.deepcopy(candidate))
        return evaluation(results[frozenset(candidate)])

    final_map, trials = refinement.greedy_refine(target_map, evaluate)
    assert final_map == {"C": .7, "D": .8}
    assert accepted_removals(trials) == ["A", "B"]
    accepted = [row for row in trials if row["accepted"]]
    assert accepted[1]["iteration"] > accepted[0]["iteration"]
    assert [row["combined_net_pnl_rs"] for row in accepted] == [160., 170.]
    assert {frozenset(candidate) for candidate in calls} == set(results)
    assert target_map == {"A": .5, "B": .6, "C": .7, "D": .8}


def test_equal_profit_choices_use_lexicographically_first_removal():
    # Deliberately place B before A to avoid an insertion-order tie-break.
    target_map = {"B": .75, "A": .5}
    results = {frozenset("AB"): 100., frozenset("A"): 120., frozenset("B"): 120.}
    final_map, trials = refinement.greedy_refine(
        target_map, lambda candidate: evaluation(results[frozenset(candidate)]))
    assert final_map == {"B": .75}
    assert accepted_removals(trials) == ["A"]


@pytest.mark.parametrize("gain", [-1., 0., 0.0000005])
def test_non_improvements_and_sub_tolerance_gains_are_never_accepted(gain):
    target_map = {"A": .5, "B": .75}
    final_map, trials = refinement.greedy_refine(
        target_map,
        lambda candidate: evaluation(100. if len(candidate) == 2 else 100. + gain))
    assert final_map == target_map
    assert not accepted_removals(trials)


def test_single_remaining_setup_is_not_removed():
    calls = []

    def evaluate(candidate):
        calls.append(dict(candidate))
        return evaluation(100.)

    final_map, trials = refinement.greedy_refine({"A": .5}, evaluate)
    assert final_map == {"A": .5}
    assert calls == [{"A": .5}]
    assert not accepted_removals(trials)


def test_greedy_refinement_requires_an_eligible_nonempty_start():
    with pytest.raises(ValueError):
        refinement.greedy_refine({"A": .5}, lambda candidate: evaluation(100., eligible=False))
    with pytest.raises(ValueError):
        refinement.greedy_refine({}, lambda candidate: evaluation(100.))
