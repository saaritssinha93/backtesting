"""Chronology, selection and uncertainty checks for retrospective monitor research."""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_robustness as robustness


def _days(count=50):
    return [date(2026, 1, 1) + timedelta(days=index) for index in range(count)]


def _strings(values):
    return [str(value) for value in values]


def _row(candidate_id, *, stress=1000.0, exits=8, **overrides):
    row = dict(
        candidate_id=candidate_id,
        net_pnl_delta_vs_baseline_rs=stress + 500.0,
        stress_10bps_net_pnl_delta_vs_baseline_rs=stress,
        monitor_exit_count=exits,
        wins=15,
        executed_trades=20,
        realized_loss_reduction_rs=1000.0,
        max_drawdown_rs=2000.0,
        baseline_max_drawdown_rs=2000.0,
        losing_days=2,
        baseline_losing_days=2,
        newly_rejected=0,
    )
    row.update(overrides)
    return row


def test_folds_cover_last_thirty_days_once_without_future_in_selection():
    days = _days()
    folds = robustness.walk_forward_folds(days)

    assert len(folds) == 6
    test_days = []
    fold_ids = set()
    for index, fold in enumerate(folds):
        fold_ids.add(fold["fold_id"])
        first_test = 20 + 5 * index
        assert _strings(fold["train_days"]) == _strings(days[: first_test - 5])
        assert _strings(fold["validation_days"]) == _strings(days[first_test - 5 : first_test])
        assert _strings(fold["test_days"]) == _strings(days[first_test : first_test + 5])
        selection_days = _strings(fold["train_days"] + fold["validation_days"])
        future_days = _strings(fold["test_days"])
        assert max(selection_days) < min(future_days)
        assert set(selection_days).isdisjoint(future_days)
        test_days.extend(future_days)
    assert len(fold_ids) == 6
    assert test_days == _strings(days[20:])


def test_existing_fold_boundaries_do_not_change_when_later_days_are_appended():
    short = robustness.walk_forward_folds(_days(35))
    extended = robustness.walk_forward_folds(_days(50))
    assert short == extended[: len(short)]


def test_selection_ranks_training_only_while_validation_is_a_gate():
    training = [_row("a", stress=1200), _row("b", stress=1000)]
    validation = [_row("a", stress=1, exits=2), _row("b", stress=1_000_000, exits=2)]
    chosen = robustness.select_candidate(training, validation)

    assert chosen["selected_id"] == "a"
    assert set(chosen["eligible_ids"]) == {"a", "b"}
    # A huge increase in the weaker training candidate's validation return
    # must not silently change the selection objective.
    validation[1]["stress_10bps_net_pnl_delta_vs_baseline_rs"] = 1e12
    rerun = robustness.select_candidate(list(reversed(training)), list(reversed(validation)))
    assert rerun["selected_id"] == "a"


def test_selection_tie_break_is_stable_and_baseline_is_the_real_fallback():
    training = [_row("z"), _row("a")]
    validation = [_row("z", exits=2), _row("a", exits=2)]
    assert robustness.select_candidate(training, validation)["selected_id"] == "a"

    failed = [_row("z", stress=-1, exits=2), _row("a", stress=0, exits=2)]
    rejected = robustness.select_candidate(training, failed)
    assert rejected["selected_id"] == "baseline"
    assert rejected["eligible_ids"] == []
    assert all(rejected["failures"].get(candidate) for candidate in ("a", "z"))
    assert robustness.select_candidate([], [])["selected_id"] == "baseline"


@pytest.mark.parametrize(
    "period,changes",
    [
        ("train", {"monitor_exit_count": 4}),
        ("validation", {"monitor_exit_count": 1}),
        ("validation", {"net_pnl_delta_vs_baseline_rs": 0}),
        ("validation", {"stress_10bps_net_pnl_delta_vs_baseline_rs": 0}),
        ("validation", {"wins": 13}),
        ("validation", {"realized_loss_reduction_rs": 0}),
        ("validation", {"max_drawdown_rs": 2001}),
        ("validation", {"losing_days": 3}),
        ("validation", {"newly_rejected": 1}),
    ],
)
def test_each_selection_gate_can_independently_reject_a_candidate(period, changes):
    training = _row("candidate", exits=5)
    validation = _row("candidate", exits=2)
    (training if period == "train" else validation).update(changes)

    chosen = robustness.select_candidate([training], [validation])
    assert chosen["selected_id"] == "baseline"
    assert chosen["eligible_ids"] == []
    assert chosen["failures"]["candidate"]


def test_missing_validation_cannot_qualify_a_training_winner():
    chosen = robustness.select_candidate([_row("unvalidated", stress=1e9)], [])
    assert chosen["selected_id"] == "baseline"
    assert chosen["eligible_ids"] == []


def test_equal_trade_counts_cancel_symmetric_costs_but_monitor_cost_remains():
    for extra_bps in (0, 5, 10, 20):
        delta = robustness.stress_delta(
            candidate_net=101_500.0,
            baseline_net=100_000.0,
            candidate_trades=10,
            baseline_trades=10,
            monitor_exits=2,
            extra_bps=extra_bps,
        )
        assert delta == pytest.approx(500.0)


def test_incremental_execution_and_monitor_exit_are_both_charged():
    delta = robustness.stress_delta(
        candidate_net=101_000.0,
        baseline_net=100_000.0,
        candidate_trades=11,
        baseline_trades=10,
        monitor_exits=2,
        extra_bps=10,
    )
    # Rs 1,000 gross delta, Rs 500 for the extra execution, and Rs 1,000
    # for two monitor exits, all at the pinned Rs 500,000 notional.
    assert delta == pytest.approx(-500.0)
    assert robustness.stress_delta(
        candidate_net=100.0,
        baseline_net=0.0,
        candidate_trades=1,
        baseline_trades=0,
        monitor_exits=1,
        extra_bps=5,
        monitor_extra_bps=10,
        notional_rs=100_000.0,
    ) == pytest.approx(-50.0)


def test_bootstrap_is_seeded_and_reports_original_sample_mean():
    values = np.array([-200.0, 0.0, 0.0, 1200.0, 0.0, -100.0, 0.0, 0.0, 0.0, 0.0])
    first = robustness.moving_block_bootstrap(values, block_size=3, samples=1000, seed=7)
    second = robustness.moving_block_bootstrap(values, block_size=3, samples=1000, seed=7)

    assert first == second
    assert first["n_days"] == len(values)
    assert first["mean"] == pytest.approx(values.mean())
    assert first["ci_lower"] <= first["ci_upper"]
    assert 0 <= first["positive_probability"] <= 1


def test_block_bootstrap_retains_serial_dependence():
    grouped = np.r_[np.ones(20), -np.ones(20)]
    alternating = np.tile([1.0, -1.0], 20)
    grouped_result = robustness.moving_block_bootstrap(grouped, block_size=5, samples=2000, seed=7)
    alternating_result = robustness.moving_block_bootstrap(alternating, block_size=5, samples=2000, seed=7)
    grouped_width = grouped_result["ci_upper"] - grouped_result["ci_lower"]
    alternating_width = alternating_result["ci_upper"] - alternating_result["ci_lower"]

    assert grouped_result["mean"] == alternating_result["mean"] == 0.0
    assert grouped_width > 2 * alternating_width


@pytest.mark.parametrize("values", [[0.0] * 10])
def test_empty_or_zero_deltas_cannot_manufacture_positive_evidence(values):
    result = robustness.moving_block_bootstrap(values)
    assert result["n_days"] == len(values)
    assert result["mean"] == result["ci_lower"] == result["ci_upper"] == 0.0
    assert result["positive_probability"] == 0.0


def test_empty_data_cannot_be_presented_as_zero_uncertainty():
    with pytest.raises(ValueError):
        robustness.moving_block_bootstrap([])


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_deltas_are_rejected_instead_of_silently_dropped(value):
    with pytest.raises(ValueError):
        robustness.moving_block_bootstrap([0.0, value, 1.0])


def test_delayed_exit_keeps_original_decision_and_uses_observed_later_open():
    event = dict(sid=1, decision_at_ist="2026-08-03T10:00:00+05:30",
        next_bar_open_at_ist="2026-08-03T10:00:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:01:00+05:30",
        original_exit_at_ist="2026-08-03T10:05:00+05:30",
        execution_path_index=10, next_bar_open_price=100.)
    features = pd.DataFrame([dict(sid=1, next_bar_open_at_ist="2026-08-03T10:01:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:02:00+05:30",
        execution_path_index=11, next_bar_open_price=99.3)])
    actual, intervening = robustness.delayed_events([event], features, 1)
    assert intervening == 0
    assert actual[0]["decision_at_ist"] == event["decision_at_ist"]
    assert actual[0]["next_bar_open_price"] == 99.3
    assert actual[0]["execution_path_index"] == 11
    assert event["next_bar_open_price"] == 100.


def test_native_exit_during_latency_wins_without_needing_a_later_price():
    event = dict(sid=1, next_bar_open_at_ist="2026-08-03T10:00:00+05:30",
        original_exit_at_ist="2026-08-03T10:01:00+05:30")
    events, intervening = robustness.delayed_events([event], pd.DataFrame(), 1)
    assert events == [] and intervening == 1


def test_missing_delayed_open_cannot_silently_cancel_an_unprofitable_exit():
    event = dict(sid=1, next_bar_open_at_ist="2026-08-03T10:00:00+05:30",
        original_exit_at_ist="2026-08-03T10:05:00+05:30")
    with pytest.raises(ValueError, match="No verified delayed"):
        robustness.delayed_events([event], pd.DataFrame(), 1)
