"""Economic and causal invariants for the isolated OI exit experiment."""
from __future__ import annotations

from datetime import timedelta
import json

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_oi_research as oi_research
from research import g3_trade_monitor_parameter_search as search
from tests.test_g3_trade_monitor_parameter_search import DAY, _inputs, _selection_rows


def _price_events(baseline, paths):
    """Use the established causal event contract for allocator-level tests."""
    cache = search.DecisionCache(search.build_feature_cache(baseline, paths))
    return cache.events(search._current_candidate("trend_failure"))


def test_paired_allocation_keeps_control_entries_when_early_exit_frees_a_slot():
    _, paths, baseline = _inputs(count=11)
    book = search.FastBook(baseline)
    events = _price_events(baseline, paths)
    before = {key: value.copy() for key, value in book.baseline.items()}

    paired = oi_research.apply_events(book, events, allocation="paired")
    reallocated = oi_research.apply_events(book, events, allocation="reallocated")

    assert len(events) == 1
    np.testing.assert_array_equal(paired["accepted"], before["accepted"])
    assert paired["accepted"].sum() == 10
    assert not paired["accepted"][10]
    assert reallocated["accepted"].sum() == 11
    assert reallocated["accepted"][10]
    assert paired["modified"][0]
    assert paired["net"][0] != pytest.approx(before["net"][0])
    for key, original in before.items():
        np.testing.assert_array_equal(book.baseline[key], original)


def test_no_events_preserve_baseline_economics_and_default_allocation_is_paired():
    _, paths, baseline = _inputs(count=11)
    book = search.FastBook(baseline)
    unchanged = oi_research.apply_events(book, [])
    for key, expected in book.baseline.items():
        np.testing.assert_array_equal(unchanged[key], expected)

    changed = oi_research.apply_events(book, _price_events(baseline, paths))
    assert not changed["accepted"][10]


def test_unknown_allocation_mode_is_rejected():
    _, _, baseline = _inputs()
    with pytest.raises(ValueError):
        oi_research.apply_events(search.FastBook(baseline), [], allocation="typo")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("allocation", ["paired", "reallocated"])
def test_saved_ledger_matches_independent_price_path_replay(side, allocation):
    _, paths, baseline = _inputs(count=11, side=side)
    book = search.FastBook(baseline)
    candidate = search._current_candidate("trend_failure")
    events = _price_events(baseline, paths)
    outcome = oi_research.apply_events(book, events, allocation=allocation)

    ledger = oi_research.canonical_ledger(baseline, outcome, events, "oi_test", allocation)
    independent = search._canonical_ledger(baseline, paths, candidate, events, "oi_test")

    search._assert_fast_parity(book, outcome, ledger, "oi_test")
    for column in ("entry_ts", "exit_ts", "exit_execution_ts", "exit_price",
                   "gross_return_pct", "net_return_pct", "cost_pct", "holding_minutes",
                   "exit_event", "target_hit", "first_target_hit", "runner_target_hit",
                   "stop_hit", "same_bar_ambiguous", "exit_gap_through", "exit_gap_bps",
                   "active_stop_pct_at_exit", "unleveraged_pre_cost_profit_rupees",
                   "unleveraged_cost_rupees", "unleveraged_net_profit_rupees",
                   "pre_cost_profit_rupees", "cost_rupees", "net_profit_rupees"):
        pd.testing.assert_series_equal(ledger[column], independent[column], check_dtype=False)
    assert ledger.execution_authority.eq(False).all()
    assert ledger.scope.eq("BACKTEST_ONLY").all()
    if allocation == "paired":
        pd.testing.assert_series_equal(ledger.portfolio_executed, baseline.portfolio_executed)
        assert ledger.loc[ledger.sid.eq(10), "portfolio_net_profit_rupees"].item() == 0.0
    else:
        search.history._same_allocation(ledger, independent)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("allocation", ["paired", "reallocated"])
def test_csv_roundtrip_preserves_native_and_candidate_ledger_economics(tmp_path, side, allocation):
    _, paths, baseline = _inputs(count=11, side=side)
    source = tmp_path / "baseline.csv"
    baseline.to_csv(source, index=False)
    restored = search.read_frozen_csv(source)
    assert isinstance(restored.loc[0, "exit_ts"], str)
    assert " " in restored.loc[0, "exit_ts"]
    candidate = search._current_candidate("trend_failure")
    events = _price_events(restored, paths)
    assert "T" in events[0]["modeled_execution_at_ist"]
    book = search.FastBook(restored)

    for current_events in ([], events):
        outcome = oi_research.apply_events(book, current_events, allocation=allocation)
        actual = oi_research.canonical_ledger(restored, outcome, current_events, "csv_test", allocation)
        expected = search._canonical_ledger(baseline, paths, candidate, current_events, "csv_test")

        search._assert_fast_parity(book, outcome, actual, "csv_test")
        for column in ("entry_ts", "exit_ts", "exit_execution_ts", "confirmation_ts"):
            pd.testing.assert_series_equal(
                pd.to_datetime(actual[column], utc=True, format="mixed"),
                pd.to_datetime(expected[column], utc=True, format="mixed"),
                check_dtype=False)
        for column in ("exit_price", "gross_return_pct", "net_return_pct", "cost_pct",
                       "holding_minutes", "unleveraged_pre_cost_profit_rupees",
                       "unleveraged_cost_rupees", "unleveraged_net_profit_rupees",
                       "pre_cost_profit_rupees", "cost_rupees", "net_profit_rupees"):
            np.testing.assert_allclose(actual[column], expected[column], rtol=0, atol=1e-6)
        if allocation == "reallocated":
            search.history._same_allocation(actual, expected)
        else:
            np.testing.assert_array_equal(actual.portfolio_executed, restored.portfolio_executed)
            for column in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
                np.testing.assert_allclose(actual[column],
                    np.where(restored.portfolio_executed, expected[column], 0.0), rtol=0, atol=1e-6)
        if not current_events:
            search.history._same_native(actual, baseline)
            search.history._same_allocation(actual, baseline)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("future_exit", ["STOP", "TARGET"])
def test_completed_decision_exit_is_not_cancelled_by_later_intrabar_outcome(side, future_exit):
    orders, paths, baseline = _inputs(side=side)
    original_events = _price_events(baseline, paths)
    original = oi_research.apply_events(search.FastBook(baseline), original_events)

    # The next bar's open is unchanged; a later high/low in that same bar
    # reaches a native target or stop after the advisory fill was available.
    upper = (future_exit == "TARGET") == (side == "LONG")
    paths[0]["high" if upper else "low"][7] = 102.0 if upper else 98.0
    revised_baseline = search.history._simulate(orders, paths, [DAY], search.history.contract)
    assert revised_baseline.iloc[0].exit_reason == future_exit
    revised_events = _price_events(revised_baseline, paths)
    revised = oi_research.apply_events(search.FastBook(revised_baseline), revised_events)

    assert len(original_events) == len(revised_events) == 1
    assert original_events[0]["next_bar_open_price"] == revised_events[0]["next_bar_open_price"]
    for key in ("exit", "gross", "net", "cost", "modified", "accepted"):
        np.testing.assert_array_equal(revised[key], original[key])


@pytest.mark.parametrize("reason", ["STOP", "TIGHTENED_STOP"])
def test_relabelled_stop_is_still_a_losing_trade_and_can_worsen_economics(reason):
    _, _, baseline = _inputs()
    baseline.loc[0, "exit_reason"] = reason
    book = search.FastBook(baseline)
    outcome = {key: value.copy() for key, value in book.baseline.items()}
    # Isolate reporting from event discovery: an early exit with a larger
    # realized loss must never become a claimed success merely by relabeling.
    outcome["modified"][0] = True
    outcome["net"][0] -= 100.0
    outcome["gross"][0] -= 100.0

    metrics = oi_research.period_metrics(book, outcome, [DAY])

    assert metrics["baseline_native_stop_count"] == 1
    assert metrics["remaining_native_stop_count"] == 0
    assert metrics["early_exit_losing_trades"] == 1
    assert metrics["total_losing_trades"] == metrics["losses"] == 1
    assert metrics["stopped_trades_improved"] == 0
    assert metrics["stopped_trades_worsened"] == 1
    assert metrics["net_pnl_delta_vs_baseline_rs"] == pytest.approx(-100.0)
    assert metrics["realized_loss_reduction_rs"] == pytest.approx(-100.0)
    assert metrics["stress_10bps_net_pnl_delta_vs_baseline_rs"] == pytest.approx(-600.0)


def test_loss_and_stop_metrics_exclude_unaccepted_orders():
    _, paths, baseline = _inputs(count=11)
    baseline["exit_reason"] = "STOP"
    book = search.FastBook(baseline)
    outcome = oi_research.apply_events(book, _price_events(baseline, paths))

    metrics = oi_research.period_metrics(book, outcome, [DAY])

    assert metrics["executed_trades"] == 10
    assert metrics["baseline_native_stop_count"] == 10
    assert metrics["remaining_native_stop_count"] == 9
    assert metrics["early_exit_losing_trades"] == 1
    assert metrics["total_losing_trades"] == 10
    assert metrics["newly_admitted"] == metrics["newly_rejected"] == 0


def test_period_metrics_cannot_include_future_days_in_losses_or_baseline_guards():
    _, _, baseline = _inputs(count=3)
    future_day = DAY + timedelta(days=1)
    baseline.loc[2, "day"] = str(future_day)
    for column in ("confirmation_ts", "entry_ts", "exit_ts", "exit_bar_end_ts", "exit_execution_ts"):
        baseline.loc[2, column] = pd.Timestamp(baseline.loc[2, column]) + pd.Timedelta(days=1)
    baseline["exit_reason"] = ["STOP", "TIGHTENED_STOP", "STOP"]
    book = search.FastBook(baseline)
    outcome = {key: value.copy() for key, value in book.baseline.items()}
    outcome["modified"][:] = True
    outcome["net"] += np.array([-100.0, 50.0, 1e9])
    outcome["gross"] += np.array([-100.0, 50.0, 1e9])

    metrics = oi_research.period_metrics(book, outcome, [DAY])
    expected_baseline = book.metrics(book.baseline, [DAY])

    assert metrics["sessions"] == 1
    assert metrics["executed_trades"] == 2
    assert metrics["baseline_native_stop_count"] == 2
    assert metrics["remaining_native_stop_count"] == 0
    assert metrics["early_exit_losing_trades"] == 2
    assert metrics["total_losing_trades"] == 2
    assert metrics["stopped_trades_improved"] == 1
    assert metrics["stopped_trades_worsened"] == 1
    assert metrics["net_pnl_delta_vs_baseline_rs"] == pytest.approx(-50.0)
    assert metrics["realized_loss_reduction_rs"] == pytest.approx(-50.0)
    assert metrics["baseline_max_drawdown_rs"] == expected_baseline["max_drawdown_rs"]
    assert metrics["baseline_losing_days"] == expected_baseline["losing_days"] == 1


def _selection_fixture():
    candidates = [item for item in oi_research.rules.candidate_registry()
                  if item["family"].startswith("oi_")][:2]
    rows = _selection_rows(candidates)
    rows["stress_delta_vs_matched_futures_control_rs"] = 100.0
    return candidates, rows


def test_development_lock_ignores_final_period_even_for_matched_control_gate():
    candidates, rows = _selection_fixture()
    first = oi_research._lock(rows.to_dict("records"), candidates)
    assert first["selected_id"] == candidates[0]["candidate_id"]

    final = rows.period.eq("evaluation15")
    rows.loc[final, "stress_delta_vs_matched_futures_control_rs"] = -1e12
    rows.loc[final, "net_pnl_delta_vs_baseline_rs"] = -1e12
    rows.loc[final, "stress_10bps_net_pnl_delta_vs_baseline_rs"] = -1e12
    second = oi_research._lock(rows.sample(frac=1, random_state=17).to_dict("records"), candidates[::-1])

    for key in ("selected_id", "locked_candidate_id", "qualified_candidate_ids",
                "development_qualified", "qualification_failures"):
        assert second[key] == first[key]


def test_beating_baseline_does_not_qualify_an_oi_rule_that_loses_to_matched_control():
    candidates, rows = _selection_fixture()
    rows = rows.loc[rows.period.ne("evaluation15")].copy()
    stronger_baseline_gain = rows.candidate_id.eq(candidates[0]["candidate_id"])
    rows.loc[stronger_baseline_gain & rows.period.eq("validation10"),
             "stress_delta_vs_matched_futures_control_rs"] = 0.0

    lock = oi_research._lock(rows.to_dict("records"), candidates)

    assert lock["selected_id"] == lock["locked_candidate_id"] == candidates[1]["candidate_id"]
    assert lock["qualified_candidate_ids"] == [candidates[1]["candidate_id"]]
    assert any("matched_futures_control" in reason
               for reason in lock["qualification_failures"][candidates[0]["candidate_id"]])

    rows["stress_delta_vs_matched_futures_control_rs"] = -1.0
    rejected = oi_research._lock(rows.to_dict("records"), candidates)
    assert rejected["selected_id"] == "baseline"
    assert rejected["development_qualified"] is False
    assert rejected["qualified_candidate_ids"] == []
    assert rejected["configuration_promoted"] is False


def test_research_wrapper_restores_operational_modules_after_exception(monkeypatch, tmp_path):
    from research.archived_g3_v7 import contract as archived_production
    from research.archived_g3_v9 import contract as archived_contract

    original_production = search.history.production
    original_contract = search.history.contract
    calls = []

    def fail_inside_pinned_run(source, output_root, data_root):
        calls.append((source, output_root, data_root))
        assert search.history.production is archived_production
        assert search.history.contract is archived_contract
        raise RuntimeError("Injected research failure")

    monkeypatch.setattr(oi_research, "_run_pinned", fail_inside_pinned_run)
    source, output_root, data_root = (tmp_path / name for name in ("source", "output", "data"))
    with pytest.raises(RuntimeError, match="Injected research failure"):
        oi_research.run(source, output_root, data_root)

    assert calls == [(source, output_root, data_root)]
    assert search.history.production is original_production
    assert search.history.contract is original_contract
    assert not output_root.exists()


@pytest.mark.parametrize("path,value", [
    (("cash_guard", "minimum_completed_post_entry_bars"), 3),
    (("oi_rules", "expansion_thresholds_pct"), [.25, 1.]),
    (("oi_rules", "exact_same_contract_lookback_minutes"), 5),
    (("base_round_trip_cost_bps",), 0),
    (("additional_advisory_exit_cost_bps",), 0),
    (("lag_sensitivity_minutes",), [0, 1, 2]),
    (("development_lock", "evaluation_sessions"), 14),
    (("execution_authority",), True),
    (("configuration_promoted",), True),
])
def test_frozen_protocol_rejects_threshold_cost_split_and_authority_drift(path, value):
    protocol = json.loads(oi_research.PROTOCOL.read_text(encoding="utf-8"))
    oi_research.validate_protocol(protocol)
    target = protocol
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(ValueError, match="Frozen OI protocol differs"):
        oi_research.validate_protocol(protocol)

