"""Two-leg execution, chronology, and unchanged-remainder cashflow checks."""
from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_partial_exits as partials


def fixture(side="LONG"):
    stamps = pd.date_range("2026-10-15 10:01", periods=10, freq="min", tz="Asia/Kolkata")
    partial_price = 101. if side == "LONG" else 99.
    final_price = 98.8 if side == "LONG" else 101.2
    row = dict(filled=True, side=side, entry_price=100., entry_ts=stamps[0], entry_path_index=0,
               exit_price=final_price, exit_ts=stamps[-1], exit_execution_ts=stamps[-1],
               exit_reason="STOP", initial_stop_pct=1.2, native_target_pct=2.)
    event = dict(family="target_giveback", decision_at_ist=stamps[3].isoformat(),
                 next_bar_open_at_ist=stamps[3].isoformat(), modeled_execution_at_ist=stamps[4].isoformat(),
                 next_bar_open_price=partial_price, execution_path_index=4)
    opens = np.full(len(stamps), 100.)
    opens[4] = partial_price
    path = dict(timestamp_ns=stamps.asi8, open=opens)
    return row, event, path


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("fraction", [.25, .50, .75])
def test_actual_two_leg_cashflows_match_weighted_return_and_single_proportional_cost(side, fraction):
    row, event, path = fixture(side)
    result = partials.partial_outcome(row, event, fraction, path=path)
    sign = 1 if side == "LONG" else -1
    partial_net = sign * (event["next_bar_open_price"] / 100. - 1) * 500000. - 250.
    native_net = sign * (row["exit_price"] / 100. - 1) * 500000. - 250.
    assert result["net_profit_rs"] == pytest.approx(fraction * partial_net + (1 - fraction) * native_net)
    assert result["cashflow_sum_rs"] == pytest.approx(result["net_profit_rs"])
    assert result["cost_rs"] == 250.
    assert sum(leg["entry_cost_rs"] for leg in result["legs"]) == 125.
    assert sum(leg["exit_cost_rs"] for leg in result["legs"]) == 125.
    assert sum(leg["quantity"] for leg in result["legs"]) == 5000.
    assert result["legs"][0]["quantity"] == 5000. * fraction
    assert result["legs"][1]["quantity"] == 5000. * (1 - fraction)
    assert result["capital_reserved_until"] == row["exit_ts"].isoformat()
    assert result["capital_reserved_rs"] == 100000.
    assert result["original_remainder_exit_unchanged"]
    assert result["path_price_verified"]
    for leg in result["legs"]:
        assert leg["entry_gross_cashflow_rs"] == -sign * leg["entry_notional_rs"]
        assert leg["exit_gross_cashflow_rs"] == sign * leg["exit_notional_rs"]
        assert leg["entry_cashflow_rs"] + leg["exit_cashflow_rs"] == pytest.approx(leg["net_profit_rs"])


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_gap_at_partial_open_is_observed_price_and_can_realize_loss(side):
    row, event, path = fixture(side)
    event["next_bar_open_price"] = 99.7 if side == "LONG" else 100.3
    path["open"][4] = event["next_bar_open_price"]
    result = partials.partial_outcome(row, event, .5, path=path)
    assert result["legs"][0]["exit_price"] == event["next_bar_open_price"]
    assert result["legs"][0]["net_profit_rs"] == pytest.approx(-875.)
    assert result["legs"][1]["exit_price"] == row["exit_price"]


def test_future_native_exit_changes_only_remaining_leg_not_partial_execution():
    row, event, path = fixture()
    before = partials.partial_outcome(row, event, .25, path=path)
    row["exit_price"] = 102.
    row["exit_ts"] += pd.Timedelta(minutes=10)
    row["exit_execution_ts"] = row["exit_ts"]
    path["open"][5:] = 1e6
    after = partials.partial_outcome(row, event, .25, path=path)
    assert before["legs"][0] == after["legs"][0]
    assert after["legs"][1]["exit_price"] == 102.
    assert after["final_exit_ts"] == row["exit_ts"].isoformat()
    assert after["capital_reserved_until"] == row["exit_ts"].isoformat()
    assert after["net_profit_rs"] - before["net_profit_rs"] == pytest.approx(.75 * 5000. * 3.2)


@pytest.mark.parametrize("fraction", [0., 1., -1., 1.1, np.nan, np.inf, True, None])
def test_invalid_fraction_cannot_create_or_remove_original_quantity(fraction):
    row, event, path = fixture()
    with pytest.raises(ValueError):
        partials.partial_outcome(row, event, fraction, path=path)


@pytest.mark.parametrize("corruption", ["entry_close", "same_native_open", "after_native", "native_actual_earlier",
                                         "decision", "reserved_end", "naive", "subminute", "path_price",
                                         "path_index", "entry_index", "failure_family", "unfilled", "bad_price"])
def test_invalid_or_noncausal_partial_orders_rejected(corruption):
    row, event, path = fixture()
    if corruption == "entry_close":
        event["decision_at_ist"] = row["entry_ts"]
        event["next_bar_open_at_ist"] = row["entry_ts"]
        event["modeled_execution_at_ist"] = row["entry_ts"] + pd.Timedelta(minutes=1)
    elif corruption == "same_native_open":
        row["exit_execution_ts"] = event["next_bar_open_at_ist"]
    elif corruption == "after_native":
        row["exit_ts"] = row["exit_execution_ts"] = pd.Timestamp(event["next_bar_open_at_ist"]) - pd.Timedelta(minutes=1)
    elif corruption == "native_actual_earlier":
        row["exit_execution_ts"] = row["entry_ts"]
    elif corruption == "decision":
        event["decision_at_ist"] = row["entry_ts"]
    elif corruption == "reserved_end":
        event["modeled_execution_at_ist"] = event["next_bar_open_at_ist"]
    elif corruption == "naive":
        event["decision_at_ist"] = "2026-10-15 10:04"
    elif corruption == "subminute":
        for key in ("decision_at_ist", "next_bar_open_at_ist", "modeled_execution_at_ist"):
            event[key] = pd.Timestamp(event[key]) + pd.Timedelta(seconds=1)
    elif corruption == "path_price":
        event["next_bar_open_price"] += 1
    elif corruption == "path_index":
        event["execution_path_index"] = len(path["open"])
    elif corruption == "entry_index":
        row["entry_path_index"] = 3
    elif corruption == "failure_family":
        event["family"] = "delayed_failure"
    elif corruption == "unfilled":
        row["filled"] = False
    elif corruption == "bad_price":
        row["exit_price"] = np.nan
    with pytest.raises(ValueError):
        partials.partial_outcome(row, event, .5, path=path)


def test_optional_path_reports_unverified_price_without_weakening_timestamp_checks():
    row, event, _ = fixture()
    result = partials.partial_outcome(row, event, .5)
    assert not result["path_price_verified"]
    event["next_bar_open_at_ist"] = row["entry_ts"]
    with pytest.raises(ValueError):
        partials.partial_outcome(row, event, .5)


def test_inputs_are_not_mutated_and_native_remainder_is_not_tightened():
    row, event, path = fixture()
    initial_row, initial_event, initial_path = copy.deepcopy(row), copy.deepcopy(event), copy.deepcopy(path)
    result = partials.partial_outcome(row, event, .75, path=path)
    assert row == initial_row and event == initial_event
    np.testing.assert_array_equal(path["open"], initial_path["open"])
    assert result["legs"][1]["exit_execution_ts"] == row["exit_execution_ts"].isoformat()
    assert result["legs"][1]["exit_price"] == 98.8
