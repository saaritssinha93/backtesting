"""Synthetic checks for no-wait, causal G4 entry and reversal actions."""

import numpy as np
import pandas as pd
import pytest

from research.g4_five_minute_reversal_action import evaluate_order


def _row(side):
    return {"side": side, "trigger": 100.2 if side == "LONG" else 99.8,
            "confirmation_low": 98.0, "confirmation_high": 102.0,
            "confirmation_close": 100.0, "native_target_pct": 1.5}


def _bar(side, kind):
    bars = {
        "LONG": {
            "quiet": (100.05, 100.1, 99.9, 100.05),
            "touch": (100.05, 100.3, 99.9, 100.05),
            "range_reverse": (100.05, 100.1, 97.8, 97.9),
            "touch_reverse": (100.05, 100.3, 97.8, 97.9),
            "midpoint_reverse": (100.05, 100.1, 99.9, 99.95),
            "touch_midpoint": (100.05, 100.3, 99.9, 99.95),
            "exit_open": (99.8, 99.9, 99.7, 99.8),
            "gap_stop": (98.5, 98.6, 98.4, 98.5),
            "touch_adverse": (100.05, 100.3, 99.8, 99.85),
            "adverse": (99.9, 100.0, 99.8, 99.85),
            "stop_target": (100.05, 102.0, 98.0, 100.0),
        },
        "SHORT": {
            "quiet": (99.95, 100.1, 99.9, 99.95),
            "touch": (99.95, 100.1, 99.7, 99.95),
            "range_reverse": (99.95, 102.2, 99.9, 102.1),
            "touch_reverse": (99.95, 102.2, 99.7, 102.1),
            "midpoint_reverse": (99.95, 100.1, 99.9, 100.05),
            "touch_midpoint": (99.95, 100.1, 99.7, 100.05),
            "exit_open": (100.2, 100.3, 100.1, 100.2),
            "gap_stop": (101.5, 101.6, 101.4, 101.5),
            "touch_adverse": (99.95, 100.2, 99.7, 100.15),
            "adverse": (100.1, 100.2, 99.9, 100.15),
            "stop_target": (99.95, 102.0, 98.0, 100.0),
        },
    }
    return bars[side][kind]


def _path(side, specials=None):
    """Full consecutive post-confirmation path through native 15:15 exit."""
    specials = specials or {}
    stamps = pd.date_range("2026-10-15 09:31", "2026-10-15 15:15",
                           freq="min", tz="Asia/Kolkata")
    bars = [_bar(side, specials.get(i, "quiet")) for i in range(len(stamps))]
    return {
        "timestamp_ns": stamps.asi8.copy(),
        **{name: np.asarray([bar[j] for bar in bars], dtype=float)
           for j, name in enumerate(("open", "high", "low", "close"))},
    }


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_original_first_minute_trigger_is_not_delayed(side):
    result = evaluate_order(_row(side), _path(side, {0: "touch"}), "range_break")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["outcome"] is not None
    assert result["monitor_action"] == "NONE"
    assert result["signal_index"] is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_prior_completed_reversal_cancels_a_still_pending_order(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "range_reverse", 1: "touch"}),
        "range_break")
    assert result["found"] is None
    assert result["outcome"] is None
    assert result["monitor_action"] == "CANCEL"
    assert result["signal_index"] == 0


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_same_minute_touch_precedes_that_minute_reversal_close(side):
    result = evaluate_order(
        _row(side), _path(side, {1: "touch_reverse"}), "range_break")
    assert result["found"] is not None and result["found"][0] == 1
    assert result["monitor_action"] != "CANCEL"
    assert result["outcome"] is not None


@pytest.mark.parametrize("side,expected_open", (("LONG", 99.8), ("SHORT", 100.2)))
def test_fill_bar_close_can_trigger_early_exit_at_next_bar_open(side, expected_open):
    result = evaluate_order(
        _row(side), _path(side, {0: "touch_midpoint", 1: "exit_open"}),
        "midpoint")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["monitor_action"] == "EARLY_EXIT"
    assert result["signal_index"] == 0
    assert result["outcome"]["exit_reason"] == "FIVE_MINUTE_REVERSAL_EXIT"
    assert result["outcome"]["exit_path_index"] == 1
    assert result["outcome"]["exit_event"] == "OPEN"
    assert result["outcome"]["exit_price"] == pytest.approx(expected_open)


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_native_stop_gap_at_next_open_beats_requested_early_exit(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "touch_midpoint", 1: "gap_stop"}),
        "midpoint")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["monitor_action"] == "NONE"
    assert result["outcome"]["exit_reason"] == "STOP"
    assert result["outcome"]["exit_event"] == "OPEN"


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_two_consecutive_point_three_percent_adverse_closes_exit_next_open(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "touch_adverse", 1: "adverse",
                                2: "exit_open"}), "adverse_03_2closes")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["monitor_action"] == "EARLY_EXIT"
    assert result["signal_index"] == 1
    assert result["outcome"]["exit_path_index"] == 2
    assert result["outcome"]["exit_reason"] == "FIVE_MINUTE_REVERSAL_EXIT"


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_native_same_bar_stop_wins_over_target_and_reversal_action(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "stop_target"}), "midpoint")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["outcome"]["exit_reason"] == "STOP"
    assert result["outcome"]["same_bar_ambiguous"] is True
    assert result["monitor_action"] == "NONE"
    assert result["signal_index"] is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_reversal_after_fifth_observation_cannot_cancel_later_entry(side):
    result = evaluate_order(
        _row(side), _path(side, {5: "range_reverse", 6: "touch"}),
        "range_break")
    assert result["found"] is not None and result["found"][0] == 6
    assert result["monitor_action"] == "NONE"
    assert result["signal_index"] is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_reversal_after_fifth_observation_cannot_exit_earlier_fill(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "touch", 5: "midpoint_reverse"}),
        "midpoint")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["monitor_action"] == "NONE"
    assert result["signal_index"] is None
    assert result["outcome"]["exit_reason"] == "TIME_EXIT_1515"


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_fifth_completed_close_can_still_request_next_open_exit(side):
    result = evaluate_order(
        _row(side), _path(side, {0: "touch", 4: "midpoint_reverse",
                                5: "exit_open"}), "midpoint")
    assert result["found"] is not None and result["found"][0] == 0
    assert result["monitor_action"] == "EARLY_EXIT"
    assert result["signal_index"] == 4
    assert result["outcome"]["exit_path_index"] == 5
