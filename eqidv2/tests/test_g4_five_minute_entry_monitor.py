"""Synthetic causal checks for the research-only five-minute G4 monitor."""

import numpy as np
import pytest

from research.g4_five_minute_entry_monitor import monitored_entry


def _row(side):
    return {"side": side, "trigger": 103.0 if side == "LONG" else 97.0,
            "confirmation_low": 98.0, "confirmation_high": 102.0,
            "confirmation_close": 100.0}


def _bar(side, kind):
    """Valid OHLC with a trigger outside the confirmation candle's range."""
    bars = {
        "LONG": {
            "quiet": (101.0, 102.0, 100.5, 101.0),
            "touch": (101.0, 103.5, 100.5, 101.0),
            "range_reverse": (101.0, 102.0, 97.0, 97.5),
            "midpoint_reverse": (101.0, 102.0, 99.0, 99.5),
            "adverse": (101.0, 102.0, 99.0, 99.5),
            "touch_reverse": (101.0, 103.5, 97.0, 97.5),
            "edge_tie": (101.0, 102.0, 98.0, 98.0),
            "gap": (104.0, 104.5, 103.5, 104.0),
        },
        "SHORT": {
            "quiet": (99.0, 99.5, 98.0, 99.0),
            "touch": (99.0, 99.5, 96.5, 99.0),
            "range_reverse": (99.0, 103.0, 98.0, 102.5),
            "midpoint_reverse": (99.0, 100.5, 98.0, 100.5),
            "adverse": (99.0, 100.5, 98.0, 100.5),
            "touch_reverse": (99.0, 103.0, 96.5, 102.5),
            "edge_tie": (99.0, 102.0, 98.0, 102.0),
            "gap": (96.0, 96.5, 95.5, 96.0),
        },
    }
    return bars[side][kind]


def _path(side, specials=None, bars=11):
    specials = specials or {}
    values = [_bar(side, specials.get(i, "quiet")) for i in range(bars)]
    return {
        "timestamp_ns": np.arange(bars, dtype=np.int64) * 60_000_000_000,
        **{name: np.asarray([bar[j] for bar in values], dtype=float)
           for j, name in enumerate(("open", "high", "low", "close"))},
    }


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_first_bar_trigger_cannot_fill_before_first_completed_close(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {0: "touch"}), "none")
    assert found is None
    assert cancel_index is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
@pytest.mark.parametrize("mode", ("none", "range_break", "midpoint", "two_adverse_closes"))
def test_clean_second_bar_trigger_fills_without_waiting_five_minutes(side, mode):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {1: "touch"}), mode)
    assert cancel_index is None
    assert found is not None
    assert found[0] == 1
    assert found[1] == pytest.approx(_row(side)["trigger"])


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_first_bar_touch_then_reversal_close_cancels_not_retroactively_fills(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {0: "touch_reverse", 1: "touch"}),
        "range_break")
    assert found is None
    assert cancel_index == 0


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
@pytest.mark.parametrize("mode,reversal", (("range_break", "range_reverse"),
                                           ("midpoint", "midpoint_reverse")))
def test_completed_reversal_cancels_before_a_later_trigger(side, mode, reversal):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {2: reversal, 3: "touch"}), mode)
    assert found is None
    assert cancel_index == 2


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_trigger_touch_precedes_same_minute_reversal_close(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {1: "touch_reverse"}), "range_break")
    assert found is not None and found[0] == 1
    assert cancel_index is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_fifth_bar_reversal_still_cancels_before_sixth_bar_hit(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {4: "range_reverse", 5: "touch"}),
        "range_break")
    assert found is None
    assert cancel_index == 4


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_after_fifth_close_monitor_stops_but_order_remains_active(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {5: "range_reverse", 6: "touch"}),
        "range_break")
    assert found is not None and found[0] == 6
    assert cancel_index is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_original_ten_minute_deadline_is_inclusive_and_not_extended(side):
    row = _row(side)
    minute_ten, cancel = monitored_entry(
        row, _path(side, {9: "touch"}), "none")
    minute_eleven, later_cancel = monitored_entry(
        row, _path(side, {10: "touch"}), "none")
    assert minute_ten is not None and minute_ten[0] == 9
    assert minute_eleven is None
    assert cancel is None and later_cancel is None


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_two_adverse_closes_are_consecutive_and_relative_to_confirmation(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {0: "adverse", 2: "adverse", 3: "adverse",
                                4: "touch"}), "two_adverse_closes")
    assert found is None
    assert cancel_index == 3


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_opposite_confirmation_edge_equality_is_not_a_range_break(side):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {0: "edge_tie", 1: "touch"}),
        "range_break")
    assert found is not None and found[0] == 1
    assert cancel_index is None


@pytest.mark.parametrize("side,expected", (("LONG", 104.0), ("SHORT", 96.0)))
def test_second_bar_gap_through_uses_worse_open_price(side, expected):
    found, cancel_index = monitored_entry(
        _row(side), _path(side, {1: "gap"}), "none")
    assert found is not None and found[0] == 1
    assert found[1] == pytest.approx(expected)
    assert found[3] is True
    assert cancel_index is None
