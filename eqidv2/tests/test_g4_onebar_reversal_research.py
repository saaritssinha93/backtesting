"""Synthetic checks for the research-only, first-completed-bar gates."""

import numpy as np
import pytest

from research.g4_onebar_reversal_research import first_bar_passes


def _path(first_open: float, first_close: float, later_close: float = 100.0):
    return {
        "open": np.array([first_open, later_close], dtype=float),
        "close": np.array([first_close, later_close], dtype=float),
    }


@pytest.mark.parametrize(
    "side,first_close,expected",
    [
        ("LONG", 98.0, True),
        ("LONG", 97.99, False),
        ("SHORT", 102.0, True),
        ("SHORT", 102.01, False),
    ],
)
def test_no_break_uses_opposite_confirmation_extreme(side, first_close, expected):
    row = {"side": side, "confirmation_high": 102.0, "confirmation_low": 98.0}
    assert first_bar_passes(row, _path(100.0, first_close), "no_break") is expected


@pytest.mark.parametrize(
    "side,first_close,expected",
    [
        ("LONG", 100.0, True),
        ("LONG", 99.99, False),
        ("SHORT", 100.0, True),
        ("SHORT", 100.01, False),
    ],
)
def test_midpoint_uses_confirmation_candle_not_trigger(side, first_close, expected):
    row = {"side": side, "confirmation_high": 102.0, "confirmation_low": 98.0}
    assert first_bar_passes(row, _path(100.0, first_close), "midpoint") is expected


@pytest.mark.parametrize(
    "side,first_open,first_close,expected",
    [
        ("LONG", 100.0, 101.0, True),
        ("LONG", 100.0, 100.0, True),
        ("LONG", 100.0, 99.0, False),
        ("SHORT", 100.0, 99.0, True),
        ("SHORT", 100.0, 100.0, True),
        ("SHORT", 100.0, 101.0, False),
    ],
)
def test_directional_bar_uses_first_open_and_close(
    side, first_open, first_close, expected
):
    row = {"side": side, "confirmation_high": 102.0, "confirmation_low": 98.0}
    assert first_bar_passes(
        row, _path(first_open, first_close), "directional_bar"
    ) is expected


@pytest.mark.parametrize("gate", ("none", "no_break", "midpoint", "directional_bar"))
def test_later_bar_cannot_change_completed_first_bar_gate(gate):
    row = {"side": "LONG", "confirmation_high": 102.0, "confirmation_low": 98.0}
    weak_second = first_bar_passes(row, _path(100.0, 99.0, 90.0), gate)
    strong_second = first_bar_passes(row, _path(100.0, 99.0, 110.0), gate)
    assert weak_second is strong_second


def test_invalid_gate_and_nonbaseline_side_rejected():
    row = {"side": "LONG", "confirmation_high": 102.0, "confirmation_low": 98.0}
    with pytest.raises(ValueError, match="Unknown first-bar gate"):
        first_bar_passes(row, _path(100.0, 100.0), "unsupported")
    with pytest.raises(ValueError, match="valid G4 side"):
        first_bar_passes({**row, "side": "FLAT"}, _path(100.0, 100.0), "midpoint")
    with pytest.raises(ValueError, match="two-bar path"):
        first_bar_passes(row, {"open": np.array([100.0]), "close": np.array([100.0])}, "midpoint")
