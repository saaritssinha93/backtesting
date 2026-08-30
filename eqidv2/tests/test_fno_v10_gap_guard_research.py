from __future__ import annotations

import math

import pytest

import fno_v10_gap_guard_research as research


@pytest.mark.parametrize(
    ("side", "opening", "trigger", "expected"),
    [
        ("LONG", 101.0, 100.0, 100.0),
        ("SHORT", 99.0, 100.0, 100.0),
        ("LONG", 100.0, 100.0, 0.0),
        ("SHORT", 100.0, 100.0, 0.0),
    ],
)
def test_adverse_gap_bps_is_side_aware(side, opening, trigger, expected):
    assert math.isclose(
        research.adverse_gap_bps(side, opening, trigger),
        expected,
        rel_tol=0.0,
        abs_tol=1e-12,
    )


@pytest.mark.parametrize(
    ("side", "opening", "trigger"),
    [
        ("LONG", 99.99, 100.0),
        ("SHORT", 100.01, 100.0),
    ],
)
def test_non_gap_open_returns_none(side, opening, trigger):
    assert research.adverse_gap_bps(side, opening, trigger) is None


def test_threshold_is_inclusive_but_reject_all_rejects_exact_trigger_open():
    max_zero = research.GapGuardSpec("MAX_0", 0.0)
    reject_all = research.GapGuardSpec("REJECT_ALL", None, True)
    assert research.gap_is_rejected(max_zero, 0.0) is False
    assert research.gap_is_rejected(max_zero, 0.0001) is True
    assert research.gap_is_rejected(reject_all, 0.0) is True


def test_two_and_five_bps_boundaries_are_inclusive():
    two = research.GapGuardSpec("MAX_2", 2.0)
    five = research.GapGuardSpec("MAX_5", 5.0)
    assert research.gap_is_rejected(two, 2.0) is False
    assert research.gap_is_rejected(two, 2.00001) is True
    assert research.gap_is_rejected(five, 5.0) is False
    assert research.gap_is_rejected(five, 5.00001) is True


def test_bad_gap_guard_specs_fail_closed():
    with pytest.raises(ValueError):
        research.GapGuardSpec("BAD", -1.0).validate()
    with pytest.raises(ValueError):
        research.GapGuardSpec("BAD", 1.0, True).validate()
    with pytest.raises(ValueError):
        research.adverse_gap_bps("SIDEWAYS", 100.0, 100.0)
