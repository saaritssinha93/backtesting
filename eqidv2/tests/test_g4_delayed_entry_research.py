"""Causal, research-only delayed-entry behaviour for G4 breakout orders."""

from types import SimpleNamespace

import numpy as np
import pytest

from fno_v13_corrected_v5_backtest import _entry as native_entry
from research.g4_delayed_entry_research import delayed_entry


def _bar(side, *, touch=False, hold=False, gap=False):
    """Return a valid OHLC bar around a 100-rupee breakout trigger."""
    if side == "LONG":
        if gap:
            return (101.0, 101.5, 100.8, 101.2)
        if touch:
            return (99.8, 100.5, 99.5, 100.1 if hold else 99.8)
        return (99.4, 99.8, 99.0, 99.5)
    if gap:
        return (99.0, 99.2, 98.5, 98.8)
    if touch:
        return (100.2, 100.5, 99.5, 99.9 if hold else 100.2)
    return (100.6, 101.0, 100.2, 100.5)


def _path(side, touches=(), *, hold_first=False, gap_at=None, bars=11):
    values = [_bar(side, touch=i in touches, hold=i == 0 and hold_first,
                   gap=i == gap_at) for i in range(bars)]
    return {
        "timestamp_ns": np.arange(bars, dtype=np.int64) * 60_000_000_000,
        **{key: np.asarray([bar[j] for bar in values], dtype=float)
           for j, key in enumerate(("open", "high", "low", "close"))},
    }


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_zero_delay_matches_native_entry_contract(side):
    row = SimpleNamespace(side=side, trigger=100.0)
    path = _path(side, touches=(0, 1))
    expected = native_entry(row, path, delay_bars=0, trigger_buffer_pct=0.0,
                            worse_fill_bps=0.0, max_entry_delay_minutes=10)
    assert delayed_entry(row, path, delay_bars=0) == expected


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_first_minute_touch_does_not_fill_when_entry_is_delayed(side):
    row = SimpleNamespace(side=side, trigger=100.0)
    assert delayed_entry(row, _path(side, touches=(0,)), delay_bars=1) is None


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_later_retouch_fills_at_original_trigger_without_hindsight(side):
    row = SimpleNamespace(side=side, trigger=100.0)
    found = delayed_entry(row, _path(side, touches=(0, 1)), delay_bars=1)
    assert found is not None
    index, entry, trigger, gap, overshoot_bps = found
    assert index == 1
    assert entry == pytest.approx(100.0)
    assert trigger == pytest.approx(100.0)
    assert not gap
    assert overshoot_bps == pytest.approx(0.0)


@pytest.mark.parametrize("side,expected_open", [("LONG", 101.0), ("SHORT", 99.0)])
def test_second_minute_gap_through_uses_adverse_open(side, expected_open):
    row = SimpleNamespace(side=side, trigger=100.0)
    found = delayed_entry(row, _path(side, touches=(0,), gap_at=1), delay_bars=1)
    assert found is not None
    index, entry, trigger, gap, overshoot_bps = found
    assert index == 1
    assert entry == pytest.approx(expected_open)
    assert trigger == pytest.approx(100.0)
    assert gap
    assert overshoot_bps > 0


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_original_ten_minute_expiry_is_not_extended_by_delay(side):
    row = SimpleNamespace(side=side, trigger=100.0)
    minute_ten = delayed_entry(row, _path(side, touches=(9,)), delay_bars=1)
    minute_eleven = delayed_entry(row, _path(side, touches=(10,)), delay_bars=1)
    assert minute_ten is not None and minute_ten[0] == 9
    assert minute_eleven is None


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_hold_condition_uses_only_completed_skipped_bar_close(side):
    row = SimpleNamespace(side=side, trigger=100.0)
    failed_hold = _path(side, touches=(0, 1), hold_first=False)
    passed_hold = _path(side, touches=(0, 1), hold_first=True)
    # Without the optional check, the same second-minute touch remains eligible.
    assert delayed_entry(row, failed_hold, delay_bars=1, require_hold=False)[0] == 1
    assert delayed_entry(row, failed_hold, delay_bars=1, require_hold=True) is None
    assert delayed_entry(row, passed_hold, delay_bars=1, require_hold=True)[0] == 1
