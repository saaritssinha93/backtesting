"""Causality and native-priority checks for the isolated G4 exit experiment."""
import numpy as np
import pandas as pd
import pytest

from research.g3_sl_target_simulator import simulate_exit
from research.g4_failed_breakout_exit import simulate_failed_breakout_exit


def path(rows, start="2026-10-16 10:01", *, long=False):
    values = np.asarray(rows, float)
    if long:
        values = 200 - values
        values[:, [1, 2]] = values[:, [2, 1]]
    stamps = pd.date_range(start, periods=len(rows), freq="min", tz="Asia/Kolkata")
    return {"timestamp_ns": stamps.asi8,
            **{name: values[:, i] for i, name in enumerate(("open", "high", "low", "close"))}}


def replay(p, *, long=False, partial=True, enabled=True, high=100.5, low=99.5):
    return simulate_failed_breakout_exit(p, 0, 100., long, 1.5, high, low,
                                         partial=partial, enable_long=enabled)


@pytest.mark.parametrize("long", [False, True])
def test_signal_close_exits_only_at_next_open_and_excludes_later_range(long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.8, 104, 96, 101]], long=long)
    r = replay(p, long=long)
    assert r["exit_reason"] == "FAILED_BREAKOUT_EXIT"
    assert r["exit_path_index"] == 2 and r["exit_event"] == "OPEN"
    assert r["exit_price"] == pytest.approx(99.2 if long else 100.8)
    assert r["exit_ts"] == pd.Timestamp("2026-10-16 10:02", tz="Asia/Kolkata")
    assert r["holding_minutes"] == 1
    assert r["invalidation_signal_path_index"] == 1
    assert pd.Timestamp(r["invalidation_signal_ts"]) == r["exit_ts"]
    assert r["mfe_pct"] == pytest.approx(.1)
    assert r["mae_pct"] == pytest.approx(-.8)
    assert not r["stop_hit"] and not r["target_hit"]


def test_appending_future_prices_cannot_change_an_already_executed_exit():
    rows = [[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6], [100.8, 100.9, 100.7, 100.8]]
    short = replay(path(rows))
    extended = replay(path(rows + [[100.8, 120, 80, 100]]))
    for key in ("exit_ts", "exit_price", "exit_reason", "mfe_pct", "mae_pct"):
        assert short[key] == extended[key]


@pytest.mark.parametrize("long", [False, True])
def test_entry_candle_and_equal_boundary_do_not_trigger(long):
    p = path([[100, 100.8, 99.9, 100.7], [100.4, 100.8, 99.9, 100.5],
              [100.4, 100.5, 99.9, 100.4]], long=long)
    r = replay(p, long=long)
    assert r["position_open"] and not r["failed_breakout_exit_applied"]
    assert r["invalidation_signal_path_index"] is None
    assert r["failed_breakout_audit_reason"] == "NO_INVALIDATING_CLOSE"


def test_intrabar_boundary_touch_without_adverse_close_does_not_trigger():
    r = replay(path([[100, 100.1, 99.9, 100], [100, 100.9, 99.9, 100.2],
                     [100.2, 100.3, 99.9, 100.1]]))
    assert r["position_open"] and not r["failed_breakout_exit_applied"]


@pytest.mark.parametrize("long", [False, True])
@pytest.mark.parametrize("native_reason,row", [
    ("STOP", [100, 101.4, 99.9, 100.6]),
    ("TARGET", [100, 100.8, 98, 100.6]),
])
def test_native_intrabar_exit_in_signal_candle_has_priority(long, native_reason, row):
    p = path([[100, 100.1, 99.9, 100], row, [100.7, 100.8, 100.5, 100.6]], long=long)
    r = replay(p, long=long)
    assert r["exit_reason"] == native_reason
    assert r["exit_path_index"] == 1
    assert not r["failed_breakout_exit_applied"]
    assert r["invalidation_signal_path_index"] is None


@pytest.mark.parametrize("long", [False, True])
def test_native_stop_gap_on_requested_open_has_priority(long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [101.5, 105, 90, 101]], long=long)
    r = replay(p, long=long)
    assert r["exit_reason"] == "STOP" and r["exit_event"] == "OPEN"
    assert r["exit_price"] == pytest.approx(98.5 if long else 101.5)
    assert r["exit_gap_through"] and not r["same_bar_ambiguous"]
    assert r["failed_breakout_audit_reason"] == "NATIVE_STOP_AT_NEXT_OPEN"
    assert not r["failed_breakout_exit_applied"]


@pytest.mark.parametrize("long", [False, True])
@pytest.mark.parametrize("next_row", [[98, 104, 97, 101], [98, 100.9, 97, 100.8]])
def test_native_target_open_keeps_exact_native_timestamp_and_stop_first_ambiguity(long, next_row):
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              next_row], long=long)
    r = replay(p, long=long)
    native = simulate_exit(p, 0, 100, long, 1.2, 1.5,
                           tighten_after_minutes=120, tightened_stop_pct=1.)
    for key in native:
        assert r[key] == native[key]
    assert r["exit_event"] == "INTRABAR"
    assert r["failed_breakout_audit_reason"] == "NATIVE_TARGET_OPEN_PRECEDENCE"
    assert not r["failed_breakout_exit_applied"]


def test_pending_signal_at_partial_cutoff_is_open_and_never_fabricates_next_open():
    r = replay(path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6]]))
    assert r["position_open"] and r["exit_reason"] == "OPEN_AT_CUTOFF"
    assert pd.isna(r["exit_ts"]) and np.isnan(r["exit_price"])
    assert r["mark_price"] == 100.6
    assert r["invalidation_pending_next_open"]
    assert r["failed_breakout_audit_reason"] == "PENDING_NEXT_OPEN_AT_CUTOFF"
    assert not r["failed_breakout_exit_applied"]


def test_signal_on_squareoff_close_cannot_schedule_an_exit_after_1515():
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.7, 105, 90, 101]], start="2026-10-16 15:14")
    r = replay(p, partial=False)
    assert r["exit_reason"] == "TIME_EXIT_1515"
    assert r["exit_path_index"] == 1
    assert r["invalidation_signal_path_index"] is None


def test_signal_at_1514_can_exit_at_1514_open_of_end_labelled_1515_candle():
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.8, 105, 90, 101]], start="2026-10-16 15:13")
    r = replay(p, partial=False)
    assert r["exit_reason"] == "FAILED_BREAKOUT_EXIT"
    assert r["exit_ts"] == pd.Timestamp("2026-10-16 15:14", tz="Asia/Kolkata")


def test_scheduled_stop_gap_is_active_on_requested_exit_open():
    rows = [[100, 100.1, 99.9, 100]] * 122
    rows[120] = [100, 100.7, 99.9, 100.6]
    rows[121] = [101.1, 101.3, 100.5, 100.8]
    r = replay(path(rows))
    assert r["exit_reason"] == "TIGHTENED_STOP"
    assert r["exit_price"] == 101.1 and r["active_stop_pct_at_exit"] == 1.
    assert r["failed_breakout_audit_reason"] == "NATIVE_STOP_AT_NEXT_OPEN"


def test_primary_short_only_experiment_keeps_long_native_result():
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.8, 104, 96, 101]], long=True)
    r = replay(p, long=True, enabled=False)
    native = simulate_exit(p, 0, 100, True, 1.2, 1.5,
                           tighten_after_minutes=120, tightened_stop_pct=1.)
    for key in native:
        assert r[key] == native[key]
    assert r["failed_breakout_audit_reason"] == "LONG_RULE_DISABLED"


def test_complete_mode_requires_complete_native_path_and_invalid_ohlc_is_rejected():
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.8, 100.9, 100.7, 100.8]])
    with pytest.raises(ValueError, match="missing.*15:15"):
        replay(p, partial=False)
    p["timestamp_ns"][2] += 60_000_000_000
    with pytest.raises(ValueError, match="contiguous"):
        replay(p)
