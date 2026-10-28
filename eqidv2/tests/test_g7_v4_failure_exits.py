"""Execution and decision causality for the bounded G7 V4 exit hypotheses."""
import numpy as np
import pandas as pd
import pytest

from research.g3_sl_target_simulator import simulate_exit as native_exit
from research.g7_v4_failure_exits import POLICIES, simulate_exit


def path(rows, *, start="2026-10-15 15:10", is_long=True):
    data = np.asarray(rows, dtype=float)
    if not is_long:
        data = 200-data
        data[:, [1, 2]] = data[:, [2, 1]]
    times = pd.date_range(start, periods=len(rows), freq="min", tz="Asia/Kolkata")
    return {"timestamp_ns": times.asi8,
            **{name: data[:, i] for i, name in enumerate(("open", "high", "low", "close"))}}


def replay(p, is_long=True, policy="confirmation_failure", **kwargs):
    return simulate_exit(p, 0, 100, is_long, 1.2, kwargs.pop("target_pct", 1.5),
                         policy=policy, confirmation_low=99.8, confirmation_high=100.2, **kwargs)


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("rows", [
    [[100, 100.1, 99.9, 100], [98.5, 104, 95, 100]],
    [[100, 100.1, 99.9, 100], [100, 102, 98, 100]],
    [[100, 100.1, 99.9, 100], [100, 102, 99.5, 101]],
    [[100, 100.1, 99.9, 100]]*6,
])
def test_none_is_exact_native_baseline(rows, is_long):
    p = path(rows, is_long=is_long)
    actual = replay(p, is_long, policy=None)
    expected = native_exit(p, 0, 100, is_long, 1.2, 1.5,
                           tighten_after_minutes=120, tightened_stop_pct=1.)
    assert actual == expected


@pytest.mark.parametrize("is_long", [True, False])
def test_failure_requires_two_full_closes_and_equality_resets(is_long):
    p = path([
        [100, 100.1, 99.5, 99.6],  # Entry candle is excluded, despite failing close.
        [99.7, 99.9, 99.6, 99.7],
        [99.7, 99.9, 99.6, 99.8],  # Exact edge is not a failure.
        [99.8, 99.9, 99.6, 99.7],
        [99.7, 99.9, 99.5, 99.6],
        [99.55, 103, 98, 100],  # Later stop/target touches cannot supersede open exit.
    ], is_long=is_long)
    r = replay(p, is_long)
    assert r["decision_path_index"] == 4
    assert r["decision_effective_path_index"] == r["exit_path_index"] == 5
    assert r["exit_reason"] == "CONFIRMATION_FAILURE"
    assert r["exit_event"] == "OPEN"
    assert r["exit_price"] == pytest.approx(99.55 if is_long else 100.45)
    assert r["confirmation_failure_count"] == 2
    assert r["mae_pct"] == pytest.approx(-.5)
    assert pd.Timestamp(r["decision_ts"]) == r["exit_ts"]


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("exit_row,reason,price", [
    ([98.5, 103, 98, 100], "STOP", 98.5),
    ([102, 103, 98, 100], "TARGET", 101.5),
    ([99.5, 103, 98, 100], "CONFIRMATION_FAILURE", 99.5),
])
def test_pending_market_exit_open_priority_and_no_later_range(exit_row, reason, price, is_long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.1, 99.6, 99.7],
              [99.7, 99.9, 99.5, 99.6], exit_row], is_long=is_long)
    r = replay(p, is_long)
    assert r["exit_reason"] == reason
    assert r["exit_event"] == "OPEN"
    assert r["exit_price"] == pytest.approx(price if is_long else 200-price)
    assert r["decision_path_index"] == 2
    assert not r["same_bar_ambiguous"]
    assert r["exit_gap_through"] == (reason == "STOP")


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("lo,hi,reason", [(98, 103, "STOP"), (99.5, 103, "TARGET")])
def test_native_touch_before_decision_close_has_precedence(lo, hi, reason, is_long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.1, 99.6, 99.7],
              [99.7, hi, lo, 99.6]], is_long=is_long)
    r = replay(p, is_long)
    assert r["exit_reason"] == reason
    assert r["decision_path_index"] is None
    assert r["same_bar_ambiguous"] == (reason == "STOP")


@pytest.mark.parametrize("is_long", [True, False])
def test_stall_checks_completed_close_at_thirty_then_next_open(is_long):
    rows = [[100, 100.1, 99.9, 100]] + [[100, 100.2, 99.6, 99.8]]*30
    rows += [[99.7, 103, 98, 100]]
    p = path(rows, start="2026-10-15 10:00", is_long=is_long)
    r = replay(p, is_long, policy="stalled_30m")
    assert r["stall_evaluation_path_index"] == r["decision_path_index"] == 30
    assert r["exit_path_index"] == 31
    assert r["exit_reason"] == "STALLED_30M"
    assert r["holding_minutes"] == 30
    assert r["best_completed_close_return_pct"] == pytest.approx(-.2)
    assert r["exit_price"] == pytest.approx(99.7 if is_long else 100.3)


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("best_prior,current_at_30", [(.4, -.2), (.3, -.2), (.1, 0.), (.1, .1)])
def test_stall_no_repeated_checks_after_first_pass(best_prior, current_at_30, is_long):
    rows = [[100, 100.1, 99.9, 100]] + [[100, 100.2, 99.6, 99.8]]*35
    rows[1] = [100, 100+best_prior, 99.9, 100+best_prior]
    rows[30] = [100, 100.2, 99.6, 100+current_at_30]
    p = path(rows, start="2026-10-15 14:40", is_long=is_long)
    r = replay(p, is_long, policy="stalled_30m")
    assert r["stall_evaluation_path_index"] == 30
    assert r["decision_path_index"] is None
    assert r["exit_reason"] == "TIME_EXIT_1515"


@pytest.mark.parametrize("is_long", [True, False])
def test_stall_ignores_profitable_entry_close_and_intrabar_peak(is_long):
    rows = [[100, 100.8, 99.9, 100.7]] + [[100, 100.8, 99.6, 99.8]]*30
    rows += [[99.7, 99.9, 99.6, 99.8]]
    r = replay(path(rows, start="2026-10-15 10:00", is_long=is_long),
               is_long, policy="stalled_30m")
    assert r["exit_reason"] == "STALLED_30M"


@pytest.mark.parametrize("is_long", [True, False])
def test_trail_needs_three_full_bars_and_updates_only_next_open(is_long):
    rows = [
        [100, 101.1, 98.9, 101.0],  # Entry low must not enter trailing window.
        [100.2, 100.5, 99.2, 100.2],
        [100.2, 100.8, 99.6, 100.6],
        [100.6, 101.2, 100.6, 101.0],  # Arms low99.2 after this close.
        [101.0, 101.5, 100.5, 101.4],  # Reconsiders low99.6 after this close.
        [101.3, 101.6, 100.6, 101.5],  # Reconsiders low100.5 after this close.
        [100.7, 101.2, 100.4, 100.6],
    ]
    r = replay(path(rows, start="2026-10-15 10:00", is_long=is_long),
               is_long, policy="confirmed_trail", target_pct=2.)
    assert r["trail_armed_path_index"] == 3
    assert r["protection_effective_path_index"] == 4
    assert r["trail_last_update_path_index"] == 5
    assert r["trail_effective_path_index"] == r["exit_path_index"] == 6
    assert r["exit_reason"] == "PROTECTED_STOP"
    assert r["exit_price"] == pytest.approx(100.5 if is_long else 99.5)
    assert r["active_stop_pct_at_exit"] == pytest.approx(-.5)


@pytest.mark.parametrize("is_long", [True, False])
def test_trail_does_not_arm_on_peak_or_early_closes_and_never_loosens(is_long):
    rows = [
        [100, 101.1, 99.9, 101.0],
        [100, 101.2, 99.8, 101.0],  # One full bar cannot arm.
        [100.9, 101.1, 99.9, 100.9],
        [100.9, 101.4, 100.7, 100.9],  # Only intrabar peak reaches threshold.
        [100.9, 101.3, 100.8, 101.1],  # Arms at3bar low99.9.
        [101.0, 101.2, 100.7, 101.1],  # Updates to100.7.
        [101.0, 101.2, 100.8, 101.1],
        [100.6, 103, 98, 100.9],      # Gap through existing trail100.7.
    ]
    r = replay(path(rows, start="2026-10-15 10:00", is_long=is_long),
               is_long, policy="confirmed_trail", target_pct=2.,
               tighten_after_minutes=6, tightened_stop_pct=1.)
    assert r["trail_armed_path_index"] == 4
    assert r["exit_path_index"] == 7
    assert r["exit_reason"] == "PROTECTED_STOP"
    assert r["exit_event"] == "OPEN"
    assert r["active_stop_pct_at_exit"] == pytest.approx(-.7)
    assert r["exit_price"] == pytest.approx(100.6 if is_long else 99.4)
    assert r["exit_gap_through"]
    assert r["mfe_pct"] == pytest.approx(1.4)


@pytest.mark.parametrize("policy", POLICIES)
@pytest.mark.parametrize("is_long", [True, False])
def test_timer_native_semantics_without_research_activation(policy, is_long):
    rows = [[100, 100.1, 99.9, 100] for _ in range(122)]
    rows[120] = [100, 100.1, 98.9, 99.5]
    rows[121] = [99.5, 99.6, 98.9, 99.2]
    r = replay(path(rows, start="2026-10-15 10:00", is_long=is_long), is_long, policy=policy)
    assert r["exit_path_index"] == 121
    assert r["exit_reason"] == "TIGHTENED_STOP"
    assert r["active_stop_pct_at_exit"] == 1.


@pytest.mark.parametrize("policy", POLICIES)
def test_squareoff_precedes_new_close_decision_and_missing_squareoff_fails(policy):
    p = path([[100, 100.1, 99.6, 99.7]]*3, start="2026-10-15 15:13")
    r = replay(p, policy=policy)
    assert r["exit_reason"] == "TIME_EXIT_1515"
    assert r["decision_path_index"] is None
    with pytest.raises(ValueError, match="missing.*15:15"):
        replay({key: value[:2] for key, value in p.items()}, policy=policy)


@pytest.mark.parametrize("is_long", [True, False])
def test_valid_future_suffix_does_not_change_past_exit(is_long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.1, 99.6, 99.7],
              [99.7, 99.9, 99.5, 99.6], [99.5, 103, 98, 100],
              [100, 105, 95, 100], [100, 103, 97, 100]], is_long=is_long)
    before = replay(p, is_long)
    p["open"][4:] = 90
    p["high"][4:] = 150
    p["low"][4:] = 50
    p["close"][4:] = 110
    # Later range in the exit candle also has no influence on an OPEN exit.
    p["high"][3], p["low"][3], p["close"][3] = 150, 50, 110
    assert replay(p, is_long) == before


@pytest.mark.parametrize("is_long", [True, False])
def test_nonzero_entry_index_excludes_entire_preentry_prefix(is_long):
    rows = [[100, 104, 90, 95], [100, 103, 94, 95],
            [100, 101.1, 98.9, 101],
            [100.2, 100.5, 99.2, 100.2],
            [100.2, 100.8, 99.6, 100.6],
            [100.6, 101.2, 100.6, 101.0],
            [99.1, 101, 99, 100]]
    p = path(rows, start="2026-10-15 10:00", is_long=is_long)
    r = simulate_exit(p, 2, 100, is_long, 1.2, 2., policy="confirmed_trail")
    assert r["trail_armed_path_index"] == 5
    assert r["exit_path_index"] == 6
    assert r["active_stop_price"] == pytest.approx(99.2 if is_long else 100.8)
    assert r["exit_event"] == "OPEN"


@pytest.mark.parametrize("policy", POLICIES)
def test_bad_path_and_missing_minute_are_rejected(policy):
    p = path([[100, 100.1, 99.9, 100]]*6)
    bad = {key: value.copy() for key, value in p.items()}
    bad["high"][2] = 90
    with pytest.raises(ValueError, match="Invalid OHLC"):
        replay(bad, policy=policy)
    bad = {key: value.copy() for key, value in p.items()}
    bad["timestamp_ns"][2:] += 60_000_000_000
    with pytest.raises(ValueError, match="contiguous"):
        replay(bad, policy=policy)
    bad = {key: value.copy() for key, value in p.items()}
    bad["close"][2] = float("nan")
    with pytest.raises(ValueError, match="finite and positive"):
        replay(bad, policy=policy)


def test_invalid_policy_edges_and_timer_fail_closed():
    p = path([[100, 100.1, 99.9, 100]]*6)
    with pytest.raises(ValueError, match="Unknown research"):
        replay(p, policy="unregistered")
    with pytest.raises(ValueError, match="confirmation_low"):
        simulate_exit(p, 0, 100, True, 1.2, 1.5, policy="confirmation_failure")
    with pytest.raises(ValueError, match="positive and ordered"):
        simulate_exit(p, 0, 100, True, 1.2, 1.5, policy="confirmation_failure",
                      confirmation_low=101, confirmation_high=99)
    with pytest.raises(ValueError, match="cannot loosen"):
        replay(p, tightened_stop_pct=1.3)
