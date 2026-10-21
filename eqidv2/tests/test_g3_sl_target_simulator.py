"""Exit-order causality and minute execution assumptions for research sweeps."""
import numpy as np
import pandas as pd
import pytest

from research.g3_sl_target_simulator import simulate_exit


def path(rows, start="2026-10-15 15:10", *, is_long=True):
    data = np.asarray(rows, dtype=float)
    if not is_long:
        data = 200-data
        data[:, [1, 2]] = data[:, [2, 1]]
    times = pd.date_range(start, periods=len(rows), freq="min", tz="Asia/Kolkata")
    return {"timestamp_ns": times.asi8,
            **{name: data[:, i] for i, name in enumerate(("open", "high", "low", "close"))}}


@pytest.mark.parametrize("is_long", [True, False])
def test_full_post_entry_close_arms_protection_only_for_following_bar(is_long):
    p = path([
        [100, 100.1, 99.9, 100],
        [100, 100.8, 99.8, 100.7],  # Crosses future stop before its close arms it.
        [100.6, 100.7, 100.05, 100.2],
    ], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5, .6, .1)
    assert r["exit_path_index"] == 2
    assert r["exit_reason"] == "PROTECTED_STOP"
    assert r["exit_price"] == pytest.approx(100.1 if is_long else 99.9)
    assert r["active_stop_pct_at_exit"] == -.1
    assert r["protection_armed_path_index"] == 1
    assert r["protection_effective_path_index"] == 2


@pytest.mark.parametrize("is_long", [True, False])
def test_exact_activation_price_is_inclusive_for_both_directions(is_long):
    p = path([[100, 100.1, 99.9, 100], [100, 100.7, 99.9, 100.6],
              [100.5, 100.6, 100.05, 100.2]], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5, .6, .1)
    assert r["protection_armed_path_index"] == 1
    assert r["exit_reason"] == "PROTECTED_STOP"


@pytest.mark.parametrize("is_long", [True, False])
def test_entry_candle_close_and_intrabar_peak_cannot_arm_protection(is_long):
    p = path([
        [100, 100.8, 99.8, 100.7],  # Entry candle close is not a full post-entry observation.
        [100.2, 100.9, 99.8, 100.2],  # Intrabar peak alone cannot arm either.
        [100.2, 100.3, 99.8, 100.1],
        [100.1, 100.2, 99.8, 100.0],
        [100.0, 100.1, 99.8, 100.0],
        [100.0, 100.2, 99.8, 100.1],
    ], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5, .6, .1)
    assert r["exit_reason"] == "TIME_EXIT_1515"
    assert r["protection_armed_path_index"] is None


@pytest.mark.parametrize("is_long", [True, False])
def test_gap_stop_uses_observed_open_and_excludes_later_exit_bar_excursions(is_long):
    p = path([[100, 100.2, 99.9, 100], [98.5, 104, 95, 100]], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5)
    assert r["exit_event"] == "OPEN"
    assert r["exit_price"] == pytest.approx(98.5 if is_long else 101.5)
    assert r["exit_gap_through"]
    assert r["mfe_pct"] == pytest.approx(.2)
    assert r["mae_pct"] == pytest.approx(-1.5)
    assert pd.Timestamp(r["exit_bar_end_ts"])-r["exit_ts"] == pd.Timedelta(minutes=1)
    assert not r["same_bar_ambiguous"]


@pytest.mark.parametrize("is_long", [True, False])
def test_entry_bar_open_cannot_be_reused_as_post_entry_gap_fill(is_long):
    p = path([[98.5, 100.1, 98.5, 100]], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5)
    assert r["exit_event"] == "INTRABAR"
    assert r["exit_price"] == pytest.approx(98.8 if is_long else 101.2)
    assert not r["exit_gap_through"]


@pytest.mark.parametrize("is_long", [True, False])
def test_intrabar_stop_target_tie_is_stop_first(is_long):
    p = path([[100, 100.1, 99.9, 100], [100, 102, 98, 100]], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5)
    assert r["exit_reason"] == "STOP"
    assert r["same_bar_ambiguous"]
    assert r["gross_return_pct"] == pytest.approx(-1.2)


@pytest.mark.parametrize("is_long", [True, False])
def test_timer_waits_for_bar_open_after_entry_end_plus_delay(is_long):
    rows = [[100, 100.1, 99.9, 100] for _ in range(122)]
    rows[120] = [100, 100.1, 98.9, 99.5]  # Opens 119 minutes after entry end: original stop.
    rows[121] = [99.5, 99.6, 98.9, 99.2]  # Opens at +120: tightened stop.
    p = path(rows, start="2026-10-15 10:00", is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5,
                      tighten_after_minutes=120, tightened_stop_pct=1.)
    assert r["exit_path_index"] == 121
    assert r["exit_reason"] == "TIGHTENED_STOP"
    assert r["active_stop_pct_at_exit"] == 1.
    assert r["gross_return_pct"] == pytest.approx(-1.)


@pytest.mark.parametrize("is_long", [True, False])
def test_timer_never_loosens_already_protected_stop(is_long):
    p = path([
        [100, 100.1, 99.9, 100],
        [100, 100.8, 99.9, 100.7],
        [100.7, 100.8, 100.3, 100.5],
        [100.5, 100.6, 100.05, 100.2],
    ], is_long=is_long)
    r = simulate_exit(p, 0, 100, is_long, 1.2, 1.5, .6, .1,
                      tighten_after_minutes=2, tightened_stop_pct=1.)
    assert r["exit_path_index"] == 3
    assert r["exit_reason"] == "PROTECTED_STOP"
    assert r["active_stop_pct_at_exit"] == -.1


def test_squareoff_ignores_all_later_bars_and_requires_actual_1515_candle():
    p = path([[100, 100.2, 99.8, 100.1], [100.1, 100.2, 99.8, 100.1],
              [100.1, 104, 96, 100]], start="2026-10-15 15:14")
    r = simulate_exit(p, 0, 100, True, 1.2, 1.5)
    assert r["exit_path_index"] == 1
    assert r["exit_reason"] == "TIME_EXIT_1515"
    assert r["exit_price"] == 100.1
    with pytest.raises(ValueError, match="missing.*15:15"):
        simulate_exit({key: value[:1] for key, value in p.items()}, 0, 100, True, 1.2, 1.5)


def test_gap_on_protection_activation_fills_worse_than_protected_level():
    p = path([[100, 100.1, 99.9, 100], [100, 100.8, 99.9, 100.7],
              [99.8, 101, 99.7, 100.2]])
    r = simulate_exit(p, 0, 100, True, 1.2, 1.5, .6, .1)
    assert r["exit_reason"] == "PROTECTED_STOP"
    assert r["exit_event"] == "OPEN"
    assert r["exit_price"] == 99.8
    assert r["gross_return_pct"] == pytest.approx(-.2)


def test_noncontiguous_path_and_loosening_timer_are_rejected():
    p = path([[100, 100.1, 99.9, 100] for _ in range(6)])
    p["timestamp_ns"][2:] += 60_000_000_000
    with pytest.raises(ValueError, match="contiguous"):
        simulate_exit(p, 0, 100, True, 1.2, 1.5)
    with pytest.raises(ValueError, match="cannot loosen"):
        simulate_exit(p, 0, 100, True, 1.2, 1.5,
                      tighten_after_minutes=120, tightened_stop_pct=1.25)
