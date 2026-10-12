"""Independent target overrides and honest target ranking with small samples."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from research.g3_0920_target_sweep import (
    TARGETS, apply_targets, assert_same_entries, choose_pair, choose_side, pair_passes,
)


def test_target_grid_covers_regular_steps_and_original_targets():
    assert len(TARGETS) == 61 and list(TARGETS) == sorted(set(TARGETS))
    assert TARGETS[0] == .1 and TARGETS[-1] == 3.
    assert {.97, 1., 2.67}.issubset(TARGETS)
    assert all(round(i / 100, 2) in TARGETS for i in range(10, 301, 5))


def test_separate_targets_preserve_entry_and_staged_stop_fields():
    orders = pd.DataFrame(dict(side=["LONG", "SHORT"], native_target_pct=[1., 1.],
                                target_pct=[1., 1.], native_stop_pct=[1.25, 1.25],
                                tightened_stop_pct=[1., 1.], tighten_after_minutes=[120, 120],
                                trigger=[100., 200.]))
    original = orders.copy(deep=True)
    changed = apply_targets(orders, .8, .35)
    assert changed.native_target_pct.tolist() == [.8, .35]
    assert changed.target_pct.tolist() == [.8, .35]
    for field in ("native_stop_pct", "tightened_stop_pct", "tighten_after_minutes", "trigger"):
        pd.testing.assert_series_equal(changed[field], original[field])
    pd.testing.assert_frame_equal(orders, original)


@pytest.mark.parametrize("long,short", [(0., 1.), (1., 3.1), (np.nan, 1.), (1., np.inf)])
def test_invalid_targets_fail_closed(long, short):
    with pytest.raises(ValueError):
        apply_targets(pd.DataFrame(dict(side=["LONG"])), long, short)


def metric(count, wr, net):
    return dict(executed_trades=count, win_rate_pct=wr, net_pnl_rs=net, max_drawdown_rs=20.)


def trial(long=1., short=1., *, short_wr=100., net=100.):
    period = dict(added_0920=metric(10, 80., net), combined=metric(122, 72., 1000. + net),
                  added_long=metric(8, 75., net - 10.), added_short=metric(2, short_wr, 10.),
                  incremental_pnl_rs=net)
    return dict(long_target_pct=long, short_target_pct=short,
                periods={p: deepcopy(period) for p in ("discovery", "holdout", "all")})


def test_two_short_winners_are_observed_success_but_not_minimum_sample_validation():
    t = trial()
    assert pair_passes(t["periods"]["all"], each_side=True)
    assert not pair_passes(t["periods"]["all"], each_side=True, side_minimum=5)
    assert choose_side([t], "SHORT") is None
    assert choose_side([t], "SHORT", minimum=1) is t
    t["periods"]["all"]["added_short"]["win_rate_pct"] = 50.
    assert pair_passes(t["periods"]["all"])
    assert not pair_passes(t["periods"]["all"], each_side=True)


def test_pair_ranking_prefers_net_after_screen_without_rounding_win_rate_up():
    a, b = trial(.5, .5, net=200.), trial(.6, .5, net=100.)
    assert choose_pair([a, b]) is a
    a["periods"]["all"]["added_0920"]["win_rate_pct"] = 69.999
    assert choose_pair([a, b]) is b


def test_side_sweep_holds_opposite_target_at_one_and_uses_requested_period():
    a, b, c = trial(.5, 1., net=200.), trial(.6, 1., net=100.), trial(.8, .3, net=500.)
    a["periods"]["holdout"]["added_long"]["win_rate_pct"] = 0.
    assert choose_side([a, b, c], "LONG", "discovery") is a
    assert choose_side([a, b, c], "LONG", "holdout") is b


def test_target_sweep_allows_exit_changes_but_rejects_entry_changes():
    frame = pd.DataFrame(dict(sid=[1], signal_id=["A"], filled=[True], entry_path_index=[0],
                               entry_price=[100.], entry_ts=["2026-10-12T09:22:00+05:30"],
                               exit_price=[101.]))
    changed = frame.copy()
    changed["exit_price"] = 100.5
    assert_same_entries(changed, frame)
    changed["entry_price"] = 100.1
    with pytest.raises(RuntimeError, match="entry_price"):
        assert_same_entries(changed, frame)
