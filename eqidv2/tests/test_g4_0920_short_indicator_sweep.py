from types import SimpleNamespace

import pandas as pd

from research import g4_0920_short_indicator_sweep as subject


def test_grid_is_unique_and_contains_baseline():
    grid = subject.profile_grid()
    assert len(grid) == 494 * len(subject.NIFTY_THRESHOLDS)
    assert sum(row["policy"] == "baseline" for row in grid) == 1
    assert len({(row["values"], row["nifty_threshold"]) for row in grid}) == len(grid)


def test_development_guard_rejects_tiny_sample():
    row = pd.Series(dict(setup_trades=4, setup_win_rate_pct=100.,
                         setup_net_pnl_rs=1000., net_delta_rs=1000.,
                         win_rate_pct=80., baseline_win_rate_pct=79.))
    assert not subject.development_qualifies(row)


def test_development_guard_accepts_economic_75_percent_result():
    row = pd.Series(dict(setup_trades=8, setup_win_rate_pct=75.,
                         setup_net_pnl_rs=1000., net_delta_rs=500.,
                         win_rate_pct=79., baseline_win_rate_pct=79.))
    assert subject.development_qualifies(row)


def test_later_guard_requires_four_trades():
    assert not subject.later_qualifies(pd.Series(dict(
        setup_trades=3, setup_win_rate_pct=100., setup_net_pnl_rs=1.)))
    assert subject.later_qualifies(pd.Series(dict(
        setup_trades=4, setup_win_rate_pct=75., setup_net_pnl_rs=1.)))
