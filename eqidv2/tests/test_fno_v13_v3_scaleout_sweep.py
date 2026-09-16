import numpy as np
import pandas as pd

import fno_v13_v3_scaleout_sweep as scaleout


def _orders() -> pd.DataFrame:
    return pd.DataFrame([{"sid": 1, "trigger": 100.0, "side": "LONG"}])


def test_runner_uses_eod_when_neither_post_t1_level_is_touched() -> None:
    paths = {
        1: {
            "high": np.array([100.5, 101.1, 101.5]),
            "low": np.array([99.8, 100.5, 100.8]),
            "close": np.array([100.3, 101.2, 101.4]),
        }
    }
    result = scaleout.simulate_scaleout(
        _orders(),
        paths,
        initial_stop_pct=1.5,
        t1_pct=1.0,
        partial_pct=0.2,
        runner_target_pct=3.0,
        runner_stop="BREAKEVEN",
        cost_bps=5.0,
    )
    assert result.loc[0, "exit_reason"] == "T1_THEN_EOD"
    assert bool(result.loc[0, "t1_hit"])
    # 20% at +1%, 80% at +1.4%, less 0.05% total cost.
    assert np.isclose(result.loc[0, "net_return_pct"], 1.27)


def test_breakeven_wins_ambiguous_t1_minute_for_runner() -> None:
    paths = {
        1: {
            "high": np.array([100.5, 101.1, 101.5]),
            "low": np.array([99.8, 99.9, 100.4]),
            "close": np.array([100.3, 100.8, 101.2]),
        }
    }
    result = scaleout.simulate_scaleout(
        _orders(),
        paths,
        initial_stop_pct=1.5,
        t1_pct=1.0,
        partial_pct=0.2,
        runner_target_pct=3.0,
        runner_stop="BREAKEVEN",
        cost_bps=5.0,
    )
    assert result.loc[0, "exit_reason"] == "T1_THEN_BREAKEVEN"
    assert np.isclose(result.loc[0, "net_return_pct"], 0.15)


def test_initial_stop_wins_same_minute_tie_with_t1() -> None:
    paths = {
        1: {
            "high": np.array([101.1]),
            "low": np.array([98.4]),
            "close": np.array([100.0]),
        }
    }
    result = scaleout.simulate_scaleout(
        _orders(),
        paths,
        initial_stop_pct=1.5,
        t1_pct=1.0,
        partial_pct=0.2,
        runner_target_pct=3.0,
        runner_stop="BREAKEVEN",
        cost_bps=5.0,
    )
    assert result.loc[0, "exit_reason"] == "FULL_STOP"
    assert not bool(result.loc[0, "t1_hit"])
    assert np.isclose(result.loc[0, "net_return_pct"], -1.55)

