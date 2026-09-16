import numpy as np
import pandas as pd

import fno_v13_corrected_v4_backtest as v4


def test_v4_frozen_configuration_and_output_isolation() -> None:
    v4.validate_configuration()
    assert v4.exit_config() == {
        "initial_stop_pct": 1.50,
        "t1_pct": 1.05,
        "partial_pct": 0.20,
        "runner_size_pct": 0.80,
        "runner_target_pct": 2.60,
        "runner_stop": "BREAKEVEN",
        "square_off": "1530",
        "same_minute_policy": "STOP_OR_BREAKEVEN_WINS_TIES",
    }
    assert v4.RESULT_DIR.resolve() != v4.v3.RESULT_DIR.resolve()
    assert v4.RESULT_DIR.resolve() != v4.scaleout.RESULT_DIR.resolve()


def test_v4_books_20_percent_and_squares_unresolved_runner_at_eod() -> None:
    orders = pd.DataFrame(
        [{"sid": 1, "trigger": 100.0, "side": "LONG", "setup_id": "TEST"}]
    )
    paths = {
        1: {
            "high": np.array([100.5, 101.1, 101.5]),
            "low": np.array([99.8, 100.5, 100.8]),
            "close": np.array([100.3, 101.2, 101.4]),
        }
    }
    result = v4._run_v4(orders, paths, cost_bps=5.0)
    assert result.loc[0, "exit_reason"] == "T1_THEN_EOD"
    assert bool(result.loc[0, "t1_hit"])
    # 20% at +1.05%, 80% at +1.40%, less 0.05% total cost.
    assert np.isclose(result.loc[0, "net_return_pct"], 1.28)


def test_v4_keeps_runner_at_breakeven_on_t1_bar_ambiguity() -> None:
    orders = pd.DataFrame(
        [{"sid": 1, "trigger": 100.0, "side": "LONG", "setup_id": "TEST"}]
    )
    paths = {
        1: {
            "high": np.array([101.1]),
            "low": np.array([99.9]),
            "close": np.array([100.8]),
        }
    }
    result = v4._run_v4(orders, paths, cost_bps=5.0)
    assert result.loc[0, "exit_reason"] == "T1_THEN_BREAKEVEN"
    assert np.isclose(result.loc[0, "net_return_pct"], 0.16)

