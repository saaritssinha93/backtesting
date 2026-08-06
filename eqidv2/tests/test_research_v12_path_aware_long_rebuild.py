import numpy as np
import pandas as pd

import research_v12_path_aware_long_rebuild as rebuild


def test_threshold_for_fraction_uses_upper_tail() -> None:
    scores = np.arange(1.0, 101.0)
    threshold = rebuild.threshold_for_fraction(scores, 0.10)
    assert threshold == np.quantile(scores, 0.90)
    assert int((scores >= threshold).sum()) == 10


def test_row_weights_prevent_ticker_day_signal_runs_dominating() -> None:
    frame = pd.DataFrame({
        "ticker": ["AAA", "AAA", "AAA", "BBB"],
        "trade_date": ["2026-03-02"] * 4,
    })
    weights = rebuild.row_weights(frame)
    assert weights.iloc[:3].tolist() == [1 / 3, 1 / 3, 1 / 3]
    assert weights.iloc[3] == 1.0
    grouped = weights.groupby(frame["ticker"]).sum()
    assert grouped["AAA"] == grouped["BBB"] == 1.0


def test_path_positive_requires_target_and_positive_net() -> None:
    outcomes = pd.DataFrame({
        "outcome": ["TARGET", "TARGET", "SL", "EOD"],
        "net_pnl_rs": [10.0, -1.0, 100.0, 5.0],
    })
    label = (outcomes["outcome"].eq("TARGET") & outcomes["net_pnl_rs"].gt(0)).astype(int)
    assert label.tolist() == [1, 0, 0, 0]


def test_contract_is_research_only_and_replay_is_not_selection_data() -> None:
    assert rebuild.PRODUCTION_APPROVED is False
    assert rebuild.DEVELOPMENT_END < rebuild.REPLAY_START
    assert rebuild.REPLAY_END == "2026-08-03"
    assert rebuild.SETUP not in rebuild.v12.ENTRY_SHADOW_SETUPS
