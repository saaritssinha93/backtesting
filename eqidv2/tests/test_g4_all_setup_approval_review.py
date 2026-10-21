import numpy as np
import pandas as pd

from research import g4_all_setup_approval_review as review
from research import g4_complete_inventory_backtest as complete


def test_requested_inventory_is_exactly_original_g4_49():
    active=set(complete.Identity(complete.configuration())._ACTIVE_BY_ID)
    assert len(review.SETUPS)==49
    assert set(review.SETUPS)==active-complete.ADDED_IDS


def test_policy_guards_require_loss_reduction_retention_and_chronological_sample():
    baseline=pd.Series([100.,100.,100.,-100.,-100.])
    improved=pd.Series([100.,100.,100.,10.,-50.])
    metrics=review._metrics(baseline,improved)
    assert metrics['win_rate_pct']==80.
    assert metrics['loss_saved_rs']==150.
    assert metrics['original_winner_profit_retention_pct']==100.
    assert review._development_eligible(metrics,baseline_losses=2)
    assert review._later_pass(metrics)
    too_small=dict(metrics,trades=2)
    assert not review._later_pass(too_small)
    harmed=dict(metrics,original_winner_profit_retention_pct=89.99)
    assert not review._development_eligible(harmed,baseline_losses=2)


def test_zero_trade_metrics_are_finite_except_retention():
    metrics=review._metrics(pd.Series(dtype=float),pd.Series(dtype=float))
    assert metrics['trades']==0
    assert metrics['win_rate_pct']==0.
    assert metrics['net_pnl_rs']==0.
    assert np.isnan(metrics['original_winner_profit_retention_pct'])
