import pandas as pd

import fno_v13_v10_balanced_research as b


def test_balanced_goal_requires_profit_retention_not_win_rate_alone():
    row=dict(TRAIN_5bps_wins=40,VALIDATION_5bps_wins=19,TRAIN_5bps_trades=43,
        VALIDATION_5bps_trades=21,minimum_split_pf=2.1,development_net=25000.,
        VALIDATION_5bps_win_rate_pct=90.,TRAIN_9bps_profit_factor=1.6,VALIDATION_9bps_profit_factor=1.6)
    rejected=b.score(pd.DataFrame([row]),76.,156000.)
    assert not rejected.balanced_pass.iloc[0]
    row['development_net']=85000.
    assert b.score(pd.DataFrame([row]),76.,156000.).balanced_pass.iloc[0]


def test_balanced_goal_rejects_weak_validation_pf():
    row=dict(TRAIN_5bps_wins=40,VALIDATION_5bps_wins=19,TRAIN_5bps_trades=43,
        VALIDATION_5bps_trades=21,minimum_split_pf=1.99,development_net=100000.,
        VALIDATION_5bps_win_rate_pct=90.,TRAIN_9bps_profit_factor=1.6,VALIDATION_9bps_profit_factor=1.6)
    assert not b.score(pd.DataFrame([row]),76.,156000.).balanced_pass.iloc[0]
