import unittest
import numpy as np
import pandas as pd
from .run import BASELINE,choose,qualification
from .data import adaptive_risk


def row(name,period,net=100.,loss=50.,dd=20.,trades=10):
    return dict(policy=name,period=period,net_pnl_rs=net,gross_loss_rs=loss,
                daily_close_drawdown_rs=dd,trades=trades)


class TrailingSelectionTests(unittest.TestCase):
    def test_new_baseline_is_fixed1p50(self):
        self.assertEqual(BASELINE,'fixed_1p50')
        frame=pd.DataFrame([row(BASELINE,'fit_44'),row('candidate','fit_44',105,40,19)])
        self.assertEqual(choose(frame),('candidate','candidate'))

    def test_future_profit_cannot_select_candidate(self):
        frame=pd.DataFrame([row(BASELINE,'fit_44'),row('bad','fit_44',99,40,19),
                            row('bad','later_7',999999,0,0)])
        self.assertEqual(choose(frame),(BASELINE,'bad'))

    def test_all_qualification_periods_must_pass(self):
        periods=('validation_15','later_7','full_66')
        frame=pd.DataFrame([row(BASELINE,p) for p in periods]+[row('good',p,105,40,19) for p in periods])
        self.assertEqual(qualification(frame,'good'),(True,[]))
        frame.loc[frame.policy.eq('good')&frame.period.eq('validation_15'),'net_pnl_rs']=99
        self.assertEqual(qualification(frame,'good'),(False,['validation_15: lower net profit']))

    def test_full_period_equality_of_losses_is_not_improvement(self):
        periods=('validation_15','later_7','full_66')
        frame=pd.DataFrame([row(BASELINE,p) for p in periods]+[row('flat',p,105,50,19) for p in periods])
        self.assertFalse(qualification(frame,'flat')[0])

    def test_adaptive_structure_range_and_invalid_edge_fallback(self):
        frame=pd.DataFrame([
            dict(sid=1,day='2026-11-05',side='LONG',entry_price=100.,entry_ts='2026-11-05 10:02+05:30',
                 confirmation_ts='2026-11-05 10:01+05:30',feature_available_at='2026-11-05 10:01+05:30',
                 atr14_1m=.1,atr14_pct=.1,confirmation_low=99.9,confirmation_high=100.,structure_distance_pct=.1),
            dict(sid=2,day='2026-11-05',side='SHORT',entry_price=100.,entry_ts='2026-11-05 10:02+05:30',
                 confirmation_ts='2026-11-05 10:01+05:30',feature_available_at='2026-11-05 10:01+05:30',
                 atr14_1m=.1,atr14_pct=.1,confirmation_low=99.,confirmation_high=99.9,structure_distance_pct=-.1)])
        np.testing.assert_allclose(adaptive_risk(frame),[.4,1.5])
        frame.loc[0,'feature_available_at']='2026-11-05 10:03+05:30'
        with self.assertRaises(ValueError):adaptive_risk(frame)


if __name__=='__main__':
    unittest.main()
