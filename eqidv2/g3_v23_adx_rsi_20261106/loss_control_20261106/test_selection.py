import unittest
import pandas as pd
from .run import BASELINE, choose, promotion


def row(policy, period, net=100., loss=50., dd=20., trades=10):
    return dict(policy=policy,period=period,net_pnl_rs=net,gross_loss_rs=loss,
                daily_close_drawdown_rs=dd,trades=trades)


class SelectionTests(unittest.TestCase):
    def test_later_profit_cannot_select_training_candidate(self):
        data=pd.DataFrame([row(BASELINE,'early_59'),row('bad','early_59',99,40,18),
                           row('bad','later_7',1000000,0,0)])
        self.assertEqual(choose(data),(BASELINE,'bad'))

    def test_training_requires_profit_loss_and_drawdown(self):
        data=pd.DataFrame([row(BASELINE,'early_59'),row('bad_dd','early_59',110,40,21),
                           row('good','early_59',105,45,20)])
        self.assertEqual(choose(data),('good','good'))

    def test_gate_rejects_later_deterioration_without_runner_up(self):
        data=pd.DataFrame([row(BASELINE,'full_66'),row('good','full_66',105,40,18),
                           row(BASELINE,'later_7'),row('good','later_7',99,40,18)])
        passed,reasons=promotion(data,'good')
        self.assertFalse(passed)
        self.assertIn('later_7: net profit below baseline',reasons)

    def test_gate_requires_later_drawdown_and_trade_preservation(self):
        data=pd.DataFrame([row(BASELINE,'full_66'),row('good','full_66',105,40,18),
                           row(BASELINE,'later_7'),row('good','later_7',105,40,21,9)])
        passed,reasons=promotion(data,'good')
        self.assertFalse(passed)
        self.assertEqual(len(reasons),2)

    def test_qualified_candidate_and_no_candidate_fallback(self):
        data=pd.DataFrame([row(BASELINE,p) for p in ('full_66','later_7')]+
                           [row('good',p,105,40,18) for p in ('full_66','later_7')])
        self.assertEqual(promotion(data,'good'),(True,[]))
        self.assertFalse(promotion(data,BASELINE)[0])


if __name__=='__main__':
    unittest.main()
