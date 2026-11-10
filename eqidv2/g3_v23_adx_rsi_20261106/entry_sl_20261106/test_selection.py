import unittest
import pandas as pd
from .run import BASELINE,choose,promotion


def row(name,period,net=100.,loss=50.,dd=20.,trades=10):
    return dict(policy=name,period=period,net_pnl_rs=net,gross_loss_rs=loss,
                daily_close_drawdown_rs=dd,trades=trades)


class EntrySelectionTests(unittest.TestCase):
    def test_next15_and_last7_cannot_choose_fit_candidate(self):
        frame=pd.DataFrame([row(BASELINE,'fit_44'),row('bad','fit_44',99,40,18),
                            row('bad','validation_15',999999,0,0),row('bad','later_7',999999,0,0)])
        self.assertEqual(choose(frame),(BASELINE,'bad'))

    def test_fit_requires_all_three_performance_constraints(self):
        frame=pd.DataFrame([row(BASELINE,'fit_44'),row('bad_dd','fit_44',120,30,21),
                            row('good','fit_44',110,40,20)])
        self.assertEqual(choose(frame),('good','good'))

    def test_validation_failure_is_not_rescued_by_final_or_full_profit(self):
        frame=pd.DataFrame([row(BASELINE,p) for p in ('validation_15','later_7','full_66')]+
            [row('candidate','validation_15',99,40,18),row('candidate','later_7',200,40,18),
             row('candidate','full_66',300,40,18)])
        passed,reasons=promotion(frame,'candidate')
        self.assertFalse(passed)
        self.assertEqual(reasons,['validation_15: net profit below baseline'])

    def test_later_drawdown_or_losses_reject_promotion(self):
        frame=pd.DataFrame([row(BASELINE,p) for p in ('validation_15','later_7','full_66')]+
            [row('candidate','validation_15',110,40,18),row('candidate','later_7',110,51,21),
             row('candidate','full_66',110,40,18)])
        passed,reasons=promotion(frame,'candidate')
        self.assertFalse(passed)
        self.assertEqual(len(reasons),2)

    def test_passing_candidate_and_baseline_fallback(self):
        frame=pd.DataFrame([row(BASELINE,p) for p in ('validation_15','later_7','full_66')]+
            [row('candidate',p,110,40,18) for p in ('validation_15','later_7','full_66')])
        self.assertEqual(promotion(frame,'candidate'),(True,[]))
        self.assertFalse(promotion(frame,BASELINE)[0])


if __name__=='__main__':
    unittest.main()
