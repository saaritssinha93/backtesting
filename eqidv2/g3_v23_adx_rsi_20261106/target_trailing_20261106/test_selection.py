import unittest
import pandas as pd

from .run import BASELINE, choose, qualification


def row(policy, period, net=100., loss=50., dd=20., trades=10):
    return dict(policy=policy, period=period, net_pnl_rs=net,
                gross_loss_rs=loss, daily_close_drawdown_rs=dd, trades=trades)


class TargetTrailingSelectionTests(unittest.TestCase):
    def test_later_profit_cannot_make_failed_fit_eligible(self):
        rows = [row(BASELINE, 'fit_44'), row('bad', 'fit_44', net=99, loss=40),
                row('bad', 'later_7', net=999999, loss=0, dd=0)]
        self.assertEqual(choose(pd.DataFrame(rows)), (BASELINE, 'bad'))

    def test_fit_prefers_profit_only_among_loss_and_drawdown_eligible_arms(self):
        rows = [row(BASELINE, 'fit_44'), row('good', 'fit_44', 105, 40, 19),
                row('more_profit_worse_dd', 'fit_44', 120, 30, 25),
                row('more_profit_more_loss', 'fit_44', 130, 60, 18)]
        self.assertEqual(choose(pd.DataFrame(rows)), ('good', 'good'))

    def test_separate_period_checks_cannot_be_hidden_by_full_profit(self):
        periods = ('validation_15', 'later_7', 'full_66')
        rows = [row(BASELINE, p) for p in periods] + [row('candidate', p, 105, 40, 19) for p in periods]
        frame = pd.DataFrame(rows)
        self.assertTrue(qualification(frame, 'candidate')[0])
        frame.loc[frame.policy.eq('candidate') & frame.period.eq('later_7'), 'gross_loss_rs'] = 51
        self.assertEqual(qualification(frame, 'candidate'), (False, ['later_7: loss improvement requirement failed']))

    def test_unchanged_losses_do_not_count_as_full_improvement(self):
        periods = ('validation_15', 'later_7', 'full_66')
        rows = [row(BASELINE, p) for p in periods] + [row('candidate', p, 105, 50, 19) for p in periods]
        self.assertFalse(qualification(pd.DataFrame(rows), 'candidate')[0])


if __name__ == '__main__':
    unittest.main()
