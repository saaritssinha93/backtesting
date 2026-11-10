"""Protect entry, payoff and fixed-risk/native-target reporting contracts."""
from pathlib import Path
import re
import tempfile
import unittest

from g3_v23_adx_rsi_20261106.trailing_20261106.test_reporting import fixture as prior_fixture
from .reporting import BASELINE, write_reports


def fixture():
    overall, daily, changes, config = prior_fixture()
    for frame in (overall, daily, changes):
        frame['policy'] = frame.policy.replace({'hybrid_runner':'cushion_half_target'})
    changes['target_mode'] = 'native'
    changes['initial_risk_pct'] = 1.50
    config['comparison_candidate'] = 'cushion_half_target'
    for name, part in changes.groupby('policy'):
        overall.loc[overall.policy.eq(name), 'average_trade_rs'] = part.candidate_pnl_rs.mean()
        overall.loc[overall.policy.eq(name), 'median_trade_rs'] = part.candidate_pnl_rs.median()
    return overall, daily, changes, config


class TargetTrailingReportingTests(unittest.TestCase):
    def write(self, overall, daily, changes, config):
        with tempfile.TemporaryDirectory() as folder:
            write_reports(folder, overall, daily, changes, config)
            return ((Path(folder)/'REPORT.md').read_text(encoding='utf-8'),
                    (Path(folder)/'DAY_WISE_RESULTS.md').read_text(encoding='utf-8'))

    def test_all_days_weighted_rates_and_native_target_explanation(self):
        report, table = self.write(*fixture())
        self.assertEqual(len(re.findall(r'^\| 2026-', table, re.M)), 66)
        self.assertIn('**20.25%**', table)
        self.assertIn('Every alternative retains the original profit target', report)
        self.assertIn('No early-trailing alternative qualified', report)
        self.assertIn('All 66 sessions were already observed', report)
        self.assertNotIn('Target-removal control', report)
        self.assertIn('Mean net/trade', report)
        self.assertIn('Median net/trade', report)
        self.assertIn('cost_stress.csv', report)

    def test_runner_target_rejected(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.ne(BASELINE), 'target_mode'] = 'runner'
        with self.assertRaisesRegex(ValueError, 'native profit target'):
            self.write(overall, daily, changes, config)

    def test_adaptive_initial_risk_rejected(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.ne(BASELINE), 'initial_risk_pct'] = .75
        with self.assertRaisesRegex(ValueError, 'fixed1.50% risk'):
            self.write(overall, daily, changes, config)

    def test_failed_selection_cannot_activate_candidate(self):
        overall, daily, changes, config = fixture()
        config['selected_policy'] = 'cushion_half_target'
        with self.assertRaisesRegex(ValueError, 'Failed promotion'):
            self.write(overall, daily, changes, config)

    def test_missing_history_date_rejected(self):
        overall, daily, changes, config = fixture()
        config['historydays'] = config['historydays'][:-1]
        with self.assertRaisesRegex(ValueError, 'declared inventory'):
            self.write(overall, daily, changes, config)

    def test_identical_totals_do_not_allow_different_entries(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.ne(BASELINE).idxmax(), 'sid'] = 999999
        with self.assertRaisesRegex(ValueError, 'identities changed'):
            self.write(overall, daily, changes, config)

    def test_mean_and_median_reconcile_to_trade_payoffs(self):
        for field in ('average_trade_rs', 'median_trade_rs'):
            overall, daily, changes, config = fixture()
            overall.loc[overall.policy.ne(BASELINE), field] += 1
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'payoff reconciliation'):
                self.write(overall, daily, changes, config)

    def test_loss_distribution_error_rejected_even_with_same_net(self):
        overall, daily, changes, config = fixture()
        part = changes.loc[changes.policy.ne(BASELINE)]
        win = part.loc[part.candidate_pnl_rs.gt(0)].index[0]
        loss = part.loc[part.candidate_pnl_rs.lt(0)].index[0]
        changes.loc[win, ['candidate_pnl_rs', 'pnl_change_rs']] = [-20, -120]
        changes.loc[loss, ['candidate_pnl_rs', 'pnl_change_rs']] = [110, 120]
        with self.assertRaisesRegex(ValueError, 'distribution'):
            self.write(overall, daily, changes, config)


if __name__ == '__main__':
    unittest.main()
