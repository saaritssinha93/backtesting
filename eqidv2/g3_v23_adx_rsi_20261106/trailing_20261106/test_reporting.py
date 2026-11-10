"""Reconciliation and benchmark-adoption tests for trailing reports."""
from pathlib import Path
import re
import tempfile
import unittest

import pandas as pd

from g3_v23_adx_rsi_20261106.trailing_20261106.reporting import (
    BASELINE, compare_days, summarize_days, write_reports)


def fixture():
    days = pd.bdate_range(end='2026-11-05', periods=66).strftime('%Y-%m-%d').tolist()
    daily, changes = [], []
    for name in (BASELINE, 'hybrid_runner'):
        sid = 0
        for index, day in enumerate(days):
            n = 0 if index == 0 else (1 if index % 2 else 9)
            wins = int(n > 0)
            daily.append(dict(policy=name, day=day, trades=n, wins=wins, losses=n-wins,
                win_rate_pct=100*wins/n if n else 0, net_pnl_rs=100*wins-10*(n-wins),
                gross_loss_rs=10*(n-wins)))
            for j in range(n):
                value = 100 if j == 0 else -10
                changes.append(dict(policy=name, day=day, sid=sid, baseline_pnl_rs=value,
                    candidate_pnl_rs=value, pnl_change_rs=0, exit_changed=False,
                    activation_count=0, stop_update_count=0, premature_exit_then_target=False,
                    target_mode='native' if name == BASELINE else 'runner'))
                sid += 1
    daily, changes = pd.DataFrame(daily), pd.DataFrame(changes)
    overall = pd.DataFrame([dict(policy=name, period='full_66', **summarize_days(part))
                           for name, part in daily.groupby('policy')])
    config = dict(baseline_policy=BASELINE, selected_policy=BASELINE, training_candidate=BASELINE,
        comparison_candidate='hybrid_runner', promotion_pass=False, user_authorized_baseline_change=True,
        historydays=days, rejection_reasons=['No fitting-period candidate qualified'])
    return overall, daily, changes, config


class TrailingReportingTests(unittest.TestCase):
    def test_adopted_baseline_does_not_require_trailing_promotion(self):
        overall, daily, changes, config = fixture()
        with tempfile.TemporaryDirectory() as folder:
            write_reports(folder, overall, daily, changes, config)
            path = Path(folder)
            report = (path/'REPORT.md').read_text(encoding='utf-8')
            self.assertIn('fixed 1.50% SL is now the V23 backtesting baseline', report)
            self.assertIn('No trailing alternative qualified', report)
            self.assertNotIn('baseline_v21_g7', report)
            self.assertNotIn('baseline already uses the entry price', report)
            self.assertIn('Target-removal control', report)
            self.assertIn('combined experiment', report)
            table = (path/'DAY_WISE_RESULTS.md').read_text(encoding='utf-8')
            self.assertEqual(len(re.findall(r'^\| 2026-', table, re.M)), 66)
            self.assertIn('**20.25%**', table)
            self.assertEqual(pd.read_csv(path/'day_wise_results.csv').trades_v23.iloc[0], 0)
            self.assertTrue((path/'diagnostics.csv').is_file())

    def test_old_baseline_cannot_be_used_as_benchmark(self):
        overall, daily, changes, config = fixture()
        config['baseline_policy'] = 'baseline_v21_g7'
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'user-adopted'):
            write_reports(folder, overall, daily, changes, config)

    def test_daily_trade_redistribution_rejected(self):
        _, daily, _, _ = fixture()
        base = daily.loc[daily.policy.eq(BASELINE)].reset_index(drop=True)
        other = base.copy()
        other.loc[1, ['trades','losses']] += 1
        other.loc[2, ['trades','losses']] -= 1
        other['win_rate_pct'] = other.wins.div(other.trades).fillna(0)*100
        with self.assertRaisesRegex(ValueError, 'daily trade count'):
            compare_days(base, other)

    def test_failed_promotion_does_not_activate_hybrid(self):
        overall, daily, changes, config = fixture()
        config['selected_policy'] = 'hybrid_runner'
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'Failed promotion'):
            write_reports(folder, overall, daily, changes, config)

    def test_aggregate_error_rejected(self):
        overall, daily, changes, config = fixture()
        overall.loc[overall.policy.eq('hybrid_runner'), 'net_pnl_rs'] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'reconciliation'):
            write_reports(folder, overall, daily, changes, config)

    def test_wrong_baseline_outcomes_rejected(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.eq('hybrid_runner'), 'baseline_pnl_rs'] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'different baseline'):
            write_reports(folder, overall, daily, changes, config)

    def test_same_totals_with_changed_trade_identity_rejected(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.eq('hybrid_runner').idxmax(), 'sid'] = 999999
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'identities changed'):
            write_reports(folder, overall, daily, changes, config)

    def test_unchanged_net_with_wrong_loss_distribution_rejected(self):
        overall, daily, changes, config = fixture()
        part = changes.loc[changes.policy.eq('hybrid_runner')]
        win = part.loc[part.candidate_pnl_rs.gt(0)].index[0]
        loss = part.loc[part.candidate_pnl_rs.lt(0)].index[0]
        changes.loc[win, ['candidate_pnl_rs','pnl_change_rs']] = [-20,-120]
        changes.loc[loss, ['candidate_pnl_rs','pnl_change_rs']] = [110,120]
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'distribution'):
            write_reports(folder, overall, daily, changes, config)

    def test_mismatched_history_rejected(self):
        overall, daily, changes, config = fixture()
        config['historydays'] = config['historydays'][:-1]
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'declared inventory'):
            write_reports(folder, overall, daily, changes, config)


if __name__ == '__main__':
    unittest.main()
