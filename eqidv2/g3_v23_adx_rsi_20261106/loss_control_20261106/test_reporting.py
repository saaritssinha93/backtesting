"""Reports must retain every day and distinguish exploratory from selected rules."""
from pathlib import Path
import re
import tempfile
import unittest

import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106.reporting import (
    BASELINE, compare_days, summarize_days, write_reports)


def fixture():
    days = pd.bdate_range(end='2026-11-05', periods=66).strftime('%Y-%m-%d').tolist()
    daily, changes = [], []
    for policy in (BASELINE, 'candidate'):
        sid = 0
        for i, day in enumerate(days):
            trades = 0 if i == 0 else (1 if i % 2 else 9)
            wins = int(trades > 0)
            pnl = 100 * wins - 10 * (trades-wins)
            daily.append(dict(policy=policy, day=day, trades=trades, wins=wins,
                losses=trades-wins, win_rate_pct=100*wins/trades if trades else 0,
                net_pnl_rs=pnl, gross_loss_rs=10*(trades-wins)))
            for j in range(trades):
                value = 100 if j == 0 else -10
                changes.append(dict(policy=policy, day=day, sid=sid,
                    baseline_pnl_rs=value, candidate_pnl_rs=value))
                sid += 1
    daily, changes = pd.DataFrame(daily), pd.DataFrame(changes)
    overall = pd.DataFrame([dict(policy=name, period='full_66', **summarize_days(part))
                            for name, part in daily.groupby('policy')])
    config = dict(selected_policy=BASELINE, training_candidate=BASELINE,
                  comparison_candidate='candidate', promotion_pass=False,
                  rejection_reasons=['No earlier-period candidate met all requirements'], historydays=days)
    return overall, daily, changes, config


class ReportingTests(unittest.TestCase):
    def test_full_history_zero_day_and_no_false_promotion(self):
        overall, daily, changes, config = fixture()
        with tempfile.TemporaryDirectory() as folder:
            write_reports(folder, overall, daily, changes, config)
            output = Path(folder)
            table = (output/'DAY_WISE_RESULTS.md').read_text(encoding='utf-8')
            self.assertEqual(len(re.findall(r'^\| 2026-', table, re.M)), 66)
            self.assertIn('**20.25%**', table)
            rows = pd.read_csv(output/'day_wise_results.csv')
            self.assertEqual(rows.trades_v23.iloc[0], 0)
            report = (output/'REPORT.md').read_text(encoding='utf-8')
            self.assertIn('No loss-control alternative qualified', report)
            self.assertNotIn('Qualified retrospective candidate:', report)
            candidate = (output/'CANDIDATE_DAY_WISE_RESULTS.md').read_text(encoding='utf-8')
            self.assertIn('Exploratory comparison only', candidate)

    def test_same_total_different_daily_count_is_rejected(self):
        _, daily, _, _ = fixture()
        baseline = daily.loc[daily.policy.eq(BASELINE)].reset_index(drop=True)
        changed = baseline.copy()
        changed.loc[1, ['trades','losses']] += 1
        changed.loc[2, ['trades','losses']] -= 1
        changed['win_rate_pct'] = changed.wins.div(changed.trades).fillna(0)*100
        with self.assertRaisesRegex(ValueError, 'daily trade count'):
            compare_days(baseline, changed)

    def test_total_mismatch_is_rejected(self):
        overall, daily, changes, config = fixture()
        overall.loc[overall.policy.eq('candidate'), 'net_pnl_rs'] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'reconciliation'):
            write_reports(folder, overall, daily, changes, config)

    def test_failed_promotion_cannot_select_candidate(self):
        overall, daily, changes, config = fixture()
        config['selected_policy'] = 'candidate'
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'Failed promotion'):
            write_reports(folder, overall, daily, changes, config)

    def test_missing_declared_day_is_rejected(self):
        overall, daily, changes, config = fixture()
        config['historydays'] = config['historydays'][:-1]
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'declared history'):
            write_reports(folder, overall, daily, changes, config)

    def test_trade_effect_mismatch_is_rejected(self):
        overall, daily, changes, config = fixture()
        changes.loc[changes.policy.eq('candidate'), 'candidate_pnl_rs'] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'does not reconcile'):
            write_reports(folder, overall, daily, changes, config)


if __name__ == '__main__':
    unittest.main()
