"""Reports must preserve daily inventory and reconcile with aggregate results."""
import re
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from g3_v23_adx_rsi_20261106.through_20261105.reporting import (
    ARCHIVED, BASELINE, PROTECTION, _summary, compare_days, write_reports)


def fixture():
    days = pd.bdate_range(end='2026-11-05', periods=66).strftime('%Y-%m-%d').tolist()
    rows = []
    for policy in (BASELINE, PROTECTION, ARCHIVED):
        for i, day in enumerate(days):
            trades = 0 if i == 0 else (1 if i % 2 else 9)
            wins = int(trades > 0)
            rows.append(dict(policy=policy, day=day, selected_orders=trades,
                trades=trades, wins=wins, losses=trades-wins,
                win_rate_pct=100*wins/trades if trades else 0,
                net_pnl_rs=100*wins-10*(trades-wins), gross_loss_rs=10*(trades-wins)))
    daily = pd.DataFrame(rows)
    overall = pd.DataFrame([dict(policy=name, period='full_66', **_summary(frame))
                            for name, frame in daily.groupby('policy')])
    config = dict(selected_exit_policy=BASELINE, history_cutoff='2026-11-05',
                  coverage_notes=['218 complete cash-minute stocks; source exclusions are retained.'])
    return overall, daily, config


class ReportingTests(unittest.TestCase):
    def test_dynamic_complete_history_zero_day_and_weighted_total(self):
        overall, daily, config = fixture()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            write_reports(output, overall, daily, config)
            text = (output/'DAY_WISE_RESULTS.md').read_text(encoding='utf-8')
            self.assertEqual(len(re.findall(r'^\| 2026-', text, re.M)), 66)
            self.assertIn('through 2026-11-05', text)
            self.assertIn('66 sessions', text)
            self.assertIn('**20.25%**', text)
            actual = pd.read_csv(output/'day_wise_results.csv')
            self.assertEqual(len(actual), 66)
            self.assertEqual(actual.trades_v23.iloc[0], 0)
            report = (output/'REPORT.md').read_text(encoding='utf-8')
            self.assertIn('Previous 65 sessions', report)
            self.assertIn('Added full session: 2026-11-05', report)
            self.assertIn('218 complete cash-minute stocks', report)

    def test_reject_changed_daily_counts_even_if_total_same(self):
        _, daily, _ = fixture()
        baseline = daily.loc[daily.policy.eq(BASELINE)].copy()
        revised = baseline.copy()
        revised.loc[1, 'trades'] += 1
        revised.loc[2, 'trades'] -= 1
        revised.loc[2, 'losses'] -= 1
        with self.assertRaisesRegex(ValueError, 'daily trade count'):
            compare_days(baseline, revised)

    def test_missing_latest_day_is_rejected(self):
        overall, daily, config = fixture()
        daily = daily.loc[~(daily.policy.eq(PROTECTION) & daily.day.eq('2026-11-05'))]
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'every session'):
            write_reports(Path(folder), overall, daily, config)

    def test_wrong_full_total_is_rejected(self):
        overall, daily, config = fixture()
        overall.loc[overall.policy.eq(PROTECTION), 'net_pnl_rs'] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'reconciliation'):
            write_reports(Path(folder), overall, daily, config)

    def test_cutoff_cannot_claim_unincluded_day(self):
        overall, daily, config = fixture()
        config['history_cutoff'] = '2026-11-06'
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'cutoff'):
            write_reports(Path(folder), overall, daily, config)


if __name__ == '__main__':
    unittest.main()
