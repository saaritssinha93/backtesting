import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from . import reporting as r


def fixtures():
    days = ['2026-11-05', '2026-11-06', '2026-11-09']
    mover_rows = []
    for day in days:
        for symbol, side, extreme, qualifies in [('A', 'LONG', 104., True),
            ('B', 'SHORT', 96., True), ('C', 'LONG', 102., False)]:
            mover_rows.append(dict(day=day, tradingsymbol=symbol, side=side, top2=qualifies,
                top5=qualifies, move_pct=extreme-100, first_open=100., extreme_price=extreme,
                first3pct_ts=day+'T10:00:00+05:30' if qualifies else None,
                extreme_ts=day+'T11:00:00+05:30'))
    movers = pd.DataFrame(mover_rows)
    rows = [dict(sid=1, day=days[0], tradingsymbol='A', side='LONG', entry_price=101.,
                 entry_ts=days[0]+'T09:40:00+05:30', portfolio_net_profit_rupees=1000.),
            dict(sid=2, day=days[0], tradingsymbol='A', side='LONG', entry_price=103.9,
                 entry_ts=days[0]+'T10:50:00+05:30', portfolio_net_profit_rupees=-500.),
            dict(sid=3, day=days[0], tradingsymbol='B', side='SHORT', entry_price=97.,
                 entry_ts=days[0]+'T10:00:00+05:30', portfolio_net_profit_rupees=200.),
            dict(sid=4, day=days[1], tradingsymbol='C', side='LONG', entry_price=101.,
                 entry_ts=days[1]+'T10:10:00+05:30', portfolio_net_profit_rupees=600.),
            dict(sid=5, day=days[1], tradingsymbol='B', side='SHORT', entry_price=97.,
                 entry_ts=days[1]+'T11:01:00+05:30', portfolio_net_profit_rupees=-400.)]
    book = pd.DataFrame(rows)
    orders = book[['sid', *r.KEY]].copy()
    return days, book, orders, movers


def prepared():
    days, book, orders, movers = fixtures()
    books, orders, movers = r.validate({r.BASELINE: book}, {r.BASELINE: orders}, movers, days)
    return days, books[r.BASELINE], orders[r.BASELINE], movers


class ReportingTests(unittest.TestCase):
    def test_unique_case_precision_and_coverage_not_repeated_trades(self):
        days, book, _, movers = prepared()
        audit = r.annotate_trades(book, movers)
        result = r.mover_metrics(audit, movers, days)
        self.assertEqual(result['traded_stock_day_side_cases'], 4)
        self.assertEqual(result['top5_cases'], 6)
        self.assertEqual(result['top5_captured_cases'], 3)
        self.assertEqual(result['top5_precision_pct'], 75.)
        self.assertEqual(result['top5_capture_pct'], 50.)
        self.assertEqual(result['top5_useful_cases'], 2)
        self.assertEqual(result['top5_before3_cases'], 1)

    def test_actual_fill_timing_and_long_short_room(self):
        _, book, _, movers = prepared()
        audit = r.annotate_trades(book, movers).set_index('sid')
        self.assertAlmostEqual(audit.loc[1, 'distance_to_day_extreme_pct'], 100*(104/101-1))
        self.assertAlmostEqual(audit.loc[3, 'distance_to_day_extreme_pct'], 100*(1-96/97))
        self.assertFalse(audit.loc[3, 'entry_before_3pct'])  # equality is conservative
        self.assertTrue(audit.loc[2, 'retrospective_near_extreme'])
        self.assertTrue(audit.loc[5, 'retrospective_near_extreme'])
        self.assertFalse(audit.loc[5, 'useful_capture'])
        self.assertTrue(np.isnan(audit.loc[5, 'remaining_to_final_extreme_pct']))

    def test_known_next_open_and_same_bar_crossing_are_ordered_correctly(self):
        _, book, _, movers = prepared()
        new = book.iloc[[0]].copy()
        new['entry_ts'] = pd.to_datetime(['2026-11-05T10:00:00+05:30'], utc=True)
        new['entry_bar_end_ts'] = new.entry_ts
        new['entry_execution_ts'] = new.entry_ts-pd.Timedelta(minutes=1)
        new['raw_entry_open'] = 102.
        context = movers.copy()
        mask = context.day.eq('2026-11-05') & context.tradingsymbol.eq('A')
        context.loc[mask, 'extreme_ts'] = pd.Timestamp('2026-11-05T10:00:00+05:30')
        audit = r.annotate_trades(new, context).iloc[0]
        self.assertTrue(audit.entry_before_3pct)
        self.assertTrue(audit.entry_before_final_extreme)
        self.assertFalse(audit.entry_before_3pct_conservative)
        self.assertFalse(audit.entry_before_final_extreme_conservative)
        self.assertTrue(audit.useful_capture)
        self.assertFalse(audit.useful_capture_conservative)
        self.assertEqual(audit.entry_timing_ts, new.entry_execution_ts.iloc[0])
        new['raw_entry_open'] = 103.5
        audit = r.annotate_trades(new, context).iloc[0]
        self.assertFalse(audit.entry_before_3pct)
        self.assertTrue(audit.entry_before_final_extreme)
        new['raw_entry_open'] = 104.
        audit = r.annotate_trades(new, context).iloc[0]
        self.assertFalse(audit.entry_before_final_extreme)
        self.assertTrue(audit.retrospective_near_extreme)

    def test_known_short_open_already_past_threshold_is_not_early(self):
        _, book, _, movers = prepared()
        new = book.iloc[[2]].copy()
        new['entry_bar_end_ts'] = new.entry_ts
        new['entry_execution_ts'] = new.entry_ts-pd.Timedelta(minutes=1)
        new['raw_entry_open'] = 98.
        self.assertTrue(r.annotate_trades(new, movers).iloc[0].entry_before_3pct)
        new['raw_entry_open'] = 96.5
        self.assertFalse(r.annotate_trades(new, movers).iloc[0].entry_before_3pct)

    def test_non_top_mover_winners_are_not_treated_as_bad_entries(self):
        days, book, _, movers = prepared()
        result = r.mover_metrics(r.annotate_trades(book, movers), movers, days)
        self.assertEqual(result['non_top5_trades'], 1)
        self.assertEqual(result['non_top5_winners'], 1)
        self.assertEqual(result['non_top5_net_pnl_rs'], 600.)
        self.assertEqual(result['non_top5_winning_pnl_rs'], 600.)

    def test_validate_identity_and_hindsight_separation(self):
        days, book, orders, movers = fixtures()
        wrong = orders.copy(); wrong.loc[0, 'tradingsymbol'] = 'BAD'
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            r.validate({r.BASELINE: book}, {r.BASELINE: wrong}, movers, days)
        _, valid_book, _, valid_movers = prepared()
        with self.assertRaisesRegex(ValueError, 'already contains hindsight'):
            r.annotate_trades(valid_book.assign(top5=True), valid_movers)
        with self.assertRaisesRegex(ValueError, 'missing full-universe'):
            r.annotate_trades(valid_book, valid_movers.loc[valid_movers.tradingsymbol.ne('A')])

    def test_invalid_flags_and_duplicate_movers_rejected(self):
        days, book, orders, movers = fixtures()
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            r.validate({r.BASELINE: book}, {r.BASELINE: orders}, pd.concat([movers, movers.iloc[:1]]), days)
        bad = movers.copy(); bad['top5'] = bad.top5.astype(object); bad.loc[0, 'top5'] = 'maybe'
        with self.assertRaisesRegex(ValueError, 'Invalid boolean'):
            r.validate({r.BASELINE: book}, {r.BASELINE: orders}, bad, days)

    def test_empty_trade_group_preserves_zero_trade_dates(self):
        days, book, _, movers = prepared()
        empty = r.annotate_trades(book.iloc[:0], movers)
        result = r.mover_metrics(empty, movers, days)
        self.assertEqual(result['top5_cases'], 6)
        self.assertEqual(result['top5_captured_cases'], 0)
        self.assertIsNone(result['top5_precision_pct'])
        measured = r.metric(book, len(book), days)
        self.assertEqual(measured['sessions'], 3)
        self.assertEqual(measured['flat_days'], 1)
        self.assertEqual(measured['net_pnl_rs'], 900.)

    def test_publish_reconciliation_catalogue_and_no_overwrite(self):
        days, book, orders, movers = fixtures()
        strategy = book.iloc[:1].copy()
        arm = 'leader_breakout_080'
        with tempfile.TemporaryDirectory() as folder:
            summary = r.publish(folder, {r.BASELINE: book, arm: strategy},
                {r.BASELINE: orders, arm: orders.iloc[:1]}, {}, movers, days,
                {'execution_authority': False}, {'validation_chosen': None, 'exploratory_best': arm})
            self.assertEqual(summary['daily_reconciliation'], 'PASS')
            self.assertEqual(summary['context_partition_reconciliation'], 'PASS')
            self.assertFalse(summary['automatic_promotion'])
            self.assertEqual(summary['displayed_comparison_arm'], arm)
            daily = pd.read_csv(Path(folder)/'daily.csv')
            self.assertEqual(len(daily), 6)
            cases = pd.read_csv(Path(folder)/'top5_daily_cases.csv')
            one = cases.loc[cases.day.eq(days[0]) & cases.tradingsymbol.eq('A')].iloc[0]
            self.assertEqual(one.baseline_fills, 2)
            self.assertEqual(one.baseline_net_pnl_rs, 500.)
            self.assertEqual(one.comparison_fills, 1)
            text = (Path(folder)/'FULL_HISTORY_DAILY_TOP5_MOVERS.md').read_text()
            self.assertIn('09:40', text)
            self.assertIn('2026-11-09', text)
            with self.assertRaisesRegex(ValueError, 'already exist'):
                r.publish(folder, {r.BASELINE: book}, {r.BASELINE: orders}, {}, movers, days, {}, {})

    def test_later_performance_does_not_choose_arm(self):
        days, book, orders, movers = fixtures()
        huge = book.copy(); huge[r.PNL] = 1e9
        with tempfile.TemporaryDirectory() as folder:
            summary = r.publish(folder, {r.BASELINE: book, 'huge': huge},
                {r.BASELINE: orders, 'huge': orders}, {}, movers, days, {},
                {'validation_chosen': None, 'exploratory_best': None})
            self.assertEqual(summary['displayed_comparison_arm'], r.BASELINE)
            self.assertIsNone(summary['selection']['validation_chosen'])


if __name__ == '__main__':
    unittest.main()
