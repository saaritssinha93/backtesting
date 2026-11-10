"""Causality, conservative execution, parity and fixed selection tests."""
import unittest

import numpy as np
import pandas as pd

from .run import BASELINE, CANDIDATES, MINUTE_NS, choose, replay, verify_path


def path(rows):
    values = np.array(rows, dtype=float)
    return dict(timestamp_ns=np.arange(len(rows), dtype=np.int64) * MINUTE_NS,
                **{key: values[:, i] for i, key in enumerate(('open', 'high', 'low', 'close'))})


class ExitStudyTests(unittest.TestCase):
    def test_protection_only_next_bar(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.7, 99.5, 100.5),
                  (100.4, 100.5, 100.0, 100.2)])
        r = replay(p, 0, 100, True, 2, False, CANDIDATES['protect_mfe0p6_lock0p1'])
        self.assertEqual(r['index'], 2)
        self.assertAlmostEqual(r['price'], 100.1)
        self.assertEqual(r['reason'], 'PROFIT_PROTECTION')

    def test_entry_extreme_does_not_activate_protection(self):
        p = path([(100, 100.8, 99.9, 100), (100, 100.1, 99.8, 100)])
        r = replay(p, 0, 100, True, 2, False, CANDIDATES['protect_mfe0p6_lock0p1'])
        self.assertEqual(r['reason'], 'TIME_EXIT_1515')

    def test_gap_is_filled_at_adverse_open(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.7, 99.9, 100.6),
                  (99.7, 100.4, 99.5, 100.2)])
        r = replay(p, 0, 100, True, 2, False, CANDIDATES['protect_mfe0p6_lock0p1'])
        self.assertEqual(r['event'], 'OPEN')
        self.assertEqual(r['price'], 99.7)

    def test_weak_stop_cannot_retroactively_apply_to_entry_bar(self):
        p = path([(100, 100.1, 98.9, 100), (100, 100.1, 98.9, 99)])
        r = replay(p, 0, 100, True, 2, True, CANDIDATES['weak_stop_1p0'])
        self.assertEqual(r['index'], 1)
        self.assertEqual(r['reason'], 'WEAK_ENTRY_STOP')
        self.assertEqual(r['price'], 99)

    def test_scheduled_market_exit_precedes_later_bar_extremes(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100, 99.5, 99.6),
                  (99.6, 99.8, 99.3, 99.5), (99.7, 103, 98, 99)])
        r = replay(p, 0, 100, True, 2, True, {'weak_loss_minutes': 2, 'weak_loss_pct': .35})
        self.assertEqual(r['index'], 3)
        self.assertEqual(r['event'], 'OPEN')
        self.assertEqual(r['price'], 99.7)

    def test_short_protection_and_stop_first(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.2, 99.3, 99.5),
                  (99.4, 100.2, 97.5, 99)])
        r = replay(p, 0, 100, False, 2, False, CANDIDATES['protect_mfe0p6_lock0p1'])
        self.assertAlmostEqual(r['price'], 99.9)
        self.assertTrue(r['ambiguous'])

    def test_future_after_exit_cannot_change_result(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.7, 99.9, 100.6),
                  (100.3, 100.4, 100, 100.2), (100, 103, 90, 95)])
        settings = CANDIDATES['protect_mfe0p6_lock0p1']
        before = replay(p, 0, 100, True, 2, False, settings)
        p['high'][-1], p['low'][-1], p['close'][-1] = 200, 2, 150
        self.assertEqual(before, replay(p, 0, 100, True, 2, False, settings))

    def test_native_engine_parity(self):
        from fno_v13_v10_g_2_backtest import staged_exit
        scenarios = [path([(100, 102, 98, 100)]),
                     path([(100, 100.1, 99.9, 100), (98, 99, 97, 98)]),
                     path([(100, 100.1, 99.9, 100), (100, 101, 99.9, 100.8)]),
                     path([(100, 100.1, 99.9, 100)] * 130)]
        for p in scenarios:
            for long in (True, False):
                expected = staged_exit(p, 0, 100, long, .8)
                r = replay(p, 0, 100, long, .8, True, {})
                self.assertEqual((r['index'], r['price'], r['reason'], r['event']),
                                 (expected[0], expected[1], expected[2], expected[4]))

    def test_missing_minute_rejected(self):
        p = path([(100, 100, 100, 100)] * 3)
        p['timestamp_ns'][-1] += MINUTE_NS
        with self.assertRaises(ValueError):
            verify_path(p, 0)

    def test_selection_only_early_and_requires_all_conditions(self):
        rows = [dict(period='early_59', policy=BASELINE, trades=10, net_pnl_rs=100,
                     gross_loss_rs=50, daily_close_drawdown_rs=30),
                dict(period='early_59', policy='bad_profit', trades=10, net_pnl_rs=99,
                     gross_loss_rs=40, daily_close_drawdown_rs=20),
                dict(period='later_6', policy='bad_profit', trades=10, net_pnl_rs=99999,
                     gross_loss_rs=0, daily_close_drawdown_rs=0)]
        self.assertEqual(choose(pd.DataFrame(rows)), BASELINE)
        rows.append(dict(period='early_59', policy='good', trades=10, net_pnl_rs=101,
                         gross_loss_rs=40, daily_close_drawdown_rs=20))
        self.assertEqual(choose(pd.DataFrame(rows)), 'good')


if __name__ == '__main__':
    unittest.main()
