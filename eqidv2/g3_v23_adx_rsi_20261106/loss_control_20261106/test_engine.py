"""Native parity, prefix causality and next-bar loss-control execution tests."""
import unittest

import numpy as np

from g3_v23_adx_rsi_20261106.loss_control_20261106.engine import (
    BASELINE, CANDIDATES, FEATURE_KEYS, MINUTE_NS, build_features, replay)


def path(rows):
    values = np.asarray(rows, dtype=float)
    return dict(timestamp_ns=np.arange(len(rows), dtype=np.int64) * MINUTE_NS,
                **{key: values[:, i] for i, key in enumerate(('open', 'high', 'low', 'close'))})


def artificial_features(count, rsi=30., er=-.8, ema9=101., ema20=102., slope=-.1):
    values = dict(ema9=ema9, ema20=ema20, ema9_slope3=slope, rsi14=rsi, er10=er)
    return {key: np.repeat(value, count).astype(float) for key, value in values.items()}


class LossEngineTests(unittest.TestCase):
    def test_baseline_matches_original_native_engine(self):
        from fno_v13_v10_g_2_backtest import staged_exit
        cases = [path([(100, 102, 98, 100)]),
                 path([(100, 100.1, 99.9, 100), (98, 99, 97, 98)]),
                 path([(100, 100.1, 99.9, 100), (102, 103, 101.5, 102)]),
                 path([(100, 100.1, 99.9, 100)] * 125 + [(100, 100.1, 98.9, 99.8)])]
        for p in cases:
            for long in (True, False):
                old = staged_exit(p, 0, 100, long, .8)
                actual = replay(p, 0, 100, long, .8, {})
                self.assertEqual((actual['index'], actual['price'], actual['reason'], actual['event']),
                                 (old[0], old[1], old[2], old[4]))

    def test_feature_prefix_is_future_invariant_with_explicit_warmup(self):
        values = [100 + np.sin(i / 3) for i in range(40)]
        p = path([(x, x + .1, x - .1, x) for x in values])
        full = build_features(p)
        for n in (5, 14, 20, 31):
            prefix = build_features({key: value[:n] for key, value in p.items()})
            for key in FEATURE_KEYS:
                np.testing.assert_allclose(prefix[key], full[key][:n], atol=0, rtol=0, equal_nan=True)
        self.assertTrue(np.isnan(full['ema9'][:8]).all())
        self.assertTrue(np.isnan(full['ema20'][:19]).all())
        self.assertTrue(np.isnan(full['rsi14'][:14]).all())
        self.assertTrue(np.isnan(full['er10'][:10]).all())

    def test_flat_prefix_rsi_and_er_defined(self):
        f = build_features(path([(100, 100, 100, 100)] * 20))
        self.assertEqual(f['rsi14'][-1], 50)
        self.assertEqual(f['er10'][-1], 0)

    def test_recovery_guard_only_next_bar(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100, 99.1, 99.95),
                  (99.9, 100, 99.7, 99.8)])
        r = replay(p, 0, 100, True, 2, CANDIDATES['recovery_guard'])
        self.assertEqual((r['index'], r['reason'], r['event']), (2, 'RECOVERY_GUARD', 'INTRABAR'))
        self.assertAlmostEqual(r['price'], 99.75)

    def test_entry_bar_extrema_cannot_arm_recovery_guard(self):
        p = path([(100, 100.1, 99.1, 100), (100, 100.1, 99.6, 100)])
        r = replay(p, 0, 100, True, 2, CANDIDATES['recovery_guard'])
        self.assertEqual(r['reason'], 'TIME_EXIT_1515')

    def test_short_recovery_guard_and_adverse_gap(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.9, 99.9, 100.05),
                  (100.6, 100.8, 100.2, 100.3)])
        r = replay(p, 0, 100, False, 2, CANDIDATES['recovery_guard'])
        self.assertEqual((r['price'], r['event'], r['reason']), (100.6, 'OPEN', 'RECOVERY_GUARD'))

    def test_sustained_failure_requires_three_completed_bars(self):
        rows = [(100, 100.1, 99.9, 100)] * 20 + [(99.3, 99.4, 99.2, 99.3)] * 3
        rows += [(99.4, 103, 98, 100)]
        p = path(rows)
        r = replay(p, 0, 100, True, 2, CANDIDATES['sustained_loss_trend20'], artificial_features(len(rows)))
        self.assertEqual((r['index'], r['price'], r['event']), (23, 99.4, 'OPEN'))
        self.assertEqual(r['reason'], 'SUSTAINED_LOSS_TREND_EXIT')

    def test_conditional_protection_waits_for_trend_failure(self):
        p = path([(100, 100.1, 99.9, 100), (100.4, 100.7, 100.3, 100.5),
                  (100.5, 100.6, 100.2, 100.3), (100.2, 100.3, 100, 100.1)])
        f = artificial_features(4, rsi=60., slope=.1)
        f['rsi14'][2], f['ema9_slope3'][2] = 40., -.1
        r = replay(p, 0, 100, True, 2, CANDIDATES['trend_failure_profit_guard'], f)
        self.assertEqual((r['index'], r['reason']), (3, 'TREND_FAILURE_PROFIT_GUARD'))
        self.assertAlmostEqual(r['price'], 100.1)

    def test_entry_extreme_cannot_arm_conditional_profit_protection(self):
        p = path([(100, 100.8, 99.9, 100.3), (100.3, 100.4, 100.2, 100.3),
                  (100.2, 100.3, 100, 100.1)])
        r = replay(p, 0, 100, True, 2, CANDIDATES['trend_failure_profit_guard'], artificial_features(3))
        self.assertEqual(r['reason'], 'TIME_EXIT_1515')

    def test_guard_stop_first_when_target_also_touched(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100, 99.1, 99.95),
                  (100, 103, 99.7, 101)])
        r = replay(p, 0, 100, True, 2, CANDIDATES['recovery_guard'])
        self.assertTrue(r['ambiguous'])
        self.assertEqual(r['reason'], 'RECOVERY_GUARD')

    def test_guard_cannot_loosen_after_profit_guard(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.7, 99.1, 100.2),
                  (100.2, 100.3, 100, 100.1)])
        r = replay(p, 0, 100, True, 2, CANDIDATES['recovery_and_profit_guard'], artificial_features(3))
        self.assertEqual(r['reason'], 'TREND_FAILURE_PROFIT_GUARD')
        self.assertAlmostEqual(r['price'], 100.1)

    def test_future_tail_does_not_change_prior_exit(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100, 99.1, 99.95),
                  (99.9, 100, 99.7, 99.8), (100, 105, 95, 100)])
        before = replay(p, 0, 100, True, 2, CANDIDATES['recovery_guard'])
        p['close'][-1], p['high'][-1], p['low'][-1] = 190, 200, 50
        self.assertEqual(before, replay(p, 0, 100, True, 2, CANDIDATES['recovery_guard']))


if __name__ == '__main__':
    unittest.main()
