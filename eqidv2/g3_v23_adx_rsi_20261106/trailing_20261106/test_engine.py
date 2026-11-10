"""Causality, execution and counterfactual-target checks for trailing rules."""
import unittest

import numpy as np

from g3_v23_adx_rsi_20261106.trailing_20261106.engine import (
    BASELINE, CANDIDATES, FEATURE_KEYS, MINUTE_NS, build_features, replay)


def path(rows):
    values = np.asarray(rows, dtype=float)
    return dict(timestamp_ns=np.arange(len(rows), dtype=np.int64) * MINUTE_NS,
                open=values[:, 0], high=values[:, 1], low=values[:, 2], close=values[:, 3])


def feature_fixture(p):
    return {key: np.full(len(p['close']), np.nan) for key in FEATURE_KEYS}


def run(p, name, long=True, target=.8, atr=.1, override=None, features=None, entry_index=0):
    return replay(p, entry_index, 100., long, target, CANDIDATES[name], atr,
                  initial_override=override, features=features)


class TrailingTests(unittest.TestCase):
    def test_native_constant1p50_parity_long_short_and_nonzero_entry(self):
        from g3_v23_adx_rsi_20261106.entry_sl_20261106.recovery_room_20261106 import engine as old
        scenarios = [path([(100, 102, 98, 100)]),
                     path([(100, 100.1, 99.9, 100), (98, 99, 97, 98)]),
                     path([(100, 100.1, 99.9, 100), (102, 103, 101, 102)]),
                     path([(100, 100.1, 99.9, 100)] * 140)]
        for p in scenarios:
            for long in (True, False):
                for entry_index in {0, len(p['close']) - 1}:
                    expected = old.replay(p, entry_index, 100, long, .8,
                                          old.CANDIDATES['constant_1p50'])
                    actual = run(p, BASELINE, long=long, entry_index=entry_index)
                    for field in ('index', 'price', 'reason', 'event', 'ambiguous'):
                        self.assertEqual(actual[field], expected[field])
                    self.assertEqual(actual['stop_updates'], [])

    def test_runner_disables_original_target_from_entry(self):
        p = path([(100, 101, 99.8, 100.9), (100.9, 102.2, 100.8, 102)])
        self.assertAlmostEqual(run(p, BASELINE)['price'], 100.8)
        result = run(p, 'fixed_1p50_runner')
        self.assertEqual((result['price'], result['event']), (102, 'CLOSE'))
        self.assertTrue(result['runner'])

    def test_breakeven_activates_next_bar_and_includes_model_cost(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101.7, 99.8, 101.6),
                  (101.2, 101.3, 100, 100.2)])
        result = run(p, 'breakeven_1r_runner')
        self.assertEqual(result['index'], 2)
        self.assertAlmostEqual(result['price'], 100.05)
        self.assertEqual(result['reason'], 'COST_BREAKEVEN')
        self.assertEqual(result['stop_updates'][0]['observed_index'], 1)
        self.assertEqual(result['stop_updates'][0]['effective_index'], 2)
        self.assertAlmostEqual((result['price'] / 100 - 1) * 500000 - 250, 0., places=7)

    def test_breakeven_stop_gap_can_still_lose_money(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101.7, 99.8, 101.6),
                  (99.7, 100.1, 99.6, 100)])
        result = run(p, 'breakeven_1r_runner')
        self.assertEqual((result['price'], result['event']), (99.7, 'OPEN'))

    def test_short_breakeven_is_mirrored(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.2, 98.3, 98.4),
                  (98.8, 100, 98.7, 99.8)])
        result = run(p, 'breakeven_1r_runner', long=False)
        self.assertEqual(result['index'], 2)
        self.assertAlmostEqual(result['price'], 99.95)

    def test_entry_bar_extremes_and_close_cannot_activate_trailing(self):
        p = path([(100, 105, 99.8, 104), (100, 100.2, 99.8, 100),
                  (100, 100.2, 99.8, 100)])
        for name in ('breakeven_1r_runner', 'step_r_runner', 'chandelier3_runner', 'hybrid_runner'):
            result = run(p, name)
            self.assertEqual(result['activation_count'], 0)
            self.assertEqual(result['stop_update_count'], 0)

    def test_step_jump_locks1r_only_on_following_bar(self):
        p = path([(100, 100.1, 99.9, 100), (100, 103.3, 99.8, 103.1),
                  (102, 102.2, 101.4, 101.6)])
        result = run(p, 'step_r_runner')
        self.assertEqual(result['index'], 2)
        self.assertAlmostEqual(result['price'], 101.5)
        self.assertEqual(result['activation_count'], 3)
        self.assertEqual(result['stop_update_count'], 1)

    def test_step_stop_does_not_loosen_after_close_falls(self):
        p = path([(100, 100.1, 99.9, 100), (100, 102.5, 99.8, 102.3),
                  (102, 102.1, 101, 101.1), (101, 101.1, 100.7, 100.8)])
        result = run(p, 'step_r_runner')
        self.assertAlmostEqual(result['price'], 100.75)
        self.assertEqual(result['index'], 3)

    def test_chandelier_atr_expansion_cannot_loosen_existing_stop(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101, 99.9, 100.8),
                  (101.1, 101.3, 100.9, 101.2), (101.1, 101.2, 100.8, 101),
                  (101, 101.1, 100.6, 100.7)])
        features = feature_fixture(p)
        features['atr14'][2:] = 1.
        result = run(p, 'chandelier3_runner', features=features)
        self.assertEqual(result['index'], 4)
        self.assertAlmostEqual(result['price'], 100.7)
        self.assertEqual(result['stop_update_count'], 1)

    def test_chandelier_uses_confirmed_entry_atr_during_warmup(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101, 99.9, 100.8),
                  (100.8, 100.9, 99, 99.2)])
        result = run(p, 'chandelier3_runner', atr=1.)
        self.assertEqual(result['stop_update_count'], 0)
        self.assertEqual(result['price'], 99.2)

    def test_swing_pivot_waits_for2right_bars_and_next_bar_stop(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101.2, 99.9, 101),
                  (101, 101.4, 101, 101.2), (101.2, 101.4, 101.1, 101.3),
                  (101.3, 101.4, 100.8, 101.2), (101.2, 101.4, 101, 101.2),
                  (101.2, 101.4, 101.1, 101.2), (101.2, 101.3, 100.5, 100.6)])
        features = build_features(p)
        self.assertTrue(np.isnan(features['pivot_low'][5]))
        self.assertEqual(features['pivot_low_index'][6], 4)
        result = run(p, 'swing_be_runner', features=features)
        self.assertEqual(result['index'], 7)
        self.assertAlmostEqual(result['price'], 100.775)
        self.assertEqual(result['stop_updates'][0]['observed_index'], 6)
        self.assertEqual(result['stop_updates'][0]['effective_index'], 7)

    def test_pivot_from_before_or_at_entry_cannot_move_stop(self):
        p = path([(100, 100.2, 99.9, 100)] * 6 +
                 [(100, 101, 99.9, 100.8), (100.8, 101, 99.9, 100.8)])
        features = feature_fixture(p)
        features['pivot_low'][6] = 100.7
        features['pivot_low_index'][6] = 4
        result = run(p, 'swing_be_runner', features=features, entry_index=5)
        self.assertEqual(result['stop_update_count'], 0)

    def test_ema_scheduled_exit_precedes_next_bar_intrabar_extremes(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101.2, 99.9, 101),
                  (101, 101.1, 100.7, 100.8), (100.8, 100.9, 100.6, 100.7),
                  (100.6, 104, 98, 103)])
        features = feature_fixture(p)
        features['ema20'][:] = 100.9
        result = run(p, 'ema20_runner', features=features)
        self.assertEqual((result['index'], result['price'], result['event']), (4, 100.6, 'OPEN'))
        self.assertEqual(result['reason'], 'EMA20_MOMENTUM_EXIT')

    def test_hybrid_requires_both_ema_alignment_and_directional_efficiency(self):
        p = path([(100, 100.1, 99.9, 100), (100, 103.3, 99.9, 103.1),
                  (102.5, 103, 102, 102.8), (102, 102.2, 101.4, 101.5)])
        features = feature_fixture(p)
        features['ema9'][:] = 103
        features['ema20'][:] = 102
        features['er10'][:] = .2
        no_trend = run(p, 'hybrid_runner', atr=.2, features=features)
        self.assertAlmostEqual(no_trend['price'], 101.5)
        features['er10'][:] = .3
        trend = run(p, 'hybrid_runner', atr=.2, features=features)
        self.assertEqual((trend['index'], trend['price'], trend['event']), (2, 102.5, 'OPEN'))
        self.assertEqual(trend['reason'], 'HYBRID_3ATR')

    def test_adaptive_fallback_and_fixed_risk_validation(self):
        p = path([(100, 100.1, 99.9, 100)] * 3)
        for risk in (.4, .7, 1., 1.5):
            self.assertEqual(run(p, 'hybrid_adaptive_runner', override=risk)['initial_risk_pct'], risk)
        for invalid in (None, .3, 1.2, 1.6):
            with self.assertRaises(ValueError):
                run(p, 'hybrid_adaptive_runner', override=invalid)
        with self.assertRaises(ValueError):
            run(p, 'hybrid_runner', override=.7)

    def test_adaptive_r_is_actual_initial_risk(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.6, 99.9, 100.55),
                  (100.4, 100.5, 100, 100.2)])
        small = run(p, 'hybrid_adaptive_runner', override=.5)
        large = run(p, 'hybrid_runner')
        self.assertAlmostEqual(small['price'], 100.05)
        self.assertEqual(large['stop_update_count'], 0)

    def test_feature_prefix_invariance_and_atr_warmup(self):
        closes = 100 + .05 * np.arange(35) + np.sin(.7 * np.arange(35))
        p = path([(x, x + .2, x - .2, x) for x in closes])
        complete = build_features(p)
        prefix = build_features({key: value[:22] for key, value in p.items()})
        self.assertTrue(np.isnan(complete['atr14'][:14]).all())
        self.assertTrue(np.isfinite(complete['atr14'][14:]).all())
        for key in FEATURE_KEYS:
            np.testing.assert_allclose(complete[key][:22], prefix[key], equal_nan=True)
        p['high'][22:] += 50
        p['low'][22:] -= 30
        p['close'][22:] += 10
        after = build_features(p)
        for key in FEATURE_KEYS:
            np.testing.assert_allclose(complete[key][:22], after[key][:22], equal_nan=True)

    def test_future_after_exit_cannot_change_result(self):
        p = path([(100, 100.1, 99.9, 100), (100, 101.7, 99.8, 101.6),
                  (101.2, 101.3, 100, 100.2), (100.2, 103, 99, 102)])
        before = run(p, 'breakeven_1r_runner')
        p['high'][-1], p['low'][-1], p['close'][-1] = 200, 1, 150
        self.assertEqual(before, run(p, 'breakeven_1r_runner'))

    def test_final_close_does_not_report_a_stop_that_never_activated(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.1, 99.9, 100),
                  (100, 103.5, 99.9, 103.1)])
        result = run(p, 'step_r_runner')
        self.assertEqual(result['stop_update_count'], 0)
        self.assertEqual(result['activation_count'], 0)

    def test_missing_execution_minute_rejected(self):
        p = path([(100, 100.1, 99.9, 100)] * 3)
        p['timestamp_ns'][-1] += MINUTE_NS
        with self.assertRaises(ValueError):
            run(p, BASELINE)


if __name__ == '__main__':
    unittest.main()
