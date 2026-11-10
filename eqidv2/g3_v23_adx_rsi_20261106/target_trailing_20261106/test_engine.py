"""Execution and causality checks for the frozen target-relative shortlist."""
import copy
import unittest

import numpy as np

from g3_v23_adx_rsi_20261106.target_trailing_20261106.engine import (
    BASELINE, CANDIDATES, FEATURE_KEYS, MINUTE_NS, build_features, replay)
from g3_v23_adx_rsi_20261106.trailing_20261106.engine import replay as old_replay


def path(rows):
    values = np.asarray(rows, dtype=float)
    return dict(timestamp_ns=np.arange(len(rows), dtype=np.int64) * MINUTE_NS,
        open=values[:, 0], high=values[:, 1], low=values[:, 2], close=values[:, 3])


def mirror(p):
    result = {key: values.copy() for key, values in p.items()}
    for key in ('open', 'close'):
        result[key] = 200 - p[key]
    result['high'], result['low'] = 200-p['low'], 200-p['high']
    return result


def run(p, name, long=True, target=.8, atr=.1, features=None, entry_index=0, override=None):
    return replay(p, entry_index, 100., long, target, CANDIDATES[name], atr,
        initial_override=override, features=features)


def feature_fixture(p):
    return {key: np.full(len(p['close']), np.nan) for key in FEATURE_KEYS}


class TargetTrailingTests(unittest.TestCase):
    def test_exact_baseline_replay_schema_and_results(self):
        paths = [path([(100, 100.1, 99.9, 100)] * 140),
            path([(100, 101, 98, 100)]),
            path([(100, 100.1, 99.9, 100), (98, 99, 97, 98)]),
            path([(100, 100.1, 99.9, 100), (102, 103, 101, 102)])]
        for p in paths:
            for long in (True, False):
                for entry_index in {0, len(p['close'])-1}:
                    self.assertEqual(run(p, BASELINE, long=long, entry_index=entry_index),
                        old_replay(p, entry_index, 100., long, .8, CANDIDATES[BASELINE], .1))

    def test_cushion_and_breakeven_thresholds_mirrored_and_next_bar(self):
        cases = [('cushion_half_target', .4, -1.00),
                 ('cushion_threequarter_target', .6, -.75),
                 ('breakeven_eighty_target', .64, .05)]
        for name, trigger, stop_return in cases:
            p = path([(100, 100.1, 99.9, 100),
                (100, 100+trigger+.01, 98.8, 100+trigger),
                (100.3, 100.4, 98.8, 99)])
            for long in (True, False):
                result = run(p if long else mirror(p), name, long=long)
                self.assertEqual(result['index'], 2)
                self.assertAlmostEqual(result['price'], 100+(1 if long else -1)*stop_return)
                self.assertEqual(result['stop_updates'][0]['observed_index'], 1)
                self.assertEqual(result['stop_updates'][0]['effective_index'], 2)

    def test_just_below_trigger_does_not_arm(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.5, 99.9, 100.399),
                  (99.1, 99.2, 98.8, 99)])
        result = run(p, 'cushion_half_target')
        self.assertEqual(result['activation_count'], 0)
        self.assertEqual(result['reason'], 'TIME_EXIT_1515')

    def test_intrabar_high_is_not_a_close_trigger(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.79, 99.9, 100.1),
                  (100, 100.1, 99.9, 100)])
        for name in CANDIDATES:
            self.assertEqual(run(p, name)['activation_count'], 0)

    def test_entry_bar_close_and_extremes_cannot_arm(self):
        p = path([(100, 100.79, 99.8, 100.75), (100, 100.1, 99.9, 100),
                  (99.1, 99.2, 98.8, 99)])
        for name in CANDIDATES:
            self.assertEqual(run(p, name)['activation_count'], 0)

    def test_native_target_exits_before_trigger_close_can_change_stop(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.81, 99.8, 100.7),
                  (99.1, 99.2, 98.8, 99)])
        for name in CANDIDATES:
            result = run(p, name)
            self.assertEqual((result['index'], result['reason']), (1, 'TARGET'))
            self.assertAlmostEqual(result['price'], 100.8)
            self.assertEqual(result['stop_update_count'], 0)
            self.assertFalse(result['runner'])

    def test_breakeven_cost_coverage_and_adverse_gap(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.7, 99.8, 100.65),
                  (100.2, 100.3, 100, 100.1)])
        result = run(p, 'breakeven_eighty_target')
        self.assertAlmostEqual((result['price']/100-1)*500000-250, 0., places=6)
        p['open'][2], p['low'][2] = 99.8, 99.7
        gap = run(p, 'breakeven_eighty_target')
        self.assertEqual((gap['price'], gap['event']), (99.8, 'OPEN'))

    def test_steps_ratchet_and_never_loosen(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.45, 99.9, 100.4),
                  (100.4, 100.65, 99.2, 100.6), (100.6, 100.75, 99.6, 100.72),
                  (100.3, 100.4, 100.1, 100.2), (100.2, 100.3, 100, 100.1)])
        result = run(p, 'target_steps')
        self.assertEqual(result['index'], 5)
        self.assertAlmostEqual(result['price'], 100.05)
        self.assertEqual([u['stop_return_pct'] for u in result['stop_updates']], [-1., -.5, .05])
        self.assertEqual(result['activation_count'], 3)

    def test_steps_jump_records_all_stages_and_one_effective_stop(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.76, 99, 100.75),
                  (100.3, 100.4, 100, 100.1)])
        result = run(p, 'target_steps')
        self.assertEqual(result['activation_count'], 3)
        self.assertEqual(result['stop_update_count'], 1)
        self.assertAlmostEqual(result['price'], 100.05)

    def test_patient_waits_five_completed_postentry_bars(self):
        p = path([(100, 100.1, 99.9, 100)] + [(100.5, 100.6, 99.1, 100.5)]*5 +
                  [(99.2, 99.3, 98.8, 99.1)])
        result = run(p, 'patient_half_cushion')
        self.assertEqual(result['index'], 6)
        self.assertEqual(result['stop_updates'][0]['observed_index'], 5)
        self.assertAlmostEqual(result['price'], 99.)

    def test_consecutive_confirmation_resets_and_ignores_preentry_bars(self):
        p = path([(100.5, 100.6, 100.4, 100.5)]*5 + [(100, 100.1, 99.9, 100)] +
            [(100.5, 100.6, 99.1, 100.5)]*3 + [(100.3, 100.4, 99.1, 100.3),
            (100.5, 100.6, 99.1, 100.5), (99.2, 99.3, 98.8, 99.1)])
        result = run(p, 'patient_half_cushion', entry_index=5)
        self.assertEqual(result['activation_count'], 0)
        self.assertEqual(result['reason'], 'TIME_EXIT_1515')

    def test_hybrid_close_peak_excludes_high_and_entry_close(self):
        p = path([(100, 101.99, 99.9, 101.9)] +
            [(101.6, 101.95, 101.5, 101.6)]*5 +
            [(101.5, 101.6, 101.3, 101.4), (101.4, 101.5, 101.2, 101.3)])
        result = run(p, 'target_atr_hybrid', target=2.)
        self.assertEqual(result['index'], 7)
        self.assertAlmostEqual(result['price'], 101.25)
        self.assertEqual(result['stop_updates'][0]['observed_index'], 5)
        mirrored = run(mirror(p), 'target_atr_hybrid', target=2., long=False)
        self.assertAlmostEqual(mirrored['price'], 98.75)

    def test_hybrid_uses_entry_atr_fallback_and_stop_floor(self):
        p = path([(100, 100.1, 99.9, 100)] +
            [(101.6, 101.7, 101.5, 101.6)]*5 + [(100, 100.1, 99, 99.5)])
        result = run(p, 'target_atr_hybrid', target=2., atr=1.)
        self.assertAlmostEqual(result['price'], 99.25)
        self.assertAlmostEqual(result['stop_updates'][0]['stop_return_pct'], -.75)

    def test_hybrid_keeps_trailing_after_confirmation_and_atr_expansion_does_not_loosen(self):
        p = path([(100, 100.1, 99.9, 100)] +
            [(101.6, 101.7, 101.5, 101.6)]*5 +
            [(101.5, 101.6, 101.3, 101.4), (101.4, 101.5, 101.2, 101.3)])
        features = feature_fixture(p)
        features['atr14'][5] = .1
        features['atr14'][6] = 1.
        result = run(p, 'target_atr_hybrid', target=2., features=features)
        self.assertEqual(result['index'], 7)
        self.assertAlmostEqual(result['price'], 101.25)
        self.assertEqual(result['stop_update_count'], 1)

    def test_hybrid_atr_contraction_can_tighten_after_price_falls_below_trigger(self):
        p = path([(100, 100.1, 99.9, 100)] +
            [(101.6, 101.7, 101.5, 101.6)]*5 +
            [(101.5, 101.6, 101.3, 101.4), (101.4, 101.5, 101.2, 101.3)])
        features = feature_fixture(p)
        features['atr14'][5] = .5
        features['atr14'][6] = .1
        result = run(p, 'target_atr_hybrid', target=2., features=features)
        self.assertEqual(result['index'], 7)
        self.assertEqual(result['stop_update_count'], 2)
        self.assertAlmostEqual(result['stop_updates'][1]['stop_return_pct'], 1.25)
        self.assertEqual(result['stop_updates'][1]['observed_index'], 6)

    def test_future_bars_cannot_alter_prior_exit_or_features(self):
        p = path([(100, 100.1, 99.9, 100)] +
            [(101.6, 101.7, 101.5, 101.6)]*5 +
            [(101.5, 101.6, 101.3, 101.4), (101.4, 101.5, 101.2, 101.3)] +
            [(100, 101, 99, 100)]*20)
        before = run(p, 'target_atr_hybrid', target=2.)
        features = build_features(p)
        p['high'][8:], p['low'][8:], p['close'][8:] = 200, 1, 150
        self.assertEqual(before, run(p, 'target_atr_hybrid', target=2.))
        later = build_features(p)
        for key in FEATURE_KEYS:
            np.testing.assert_allclose(features[key][:8], later[key][:8], equal_nan=True)

    def test_native_stop_first_ambiguity_and_entry_open_not_used_for_gap_fill(self):
        p = path([(97, 101, 96, 100)])
        for name in CANDIDATES:
            result = run(p, name)
            self.assertEqual((result['price'], result['event'], result['ambiguous']), (98.5, 'INTRABAR', True))

    def test_final_bar_cannot_create_unusable_stop(self):
        p = path([(100, 100.1, 99.9, 100), (100, 100.76, 99.9, 100.75)])
        for name in CANDIDATES:
            self.assertEqual(run(p, name)['stop_update_count'], 0)

    def test_frozen_settings_and_data_validation(self):
        p = path([(100, 100.1, 99.9, 100)]*3)
        settings = copy.deepcopy(CANDIDATES['target_atr_hybrid'])
        settings['minimum_trail_pct'] = .1
        with self.assertRaises(ValueError):
            replay(p, 0, 100, True, .8, settings, .1)
        with self.assertRaises(ValueError):
            run(p, 'patient_half_cushion', override=.75)
        p['timestamp_ns'][-1] += MINUTE_NS
        for name in CANDIDATES:
            with self.assertRaises(ValueError):
                run(p, name)


if __name__ == '__main__':
    unittest.main()
