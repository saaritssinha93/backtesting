import copy
import unittest

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.entry_sl_20261106 import calibration as c


def fixture():
    days = list(pd.bdate_range(end='2026-10-05', periods=44).strftime('%Y-%m-%d'))
    rows = []
    for i in range(176):
        day, side = days[i // 4], 'LONG' if i % 2 == 0 else 'SHORT'
        atr = .10 if i % 4 < 2 else .20
        rows.append(dict(sid=i + 1, day=day, side=side, entry_price=100.,
            entry_ts=day + ' 10:01:00+05:30', confirmation_ts=day + ' 10:00:00+05:30',
            feature_available_at=day + ' 10:00:00+05:30', atr14_1m=atr, atr14_pct=atr,
            confirmation_low=99.8, confirmation_high=100.2, structure_distance_pct=.2,
            mae_upper_pct=.2 + .9 * i / 175, baseline_net_pnl_rs=100.))
    return pd.DataFrame(rows), days


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        self.frame, self.days = fixture()

    def test_future_features_and_outcomes_cannot_affect_fit(self):
        expected = c.fit(self.frame, self.days)
        future = self.frame.iloc[:3].copy()
        future['sid'] += 1000
        future['day'] = '2026-11-05'
        future['mae_upper_pct'] = [1e8, np.nan, -1]
        future['baseline_net_pnl_rs'] = [-1e10, np.nan, 1e10]
        future['atr14_pct'] = 0
        future['feature_available_at'] = 'bad'
        self.assertEqual(c.fit(pd.concat([self.frame, future], ignore_index=True), self.days), expected)

    def test_training_inventory_must_be44_ordered_sessions_ending_oct5(self):
        for days in (self.days[:-1], list(reversed(self.days)), [*self.days[:-1], '2026-10-06']):
            with self.assertRaises(ValueError):
                c.fit(self.frame, days)

    def test_rounding_and_stop_bounds(self):
        self.assertAlmostEqual(c.round_up(.9, .05), .9)
        self.assertAlmostEqual(c.round_up(.900001, .05), .95)
        self.assertAlmostEqual(c.round_up(6.2208, .25), 6.25)
        params = c.fit(self.frame, self.days)
        frame = self.frame.iloc[:2].copy()
        frame.loc[frame.index[0], ['atr14_1m', 'atr14_pct']] = .0001
        frame.loc[frame.index[1], ['atr14_1m', 'atr14_pct']] = 10.
        result = c.distances(frame, params)
        self.assertEqual(list(result.sid), list(frame.sid))
        self.assertEqual(list(result.atr_q95), [.5, 1.25])
        self.assertTrue(result[list(c.POLICIES)].ge(.5).all().all())
        self.assertTrue(result[list(c.POLICIES)].le(1.25).all().all())

    def test_long_short_structure_uses_correct_opposing_edge(self):
        params = c.fit(self.frame, self.days)
        frame = self.frame.iloc[:2].copy()
        frame['confirmation_low'] = 98.82
        frame['confirmation_high'] = 101.18
        frame['structure_distance_pct'] = 1.18
        result = c.distances(frame, params)
        for actual in result.structure_q95:
            self.assertAlmostEqual(actual, 1.205)
        frame.loc[frame.index[0], ['confirmation_low', 'confirmation_high']] = [100.1, 100.2]
        frame.loc[frame.index[1], ['confirmation_low', 'confirmation_high']] = [99.8, 99.9]
        frame['structure_distance_pct'] = -.1
        self.assertEqual(list(c.distances(frame, params).structure_q95), [1.25, 1.25])

    def test_no_missing_duplicated_or_future_entry_feature(self):
        params = c.fit(self.frame, self.days)
        bad = [self.frame.assign(sid=1), self.frame.assign(atr14_pct=np.nan),
               self.frame.assign(feature_available_at='2026-11-05 12:00:00+05:30'),
               self.frame.assign(structure_distance_pct=10.),
               self.frame.assign(atr14_pct=.00001)]
        for frame in bad:
            with self.assertRaises(ValueError):
                c.distances(frame, params)

    def test_underpopulated_volatility_bin_uses_global_fallback(self):
        frame = self.frame.copy()
        high = frame.index[frame.atr14_pct.eq(.2)]
        frame.loc[high[10:], 'baseline_net_pnl_rs'] = -100
        params = c.fit(frame, self.days)
        self.assertTrue(params['volatility_bins']['high']['pooled_fallback'])
        self.assertEqual(params['volatility_bins']['high']['stop_pct'], params['fixed_stop_pct']['mae_q95'])
        self.assertFalse(params['volatility_bins']['low']['pooled_fallback'])

    def test_assignment_never_reads_future_outcomes(self):
        params = c.fit(self.frame, self.days)
        first = c.distances(self.frame, params)
        modified = self.frame.assign(mae_upper_pct=999, baseline_net_pnl_rs=-999)
        pd.testing.assert_frame_equal(first, c.distances(modified, params))

    def test_fitted_limits_cannot_be_silently_widened(self):
        params = c.fit(self.frame, self.days)
        for mutation in ({'maximum_stop_pct': 2.}, {'structure_atr_buffer_multiple': .5}):
            bad = copy.deepcopy(params)
            bad.update(mutation)
            with self.assertRaises(ValueError):
                c.distances(self.frame, bad)


if __name__ == '__main__':
    unittest.main()
