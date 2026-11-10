import unittest

import numpy as np
import pandas as pd

from . import features as f


DAY = '2026-11-09'


def sample(periods=80, slopes=(.08, .00, -.06), day=DAY):
    times = pd.date_range(day + ' 09:16', periods=periods, freq='min', tz=f.IST)
    pieces = []
    for symbol, slope in zip(('A', 'B', 'C'), slopes):
        close = 100 + np.arange(periods) * slope
        opening = np.r_[100., close[:-1]]
        pieces.append(pd.DataFrame(dict(tradingsymbol=symbol, ts=times, open=opening,
            high=np.maximum(opening, close) + .10, low=np.minimum(opening, close) - .10, close=close)))
    return pd.concat(pieces, ignore_index=True)


class FeatureTests(unittest.TestCase):
    def test_prefix_and_future_mutation_invariance(self):
        minutes = sample()
        cutoff = pd.Timestamp(DAY + ' 10:00', tz=f.IST)
        full = f.build_states(minutes, DAY)
        expected = full.loc[full.confirmation_ts.le(cutoff)].reset_index(drop=True)
        prefix = f.build_states(minutes.loc[minutes.ts.le(cutoff)], DAY)
        pd.testing.assert_frame_equal(expected, prefix)
        changed = minutes.copy()
        changed.loc[changed.ts.gt(cutoff), list(f.OHLC)] *= 1.25
        mutated = f.build_states(changed, DAY)
        pd.testing.assert_frame_equal(expected, mutated.loc[mutated.confirmation_ts.le(cutoff)].reset_index(drop=True))

    def test_ema_and_atr_oracles_and_same_session_warmup(self):
        minutes = sample(periods=30, slopes=(.1,))
        previous = sample(periods=360, slopes=(.2,), day='2026-11-06')
        states = f.build_states(pd.concat([previous, minutes], ignore_index=True), DAY)
        first = states.iloc[0]
        self.assertEqual(first.confirmation_ts.strftime('%H:%M'), '09:35')
        self.assertEqual(len(states), 3)
        self.assertAlmostEqual(first.ema20, minutes.close.iloc[:20].ewm(span=20, adjust=False).mean().iloc[-1])
        self.assertAlmostEqual(first.atr14, .3)
        self.assertAlmostEqual(first.anchor_atr, .3)
        self.assertTrue(f.build_states(minutes.iloc[:19], DAY).empty)

    def test_previous_ranges_and_fixed_anchor_exclude_confirmation(self):
        minutes = sample(periods=30, slopes=(.1,))
        stamp = pd.Timestamp(DAY + ' 09:40', tz=f.IST)
        before = f.build_states(minutes, DAY).set_index('confirmation_ts').loc[stamp]
        at = int(minutes.index[minutes.ts.eq(stamp)][0])
        self.assertAlmostEqual(before.prev5_high, minutes.high.iloc[at-5:at].max())
        self.assertAlmostEqual(before.prev2_low, minutes.low.iloc[at-2:at].min())
        self.assertAlmostEqual(before.intervening_low, minutes.low.iloc[at-4:at].min())
        self.assertAlmostEqual(before.intervening_high, minutes.high.iloc[at-4:at].max())
        self.assertAlmostEqual(before.anchor_close, minutes.close.iloc[at-5])
        self.assertAlmostEqual(before.impulse, minutes.close.iloc[at-5] - minutes.close.iloc[at-15])
        changed = minutes.copy()
        changed.loc[at, 'high'] = 150.
        changed.loc[at, 'low'] = 50.
        after = f.build_states(changed, DAY).set_index('confirmation_ts').loc[stamp]
        for field in ('prev5_high', 'prev5_low', 'prev2_high', 'prev2_low',
                      'anchor_close', 'anchor_atr', 'impulse', 'intervening_low', 'intervening_high'):
            self.assertEqual(before[field], after[field], field)

    def test_efficiency_and_ema_gap_window(self):
        minutes = sample(periods=30, slopes=(.1,))
        states = f.build_states(minutes, DAY)
        ema = minutes.close.ewm(span=20, adjust=False).mean()
        for row in states.itertuples():
            at = int(minutes.index[minutes.ts.eq(row.confirmation_ts)][0])
            gap = (minutes.close - ema).iloc[at-4:at+1]
            self.assertAlmostEqual(row.signed_efficiency10, 1.)
            self.assertAlmostEqual(row.ema_gap_min5, gap.min())
            self.assertAlmostEqual(row.ema_gap_max5, gap.max())
        flat = f.build_states(sample(periods=30, slopes=(0.,)), DAY)
        self.assertTrue(flat.signed_efficiency10.eq(0.).all())

    def test_long_short_reflection(self):
        minutes = sample(periods=45, slopes=(.1,))
        reflected = minutes.copy()
        reflected['open'] = 200. - minutes.open
        reflected['close'] = 200. - minutes.close
        reflected['high'] = 200. - minutes.low
        reflected['low'] = 200. - minutes.high
        original = f.build_states(minutes, DAY)
        reverse = f.build_states(reflected, DAY)
        np.testing.assert_allclose(original.atr14, reverse.atr14)
        np.testing.assert_allclose(original.impulse, -reverse.impulse)
        np.testing.assert_allclose(original.signed_efficiency10, -reverse.signed_efficiency10)
        np.testing.assert_allclose(original.ema_gap_min5, -reverse.ema_gap_max5, atol=1e-12)
        np.testing.assert_allclose(original.prev5_high, 200. - reverse.prev5_low)

    def test_contemporaneous_full_universe_ranks(self):
        states = f.build_states(sample(periods=25), DAY)
        first = states.loc[states.confirmation_ts.eq(states.confirmation_ts.min())].set_index('tradingsymbol')
        for prefix in ('session', 'recent'):
            self.assertAlmostEqual(first.loc['A', prefix + '_rank_long'], 1.)
            self.assertAlmostEqual(first.loc['B', prefix + '_rank_long'], 2/3)
            self.assertAlmostEqual(first.loc['C', prefix + '_rank_long'], 1/3)
            self.assertAlmostEqual(first.loc['C', prefix + '_rank_short'], 1.)
        self.assertTrue(first.universe_count.eq(3).all())
        ties = f.build_states(sample(periods=25, slopes=(0., 0., 0.)), DAY)
        self.assertTrue(np.allclose(ties.session_rank_long, 2/3))

    def test_one_stock_gap_does_not_block_healthy_stocks(self):
        minutes = sample(periods=40)
        damaged = minutes.loc[~(minutes.tradingsymbol.eq('A') & minutes.ts.eq(pd.Timestamp(DAY + ' 09:25', tz=f.IST)))]
        states = f.build_states(damaged, DAY)
        self.assertEqual(set(states.tradingsymbol), {'B', 'C'})
        self.assertTrue(states.universe_count.eq(2).all())
        self.assertEqual(len(states), 10)

    def test_no_volume_or_outcomes_consumed_or_published(self):
        minutes = sample(periods=25)
        base = f.build_states(minutes, DAY)
        decorated = minutes.assign(volume=0., outcome='WIN', portfolio_net_profit_rupees=1e12)
        pd.testing.assert_frame_equal(base, f.build_states(decorated, DAY))
        self.assertEqual(list(base.columns), f.STATE_COLUMNS)
        for name in f.OHLC:
            pd.testing.assert_series_equal(base[name], base['confirmation_' + name], check_names=False)

    def test_exhaustive_movers_and_threshold_times(self):
        minutes = sample(periods=360, slopes=(0., 0., 0.))
        times = minutes.loc[minutes.tradingsymbol.eq('A'), 'ts'].reset_index(drop=True)
        minutes.loc[minutes.tradingsymbol.eq('A') & minutes.ts.eq(times.iloc[10]), 'high'] = 103.
        minutes.loc[minutes.tradingsymbol.eq('A') & minutes.ts.eq(times.iloc[20]), 'high'] = 105.
        minutes.loc[minutes.tradingsymbol.eq('B') & minutes.ts.eq(times.iloc[15]), 'low'] = 96.
        # Same high ties are resolved by symbol, never by later trade outcomes.
        minutes.loc[minutes.tradingsymbol.eq('C') & minutes.ts.eq(times.iloc[30]), 'high'] = 105.
        movers = f.build_movers(minutes, DAY)
        self.assertEqual(len(movers), 6)
        self.assertFalse(movers.duplicated(['day', 'symbol', 'side']).any())
        long_a = movers.loc[movers.side.eq('LONG') & movers.symbol.eq('A')].iloc[0]
        long_c = movers.loc[movers.side.eq('LONG') & movers.symbol.eq('C')].iloc[0]
        short_b = movers.loc[movers.side.eq('SHORT') & movers.symbol.eq('B')].iloc[0]
        self.assertEqual(long_a['rank'], 1)
        self.assertEqual(long_c['rank'], 2)
        self.assertEqual(long_a.first3pct_ts, times.iloc[10])
        self.assertEqual(long_a.extreme_ts, times.iloc[20])
        self.assertAlmostEqual(long_a.move_pct, 5.)
        self.assertAlmostEqual(short_b.move_pct, -4.)
        self.assertTrue(long_a.top2 and long_c.top2 and short_b.top2)
        self.assertEqual(int(movers.top5.sum()), 3)
        self.assertTrue(movers.loc[~movers.qualifies3pct, 'first3pct_ts'].isna().all())

    def test_partial_mover_session_and_duplicate_states_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, 'complete'):
            f.build_movers(sample(periods=80), DAY)
        minutes = sample(periods=25)
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            f.build_states(pd.concat([minutes, minutes.iloc[:1]], ignore_index=True), DAY)


if __name__ == '__main__':
    unittest.main()
