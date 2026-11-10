"""Native parity and causal entry-relative stop execution checks."""
import unittest

import numpy as np

from g3_v23_adx_rsi_20261106.entry_sl_20261106.engine import MINUTE_NS, replay


def path(rows):
    values = np.asarray(rows, dtype=float)
    return dict(timestamp_ns=np.arange(len(rows), dtype=np.int64) * MINUTE_NS,
                **{key: values[:, i] for i, key in enumerate(('open','high','low','close'))})


class EntryStopTests(unittest.TestCase):
    def test_default_native_parity_long_and_short(self):
        from fno_v13_v10_g_2_backtest import staged_exit
        cases = [path([(100,102,98,100)]),
                 path([(100,100.1,99.9,100),(98,99,97,98)]),
                 path([(100,100.1,99.9,100),(102,103,101,102)]),
                 path([(100,100.1,99.9,100),(100,101,99.9,100.8)]),
                 path([(100,100.1,99.9,100)]*121+[(100,101.1,98.9,100)])]
        for p in cases:
            for long in (True, False):
                actual = replay(p,0,100,long,.8)
                expected = staged_exit(p,0,100,long,.8)
                self.assertEqual((actual['index'],actual['price'],actual['reason'],actual['event']),
                                 (expected[0],expected[1],expected[2],expected[4]))

    def test_initial_stop_applies_on_entry_bar_without_using_earlier_open(self):
        p = path([(98,100.5,97,100)])
        actual = replay(p,0,100,True,2,initial_stop_pct=.8)
        self.assertEqual(actual['index'],0)
        self.assertAlmostEqual(actual['price'],99.2)
        self.assertEqual(actual['event'],'INTRABAR')
        self.assertEqual(actual['reason'],'ENTRY_RELATIVE_STOP')

    def test_short_entry_relative_level_and_stop_first_tie(self):
        p = path([(100,101,97,100)])
        actual = replay(p,0,100,False,2,initial_stop_pct=.8)
        self.assertAlmostEqual(actual['price'],100.8)
        self.assertTrue(actual['ambiguous'])

    def test_post_entry_gaps_fill_at_adverse_open(self):
        long = replay(path([(100,100.1,99.9,100),(98.5,99.5,98,99)]),0,100,True,2,.8)
        short = replay(path([(100,100.1,99.9,100),(101.5,102,100.5,101)]),0,100,False,2,.8)
        self.assertEqual((long['price'],long['event']),(98.5,'OPEN'))
        self.assertEqual((short['price'],short['event']),(101.5,'OPEN'))

    def test_stage_tightens_at_native_time_and_never_widens(self):
        p = path([(100,100.1,99.9,100)]*121+[(100,100.1,98.9,99)])
        actual = replay(p,0,100,True,3,initial_stop_pct=1.2)
        self.assertEqual(actual['index'],121)
        self.assertAlmostEqual(actual['price'],99)
        self.assertEqual(actual['reason'],'TIGHTENED_STOP')
        p = path([(100,100.1,99.9,100)]*121+[(100,100.1,99.1,99.5)])
        actual = replay(p,0,100,True,3,initial_stop_pct=.8)
        self.assertAlmostEqual(actual['price'],99.2)
        self.assertEqual(actual['reason'],'ENTRY_RELATIVE_STOP')

    def test_completed_close_trigger_activates_next_bar_only(self):
        p = path([(100,100.1,99.9,100),(100,100.7,99.5,100.6),(100.4,100.5,100,100.2)])
        actual = replay(p,0,100,True,2,breakeven_trigger_pct=.6)
        self.assertEqual(actual['index'],2)
        self.assertAlmostEqual(actual['price'],100.05)
        self.assertEqual(actual['reason'],'ENTRY_PROGRESS_LOCK')

    def test_high_touch_and_entry_bar_close_cannot_trigger_lock(self):
        p = path([(100,100.8,99.9,100.7),(100.7,100.9,99.9,100),(100,100.1,99.9,100)])
        actual = replay(p,0,100,True,2,breakeven_trigger_pct=.6)
        self.assertEqual(actual['reason'],'TIME_EXIT_1515')

    def test_short_lock_is_entry_relative_and_handles_gap(self):
        p = path([(100,100.1,99.9,100),(100,100.2,99.3,99.4),(100.2,100.3,99.8,100)])
        actual = replay(p,0,100,False,2,breakeven_trigger_pct=.6)
        self.assertEqual(actual['index'],2)
        self.assertEqual(actual['price'],100.2)
        self.assertEqual(actual['event'],'OPEN')

    def test_lock_stays_active_after_close_falls_below_trigger(self):
        p = path([(100,100.1,99.9,100),(100,100.7,99.9,100.6),
                  (100.6,100.7,100.2,100.3),(100.3,100.4,100,100.1)])
        actual = replay(p,0,100,True,2,breakeven_trigger_pct=.6)
        self.assertEqual(actual['index'],3)
        self.assertAlmostEqual(actual['price'],100.05)

    def test_nonzero_entry_index_ignores_earlier_extrema_and_close(self):
        p = path([(100,103,97,102),(100,100.1,99.9,100),
                  (100,100.1,99.9,100)])
        actual = replay(p,1,100,True,2,breakeven_trigger_pct=.6)
        self.assertEqual(actual['reason'],'TIME_EXIT_1515')
        from fno_v13_v10_g_2_backtest import staged_exit
        native = staged_exit(p,1,100,True,2)
        baseline = replay(p,1,100,True,2)
        self.assertEqual((baseline['index'],baseline['price'],baseline['reason'],baseline['event']),
                         (native[0],native[1],native[2],native[4]))

    def test_price_level_scaling_preserves_relative_risk(self):
        p = path([(100,100.1,99.9,100),(100,100.1,99,99.5)])
        scaled = {key:(value if key=='timestamp_ns' else value*10) for key,value in p.items()}
        one = replay(p,0,100,True,2,.8)
        ten = replay(scaled,0,1000,True,2,.8)
        self.assertAlmostEqual(ten['price'],one['price']*10)
        self.assertEqual(ten['index'],one['index'])

    def test_future_tail_does_not_change_exit(self):
        p = path([(100,100.1,99.9,100),(100,100.1,99,99.5),(99.5,103,90,98)])
        before = replay(p,0,100,True,2,.8)
        p['high'][-1],p['low'][-1],p['close'][-1]=200,2,150
        self.assertEqual(before,replay(p,0,100,True,2,.8))

    def test_invalid_stop_and_noncausal_trigger_mode_rejected(self):
        p = path([(100,100.1,99.9,100)])
        for invalid in (0,-1,1.3,np.nan,np.inf):
            with self.assertRaises(ValueError):
                replay(p,0,100,True,2,initial_stop_pct=invalid)
        with self.assertRaises(ValueError):
            replay(p,0,100,True,2,breakeven_trigger_pct=.6,trigger_on='high')
        with self.assertRaises(ValueError):
            replay(p,0,100,True,2,breakeven_trigger_pct=.05,lock_return_pct=.05)


if __name__=='__main__':
    unittest.main()
