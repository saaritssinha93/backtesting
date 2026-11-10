"""Native parity and declared delayed/wider entry-stop mechanics."""
import unittest

import numpy as np

from g3_v23_adx_rsi_20261106.entry_sl_20261106.recovery_room_20261106.engine import (
    BASELINE,CANDIDATES,MINUTE_NS,replay)


def path(rows):
    values=np.asarray(rows,dtype=float)
    return dict(timestamp_ns=np.arange(len(rows),dtype=np.int64)*MINUTE_NS,
                **{key:values[:,i] for i,key in enumerate(('open','high','low','close'))})


class RecoveryRoomEngineTests(unittest.TestCase):
    def test_native_baseline_parity(self):
        from fno_v13_v10_g_2_backtest import staged_exit
        cases=[path([(100,102,98,100)]),
               path([(100,100.1,99.9,100),(98,99,97,98)]),
               path([(100,100.1,99.9,100),(102,103,101,102)]),
               path([(100,100.1,99.9,100)]*121+[(100,101.1,98.9,100)])]
        for p in cases:
            for long in (True,False):
                expected=staged_exit(p,0,100,long,.8)
                actual=replay(p,0,100,long,.8,CANDIDATES[BASELINE])
                self.assertEqual((actual['index'],actual['price'],actual['reason'],actual['event']),
                                 (expected[0],expected[1],expected[2],expected[4]))

    def test_constant_stop_can_hold_beyond_native_tightened_stop_then_hit_target(self):
        p=path([(100,100.1,99.9,100)]*121+
               [(100,100.1,98.9,99.1),(99.1,101,99,100.9)])
        baseline=replay(p,0,100,True,.8,CANDIDATES[BASELINE])
        candidate=replay(p,0,100,True,.8,CANDIDATES['constant_1p25'])
        self.assertEqual(baseline['index'],121)
        self.assertEqual(baseline['reason'],'TIGHTENED_STOP')
        self.assertEqual(candidate['index'],122)
        self.assertEqual(candidate['reason'],'TARGET')
        self.assertAlmostEqual(candidate['price'],100.8)

    def test_wider_initial_stop_can_recover_after_native_initial_stop(self):
        p=path([(100,100.1,99.9,100),(100,100.1,98.6,98.7),(98.7,101,98.6,100.9)])
        baseline=replay(p,0,100,True,.8,CANDIDATES[BASELINE])
        wider=replay(p,0,100,True,.8,CANDIDATES['constant_1p50'])
        self.assertEqual(baseline['reason'],'STOP')
        self.assertAlmostEqual(baseline['price'],98.75)
        self.assertEqual(wider['reason'],'TARGET')
        self.assertEqual(wider['index'],2)

    def test_wider_stop_can_produce_a_larger_loss(self):
        p=path([(100,100.1,99.9,100),(100,100.1,98.4,98.6)])
        baseline=replay(p,0,100,True,2,CANDIDATES[BASELINE])
        wider=replay(p,0,100,True,2,CANDIDATES['constant_1p50'])
        self.assertAlmostEqual(baseline['price'],98.75)
        self.assertAlmostEqual(wider['price'],98.5)
        self.assertLess(wider['price'],baseline['price'])

    def test_120_and_240_minutes_activate_at_native_bar_start_boundary(self):
        for wait,name in [(120,BASELINE),(240,'delay_tighten240')]:
            p=path([(100,100.1,99.9,100)]*wait+
                   [(100,100.1,98.9,99.5),(100,100.1,98.9,99.5)])
            actual=replay(p,0,100,True,2,CANDIDATES[name])
            self.assertEqual(actual['index'],wait+1)
            self.assertAlmostEqual(actual['price'],99)
            self.assertEqual(actual['reason'],'TIGHTENED_STOP')

    def test_short_delayed_stop_and_larger_short_stop(self):
        p=path([(100,100.1,99.9,100)]*121+
               [(100,101.1,99.9,100.9),(100.9,101,99,99.1)])
        baseline=replay(p,0,100,False,.8,CANDIDATES[BASELINE])
        constant=replay(p,0,100,False,.8,CANDIDATES['constant_1p25'])
        self.assertAlmostEqual(baseline['price'],101)
        self.assertEqual(constant['reason'],'TARGET')
        self.assertAlmostEqual(constant['price'],99.2)
        loss=replay(path([(100,101.6,99.9,101)]),0,100,False,2,CANDIDATES['constant_1p50'])
        self.assertAlmostEqual(loss['price'],101.5)

    def test_post_entry_gap_fill_is_actual_open_for_both_directions(self):
        long=replay(path([(100,100.1,99.9,100),(98,99,97,98)]),0,100,True,2,CANDIDATES['constant_1p50'])
        short=replay(path([(100,100.1,99.9,100),(102,103,101,102)]),0,100,False,2,CANDIDATES['constant_1p50'])
        self.assertEqual((long['price'],long['event']),(98,'OPEN'))
        self.assertEqual((short['price'],short['event']),(102,'OPEN'))

    def test_entry_bar_never_uses_preentry_open_and_stop_first_tie_is_retained(self):
        actual=replay(path([(97,103,96,101)]),0,100,True,2,CANDIDATES['constant_1p50'])
        self.assertAlmostEqual(actual['price'],98.5)
        self.assertEqual(actual['event'],'INTRABAR')
        self.assertTrue(actual['ambiguous'])

    def test_future_after_exit_cannot_change_exit(self):
        p=path([(100,100.1,99.9,100),(100,101,99.9,100.9),(100.9,103,90,99)])
        before=replay(p,0,100,True,.8,CANDIDATES['constant_1p50'])
        p['high'][-1],p['low'][-1],p['close'][-1]=200,2,150
        self.assertEqual(before,replay(p,0,100,True,.8,CANDIDATES['constant_1p50']))

    def test_time_exit_uses_final_available_session_close(self):
        p=path([(100,100.1,99.9,100),(100,100.4,99.9,100.3)])
        for name in CANDIDATES:
            actual=replay(p,0,100,True,2,CANDIDATES[name])
            self.assertEqual((actual['index'],actual['price'],actual['reason'],actual['event']),
                             (1,100.3,'TIME_EXIT_1515','CLOSE'))

    def test_nonzero_entry_index_sets_its_own_tightening_clock(self):
        p=path([(100,100.1,99.9,100)]*122+
               [(100,100.1,98.9,99.5),(100,100.1,98.9,99.5)])
        actual=replay(p,2,100,True,2,CANDIDATES[BASELINE])
        self.assertEqual(actual['index'],123)

    def test_undeclared_or_risk_loosening_settings_rejected(self):
        p=path([(100,100.1,99.9,100)])
        invalid=[dict(initial_stop_pct=2),dict(initial_stop_pct=np.nan),
                 dict(tighten_after_minutes=60),dict(tightened_stop_pct=1.5),
                 dict(outcome='loser')]
        for settings in invalid:
            with self.assertRaises(ValueError):
                replay(p,0,100,True,2,settings)


if __name__=='__main__':
    unittest.main()
