import unittest

import numpy as np
import pandas as pd

from . import execution as e


def orders(side='LONG', close=100.):
    return pd.DataFrame([dict(sid=1, day='2026-11-09', setup_id='MOMENTUM_'+side,
        tradingsymbol='TEST', side=side, confirmation_ts=pd.Timestamp('2026-11-09 15:10', tz=e.IST),
        confirmation_close=close)])


def minutes():
    times = pd.date_range('2026-11-09 15:10', '2026-11-09 15:15', freq='min', tz=e.IST)
    return pd.DataFrame(dict(ts=times, tradingsymbol='TEST', open=100., high=100.1, low=99.9, close=100.))


class ExecutionTests(unittest.TestCase):
    def test_confirmation_bar_cannot_fill_or_exit(self):
        frame = minutes()
        frame.loc[0, ['high','low']] = [110., 90.]
        trade, audit = e.simulate(orders(), frame)
        self.assertEqual(audit.status.tolist(), ['FILLED'])
        self.assertEqual(trade.entry_price.iloc[0], 100.)
        self.assertIn('15:11:00', trade.entry_ts.iloc[0])
        self.assertIn('15:10:00', trade.entry_execution_ts.iloc[0])
        self.assertEqual(trade.exit_reason.iloc[0], 'TIME_EXIT_1515')
        self.assertEqual(trade.portfolio_net_profit_rupees.iloc[0], -250.)

    def test_adverse_gap_cancels_both_sides_and_does_not_retry(self):
        for side, opening in [('LONG', 100.16), ('SHORT', 99.84)]:
            frame = minutes()
            frame.loc[1, ['open','high','low','close']] = [opening, opening+.1, opening-.1, opening]
            trade, audit = e.simulate(orders(side), frame)
            self.assertTrue(trade.empty)
            self.assertEqual(audit.status.tolist(), ['CANCELLED_ADVERSE_OPEN_GAP'])
        for side, opening in [('LONG', 100.15), ('SHORT', 99.85)]:
            decision = e.find_entry(orders(side).iloc[0], dict(ts=minutes().ts.iloc[1], open=opening))
            self.assertTrue(decision['filled'])

    def test_missing_exact_next_minute_never_uses_later_open(self):
        frame = minutes().drop(index=1)
        trade, audit = e.simulate(orders(), frame)
        self.assertTrue(trade.empty)
        self.assertEqual(audit.status.tolist(), ['MISSING_NEXT_MINUTE'])

    def test_entry_bar_stop_and_target_is_stop_first(self):
        for side in ['LONG', 'SHORT']:
            frame = minutes()
            frame.loc[1, ['high','low']] = [102.,98.]
            trade, _ = e.simulate(orders(side), frame)
            self.assertEqual(trade.exit_reason.iloc[0], 'STOP')
            self.assertTrue(trade.same_bar_ambiguous.iloc[0])
            self.assertAlmostEqual(trade.portfolio_net_profit_rupees.iloc[0], -7750.)

    def test_entry_bar_close_does_not_arm_and_next_close_cannot_backdate_stop(self):
        frame = minutes()
        frame.loc[1, ['open','high','low','close']] = [100.,101.4,99.9,101.3]
        # Entry-bar close reaches threshold, but inherited policy skips it.
        frame.loc[2, ['open','high','low','close']] = [101.3,101.4,99.5,101.3]
        frame.loc[3, ['open','high','low','close']] = [100.,100.2,99.9,100.1]
        trade, _ = e.simulate(orders(), frame)
        self.assertEqual(trade.stop_update_count.iloc[0], 1)
        self.assertEqual(trade.exit_reason.iloc[0], 'TARGET_PROGRESS_BREAKEVEN')
        self.assertEqual(trade.exit_event.iloc[0], 'OPEN')
        self.assertIn('15:12:00', trade.exit_ts.iloc[0])
        self.assertEqual(trade.exit_price.iloc[0], 100.)

    def test_future_missing_or_flagged_path_errors_not_entry_rejection(self):
        order = orders().iloc[0]
        first = dict(ts=minutes().ts.iloc[1], open=100.)
        self.assertTrue(e.find_entry(order, first)['filled'])
        damaged = minutes().drop(index=4)
        with self.assertRaisesRegex(ValueError, 'Entered trade lacks complete'):
            e.simulate(orders(), damaged)
        flagged = minutes().assign(gap_filled=False)
        flagged.loc[4,'gap_filled'] = True
        with self.assertRaisesRegex(ValueError, 'flagged execution evidence'):
            e.simulate(orders(), flagged)

    def test_slippage_both_sides_is_adverse_at_entry_and_exit(self):
        for side in ['LONG','SHORT']:
            trade, _ = e.simulate(orders(side), minutes(), slippage_bps=2.)
            sign = 1 if side=='LONG' else -1
            self.assertAlmostEqual(trade.entry_price.iloc[0], 100*(1+sign*.0002))
            self.assertAlmostEqual(trade.exit_price.iloc[0], 100*(1-sign*.0002))
            self.assertLess(trade.portfolio_net_profit_rupees.iloc[0], -449.9)
            self.assertEqual(trade.cost_rs.iloc[0], 250.)

    def test_exit_gap_uses_adverse_open(self):
        frame = minutes()
        frame.loc[2, ['open','high','low','close']] = [97.,97.1,96.9,97.]
        trade, _ = e.simulate(orders(), frame)
        self.assertEqual(trade.exit_reason.iloc[0], 'STOP')
        self.assertEqual(trade.exit_price.iloc[0], 97.)
        self.assertEqual(trade.exit_event.iloc[0], 'OPEN')

    def test_baseline_counterfactual_preserves_entries_and_changes_target_only(self):
        frame = minutes()
        frame.loc[2, ['open','high','low','close']] = [100.,101.6,99.9,101.5]
        baseline = orders().drop(columns=['confirmation_ts','confirmation_close'])
        baseline['entry_ts'] = frame.ts.iloc[1]
        baseline['entry_price'] = 100.
        baseline['native_target_pct'] = .8
        trade = e.counterfactual_exit_replay(baseline, frame)
        self.assertEqual(trade.entry_price.iloc[0], baseline.entry_price.iloc[0])
        self.assertEqual(pd.Timestamp(trade.entry_ts.iloc[0]), baseline.entry_ts.iloc[0])
        self.assertEqual(trade.exit_reason.iloc[0], 'TARGET')
        self.assertAlmostEqual(trade.exit_price.iloc[0], 101.5)
        self.assertAlmostEqual(trade.portfolio_net_profit_rupees.iloc[0], 7250.)

    def test_entry_decision_does_not_read_next_high_low_or_close(self):
        order = orders().iloc[0]
        a = e.find_entry(order, dict(ts=minutes().ts.iloc[1],open=100.))
        b = e.find_entry(order, dict(ts=minutes().ts.iloc[1],open=100.,high=np.nan,low=0.,close=1000.))
        self.assertEqual(a,b)

    def test_invalid_order_or_side_rejected(self):
        with self.assertRaisesRegex(ValueError,'SID'):
            e.simulate(pd.concat([orders(),orders()]),minutes())
        with self.assertRaisesRegex(ValueError,'side'):
            e.simulate(orders('FLAT'), minutes())


if __name__ == '__main__':
    unittest.main()
