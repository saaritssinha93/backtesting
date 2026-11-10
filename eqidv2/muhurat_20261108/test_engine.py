import copy
import json
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from . import engine as e


def fixture():
    old=pd.date_range('2026-11-06 09:16', '2026-11-06 15:30', freq='min',tz=e.IST)
    prior=pd.DataFrame(dict(ts=old,open=100.,high=100.01,low=99.99,close=100.,volume=100.))
    now=pd.date_range(e.at('14:21'),e.at('15:10'),freq='min')
    n=np.arange(len(now))
    current=pd.DataFrame(dict(ts=now,open=100.+n*.2,high=100.+(n+1)*.2+.01,
        low=100.+n*.2-.01,close=100.+(n+1)*.2,volume=600.))
    minute=pd.concat([prior,current],ignore_index=True)
    ends=pd.date_range(e.at('14:25'),e.at('15:10'),freq='5min')
    futures=pd.DataFrame(dict(ts=ends,open=100.,high=101.,low=99.,close=100.,oi=1005.*1.002**np.arange(len(ends))))
    nifty=futures.copy();nifty['close']=99.9
    return dict(universe=pd.DataFrame([
        dict(underlying='ABC',equity_symbol='ABC',futures_tradingsymbol='ABCNOVFUT',is_index_future=False),
        dict(underlying='NIFTY',equity_symbol='',futures_tradingsymbol='NIFTYNOVFUT',is_index_future=True)]),
        equity_1m={'ABC':minute},futures_5m={'ABCNOVFUT':futures,'NIFTYNOVFUT':nifty},
        opening_oi={'ABC':dict(oi=1000.,candle_start=e.at('14:20').isoformat(),
            candle_end=e.at('14:21').isoformat(),futures_tradingsymbol='ABCNOVFUT')})


def order(side='LONG',start='14:27',symbol='ABC'):
    pair=next(p for p in e.build_plan()['pairs'] if p['actual_signal_end']=='14:25' and p['donor']['side']==side)
    return e._new_order(dict(tradingsymbol=symbol,trigger=100.),pair,e.at(start),'paper')


def bar(op=100.,high=100.1,low=99.9,close=100.):
    return dict(open=op,high=high,low=low,close=close)


class PlanTests(unittest.TestCase):
    def test_all18_pairs_nine_real_clocks_and_three_missing_donors(self):
        plan=e.build_plan()
        self.assertEqual(plan['total_pairs'],18)
        self.assertEqual(plan['inherited_pairs'],8)
        self.assertEqual(plan['signal_clocks'],['14:25','14:30','14:35','14:40','14:45','14:50','14:55','15:00','15:05'])
        nearest=[p for p in plan['pairs'] if p['mapping']=='NEAREST_EARLIER_SAME_SIDE_DONOR']
        self.assertEqual({p['actual_setup_id'] for p in nearest},{'1456_SHORT','1501_SHORT','1506_SHORT'})
        self.assertTrue(all(p['donor_setup_id']=='0951_SHORT' for p in nearest))
        self.assertFalse(plan['execution_authority'])

    def test_approved_morning_mapping_preserves_source_rules(self):
        plan=e.build_plan()
        expected=[
            ('14:25','09:25','0926_LONG','0926_SHORT'),
            ('14:30','09:30','0931_LONG','0931_SHORT'),
            ('14:35','09:35','0936_LONG','0936_SHORT'),
            ('14:40','09:40','0941_LONG','0941_SHORT'),
            ('14:45','09:45','0946_LONG','0946_SHORT'),
            ('14:50','09:50','0951_LONG','0951_SHORT'),
            ('14:55','09:55','0956_LONG','0951_SHORT'),
            ('15:00','10:00','1001_LONG','0951_SHORT'),
            ('15:05','10:05','1006_LONG','0951_SHORT')]
        observed=[]
        for long,short in zip(plan['pairs'][::2],plan['pairs'][1::2]):
            self.assertEqual(long['logical_signal_end'],short['logical_signal_end'])
            observed.append((long['actual_signal_end'],long['logical_signal_end'],
                             long['donor_setup_id'],short['donor_setup_id']))
        self.assertEqual(observed,expected)
        for pair in plan['pairs']:
            source=e.friday if pair['enabled_friday'] else e.donor
            spec=next(s for s in source.ACTIVE_SETUPS if s.setup_id==pair['donor_setup_id'])
            self.assertEqual(pair['donor'],e.asdict(spec))

    def test_previous_mapping_state_cannot_be_reused(self):
        with patch.object(e,'SHIFT',pd.Timedelta(minutes=305)):
            old=e.evaluate({},e.at('10:00'))
        with self.assertRaisesRegex(ValueError,'fingerprint'):
            e.evaluate({},e.at('10:01'),state=old['state'])

    def test_scope_rejects_other_date_live_and_naive_clock(self):
        for asof,mode in [(pd.Timestamp('2026-11-09 10:00',tz=e.IST),'paper'),(e.at('14:30'),'live'),(pd.Timestamp('2026-11-08 10:00'),'paper')]:
            with self.subTest(asof=asof,mode=mode),self.assertRaises(ValueError):
                e.evaluate({},asof,mode=mode)

    def test_writes_are_json_serializable_and_preopen_has_no_decisions(self):
        result=e.evaluate(fixture(),e.at('10:00'))
        json.dumps(result['state'],allow_nan=False)
        self.assertEqual(result['summary']['decided_pairs'],0)
        self.assertFalse(result['summary']['complete'])


class FeatureTests(unittest.TestCase):
    def test_opening_uses_cash_open_and_firstminute_oi(self):
        cache,nifty=e._prepare(fixture(),e.at('14:26'))
        pair=e.build_plan()['pairs'][0]
        row,reason=e._candidate('ABC',cache['ABC'],pair,nifty)
        self.assertEqual(reason,'ELIGIBLE')
        self.assertAlmostEqual(row['price_change_pct'],1.)
        self.assertAlmostEqual(row['oi_change_pct'],.5)
        self.assertEqual(row['actual_signal_timestamp'],e.at('14:25').isoformat())
        self.assertEqual(row['signal_timestamp'],e.at('09:25').isoformat())

    def test_wrong_opening_oi_time_rejects_firstslot_only(self):
        frames=fixture();frames['opening_oi']['ABC']['candle_end']=e.at('14:22').isoformat()
        cache,nifty=e._prepare(frames,e.at('14:31'))
        plan=e.build_plan()
        self.assertIn('OI',e._candidate('ABC',cache['ABC'],plan['pairs'][0],nifty)[1])
        self.assertIsNotNone(e._candidate('ABC',cache['ABC'],plan['pairs'][2],nifty)[0])

    def test_future_mutation_cannot_change_earlier_decision(self):
        frames=fixture();changed=copy.deepcopy(frames)
        mask=changed['equity_1m']['ABC'].ts.gt(e.at('14:26'))
        changed['equity_1m']['ABC'].loc[mask,['open','high','low','close']]*=10
        a=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        b=e.evaluate(changed,e.at('14:26')+pd.Timedelta(seconds=4))
        self.assertEqual(a['state'],b['state'])

    def test_malformed_stock_does_not_block_healthy_stock(self):
        frames=fixture()
        frames['universe']=pd.concat([frames['universe'],pd.DataFrame([dict(underlying='BAD',equity_symbol='BAD',futures_tradingsymbol='BADNOVFUT',is_index_future=False)])],ignore_index=True)
        frames['equity_1m']['BAD']=pd.DataFrame([dict(open=1.,high=2.,low=1.,close=2.,volume=1.)])
        result=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        self.assertEqual(result['summary']['selected_orders'],1)
        self.assertIn('MALFORMED_STOCK_SOURCE',set(result['audit'].reason))

    def test_inherited_pair_uses_friday_admission(self):
        with patch.object(e.friday,'passes_selected_filters',return_value=False) as check:
            cache,nifty=e._prepare(fixture(),e.at('14:26'))
            _,reason=e._candidate('ABC',cache['ABC'],e.build_plan()['pairs'][0],nifty)
            self.assertEqual(reason,'DONOR_FILTER_REJECTED')
            check.assert_called_once()

    def test_missing_nifty_context_does_not_block_opening_long(self):
        frames=fixture();frames['futures_5m'].pop('NIFTYNOVFUT')
        cache,nifty=e._prepare(frames,e.at('14:26'))
        long,short=e.build_plan()['pairs'][:2]
        self.assertEqual(e._candidate('ABC',cache['ABC'],long,nifty)[1],'ELIGIBLE')
        self.assertEqual(e._candidate('ABC',cache['ABC'],short,nifty)[1],'MISSING_REQUIRED_NIFTY_CONTEXT')


class PaperStateTests(unittest.TestCase):
    def test_new_paper_decision_cannot_fill_already_elapsed_entry_bar(self):
        frames=fixture()
        first=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        self.assertEqual(first['summary']['selected_orders'],1)
        self.assertEqual(first['summary']['filled_trades'],0)
        second=e.evaluate(frames,e.at('14:27')+pd.Timedelta(seconds=4),state=first['state'])
        self.assertEqual(second['summary']['filled_trades'],0)
        third=e.evaluate(frames,e.at('14:28')+pd.Timedelta(seconds=4),state=second['state'])
        self.assertEqual(third['summary']['filled_trades'],1)
        trade=third['trades'].iloc[0]
        self.assertEqual(trade.entry_bar_end,e.at('14:28').isoformat())
        self.assertGreaterEqual(e.stamp(trade.entry_ts),e.stamp(trade.first_seen))

    def test_repeated_cycle_never_duplicates_signal_or_trade(self):
        frames=fixture();now=e.at('14:28')+pd.Timedelta(seconds=4)
        first=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        second=e.evaluate(frames,now,state=first['state'])
        third=e.evaluate(frames,now,state=second['state'])
        self.assertEqual(second['state'],third['state'])
        self.assertEqual(len(third['trades']),1)

    def test_late_start_misses_paper_slot_but_replay_can_evaluate_it(self):
        frames=fixture()
        paper=e.evaluate(frames,e.at('14:28'))
        replay=e.evaluate(frames,e.at('14:28'),mode='replay')
        self.assertEqual(paper['state']['decisions']['1426_LONG']['status'],'MISSED_PUBLICATION_WINDOW')
        self.assertEqual(paper['summary']['filled_trades'],0)
        self.assertEqual(replay['summary']['filled_trades'],1)

    def test_cross_minute_fetch_waits_on_watermark_then_processes_next_cycle(self):
        frames=fixture()
        first=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        frames['source_completed_through']=e.at('14:27').isoformat()
        second=e.evaluate(frames,e.at('14:28')+pd.Timedelta(seconds=4),state=first['state'])
        self.assertEqual(second['trades'].iloc[0].status,'PENDING_ENTRY')
        self.assertFalse(second['trades'].iloc[0].data_incomplete)
        frames['source_completed_through']=e.at('14:28').isoformat()
        third=e.evaluate(frames,e.at('14:28')+pd.Timedelta(seconds=15),state=second['state'])
        self.assertEqual(third['summary']['filled_trades'],1)

    def test_latest_bar_delayed_is_waiting_but_internal_entry_gap_unresolved(self):
        frames=fixture();first=e.evaluate(frames,e.at('14:26')+pd.Timedelta(seconds=4))
        late=copy.deepcopy(frames)
        late['equity_1m']['ABC']=late['equity_1m']['ABC'].loc[late['equity_1m']['ABC'].ts.le(e.at('14:27'))]
        waiting=e.evaluate(late,e.at('14:28')+pd.Timedelta(seconds=4),state=first['state'])
        self.assertTrue(waiting['trades'].iloc[0].waiting_for_data)
        recovered=e.evaluate(frames,e.at('14:28')+pd.Timedelta(seconds=30),state=waiting['state'])
        self.assertEqual(recovered['summary']['filled_trades'],1)
        gap=copy.deepcopy(frames)
        gap['equity_1m']['ABC']=gap['equity_1m']['ABC'].loc[gap['equity_1m']['ABC'].ts.ne(e.at('14:28'))]
        blocked=e.evaluate(gap,e.at('14:29')+pd.Timedelta(seconds=4),state=first['state'])
        self.assertEqual(blocked['trades'].iloc[0].status,'UNRESOLVED_ENTRY_DATA')
        later=e.evaluate(frames,e.at('14:30'),state=blocked['state'])
        self.assertEqual(later['summary']['filled_trades'],0)

    def test_empty_source_cannot_be_published_as_complete_zero_trade_day(self):
        result=e.evaluate({},e.at('15:40'),mode='replay')
        self.assertFalse(result['summary']['complete'])
        self.assertEqual(result['summary']['missing_or_missed_pairs'],18)

    def test_state_date_fingerprint_and_clock_are_checked(self):
        first=e.evaluate({},e.at('10:00'))
        with self.assertRaises(ValueError):e.evaluate({},e.at('09:59'),state=first['state'])
        first['state']['fingerprint']='changed'
        with self.assertRaises(ValueError):e.evaluate({},e.at('10:01'),state=first['state'])


class ExecutionTests(unittest.TestCase):
    def test_long_short_gap_fills_and_stopfirst_ambiguity(self):
        for side in ('LONG','SHORT'):
            o=order(side)
            book={o['signal_id']:o}
            opening=100.2 if side=='LONG' else 99.8
            e._advance(o,bar(opening,104.,96.,100.),e.at('14:28'),book)
            self.assertTrue(o['filled'])
            self.assertEqual(o['entry_price'],opening)
            self.assertEqual(o['status'],'CLOSED')
            self.assertEqual(o['exit_reason'],'STOP')
            self.assertTrue(o['same_bar_stop_target_ambiguous'])

    def test_same_symbol_open_blocks_second_setup(self):
        first=order();book={first['signal_id']:first}
        e._advance(first,bar(),e.at('14:28'),book)
        second=order();second['signal_id']+='SECOND';book[second['signal_id']]=second
        e._advance(second,bar(),e.at('14:29'),book)
        self.assertEqual(second['status'],'BLOCKED_PORTFOLIO')
        self.assertEqual(second['status_reason'],'SAME_SYMBOL_OPEN')

    def test_capital_limit_blocks_eleventh_position(self):
        book={}
        for i in range(11):
            o=order(symbol=f'S{i}');book[o['signal_id']]=o
            e._advance(o,bar(),e.at('14:28'),book)
        self.assertEqual(sum(x['status']=='OPEN' for x in book.values()),10)
        self.assertEqual(o['status_reason'],'CAPITAL_LIMIT')

    def test_no_time_exit_without_exact1510_candle(self):
        o=order();book={o['signal_id']:o}
        e._advance(o,bar(),e.at('14:28'),book)
        e._advance(o,None,e.at('15:10'),book)
        self.assertEqual(o['status'],'OPEN')
        self.assertTrue(o['data_incomplete'])
        e._advance(o,bar(close=100.1),e.at('15:10'),book)
        self.assertEqual(o['status'],'CLOSED')
        self.assertEqual(o['exit_reason'],'TIME_EXIT_1510')
        self.assertEqual(o['exit_ts'],e.at('15:10').isoformat())


if __name__=='__main__':
    unittest.main()
