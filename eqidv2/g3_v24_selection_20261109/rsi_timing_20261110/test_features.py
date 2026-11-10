import unittest
from unittest.mock import patch
from pathlib import Path

import numpy as np
import pandas as pd

from . import features as f


def history(count=65):
    close = 100 + np.sin(np.arange(count)/3.) + np.arange(count)*.02
    return pd.DataFrame(dict(ts=pd.date_range('2026-11-09 09:16', periods=count, freq='min', tz=f.IST),
        open=close-.05, high=close+.2, low=close-.2, close=close, volume=100.))


def row(frame, at=40):
    bar = frame.iloc[at]
    result = dict(sid=1, day=str(bar.ts.date()), setup_id='0956_LONG', tradingsymbol='X',
                  side='LONG', confirmation_ts=bar.ts, trigger=bar.high)
    result.update({'confirmation_'+key:bar[key] for key in f.OHLCV})
    return result


class RsiFeaturesTests(unittest.TestCase):
    def test_standard_seed_and_recurrence(self):
        prices = np.array([44.34,44.09,44.15,43.61,44.33,44.83,45.10,45.42,
                           45.84,46.08,45.89,46.03,45.61,46.28,46.28,46.00,46.03,46.41,46.22,45.64])
        actual = f.rsi_wilder(prices)
        self.assertTrue(np.isnan(actual[:14]).all())
        self.assertAlmostEqual(actual[14],70.46413502109705)
        deltas=np.diff(prices);gain=np.maximum(deltas[:14],0).mean();loss=np.maximum(-deltas[:14],0).mean()
        for i in range(15,len(prices)):
            gain=(13*gain+max(prices[i]-prices[i-1],0))/14
            loss=(13*loss+max(prices[i-1]-prices[i],0))/14
            self.assertAlmostEqual(actual[i],100*gain/(gain+loss))

    def test_flat_monotonic_and_mirror_symmetry(self):
        self.assertTrue(np.equal(f.rsi_wilder(np.ones(30)*50)[14:],50).all())
        self.assertTrue(np.equal(f.rsi_wilder(np.arange(1,31))[14:],100).all())
        self.assertTrue(np.equal(f.rsi_wilder(np.arange(30,0,-1))[14:],0).all())
        p=history().close.to_numpy()
        np.testing.assert_allclose(f.rsi_wilder(p)[14:]+f.rsi_wilder(300-p)[14:],100)

    def test_prefix_and_future_mutation(self):
        frame=history();candidate=row(frame)
        before=f.window_from_frame(frame,candidate)
        mutated=frame.copy();mutated.loc[52:,'close']=9999;mutated.loc[52:,'high']=10000
        after=f.window_from_frame(mutated,candidate)
        for key in ['timestamp_ns','rsi14','valid','close']:
            np.testing.assert_equal(before[key],after[key])
        # Computing later closes cannot alter an earlier completed RSI value.
        for at in (20,30,40,50):
            self.assertAlmostEqual(f.rsi_wilder(frame.close.iloc[:at+1])[-1],f.rsi_wilder(frame.close)[at])

    def test_exact_window_and_previous_close(self):
        frame=history();candidate=row(frame)
        result=f.window_from_frame(frame,candidate)
        self.assertTrue(result['available']);self.assertEqual(result['status'],'READY')
        self.assertEqual(len(result['timestamp_ns']),11)
        np.testing.assert_equal(np.diff(result['timestamp_ns']),f.MINUTE_NS)
        np.testing.assert_allclose(result['close'],frame.close.iloc[40:51])
        np.testing.assert_allclose(result['prior_close'],frame.close.iloc[39:50])

    def test_missing_future_bar_is_not_a_retroactive_veto(self):
        frame=history();candidate=row(frame)
        baseline=f.window_from_frame(frame,candidate)
        result=f.window_from_frame(frame.drop(index=46),candidate)
        self.assertTrue(result['available'])
        np.testing.assert_allclose(result['rsi14'][:6],baseline['rsi14'][:6])
        self.assertTrue(result['valid'][:6].all());self.assertFalse(result['valid'][6:].any())
        self.assertTrue(np.isnan(result['close'][6]));self.assertEqual(result['bar_status'][6],'MISSING_MINUTE')

    def test_future_flag_is_causal_and_invalid_prefix_unavailable(self):
        frame=history();candidate=row(frame)
        frame['gap_filled']=False;frame.loc[46,'gap_filled']=True
        result=f.window_from_frame(frame,candidate)
        self.assertTrue(result['valid'][:6].all());self.assertFalse(result['valid'][6:].any())
        frame.loc[20,'close']=np.nan
        bad=f.window_from_frame(frame,candidate)
        self.assertFalse(bad['available']);self.assertTrue(np.isnan(bad['rsi14']).all())

    def test_confirmation_mismatch_and_duplicate_fail(self):
        frame=history();candidate=row(frame);candidate['confirmation_close']+=1
        with self.assertRaisesRegex(ValueError,'confirmation close mismatch'):
            f.window_from_frame(frame,candidate)
        with self.assertRaisesRegex(ValueError,'duplicate'):
            f.window_from_frame(pd.concat([frame,frame.iloc[[20]]]),row(frame))

    def test_insufficient_warmup(self):
        frame=history();result=f.window_from_frame(frame,row(frame,5))
        self.assertFalse(result['available'])
        self.assertFalse(result['valid'][:9].any());self.assertTrue(result['valid'][9:].all())

    def test_batch_loading_uses_expiry_but_checks_original_confirmation(self):
        frame=history();candidate=row(frame);orders=pd.DataFrame([candidate]);path=Path('unused.parquet')
        proof=dict(stock_sha256='abc',stock_path=str(path))
        def load(actual_path,rows,source):
            self.assertEqual(actual_path,path)
            self.assertEqual(rows[0]['confirmation_ts'],candidate['confirmation_ts']+pd.Timedelta(minutes=10))
            return frame,proof
        with patch.object(f.ranking_features,'_inventory_with_missing',return_value=({path:[candidate]},{path:proof},[],[])), \
                patch.object(f.stock_features,'_prepared_frame',side_effect=load):
            windows,audit,proofs=f.build_rsi_windows(orders)
        self.assertEqual(set(windows),{1});self.assertTrue(audit.available.iloc[0])
        self.assertEqual(windows[1]['timestamp_ns'][0],candidate['confirmation_ts'].value)
        self.assertEqual(len(proofs),1)

    def test_missing_source_retains_inventory(self):
        candidate=row(history());orders=pd.DataFrame([candidate])
        with patch.object(f.ranking_features,'_inventory_with_missing',return_value=({}, {}, [{'sid':1}], [{'kind':'MISSING_SEALED_SOURCE'}])):
            windows,audit,proofs=f.build_rsi_windows(orders)
        self.assertEqual(set(windows),{1});self.assertFalse(windows[1]['available'])
        self.assertEqual(audit.status.iloc[0],'MISSING_SEALED_SOURCE')
        self.assertTrue(np.isnan(windows[1]['rsi14']).all());self.assertEqual(len(proofs),1)


if __name__=='__main__':
    unittest.main()
