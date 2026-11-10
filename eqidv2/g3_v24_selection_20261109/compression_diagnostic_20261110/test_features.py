import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from . import features as f


def history():
    times = pd.date_range('2026-11-06 09:16',periods=200,freq='min',tz=f.IST).append(
        pd.date_range('2026-11-09 09:16',periods=70,freq='min',tz=f.IST))
    return pd.DataFrame(dict(ts=times,open=100.,high=100.1,low=99.9,close=100.,volume=100.))


def set_close(frame,at,value):
    frame.loc[at,['open','high','low','close']] = [value,value+.1,value-.1,value]


def row(frame,at=235,side='LONG'):
    bar=frame.iloc[at]
    result=dict(sid=1,day=str(bar.ts.date()),setup_id='0951_'+side,
        tradingsymbol='X',side=side,confirmation_ts=bar.ts,
        trigger=bar.high if side=='LONG' else bar.low,volume_ratio=2.,v9_1m_volume_ratio=1.5)
    result.update({'confirmation_'+key:bar[key] for key in f.OHLCV})
    return result


class CompressionTests(unittest.TestCase):
    def test_known_bandwidth_and_own_width_excluded_from_reference(self):
        frame=history().iloc[:150].copy()
        for i in range(len(frame)):
            set_close(frame,i,100+i*.1+np.sin(i/5.))
        values=f._prepared_values(frame)
        expected=400*np.std(frame.close.iloc[:20],ddof=0)/frame.close.iloc[:20].mean()
        self.assertAlmostEqual(values['bandwidth'][19],expected)
        self.assertAlmostEqual(values['q20'][119],np.quantile(values['bandwidth'][19:119],.2))
        mutated=frame.copy();set_close(mutated,119,500.)
        other=f._prepared_values(mutated)
        self.assertEqual(values['q20'][119],other['q20'][119])
        self.assertNotEqual(values['bandwidth'][119],other['bandwidth'][119])

    def test_primary_and_long_short_symmetry(self):
        for side,price in [('LONG',102.),('SHORT',98.)]:
            frame=history();set_close(frame,235,price)
            value=f.features_at(frame,row(frame,side=side))
            self.assertTrue(value['compression_ready']);self.assertTrue(value['is_primary'])
            self.assertEqual(value['compression_label'],'COMPRESSION_BREAKOUT')
            self.assertEqual(value['first_break_side'],side)
            self.assertEqual(value['first_break_ts'],frame.ts.iloc[235].isoformat())
            self.assertEqual(value['anchor_ts'],frame.ts.iloc[234].isoformat())

    def test_confirmation_excluded_from_prior_range(self):
        frame=history();set_close(frame,235,102.)
        a=f.features_at(frame,row(frame))
        frame.loc[235,'high']=150.
        b=f.features_at(frame,row(frame))
        self.assertEqual(a['prior_range_high'],100.1)
        self.assertEqual(a['prior_range_high'],b['prior_range_high'])
        self.assertEqual(a['prior_range_low'],b['prior_range_low'])

    def test_future_mutation_and_prefix_invariance(self):
        frame=history();set_close(frame,235,102.);candidate=row(frame)
        a=f.features_at(frame,candidate)
        changed=frame.copy();changed.loc[236:,'close']=np.nan
        changed['gap_filled']=False;changed.loc[236:,'gap_filled']=True
        b=f.features_at(changed,candidate)
        c=f.features_at(frame.iloc[:236],candidate)
        pd.testing.assert_series_equal(pd.Series(a),pd.Series(b))
        pd.testing.assert_series_equal(pd.Series(a),pd.Series(c))

    def test_earlier_breakout_uses_fixed_anchor_and_first_either_side(self):
        frame=history()
        for at,price in [(232,101.),(233,101.),(234,101.1),(235,101.15)]:
            set_close(frame,at,price)
        value=f.features_at(frame,row(frame))
        self.assertEqual(value['compression_label'],'EARLIER_BREAKOUT_CONTINUATION')
        self.assertEqual(value['anchor_ts'],frame.ts.iloc[231].isoformat())
        self.assertEqual(value['first_break_ts'],frame.ts.iloc[232].isoformat())
        self.assertEqual(value['anchor_range_high'],100.1)
        # The first break was SHORT; a subsequent LONG crossing cannot be
        # relabeled as a preceding LONG breakout.
        set_close(frame,232,99.)
        other=f.features_at(frame,row(frame))
        self.assertEqual(other['first_break_side'],'SHORT')
        self.assertNotEqual(other['compression_label'],'EARLIER_BREAKOUT_CONTINUATION')

    def test_early_window_and_overnight_never_establish_band_warmup(self):
        frame=history()
        # Prior-session prices deliberately differ radically.
        frame.loc[:199,['open','high','low','close']]=[200.,200.1,199.9,200.]
        values=f._prepared_values(frame)
        self.assertTrue(np.isnan(values['bandwidth'][218]))
        self.assertAlmostEqual(values['bandwidth'][219],0.)
        self.assertEqual(values['run_length'][200],1)
        early=f.features_at(frame,row(frame,at=223))
        self.assertEqual(early['compression_label'],'EARLY_SESSION_NOT_APPLICABLE')
        self.assertFalse(early['compression_ready'])
        eligible=f.features_at(frame,row(frame,at=224))
        self.assertTrue(eligible['compression_ready'])

    def test_gap_and_flag_mark_unavailable_without_dropping_identity(self):
        frame=history();candidate=row(frame)
        for damaged in [frame.drop(index=232),frame.assign(gap_filled=frame.index==232)]:
            result=f.features_at(damaged,candidate)
            self.assertEqual(result['sid'],1)
            self.assertEqual(result['compression_label'],'UNAVAILABLE')
            self.assertEqual(result['compression_status'],'INCOMPLETE_OR_FLAGGED_CURRENT_25_BARS')

    def test_missing_reference_and_confirmation_parity(self):
        frame=history().iloc[200:].copy().reset_index(drop=True)
        result=f.features_at(frame,row(frame,at=35))
        self.assertEqual(result['compression_label'],'UNAVAILABLE')
        self.assertEqual(result['compression_status'],'INSUFFICIENT_PRIOR_100_VALID_BANDWIDTHS')
        candidate=row(frame,at=35);candidate['confirmation_close']=101.
        with self.assertRaisesRegex(ValueError,'confirmation close mismatch'):
            f.features_at(frame,candidate)

    def test_noncompressed_breakout_and_other_are_not_automatically_continuation(self):
        frame=history()
        self.assertEqual(f.features_at(frame,row(frame))['compression_label'],'OTHER_READY')
        for i in range(35):
            set_close(frame,200+i,100+.3*np.sin(i))
        set_close(frame,235,102.)
        value=f.features_at(frame,row(frame))
        self.assertEqual(value['compression_label'],'BREAKOUT_WITHOUT_COMPRESSION')
        self.assertIsNone(value['anchor_ts'])

    def test_missing_source_retains_admitted_candidate(self):
        frame=history();candidate=row(frame)
        orders=pd.DataFrame([candidate])
        with patch.object(f.source_adapter,'_inventory_with_missing',return_value=({}, {}, [{'sid':1}], [{'kind':'MISSING_SEALED_SOURCE'}])):
            audit,proofs=f.build_features(orders)
        self.assertEqual(audit.sid.tolist(),[1])
        self.assertEqual(audit.compression_label.tolist(),['UNAVAILABLE'])
        self.assertEqual(audit.compression_status.tolist(),['MISSING_SEALED_SOURCE'])
        self.assertEqual(len(proofs),1)


if __name__=='__main__':
    unittest.main()
