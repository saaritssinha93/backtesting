import unittest
import pandas as pd
from . import policy


def rows():
    return pd.DataFrame([dict(day='2026-11-09',tradingsymbol=s,side=side,
        confirmation_ts=pd.Timestamp('2026-11-09 '+time,tz='Asia/Kolkata'),
        rank_score=score,open=100.,high=101.,low=99.,close=100.5,
        **{f:True for f in policy.FAMILIES},
        **{f:True for f in ['broad_pass','leader_pass','efficiency_pass','extension_pass',
            'impulse_pass','pullback_depth_pass','trend_hold_pass','reclaim_pass']})
        for time in ['09:35','09:40','09:45','09:50','09:55','10:00','10:05','10:10']
        for side in ['LONG','SHORT'] for s,score in [('A',.9),('B',.8),('C',.7),('D',.6),('E',.5),('F',.4),('G',.3)]])


class PolicyTests(unittest.TestCase):
    def test_quotas_and_dedup_are_chronological(self):
        orders,audit=policy.select(rows(),'breakout_control')
        self.assertEqual(len(orders),12)
        self.assertTrue(orders.groupby('side').size().eq(6).all())
        self.assertFalse(orders.duplicated(['tradingsymbol','side']).any())
        self.assertEqual(set(orders.tradingsymbol),set('ABCDEF'))
        self.assertEqual(audit.selected_clocks.sum(),12)

    def test_future_does_not_change_earlier_admissions(self):
        x=rows(); cutoff=x.confirmation_ts.min()+pd.Timedelta(minutes=10)
        full,_=policy.select(x,'breakout_control')
        prefix,_=policy.select(x.loc[x.confirmation_ts.le(cutoff)],'breakout_control')
        keys=['day','tradingsymbol','side','confirmation_ts']
        pd.testing.assert_frame_equal(full.loc[full.confirmation_ts.le(cutoff),keys].reset_index(drop=True),prefix[keys])

    def test_ties_are_by_symbol_and_future_outcomes_irrelevant(self):
        x=rows();x['rank_score']=.5
        a,_=policy.select(x,'leader_union')
        x=x.sample(frac=1,random_state=5)
        x['future_profit']=range(len(x))
        b,_=policy.select(x,'leader_union')
        self.assertEqual(a.tradingsymbol.tolist(),b.tradingsymbol.tolist())

    def test_no_signal_means_no_forced_trade(self):
        x=rows();x['leader_pullback']=False
        orders,audit=policy.select(x,'leader_pullback')
        self.assertEqual(len(orders),0)
        self.assertEqual(audit.signal_clocks.sum(),0)


if __name__=='__main__': unittest.main()
