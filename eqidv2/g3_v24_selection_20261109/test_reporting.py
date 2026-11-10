import copy
from pathlib import Path
import re
import tempfile
import unittest

import pandas as pd

from .reporting import BASELINE, POLICIES, choose_diagnostic, metric, publish


def fixture():
    days = pd.bdate_range('2026-07-29', periods=68).strftime('%Y-%m-%d').tolist()
    def trade(sid, day, pnl):
        return dict(sid=sid,day=day,setup_id='0926_LONG',tradingsymbol=f'S{sid}',side='LONG',
            entry_ts=day+'T09:27:00+05:30',exit_ts=day+'T10:00:00+05:30',
            entry_price=100.,exit_price=100.+(pnl+250)/5000,exit_reason='TARGET',
            portfolio_net_profit_rupees=pnl)
    base = pd.DataFrame([trade(1,days[0],100.),trade(2,days[1],-50.),
        trade(3,days[2],1e-8),trade(4,days[44],-300.),trade(5,days[60],200.)])
    books = {p:base.copy(deep=True) for p in POLICIES}
    # First44 improves by100; validation deteriorates by1000. Choice must remain fit-only.
    books['stretch_rank'] = pd.concat([base.loc[base.sid.ne(2)&base.sid.ne(4)],
        pd.DataFrame([trade(6,days[1],50.),trade(7,days[44],-1300.)])],ignore_index=True)
    orders = {p:b[['sid','day','setup_id','tradingsymbol','side']].copy() for p,b in books.items()}
    return books,orders,days,dict(execution_authority=False,live_changed=False),[]


class ReportingTests(unittest.TestCase):
    def test_flat_tolerance_zero_trade_days_and_drawdown(self):
        books,orders,days,_,_=fixture()
        result=metric(books[BASELINE],len(orders[BASELINE]),days)
        self.assertEqual((result['wins'],result['losses'],result['breakeven_trades']),(2,2,1))
        self.assertEqual(result['win_rate_pct'],40.)
        self.assertEqual(result['daily_close_drawdown_rs'],350.)
        self.assertEqual(result['net_pnl_rs'],-50.)
        self.assertEqual(result['sessions'],68)

    def test_all_days_all_arms_and_change_attribution_reconcile(self):
        args=fixture()
        with tempfile.TemporaryDirectory() as folder:
            summary=publish(folder,*args)
            self.assertEqual(summary['diagnostic_candidate'],'stretch_rank')
            self.assertFalse(summary['automatic_promotion'])
            self.assertEqual(summary['comparison_delta_net_pnl_rs'],-900.)
            table=(Path(folder)/'DAY_WISE_RESULTS.md').read_text()
            self.assertEqual(len(re.findall(r'^\| 2026-',table,re.M)),68)
            daily=pd.read_csv(Path(folder)/'daily.csv')
            self.assertEqual(len(daily),68*5)
            row=next(x for x in summary['change_summary'] if x['policy']=='stretch_rank')
            self.assertEqual(row['removed_losers'],2)
            self.assertEqual(row['added_winners'],1)
            self.assertEqual(row['added_losers'],1)
            self.assertEqual(row['delta_net_pnl_rs'],-900.)
            self.assertIn('not untouched', (Path(folder)/'REPORT.md').read_text())
            with self.assertRaisesRegex(ValueError,'already exist'):
                publish(folder,*args)
        # Input PnL is not normalized in-place.
        self.assertEqual(float(args[0][BASELINE].loc[lambda f:f.sid.eq(3),'portfolio_net_profit_rupees'].iloc[0]),1e-8)

    def test_common_trade_cannot_change_exit_under_ranking(self):
        args=list(fixture())
        args[0]['combined_rank'].loc[0,'exit_price']+=1
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'Common trade economics'):
            publish(folder,*args)

    def test_unadmitted_fill_and_out_of_inventory_date_rejected(self):
        args=list(fixture()); args[1][BASELINE]=args[1][BASELINE].iloc[1:]
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'not an admitted'):
            publish(folder,*args)
        args=list(fixture()); args[2]=args[2][1:]
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'outside declared'):
            publish(folder,*args)

    def test_later_only_winner_cannot_be_diagnostic_choice(self):
        rows=[]
        for name in POLICIES:
            rows.append(dict(policy=name,period='fit_44',net_pnl_rs=100.,gross_loss_rs=10.,daily_close_drawdown_rs=10.))
            rows.append(dict(policy=name,period='later_9',net_pnl_rs=10000. if name=='combined_rank' else 100.,
                             gross_loss_rs=0.,daily_close_drawdown_rs=0.))
        self.assertEqual(choose_diagnostic(pd.DataFrame(rows)),BASELINE)

    def test_live_authority_is_rejected(self):
        args=list(fixture());args[3]['execution_authority']=True
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'backtest-only'):
            publish(folder,*args)


if __name__ == '__main__':
    unittest.main()
