"""Dynamic day-wise reporting and honest qualification for entry-relative stops."""
from pathlib import Path
import re
import tempfile
import unittest

import pandas as pd

from g3_v23_adx_rsi_20261106.entry_sl_20261106.reporting import (
    BASELINE,compare_days,summarize_days,write_reports)


def fixture():
    days=pd.bdate_range(end='2026-11-05',periods=66).strftime('%Y-%m-%d').tolist()
    daily,changes=[],[]
    for name in (BASELINE,'mae_q95'):
        sid=0
        for index,day in enumerate(days):
            n=0 if index==0 else (1 if index%2 else 9)
            wins=int(n>0)
            daily.append(dict(policy=name,day=day,trades=n,wins=wins,losses=n-wins,
                win_rate_pct=100*wins/n if n else 0,net_pnl_rs=100*wins-10*(n-wins),
                gross_loss_rs=10*(n-wins)))
            for j in range(n):
                value=100 if j==0 else -10
                changes.append(dict(policy=name,day=day,sid=sid,
                    baseline_pnl_rs=value,candidate_pnl_rs=value,pnl_change_rs=0,exit_changed=False))
                sid+=1
    daily,changes=pd.DataFrame(daily),pd.DataFrame(changes)
    overall=pd.DataFrame([dict(policy=name,period='full_66',**summarize_days(part))
                          for name,part in daily.groupby('policy')])
    config=dict(selected_policy=BASELINE,training_candidate=BASELINE,comparison_candidate='mae_q95',
                promotion_pass=False,historydays=days,rejection_reasons=['No fit-period candidate qualified'])
    return overall,daily,changes,config


class EntryStopReportingTests(unittest.TestCase):
    def test_all_66_rows_zero_day_weighted_rate_and_no_false_improvement(self):
        overall,daily,changes,config=fixture()
        with tempfile.TemporaryDirectory() as folder:
            write_reports(folder,overall,daily,changes,config,{})
            p=Path(folder)
            table=(p/'DAY_WISE_RESULTS.md').read_text(encoding='utf-8')
            self.assertEqual(len(re.findall(r'^\| 2026-',table,re.M)),66)
            self.assertIn('**20.25%**',table)
            self.assertEqual(pd.read_csv(p/'day_wise_results.csv').trades_v23.iloc[0],0)
            report=(p/'REPORT.md').read_text(encoding='utf-8')
            self.assertIn('No entry-stop alternative qualified',report)
            self.assertIn('The baseline already uses the entry price',report)
            self.assertIn('44 sessions',report)
            self.assertNotIn('earlier 59',report)
            self.assertNotIn('Qualified retrospective candidate:',report)
            self.assertIn('Exploratory comparison only',(p/'CANDIDATE_DAY_WISE_RESULTS.md').read_text(encoding='utf-8'))

    def test_daily_count_redistribution_rejected(self):
        _,daily,_,_=fixture()
        base=daily.loc[daily.policy.eq(BASELINE)].reset_index(drop=True)
        changed=base.copy()
        changed.loc[1,['trades','losses']]+=1
        changed.loc[2,['trades','losses']]-=1
        changed['win_rate_pct']=changed.wins.div(changed.trades).fillna(0)*100
        with self.assertRaisesRegex(ValueError,'daily trade count'):
            compare_days(base,changed)

    def test_failed_promotion_cannot_select_new_stop(self):
        overall,daily,changes,config=fixture()
        config['selected_policy']='mae_q95'
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'Failed promotion'):
            write_reports(folder,overall,daily,changes,config,{})

    def test_aggregate_mismatch_rejected(self):
        overall,daily,changes,config=fixture()
        overall.loc[overall.policy.eq('mae_q95'),'net_pnl_rs']+=1
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'reconciliation'):
            write_reports(folder,overall,daily,changes,config,{})

    def test_trade_identity_change_with_same_totals_rejected(self):
        overall,daily,changes,config=fixture()
        idx=changes.loc[changes.policy.eq('mae_q95')].index[0]
        changes.loc[idx,'sid']=999999
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'identities changed'):
            write_reports(folder,overall,daily,changes,config,{})

    def test_trade_net_change_mismatch_rejected(self):
        overall,daily,changes,config=fixture()
        changes.loc[changes.policy.eq('mae_q95'),'candidate_pnl_rs']+=1
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'does not reconcile'):
            write_reports(folder,overall,daily,changes,config,{})

    def test_declared_history_mismatch_rejected(self):
        overall,daily,changes,config=fixture()
        config['historydays']=config['historydays'][:-1]
        with tempfile.TemporaryDirectory() as folder,self.assertRaisesRegex(ValueError,'declared history'):
            write_reports(folder,overall,daily,changes,config,{})


if __name__=='__main__':
    unittest.main()
