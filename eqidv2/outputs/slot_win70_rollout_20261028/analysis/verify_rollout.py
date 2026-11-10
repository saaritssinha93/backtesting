"""Independent read-only reconciliation of the requested-ten rollout."""
from pathlib import Path
import hashlib
import json
import math
import sys
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT))
import strategy_slot_selection as policy

OUT=Path(__file__).resolve().parent
RUN=ROOT/'outputs/slot_win70_rollout_20261028/replay/requested_ten_cli_verified'
SOURCE=Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2')
TARGETS={'0931_LONG','0931_SHORT','1001_LONG','1051_LONG','1111_SHORT','1146_SHORT','1226_SHORT','1231_SHORT','1301_SHORT','1321_SHORT'}

def flag(series):
    values=series.astype(str).str.lower()
    assert values.isin(['true','false']).all()
    return values.eq('true')

def main():
    manifest=json.loads((RUN/'artifact_manifest.json').read_text())
    for name,digest in manifest['files'].items():
        assert Path(name).name==name
        assert hashlib.sha256((RUN/name).read_bytes()).hexdigest()==digest,name
    summary=json.loads((RUN/'summary.json').read_text())
    assert summary['slot_policy_fingerprint']==policy.policy_fingerprint()
    days=summary['days']; assert len(days)==59
    comparisons=pd.read_csv(RUN/'overall_comparison.csv')
    daywise=pd.read_csv(RUN/'daywise_comparison.csv')
    audit=[]
    for strategy,filename in [('G3','g3_reference_portfolio_trades.csv'),('G7_V4_LOCK2','portfolio_trades.csv')]:
        source=pd.read_csv(SOURCE/filename,low_memory=False).set_index('sid').sort_index()
        selected=pd.read_csv(RUN/filename,low_memory=False).set_index('sid').sort_index()
        assert source.index.equals(selected.index)
        executed=flag(selected.portfolio_executed)
        enabled=policy.enabled_setup_ids(strategy)
        represented=set(source.setup_id)
        assert represented-TARGETS <= enabled, 'Unrequested setup disabled'
        expected=set(represented-TARGETS)
        for setup_id,part in source[source.setup_id.isin(TARGETS)].groupby('setup_id'):
            executed_before=part[flag(part.portfolio_executed)]
            wins=int(executed_before.portfolio_net_profit_rupees.gt(1e-8).sum())
            count=len(executed_before)
            if count>0 and 100*wins>=70*count: expected.add(setup_id)
        assert enabled==expected, 'Policy is not the requested-ten-only selection'
        assert set(selected.loc[executed,'setup_id']).issubset(enabled)
        assert flag(selected.slot_policy_allowed).equals(selected.setup_id.isin(enabled))
        assert selected.setup_id.equals(source.setup_id)
        for col in ['entry_price','exit_price','gross_return_pct']:
            pd.testing.assert_series_equal(selected[col],source[col],check_dtype=False,atol=1e-9,rtol=0)
        for col in ['entry_ts','exit_ts']:
            assert pd.to_datetime(selected[col],utc=True).equals(pd.to_datetime(source[col],utc=True))
        pnl=selected.loc[executed,'portfolio_net_profit_rupees']
        assert (selected.loc[~executed,'portfolio_net_profit_rupees']==0).all()
        for period,period_days in [('all',days),('october',[d for d in days if d.startswith('2026-10')])]:
            rows=selected.loc[executed&selected.day.isin(period_days)]
            row=comparisons.loc[comparisons.strategy.eq(strategy)&comparisons.period.eq(period)&comparisons.variant.eq('selected_slots')&comparisons.cost_bps.eq(5)].iloc[0]
            assert len(rows)==row.trades
            assert rows.portfolio_net_profit_rupees.gt(1e-8).sum()==row.wins
            assert rows.portfolio_net_profit_rupees.lt(-1e-8).sum()==row.losses
            assert math.isclose(rows.portfolio_net_profit_rupees.sum(),row.net_pnl_rs,abs_tol=1e-6)
        for day in days:
            rows=selected.loc[executed&selected.day.eq(day)]
            row=daywise.loc[daywise.strategy.eq(strategy)&daywise.day.eq(day)&daywise.variant.eq('selected_slots')].iloc[0]
            assert len(rows)==row.trades
            assert math.isclose(rows.portfolio_net_profit_rupees.sum(),row.net_pnl_rs,abs_tol=1e-6)
        if strategy=='G7_V4_LOCK2':
            expected_permission=flag(source.base_entry_allowed)&selected.setup_id.isin(enabled)
            assert flag(selected.base_entry_allowed).equals(expected_permission)
            assert flag(selected.slot_policy_source_base_entry_allowed).equals(flag(source.base_entry_allowed))
            for day in days:
                part=selected[selected.day.eq(day)]
                losses=part[flag(part.portfolio_executed)&part.portfolio_net_profit_rupees.lt(-1e-8)].copy()
                losses['exit']=pd.to_datetime(losses.exit_ts,utc=True)
                losses=losses.sort_values('exit')
                locks=pd.to_datetime(part.loss_lock_ts,utc=True)
                if len(losses)>=2:
                    expected=losses.exit.iloc[1]
                    assert locks.eq(expected).all()
                    entered=pd.to_datetime(part.loc[flag(part.portfolio_executed),'allocation_entry_ts'],utc=True)
                    assert entered.le(expected).all()
                else: assert locks.isna().all()
        old=flag(source.portfolio_executed)
        audit.append(dict(strategy=strategy,source_rows=len(source),source_executions=int(old.sum()),selected_executions=int(executed.sum()),
                          wins=int(pnl.gt(1e-8).sum()),losses=int(pnl.lt(-1e-8).sum()),net_pnl_rs=float(pnl.sum()),
                          removed_executions=int((old&~executed).sum()),newly_admitted=int((~old&executed).sum()),
                          all_frozen_paths_unchanged=True,period_checks=2,daywise_checks=len(days),
                          exact_slot_inventory_pass=True,causal_loss_lock_pass=True))
    result=dict(status='PASS',verified_artifact_members=len(manifest['files']),strategies=audit,
                retrospective=True,independent_holdout=False)
    result['scope']='REQUESTED_TEN_ONLY_OTHER_41_UNCHANGED'
    (OUT/'final_reconciliation_requested_ten.json').write_text(json.dumps(result,indent=2),encoding='utf8')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
