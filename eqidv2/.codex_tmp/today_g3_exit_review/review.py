from pathlib import Path
import sys,json
from datetime import date
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from research import g3_trade_monitor_backtest as monitor
from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv

RUN=Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\production\runs\2026-10-15\20261015T170545010772_39928700')
OUT=Path(__file__).parent
payload=json.loads((RUN/'run_manifest.json').read_text())
orders=read_frozen_csv(RUN/'selected_orders.csv')
saved=read_frozen_csv(RUN/'portfolio_trades.csv')
paths,proof=history._daily_paths(RUN,payload,orders)
fresh=history._simulate(orders,paths,[date(2026,10,15)],history.contract)
history._same_native(fresh,saved)
history._same_allocation(fresh,saved)
ledgers,signals=monitor.evaluate_policies(fresh,paths)
signals.to_csv(OUT/'monitor_signals.csv',index=False)
results=[]
for policy,ledger in ledgers.items():
    ledger.to_csv(OUT/f'{policy}.csv',index=False)
    executed=ledger.loc[ledger.portfolio_executed]
    results.append(dict(policy=policy,trades=len(executed),wins=int(executed.portfolio_net_profit_rupees.gt(0).sum()),losses=int(executed.portfolio_net_profit_rupees.lt(0).sum()),net=float(executed.portfolio_net_profit_rupees.sum()),stops=int(executed.exit_reason.str.contains('STOP').sum())))
print(pd.DataFrame(results).to_string(index=False))
comparisons=[]
for row in fresh.loc[fresh.portfolio_executed].itertuples(index=False):
    p=paths[int(row.sid)]
    start=int(row.entry_path_index)+1
    end=int(row.exit_path_index)
    sign=1 if row.side=='LONG' else -1
    # Exclude entry bar (intrabar order is unknown) and exit bar (could follow exit).
    close=np.asarray(p['close'][start:end])
    favorable=np.asarray(p['high' if sign==1 else 'low'][start:end])
    adverse=np.asarray(p['low' if sign==1 else 'high'][start:end])
    rec=dict(sid=row.sid,symbol=row.tradingsymbol,setup=row.setup_id,side=row.side,entry=str(row.entry_ts),exit=str(row.exit_ts),target_pct=row.target_pct,baseline_net=row.portfolio_net_profit_rupees,pre_exit_mfe_pct=float(max(0,max(sign*(favorable/row.entry_price-1)*100,default=0))),pre_exit_mae_pct=float(min(0,min(sign*(adverse/row.entry_price-1)*100,default=0))))
    for policy,ledger in ledgers.items():
        m=ledger.loc[ledger.sid.eq(row.sid)].iloc[0]
        rec[policy+'_net']=m.portfolio_net_profit_rupees
        rec[policy+'_exit']=str(m.exit_execution_ts)
        rec[policy+'_reason']=m.exit_reason
    comparisons.append(rec)
comp=pd.DataFrame(comparisons)
comp.to_csv(OUT/'trade_exit_comparison.csv',index=False)
print(comp.to_string(index=False))

# Small transparent target caps; preserve order selection and reapply allocator.
# Native simulator supplies intrabar stop-before-target, gaps and staged-stop rules.
target_results=[]
for cap in [0.5,0.75,1.0]:
    modified=orders.copy()
    for col in ['target_pct','native_target_pct']:
        modified[col]=np.minimum(pd.to_numeric(modified[col]),cap)
    ledger=history._simulate(modified,paths,[date(2026,10,15)],history.contract)
    ledger.to_csv(OUT/f'target_cap_{cap:.2f}.csv',index=False)
    executed=ledger.loc[ledger.portfolio_executed]
    target_results.append(dict(cap_pct=cap,trades=len(executed),wins=int(executed.portfolio_net_profit_rupees.gt(0).sum()),losses=int(executed.portfolio_net_profit_rupees.lt(0).sum()),net=float(executed.portfolio_net_profit_rupees.sum()),stops=int(executed.exit_reason.str.contains('STOP').sum())))
print('TARGET CAPS')
print(pd.DataFrame(target_results).to_string(index=False))
(OUT/'summary.json').write_text(json.dumps(dict(source=str(RUN),monitor_results=results,target_cap_results=target_results,scope='DIAGNOSTIC_TODAY_ONLY_REPLAY_FROZEN_SELECTED_ORDERS_WITH_REAPPLIED_PORTFOLIO_ALLOCATION',source_paths_verified=True),indent=2))

# Feed/decision latency sensitivity: delay each first eligible combined decision
# one additional full minute, but preserve native exits that occur meanwhile.
delayed=fresh.copy()
delayed['monitor_exit']=False
delayed['monitor_flags']='[]'
delayed['monitor_decision_at_ist']=''
delayed['monitor_open_observed_at_ist']=''
for i,row in fresh.iterrows():
    first,_=monitor.trade_advisories(row,paths[int(row.sid)])
    if not first:
        continue
    event=dict(min(first.values(),key=lambda x:(x['execution_path_index'],x['flags'])))
    path=paths[int(row.sid)]
    idx=int(event['execution_path_index'])+1
    if idx>=len(path['timestamp_ns']):
        continue
    end=pd.Timestamp(int(path['timestamp_ns'][idx]),tz='UTC').tz_convert(history.contract.IST)
    open_at=end-pd.Timedelta(minutes=1)
    if monitor._timestamp(row.exit_ts)<=open_at:
        continue
    event.update(execution_path_index=idx,next_bar_open_at_ist=open_at.isoformat(),modeled_execution_at_ist=end.isoformat(),next_bar_open_price=float(path['open'][idx]))
    monitor._apply_monitor_exit(delayed,i,event,path,'combined_one_minute_delay')
delayed=monitor._allocate(delayed,'combined_one_minute_delay')
delayed.to_csv(OUT/'combined_one_minute_delay.csv',index=False)
ledgers['combined_one_minute_delay']=delayed
stress=[]
for name,ledger in ledgers.items():
    a=ledger.loc[ledger.portfolio_executed]
    exits=a.loc[a.monitor_exit]
    for bps in [0,2,5,10]:
        increment=float((exits.portfolio_trade_exposure_rupees*bps/10000).sum())
        stress.append(dict(policy=name,extra_bps_per_monitor_exit=bps,monitor_exits=len(exits),net=float(a.portfolio_net_profit_rupees.sum()-increment)))
pd.DataFrame(stress).to_csv(OUT/'cost_latency_stress.csv',index=False)
print('LATENCY AND COST STRESS')
print(pd.DataFrame(stress).to_string(index=False))
s=json.loads((OUT/'summary.json').read_text())
s['cost_latency_stress']=stress
(OUT/'summary.json').write_text(json.dumps(s,indent=2))
