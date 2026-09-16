"""Independent artifact audit; does not import the projection implementation."""
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

BASE=Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25')
OUT=BASE/'one_year_scenarios'
p=json.loads((OUT/'projection_payload.json').read_text(encoding='utf8'))
m=p['meta']
trades=pd.read_csv(BASE/'options_trades.csv',float_precision='round_trip')
trades=trades.loc[trades.portfolio_status.eq('ADMITTED')].copy()
for col in ['entry_ts','exit_observed_ts']:
    trades[col]=pd.to_datetime(trades[col],utc=True)
checks=[]
def check(name,value):
    checks.append(dict(name=name,passed=bool(value)))
    assert value,name
def near(name,actual,expected):
    check(name,np.allclose(actual,expected,rtol=1e-10,atol=1e-6))
def fee(turnover,sell):
    regulatory=turnover*(.0003553+.000001+.000000001)
    return 20+regulatory+.18*(20+regulatory)+turnover*(.0015 if sell else .00003)

check('source_profile',m['lots']==3 and m['stop_pct']==12.5 and m['target_pct']==25)
check('calendar',len(p['history'])==13 and sum(r['trades']==0 for r in p['history'])==3)
near('history_total',trades.net_pnl.sum(),m['history_net_pnl'])
near('history_opening',1500000+trades.net_pnl.sum(),m['projection_start_equity'])
for manifest_name in ['manifest.json']:
    manifest=json.loads((OUT/manifest_name).read_text(encoding='utf8'))
    for name,expected in manifest['artifacts'].items():
        check('artifact_hash_'+name,hashlib.sha256((OUT/name).read_bytes()).hexdigest()==expected)
rng=np.random.default_rng(m['seed'])
starts=rng.integers(0,13,(m['paths'],(m['horizon']+4)//5))
draws=np.stack([(starts+i)%13 for i in range(5)],axis=2).reshape(m['paths'],-1)[:,:m['horizon']]
validation=json.loads((OUT/'validation.json').read_text(encoding='utf8'))
check('independent_draw_hash',hashlib.sha256(draws.tobytes()).hexdigest()==validation['shared_draws_sha256'])
source_count=np.array([r['trades'] for r in p['history']])
for s in p['scenarios']:
    nets=[]; required=[]; peaks=[]
    for day in [r['day'] for r in p['history']]:
        events=[]
        daytrades=trades.loc[trades.day.eq(day)].sort_values(['entry_ts','setup_id','trade_id'],kind='stable')
        for rank,t in enumerate(daytrades.to_dict('records')):
            buy=t['entry_price']*t['quantity']
            historical_gross=(t['exit_price']-t['entry_price'])*t['quantity']
            sell=buy+historical_gross*(s['fraction'] if historical_gross>0 else 1)
            debit=buy+fee(buy,False)+buy*s['extra_cost_bps_per_side']/10000
            receipt=sell-fee(sell,True)-sell*s['extra_cost_bps_per_side']/10000
            events.append((t['entry_ts'],1,rank*2,-debit,debit))
            events.append((t['exit_observed_ts'],int(t['exit_observed_ts']==t['entry_ts']),rank*2+1,receipt,-debit))
        balance=reserve=peak=need=0.
        for _,_,_,cashchange,reservechange in sorted(events):
            balance+=cashchange;reserve+=reservechange
            need=max(need,-balance);peak=max(peak,reserve)
        nets.append(balance);required.append(need);peaks.append(peak)
    nets=np.array(nets)
    eq=np.concatenate([np.full((m['paths'],1),m['projection_start_equity']),m['projection_start_equity']+np.cumsum(nets[draws],axis=1)],axis=1)
    check(s['id']+'_every_path_can_fund_every_entry',np.all(eq[:,:-1]>=np.array(required)[draws]-1e-7))
    a=next(r for r in p['annual'] if r['scenario']==s['id'] and r['sizing']=='fixed')
    ending=eq[:,-1];future=ending-m['projection_start_equity']
    dd=np.max(np.maximum.accumulate(eq,axis=1)-eq,axis=1)
    expected=dict(mean_future_pnl=future.mean(),mean_ending_equity=ending.mean(),mean_return_pct=future.mean()/m['projection_start_equity']*100,
        mean_cumulative_pnl=ending.mean()-1500000,mean_total_return_pct=(ending.mean()/1500000-1)*100,probability_loss=np.mean(future<0),
        mean_max_drawdown=dd.mean(),p90_max_drawdown=np.quantile(dd,.9),mean_trades=source_count[draws].sum(axis=1).mean(),mean_rejections=0,
        mean_peak_premium=np.array(peaks)[draws].max(axis=1).mean(),mean_lots=3)
    for q in [10,50,90]:
        expected[f'p{q}_ending_equity']=np.quantile(ending,q/100)
        expected[f'p{q}_future_pnl']=np.quantile(future,q/100)
    for key,val in expected.items():near(s['id']+'_annual_'+key,a[key],val)
    curve=[r for r in p['curves'] if r['scenario']==s['id'] and r['sizing']=='fixed']
    near(s['id']+'_all_daily_means',[r['mean_equity'] for r in curve],eq.mean(axis=0))
    for q in [10,50,90]:near(s['id']+f'_all_daily_p{q}',[r[f'p{q}_equity'] for r in curve],np.quantile(eq,q/100,axis=0))
    for r in [r for r in p['monthly'] if r['scenario']==s['id'] and r['sizing']=='fixed']:
        start=(r['model_month']-1)*21;end=start+21
        profit=eq[:,end]-eq[:,start]
        for key,val in [('mean_pnl',profit.mean()),('mean_opening_equity',eq[:,start].mean()),('mean_equity',eq[:,end].mean()),('mean_return_pct',np.mean(profit/eq[:,start])*100)]:
            near(s['id']+f'_month{r["model_month"]}_{key}',r[key],val)
        for q in [10,50,90]:near(s['id']+f'_month{r["model_month"]}_p{q}',r[f'p{q}_pnl'],np.quantile(profit,q/100))

result=dict(passed=True,checks_count=len(checks),checks=checks,payload_sha256=hashlib.sha256((OUT/'projection_payload.json').read_bytes()).hexdigest(),method='Independent fees, cash chronology, all-path funding bounds, daily P&L resampling, annual/daily/monthly means and quantiles; does not import projection code.')
(OUT/'independent_projection_audit.json').write_text(json.dumps(result,indent=2),encoding='utf8')
print(json.dumps({k:v for k,v in result.items() if k!='checks'},indent=2))
