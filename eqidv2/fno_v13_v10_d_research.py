"""Freeze, replay, compare, and document V13-v10-D."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_d_backtest as d

c,v10,v9,r=d.c,d.v10,d.v9,d.r


def detailed_daily(ledger,days):
    rows=[];cumulative=peak=0.
    date_text=pd.to_datetime(ledger.day).dt.strftime('%Y-%m-%d')
    for day in days:
        key=str(day); selected=ledger.loc[date_text.eq(key)]; executed=selected.loc[selected.portfolio_executed]
        pnl=executed.portfolio_net_profit_rupees;net=float(pnl.sum());cumulative+=net;peak=max(peak,cumulative)
        gains=float(pnl[pnl>r.EPS].sum());losses=float(-pnl[pnl< -r.EPS].sum());reasons=executed.exit_reason.value_counts().to_dict()
        rows.append(dict(day=key,selected=len(selected),triggered=int(selected.filled.sum()),executed=len(executed),
            wins=int((pnl>r.EPS).sum()),losses=int((pnl< -r.EPS).sum()),breakeven=int((pnl.abs()<=r.EPS).sum()),
            win_rate_pct=100*float((pnl>r.EPS).mean()) if len(pnl) else np.nan,
            profit_factor=gains/losses if losses>r.EPS else np.inf if gains>r.EPS else np.nan,
            net_profit_rupees=net,cumulative_net_profit_rupees=cumulative,drawdown_rupees=peak-cumulative,
            targets=int(reasons.get('TARGET',0)),stops=int(reasons.get('STOP',0)),
            time_exits=sum(value for name,value in reasons.items() if 'TIME_EXIT' in str(name))))
    return pd.DataFrame(rows)


def run():
    output=d.DEFAULT_OUTPUT;output.mkdir(parents=True,exist_ok=True)
    source_exit=json.loads(d.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    settings=d.config(source_exit,1.2);r.dump(output/'frozen_config.json',settings)
    dataset_d=d.load_source(minimum_volume_ratio=1.2)
    result_d=d.evaluate(dataset_d,settings);r.save(output/'final/V13_V10_D',*result_d)
    dataset_c=v10.load_source();result_c=c.evaluate(dataset_c,source_exit);r.save(output/'final/V13_V10_C_CONTROL',*result_c)
    prior=pd.read_csv(c.DEFAULT_OUTPUT/'final/V13_V10_C/portfolio_trades.csv',float_precision='round_trip')
    r.dump(output/'v10_c_control_parity.json',v9.assert_control_parity(prior,result_c[1]))
    groups=r.periods(dataset_d['days']);groups.update({name:[day for day in dataset_d['days'] if str(day).startswith(prefix)]
        for name,prefix in [('JULY','2026-07'),('AUGUST','2026-08')]})
    metrics=[]
    for name,result in [('V10-C',result_c),('V10-D',result_d)]:
        for period,days in groups.items():
            for cost in [5,9]:metrics.append(dict(version=name,period=period,**r.metric(result[1],days,cost_bps=cost)))
    metrics=pd.DataFrame(metrics);metrics.to_csv(output/'comparison_metrics.csv',index=False)
    daily_d=detailed_daily(result_d[1],dataset_d['days']);daily_d.to_csv(output/'daily_detailed.csv',index=False)
    daily_c=detailed_daily(result_c[1],dataset_d['days']);
    daily_c.insert(0,'version','V10-C');daily_compare=daily_c.copy();daily_d_copy=daily_d.copy();daily_d_copy.insert(0,'version','V10-D')
    pd.concat([daily_compare,daily_d_copy],ignore_index=True).to_csv(output/'daily_comparison.csv',index=False)
    exits=result_d[1].loc[result_d[1].portfolio_executed].groupby('exit_reason').agg(
        trades=('sid','size'),net_profit_rupees=('portfolio_net_profit_rupees','sum'));exits.to_csv(output/'exit_reason_summary.csv')
    original_orders=set(dataset_c['orders'].sid.astype(int));new_orders=set(dataset_d['orders'].sid.astype(int))
    replacement=dict(c_selected=len(original_orders),d_selected=len(new_orders),retained=len(original_orders&new_orders),
                     removed=len(original_orders-new_orders),added_after_reranking=len(new_orders-original_orders),
                     all_d_confirmation_volume_pass=bool(dataset_d['orders'].v9_1m_volume_ratio.ge(1.2).all()))
    r.dump(output/'selection_attribution.json',replacement)
    show=metrics.loc[metrics.cost_bps.eq(5)&metrics.period.isin(['FULL','JULY','AUGUST','SEPTEMBER']),
        ['version','period','selected_orders','trades','wins','losses','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees']]
    day_show=daily_d[['day','selected','triggered','executed','wins','losses','win_rate_pct','profit_factor','net_profit_rupees','cumulative_net_profit_rupees','drawdown_rupees','targets','stops','time_exits']]
    report='\n\n'.join(['# V13-v10-D detailed results',
        'V10-D keeps V10-C full exits (unchanged SL, target=2xSL) and requires the completed one-minute confirmation candle volume ratio to be at least 1.20 before native per-setup ranking. Entry remains next-minute trigger with the inherited ten-minute expiry.',
        show.to_markdown(index=False,floatfmt='.2f'),'## Daywise V10-D',day_show.to_markdown(index=False,floatfmt='.2f'),
        'Selection attribution: '+json.dumps(replacement)+'.',
        'Accounting remains Rs 100,000 capital per entry, 5x exposure, Rs 300,000 portfolio, three positions and 5bps round-trip costs. Same stop-first ambiguity, adverse-gap fills and 15:15 exit. This rule was chosen after reviewing September on previously seen history; the eight executed September trades are descriptive and not independent validation.'])
    (output/'V13_V10_D_DETAILED_RESULTS.md').write_text(report+'\n',encoding='utf-8')
    sources=[Path(__file__),Path(d.__file__),Path(c.__file__),Path(v10.__file__)];snapshot=output/'source_snapshot';snapshot.mkdir(exist_ok=True)
    for path in sources+[Path('tests/test_fno_v13_v10_d.py')]:
        if path.is_file():shutil.copy2(path,snapshot/path.name)
    r.dump(output/'research_manifest.json',dict(complete=True,source_exit_config=str(d.DEFAULT_SOURCE_CONFIG),
        source_exit_config_sha256=v10.sha(d.DEFAULT_SOURCE_CONFIG),code_sha256={str(p.resolve()):v10.sha(p) for p in sources},
        artifacts={str(p.relative_to(output)):v10.sha(p) for p in sorted(output.rglob('*')) if p.is_file() and p.name!='research_manifest.json'}))
    print(show.to_string(index=False));print(day_show.to_string(index=False))


if __name__=='__main__':run()
