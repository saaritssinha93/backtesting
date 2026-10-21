"""Frozen, chronological G3 exit research. Never imports a broker client."""
from __future__ import annotations
import argparse
from dataclasses import asdict
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
import numpy as np
import pandas as pd

from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv
from research.g3_sl_target_simulator import simulate_exit
from research import g3_sl_target_simulator as exit_engine

VERSION = 'G3-SL-TP-CHRONOLOGICAL-RESEARCH-V1'
SOURCE = Path(r'C:\TradingData\eqidv2\g3_sl_target_research\20261016\source')
OUTPUT = Path(r'C:\TradingData\eqidv2\g3_sl_target_research\20261016\study_v1')
GROUPS = [f'{clock}_{side}' for clock in ('opening','midmorning','midday') for side in ('LONG','SHORT')]


def group_for(setup):
    minutes = int(setup[:2])*60+int(setup[2:4])-1
    clock = 'opening' if minutes <= 600 else 'midmorning' if minutes <= 685 else 'midday'
    return clock+'_'+setup.split('_')[-1]


def candidates():
    rows = [dict(candidate_id='bounded_current', family='bounded_current', stop_pct=1.2,
                 target_pct=None, tighten_after_minutes=120, tightened_stop_pct=1.)]
    for stop in (.8, 1., 1.2):
        for target in (.6,.8,1.,1.2,1.5,1.8,2.):
            rows.append(dict(candidate_id=f'fixed_s{stop:.2f}_t{target:.2f}',family='fixed',stop_pct=stop,target_pct=target))
    for target in (1.2,1.5,1.8,2.):
        for activation in (.8,1.):
            rows.append(dict(candidate_id=f'protect_s1.20_t{target:.2f}_a{activation:.2f}_p0.20',
                family='protected',stop_pct=1.2,target_pct=target,
                protect_after_close_pct=activation,protected_profit_pct=.2))
    return rows


def protocol():
    return dict(version=VERSION,scope='RESEARCH_ONLY',execution_authority=False,
        source=str(SOURCE),latest_source_date='2026-10-15',development_sessions=35,later_sessions=17,
        stop_max_pct=1.2,target_min_pct=.6,target_max_pct=2.,
        target_cap_basis='User chose cap from evidence; select development-only within bounded0.60..2.00 search',
        grid=candidates(),groups=GROUPS,primary_policy='locked_pooled',comparison_policy='locked_global',
        selection_order=['profitable_days','trade_win_rate','net_pnl','lower_daily_close_drawdown','candidate_id'],
        eligibility=dict(minimum_native_fill_retention=.90,positive_development_net=True,
                         positive_development_net_with_10bps_extra_cost=True),
        group_selection='Same fixed score and eligibility within six isolated clock/side books, then joint allocation; a heuristic, not joint portfolio optimization; no later outcome used',
        bespoke_setup_minimum_development_native_fills=20,bespoke_setup_minimum_development_days=15,
        sparse_setup_policy='Map pooled group recommendation; disclose training-only exploratory pair',
        prospective_status='All source history was inspected in prior strategy research; later period is not untouched holdout',
        selection_inputs='First 35 sessions only; choices written before candidate exits evaluated on later 17',
        price_model='Completed one-minute OHLC; adverse stop-first ties; gap stop at actual open; 15:15 squareoff',
        dynamic_protection='Completed full post-entry close milestone; stop effective following minute, never widened',
        protected_profit_basis='Gross0.20% exceeds indicative50k live fees0.106% plus5bps roundtrip slippage; gaps may still produce a net loss',
        structure='Not tested:364/493 nativefills lack sealed signal5m high/low fields; missing anchors are never filled with a default',
        baseline_legacy_constraint_exceptions='Original control SL1.25 and current targets may be outside user bounds',
        no_trade_days='Included in all calendar denominators; not counted as profitable',
        costs=dict(backtest_roundtrip_bps=5,stress_extra_roundtrip_bps=10,
                   live_exposure_rs=50000,live_slippage_roundtrip_bps=5),
        parameter_tuning_after_later_period=False)


def dump(path, value):
    path.write_text(json.dumps(history.replay.json_ready(value),indent=2,allow_nan=False),encoding='utf-8')


def load_source():
    meta=json.loads((SOURCE/'source.json').read_text())
    if not (meta.get('native_parity') is True and meta.get('allocation_parity') is True):
        raise ValueError('Source reconstruction parity is not verified')
    for name,digest in meta['files'].items():
        if history.sha256(SOURCE/name)!=digest: raise ValueError(f'Source cache changed: {name}')
    if history.sha256(Path(meta['source'])/'manifest.json')!=meta['source_manifest_sha256']:
        raise ValueError('Original source manifest changed')
    archive=np.load(SOURCE/'paths.npz',allow_pickle=False)
    paths={}
    for name in archive.files:
        sid,key=name.split('__',1)
        paths.setdefault(int(sid),{})[key]=archive[name]
    archive.close()
    base=read_frozen_csv(SOURCE/'baseline.csv')
    base['day']=base.day.astype(str).str[:10]
    base['group']=base.setup_id.map(group_for)
    return base,paths,meta


def verify_exit_parity(base,paths):
    count=0
    for row in base.loc[base.filled.eq(True)].itertuples(index=False):
        replay=simulate_exit(paths[int(row.sid)],int(row.entry_path_index),float(row.entry_price),row.side=='LONG',
            1.25,float(row.native_target_pct),tighten_after_minutes=120,tightened_stop_pct=1.)
        for field in ('exit_price','gross_return_pct'):
            if not np.isclose(float(replay[field]),float(getattr(row,field)),rtol=0,atol=1e-8):
                raise ValueError(f'Exit engine baseline parity failed: sid={row.sid}, {field}')
        if pd.Timestamp(replay['exit_ts'])!=pd.Timestamp(row.exit_ts) or replay['exit_reason']!=row.exit_reason:
            raise ValueError(f'Exit engine time/reason parity failed: sid={row.sid}')
        count+=1
    return dict(verified=True,native_fills_checked=count,initial_stop_pct=1.25,tightened_stop_pct=1.,tighten_after_minutes=120)


def parameters(row,candidate):
    if candidate['family']=='bounded_current':
        return 1.2,float(np.clip(row.native_target_pct,.6,2.)),False
    return candidate['stop_pct'],candidate['target_pct'],False


def raw_candidate(base, paths, candidate):
    rows=[]
    for source in base.itertuples(index=False):
        row=source._asdict()
        row.update(research_candidate=candidate['candidate_id'],research_skip=False,scope='RESEARCH_ONLY',execution_authority=False)
        if not source.filled:
            rows.append(row); continue
        stop,target,skip=parameters(source,candidate)
        row.update(research_initial_stop_pct=stop,research_target_pct=target)
        if skip:
            row.update(filled=False,research_skip=True,exit_reason='REJECT_STRUCTURE_RISK_GT_1P2',
                entry_ts=pd.NaT,exit_ts=pd.NaT,entry_price=np.nan,exit_price=np.nan,
                gross_return_pct=np.nan,net_return_pct=np.nan,cost_pct=np.nan)
            rows.append(row);continue
        result=simulate_exit(paths[int(source.sid)],int(source.entry_path_index),float(source.entry_price),source.side=='LONG',
            stop,target,candidate.get('protect_after_close_pct'),candidate.get('protected_profit_pct',0.),
            tighten_after_minutes=candidate.get('tighten_after_minutes'),tightened_stop_pct=candidate.get('tightened_stop_pct'))
        row.update(result)
        row.update(native_stop_pct=stop,initial_stop_pct=stop,stop_pct=stop,
            native_target_pct=target,target_pct=target,cost_pct=.05,net_return_pct=result['gross_return_pct']-.05,
            first_target_hit=result['target_hit'],runner_target_hit=result['target_hit'])
        rows.append(row)
    return pd.DataFrame(rows)


def allocate(raw):
    settings=history.simulator.g.v9.V9Config(portfolio_capital_rupees=history.contract.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=history.contract.CAPITAL_PER_ENTRY_RS,leverage_factor=history.contract.LEVERAGE,
        max_positions=history.contract.MAX_POSITIONS,cost_bps=5.)
    work=history.simulator.g.v9.v5.apply_fixed_capital_model(raw.copy(),settings.capital_per_entry_rupees,settings.leverage_factor)
    ledger,_=history.simulator.g.v9.v6.apply_portfolio_constraints(work,settings.portfolio_config())
    ledger['day']=ledger.day.astype(str).str[:10]
    return ledger


def stats(ledger,days,extra_bps=0):
    frame=ledger.loc[ledger.day.isin(days)&ledger.portfolio_executed.eq(True)].copy()
    pnl=frame.portfolio_net_profit_rupees-frame.portfolio_trade_exposure_rupees*extra_bps/10000
    frame['_pnl']=pnl
    daily=frame.groupby('day')._pnl.sum().reindex(days,fill_value=0.)
    equity=np.r_[0.,daily.cumsum().to_numpy()]
    wins=int(pnl.gt(1e-8).sum());losses=int(pnl.lt(-1e-8).sum())
    return dict(sessions=len(days),trades=len(frame),wins=wins,losses=losses,
        win_rate_pct=100*wins/len(frame) if len(frame) else 0.,
        profitable_days=int(daily.gt(1e-8).sum()),losing_days=int(daily.lt(-1e-8).sum()),
        zero_days=int(daily.abs().le(1e-8).sum()),profitable_day_rate_pct=100*daily.gt(1e-8).sum()/len(days),
        net_pnl_rs=float(pnl.sum()),negative_trade_sum_abs_rs=float(-pnl.loc[pnl<0].sum()),
        daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
        worst_day_net_rs=float(daily.min()),stop_exits=int(frame.exit_reason.isin(['STOP','TIGHTENED_STOP','PROTECTED_STOP']).sum()),
        initial_stop_exits=int(frame.exit_reason.eq('STOP').sum()),
        protected_stop_exits=int(frame.exit_reason.eq('PROTECTED_STOP').sum()),
        median_trade_rs=float(pnl.median()) if len(frame) else 0.,
        native_fills=int(ledger.loc[ledger.day.isin(days),'filled'].sum()))


def choose(metrics, reference_native):
    eligible=[r for r in metrics if r['native_fills'] >= .9*reference_native and r['net_pnl_rs']>0 and r['stress_net_pnl_rs']>0]
    if not eligible:return 'bounded_current', 'No candidate passed predeclared development gates'
    order=sorted(eligible,key=lambda r:(-r['profitable_days'],-r['win_rate_pct'],-r['net_pnl_rs'],r['daily_close_drawdown_rs'],r['candidate_id']))
    return order[0]['candidate_id'],'Development profitable days, trade win rate, net profit; 90% fill retention and positive stressed net'


def fees(buy,sell):
    turnover=buy+sell
    brokerage=min(.0003*buy,20)+min(.0003*sell,20)
    exchange=.0000307*turnover;sebi=.000001*turnover;ipft=.000000001*turnover
    return brokerage+.00025*sell+exchange+sebi+ipft+.00003*buy+.18*(brokerage+exchange+sebi+ipft)


def live_projection(ledger,days):
    records=[]
    for row in ledger.loc[ledger.portfolio_executed.eq(True)].itertuples(index=False):
        q=int(50000/float(row.trigger))
        if q<=0:continue
        entry=q*float(row.entry_price);exit_value=q*float(row.exit_price)
        buy,sell=(entry,exit_value) if row.side=='LONG' else (exit_value,entry)
        gross=sell-buy;cost=fees(buy,sell);slippage=entry*.0005
        records.append(dict(day=row.day,sid=row.sid,setup_id=row.setup_id,tradingsymbol=row.tradingsymbol,
            quantity=q,entry_notional_rs=entry,fees_rs=cost,gross_pnl_rs=gross,
            net_after_fees_rs=gross-cost,net_after_fees_and_5bps_slippage_rs=gross-cost-slippage))
    frame=pd.DataFrame(records,columns=['day','sid','setup_id','tradingsymbol','quantity','entry_notional_rs','fees_rs',
        'gross_pnl_rs','net_after_fees_rs','net_after_fees_and_5bps_slippage_rs'])
    summaries=[]
    for cost in ['net_after_fees_rs','net_after_fees_and_5bps_slippage_rs']:
        pnl=frame[cost];daily=frame.groupby('day')[cost].sum().reindex(days,fill_value=0.)
        eq=np.r_[0.,daily.cumsum()]
        summaries.append(dict(cost_case=cost,trades=len(frame),wins=int(pnl.gt(0).sum()),losses=int(pnl.lt(0).sum()),
            win_rate_pct=100*pnl.gt(0).mean() if len(frame) else 0.,profitable_days=int(daily.gt(0).sum()),losing_days=int(daily.lt(0).sum()),
            net_pnl_rs=float(pnl.sum()),daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(eq)-eq))))
    return frame,summaries


def pullback_diagnostics(base,paths):
    rows=[]
    for row in base.loc[base.filled.eq(True)].itertuples(index=False):
        p=paths[int(row.sid)];start=int(row.entry_path_index);entry=float(row.entry_price);sign=1 if row.side=='LONG' else -1
        full=np.arange(start+1,len(p['close']))
        favorable=sign*(p['high' if sign==1 else 'low']/entry-1)*100
        adverse=sign*(p['low' if sign==1 else 'high']/entry-1)*100
        for target in (.6,.8,1.,1.2,1.5,1.8,2.):
            hits=full[favorable[full]>=target-1e-8]
            hit=int(hits[0]) if len(hits) else None
            # Excluding the target bar avoids using a later same-bar low/high.
            before=adverse[start+1:hit] if hit is not None else np.array([])
            with_hit=adverse[start:hit+1] if hit is not None else np.array([])
            rows.append(dict(sid=row.sid,day=row.day,setup_id=row.setup_id,group=row.group,target_pct=target,
                later_completed_bar_target_touch=hit is not None,
                adverse_before_target_pct=float(max(0.,-min(before))) if len(before) else 0. if hit is not None else None,
                conservative_adverse_through_target_bar_pct=float(max(0.,-min(with_hit))) if len(with_hit) else None))
    return pd.DataFrame(rows)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--prepare-only',action='store_true');args=parser.parse_args()
    OUTPUT.mkdir(parents=True,exist_ok=True)
    p=protocol();protocol_path=OUTPUT/'protocol.json'
    if protocol_path.exists() and json.loads(protocol_path.read_text())!=p: raise ValueError('Frozen protocol changed')
    if not protocol_path.exists():dump(protocol_path,p)
    if args.prepare_only:
        print(protocol_path);return
    if (OUTPUT/'selection_lock.json').exists(): raise ValueError('Do not rerun/tune an evaluated study; preserve frozen lock')
    code_paths=[Path(__file__).resolve(),Path(exit_engine.__file__).resolve()]
    code_hashes={str(path):history.sha256(path) for path in code_paths}
    source_cache_sha256=history.sha256(SOURCE/'source.json')
    base,paths,meta=load_source();days=meta['summary']['days'];dev=days[:35];later=days[35:]
    parity=verify_exit_parity(base,paths)
    dump(OUTPUT/'exit_engine_baseline_parity.json',parity)
    if len(days)!=52:raise ValueError('Unexpected source cohort; revisit protocol before reading new outcomes')
    dev_base=base.loc[base.day.isin(dev)].copy();registry={c['candidate_id']:c for c in candidates()}
    raw_dev={};scores=[];group_scores=[]
    for i,(name,candidate) in enumerate(registry.items(),1):
        raw=raw_candidate(dev_base,paths,candidate);raw_dev[name]=raw;ledger=allocate(raw)
        s=stats(ledger,dev);s.update(candidate_id=name,stress_net_pnl_rs=stats(ledger,dev,10)['net_pnl_rs']);scores.append(s)
        for group in GROUPS:
            part=allocate(raw.loc[raw.group.eq(group)].copy());g=stats(part,dev)
            group_scores.append(dict(group=group,candidate_id=name,stress_net_pnl_rs=stats(part,dev,10)['net_pnl_rs'],**g))
        print(f'Development candidate {i}/{len(registry)} {name}: {s["profitable_days"]}/35 positive days, net {s["net_pnl_rs"]:.2f}',flush=True)
    pd.DataFrame(scores).to_csv(OUTPUT/'development_candidates.csv',index=False)
    pd.DataFrame(group_scores).to_csv(OUTPUT/'development_group_candidates.csv',index=False)
    global_choice,reason=choose(scores,int(dev_base.filled.sum()))
    group_choices={g:choose([r for r in group_scores if r['group']==g],int(dev_base.loc[dev_base.group.eq(g),'filled'].sum()))[0] for g in GROUPS}
    # Setup-specific winners are disclosed as exploratory, never used in the pooled strategy.
    setup_choices={};setup_scores=[]
    for spec in history.contract.ACTIVE_SETUPS:
        sid=spec.setup_id;subset=dev_base.loc[dev_base.setup_id.eq(sid)]
        options=[]
        for name,raw in raw_dev.items():
            l=allocate(raw.loc[raw.setup_id.eq(sid)].copy());s=stats(l,dev)
            record=dict(setup_id=sid,candidate_id=name,stress_net_pnl_rs=stats(l,dev,10)['net_pnl_rs'],**s)
            options.append(record);setup_scores.append(record)
        choice,_=choose(options,int(subset.filled.sum()))
        setup_choices[sid]=dict(exploratory_training_winner=choice,development_native_fills=int(subset.filled.sum()),
            development_fill_days=int(subset.loc[subset.filled.eq(True),'day'].nunique()),
            qualifies_for_bespoke_recommendation=bool(subset.filled.sum()>=20 and subset.loc[subset.filled.eq(True),'day'].nunique()>=15),
            pooled_candidate=group_choices[group_for(sid)])
    lock=dict(version=VERSION,protocol_sha256=history.sha256(protocol_path),source_manifest_sha256=meta['source_manifest_sha256'],
        source_cache_sha256=source_cache_sha256,code_hashes=code_hashes,exit_engine_baseline_parity=parity,primary_policy='locked_pooled',
        development_days=dev,later_days=later,global_candidate=global_choice,global_selection_reason=reason,
        group_candidates=group_choices,setup_choices=setup_choices,
        selection_uses_later_outcomes=False,created_at_ist=pd.Timestamp.now(tz='Asia/Kolkata').isoformat())
    dump(OUTPUT/'selection_lock.json',lock)
    pd.DataFrame(setup_scores).to_csv(OUTPUT/'development_setup_exploration.csv',index=False)
    print('SELECTION LOCK WRITTEN; evaluating later 17 sessions now',flush=True)
    needed={'bounded_current',global_choice,*group_choices.values(),*(s['exploratory_training_winner'] for s in setup_choices.values())}
    raw_all={name:pd.concat([raw_dev[name],raw_candidate(base.loc[base.day.isin(later)],paths,registry[name])],ignore_index=True) for name in needed}
    ledgers={'original_g3':base,'bounded_current':allocate(raw_all['bounded_current']),
        'locked_global':allocate(raw_all[global_choice]),
        'locked_pooled':allocate(pd.concat([raw_all[name].loc[lambda f:f.group.eq(g)] for g,name in group_choices.items()],ignore_index=True))}
    # Development stability checks select using only preceding sessions within development.
    rolling=[]
    for cut in (20,25,30):
        training=dev[:cut];evaluation=dev[cut:cut+5];foldscores=[]
        for name,raw in raw_dev.items():
            l=allocate(raw.loc[raw.day.isin(training)].copy());s=stats(l,training)
            foldscores.append(dict(candidate_id=name,stress_net_pnl_rs=stats(l,training,10)['net_pnl_rs'],**s))
        chosen,_=choose(foldscores,int(dev_base.loc[dev_base.day.isin(training),'filled'].sum()))
        for name,c in [('chosen',chosen),('bounded_current','bounded_current')]:
            l=allocate(raw_dev[c].loc[raw_dev[c].day.isin(evaluation)].copy())
            rolling.append(dict(train_sessions=cut,evaluation_first=evaluation[0],evaluation_last=evaluation[-1],policy=name,candidate_id=c,**stats(l,evaluation)))
    pd.DataFrame(rolling).to_csv(OUTPUT/'development_rolling_check.csv',index=False)
    (OUTPUT/'ledgers').mkdir(exist_ok=True)
    metrics=[];daily=[];live=[];setup_rows=[];monthly=[]
    for policy,ledger in ledgers.items():
        ledger.to_csv(OUTPUT/'ledgers'/f'{policy}.csv',index=False)
        for period,cohort in [('development35',dev),('later17',later),('all',days)]:
            for extra in (0,10):metrics.append(dict(policy=policy,period=period,extra_roundtrip_bps=extra,**stats(ledger,cohort,extra)))
            projection,ls=live_projection(ledger.loc[ledger.day.isin(cohort)],cohort)
            live.extend(dict(policy=policy,period=period,**r) for r in ls)
            if period=='all':projection.to_csv(OUTPUT/f'live_projection_{policy}.csv',index=False)
        for day in days:daily.append(dict(policy=policy,day=day,**stats(ledger,[day])))
        for month in sorted({d[:7] for d in days}):monthly.append(dict(policy=policy,month=month,**stats(ledger,[d for d in days if d.startswith(month)])))
    for spec in history.contract.ACTIVE_SETUPS:
        sid=spec.setup_id;chosen=setup_choices[sid];pool=chosen['pooled_candidate'];exp=chosen['exploratory_training_winner']
        sbase=base.loc[base.setup_id.eq(sid)];pooled=ledgers['locked_pooled'].loc[ledgers['locked_pooled'].setup_id.eq(sid)]
        exploratory=allocate(raw_all[exp].loc[raw_all[exp].setup_id.eq(sid)].copy())
        rec=dict(setup_id=sid,signal_time=spec.signal_end,confirmation_time=spec.confirmation_end,side=spec.side,
            group=group_for(sid),current_initial_stop_pct=1.25,current_target_pct=spec.target_pct,
            **chosen,pooled_parameters=json.dumps(registry[pool],sort_keys=True),exploratory_parameters=json.dumps(registry[exp],sort_keys=True))
        for prefix,frame,cohort in [('baseline_all',sbase,days),('pooled_all',pooled,days),('pooled_later',pooled,later),('exploratory_later',exploratory,later)]:
            rec.update({prefix+'_'+k:v for k,v in stats(frame,cohort).items()})
        setup_rows.append(rec)
    pd.DataFrame(metrics).to_csv(OUTPUT/'policy_comparison.csv',index=False)
    pd.DataFrame(daily).to_csv(OUTPUT/'daily_comparison.csv',index=False)
    pd.DataFrame(monthly).to_csv(OUTPUT/'monthly_comparison.csv',index=False)
    pd.DataFrame(live).to_csv(OUTPUT/'live_cost_comparison.csv',index=False)
    pd.DataFrame(setup_rows).to_csv(OUTPUT/'setup_recommendations.csv',index=False)
    pb=pullback_diagnostics(base,paths);pb.to_csv(OUTPUT/'pullback_diagnostics.csv',index=False)
    summary=dict(version=VERSION,source=meta['source'],source_date=days[-1],first_date=days[0],sessions=len(days),
        active_setups=len(history.contract.ACTIVE_SETUPS),configuration_promoted=False,execution_authority=False,
        primary_policy='locked_pooled',global_choice=global_choice,group_choices=group_choices,
        bespoke_qualified_count=sum(s['qualifies_for_bespoke_recommendation'] for s in setup_choices.values()),
        all_base_cost_metrics=[r for r in metrics if r['period']=='all' and r['extra_roundtrip_bps']==0],
        later_base_cost_metrics=[r for r in metrics if r['period']=='later17' and r['extra_roundtrip_bps']==0],
        live_projection=live,source_baseline_native_and_allocation_parity=bool(meta['native_parity'] and meta['allocation_parity']),
        exit_engine_baseline_parity=parity,code_hashes=code_hashes,source_cache_sha256=source_cache_sha256,
        limitations=['Existing strategy selections and targets were fitted on this history; later17 is diagnostic, not a pristine holdout.',
        'One-minute OHLC cannot establish within-bar order or broker latency. Stop-first ambiguity is conservative.',
        'Live projection uses actual integer shares at50k trigger notional and current indicative fees, same modeled executions/allocation, not broker fills.',
        'Protective stop can gap;1.2% is intended trigger risk, not a guaranteed realized-loss ceiling.',
        'Original data exclusions, including after-close stock-day eligibility, are inherited. ATR is missing for most historical selections; no ATR values were invented.',
        'Training-only setup-specific winners are exploratory, not independently validated recommendations.',
        'Pullback diagnostics include counterfactual movement after native exits; they do not prove a held trade could survive.',
        'Pooled choices optimize six isolated group books then reallocate jointly; this is a predeclared heuristic, not a global portfolio optimum.'])
    if any(history.sha256(Path(path))!=digest for path,digest in code_hashes.items()):
        raise ValueError('Research code changed while study was running')
    if history.sha256(SOURCE/'source.json')!=source_cache_sha256:
        raise ValueError('Source metadata changed while study was running')
    dump(OUTPUT/'summary.json',summary)
    dump(OUTPUT/'manifest.json',dict(version=VERSION,execution_authority=False,
        files={str(p.relative_to(OUTPUT)):history.sha256(p) for p in OUTPUT.rglob('*') if p.is_file() and p.name!='manifest.json'},
        code=code_hashes,source_cache_sha256=source_cache_sha256))
    print(pd.DataFrame(summary['all_base_cost_metrics']).to_string(index=False),flush=True)
    print(OUTPUT,flush=True)


if __name__=='__main__':main()
