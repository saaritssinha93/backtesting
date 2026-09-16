"""Development-only refinement with pooled win improvement and profit retention.

Adaptive second research round: first-round later outcomes have already been
seen. Selection here reads development metrics only; this is not a holdout test.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from pathlib import Path

import pandas as pd

import fno_v13_v10_backtest as e
import fno_v13_v10_research as r


def score(frame,base_win,base_net):
    out=frame.copy()
    out['pooled_win_pct']=100*(out.TRAIN_5bps_wins+out.VALIDATION_5bps_wins)/(out.TRAIN_5bps_trades+out.VALIDATION_5bps_trades)
    out['balanced_pass']=(out.pooled_win_pct.gt(base_win+r.EPS)&out.minimum_split_pf.ge(2)
        &out.development_net.ge(.5*base_net)&out.VALIDATION_5bps_win_rate_pct.ge(73.68421052631578-r.EPS)
        &out.TRAIN_9bps_profit_factor.ge(1.5)&out.VALIDATION_9bps_profit_factor.ge(1.5)
        &out.TRAIN_5bps_trades.ge(15)&out.VALIDATION_5bps_trades.ge(10))
    return out


def ranked(frame):
    return frame.sort_values(['pooled_win_pct','minimum_split_pf','development_net','candidate_id'],
                            ascending=[False,False,False,True],kind='stable')


def run(parent=e.DEFAULT_OUTPUT):
    output=parent/'balanced'
    output.mkdir(parents=True,exist_ok=True)
    old=pd.read_csv(parent/'development_grid_results.csv')
    # Exact original pooled development baseline, independently computed below.
    base_win,base_net=76.,156007.42494335875
    scored=score(old,base_win,base_net)
    anchors=ranked(scored.loc[scored.balanced_pass]).head(3)
    if anchors.empty:
        raise RuntimeError('No balanced development anchors; original results remain available.')
    candidates=[]
    seen={(round(x.initial_stop_pct,6),round(x.first_target_pct,6),round(x.runner_target_pct,6),x.partial_pct,x.runner_stop) for x in old.itertuples()}
    for anchor in anchors.to_dict('records'):
        for delta_s in range(-5,6):
            for delta_t in range(-5,6):
                stop=round(anchor['initial_stop_pct']+delta_s/100,6)
                target=round(anchor['runner_target_pct']+delta_t/100,6)
                if not .25<=stop<=1.5 or not .25<=target<=2:
                    continue
                t1=1.075 if anchor['policy']=='LEGACY_T1' else round(target*anchor['first_target_pct']/anchor['runner_target_pct'],6)
                key=(stop,t1,target,anchor['partial_pct'],anchor['runner_stop'])
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(dict(candidate_id=len(old)+len(candidates),initial_stop_pct=stop,first_target_pct=t1,
                    runner_target_pct=target,partial_pct=anchor['partial_pct'],runner_stop=anchor['runner_stop'],policy=anchor['policy']))
    protocol=dict(evidence='ADAPTIVE_PREVIOUSLY_SEEN_HISTORY_RESEARCH',
        reason='First-round maximum-win result had insufficient overall PF/profit; this round changes the development objective explicitly.',
        selection='Pooled development win rate >76%; PF>=2 each development split; >=50% original pooled net; validation win >=73.6842%; PF>=1.5 at9bps each split; >=15/10 trades',
        refinement='Top3 balanced development anchors; SL/runner +/-0.05 in0.01 steps, same first-target ratio/partial fraction/stop rule; caps unchanged.',
        tie_break='Prefer >=2 passing 0.01 adjacent cells; then pooled win%, weaker split PF, development net, candidate id',
        later_results_used_to_rank=False,first_round_later_results_already_seen=True,
        prior_grid_sha256=e.sha(parent/'development_grid_results.csv'),new_iterations=len(candidates))
    r.dump(output/'research_protocol.json',protocol)
    pd.DataFrame(candidates).to_csv(output/'registered_grid.csv',index=False)
    anchors.to_csv(output/'refinement_anchors.csv',index=False)
    print(f'[V10 BALANCED] Registered {len(candidates)} additional settings around development anchors.',flush=True)
    dataset=e.load_source()
    orders,paths,base=dataset['orders'],dataset['paths'],dataset['v9_config']
    groups=r.periods(dataset['days'])
    devdays=groups['TRAIN']+groups['VALIDATION']
    devorders=orders.loc[pd.to_datetime(orders.day).dt.date.isin(devdays)]
    baseline_trades=e.v9.replay_candidates(orders,paths,base)
    baseline,summary=e.v9.v6.apply_portfolio_constraints(baseline_trades,base.portfolio_config())
    bm=r.metric(baseline,devdays)
    assert abs(bm['win_rate_pct']-base_win)<1e-8 and abs(bm['net_profit_rupees']-base_net)<1e-7
    controls={(p,c):r.metric(baseline,groups[p],cost_bps=c) for p in ['TRAIN','VALIDATION'] for c in [5,9]}
    more=[]
    for i,row in enumerate(candidates,1):
        _,ledger,_=e.evaluate_orders(devorders,paths,r.config(row),base,validate_paths=False)
        stats={(p,c):r.metric(ledger,groups[p],cost_bps=c) for p in ['TRAIN','VALIDATION'] for c in [5,9]}
        more.append(r.assessed(row,stats,controls))
        if i%50==0:
            print(f'[V10 BALANCED] {i}/{len(candidates)}',flush=True)
    all_rows=score(pd.concat([old,pd.DataFrame(more)],ignore_index=True),base_win,base_net)
    passing=all_rows.loc[all_rows.balanced_pass]
    keys={(x.policy,x.partial_pct,x.runner_stop,round(x.initial_stop_pct*100),round(x.runner_target_pct*100)) for x in passing.itertuples()}
    all_rows['adjacent_passing_cells']=[sum((x.policy,x.partial_pct,x.runner_stop,round(x.initial_stop_pct*100)+ds,round(x.runner_target_pct*100)+dt) in keys for ds,dt in [(-1,0),(1,0),(0,-1),(0,1)]) for x in all_rows.itertuples()]
    stable=all_rows.loc[all_rows.balanced_pass & all_rows.adjacent_passing_cells.ge(2)]
    selected=ranked(stable if len(stable) else all_rows.loc[all_rows.balanced_pass]).iloc[0].to_dict()
    cfg=r.config(selected)
    status='BALANCED_DEVELOPMENT_PASSED_WITH_LOCAL_SUPPORT' if len(stable) else 'BALANCED_DEVELOPMENT_PASSED_ISOLATED'
    all_rows.to_csv(output/'development_grid_results.csv',index=False)
    r.dump(output/'frozen_config.json',asdict(cfg))
    r.dump(output/'selection_freeze.json',dict(config=asdict(cfg),status=status,candidate_id=int(selected['candidate_id']),
        later_results_used=False,adaptive_round=True,grid_sha256=e.sha(output/'development_grid_results.csv')))
    print(f'[V10 BALANCED FREEZE] {asdict(cfg)}; {status}',flush=True)
    outputs={'V9_CONTROL':(baseline_trades,baseline,summary),
        'V9_TARGET_CAPPED_2':e.evaluate_orders(orders,paths,e.V10Config(),base),
        'V13_V10':e.evaluate_orders(orders,paths,cfg,base)}
    metrics,stress,daily=[],[],[]
    for name,(trades,ledger,stats) in outputs.items():
        r.save(output/'final'/name,trades,ledger,stats)
        for period,days in groups.items():
            metrics.append(dict(name=name,period=period,**r.metric(ledger,days)))
            stress.append(dict(name=name,period=period,**r.metric(ledger,days,cost_bps=9)))
        byday=ledger.groupby('day').portfolio_net_profit_rupees.sum()
        daily.extend(dict(name=name,day=day,net_profit_rupees=float(byday.get(day,0))) for day in dataset['days'])
    metrics=pd.DataFrame(metrics)
    metrics.to_csv(output/'final_metrics.csv',index=False)
    pd.DataFrame(stress).to_csv(output/'cost_stress_metrics.csv',index=False)
    pd.DataFrame(daily).to_csv(output/'final_daily.csv',index=False)
    r.report(output,cfg,status,metrics,pd.DataFrame(stress),selected,len(all_rows))
    report=output/'V13_V10_DETAILED_RESULTS.md'
    report.write_text(report.read_text(encoding='utf-8')+'\n\nThis is an adaptive second research round. The initial maximum-win search and its later results were already seen. The revised pooled-win/profit-retention objective and refinement used development metrics only; this is not an untouched holdout evaluation.\n',encoding='utf-8')
    original=pd.read_csv(e.DEFAULT_SOURCE/'final/V13_V9_FROZEN/portfolio_trades.csv',float_precision='round_trip')
    r.dump(output/'baseline_parity.json',e.v9.assert_control_parity(original,baseline))
    files=[Path(e.__file__).resolve(),Path(r.__file__).resolve(),Path(__file__).resolve()]
    (output/'source_snapshot').mkdir(exist_ok=True)
    for path in files+list(Path(__file__).parent.glob('tests/test_fno_v13_v10*.py')):
        shutil.copy2(path,output/'source_snapshot'/path.name)
    r.dump(output/'research_manifest.json',dict(status=status,iterations=len(all_rows),code_sha256={str(path):e.sha(path) for path in files},
        artifacts={str(path.relative_to(output)):e.sha(path) for path in sorted(output.rglob('*')) if path.is_file() and path.name!='research_manifest.json'}))
    print(metrics.loc[metrics.period.isin(['FULL','SEPTEMBER'])].to_string(index=False),flush=True)


if __name__=='__main__':
    run()
