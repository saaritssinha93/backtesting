"""Isolated full-universe momentum research; never imports a broker client."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import pickle
import sys
import numpy as np
import pandas as pd

from . import features, policy, execution
from g3_v18_20261102 import data as source
from g3_v24_selection_20261109.rsi_timing_20261110 import inputs
from g3_v24_selection_20261109.reporting import metric, PNL

PACKAGE=Path(__file__).resolve().parent
ROOT=PACKAGE.parent


def dump(path,value):
    path.write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')


def code_hashes():
    paths={Path(__file__).resolve(),PACKAGE/'RESEARCH_PLAN.md'}
    for module in tuple(sys.modules.values()):
        name=getattr(module,'__file__',None)
        if name and str(name).endswith('.py'):
            path=Path(name).resolve()
            if path.is_relative_to(ROOT) and path.is_file():paths.add(path)
    return {str(p):inputs.sha(p) for p in sorted(paths)}


def consumed(proof):
    files={}
    pairs=[('feature_path','features_sha256'),('minute_path','minutes_sha256'),
        ('provenance_path','provenance_sha256')]
    if proof['loader_source']!='HASH_VERIFIED_PRESELECTION_RESEARCH_CACHE':
        pairs += [('run_manifest','run_manifest_sha256'),('source_manifest','source_manifest_sha256'),
            ('snapshot_manifest','snapshot_manifest_sha256')]
        for item in proof.get('minute_sources',[]):files[str(Path(item['path']).resolve())]=item['sha256']
    for pk,hk in pairs:
        if proof.get(pk) and proof.get(hk):files[str(Path(proof[pk]).resolve())]=proof[hk]
    universe=proof.get('breadth_universe',{})
    if universe.get('path') and universe.get('sha256'):
        files[str(Path(universe['path']).resolve())]=universe['sha256']
    if not files:raise ValueError('No consumed source fingerprints')
    inputs.verify(files)
    return files


def baseline_stages(frozen,day):
    item=frozen['metadata'][day]
    path=Path(item['path'])
    inputs.verify({path:item['sha256']})
    meta=json.loads(path.read_text())
    payload=path.with_suffix('.pkl')
    inputs.verify({payload:meta['cache_sha256']})
    record=pickle.loads(payload.read_bytes())
    audit=record['audit'].copy()
    keys=['day','tradingsymbol','side']
    audit['pre_g7']=audit.sid.isin(record['baseline_selected_sids'])
    audit['admitted']=audit.sid.isin(frozen['orders'].sid)
    audit['filled']=audit.sid.isin(frozen['expected'].sid)
    stages=audit.groupby(keys).agg(native_candidates=('sid','size'),
        g3_filter_pass=('v9_filter_pass','sum'),pre_g7_orders=('pre_g7','sum'),
        admitted_orders=('admitted','sum'),filled_trades=('filled','sum')).reset_index()
    return stages,{str(path):item['sha256'],str(payload):meta['cache_sha256']}


def arm_name(family,target):return family+'__tp'+format(round(target*100),'03d')


def build_day(index,day,frozen):
    raw,minutes,proof=source.load_day(day)
    files=consumed(proof)
    states=features.build_states(minutes,day)
    signal=policy.signals(states)
    base=frozen['expected'].loc[frozen['expected'].day.eq(day)].copy()
    base_orders=frozen['orders'].loc[frozen['orders'].day.eq(day)].copy()
    baseline_replay=[]
    for target,part in base.groupby('native_target_pct'):
        baseline_replay.append(execution.counterfactual_exit_replay(part,minutes,target_pct=float(target)))
    rebuilt=pd.concat(baseline_replay,ignore_index=True) if baseline_replay else base.copy()
    parity=inputs.baseline_parity(rebuilt,base)
    books={'baseline_v24':base}
    orders={'baseline_v24':base_orders}
    decisions={'baseline_v24':base_orders[['sid','day','tradingsymbol','side']].assign(filled=lambda x:x.sid.isin(base.sid))}
    for target in policy.TARGETS:
        name='matched_exit_'+format(round(target*100),'03d')
        books[name]=execution.counterfactual_exit_replay(base,minutes,target_pct=target)
        orders[name]=base_orders.copy()
        decisions[name]=decisions['baseline_v24'].copy()
    stressed=[]
    for target,part in base.groupby('native_target_pct'):
        stressed.append(execution.counterfactual_exit_replay(part,minutes,target_pct=float(target),slippage_bps=2))
    books['baseline_v24__stress']=pd.concat(stressed,ignore_index=True) if stressed else base.copy()
    orders['baseline_v24__stress']=base_orders.copy()
    decisions['baseline_v24__stress']=decisions['baseline_v24'].copy()
    case_audits=[]
    for fi,family in enumerate(policy.FAMILIES):
        selected,audit=policy.select(signal,family)
        selected['sid']=1_000_000_000+index*1_000_000+fi*10_000+np.arange(len(selected),dtype=np.int64)
        case_audits.append(audit)
        for target in policy.TARGETS:
            name=arm_name(family,target)
            selected_target=selected.copy()
            selected_target['native_target_pct']=target
            orders[name]=selected_target
            books[name],decisions[name]=execution.simulate(selected_target,minutes,target_pct=target)
            orders[name+'__stress']=selected_target.copy()
            books[name+'__stress'],decisions[name+'__stress']=execution.simulate(selected_target,minutes,target_pct=target,slippage_bps=2)
            if books[name].sid.tolist()!=books[name+'__stress'].sid.tolist():
                raise ValueError('Slippage stress changed entry acceptance')
    # Hindsight labels are built only after all causal selections and fills.
    movers=features.build_movers(minutes,day)
    stages,stage_files=baseline_stages(frozen,day)
    files.update(stage_files)
    inputs.verify(files)
    return dict(day=day,books=books,orders=orders,decisions=decisions,movers=movers,
        case_audits=pd.concat(case_audits,ignore_index=True),baseline_stages=stages,
        baseline_parity=parity,input_hashes=files,
        source_proof=dict(day=day,loader_source=proof['loader_source'],minute_stocks=proof['minute_stocks'],
            feature_stocks=proof['feature_stocks'],breadth_universe=proof.get('breadth_universe'),
            minute_volume_available='volume' in minutes,minute_rows=len(minutes),state_rows=len(states),
            source_input_hashes=files,ancestry_not_reread='Archived cache ancestry is provenance only; consumed payloads verified'))


def choose(books,orders,validation):
    baseline=metric(books['baseline_v24'],int(orders['baseline_v24'].day.isin(validation).sum()),validation)
    rows=[]
    for family in policy.FAMILIES:
        for target in policy.TARGETS:
            arm=arm_name(family,target)
            b=books[arm].loc[books[arm].day.isin(validation)]
            measure=metric(books[arm],int(orders[arm].day.isin(validation).sum()),validation)
            pf=measure['profit_factor'] if measure['profit_factor'] is not None else float('inf')
            base_pf=baseline['profit_factor'] if baseline['profit_factor'] is not None else float('inf')
            checks=dict(minimum_30_fills=measure['trades']>=30,minimum_8_active_dates=b.day.nunique()>=8,
                positive_net=measure['net_pnl_rs']>0,mean_at_least_baseline=measure['average_trade_rs']>=baseline['average_trade_rs'],
                profit_factor_at_least_baseline=pf>=base_pf,
                drawdown_no_larger=measure['daily_close_drawdown_rs']<=baseline['daily_close_drawdown_rs'])
            rows.append(dict(arm=arm,**measure,active_dates=int(b.day.nunique()),checks=checks,qualified=all(checks.values())))
    ranked=sorted(rows,key=lambda r:(-r['net_pnl_rs'],r['arm']))
    passing=[r for r in ranked if r['qualified']]
    chosen=passing[0]['arm'] if passing else None
    return dict(validation_chosen=chosen,exploratory_best=ranked[0]['arm'],validation_days=validation,
        baseline_validation=baseline,variants=ranked,
        reason='Best validation net among all predeclared gates' if chosen else 'No variant passed all predeclared validation gates',
        selection_uses_later_nine=False,automatic_promotion=False)


def missed_cases(movers,case_audits,stages,orders,books,selection):
    arm=selection['validation_chosen'] or selection['exploratory_best']
    family=arm.split('__tp')[0]
    keys=['day','tradingsymbol','side']
    cases=movers.loc[movers.top5].merge(stages,on=keys,how='left',validate='one_to_one')
    for key in ['native_candidates','g3_filter_pass','pre_g7_orders','admitted_orders','filled_trades']:
        cases[key]=cases[key].fillna(0).astype(int)
    cases['baseline_miss_reason']=np.select([
        cases.native_candidates.eq(0),cases.g3_filter_pass.eq(0),cases.pre_g7_orders.eq(0),
        cases.admitted_orders.eq(0),cases.filled_trades.eq(0)],
        ['NO_NATIVE_SIGNAL_IN_CONFIGURED_PAIRS','G3_FILTERS','QUOTA_OR_NEW_PAIR_VOLUME',
         'G7_CONFIRMATION','UNFILLED_TRIGGER'],default='FILLED')
    cases=cases.merge(case_audits.loc[case_audits.family.eq(family)],on=keys,how='left',validate='one_to_one')
    fill=books[arm].groupby(keys).agg(new_fills=('sid','size'),first_new_entry=('entry_ts','min'),new_net=(PNL,'sum')).reset_index()
    cases=cases.merge(fill,on=keys,how='left',validate='one_to_one')
    cases['new_fills']=cases.new_fills.fillna(0).astype(int)
    cases['new_miss_reason']=np.select([cases.signal_clocks.eq(0),cases.selected_clocks.eq(0),cases.new_fills.eq(0)],
        ['SIGNAL_NOT_MET','CLOCK_RANK_OR_DAILY_CAP','NEXT_OPEN_GAP_CANCELLED'],default='FILLED_SEE_CAPTURE_DETAILS')
    cases['evaluated_arm']=arm
    return cases


def run(output=None,limit=None):
    from . import reporting
    output=Path(output).resolve() if output else PACKAGE/'results_through_20261109'
    if output.exists() or output==PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a new output directory inside V25')
    frozen=inputs.load_baseline()
    if source.available_days('2026-11-09')!=frozen['days']:raise ValueError('Full-universe dates differ from baseline')
    days=frozen['days'][:limit] if limit else frozen['days']
    code=code_hashes()
    signature=dict(code=code,baseline=frozen['parent_hashes'],days=days)
    stamp=hashlib.sha256(json.dumps(signature,sort_keys=True).encode()).hexdigest()[:16]
    cache=PACKAGE/('checkpoints_'+stamp);cache.mkdir(exist_ok=True)
    records=[]
    for index,day in enumerate(days):
        path=cache/f'{index:02d}_{day}.pkl';meta_path=path.with_suffix('.json')
        if path.exists() and meta_path.exists():
            meta=json.loads(meta_path.read_text())
            if meta['signature']!=signature:raise ValueError('Checkpoint signature changed')
            inputs.verify({path:meta['sha256']})
            record=pickle.loads(path.read_bytes())
            inputs.verify(record['input_hashes'])
        else:
            record=build_day(index,day,frozen)
            inputs.verify(code)
            path.write_bytes(pickle.dumps(record,protocol=5))
            dump(meta_path,dict(signature=signature,sha256=inputs.sha(path)))
        records.append(record)
        # Progress contains coverage only, never later-period performance.
        print(f"Completed {index+1}/{len(days)} {day}: {record['source_proof']['minute_stocks']} stocks; baseline parity PASS",flush=True)
    books={arm:pd.concat([r['books'][arm] for r in records],ignore_index=True) for arm in records[0]['books']}
    orders={arm:pd.concat([r['orders'][arm] for r in records],ignore_index=True) for arm in records[0]['orders']}
    decisions={arm:pd.concat([r['decisions'][arm] for r in records],ignore_index=True) for arm in records[0]['decisions']}
    movers=pd.concat([r['movers'] for r in records],ignore_index=True)
    cases=pd.concat([r['case_audits'] for r in records],ignore_index=True)
    stages=pd.concat([r['baseline_stages'] for r in records],ignore_index=True)
    validation=days[44:59] if len(days)>44 else days
    selection=choose(books,orders,validation)
    chosen=selection['validation_chosen'] or selection['exploratory_best']
    last=days[59:]
    # Choice is already frozen; these values cannot feed choose().
    selection['frozen_later_check']=dict(days=last,
        baseline=metric(books['baseline_v24'],int(orders['baseline_v24'].day.isin(last).sum()),last) if last else None,
        chosen=metric(books[chosen],int(orders[chosen].day.isin(last).sum()),last) if last else None)
    inputs.baseline_parity(books['baseline_v24'],frozen['expected'].loc[frozen['expected'].day.isin(days)])
    inputs.verify(code);inputs.verify(frozen['parent_hashes'])
    for r in records:inputs.verify(r['input_hashes'])
    config=dict(version='G3-V25-MOMENTUM-20261110',scope='BACKTEST_ONLY',execution_authority=False,
        live_changed=False,paper_changed=False,previous_results_changed=False,days=days,
        families=list(policy.FAMILIES),targets=list(policy.TARGETS),max_orders_per_side_day=policy.MAX_PER_SIDE_DAY,
        execution=execution.configuration(),feature_configuration=features.configuration(),
        arm_targets={arm_name(f,t):t for f in policy.FAMILIES for t in policy.TARGETS},
        primary_target_pct=.8,sensitivity_target_pct=1.5,source_fingerprints=signature,
        periods={'fit_44':days[:44],'validation_15':days[44:59],'later_9':days[59:]},
        volume_used=False,excluded_current_session='2026-11-10 incomplete',
        limitations=['Retrospective archived universes and full-stock-day completeness eligibility',
            '09:15 minute absent; session anchor is observed09:16open',
            'Oct1 incomplete and Nov8 special schedule excluded from inherited68-session cohort',
            'These dates and baseline were examined before; later9 is not untouched out-of-sample',
            'New next-open entries and common targets differ from G3 native trigger/target policy',
            'One-minute OHLC cannot establish tick-level order sequencing; adverse ambiguity modeled'],
        automatic_promotion=False)
    output.mkdir(parents=True)
    dump(output/'config.json',config);dump(output/'selection.json',selection)
    summary=reporting.publish(output,books,orders,decisions,movers,days,config,selection)
    dump(output/'summary.json',summary)
    cases.to_csv(output/'signal_case_audit.csv',index=False)
    stages.to_csv(output/'baseline_case_stages.csv',index=False)
    movers.to_csv(output/'all_daily_movers.csv',index=False)
    missed_cases(movers,cases,stages,orders,books,selection).to_csv(output/'missed_cases.csv',index=False)
    dump(output/'source_provenance.json',dict(sessions=[r['source_proof'] for r in records],
        checkpoints={str(cache/f'{i:02d}_{d}.pkl'):inputs.sha(cache/f'{i:02d}_{d}.pkl') for i,d in enumerate(days)}))
    dump(output/'verification.json',dict(status='PASS',baseline_parity=[r['baseline_parity'] for r in records],
        full_universe_stock_days=int(movers[['day','tradingsymbol']].drop_duplicates().shape[0]),
        features_use_future=False,labels_joined_after_fills=True,baseline_artifacts_unchanged=True))
    dump(output/'manifest.json',{p.name:inputs.sha(p) for p in output.iterdir() if p.is_file()})
    print('FROZEN_SELECTION '+json.dumps({k:selection[k] for k in ['validation_chosen','exploratory_best','reason']}),flush=True)
    print('RESULTS '+str(output),flush=True)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path);parser.add_argument('--limit',type=int)
    args=parser.parse_args();run(args.output,args.limit)
