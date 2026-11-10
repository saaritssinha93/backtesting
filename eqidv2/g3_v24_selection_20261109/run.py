"""Isolated full-pool G3 V24 ranking comparison. Never imports a broker client."""
import argparse
import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from . import policy, features, engine
from g3_v21_expanded_20261104 import source
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import policy as g7
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.stock_features import build_stock_audit
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.nifty_features import build_nifty_audit
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest

PACKAGE=Path(__file__).resolve().parent
ROOT=PACKAGE.parent
BASELINE=ROOT/'g3_v23_adx_rsi_20261106/breakeven_baseline_20261106/results_through_20261105'
G7_CACHE=ROOT/'g3_v23_adx_rsi_20261106/through_20261105/results_through_20261105'


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def dump(path,value):
    path.write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')


def verify_aux(proof):
    """Recheck sealed feature and G7 evidence before cached research reuse."""
    from . import data
    data._verify(data._identities(proof))
    if isinstance(proof,dict):
        for a,b in [('stock_path','stock_sha256'),('g3_source_proof','g3_source_proof_sha256')]:
            if a in proof and b in proof and data.sha(proof[a])!=proof[b]:
                raise ValueError('Changed auxiliary source: '+proof[a])
        for child in proof.values():verify_aux(child)
    elif isinstance(proof,list):
        for child in proof:verify_aux(child)


def g7_allowed(orders, saved):
    if orders.empty:return {},[]
    known=orders.loc[orders.sid.isin(saved.index)]
    for key in ['day','setup_id','tradingsymbol','side']:
        if not known[key].astype(str).to_numpy().tolist()==saved.loc[known.sid,key].astype(str).tolist():
            raise ValueError('Saved G7 identity mismatch: '+key)
    field='all_g7_allowed' if 'all_g7_allowed' in saved else 'all_g7_entry_allowed'
    allowed={int(sid):bool(saved.loc[sid,field]) for sid in known.sid}
    unknown=orders.loc[~orders.sid.isin(saved.index)]
    proofs=[]
    if len(unknown):
        stock,sp=build_stock_audit(unknown)
        nifty,npf=build_nifty_audit(unknown)
        audit=g7.decisions(unknown,stock,nifty)
        allowed.update(dict(zip(audit.sid.astype(int),audit.all_g7_entry_allowed.astype(bool))))
        proofs=[dict(kind='stock',sources=sp),dict(kind='nifty',sources=npf)]
    return allowed,proofs


def baseline_parity(book,orders,days):
    cohort=[day for day in days if day<='2026-11-05']
    expected=pd.read_csv(BASELINE/'selected_trades.csv')
    expected=expected.loc[expected.day.isin(cohort)].set_index('sid').sort_index()
    actual=book.loc[book.day.isin(cohort)].set_index('sid').sort_index()
    if not actual.index.equals(expected.index):
        raise ValueError('V23 baseline filled identities changed')
    for key in ['day','setup_id','tradingsymbol','side','exit_reason']:
        if not actual[key].astype(str).equals(expected[key].astype(str)):
            raise ValueError('V23 baseline identity/exit mismatch: '+key)
    for key in ['entry_ts','exit_ts']:
        if not pd.to_datetime(actual[key],utc=True).equals(pd.to_datetime(expected[key],utc=True)):
            raise ValueError('V23 baseline clock mismatch: '+key)
    for key in ['entry_price','exit_price','portfolio_net_profit_rupees']:
        if not np.allclose(actual[key],expected[key],rtol=0,atol=1e-6):
            raise ValueError('V23 baseline economics mismatch: '+key)
    expected_orders=pd.read_csv(G7_CACHE/'admission_audit.csv')
    expected_orders=expected_orders.loc[expected_orders.day.isin(cohort)&expected_orders.all_g7_allowed.eq(True)]
    if set(expected_orders.sid)!=set(orders.loc[orders.day.isin(cohort),'sid']):
        raise ValueError('V23 baseline admitted-order identities changed')
    return dict(status='PASS',sessions=len(cohort),filled_trades=len(actual),admitted_orders=len(expected_orders),
        net_pnl_rs=float(actual.portfolio_net_profit_rupees.sum()))


def run(through='2026-11-09',output=None,limit=None):
    from . import data,reporting
    output=Path(output) if output else PACKAGE/'results_through_20261109'
    output=output.resolve()
    if not output.is_relative_to(PACKAGE) or output==PACKAGE or output.exists():
        raise ValueError('Use a fresh results folder inside V24')
    verify_manifest(BASELINE);verify_manifest(G7_CACHE)
    saved=pd.read_csv(G7_CACHE/'admission_audit.csv').set_index('sid')
    if not saved.all_g7_allowed.isin([True,False]).all():raise ValueError('Invalid saved G7 decisions')
    days=source.available_days(through)
    if limit:days=days[:limit]
    code_files=[Path(m.__file__) for m in [policy,features,engine,data,g7,source,reporting,engine.exits,engine.native]]
    code_files+=[Path(__file__),Path(build_stock_audit.__code__.co_filename),Path(build_nifty_audit.__code__.co_filename)]
    code_files+=[Path(engine.execution_paths.__code__.co_filename),Path(engine.exit_timestamp.__code__.co_filename)]
    code_files+=[Path(m.__file__) for m in [engine.native.g,engine.native.g.v9,engine.native.g.v9.v5]]
    code_files+=list(map(Path,data._code()))
    code_files += [ROOT/name for name in [
        'research/g7_g6_replica_backtest.py','research/g7_v4_extremes.py',
        'research/g7_v4_indicator_iteration.py','research/g4_nifty_direction_backtest.py',
        'research/g6_nifty_direction.py','g3_one_minute_extension.py',
        'g3_v23_adx_rsi_20261106/trailing_20261106/engine.py']]
    code_hashes={str(p):sha(p) for p in code_files}
    signature=dict(code=code_hashes,policy=policy.fingerprint(),baseline_manifest=sha(BASELINE/'manifest.json'),
                   g7_manifest=sha(G7_CACHE/'manifest.json'))
    stamp=hashlib.sha256(json.dumps(signature,sort_keys=True).encode()).hexdigest()[:16]
    checkpoint_dir=PACKAGE/('checkpoints_'+stamp);checkpoint_dir.mkdir(exist_ok=True)
    source_cache=PACKAGE/('source_cache_'+sha(data.__file__)[:12])
    pools=[];source_meta=[]
    for i,day in enumerate(days):
        item=data.load_day(i,day,source_cache)
        pool=item['audit'].loc[item['audit'].v9_filter_pass.eq(True)].copy()
        signal=pd.to_datetime(pool.signal_ts,utc=True)
        confirmation=pd.to_datetime(pool.confirmation_ts,utc=True)
        if (not pd.to_datetime(pool.v9_5m_feature_ts,utc=True).eq(signal).all()
            or not pd.to_datetime(pool.v9_feature_available_ts,utc=True).le(confirmation).all()
            or not confirmation.sub(signal).eq(pd.Timedelta(minutes=1)).all()):
            raise ValueError('Candidate feature chronology failed: '+day)
        before,_=policy.select(pool,'baseline_v23')
        if set(before.sid)!=set(item['baseline_selected_sids']):
            raise ValueError('Native pre-G7 quota/volume parity failed: '+day)
        pools.append(pool)
        meta=source_cache/f'{i:02d}_{day}.json'
        source_meta.append(dict(path=str(meta),sha256=sha(meta),metadata=json.loads(meta.read_text())))
        print(f'Input validation {i+1}/{len(days)} {day}: {len(pool)} eligible candidates',flush=True)
        del item
    pool=pd.concat(pools,ignore_index=True)
    prepared_path=checkpoint_dir/('prepared_'+str(len(days))+'.pkl')
    prepared_meta=prepared_path.with_suffix('.json')
    if prepared_path.exists() and prepared_meta.exists():
        meta=json.loads(prepared_meta.read_text())
        if meta['days']!=days or meta['sha256']!=sha(prepared_path):raise ValueError('Prepared feature cache changed')
        prepared=pickle.loads(prepared_path.read_bytes())
        verify_aux(prepared['feature_proofs']);verify_aux(prepared['g7_proofs'])
    else:
        print('Building batched causal selection features for '+str(len(pool))+' candidates',flush=True)
        fa,fp=features.build_selection_features(pool)
        pool=pool.merge(fa[['sid']+[c for c in fa if c.startswith('v24_')]],on='sid',validate='one_to_one',how='left')
        selected={};rank_audits=[]
        for name in policy.POLICIES:
            selected[name],audit=policy.select(pool,name)
            audit['v24_policy']=name;rank_audits.append(audit)
        union=pd.concat(selected.values(),ignore_index=True).drop_duplicates('sid')
        print('Applying unchanged G7 gates to '+str(len(union))+' unique ranked candidates',flush=True)
        allowed,gp=g7_allowed(union,saved)
        prepared=dict(pool=pool,selected=selected,rank_audit=pd.concat(rank_audits,ignore_index=True),
                      allowed=allowed,feature_proofs=fp,g7_proofs=gp)
        prepared_path.write_bytes(pickle.dumps(prepared,protocol=5))
        dump(prepared_meta,dict(days=days,sha256=sha(prepared_path)))
    all_pool=prepared['pool'];allowed=prepared['allowed']
    books={p:[] for p in policy.POLICIES};orders={p:[] for p in policy.POLICIES}
    coverage=[];proofs=[dict(source_cache_metadata=source_meta,features=prepared['feature_proofs'],
                           g7_new_candidates=prepared['g7_proofs'])];audits=[]
    for i,day in enumerate(days):
        target=checkpoint_dir/(day+'.pkl');meta_path=checkpoint_dir/(day+'.json')
        if target.exists() and meta_path.exists():
            meta=json.loads(meta_path.read_text())
            if meta['signature']!=signature or meta['sha256']!=sha(target):
                raise ValueError('V24 checkpoint code/source changed: '+day)
            record=pickle.loads(target.read_bytes())
            if record['day']!=day:raise ValueError('Checkpoint session mismatch')
            cached=True
        else:
            item=data.load_day(i,day,source_cache)
            pool=all_pool.loc[all_pool.day.eq(day)]
            selected={name:frame.loc[frame.day.eq(day)].copy() for name,frame in prepared['selected'].items()}
            admitted={name:frame.loc[frame.sid.map(allowed).fillna(False)].copy() for name,frame in selected.items()}
            union=pd.concat(admitted.values(),ignore_index=True).drop_duplicates('sid')
            fills,native_fills=engine.simulate(union,item['minutes'])
            day_books={name:fills.loc[fills.sid.isin(frame.sid)].copy() for name,frame in admitted.items()}
            rank_audit=prepared['rank_audit'].loc[prepared['rank_audit'].day.eq(day)].copy()
            rank_audit['g7_allowed_if_rank_selected']=rank_audit.sid.map(allowed)
            record=dict(day=day,books=day_books,orders=admitted,audit=rank_audit,
                coverage=dict(day=day,eligible_candidates=len(pool),candidate_symbols=pool.tradingsymbol.nunique(),
                    feature_ready=int(pool.v24_feature_ready.sum()),feature_missing=int((~pool.v24_feature_ready).sum()),
                    groups_with_competition=int(pool.groupby('setup_id').size().gt(1).sum())),
                proof=dict(source=item['proof']))
            target.write_bytes(pickle.dumps(record,protocol=5))
            dump(meta_path,dict(signature=signature,sha256=sha(target)))
            cached=False
        for name in policy.POLICIES:
            books[name].append(record['books'][name]);orders[name].append(record['orders'][name])
        coverage.append(record['coverage']);proofs.append(record['proof']);audits.append(record['audit'])
        b=record['books']['baseline_v23'];c=record['books']['combined_rank']
        print(f"{i+1}/{len(days)} {day}: baseline {len(b)} trades Rs{b.portfolio_net_profit_rupees.sum():.2f}; "
              f"combined {len(c)} Rs{c.portfolio_net_profit_rupees.sum():.2f}"+(' cached' if cached else ''),flush=True)
    if any(sha(p)!=h for p,h in code_hashes.items()):raise ValueError('Code changed while running')
    books={name:pd.concat(parts,ignore_index=True) for name,parts in books.items()}
    orders={name:pd.concat(parts,ignore_index=True) for name,parts in orders.items()}
    parity=baseline_parity(books['baseline_v23'],orders['baseline_v23'],days)
    config=policy.configuration()|dict(historydays=days,history_through=days[-1],baseline_parity=parity,
        baseline_exit_policy='breakeven_eighty_target',baseline_exit_settings=engine.SETTINGS,
        per_trade_exposure_rs=500000,cost_per_trade_rs=250,unlimited_concurrent_capital=True,
        excluded_dates=['2026-10-01 (inherited incomplete source)','2026-11-08 (special session, different schedule)'],
        all_results_retrospective=True,live_changed=False,source_fingerprints=signature)
    output.mkdir(parents=True)
    summary=reporting.publish(output,books,orders,days,config,coverage)
    dump(output/'config.json',config);dump(output/'summary.json',summary)
    dump(output/'source_provenance.json',proofs)
    pd.concat(audits,ignore_index=True).to_csv(output/'ranking_audit.csv',index=False)
    pd.DataFrame(coverage).to_csv(output/'coverage.csv',index=False)
    dump(output/'manifest.json',{p.name:sha(p) for p in output.iterdir() if p.is_file()})
    print('V23 baseline parity: '+json.dumps(parity),flush=True)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--through',default='2026-11-09');parser.add_argument('--output',type=Path)
    parser.add_argument('--limit',type=int)
    args=parser.parse_args();run(args.through,args.output,args.limit)
