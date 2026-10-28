"""Bounded, chronological G7 V4 research. Never publishes a live strategy.

python fno_v13_v10_g_7_v4_backtest.py
Stage 1 measures location, stage 2 adds exhaustion, stage 3 reprices retests.
Only development dates choose a rule; validation cannot select a replacement.
"""
from __future__ import annotations
import argparse
from datetime import date,datetime,timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g7_v4_extremes as model
from research.g7_entry_extremes_20261026 import SOURCE,DAILY,ROOT
from research.g7_entry_quality_20261026 import verify_members,same_book,DAILIES
from research.g8_exit_engine import allocate_variant,metrics,_flags
from research.g8_vix_selection_research import sha,dump
from research import g7_daily_backtest as daily
from research import g8_sources

VERSION='V13-V10-G-7-V4-EXTREME-ENTRY-RESEARCH'
CONFIG=ROOT/'configs/strategies/v13_v10_g_7_extreme_entries_v4.json'
OUT_ROOT=daily.g7.G7_ROOT/'v4_research'
SOURCE_SHA='1d419d78bc46371c78dac72889262ba00cfd2eae0d2e4f3bc246d2a7e13042be'
FINAL_DAY='2026-10-27'


def read(path): return json.loads(Path(path).read_text(encoding='utf-8'))


def seal(folder,**extra):
    dump(folder/'manifest.json',dict(files={p.relative_to(folder).as_posix():sha(p) for p in folder.rglob('*') if p.is_file() and p!=folder/'manifest.json'},**extra))


def protected_hashes():
    paths=[ROOT/'configs/strategies/active_profile.json',daily.g7.V2_CONFIG,daily.g7.DEFAULT_CONFIG]
    paths+=list((daily.g7.G7_ROOT/'latest').glob('*.json'))
    return {str(p):sha(p) for p in paths if p.is_file()}


def prepare(cache):
    """Freeze all opportunities, including never-filled and capital-rejected ones."""
    cache.mkdir(parents=True,exist_ok=False)
    proof=[verify_members(SOURCE,SOURCE_SHA)]
    reference=read(DAILY/'config.json')
    settings=read(CONFIG)
    if settings['base_config_sha256']!=sha(DAILY/'config.json') or settings['base_parameters']!=reference:
        raise ValueError('V4 must preserve the pinned V2 base parameters')
    native=pd.read_csv(SOURCE/'no_index_filter/portfolio_trades.csv')
    baseline=pd.read_csv(SOURCE/'g7_v2/portfolio_trades.csv')
    info=pd.read_csv(SOURCE/'entry_features_and_decisions.csv')[['sid','allowed_g7_v2','stock_source_path','stock_source_sha256']]
    source=g8_sources.load_sources()
    paths=dict(source['paths'])
    proof.append(dict(path=str(source['cache']),manifest_sha256=source['manifest_sha256']))
    for folder,expected in DAILIES:
        proof.append(verify_members(folder,expected))
        orders=pd.read_csv(folder/'selected_orders.csv')
        day=str(orders.day.iloc[0])[:10]
        remap={int(sid):int(day.replace('-',''))*10000+i for i,sid in enumerate(orders.sid)}
        for sid,part in pd.read_parquet(folder/'minute_paths.parquet').groupby('sid'):
            paths[remap[int(sid)]]={k:part[k].to_numpy() for k in g8_sources.PATH_FIELDS}
    # This constructs the new baseline under V2 rules without touching daily/latest.
    directory,payload,provenance=daily.load_source(date.fromisoformat(FINAL_DAY))
    if payload['strategy_fingerprint']!=daily.g7.DONOR_FINGERPRINT:
        raise ValueError('Final-day donor profile differs')
    source_manifest_sha=sha(directory/'run_manifest.json')
    prices=daily.g7.Prices(dict(daily_path_evidence=[{k:provenance[k] for k in ('snapshot_manifest','snapshot_manifest_sha256')}]))
    orders,pool,changes,slot_audit=daily.select_orders(date.fromisoformat(FINAL_DAY),directory,payload,prices,reference)
    raw_paths=prices.paths(orders.sort_values(['tradingsymbol','confirmation_ts']))
    unfiltered=daily.bounded_replay(orders,raw_paths,date.fromisoformat(FINAL_DAY),reference)
    audit,nifty_proof,nifty_inputs=daily.nifty.order_audit(unfiltered,prices)
    filtered=daily.filter_book(unfiltered,audit,reference)
    holdout=cache/'final_day_source'; holdout.mkdir()
    for name,frame in [('selected_orders',orders),('unfiltered',unfiltered),('baseline',filtered),('nifty_audit',audit),('nifty_proof',nifty_proof),('slot_audit',slot_audit),('selection_changes',changes)]:
        frame.to_csv(holdout/(name+'.csv'),index=False)
    for day,frame in nifty_inputs.items(): frame.to_parquet(holdout/(day+'_nifty.parquet'),index=False)
    for path,item in prices.proof.items():
        if sha(Path(path))!=item['sha256']: raise ValueError('Final stock input drift')
    for row in nifty_proof.itertuples():
        if sha(Path(row.path))!=row.sha256: raise ValueError('Nifty source drift')
    if sha(directory/'run_manifest.json')!=source_manifest_sha: raise ValueError('Daily source drift')
    dump(holdout/'provenance.json',dict(source_run=str(directory),source_manifest_sha256=source_manifest_sha,
        source_coverage=payload.get('coverage',{}),price_sources=prices.proof,config=reference))
    remap={int(sid):int(FINAL_DAY.replace('-',''))*10000+i for i,sid in enumerate(orders.sid)}
    names={Path(p).name:(p,v['sha256']) for p,v in prices.proof.items()}
    extra=[]
    for row in orders.to_dict('records'):
        path,digest=names[row['tradingsymbol']+'_stocks_indicators_1min.parquet']
        extra.append(dict(sid=remap[int(row['sid'])],allowed_g7_v2=bool(audit.set_index('sid').loc[row['sid'],'both_counter_allowed']),stock_source_path=path,stock_source_sha256=digest))
    for frame in (unfiltered,filtered):
        frame['source_sid']=frame.sid; frame['sid']=frame.sid.map(remap).astype('int64')
        frame['day']=frame.day.astype(str).str[:10]
        frame['g8_baseline_filled']=_flags(unfiltered.filled,'native_filled').to_numpy()
    paths.update({remap[int(k)]:v for k,v in raw_paths.items()})
    native=pd.concat([native,unfiltered],ignore_index=True,sort=False)
    baseline=pd.concat([baseline,filtered],ignore_index=True,sort=False)
    info=pd.concat([info,pd.DataFrame(extra)],ignore_index=True).set_index('sid',drop=False)
    if native.sid.duplicated().any() or set(native.sid)!=set(info.sid) or set(paths)!=set(info.sid):
        raise ValueError('Candidate/path inventory mismatch')
    baseline=baseline.set_index('sid').loc[native.sid].reset_index()
    computed=[]
    groups=info.groupby(['stock_source_path','stock_source_sha256'],sort=False)
    indexed=native.set_index('sid')
    for number,((path,digest),group) in enumerate(groups,1):
        path=Path(path)
        if sha(path)!=digest: raise ValueError('Changed stock snapshot')
        schema=pq.read_schema(path).names
        columns=['date',*model.FIELDS,'gap_filled','opening_snapshot','provisional_stale','is_synthetic']
        raw=pd.read_parquet(path,columns=[c for c in columns if c in schema])
        raw['ts']=pd.to_datetime(raw.date,utc=True).dt.tz_convert(model.IST)
        days={str(k):v.sort_values('ts') for k,v in raw.groupby(raw.ts.dt.date,sort=False)}
        memo={}
        for sid in group.sid:
            row=indexed.loc[sid].to_dict(); row['sid']=sid
            computed.append(model.feature_row(days,row,memo))
        if sha(path)!=digest: raise ValueError('Price changed during feature build')
        if number%30==0: print('Causal feature snapshots',number,'/',len(groups),flush=True)
    features=pd.DataFrame(computed).set_index('sid',drop=False).join(info.drop(columns='sid'),validate='one_to_one').loc[native.sid]
    native.to_csv(cache/'native.csv',index=False); baseline.to_csv(cache/'baseline.csv',index=False)
    features.to_csv(cache/'features.csv',index=False)
    np.savez_compressed(cache/'minute_paths.npz',**{f'{sid}_{field}':a for sid,value in paths.items() for field,a in value.items()})
    prior=read(SOURCE/'summary.json')
    periods=dict(all=sorted(native.day.unique()),development=prior['periods']['development'],
        validation=prior['periods']['validation'],assessment=prior['periods']['assessment'],
        known_oct26=['2026-10-26'],final_oct27=[FINAL_DAY])
    if len(periods['all'])!=59: raise ValueError('Unexpected session inventory')
    dump(cache/'provenance.json',dict(source_packages=proof,periods=periods,base_config=reference,
        final_source_run=str(directory),final_source_manifest_sha256=source_manifest_sha,
        baseline_orders=len(native),limitations=prior['limitations'],
        feature_code_sha256=sha(Path(model.__file__)),config_sha256=sha(CONFIG)))
    seal(cache)
    return load_cache(cache)


def load_cache(cache):
    verify_members(cache)
    provenance=read(cache/'provenance.json')
    if provenance['feature_code_sha256']!=sha(Path(model.__file__)) or provenance['config_sha256']!=sha(CONFIG):
        raise ValueError('Cache implementation/configuration changed; rebuild features')
    native=pd.read_csv(cache/'native.csv'); baseline=pd.read_csv(cache/'baseline.csv')
    features=pd.read_csv(cache/'features.csv').set_index('sid',drop=False)
    with np.load(cache/'minute_paths.npz',allow_pickle=False) as arrays:
        paths={int(sid):{k:arrays[f'{sid}_{k}'].copy() for k in g8_sources.PATH_FIELDS} for sid in native.sid}
    return native,baseline,features,paths,provenance


def comparison_row(book,baseline,dates):
    a=book.set_index('sid'); b=baseline.set_index('sid').loc[a.index]
    keep=a.day.isin(dates)
    ax=_flags(a.portfolio_executed,'executed'); bx=_flags(b.portfolio_executed,'baseline')
    changed=(ax.ne(bx) | (ax & bx & (~np.isclose(a.entry_price,b.entry_price,equal_nan=True)))) & keep
    missed=bx & ~ax & keep
    stat=metrics(book,dates); original=metrics(baseline,dates)
    return dict(**stat,net_change_rs=stat['net_pnl_rs']-original['net_pnl_rs'],
        loss_amount_reduction_rs=original['negative_trade_sum_abs_rs']-stat['negative_trade_sum_abs_rs'],
        trade_retention_pct=100*stat['trades']/original['trades'] if original['trades'] else 100.,
        changed_executions=int(changed.sum()),changed_days=int(a.loc[changed,'day'].nunique()),
        avoided_baseline_losers=int((missed & b.portfolio_net_profit_rupees.lt(0)).sum()),
        missed_baseline_winners=int((missed & b.portfolio_net_profit_rupees.gt(0)).sum()),
        new_allocations=int((ax & ~bx & keep).sum()))


def screen(comparison,policies,period='development'):
    """Read ONLY the named chronological slice; never choose from future results."""
    table=comparison.loc[comparison.period.eq(period)].set_index('policy')
    b=table.loc['g7_v2']; audit=[]; eligible=[]
    for policy in policies:
        r=table.loc[policy]; failures=[]
        if r.net_pnl_rs < .95*b.net_pnl_rs-1e-6: failures.append('net profit retention below 95%')
        if r.negative_trade_sum_abs_rs > .95*b.negative_trade_sum_abs_rs+1e-6: failures.append('losing-trade amount reduction below 5%')
        if r.trades < .70*b.trades: failures.append('trade retention below 70%')
        if r.wins < .85*b.wins: failures.append('winner retention below 85%')
        if r.daily_close_drawdown_rs > b.daily_close_drawdown_rs+1e-6: failures.append('larger daily-close drawdown')
        if r.changed_executions < 5 or r.changed_days < 3: failures.append('insufficient changed trades/days')
        audit.append(dict(policy=policy,period=period,eligible=not failures,failure_reasons='; '.join(failures)))
        if not failures:
            eligible.append((r.loss_amount_reduction_rs,r.net_pnl_rs,policy))
    return (max(eligible)[2] if eligible else None),audit


def verify(output,check_sources=True):
    out=Path(output).resolve(); verify_members(out)
    summary=read(out/'summary.json'); manifest=read(out/'manifest.json')
    for path,digest in manifest['code'].items():
        if sha(ROOT/path)!=digest: raise ValueError('Research code changed: '+path)
    cache=Path(summary['cache']); native,base,f,paths,provenance=load_cache(cache)
    if check_sources:
        for path,digest in f[['stock_source_path','stock_source_sha256']].drop_duplicates().itertuples(index=False,name=None):
            if sha(Path(path))!=digest: raise ValueError('Price evidence changed')
        for package in provenance['source_packages']: verify_members(package['path'],package['manifest_sha256'])
        if sha(Path(provenance['final_source_run'])/'run_manifest.json')!=provenance['final_source_manifest_sha256']: raise ValueError('Final-day source drift')
    fl=model.flags(f); allowed=_flags(f.allowed_g7_v2,'nifty')
    expected=pd.read_csv(out/'comparison.csv').set_index(['policy','period'])
    settings=read(CONFIG); config=dict(provenance['base_config'],max_positions_per_symbol=1)
    for name in ['g7_v2','v4_symbol_guard',*model.POLICIES]:
        mask=allowed if name in ('g7_v2','v4_symbol_guard','conditional_retest') else allowed & ~fl[name]
        original=model.replay_retests(native,paths,fl['extreme']) if name=='conditional_retest' else native
        replay=allocate_variant(original,mask,provenance['base_config'] if name=='g7_v2' else config,name)
        book=pd.read_csv(out/name/'portfolio_trades.csv')
        same_book(replay,book)
        if name=='g7_v2': same_book(book,base)
        for period,dates in summary['periods'].items():
            actual=comparison_row(book,base,dates)
            for key in ('trades','wins','losses','net_pnl_rs','daily_close_drawdown_rs','negative_trade_sum_abs_rs','missed_baseline_winners','avoided_baseline_losers'):
                if not np.isclose(actual[key],expected.loc[(name,period),key],atol=1e-6,rtol=0): raise ValueError('Saved accounting mismatch '+name+'/'+key)
    selected,_=screen(expected.reset_index(),list(model.POLICIES))
    if selected!=summary['selected_candidate']: raise ValueError('Development selection differs')
    final=pd.read_csv(out/'g7_v4/portfolio_trades.csv')
    same_book(final,pd.read_csv(out/(selected or 'v4_symbol_guard')/'portfolio_trades.csv'))
    return True


def run(cache_path=None,prepare_only=False):
    config_digest=sha(CONFIG); protected=protected_hashes()
    out=OUT_ROOT/'runs'/FINAL_DAY/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_v4_extremes')
    out.mkdir(parents=True,exist_ok=False)
    dump(out/'protocol.json',read(CONFIG))
    cache=Path(cache_path) if cache_path else out/'inputs'
    native,base,f,paths,provenance=load_cache(cache) if cache_path else prepare(cache)
    print('PREPARED',cache,'orders',len(native),flush=True)
    if prepare_only: return out
    periods=provenance['periods']; base_config=provenance['base_config']
    v4_config=dict(base_config,max_positions_per_symbol=1)
    fl=model.flags(f); permitted=_flags(f.allowed_g7_v2,'nifty')
    books={}; train=[]; saved_decisions=f.copy()
    for name in ['g7_v2','v4_symbol_guard',*model.POLICIES]:
        mask=permitted if name in ('g7_v2','v4_symbol_guard','conditional_retest') else permitted & ~fl[name]
        original=model.replay_retests(native,paths,fl['extreme']) if name=='conditional_retest' else native
        book=allocate_variant(original,mask,base_config if name=='g7_v2' else v4_config,name)
        book['strategy']='V13-V10-G-7'; book['strategy_version']=VERSION if name!='g7_v2' else base_config['variant_id']
        book['v4_policy']=name; book['v4_protocol_sha256']=config_digest
        # Include the full protocol identity rather than just the scenario label.
        book['strategy_fingerprint']=__import__('hashlib').sha256((str(book.strategy_fingerprint.iloc[0])+config_digest).encode()).hexdigest()
        books[name]=book
        if name=='g7_v2': same_book(book,base)
        stat=comparison_row(book,base,periods['development'])
        train.append(dict(policy=name,period='development',stage=model.POLICIES.get(name,(0,''))[0],**stat))
        folder=out/name; folder.mkdir(); book.to_csv(folder/'portfolio_trades.csv',index=False)
        saved_decisions['allowed_'+name]=mask
        print('DEVELOPMENT',name,'net',round(stat['net_pnl_rs'],2),'loss amount',round(stat['negative_trade_sum_abs_rs'],2),flush=True)
    development=pd.DataFrame(train)
    selected,selection=screen(development,list(model.POLICIES))
    # This durable lock precedes evaluation/printing of validation and later dates.
    dump(out/'selection_lock.json',dict(selected_candidate=selected,selected_from='development_only',
        config_sha256=config_digest,locked_at_utc=datetime.now(timezone.utc).isoformat(),
        fallback='v4_symbol_guard',validation_cannot_select_replacement=True))
    pd.DataFrame(selection).to_csv(out/'development_selection.csv',index=False)
    final_policy=selected or 'v4_symbol_guard'
    final=books[final_policy].copy(); final['v4_policy']=final_policy
    (out/'g7_v4').mkdir(); final.to_csv(out/'g7_v4/portfolio_trades.csv',index=False)
    comparisons=[]; daily_rows=[]; costs=[]; changes=[]
    for name,book in books.items():
        for period,dates in periods.items():
            comparisons.append(dict(policy=name,period=period,**comparison_row(book,base,dates)))
        for day in periods['all']:
            daily_rows.append(dict(policy=name,day=day,**comparison_row(book,base,[day])))
        for cost in (5,15,30):
            stressed=allocate_variant(book,_flags(book.set_index('sid').g8_direction_allowed,'allowed'),base_config if name=='g7_v2' else v4_config,name,cost_bps=cost)
            for period,dates in periods.items(): costs.append(dict(policy=name,cost_bps=cost,period=period,**metrics(stressed,dates)))
        a=book.set_index('sid'); b=base.set_index('sid').loc[a.index]
        change=pd.DataFrame(dict(sid=a.index,day=a.day,tradingsymbol=a.tradingsymbol,side=a.side,
            baseline_executed=b.portfolio_executed,variant_executed=a.portfolio_executed,
            baseline_entry=b.entry_ts,variant_entry=a.entry_ts,baseline_net_rs=b.portfolio_net_profit_rupees,
            variant_net_rs=a.portfolio_net_profit_rupees,net_change_rs=a.portfolio_net_profit_rupees-b.portfolio_net_profit_rupees))
        change['policy']=name; changes.append(change)
    comparison=pd.DataFrame(comparisons); daywise=pd.DataFrame(daily_rows)
    validation_qualified=False; validation_audit=[]
    if selected:
        candidate,validation_audit=screen(comparison,[selected],period='validation')
        validation_qualified=candidate==selected
    assessment_qualified=False
    if selected and validation_qualified:
        index=comparison.set_index(['policy','period']); a=index.loc[(selected,'assessment')]; b=index.loc[('g7_v2','assessment')]
        assessment_qualified=bool(a.net_pnl_rs>=b.net_pnl_rs and a.negative_trade_sum_abs_rs<b.negative_trade_sum_abs_rs and a.daily_close_drawdown_rs<=b.daily_close_drawdown_rs+1e-6)
    # Paired daily uncertainty is descriptive; not a correction for trying nine rules.
    rng=np.random.default_rng(20261027); uncertainty=[]
    wide=daywise.pivot(index='day',columns='policy',values='net_pnl_rs')
    for name in model.POLICIES:
        dates=periods['validation']+periods['assessment']
        delta=(wide[name]-wide.g7_v2).loc[dates].to_numpy()
        draws=rng.choice(delta,size=(5000,len(delta)),replace=True).mean(axis=1)
        uncertainty.append(dict(policy=name,sessions=len(delta),mean_daily_change_rs=float(delta.mean()),
            ci95_low_rs=float(np.quantile(draws,.025)),ci95_high_rs=float(np.quantile(draws,.975))))
    saved_decisions.to_csv(out/'entry_features_and_decisions.csv',index=False)
    comparison.to_csv(out/'comparison.csv',index=False); daywise.to_csv(out/'daywise_comparison.csv',index=False)
    pd.DataFrame(costs).to_csv(out/'cost_sensitivity.csv',index=False)
    pd.concat(changes,ignore_index=True).to_csv(out/'trade_changes.csv',index=False)
    pd.DataFrame(uncertainty).to_csv(out/'paired_day_uncertainty.csv',index=False)
    pd.DataFrame(validation_audit,columns=['policy','period','eligible','failure_reasons']).to_csv(out/'validation_selection.csv',index=False)
    # One compact V2/V4 daily file for the user's usual IDE workflow.
    dates=periods['all']; idx=daywise.set_index(['policy','day'])
    pd.DataFrame([dict(day=d,g7_v2_net_rs=idx.loc[('g7_v2',d),'net_pnl_rs'],g7_v4_net_rs=idx.loc[(final_policy,d),'net_pnl_rs'],
        net_change_rs=idx.loc[(final_policy,d),'net_change_rs'],g7_v2_trades=idx.loc[('g7_v2',d),'trades'],
        g7_v4_trades=idx.loc[(final_policy,d),'trades'],g7_v4_losses=idx.loc[(final_policy,d),'losses']) for d in dates]).to_csv(out/'g7_v4/daywise_comparison.csv',index=False)
    summary=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,promotion=False,
        cache=str(cache.resolve()),protocol_sha256=config_digest,periods=periods,
        selected_candidate=selected,final_policy=final_policy,validation_qualified=validation_qualified,
        assessment_qualified=assessment_qualified,independent_forward_validation=False,
        baseline_reproduced=True,orders=len(native),sessions=len(dates),candidate_count=len(model.POLICIES),
        historical_feature_coverage=int(_flags(f.historical_ready,'ready').sum()),
        protected_files_unchanged=protected,limitations=provenance['limitations']+[
            'Development/validation/assessment history and Oct26 were inspected in earlier research; these are retrospective comparisons.',
            'Oct27 was withheld from V4 selection but is one retrospective day, not independent forward performance.',
            'Historical percentiles use up to60 prior same-stock/same-clock sessions, minimum20 valid prefixes; insufficient warmup preserves baseline and is reported.',
            'VWAP/RSI need30 completed session bars; Bollinger needs20. Zero dispersion is NOT_APPLICABLE, not a profitable missing-data filter.',
            'Retest order arms only after separate completed breakout and retest bars, keeps original10-minute expiry, then reprices stops and targets.',
            'One active position per symbol is a fixed V4 rule; symbol-guard-only control separates its effect.',
            'Drawdown is end-of-day realized equity, not intraday marked-to-market drawdown.'])
    if sha(CONFIG)!=config_digest or protected_hashes()!=protected: raise ValueError('Protocol or active controls changed')
    dump(out/'summary.json',summary)
    report(out,summary,comparison,daywise,pd.DataFrame(selection),pd.DataFrame(uncertainty))
    code=[Path(__file__),Path(model.__file__),CONFIG,ROOT/'fno_v13_v10_g_7_v4_backtest.py',ROOT/'research/g8_exit_engine.py',
          ROOT/'research/g3_sl_target_simulator.py',ROOT/'research/g7_daily_backtest.py',ROOT/'fno_v13_corrected_v5_backtest.py',ROOT/'research/g4_vwap_rsi_features.py']
    seal(out,code={str(p.relative_to(ROOT)):sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT/'latest.json',dict(output=str(out.resolve()),manifest_sha256=sha(out/'manifest.json'),execution_authority=False))
    print(comparison.loc[comparison.policy.isin(['g7_v2',final_policy]),['policy','period','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('OUTPUT',out,'SELECTED',selected,'VALIDATION',validation_qualified,'ASSESSMENT',assessment_qualified,flush=True)
    return out


def report(out,summary,comparison,daywise,selection,uncertainty):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    chosen=summary['final_policy']
    fig,axes=plt.subplots(2,1,figsize=(12,8),constrained_layout=True)
    for policy,label in [('g7_v2','G7 V2'),(chosen,'G7 V4 ('+chosen+')')]:
        rows=daywise.loc[daywise.policy.eq(policy)].sort_values('day')
        x=pd.to_datetime(rows.day)
        axes[0].plot(x,rows.net_pnl_rs.cumsum(),label=label,lw=1.8)
        axes[1].plot(x,rows.negative_trade_sum_abs_rs.cumsum(),label=label,lw=1.8)
    axes[0].set_title('Cumulative net profit after model costs (INR)')
    axes[1].set_title('Cumulative losing-trade amount (INR; lower is better)')
    for ax in axes:
        ax.grid(alpha=.2); ax.legend(); ax.tick_params(axis='x',rotation=20)
        for d in ('2026-09-21','2026-10-13','2026-10-26'): ax.axvline(pd.Timestamp(d),ls=':',color='#888',lw=.8)
    fig.savefig(out/'g7_v4_comparison.png',dpi=140); plt.close(fig)
    columns=['policy','trades','wins','losses','net_pnl_rs','net_change_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','avoided_baseline_losers','missed_baseline_winners']
    html="<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>G7 V4 extreme-entry research</title><style>body{font:15px/1.5 system-ui;margin:28px;color:#182638}table{border-collapse:collapse;font-size:13px}td,th{padding:7px;border:1px solid #ddd;white-space:nowrap}.scroll{overflow:auto}img{max-width:100%}</style>"
    html+='<h1>G7 V4 — extreme-entry research through 27 October 2026</h1>'
    html+=f'<p>59 available sessions; {summary["orders"]} selected opportunities. V2 baseline reproduced exactly. Nine fixed candidates in three stages. Development selection: <b>{summary["selected_candidate"] or "NONE"}</b>. V4 output policy: {chosen}. Validation passed: {summary["validation_qualified"]}; later assessment passed: {summary["assessment_qualified"]}. Backtest only; no live activation.</p>'
    html+='<p>The development screen requires at least95% net-profit retention, at least5% reduction in losing-trade amount, at least70% trade retention, at least85% winner retention, no larger daily-close drawdown, and at least5 changed trades across3 days. If none qualifies, V4 falls back to the one-position-per-symbol control. Validation cannot choose a replacement. Oct26 is a known diagnostic day; Oct27 was excluded from selection.</p>'
    html+='<p>INR10 lakh portfolio, INR1 lakh margin per entry,5x exposure and5bps round-trip cost. Costs at15/30bps are separate sensitivity checks. Three missing source sessions (24/25 August and1 October) are excluded. Historical targets and source-universe eligibility were previously inspected; no independent forward-validation claim.</p>'
    html+='<img src="g7_v4_comparison.png"><h2>Fixed iterations</h2><ol>'
    for stage in (1,2,3):
        html+=f'<li>Stage {stage}: '+ '; '.join(name+' — '+desc for name,(s,desc) in model.POLICIES.items() if s==stage)+'</li>'
    html+='</ol>'
    for period in summary['periods']:
        html+='<h2>'+period+'</h2><div class="scroll">'+comparison.loc[comparison.period.eq(period),columns].to_html(index=False,float_format=lambda x:f'{x:,.2f}')+'</div>'
    html+='<h2>Development screening</h2>'+selection.to_html(index=False)
    html+='<h2>Paired daily uncertainty: validation and assessment</h2><p>Descriptive bootstrap only, unadjusted for trying multiple rules.</p>'+uncertainty.to_html(index=False,float_format=lambda x:f'{x:,.2f}')
    html+='<h2>Artifacts</h2><p><a href="g7_v4/daywise_comparison.csv">V2 vs V4 daywise results</a> | <a href="g7_v4/portfolio_trades.csv">V4 all-order ledger</a> | <a href="trade_changes.csv">Every avoided/changed trade</a> | <a href="entry_features_and_decisions.csv">Causal features</a> | <a href="cost_sensitivity.csv">Costs</a> | <a href="summary.json">Limitations and provenance</a></p>'
    (out/'report.html').write_text(html,encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache',type=Path); parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--verify',type=Path)
    args=parser.parse_args()
    if args.verify: print('VERIFIED',verify(args.verify))
    else: run(args.cache,args.prepare_only)


if __name__=='__main__': main()
