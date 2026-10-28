"""Second and final bounded V4 iteration, motivated by development-only failures.

The first round stays immutable. Five narrower candidates are declared before
their outcomes are computed. The same acceptance criteria apply to all14.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_backtest as base
from research import g7_v4_extremes as model
from research.g8_exit_engine import allocate_variant,metrics,_flags
from research.g7_entry_quality_20261026 import same_book,verify_members
from research.g8_vix_selection_research import sha,dump

ADDITIONAL={
    'vwap_failed_breakout':'VWAP extreme AND high/low breaks prior20 edge but completed confirmation closes back inside',
    'historical975_failed_breakout':'Historical97.5 extreme AND actual prior20-edge rejection; wick alone insufficient',
    'bollinger_rsi':'Bollinger2.5 extreme AND directional RSI14 weakens over3 completed minutes',
    'dual_extreme_rsi':'Both VWAP2.5 and historical95 extreme AND weakening RSI14',
    'retest_exhaustion_only':'Completed breakout/retest required only for combined-exhaustion candidates; remaining orders unchanged',
}


def triggers(f):
    fl=model.flags(f)
    failed=_flags(f.failed_prior20_breakout,'failed_breakout')
    weak=_flags(f.rsi_ready,'rsi_ready') & f.signed_rsi_delta3.lt(0)
    hist975=_flags(f.historical_ready,'historical_ready') & (
        (f.signed_vwap_distance_pct.gt(f.vwap_q975)&f.signed_vwap_distance_pct.gt(0)) |
        (f.signed_opening_move_pct.gt(f.move_q975)&f.signed_opening_move_pct.gt(0)))
    return dict(vwap_failed_breakout=fl['vwap']&failed,historical975_failed_breakout=hist975&failed,
        bollinger_rsi=fl['bollinger']&weak,dual_extreme_rsi=fl['vwap']&fl['historical']&weak,
        retest_exhaustion_only=fl['combined_exhaustion'])


def verify(out):
    out=Path(out);verify_members(out)
    summary=base.read(out/'summary.json');manifest=base.read(out/'manifest.json')
    for p,h in manifest['code'].items():
        if sha(base.ROOT/p)!=h:raise ValueError('Code changed: '+p)
    first=Path(summary['round1_output']);base.verify(first)
    native,baseline,f,paths,provenance=base.load_cache(Path(summary['cache']))
    reject=triggers(f); allowed=_flags(f.allowed_g7_v2,'nifty')
    cfg=dict(provenance['base_config'],max_positions_per_symbol=1)
    c=pd.read_csv(out/'comparison.csv').set_index(['policy','period'])
    for name in [*model.POLICIES,*ADDITIONAL,'g7_v2','v4_symbol_guard']:
        book=pd.read_csv(out/name/'portfolio_trades.csv')
        if name in ADDITIONAL:
            original=model.replay_retests(native,paths,reject[name]) if name=='retest_exhaustion_only' else native
            mask=allowed if name=='retest_exhaustion_only' else allowed & ~reject[name]
            actual=allocate_variant(original,mask,cfg,name)
            same_book(actual,book)
        else: same_book(book,pd.read_csv(first/name/'portfolio_trades.csv'))
        for period,days in summary['periods'].items():
            stats=base.comparison_row(book,baseline,days)
            for key in ('trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','avoided_baseline_losers','missed_baseline_winners'):
                if not np.isclose(stats[key],c.loc[(name,period),key],atol=1e-6,rtol=0):raise ValueError('Accounting changed '+name+'/'+key)
    selected,_=base.screen(c.reset_index(),[*model.POLICIES,*ADDITIONAL])
    if selected!=summary['selected_candidate']:raise ValueError('Selected candidate differs')
    same_book(pd.read_csv(out/'g7_v4/portfolio_trades.csv'),pd.read_csv(out/(selected or 'v4_symbol_guard')/'portfolio_trades.csv'))
    return True


def run(first):
    first=Path(first).resolve();base.verify(first)
    protected=base.protected_hashes()
    out=base.OUT_ROOT/'runs'/base.FINAL_DAY/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_v4_round2')
    out.mkdir(parents=True,exist_ok=False)
    # Fix the new protocol before calculating these five candidates' outcomes.
    protocol=dict(variant_id=base.VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        round1_output=str(first),round1_manifest_sha256=sha(first/'manifest.json'),
        reason='Development-only results: generic extreme vetoes and wick-only rejection removed too many winners; full retest overfiltered.',
        new_candidates=ADDITIONAL,total_candidate_count=14,selection=base.read(base.CONFIG)['selection'],
        stop_rule='No further candidate additions in this run. Same criteria; no validation-driven replacement.',
        chronology='First35 development choose once; next14 validation; next8 assessment; Oct26 known diagnostic; Oct27 retrospective final check.',
        independent_holdout=False,reference_parameters=base.read(base.CONFIG))
    dump(out/'protocol.json',protocol);protocol_digest=sha(out/'protocol.json')
    old=base.read(first/'summary.json');cache=Path(old['cache'])
    native,baseline,f,paths,provenance=base.load_cache(cache)
    config=dict(provenance['base_config'],max_positions_per_symbol=1)
    allowed=_flags(f.allowed_g7_v2,'nifty');reject=triggers(f)
    periods=provenance['periods'];books={};training=[]
    names=['g7_v2','v4_symbol_guard',*model.POLICIES,*ADDITIONAL]
    decisions=pd.read_csv(first/'entry_features_and_decisions.csv').set_index('sid',drop=False)
    for name in names:
        if name in ADDITIONAL:
            original=model.replay_retests(native,paths,reject[name]) if name=='retest_exhaustion_only' else native
            mask=allowed if name=='retest_exhaustion_only' else allowed & ~reject[name]
            book=allocate_variant(original,mask,config,name)
            book['strategy']='V13-V10-G-7';book['strategy_version']=base.VERSION
            book['v4_policy']=name;book['v4_protocol_sha256']=protocol_digest
            book['strategy_fingerprint']=__import__('hashlib').sha256((str(book.strategy_fingerprint.iloc[0])+protocol_digest).encode()).hexdigest()
            decisions['allowed_'+name]=mask
        else: book=pd.read_csv(first/name/'portfolio_trades.csv')
        books[name]=book
        stat=base.comparison_row(book,baseline,periods['development'])
        training.append(dict(policy=name,period='development',**stat))
        folder=out/name;folder.mkdir();book.to_csv(folder/'portfolio_trades.csv',index=False)
        if name in ADDITIONAL:print('ROUND2 DEVELOPMENT',name,'net',round(stat['net_pnl_rs'],2),'loss amount',round(stat['negative_trade_sum_abs_rs'],2),flush=True)
    selected,audit=base.screen(pd.DataFrame(training),[*model.POLICIES,*ADDITIONAL])
    final_policy=selected or 'v4_symbol_guard'
    dump(out/'selection_lock.json',dict(selected_candidate=selected,selected_from='DEVELOPMENT_ONLY',protocol_sha256=protocol_digest,
        locked_at_utc=datetime.now(timezone.utc).isoformat(),validation_cannot_select_replacement=True))
    (out/'g7_v4').mkdir();books[final_policy].to_csv(out/'g7_v4/portfolio_trades.csv',index=False)
    comparisons=[];daily=[];costs=[];changes=[]
    for name,book in books.items():
        for period,days in periods.items(): comparisons.append(dict(policy=name,period=period,**base.comparison_row(book,baseline,days)))
        for day in periods['all']:daily.append(dict(policy=name,day=day,**base.comparison_row(book,baseline,[day])))
        for cost in (5,15,30):
            stressed=allocate_variant(book,_flags(book.set_index('sid').g8_direction_allowed,'allowed'),provenance['base_config'] if name=='g7_v2' else config,name,cost_bps=cost)
            for period,days in periods.items():costs.append(dict(policy=name,cost_bps=cost,period=period,**metrics(stressed,days)))
        a=book.set_index('sid');b=baseline.set_index('sid').loc[a.index]
        changes.append(pd.DataFrame(dict(policy=name,sid=a.index,day=a.day,tradingsymbol=a.tradingsymbol,side=a.side,
            baseline_executed=b.portfolio_executed,variant_executed=a.portfolio_executed,baseline_entry=b.entry_ts,variant_entry=a.entry_ts,
            baseline_net_rs=b.portfolio_net_profit_rupees,variant_net_rs=a.portfolio_net_profit_rupees,
            net_change_rs=a.portfolio_net_profit_rupees-b.portfolio_net_profit_rupees)))
    comparison=pd.DataFrame(comparisons);daywise=pd.DataFrame(daily)
    valid,va=base.screen(comparison,[selected],period='validation') if selected else (None,[])
    validation=selected is not None and valid==selected
    assessed=False
    if selected and validation:
        index=comparison.set_index(['policy','period']);a=index.loc[(selected,'assessment')];b=index.loc[('g7_v2','assessment')]
        assessed=bool(a.net_pnl_rs>=b.net_pnl_rs and a.negative_trade_sum_abs_rs<b.negative_trade_sum_abs_rs and a.daily_close_drawdown_rs<=b.daily_close_drawdown_rs+1e-6)
    rng=np.random.default_rng(20261027);uncertainty=[]
    wide=daywise.pivot(index='day',columns='policy',values='net_pnl_rs')
    for name in [*model.POLICIES,*ADDITIONAL]:
        delta=(wide[name]-wide.g7_v2).loc[periods['validation']+periods['assessment']].to_numpy()
        draws=rng.choice(delta,size=(5000,len(delta)),replace=True).mean(axis=1)
        uncertainty.append(dict(policy=name,sessions=len(delta),mean_daily_change_rs=float(delta.mean()),ci95_low_rs=float(np.quantile(draws,.025)),ci95_high_rs=float(np.quantile(draws,.975))))
    comparison.to_csv(out/'comparison.csv',index=False);daywise.to_csv(out/'daywise_comparison.csv',index=False)
    pd.DataFrame(audit).to_csv(out/'development_selection.csv',index=False)
    pd.DataFrame(va,columns=['policy','period','eligible','failure_reasons']).to_csv(out/'validation_selection.csv',index=False)
    pd.DataFrame(costs).to_csv(out/'cost_sensitivity.csv',index=False)
    pd.DataFrame(uncertainty).to_csv(out/'paired_day_uncertainty.csv',index=False)
    pd.concat(changes,ignore_index=True).to_csv(out/'trade_changes.csv',index=False)
    decisions.to_csv(out/'entry_features_and_decisions.csv',index=False)
    idx=daywise.set_index(['policy','day'])
    pd.DataFrame([dict(day=d,g7_v2_net_rs=idx.loc[('g7_v2',d),'net_pnl_rs'],g7_v4_net_rs=idx.loc[(final_policy,d),'net_pnl_rs'],
        net_change_rs=idx.loc[(final_policy,d),'net_change_rs'],g7_v2_trades=idx.loc[('g7_v2',d),'trades'],g7_v4_trades=idx.loc[(final_policy,d),'trades'],
        g7_v4_losses=idx.loc[(final_policy,d),'losses']) for d in periods['all']]).to_csv(out/'g7_v4/daywise_comparison.csv',index=False)
    summary={**old,'round1_output':str(first),'protocol_sha256':protocol_digest,'selected_candidate':selected,'final_policy':final_policy,
        'validation_qualified':validation,'assessment_qualified':assessed,'candidate_count':14,'research_rounds':2,
        'protected_files_unchanged':protected,'limitations':old['limitations']+['Second round adds5 predefined narrower candidates after development-only analysis. All14 attempts reported; no independent statistical significance claim.']}
    dump(out/'summary.json',summary)
    base.report(out,summary,comparison,daywise,pd.DataFrame(audit),pd.DataFrame(uncertainty))
    html=(out/'report.html').read_text(encoding='utf-8').replace('Nine fixed candidates in three stages.','Fourteen candidates across two bounded research rounds.')
    extra='<h2>Second iteration: narrower confirmation</h2><p>Chosen after development-only analysis of round1; same acceptance thresholds.</p><ul>'
    extra+=''.join('<li>'+k+' — '+v+'</li>' for k,v in ADDITIONAL.items())+'</ul>'
    html=html.replace('<h2>development</h2>',extra+'<h2>development</h2>')
    (out/'report.html').write_text(html,encoding='utf-8')
    if sha(out/'protocol.json')!=protocol_digest or base.protected_hashes()!=protected:raise ValueError('Protocol or active config changed')
    code=[Path(__file__),Path(base.__file__),Path(model.__file__),base.CONFIG,base.ROOT/'fno_v13_v10_g_7_v4_iterative_backtest.py']
    base.seal(out,code={str(p.relative_to(base.ROOT)):sha(p) for p in code})
    verify(out)
    dump(base.OUT_ROOT/'latest.json',dict(output=str(out),manifest_sha256=sha(out/'manifest.json'),execution_authority=False))
    print(comparison.loc[comparison.policy.isin(['g7_v2',final_policy]),['policy','period','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('OUTPUT',out,'SELECTED',selected,'VALIDATION',validation,'ASSESSMENT',assessed,flush=True)
    return out


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--round1',type=Path,help='Resume from a sealed first-round result; otherwise build it')
    parser.add_argument('--verify',type=Path)
    args=parser.parse_args()
    if args.verify: print('VERIFIED',verify(args.verify))
    else:run(args.round1 or base.run())


if __name__=='__main__':main()
