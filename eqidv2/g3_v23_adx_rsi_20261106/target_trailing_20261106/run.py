"""Test six earlier target-relative stops, retaining all original V23 targets."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import run as original,policy as admission
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study import run as native
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from g3_v23_adx_rsi_20261106.trailing_20261106 import data
from . import engine
from .reporting import write_reports

PACKAGE=Path(__file__).resolve().parent
OUTPUT=PACKAGE/'results_through_20261105'
BASELINE=engine.BASELINE


def choose(overall):
    fit=overall.loc[overall.period.eq('fit_44')]
    base=fit.loc[fit.policy.eq(BASELINE)].iloc[0]
    eligible=fit.loc[fit.policy.ne(BASELINE)&fit.trades.eq(base.trades)
        &fit.net_pnl_rs.ge(base.net_pnl_rs)&fit.gross_loss_rs.lt(base.gross_loss_rs)
        &fit.daily_close_drawdown_rs.le(base.daily_close_drawdown_rs)]
    order=['net_pnl_rs','daily_close_drawdown_rs','gross_loss_rs','policy']
    ascending=[False,True,True,True]
    candidate=BASELINE if eligible.empty else eligible.sort_values(order,ascending=ascending).iloc[0].policy
    alternate=fit.loc[fit.policy.ne(BASELINE)&fit.gross_loss_rs.lt(base.gross_loss_rs)]
    if alternate.empty:
        alternate=fit.loc[fit.policy.ne(BASELINE)]
    diagnostic=(candidate if candidate!=BASELINE else BASELINE if alternate.empty else
                alternate.sort_values(order,ascending=ascending).iloc[0].policy)
    return candidate,diagnostic


def qualification(overall,candidate):
    if candidate==BASELINE:
        return False,['No fit44 trailing model improved losses while preserving fixed1.50% baseline profit and drawdown.']
    reasons=[]
    for period in ('validation_15','later_7','full_66'):
        values=overall.loc[overall.period.eq(period)].set_index('policy')
        base,row=values.loc[BASELINE],values.loc[candidate]
        if row.trades!=base.trades:
            reasons.append(period+': changed entry count')
        if row.net_pnl_rs<base.net_pnl_rs:
            reasons.append(period+': lower net profit')
        if row.gross_loss_rs>base.gross_loss_rs or (period=='full_66' and row.gross_loss_rs==base.gross_loss_rs):
            reasons.append(period+': loss improvement requirement failed')
        if row.daily_close_drawdown_rs>base.daily_close_drawdown_rs:
            reasons.append(period+': worse daily-close drawdown')
    return not reasons,reasons


def metric(book,selected,days):
    result=original.metrics(book,selected,days)
    pnl=book.loc[book.day.isin(days),'portfolio_net_profit_rupees']
    wins=pnl.loc[pnl>0];losses=pnl.loc[pnl<0]
    result.update(breakeven_trades=int(pnl.eq(0).sum()),
        average_win_rs=float(wins.mean()) if len(wins) else 0.,
        average_loss_rs=float(-losses.mean()) if len(losses) else 0.,
        worst_trade_rs=float(pnl.min()) if len(pnl) else 0.,
        net_expectancy_rs=float(pnl.mean()) if len(pnl) else 0.)
    return result


def replay_book(source,prefix,name,settings):
    frozen=source['trades'];meta_lookup=source['parity'].set_index('sid')
    entries=source['entry_features'].set_index('sid')
    book=frozen.copy();changes=[];parity=[];updates=[]
    for idx,row in frozen.iterrows():
        meta=meta_lookup.loc[row.sid];path=source['paths'][int(row.sid)];entry=entries.loc[row.sid]
        override=float(entry.adaptive_initial_stop_pct) if settings.get('adaptive_initial') else None
        result=engine.replay(path,int(meta.entry_index),float(row.entry_price),row.side=='LONG',
            float(meta.native_target_pct),settings,float(entry.atr14_1m),initial_override=override,
            features=prefix[int(row.sid)])
        stamp=native.exit_timestamp(path,result)
        unchanged=(stamp==pd.Timestamp(row.exit_ts) and abs(result['price']-row.exit_price)<1e-8)
        derived=(1 if row.side=='LONG' else -1)*(result['price']/row.entry_price-1)*500000-250
        pnl=float(row.portfolio_net_profit_rupees) if unchanged else derived
        if abs(pnl)<1e-7:pnl=0.
        if name==BASELINE:
            if not unchanged or result['reason']!=row.exit_reason or not np.isclose(
                    derived,row.portfolio_net_profit_rupees,atol=1e-5,rtol=0):
                raise ValueError('Fixed1.50% baseline replay mismatch: '+str(row.sid))
            parity.append(dict(sid=int(row.sid),day=row.day,constant_1p50_parity=True))
        book.loc[idx,'exit_ts']=str(stamp);book.loc[idx,'exit_price']=result['price']
        book.loc[idx,'exit_reason']=result['reason'];book.loc[idx,'portfolio_net_profit_rupees']=pnl
        # Future/session extrema below are ex-post diagnostics only. The engine
        # has already returned its decision and cannot read these values.
        sign=1 if row.side=='LONG' else -1
        # Include the entire entry bar as an upper bound, since its chronology
        # is unknown. This keeps the diagnostic denominator common across arms.
        tail_start=int(meta.entry_index)
        favorable=path['high'][tail_start:] if sign==1 else path['low'][tail_start:]
        session_mfe=max(0.,float(np.max(sign*(favorable/row.entry_price-1)*100))) if len(favorable) else 0.
        gross_return=sign*(result['price']/row.entry_price-1)*100
        capture=max(gross_return,0.)/session_mfe if session_mfe>0 else 0.
        after=result['index']+1
        original_target=row.entry_price*(1+sign*float(meta.native_target_pct)/100)
        later=path['high'][after:] if sign==1 else path['low'][after:]
        target_later=bool(np.any(later>=original_target) if sign==1 else np.any(later<=original_target)) if len(later) else False
        premature=result['reason'] not in ('TARGET','TIME_EXIT_1515') and target_later
        before=float(row.portfolio_net_profit_rupees)
        changes.append(dict(policy=name,sid=int(row.sid),day=row.day,baseline_pnl_rs=before,
            candidate_pnl_rs=pnl,pnl_change_rs=pnl-before,exit_changed=not unchanged,
            baseline_winner_harmed=before>0 and pnl<before-1e-7,
            baseline_loser_improved=before<0 and pnl>before+1e-7,
            winner_turned_loss=before>0 and pnl<0,loser_turned_win=before<0 and pnl>0,
            initial_risk_pct=result['initial_risk_pct'],original_target_pct=float(meta.native_target_pct),
            target_mode=result['target_mode'],activation_count=result['activation_count'],
            stop_update_count=result['stop_update_count'],exit_reason=result['reason'],
            same_bar_ambiguous=result['ambiguous'],candidate_exit_ts=str(stamp),
            session_mfe_pct=session_mfe,session_mfe_capture_ratio=capture,
            premature_exit_then_target=premature))
        previous=-result['initial_risk_pct']
        for update in result['stop_updates']:
            if update['effective_index']!=update['observed_index']+1 or update['stop_return_pct']<=previous:
                raise ValueError('Noncausal or loosening trailing update')
            previous=update['stop_return_pct']
            updates.append(dict(policy=name,sid=int(row.sid),day=row.day,
                observed_bar_end=str(pd.Timestamp(int(path['timestamp_ns'][update['observed_index']]),tz='UTC').tz_convert('Asia/Kolkata')),
                effective_bar_end=str(pd.Timestamp(int(path['timestamp_ns'][update['effective_index']]),tz='UTC').tz_convert('Asia/Kolkata')),
                stop_price=row.entry_price*(1+sign*update['stop_return_pct']/100),**update))
    assert_entries(frozen,book)
    return book,pd.DataFrame(changes),pd.DataFrame(parity),pd.DataFrame(updates)


def stress(books,changes,periods):
    rows=[]
    for name,book in books.items():
        changed=changes.loc[changes.policy.eq(name)].set_index('sid').exit_changed
        for bps in (0,1,2,5):
            adjusted=book.copy();extra=adjusted.sid.map(changed).astype(float)*500000*bps/10000
            adjusted['portfolio_net_profit_rupees']-=extra
            for period in ('full_66','fit_44','validation_15','later_7'):
                values=metric(adjusted,0,periods[period])
                rows.append(dict(policy=name,period=period,incremental_slippage_bps=bps,
                    extra_slippage_rs=float(extra.loc[book.day.isin(periods[period])].sum()),
                    **{key:values[key] for key in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs')}))
    return pd.DataFrame(rows)


def run(output=OUTPUT):
    output=Path(output).resolve()
    if output.exists() or output==PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh output folder inside target_trailing_20261106')
    protocol=json.loads((PACKAGE/'protocol.json').read_text(encoding='utf-8'))
    if set(protocol['definitions'])!=set(engine.CANDIDATES) or len(engine.CANDIDATES)!=7:
        raise ValueError('Frozen hypothesis inventory changed')
    source=data.load();days=source['days']
    periods=dict(full_66=days,fit_44=days[:44],validation_15=days[44:59],later_7=days[59:],nov05=[days[-1]])
    for month in sorted({d[:7] for d in days}):
        periods['month_'+month]=[d for d in days if d.startswith(month)]
    counts=source['audit'].loc[admission.flag(source['audit'].all_g7_allowed)].groupby('day').size().to_dict()
    prefix={}
    lookup=source['parity'].set_index('sid')
    for sid,path in source['paths'].items():
        native.verify_path(path,int(lookup.loc[sid,'entry_index']))
        prefix[sid]=engine.build_features(path)
    baseline,changes,parity,updates=replay_book(source,prefix,BASELINE,engine.CANDIDATES[BASELINE])
    if len(parity)!=790 or sum(counts.values())!=891:
        raise ValueError('Baseline admission/fill inventory changed')
    output.mkdir(parents=True)
    (output/'predeclared_plan.json').write_text(json.dumps(dict(protocol=protocol,candidates=engine.CANDIDATES,
        engine_sha256=original.sha(engine.__file__)),indent=2)+'\n')
    print('PASS:790 fixed1.50% baseline fills/exits reproduced exactly.',flush=True)
    books={BASELINE:baseline};differences=[changes];all_updates=[updates]
    for name,settings in engine.CANDIDATES.items():
        if name==BASELINE:continue
        book,change,_,update=replay_book(source,prefix,name,settings)
        books[name]=book;differences.append(change);all_updates.append(update)
        print('Completed frozen trailing policy: '+name,flush=True)
    changes=pd.concat(differences,ignore_index=True)
    updates=pd.concat(all_updates,ignore_index=True)
    overall=[];daily=[]
    for name,book in books.items():
        for period,cohort in periods.items():
            overall.append(dict(policy=name,period=period,**metric(book,sum(counts.get(d,0) for d in cohort),cohort)))
        for day in days:
            daily.append(dict(policy=name,day=day,**metric(book,counts.get(day,0),[day])))
    overall,daily=pd.DataFrame(overall),pd.DataFrame(daily)
    for name in books:
        total=overall.loc[overall.policy.eq(name)&overall.period.eq('full_66')].iloc[0]
        part=daily.loc[daily.policy.eq(name)]
        for key in ('selected_orders','trades','wins','losses','net_pnl_rs','gross_loss_rs','costs_rs'):
            if not np.isclose(total[key],part[key].sum(),atol=1e-5,rtol=0):
                raise ValueError('Daily/full reconciliation failed: '+name+'/'+key)
    candidate,diagnostic=choose(overall)
    passed,reasons=qualification(overall,candidate)
    selected=candidate if passed else BASELINE
    config=dict(version='G3-V23-TARGET-TRAILING-STUDY-20261106',scope='BACKTEST_ONLY',execution_authority=False,
        live_changed=False,user_authorized_baseline_change=False,current_policy_unchanged=True,baseline_policy=BASELINE,
        baseline_initial_stop_pct=1.5,baseline_timed_tightening=False,baseline_native_target=True,
        selected_policy=selected,selected_exit_policy=selected,selected_sizing_policy='baseline_fixed_size',
        training_candidate=candidate,comparison_candidate=diagnostic,promotion_pass=passed,rejection_reasons=reasons,
        policy_settings=engine.CANDIDATES,definitions=protocol['definitions'],historydays=days,periods=periods,
        fit_cutoff=days[43],validation_cutoff=days[58],history_through=days[-1],selected_orders=891,filled_trades=790,
        entry_veto_enabled=False,notional_per_trade_rs=500000,cost_per_trade_rs=250,total_cost_rs=197500,
        unlimited_concurrent_capital=True,retrospective_only=True,no_later_period_reselection=True,
        coverage_notes=source['baseline_config']['coverage_notes'],
        indicator_definition=protocol['indicator_scope'],
        mfe_definition='Positive gross realized price return divided by the full-session favorable-excursion upper bound from the entry bar through15:15. Entire entry-bar extremes are included and may precede entry. This common ex-post OHLC bound is never an entry or exit signal.',
        premature_exit_diagnostic='Original target touched in a bar strictly after candidate exit; this does not prove a feasible avoided-stop profit.',
        untested_families='Only six early target-relative rules tested; no target removal, partial exits, new entry gates or adaptive initial stops.')
    overall.to_csv(output/'overall.csv',index=False);daily.to_csv(output/'daily.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False);updates.to_csv(output/'stop_updates.csv',index=False)
    parity.to_csv(output/'baseline_parity.csv',index=False)
    stress(books,changes,periods).to_csv(output/'cost_stress.csv',index=False)
    for name,book in books.items():book.to_csv(output/(name+'_trades.csv'),index=False)
    books[BASELINE].to_csv(output/'baseline_trades.csv',index=False)
    books[selected].to_csv(output/'selected_trades.csv',index=False)
    source['entry_features'].to_csv(output/'entry_features.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    write_reports(output,overall,daily,changes,config)
    (output/'validation.json').write_text(json.dumps(dict(new_baseline_parity=790,
        all_candidate_entries=790,all_candidate_days=66,source_baseline_net_pnl_rs=1220580.5488569569,
        same_costs_rs=197500,stop_updates_causal_and_ratchet=True,
        exact_native_target_control=True),indent=2)+'\n')
    from g3_v23_adx_rsi_20261106.loss_control_20261106 import reporting as helpers
    from g3_v23_adx_rsi_20261106.trailing_20261106 import engine as prior_engine, reporting as prior_reporting
    code=[Path(__file__),PACKAGE/'protocol.json',Path(engine.__file__),Path(data.__file__),PACKAGE/'reporting.py',Path(helpers.__file__),Path(prior_engine.__file__),Path(prior_reporting.__file__)]
    (output/'provenance.json').write_text(json.dumps(dict(source_manifests=source['source_manifests'],
        code_sha256={str(p):original.sha(p) for p in code}),indent=2)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:original.sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(overall.loc[overall.period.isin(['full_66','fit_44','validation_15','later_7']),
        ['period','policy','trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('BASELINE='+BASELINE+' TRAINING='+candidate+' RESEARCH_SELECTED='+selected+' QUALIFIED='+str(passed),flush=True)
    return config


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    run(parser.parse_args().output)
