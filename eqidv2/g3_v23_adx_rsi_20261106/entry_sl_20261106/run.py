"""Fit entry-relative stops on44 sessions and evaluate the fixed66-day history."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import policy as entry_policy, run as original
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study import run as native
from g3_v23_adx_rsi_20261106.loss_control_20261106 import inputs
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from . import calibration, engine
from .reporting import write_reports

PACKAGE=Path(__file__).resolve().parent
SOURCE=PACKAGE/'analysis_source'
OUTPUT=PACKAGE/'results_through_20261105'
BASELINE='baseline_v21_g7'


def choose(overall):
    fit=overall.loc[overall.period.eq('fit_44')]
    base=fit.loc[fit.policy.eq(BASELINE)].iloc[0]
    eligible=fit.loc[fit.policy.ne(BASELINE)&fit.trades.eq(base.trades)
        &fit.net_pnl_rs.ge(base.net_pnl_rs)&fit.gross_loss_rs.lt(base.gross_loss_rs)
        &fit.daily_close_drawdown_rs.le(base.daily_close_drawdown_rs)]
    order=['net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs','policy']
    ascending=[False,True,True,True]
    candidate=BASELINE if eligible.empty else eligible.sort_values(order,ascending=ascending).iloc[0].policy
    exploratory=fit.loc[fit.policy.ne(BASELINE)&fit.gross_loss_rs.lt(base.gross_loss_rs)]
    diagnostic=(candidate if candidate!=BASELINE else BASELINE if exploratory.empty else
                exploratory.sort_values(order,ascending=ascending).iloc[0].policy)
    return candidate,diagnostic


def promotion(overall,candidate):
    if candidate==BASELINE:
        return False,['No fit44 model reduced losses while preserving baseline net profit and drawdown.']
    reasons=[]
    for label in ('validation_15','later_7','full_66'):
        part=overall.loc[overall.period.eq(label)].set_index('policy')
        before,after=part.loc[BASELINE],part.loc[candidate]
        if after.trades!=before.trades:
            reasons.append(label+': changed entry count')
        if after.net_pnl_rs<before.net_pnl_rs:
            reasons.append(label+': net profit below baseline')
        if after.gross_loss_rs>before.gross_loss_rs or (label=='full_66' and after.gross_loss_rs==before.gross_loss_rs):
            reasons.append(label+': loss reduction requirement failed')
        if after.daily_close_drawdown_rs>before.daily_close_drawdown_rs:
            reasons.append(label+': daily-close drawdown increased')
    return not reasons,reasons


def metric(book,selected,days):
    values=original.metrics(book,selected,days)
    values['breakeven_trades']=values['trades']-values['wins']-values['losses']
    return values


def replay_book(data,distances,policy):
    frozen=data['trades']; lookup=data['parity'].set_index('sid')
    stop_lookup=distances.set_index('sid')[policy]
    book=frozen.copy(); changes=[]; parity=[]
    for idx,row in frozen.iterrows():
        meta=lookup.loc[row.sid]; path=data['paths'][int(row.sid)]
        native.verify_path(path,int(meta.entry_index))
        distance=float(stop_lookup.loc[row.sid])
        result=engine.replay(path,int(meta.entry_index),float(row.entry_price),row.side=='LONG',
                             float(meta.native_target_pct),initial_stop_pct=distance)
        stamp=native.exit_timestamp(path,result)
        if result['index']>int(meta.exit_index):
            raise ValueError('Tighter initial stop delayed native exit')
        unchanged=(result['index']==int(meta.exit_index) and stamp==pd.Timestamp(row.exit_ts)
                   and abs(result['price']-row.exit_price)<1e-8)
        pnl=float(row.portfolio_net_profit_rupees) if unchanged else (
            (1 if row.side=='LONG' else -1)*(result['price']/row.entry_price-1)*500000-250)
        if abs(pnl)<1e-7:
            pnl=0.
        if policy==BASELINE:
            derived=(1 if row.side=='LONG' else -1)*(result['price']/row.entry_price-1)*500000-250
            if not unchanged or result['reason']!=row.exit_reason or not np.isclose(derived,row.portfolio_net_profit_rupees,atol=1e-5,rtol=0):
                raise ValueError('Native790 baseline parity failed: '+str(row.sid))
            parity.append(dict(sid=int(row.sid),day=row.day,native_parity=True))
        book.loc[idx,'exit_ts']=str(stamp)
        book.loc[idx,'exit_price']=result['price']
        book.loc[idx,'exit_reason']=result['reason']
        book.loc[idx,'portfolio_net_profit_rupees']=pnl
        before=float(row.portfolio_net_profit_rupees)
        changes.append(dict(policy=policy,sid=int(row.sid),day=row.day,
            baseline_pnl_rs=before,candidate_pnl_rs=pnl,pnl_change_rs=pnl-before,exit_changed=not unchanged,
            baseline_winner_harmed=before>0 and pnl<before-1e-7,baseline_loser_improved=before<0 and pnl>before+1e-7,
            winner_turned_loss=before>0 and pnl<0,loser_turned_win=before<0 and pnl>0,
            initial_stop_pct=distance,exit_reason=result['reason'],candidate_exit_ts=str(stamp),
            same_bar_ambiguous=result['ambiguous'],entry_bar_exit=result['index']==int(meta.entry_index)))
    assert_entries(frozen,book)
    return book,pd.DataFrame(changes),pd.DataFrame(parity)


def stress(books,changes,periods):
    rows=[]
    for name,book in books.items():
        changed=changes.loc[changes.policy.eq(name)].set_index('sid').exit_changed
        for bps in (0,1,2,5):
            adjusted=book.copy()
            extra=adjusted.sid.map(changed).astype(float)*500000*bps/10000
            adjusted['portfolio_net_profit_rupees']-=extra
            for label in ('full_66','fit_44','validation_15','later_7'):
                values=metric(adjusted,0,periods[label])
                rows.append(dict(policy=name,period=label,incremental_slippage_bps=bps,
                    extra_slippage_rs=float(extra.loc[book.day.isin(periods[label])].sum()),
                    **{key:values[key] for key in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs')}))
    return pd.DataFrame(rows)


def run(output=OUTPUT):
    output=Path(output).resolve()
    if output.exists() or output==PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh entry-SL result directory')
    verify_manifest(SOURCE)
    for name,expected in json.loads((SOURCE/'provenance.json').read_text()).get('code_sha256',{}).items():
        if original.sha(name)!=expected:
            raise ValueError('Entry feature study code changed: '+name)
    protocol=json.loads((PACKAGE/'protocol.json').read_text())
    if set(protocol['definitions'])!=set(calibration.POLICIES):
        raise ValueError('Declared model inventory changed')
    data=inputs.load(); days=data['days']; frozen=data['trades']
    features=pd.read_csv(SOURCE/'entry_features.csv')
    assert_entries(frozen,features)
    if not features.sid.is_unique or set(features.sid)!=set(frozen.sid):
        raise ValueError('Missing entry features')
    excursions=pd.read_csv(SOURCE/'training_excursions.csv')
    if set(excursions.day)-set(days[:44]) or len(excursions)!=int(frozen.day.isin(days[:44]).sum()):
        raise ValueError('Training excursion inventory crossed calibration boundary')
    fit_frame=features.merge(excursions[['sid','mae_upper_pct','baseline_net_pnl_rs']],on='sid',how='left',validate='one_to_one')
    fitparams=calibration.fit(fit_frame,days[:44])
    distances=calibration.distances(features,fitparams)
    periods=dict(full_66=days,fit_44=days[:44],validation_15=days[44:59],later_7=days[59:],nov05=[days[-1]])
    for month in sorted({day[:7] for day in days}):
        periods['month_'+month]=[day for day in days if day.startswith(month)]
    counts=data['audit'].loc[entry_policy.flag(data['audit'].all_g7_allowed)].groupby('day').size().to_dict()
    baseline,baseline_changes,parity=replay_book(data,distances,BASELINE)
    if len(parity)!=790:
        raise ValueError('Missing native parity rows')
    output.mkdir(parents=True)
    # Persist the frozen fit and plan before evaluating any nonbaseline stop.
    (output/'fit.json').write_text(json.dumps(fitparams,indent=2)+'\n')
    (output/'predeclared_plan.json').write_text(json.dumps(dict(protocol=protocol,
        fit_sha256=original.sha(output/'fit.json'),engine_sha256=original.sha(engine.__file__),
        calibration_sha256=original.sha(calibration.__file__)),indent=2)+'\n')
    print('PASS:790 native baseline exits; fixed calibration uses first44 sessions only.',flush=True)
    books={BASELINE:baseline}; differences=[baseline_changes]
    for name in calibration.POLICIES:
        if name==BASELINE:
            continue
        book,change,_=replay_book(data,distances,name)
        books[name]=book; differences.append(change)
        print('Completed frozen entry-SL model: '+name,flush=True)
    changes=pd.concat(differences,ignore_index=True)
    overall=[]; daily=[]; levels=[]
    for name,book in books.items():
        for label,cohort in periods.items():
            overall.append(dict(policy=name,period=label,**metric(book,sum(counts.get(d,0) for d in cohort),cohort)))
        for day in days:
            daily.append(dict(policy=name,day=day,**metric(book,counts.get(day,0),[day])))
        level=features[['sid','day','tradingsymbol','side','entry_ts','entry_price','confirmation_ts','feature_available_at','atr14_pct']].copy()
        level['policy']=name
        level['initial_stop_pct']=distances[name].to_numpy()
        sign=np.where(level.side.eq('LONG'),1.,-1.)
        level['initial_stop_price']=level.entry_price*(1-sign*level.initial_stop_pct/100)
        level['modelled_loss_at_stop_rs']=500000*level.initial_stop_pct/100+250
        level['stop_pct_after_120m']=np.minimum(level.initial_stop_pct,1.)
        levels.append(level)
    overall,daily=pd.DataFrame(overall),pd.DataFrame(daily)
    for name in books:
        total=overall.loc[overall.policy.eq(name)&overall.period.eq('full_66')].iloc[0]
        part=daily.loc[daily.policy.eq(name)]
        for key in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','selected_orders','costs_rs'):
            if not np.isclose(total[key],part[key].sum(),atol=1e-5,rtol=0):
                raise ValueError('Daily/full reconciliation failed: '+name+'/'+key)
    candidate,diagnostic=choose(overall)
    passed,reasons=promotion(overall,candidate)
    selected=candidate if passed else BASELINE
    definitions=dict(protocol['definitions'])
    for name,value in fitparams['fixed_stop_pct'].items():
        definitions[name]+=f' Fitted initial stop: {value:.2f}% of actual entry.'
    definitions['atr_q95']+=f' Fitted multiple: {fitparams["atr_multiplier"]:.2f}x ATR.'
    bins=fitparams['volatility_bins']
    definitions['volatility_bin_q95']+=(f' Fitted ATR split: {fitparams["volatility_split_atr_pct"]:.6f}% of entry; '
        f'low stop {bins["low"]["stop_pct"]:.2f}%, high stop {bins["high"]["stop_pct"]:.2f}%.')
    definitions['structure_q95']+=f' Fitted floor: {fitparams["structure_floor_pct"]:.2f}%.'
    config=dict(version='G3-V23-ENTRY-RELATIVE-SL-20261106',scope='BACKTEST_ONLY',execution_authority=False,
        live_changed=False,selected_policy=selected,training_candidate=candidate,comparison_candidate=diagnostic,
        promotion_pass=passed,rejection_reasons=reasons,definitions=definitions,historydays=days,
        training_cutoff=days[43],validation_cutoff=days[58],periods=periods,
        selected_orders=891,filled_trades=790,entry_veto_enabled=False,
        selected_sizing_policy='baseline_fixed_size',notional_per_trade_rs=500000,cost_per_trade_rs=250,
        unlimited_concurrent_capital=True,coverage_notes=data['baseline_config']['coverage_notes'],
        no_validation_outcome_fit=True,no_later_period_reselection=True,retrospective_only=True,
        formulas={'LONG':'entry_price * (1 - initial_stop_pct / 100)','SHORT':'entry_price * (1 + initial_stop_pct / 100)'},
        unchanged_native_targets=True,all_entry_identities_prices_times_preserved=True,
        entry_bar_convention='Inherited conservative full-bar stop-first; entry-bar open cannot determine a stop fill',
        cost_stress_diagnostic_only=True)
    overall.to_csv(output/'overall.csv',index=False); daily.to_csv(output/'daily.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    parity.to_csv(output/'baseline_parity.csv',index=False)
    pd.concat(levels,ignore_index=True).to_csv(output/'stop_levels.csv',index=False)
    stress(books,changes,periods).to_csv(output/'cost_stress.csv',index=False)
    for name,book in books.items():
        book.to_csv(output/(name+'_trades.csv'),index=False)
    books[selected].to_csv(output/'selected_trades.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    for name in ('training_quantiles.csv','training_excursions.csv','entry_features.csv','coverage.json'):
        shutil.copyfile(SOURCE/name,output/name)
    shutil.copyfile(SOURCE/'STUDY.md',output/'mae_study.md')
    write_reports(output,overall,daily,changes,config,fitparams)
    (output/'validation.json').write_text(json.dumps(dict(native_parity_count=790,unchanged_entries=790,
        fit44_entries=fitparams['training_rows'],fit44_winners=fitparams['training_winners'],
        full_days=66,all_same_costs_rs=197500,all_daily_totals_reconcile=True),indent=2)+'\n')
    from g3_v23_adx_rsi_20261106.loss_control_20261106 import reporting as helper
    files=[Path(__file__),PACKAGE/'protocol.json',Path(calibration.__file__),Path(engine.__file__),
           PACKAGE/'reporting.py',Path(helper.__file__)]
    provenance=dict(code_sha256={str(p):original.sha(p) for p in files},
        source_manifests={**data['source_manifests'],str(SOURCE):original.sha(SOURCE/'manifest.json')})
    (output/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:original.sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(overall.loc[overall.period.isin(['full_66','fit_44','validation_15','later_7']),
        ['period','policy','trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('FIT_CANDIDATE='+candidate+' CURRENT='+selected+' PROMOTED='+str(passed),flush=True)
    return config


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    run(parser.parse_args().output)
