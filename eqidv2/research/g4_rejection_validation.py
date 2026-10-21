"""Fixed, causal rejection experiment on the ten G4 V8 guard setups.

Consumes the verified pre-quota V8 candidate pool, keeps G3/G4 defaults intact,
and freshly simulates replacement entries and portfolio constraints.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_g3_mirror_backtest as base

VERSION = 'V13-V10-G-4-REJECTION-VALIDATION-V1'
LOOKBACK = 10
REFERENCE = base.G4_ROOT/'runs/2026-10-14/20261014_g3_mirror_ten_guards_full_eod_v8'
REFERENCE_HASH = '0cc46c6361f06531336caa71851c3fddd32d5e2f5446df9eaf0f987d1f89be90'
SCENARIOS = ('g3_control', 'g4_current', 'rejection_only_ten', 'atr_plus_rejection_ten')


def rejection_features(raw, row):
    """Prior ten consecutive same-session bars, excluding confirmation itself."""
    stamp = pd.Timestamp(row.confirmation_ts)
    stamp = stamp.tz_localize(base.production.IST) if stamp.tzinfo is None else stamp.tz_convert(base.production.IST)
    prior = raw.loc[raw.ts.lt(stamp) & raw.ts.ge(stamp-pd.Timedelta(minutes=LOOKBACK))].copy()
    expected = pd.date_range(stamp-pd.Timedelta(minutes=LOOKBACK),periods=LOOKBACK,freq='min')
    valid = (len(prior)==LOOKBACK and not prior.ts.duplicated().any()
             and prior.ts.dt.date.eq(stamp.date()).all()
             and np.array_equal(prior.ts.astype('int64').to_numpy(),expected.asi8))
    current = raw.loc[raw.ts.eq(stamp)]
    if len(current)!=1:
        raise ValueError('No exact confirmation candle')
    for name in ('open','high','low','close'):
        if not np.isclose(float(current.iloc[0][name]),float(row['confirmation_'+name]),atol=.001,rtol=0):
            raise ValueError('Confirmation OHLC drift')
    window = pd.concat([prior,current])
    numeric = window[['open','high','low','close']].apply(pd.to_numeric,errors='coerce')
    valid = valid and np.isfinite(numeric.to_numpy()).all()
    for field in ('gap_filled','opening_snapshot','provisional_stale'):
        if field in window:
            values = window[field]
            flagged = pd.to_numeric(values,errors='coerce').astype('Float64').fillna(0).ne(0)
            flagged |= values.astype(str).str.strip().str.lower().isin(['true','yes','on'])
            valid = valid and not flagged.any()
    high = float(pd.to_numeric(prior.high).max()) if valid else np.nan
    low = float(pd.to_numeric(prior.low).min()) if valid else np.nan
    if row.side not in ('LONG','SHORT'):
        raise ValueError('Unknown side')
    rejected = bool(valid and (
        (row.confirmation_high>high and row.confirmation_close<high) if row.side=='LONG'
        else (row.confirmation_low<low and row.confirmation_close>low)))
    return dict(rejection_inputs_valid=bool(valid),prior_10_high=high,prior_10_low=low,
                prior_bar_count=len(prior),rejection_flag=rejected)


def select(pool, limits, reject):
    eligible = pool
    if reject:
        eligible = eligible.loc[eligible.rejection_inputs_valid.eq(True) & ~eligible.rejection_flag.eq(True)]
    return base.reselect(eligible, limits)


def metrics(ledger, days):
    result = base._stats(ledger,days)
    executed = ledger.loc[ledger.portfolio_executed.eq(True)]
    daily = base._daily(ledger,days)
    stops = int(executed.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum())
    result.update(stop_exits=stops,stop_rate_pct=100*stops/len(executed) if len(executed) else None,
        losing_days=int(daily.net_pnl.lt(0).sum()),average_daily_net_rs=float(daily.net_pnl.mean()),
        losing_trade_total_rs=float(executed.loc[executed.portfolio_net_profit_rupees.lt(0),'portfolio_net_profit_rupees'].sum()),
        winning_trade_total_rs=float(executed.loc[executed.portfolio_net_profit_rupees.gt(0),'portfolio_net_profit_rupees'].sum()))
    return result


def run(output):
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to(base.G4_ROOT.resolve()):
        raise ValueError('Requires a fresh G4 result directory')
    if base.sha256(REFERENCE/'manifest.json')!=REFERENCE_HASH:
        raise ValueError('Pinned G4 V8 reference changed')
    reference=base.verify(REFERENCE)
    cfg=base.configuration()
    source_code={**base.code_proof(base.DEFAULT_CONFIG),str(Path(__file__).resolve()):base.sha256(Path(__file__))}
    pointers={str(p):base.sha256(p) for p in (
        base.G4_ROOT/'latest/g3_mirror.json',base.G3_ROOT/'latest/current_backtest.json')}
    g3dir=Path(reference['baseline_g3_run'])
    if base.sha256(g3dir/'manifest.json')!=reference['baseline_g3_manifest_sha256']:
        raise ValueError('Pinned G3 reference changed')
    g3,_=base.verified_package(g3dir,base.G3_ROOT)
    days=[date.fromisoformat(d) for d in g3['days']]
    if str(days[-1])!='2026-10-14':
        raise ValueError('This declared experiment uses the 14 October cutoff')
    original=base.read_frozen_csv(g3dir/'selected_orders.csv')
    old_g3=base.read_frozen_csv(g3dir/'portfolio_trades.csv')
    old_g4=base.read_frozen_csv(REFERENCE/'guard_all_ten/portfolio_trades.csv')
    pool=base.read_frozen_csv(REFERENCE/'candidate_extension_audit.csv')
    limits=cfg['scenarios']['guard_all_ten']
    if set(pool.setup_id)!=set(limits):
        raise ValueError('Candidate scope must contain the ten predeclared setups')
    expected=original.loc[original.setup_id.isin(limits)]
    if base.selected_keys(base.reselect(pool,{}))!=base.selected_keys(expected):
        raise ValueError('Candidate selection does not reproduce G3')
    if base.selected_keys(base.reselect(pool,limits))!=base.selected_keys(old_g4.loc[old_g4.setup_id.isin(limits)]):
        raise ValueError('Candidate selection does not reproduce G4')
    prices=base.Prices(g3)
    feature_rows=[]
    for row in pool.itertuples(index=False):
        raw,key=prices.frame(pd.Timestamp(row.day).date(),row.tradingsymbol)
        if prices.proof[key]!=reference['price_sources'].get(key):
            raise ValueError('Candidate market input differs from V8 reference: '+key)
        feature_rows.append(rejection_features(raw,pd.Series(row._asdict())))
    pool=pd.concat([pool.reset_index(drop=True),pd.DataFrame(feature_rows)],axis=1)
    print(f'Verified {len(pool)} pre-quota candidates; {int(pool.rejection_flag.sum())} rejection flags; '
          f'{int((~pool.rejection_inputs_valid).sum())} invalid windows',flush=True)
    paths=prices.paths(original)
    ledgers={};orders_by={};payloads={};summary_rows=[];daily_frames=[];change_rows=[]
    setup_rows=[];period_rows=[]
    for name in SCENARIOS:
        guard=limits if name in ('g4_current','atr_plus_rejection_ten') else {}
        reject=name in ('rejection_only_ten','atr_plus_rejection_ten')
        selected=select(pool,guard,reject)
        orders=pd.concat([original.loc[~original.setup_id.isin(limits)],selected],ignore_index=True,sort=False)
        trial_cfg=copy.deepcopy(cfg)
        trial_cfg.update(variant_id=VERSION,scenarios={name:guard})
        identity=base.Identity(trial_cfg,name)
        identity.payload['rejection_guard']=dict(enabled=reject,lookback_bars=LOOKBACK,
            setups=sorted(limits) if reject else [],completed_confirmation_only=True,
            same_session_consecutive_minutes=True,unavailable='REJECT_CANDIDATE',
            long='high > prior_high AND close < prior_high',short='low < prior_low AND close > prior_low')
        identified=identity.identify(orders)
        if identified.sid.duplicated().any():
            raise ValueError('Duplicate candidate SID')
        paths.update(prices.paths(identified.loc[~identified.sid.isin(paths)]))
        ledger=base._simulate(identified,paths,days,identity)
        ledger['strategy']='V13-V10-G-4'
        shared=ledger.loc[ledger.sid.isin(old_g3.sid)]
        base._same_native(shared,old_g3.loc[old_g3.sid.isin(shared.sid)])
        if name in ('g3_control','g4_current'):
            pinned=old_g3 if name=='g3_control' else old_g4
            base._same_native(ledger,pinned);base._same_allocation(ledger,pinned)
        ledgers[name]=ledger;orders_by[name]=identified;payloads[name]=identity.payload
        stats=metrics(ledger,days)
        stats.update(scenario=name,net_delta_vs_current_g4_rs=stats['net_pnl_rs']-reference['scenarios'][
            next(i for i,x in enumerate(reference['scenarios']) if x['scenario']=='guard_all_ten')]['net_pnl_rs'])
        summary_rows.append(stats)
        daily=base._daily(ledger,days);daily['scenario']=name;daily_frames.append(daily)
        baseline=old_g4.loc[old_g4.portfolio_executed.eq(True)].set_index('sid')
        active=ledger.loc[ledger.portfolio_executed.eq(True)].set_index('sid')
        for action,frame in [('REMOVED_EXECUTED',baseline.loc[~baseline.index.isin(active.index)]),
                             ('ADDED_EXECUTED',active.loc[~active.index.isin(baseline.index)])]:
            for sid,row in frame.iterrows():
                change_rows.append(dict(scenario=name,action=action,sid=int(sid),day=str(row.day),
                    setup_id=row.setup_id,tradingsymbol=row.tradingsymbol,exit_reason=row.exit_reason,
                    net_pnl_rs=float(row.portfolio_net_profit_rupees)))
        for spec in base.donor.ACTIVE_SETUPS:
            setup_rows.append(dict(scenario=name,setup_id=spec.setup_id,signal_end=spec.signal_end,
                side=spec.side,**metrics(ledger.loc[ledger.setup_id.eq(spec.setup_id)],days)))
        periods={'EARLY_25':days[:25],'LATER_26':days[25:]}
        periods.update({month:[d for d in days if str(d).startswith(month)] for month in sorted({str(d)[:7] for d in days})})
        for label,period_days in periods.items():
            subset=ledger.loc[ledger.day.isin([str(d) for d in period_days])]
            period_rows.append(dict(scenario=name,period=label,**metrics(subset,period_days)))
        print(f"{name}: net {stats['net_pnl_rs']:.2f}; stops {stats['stop_exits']}; "
              f"losses {stats['losses']}; losing days {stats['losing_days']}",flush=True)
    for path,digest in {**source_code,**pointers}.items():
        if base.sha256(Path(path))!=digest:
            raise ValueError('Code or active pointer changed during experiment')
    for key,proof in prices.proof.items():
        if base.sha256(Path(key))!=proof['sha256']:
            raise ValueError('Market input changed during experiment')
    comparison=pd.DataFrame(summary_rows)
    changes=pd.DataFrame(change_rows)
    for name in SCENARIOS:
        removed=changes.loc[changes.scenario.eq(name)&changes.action.eq('REMOVED_EXECUTED')]
        added=changes.loc[changes.scenario.eq(name)&changes.action.eq('ADDED_EXECUTED')]
        delta=float(comparison.set_index('scenario').loc[name,'net_delta_vs_current_g4_rs'])
        if not np.isclose(added.net_pnl_rs.sum()-removed.net_pnl_rs.sum(),delta,rtol=0,atol=1e-6):
            raise ValueError('Changed-trade P&L does not reconcile')
    output.mkdir(parents=True)
    for name in SCENARIOS:
        folder=output/name;folder.mkdir()
        orders_by[name].to_csv(folder/'selected_orders.csv',index=False)
        ledgers[name].to_csv(folder/'portfolio_trades.csv',index=False)
        base._daily(ledgers[name],days).to_csv(folder/'daily_results.csv',index=False)
        (folder/'config.json').write_text(json.dumps(payloads[name],indent=2)+'\n',encoding='utf-8')
    comparison.to_csv(output/'comparison.csv',index=False)
    daily=pd.concat(daily_frames,ignore_index=True)
    reference_daily=daily.loc[daily.scenario.eq('g4_current')].set_index('day').net_pnl
    daily['current_g4_net_pnl']=daily.day.map(reference_daily)
    daily['delta_vs_current_g4']=daily.net_pnl-daily.current_g4_net_pnl
    daily.to_csv(output/'daily_comparison.csv',index=False)
    changes.to_csv(output/'changed_executed_trades.csv',index=False)
    pool.to_csv(output/'candidate_rejection_audit.csv',index=False)
    pd.DataFrame(setup_rows).to_csv(output/'setup_results.csv',index=False)
    pd.DataFrame(period_rows).to_csv(output/'period_comparison.csv',index=False)
    result=dict(version=VERSION,scope='BACKTEST_ONLY',execution_authority=False,first_date=str(days[0]),
        last_date=str(days[-1]),sessions=len(days),lookback_bars=LOOKBACK,setup_ids=sorted(limits),
        reference=str(REFERENCE),reference_manifest_sha256=REFERENCE_HASH,scenarios=summary_rows,
        candidates=len(pool),invalid_windows=int((~pool.rejection_inputs_valid).sum()),
        current_g3_g4_reproduced=True,active_pointers_unchanged=True,price_sources=prices.proof,
        limitations=reference['limitations']+[
            'Only the ten previously nominated guard setups receive this rejection experiment.',
            'The lookback was fixed at 10 before this run; no parameter sweep or automatic promotion.',
            'This entire period has already informed strategy selection; chronological splits are diagnostics, not untouched validation.',
            'Entry triggers may subsequently recover beyond a rejected confirmation candle; profitable trades rejected are reported.'])
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    columns=['scenario','executed_trades','wins','losses','win_rate_pct','stop_exits','stop_rate_pct',
             'losing_days','net_pnl_rs','net_delta_vs_current_g4_rs','max_drawdown_rs']
    page='''<!doctype html><html><head><meta charset="utf-8"><title>G4 rejection validation</title>
<style>body{font:16px/1.5 system-ui;background:#111827;color:#f3f4f6;margin:24px}a{color:#7dd3fc}table{border-collapse:collapse;font-size:14px}td,th{padding:8px;border-bottom:1px solid #475569;text-align:left}.scroll{overflow-x:auto}</style></head><body>
<h1>G4: fixed 10-bar rejection validation</h1>
<p>29 July through 14 October 2026. 51 sessions. October 1 excluded for incomplete inputs. G3 and current G4 controls reproduce the pinned results exactly.</p>
<p>Rejection-only replaces the ten additional ATR guards; both existing 10:35 G3 guards remain. Combined applies ATR plus rejection on those same ten setups. All trials reselect before quota and replay the full portfolio with unchanged targets, stops, costs and capital.</p>'''
    page+='<div class="scroll">'+comparison[columns].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    page+='<h2>Executed trades removed or added versus current G4</h2><div class="scroll">'+changes.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    page+='<h2>Daily comparison</h2><div class="scroll">'+daily.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    page+='<h2>Limits</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in result['limitations'])+'</ul>'
    page+='<p><a href="comparison.csv">Totals</a> | <a href="daily_comparison.csv">Daily results</a> | <a href="period_comparison.csv">Chronological diagnostics</a> | <a href="setup_results.csv">Setup results</a> | <a href="candidate_rejection_audit.csv">Candidate audit</a></p></body></html>'
    (output/'report.html').write_text(page,encoding='utf-8')
    manifest=dict(code_sha256=source_code,files={p.relative_to(output).as_posix():base.sha256(p)
        for p in sorted(output.rglob('*')) if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    for name in SCENARIOS:
        ledger=base.read_frozen_csv(output/name/'portfolio_trades.csv')
        checked=metrics(ledger,days)
        recorded=comparison.set_index('scenario').loc[name]
        for key,value in checked.items():
            if not np.isclose(value,recorded[key],atol=1e-6,rtol=0):
                raise ValueError('Saved accounting mismatch: '+key)
    print(comparison[columns].to_string(index=False),flush=True)
    print(output,flush=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    run(parser.parse_args().output_dir)
