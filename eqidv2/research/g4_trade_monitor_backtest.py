"""Replay the fixed G3 one-minute monitor advisories against sealed G4 trades.

Research only. No strategy pointer, dashboard setting, paper order or live order is changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g3_trade_monitor_backtest as monitor
from research import g4_bounded_exit_backtest as bounded
from research import g4_complete_inventory_backtest as current
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base

VERSION = 'V13-V10-G-4-ONE-MINUTE-TRADE-MONITOR-RESEARCH-V1'
EXTRA_SNAPSHOT_MANIFEST = (base.G4_ROOT/'partial/2026-10-16/20261016T142742_partial/'
    'input_snapshots/737e71d96e92c7f5eb2b919a831cea2482d4ea5b7c52ec9b61743b95fb732f81/'
    'snapshot_manifest.json')
EXTRA_SNAPSHOT_SHA256 = 'a3d06f6e154e943035008d6a1d44b5a2cc5b4611a7629e1de13c21fc8e9607a4'


class CurrentSealedPrices(sealed.SealedPrices):
    """Resolve later G4 price hashes to byte-identical captured inputs."""

    def __init__(self, summary, original_proof):
        super().__init__(summary, original_proof)
        if base.sha256(EXTRA_SNAPSHOT_MANIFEST)!=EXTRA_SNAPSHOT_SHA256:
            raise ValueError('Supplementary G4 price snapshot manifest changed')
        for member in base.read(EXTRA_SNAPSHOT_MANIFEST)['sources']:
            if member.get('captured') and member['role']=='EQUITY_ONE_MINUTE':
                self.by_digest.setdefault(member['sha256'],
                    EXTRA_SNAPSHOT_MANIFEST.parent/member['snapshot_relative_path'])


def historical_paths(original, summary, output):
    g3=Path(summary['current_g3_source_run'])
    if base.sha256(g3/'manifest.json')!=summary['current_g3_manifest_sha256']:
        raise ValueError('G3 source manifest changed')
    prices=CurrentSealedPrices(base.read(g3/'summary.json'),summary['price_sources'])
    paths={}
    for n,row in enumerate(original.sort_values(['tradingsymbol','day']).to_dict('records'),1):
        raw,_=prices.frame(pd.Timestamp(row['day']).date(),row['tradingsymbol'])
        confirmation=raw.loc[raw.ts.eq(pd.Timestamp(row['confirmation_ts']))]
        if len(confirmation)!=1 or any(not np.isclose(float(confirmation.iloc[0][key]),
            float(row['confirmation_'+key]),rtol=0,atol=.001) for key in ('open','high','low','close')):
            raise ValueError('Confirmation differs from pinned selected order')
        paths.update(prices.paths(pd.DataFrame([row])))
        if n%100==0:
            print(f'G4 minute paths: {n}/{len(original)}',flush=True)
    np.savez_compressed(output/'historical_paths.npz',**{
        f'{sid}_{key}':value for sid,path in paths.items() for key,value in path.items()})
    return paths,prices.resolution


def allocate(raw, policy):
    engine = bounded.history.simulator.g.v9
    settings = engine.V9Config(portfolio_capital_rupees=1_000_000.,
        capital_per_entry_rupees=100_000., leverage_factor=5., max_positions=10, cost_bps=5.)
    sized = engine.v5.apply_fixed_capital_model(raw, 100_000., 5.)
    result, _ = engine.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    result['day'] = result.day.astype(str).str[:10]
    result['strategy_version'] = VERSION
    result['monitor_policy'] = policy
    result['scope'] = 'BACKTEST_ONLY'
    result['execution_authority'] = False
    return result


def apply_policies(native, paths):
    first, observations = {}, []
    for n, row in enumerate(native.itertuples(index=False), 1):
        found, seen = monitor.trade_advisories(row, paths[int(row.sid)])
        first[int(row.sid)] = found
        observations.extend(seen)
        if n % 100 == 0:
            print(f'Monitor decisions: {n}/{len(native)}', flush=True)
    books = {}
    for policy, wanted in monitor.POLICIES.items():
        raw = native.copy()
        raw['monitor_exit'] = False
        raw['monitor_flags'] = '[]'
        raw['monitor_decision_at_ist'] = ''
        raw['monitor_open_observed_at_ist'] = ''
        for index, row in raw.iterrows():
            candidates = [event for flag, event in first[int(row.sid)].items() if flag in wanted]
            if candidates:
                event = min(candidates, key=lambda item: (item['execution_path_index'], item['flags']))
                monitor._apply_monitor_exit(raw, index, event, paths[int(row.sid)], policy)
                # G4 uses 1.20% initial stop, tightening to 1.00% after 120 minutes.
                open_at = pd.Timestamp(event['next_bar_open_at_ist'])
                entry_at = pd.Timestamp(row.entry_ts)
                raw.loc[index, 'active_stop_pct_at_exit'] = (1. if open_at >= entry_at + pd.Timedelta(minutes=120) else 1.2)
        books[policy] = allocate(raw, policy)
    sealed.parity(books['baseline'], native)
    return books, pd.DataFrame(observations)


def paired_changes(baseline, policy, name):
    cols = ['sid','day','setup_id','tradingsymbol','side','portfolio_executed',
        'exit_reason','exit_ts','portfolio_net_profit_rupees']
    result = baseline[cols].merge(policy[cols+['monitor_exit','monitor_decision_at_ist','monitor_flags']],
        on='sid', suffixes=('_baseline','_policy'), validate='one_to_one')
    result.insert(0, 'policy', name)
    result['delta_net_rs'] = result.portfolio_net_profit_rupees_policy-result.portfolio_net_profit_rupees_baseline
    both = result.portfolio_executed_baseline & result.portfolio_executed_policy
    result['baseline_stop_avoided'] = both & result.exit_reason_baseline.isin(['STOP','TIGHTENED_STOP']) & result.monitor_exit
    result['stop_loss_saved_rs'] = np.where(result.baseline_stop_avoided, result.delta_net_rs.clip(lower=0), 0.)
    result['baseline_winner_turned_loss'] = both & result.portfolio_net_profit_rupees_baseline.gt(0) & result.portfolio_net_profit_rupees_policy.lt(0)
    result['newly_allocated'] = ~result.portfolio_executed_baseline & result.portfolio_executed_policy
    result['newly_rejected'] = result.portfolio_executed_baseline & ~result.portfolio_executed_policy
    return result


def compare(books, days):
    periods = {'all':days, 'development35':days[:35], 'evaluation15':days[35:50],
        'later_extensions':days[50:], 'last_day':[days[-1]]}
    baseline = books['baseline']
    changes = {name:paired_changes(baseline, book, name) for name,book in books.items()}
    metrics, daily = [], []
    for period, cohort in periods.items():
        if not cohort:
            continue
        base_stats = bounded.stats(baseline,cohort)
        base_daily = baseline.loc[baseline.portfolio_executed & baseline.day.isin(cohort)].groupby('day').portfolio_net_profit_rupees.sum().reindex(cohort,fill_value=0.)
        for name, book in books.items():
            stats = bounded.stats(book,cohort)
            comp = changes[name].loc[changes[name].day_baseline.isin(cohort)]
            day_pnl = book.loc[book.portfolio_executed & book.day.isin(cohort)].groupby('day').portfolio_net_profit_rupees.sum().reindex(cohort,fill_value=0.)
            metrics.append(dict(period=period, policy=name, first_date=cohort[0], last_date=cohort[-1],
                **stats, net_change_rs=stats['net_pnl_rs']-base_stats['net_pnl_rs'],
                losing_days_saved=int((base_daily.lt(-1e-8)&~day_pnl.lt(-1e-8)).sum()),
                new_losing_days=int((~base_daily.lt(-1e-8)&day_pnl.lt(-1e-8)).sum()),
                reduced_loss_days=int((base_daily.lt(-1e-8)&day_pnl.lt(-1e-8)&day_pnl.gt(base_daily)).sum()),
                increased_loss_days=int((base_daily.lt(-1e-8)&day_pnl.lt(-1e-8)&day_pnl.lt(base_daily)).sum()),
                monitor_exits=int((comp.monitor_exit & comp.portfolio_executed_policy).sum()),
                baseline_stops_avoided=int(comp.baseline_stop_avoided.sum()),
                gross_stop_loss_saved_rs=float(comp.stop_loss_saved_rs.sum()),
                baseline_winners_turned_losses=int(comp.baseline_winner_turned_loss.sum()),
                newly_allocated=int(comp.newly_allocated.sum()), newly_rejected=int(comp.newly_rejected.sum())))
    for day in days:
        base_pnl = bounded.stats(baseline,[day])['net_pnl_rs']
        for name, book in books.items():
            stats = bounded.stats(book,[day])
            daily.append(dict(day=day,policy=name,**stats,delta_net_rs=stats['net_pnl_rs']-base_pnl))
    return pd.DataFrame(metrics),pd.DataFrame(daily),pd.concat(changes.values(),ignore_index=True)


def run(output, source=None, source_sha256=None):
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/'research').resolve()):
        raise ValueError('Fresh isolated G4 research directory required')
    pointer=base.read(base.G4_ROOT/'latest/bounded_exit.json')
    source=Path(source).resolve() if source else (base.G4_ROOT/pointer['run_dir']).resolve()
    expected=source_sha256 or (pointer['manifest_sha256'] if source==(base.G4_ROOT/pointer['run_dir']).resolve() else None)
    if not expected or not source.is_relative_to(base.G4_ROOT.resolve()) or base.sha256(source/'manifest.json')!=expected:
        raise ValueError('Supply a verified G4 source manifest hash')
    source_summary=sealed.package(source)
    if source_summary['variant_id']!=current.VERSION or source_summary['active_setups']!=51 or source_summary['sessions']<50:
        raise ValueError('Expected current complete 51-setup G4 backtest')
    config=monitor.configuration()
    original=base.read_frozen_csv(source/'g4_bounded_exit/portfolio_trades.csv')
    days=list(source_summary['days'])
    output.mkdir(parents=True)
    paths, price_proof=historical_paths(original,source_summary,output)
    native=sealed.replay(original,paths,'current_g4')
    sealed.parity(native,original)
    print('G4 native trade and allocation control reproduced exactly',flush=True)
    books,signals=apply_policies(native,paths)
    metrics,daily,changes=compare(books,days)
    for name,book in books.items():
        folder=output/name
        folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv',index=False)
    metrics.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    signals.to_csv(output/'signals.csv',index=False)
    configuration=dict(config,variant_id=VERSION,g4_source_variant_id=current.VERSION,
        g4_initial_stop_pct=1.2,g4_tightened_stop_pct=1.,g4_tighten_after_minutes=120,
        target_rule='G4 original 0.60%-2.00% bounded targets',
        native_control_exactly_reproduced=True)
    summary=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        source_run=str(source),source_manifest_sha256=expected,source_variant_id=current.VERSION,
        first_date=days[0],last_date=days[-1],days=days,sessions=len(days),active_setups=51,
        source_orders=len(original),historical_bar_resolution_seconds=60,dashboard_refresh_seconds=15,
        native_control_exactly_reproduced=True,price_sources=price_proof,
        policy_totals=metrics.loc[metrics.period.eq('all')].to_dict('records'),
        limitations=['15 seconds is the dashboard refresh interval; historical candles are one minute.',
            'No synthetic 15-second fill or intraminute path is used.',
            'The source G4 strategy was developed on historical data; the 35/15 split is not an untouched strategy holdout.',
            'Advisory exits use the next observed minute open with capital reserved through that bar end.',
            'Five basis points round-trip cost; extra spread and broker latency are not modeled.',
            'Historical results do not establish future performance.'])
    (output/'config.json').write_text(json.dumps(base.source._json_ready(configuration),indent=2)+'\n',encoding='utf-8')
    (output/'summary.json').write_text(json.dumps(base.source._json_ready(summary),indent=2)+'\n',encoding='utf-8')
    show=['period','policy','trades','losses','losing_days','negative_trade_sum_abs_rs','net_pnl_rs',
        'net_change_rs','losing_days_saved','new_losing_days','reduced_loss_days','monitor_exits',
        'baseline_stops_avoided','gross_stop_loss_saved_rs','baseline_winners_turned_losses',
        'stop_exits','daily_close_drawdown_rs']
    report='<!doctype html><html><head><meta charset="utf-8"><title>G4 one-minute trade monitor replay</title><style>body{font:15px/1.5 system-ui;margin:30px;background:#101927;color:#edf4ff}a{color:#8ed6ff}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:8px;border-bottom:1px solid #40526b;white-space:nowrap;text-align:left}th{background:#243449}.scroll{overflow:auto}</style></head><body><h1>G4 one-minute trade monitor replay</h1>'
    report+=f'<p>{days[0]} to {days[-1]} · {len(days)} completed sessions · 51 setups. Dashboard refresh: 15 seconds. Price evidence: completed one-minute candles.</p>'
    report+='<p>Trend failure and profit giveback are the existing fixed G3 monitor rules. Advisory decisions occur on completed candles and replay at the next observed candle open. Native G4 entries, bounded targets, stops, cost and portfolio sizing are preserved.</p>'
    report+='<h2>Policy comparison</h2><div class="scroll">'+metrics[show].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    report+='<h2>Daywise comparison</h2><div class="scroll">'+daily[['day','policy','trades','losses','losing_days','net_pnl_rs','delta_net_rs']].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    report+='<h2>Limitations</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])+'</ul>'
    report+='<p><a href="comparison.csv">Policy results</a> · <a href="daily_comparison.csv">Daily results</a> · <a href="trade_changes.csv">Trade changes</a> · <a href="signals.csv">Monitor decisions</a></p></body></html>'
    (output/'report.html').write_text(report,encoding='utf-8')
    if base.sha256(source/'manifest.json')!=expected:
        raise ValueError('G4 source manifest changed during replay')
    sealed.package(source)
    for proof in price_proof.values():
        if base.sha256(Path(proof['resolved_path']))!=proof['sha256']:
            raise ValueError('Price evidence changed during replay')
    for name,book in books.items():
        saved=base.read_frozen_csv(output/name/'portfolio_trades.csv')
        actual=bounded.stats(saved,days)
        recorded=metrics.loc[(metrics.period=='all')&(metrics.policy==name)].iloc[0]
        for key,value in actual.items():
            if not np.isclose(value,recorded[key],atol=1e-6,rtol=0):
                raise ValueError('Saved ledger statistic mismatch: '+key)
    manifest=dict(variant_id=VERSION,source_manifest_sha256=expected,execution_authority=False,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    return output,metrics


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-dir',type=Path)
    p.add_argument('--source-run',type=Path)
    p.add_argument('--source-manifest-sha256')
    a=p.parse_args()
    if bool(a.source_run)!=bool(a.source_manifest_sha256):
        p.error('--source-run and --source-manifest-sha256 must be supplied together')
    now=datetime.now(base.production.IST)
    output=a.output_dir or base.G4_ROOT/'research'/f'{now:%Y%m%dT%H%M%S}_one_minute_trade_monitor_v1'
    directory,metrics=run(output,a.source_run,a.source_manifest_sha256)
    print(metrics.loc[metrics.period.eq('all')].to_string(index=False),flush=True)
    print(directory,flush=True)


if __name__=='__main__':
    main()
