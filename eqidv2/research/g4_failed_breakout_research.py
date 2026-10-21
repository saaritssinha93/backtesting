"""Fixed G4 post-fill invalidation experiment, using exactly pinned price bytes.

python -m research.g4_failed_breakout_research --partial-run <sealed partial run>
Writes isolated research evidence; never publishes strategy/result pointers.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import datetime
import html
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g4_bounded_exit_backtest as bounded
from research import g4_failed_breakout_exit as rule
from research import g4_g3_mirror_backtest as base
from research import g4_partial_day_backtest as partial
from research.g3_sl_target_simulator import simulate_exit

VERSION = 'V13-V10-G-4-FAILED-BREAKOUT-EXIT-RESEARCH-V1'
SCENARIOS = ('current_g4', 'short_failed_breakout', 'symmetric_failed_breakout')


def package(directory):
    """Verify saved artifacts independently of today's mutable source code."""
    directory = Path(directory).resolve()
    manifest = base.read(directory/'manifest.json')
    for relative, digest in manifest['files'].items():
        path = (directory/relative).resolve()
        if not path.is_relative_to(directory) or base.sha256(path) != digest:
            raise ValueError('Saved evidence changed: '+str(path))
    return base.read(directory/'summary.json')


class SealedPrices(base.Prices):
    """Resolve every old mutable file to an exact byte-identical sealed copy."""

    def __init__(self, summary, original_proof):
        super().__init__(summary)
        self.original_proof = original_proof
        self.by_digest = {}
        self.resolution = {}
        self.cache = OrderedDict()
        for item in summary['daily_path_evidence']:
            manifest = Path(item['snapshot_manifest'])
            if base.sha256(manifest) != item['snapshot_manifest_sha256']:
                raise ValueError('Sealed fallback manifest changed')
            for member in base.read(manifest)['sources']:
                if member.get('captured') and member['role'] == 'EQUITY_ONE_MINUTE':
                    self.by_digest.setdefault(member['sha256'], manifest.parent/member['snapshot_relative_path'])

    def frame(self, day, symbol):
        directory = self.snapshots.get(day)
        original = ((directory/'equity_1m') if directory else base.source.DataRoots().equity_1m)/f'{symbol}_stocks_indicators_1min.parquet'
        key = str(original.resolve())
        expected = self.original_proof.get(key, {}).get('sha256')
        if not expected:
            raise ValueError('Price file absent from original G4 evidence: '+key)
        if key not in self.cache:
            # Snapshot originals remain immutable; mutable originals always use
            # the pinned content-addressed fallback, even if their bytes match.
            path = original if directory else self.by_digest.get(expected)
            if path is None or base.sha256(path) != expected:
                raise ValueError('Exact original price bytes unavailable: '+key)
            available = set(pq.ParquetFile(path).schema_arrow.names)
            columns = ['date','open','high','low','close']
            columns += [x for x in ('gap_filled','opening_snapshot','provisional_stale') if x in available]
            frame = pd.read_parquet(path, columns=columns)
            if base.sha256(path) != expected:
                raise ValueError('Sealed price changed while reading')
            frame['ts'] = base.source.hybrid._to_ist(frame.date).dt.as_unit('ns')
            self.cache[key] = frame.sort_values('ts', kind='stable').reset_index(drop=True)
            self.resolution[key] = dict(original_sha256=expected, resolved_path=str(path.resolve()),
                sha256=expected, exact_original_bytes=True, sealed_fallback_used=not bool(directory))
            while len(self.cache) > 4:
                self.cache.popitem(last=False)
        else:
            self.cache.move_to_end(key)
        return self.cache[key], key


def historical_paths(original, summary, output):
    g3 = Path(summary['current_g3_source_run'])
    if base.sha256(g3/'manifest.json') != summary['current_g3_manifest_sha256']:
        raise ValueError('G3 source manifest changed')
    prices = SealedPrices(base.read(g3/'summary.json'), summary['price_sources'])
    paths = {}
    for n, row in enumerate(original.sort_values(['tradingsymbol','day']).to_dict('records'), 1):
        raw, _ = prices.frame(pd.Timestamp(row['day']).date(), row['tradingsymbol'])
        confirmation = raw.loc[raw.ts.eq(pd.Timestamp(row['confirmation_ts']))]
        if len(confirmation) != 1 or any(not np.isclose(float(confirmation.iloc[0][c]),
                float(row['confirmation_'+c]), rtol=0, atol=.001) for c in ('open','high','low','close')):
            raise ValueError('Exact confirmation differs from selected order')
        paths.update(prices.paths(pd.DataFrame([row])))
        if n % 100 == 0:
            print(f'Sealed historical paths: {n}/{len(original)}', flush=True)
    np.savez_compressed(output/'historical_paths.npz', **{
        f'{sid}_{key}': value for sid, path in paths.items() for key, value in path.items()})
    return paths, prices.resolution


class PartialPrices(partial.SnapshotPrices):
    def __init__(self, summary):
        manifest_path = Path(summary['snapshot_manifest'])
        if base.sha256(manifest_path) != summary['snapshot_manifest_sha256']:
            raise ValueError('Partial snapshot manifest changed')
        self.snapshot = manifest_path.parent
        manifest = base.read(manifest_path)
        self.expected = {str((self.snapshot/m['snapshot_relative_path']).resolve()):m['sha256']
                         for m in manifest['sources'] if m.get('captured')}
        self.day = pd.Timestamp(summary['session_date']).date()
        self.cutoff = pd.Timestamp(summary['as_of_ist'])
        self.proof = {}
        self.cache = {}

    def frame(self, day, symbol):
        if day != self.day:
            raise ValueError('Partial path escaped its session')
        path = self.snapshot/'equity_1m'/f'{symbol}_stocks_indicators_1min.parquet'
        key = str(path.resolve())
        if key not in self.cache:
            if base.sha256(path) != self.expected[key]:
                raise ValueError('Partial minute source changed')
            frame = pd.read_parquet(path, columns=['date','open','high','low','close'])
            frame['ts'] = base.source.hybrid._to_ist(frame.date).dt.as_unit('ns')
            self.cache[key] = frame.loc[frame.ts.dt.date.eq(day)&frame.ts.le(self.cutoff)].copy()
            self.proof[key] = dict(sha256=self.expected[key], resolved_path=key)
        return self.cache[key], key


def replay(original, paths, scenario, cutoff=None):
    """Recreate every native entry and exit, then recalculate the portfolio."""
    engine = bounded.history.simulator.g.v9
    records = []
    for source in original.to_dict('records'):
        row = source.copy()
        path = paths[int(row['sid'])]
        found = engine.v5._entry(SimpleNamespace(**row), path, delay_bars=0,
            trigger_buffer_pct=0., worse_fill_bps=0., max_entry_delay_minutes=10)
        row.update(filled=False, position_open=False, entry_ts=pd.NaT, entry_price=np.nan,
            entry_path_index=np.nan, exit_ts=pd.NaT, exit_price=np.nan, exit_path_index=None,
            gross_return_pct=np.nan, net_return_pct=np.nan, cost_pct=np.nan,
            failed_breakout_exit_applied=False, failed_breakout_audit_reason='UNFILLED',
            exit_reason=('UNFILLED_EXPIRED' if cutoff >= pd.Timestamp(row['confirmation_ts'])+pd.Timedelta(minutes=10)
                else 'PENDING_AT_CUTOFF') if cutoff is not None else 'UNFILLED')
        if found is not None:
            index, entry, trigger, gap, overshoot = found
            row.update(filled=True, entry_path_index=index, entry_price=entry, trigger_used=trigger,
                entry_gap_through=gap, entry_overshoot_bps=overshoot,
                entry_ts=pd.Timestamp(int(path['timestamp_ns'][index]),tz='UTC').tz_convert(base.production.IST))
            if scenario == 'current_g4':
                outcome = (partial.partial_exit(path,index,entry,row['side']=='LONG',float(row['native_target_pct']))
                    if cutoff is not None else simulate_exit(path,index,entry,row['side']=='LONG',1.2,
                    float(row['native_target_pct']),tighten_after_minutes=120,tightened_stop_pct=1.))
            else:
                outcome = rule.simulate_failed_breakout_exit(path,index,entry,row['side']=='LONG',
                    float(row['native_target_pct']),float(row['confirmation_high']),float(row['confirmation_low']),
                    partial=cutoff is not None, enable_long=scenario=='symmetric_failed_breakout')
            row.update(outcome)
            row.update(cost_pct=.05, net_return_pct=row['gross_return_pct']-.05)
        records.append(row)
    book = pd.DataFrame(records)
    settings = engine.V9Config(portfolio_capital_rupees=1000000., capital_per_entry_rupees=100000.,
        leverage_factor=5., max_positions=10, cost_bps=5.)
    if cutoff is not None:
        book['allocation_release_ts'] = pd.to_datetime(book.exit_ts,utc=True).dt.tz_convert(base.production.IST)
        book.loc[book.position_open.eq(True),'allocation_release_ts'] = cutoff+pd.Timedelta(minutes=1)
        book['exit_ts'] = book.allocation_release_ts
    sized = engine.v5.apply_fixed_capital_model(book,100000.,5.)
    ledger,_ = engine.v6.apply_portfolio_constraints(sized,settings.portfolio_config())
    if cutoff is not None:
        opened = ledger.position_open.eq(True)&ledger.portfolio_executed.eq(True)
        closed = ~ledger.position_open.eq(True)&ledger.portfolio_executed.eq(True)
        ledger['position_state'] = np.select([opened,closed,ledger.portfolio_status.eq('REJECTED')],
            ['OPEN','CLOSED','PORTFOLIO_REJECTED'],default=ledger.exit_reason)
        ledger['valuation_net_rs'] = ledger.portfolio_net_profit_rupees
        ledger['realized_net_rs'] = np.where(closed,ledger.valuation_net_rs,0.)
        ledger['open_mtm_net_after_reserved_cost_rs'] = np.where(opened,ledger.valuation_net_rs,0.)
        ledger.loc[ledger.position_open.eq(True),'exit_ts'] = pd.NaT
        ledger.loc[~closed,['portfolio_net_profit_rupees','portfolio_gross_profit_rupees','portfolio_cost_rupees']] = 0.
    ledger['day'] = ledger.day.astype(str).str[:10]
    ledger['strategy_version'] = VERSION+'-'+scenario
    ledger['scope'] = 'PARTIAL_DAY_RESEARCH' if cutoff is not None else 'BACKTEST_ONLY'
    ledger['execution_authority'] = False
    return ledger


def parity(actual, expected, *, is_partial=False):
    bounded.history._same_native(actual, expected)
    bounded.history._same_allocation(actual, expected)
    if is_partial:
        a,b = (x.set_index('sid').sort_index() for x in (actual,expected))
        if not a.position_state.equals(b.position_state) or not np.allclose(a.valuation_net_rs,b.valuation_net_rs,atol=1e-6,rtol=0):
            raise ValueError('Partial control state/MTM does not reproduce source')


def changes(before, after, value='portfolio_net_profit_rupees'):
    keys = ['day','setup_id','tradingsymbol','side','portfolio_executed','exit_reason','exit_ts',value]
    paired = before[['sid']+keys].merge(after[['sid']+keys+['failed_breakout_exit_applied']],on='sid',
        suffixes=('_before','_after'),validate='one_to_one')
    paired['delta_net_rs'] = paired[value+'_after']-paired[value+'_before']
    paired['baseline_winner_turned_loss'] = paired[value+'_before'].gt(0)&paired[value+'_after'].lt(0)
    paired['baseline_stop_avoided'] = paired.portfolio_executed_before.eq(True)&paired.exit_reason_before.isin(['STOP','TIGHTENED_STOP'])&~paired.exit_reason_after.isin(['STOP','TIGHTENED_STOP'])
    paired['newly_allocated'] = ~paired.portfolio_executed_before.eq(True)&paired.portfolio_executed_after.eq(True)
    return paired


def run(output, partial_run=None, source_run=None, source_manifest_sha256=None):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/'research').resolve()):
        raise ValueError('Fresh isolated G4 research output required')
    pointer_paths = [base.G4_ROOT/'latest/bounded_exit.json',base.G4_ROOT/'latest/g3_mirror.json',
        base.G3_ROOT/'latest/current_backtest.json',base.ROOT/'configs/strategies/active_profile.json']
    pointers = {str(p):base.sha256(p) for p in pointer_paths}
    code_paths = [Path(__file__),Path(rule.__file__),Path(partial.__file__),Path(bounded.__file__),
        Path(base.__file__),Path(bounded.history.__file__),Path(bounded.history.simulator.g.v9.__file__),
        base.ROOT/'research/g3_sl_target_simulator.py',
        Path(bounded.history.simulator.g.v9.v5.__file__),Path(bounded.history.simulator.g.v9.v6.__file__)]
    code = {str(p.resolve()):base.sha256(p) for p in code_paths}
    pointer = base.read(pointer_paths[0])
    source = (Path(source_run) if source_run is not None else base.G4_ROOT/pointer['run_dir']).resolve()
    source_digest = source_manifest_sha256 if source_run is not None else pointer['manifest_sha256']
    if not source.is_relative_to(base.G4_ROOT.resolve()) or base.sha256(source/'manifest.json') != source_digest:
        raise ValueError('G4 source pointer identity changed')
    summary = package(source)
    if summary['variant_id'] != bounded.VERSION or summary['execution_authority'] is not False:
        raise ValueError('Unexpected bounded baseline')
    original = base.read_frozen_csv(source/'g4_bounded_exit/portfolio_trades.csv')
    days = base.read_frozen_csv(source/'g4_bounded_exit/daily_results.csv').day.astype(str).tolist()
    output.mkdir(parents=True)
    paths,price_proof = historical_paths(original,summary,output)
    books = {name:replay(original,paths,name) for name in SCENARIOS}
    parity(books['current_g4'],original)
    print('Historical control exactly reproduced, including portfolio allocation',flush=True)
    periods = {'all':days,'through_september':[d for d in days if d<'2026-10-01'],
        'october_through_15':[d for d in days if d>='2026-10-01'],'last_completed_day':[days[-1]]}
    comparison=[]; daily=[]; all_changes=[]
    for name,book in books.items():
        paired = changes(original,book)
        paired['scenario'] = name
        all_changes.append(paired)
        for period,cohort in periods.items():
            if not cohort:
                continue
            metrics = bounded.stats(book,cohort)
            control = bounded.stats(original,cohort)
            selected = book.day.isin(cohort)&book.portfolio_executed.eq(True)
            delta = paired.loc[paired.day_before.isin(cohort)]
            comparison.append(dict(scenario=name,period=period,**metrics,
                net_change_rs=metrics['net_pnl_rs']-control['net_pnl_rs'],
                failed_breakout_exits=int(book.loc[selected].failed_breakout_exit_applied.eq(True).sum()),
                baseline_stops_avoided=int(delta.baseline_stop_avoided.sum()),
                baseline_winners_turned_losses=int(delta.baseline_winner_turned_loss.sum()),
                newly_allocated=int(delta.newly_allocated.sum())))
        daily.extend(dict(scenario=name,day=day,**bounded.stats(book,[day])) for day in days)
        folder=output/name;folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv',index=False)
    partial_comparison=[]; partial_changes=[]; partial_proof={}
    if partial_run is not None:
        partial_run=Path(partial_run).resolve()
        if not partial_run.is_relative_to((base.G4_ROOT/'partial').resolve()):
            raise ValueError('Partial source escaped G4 partial root')
        ps=package(partial_run)
        if ps['strategy_version'] != bounded.VERSION or ps['complete'] is not False:
            raise ValueError('Partial source differs from nominated bounded G4')
        old=base.read_frozen_csv(partial_run/'portfolio_trades.csv')
        prices=PartialPrices(ps)
        ppaths=prices.paths(old)
        np.savez_compressed(output/'partial_paths.npz',**{
            f'{sid}_{key}':v for sid,path in ppaths.items() for key,v in path.items()})
        for name in SCENARIOS:
            book=replay(old,ppaths,name,prices.cutoff)
            if name=='current_g4':
                parity(book,old,is_partial=True)
            paired=changes(old,book,'valuation_net_rs');paired['scenario']=name
            partial_changes.append(paired)
            allocated=book.loc[book.portfolio_executed.eq(True)]
            closed=allocated.loc[allocated.position_state.eq('CLOSED')]
            partial_comparison.append(dict(scenario=name,as_of_ist=str(prices.cutoff),
                selected_orders=len(book),executed_entries=len(allocated),closed_trades=len(closed),
                open_positions=int(allocated.position_state.eq('OPEN').sum()),
                stop_exits=int(closed.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum()),
                failed_breakout_exits=int(closed.failed_breakout_exit_applied.eq(True).sum()),
                realized_net_rs=float(book.realized_net_rs.sum()),
                open_mtm_net_after_reserved_cost_rs=float(book.open_mtm_net_after_reserved_cost_rs.sum()),
                combined_net_including_open_mtm_rs=float(book.valuation_net_rs.sum()),
                net_change_rs=float(book.valuation_net_rs.sum()-old.valuation_net_rs.sum())))
            book.to_csv(output/name/'partial_portfolio_trades.csv',index=False)
        partial_proof=dict(run_dir=str(partial_run),manifest_sha256=base.sha256(partial_run/'manifest.json'),
            price_sources=prices.proof,as_of_ist=ps['as_of_ist'],coverage_exclusions=ps.get('excluded_stocks'),
            unavailable_setup_ids=ps.get('unavailable_setup_ids'),complete=False,control_exactly_reproduced=True)
        pd.DataFrame(partial_comparison).to_csv(output/'partial_comparison.csv',index=False)
        pd.concat(partial_changes,ignore_index=True).to_csv(output/'partial_trade_changes.csv',index=False)
    pd.DataFrame(comparison).to_csv(output/'comparison.csv',index=False)
    pd.DataFrame(daily).to_csv(output/'daily_comparison.csv',index=False)
    pd.concat(all_changes,ignore_index=True).to_csv(output/'trade_changes.csv',index=False)
    config=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        primary_scenario='short_failed_breakout',secondary_scenario='symmetric_failed_breakout',
        rule='Full post-entry close strictly above confirmation high SHORT / below confirmation low LONG; next observed open exit.',
        entry_candle_excluded=True,initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
        native_targets_unchanged=True,round_trip_cost_bps=5.,exposure_per_trade_rs=500000.,
        portfolio_capital_rs=1000000.,parameter_search=False)
    result=dict(variant_id=VERSION,source_run=str(source),source_manifest_sha256=source_digest,
        first_date=days[0],last_date=days[-1],sessions=len(days),source_orders=len(original),
        scope='BACKTEST_ONLY',execution_authority=False,default_configuration_changed=False,
        historical_control_exactly_reproduced=True,price_sources=price_proof,
        comparison=comparison,partial_comparison=partial_comparison,partial_source=partial_proof,
        limitations=['The observed October 16 losses motivated this rule; that partial day is diagnostic, not a holdout.',
            'Earlier history has already informed G4 development and is not untouched out-of-sample evidence.',
            'Next-open execution assumes decision/order latency below one minute; spreads and broker latency are not modeled.',
            'Native stop-first ambiguity, target fills, staged stops and 15:15 square-off are preserved.',
            'No entries are filtered or replaced. Every scenario recomputes all portfolio allocations.',
            'Costs are flat 5bps; exposure is fractional Rs500000 rather than live integer-share sizing.',
            'Partial-day open MTM is not final realized profit; full roundtrip modeled costs are reserved.'])
    for name,value in [('config.json',config),('summary.json',result)]:
        (output/name).write_text(json.dumps(base.source._json_ready(value),indent=2)+'\n',encoding='utf-8')
    show=['scenario','period','trades','wins','losses','stop_exits','failed_breakout_exits',
        'net_pnl_rs','net_change_rs','daily_close_drawdown_rs','baseline_winners_turned_losses','newly_allocated']
    table=lambda f:f.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')
    document='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 failed-breakout exit test</title><style>body{font:15px/1.5 system-ui;background:#101927;color:#edf4ff;margin:30px}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:9px;border-bottom:1px solid #40526b;white-space:nowrap;text-align:left}th{background:#243449}.scroll{overflow:auto}a{color:#8ed6ff}</style></head><body><h1>G4 failed-breakout exit test</h1>'
    document+=f'<p>{len(days)} completed sessions: {days[0]} through {days[-1]}. Primary test: SHORT only. Secondary: symmetric LONG and SHORT. Research only.</p>'
    document+='<p>'+html.escape(config['rule'])+' The entry candle cannot trigger the rule. Native exits have priority on the signal candle; next-open native priority and conservative target conventions remain intact.</p>'
    document+='<h2>Historical comparison</h2><div class="scroll">'+table(pd.DataFrame(comparison)[show])+'</div>'
    if partial_comparison:
        document+='<h2>October 16 partial replay</h2><p>Same saved 14:05 cutoff as the nominated table; later prices are excluded. Existing coverage omissions are inherited.</p><div class="scroll">'+table(pd.DataFrame(partial_comparison))+'</div>'
        detail=pd.concat(partial_changes,ignore_index=True)
        document+='<h2>Every partial-day order</h2><div class="scroll">'+table(detail[['scenario','setup_id_before','tradingsymbol_before','exit_reason_before','exit_reason_after','valuation_net_rs_before','valuation_net_rs_after','delta_net_rs']])+'</div>'
    document+='<h2>Limits</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in result['limitations'])+'</ul>'
    document+='<p><a href="comparison.csv">Historical comparison</a> · <a href="daily_comparison.csv">Daily results</a> · <a href="trade_changes.csv">Trade changes</a> · <a href="partial_comparison.csv">Partial comparison</a> · <a href="summary.json">Evidence</a></p></body></html>'
    (output/'report.html').write_text(document,encoding='utf-8')
    for path,digest in {**pointers,**code}.items():
        if base.sha256(Path(path)) != digest:
            raise ValueError('A pinned pointer, configuration or source changed during the experiment: '+path)
    if base.sha256(source/'manifest.json') != source_digest:
        raise ValueError('Historical source manifest changed during research')
    package(source)
    if partial_run is not None:
        if base.sha256(partial_run/'manifest.json') != partial_proof['manifest_sha256']:
            raise ValueError('Partial source manifest changed during research')
        package(partial_run)
    for proof in list(price_proof.values())+list(partial_proof.get('price_sources',{}).values()):
        if base.sha256(Path(proof['resolved_path'])) != proof['sha256']:
            raise ValueError('Sealed price evidence changed during computation')
    # Roundtrip each saved ledger and recompute every displayed historical metric.
    for name in SCENARIOS:
        saved=base.read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,cohort in periods.items():
            if not cohort:
                continue
            actual=bounded.stats(saved,cohort)
            recorded=next(r for r in comparison if r['scenario']==name and r['period']==period)
            if any(not np.isclose(v,recorded[k],atol=1e-6,rtol=0) for k,v in actual.items()):
                raise ValueError('Saved research ledger accounting mismatch')
    manifest=dict(code_sha256=code,pointer_sha256=pointers,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()},
        complete=True,execution_authority=False)
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--partial-run',type=Path)
    parser.add_argument('--source-run',type=Path,help='Explicit sealed G4 baseline matching the nominated partial table')
    parser.add_argument('--source-manifest-sha256',help='Required expected hash with --source-run')
    args=parser.parse_args()
    now=datetime.now(base.production.IST)
    output=args.output_dir or base.G4_ROOT/'research'/f'{now:%Y%m%dT%H%M%S}_failed_breakout_exit_v1'
    if bool(args.source_run) != bool(args.source_manifest_sha256):
        parser.error('--source-run and --source-manifest-sha256 must be supplied together')
    result=run(output,args.partial_run,args.source_run,args.source_manifest_sha256)
    print(pd.DataFrame(result['comparison']).query("period == 'all'").to_string(index=False),flush=True)
    print(pd.DataFrame(result['partial_comparison']).to_string(index=False),flush=True)
    print(output.resolve(),flush=True)


if __name__=='__main__':
    main()
