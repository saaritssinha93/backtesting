"""Evaluate fixed, optional G4 entry vetoes against the bounded-exit book.

Run with: python -m research.g4_entry_veto_research
No live orders, strategy defaults, or latest result pointers are changed.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import date, datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as base
from research import g4_entry_veto_rules as rules
from research.g4_vwap_rsi_validation import trade_changes

ROOT = base.ROOT
VERSION = 'V13-V10-G-4-ENTRY-VETO-RESEARCH-V1'
CONFIG = ROOT/'configs/strategies/v13_v10_g_4_entry_veto_research_v1.json'


class EvidencePrices(base.Prices):
    """Read the same verified raw prices with a bounded memory footprint."""

    def __init__(self, summary, expected):
        super().__init__(summary)
        self.baseline_proof = expected
        self.cache = OrderedDict()

    def frame(self, day, symbol):
        directory = self.snapshots.get(day)
        path = ((directory/'equity_1m') if directory else base.source.DataRoots().equity_1m)/f'{symbol}_stocks_indicators_1min.parquet'
        key = str(path.resolve())
        if key not in self.cache:
            digest = base.sha256(path)
            if self.baseline_proof.get(key,{}).get('sha256') != digest:
                raise ValueError('Price evidence differs from the bounded G4 run: '+key)
            if directory and self.expected.get(key) != digest:
                raise ValueError('Immutable snapshot differs')
            available = set(pq.ParquetFile(path).schema.names)
            columns = ['date','open','high','low','close','volume']
            columns += [x for x in ('gap_filled','opening_snapshot','provisional_stale') if x in available]
            frame = pd.read_parquet(path,columns=columns)
            frame['ts'] = base.source.hybrid._to_ist(frame.date).dt.as_unit('ns')
            if frame.ts.isna().any():
                raise ValueError('Invalid timestamps')
            self.cache[key] = frame.sort_values('ts',kind='stable').reset_index(drop=True)
            self.proof[key] = self.baseline_proof[key]
            while len(self.cache)>8:
                self.cache.popitem(last=False)
        else:
            self.cache.move_to_end(key)
        frame = self.cache[key]
        if frame.loc[frame.ts.dt.date.eq(day)].ts.duplicated().any():
            raise ValueError('Duplicate session minutes')
        return frame,key


def allocate(book):
    engine = bounded.history.simulator
    settings = engine.g.v9.V9Config(portfolio_capital_rupees=1000000.,
        capital_per_entry_rupees=100000.,leverage_factor=5.,max_positions=10,cost_bps=5.)
    work = engine.g.v9.v5.apply_fixed_capital_model(book.copy(),100000.,5.)
    ledger,_ = engine.g.v9.v6.apply_portfolio_constraints(work,settings.portfolio_config())
    return ledger


def run(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(base.G4_ROOT.resolve()):
        raise ValueError('Fresh isolated G4 research directory required')
    config = base.read(CONFIG)
    expected = {
        'current_g4':(None,False),'stretch_3atr':(3.,False),'stretch_4atr':(4.,False),
        'pending_invalidation':(None,True),'stretch_3atr_and_pending':(3.,True),
        'stretch_4atr_and_pending':(4.,True)}
    if (config['variant_id']!=VERSION or config['scope']!='BACKTEST_ONLY'
            or config['execution_authority'] is not False
            or config['entry_expiry_minutes']!=10
            or {k:(v['maximum_extension_atr'],v['cancel_invalidated_pending'])
                for k,v in config['scenarios'].items()}!=expected):
        raise ValueError('Unexpected experiment configuration')
    pointer_path = base.G4_ROOT/'latest/bounded_exit.json'
    pointer_digest = base.sha256(pointer_path)
    pointer = base.read(pointer_path)
    source = (base.G4_ROOT/pointer['run_dir']).resolve()
    if not source.is_relative_to(base.G4_ROOT.resolve()) or base.sha256(source/'manifest.json')!=pointer['manifest_sha256']:
        raise ValueError('Bounded baseline pointer changed')
    summary = bounded.verify(source)
    if summary['variant_id']!=config['baseline_variant']:
        raise ValueError('Unexpected baseline profile')
    g3dir = Path(summary['current_g3_source_run'])
    if base.sha256(g3dir/'manifest.json')!=summary['current_g3_manifest_sha256']:
        raise ValueError('G3 price-source manifest changed')
    code = {str(p.resolve()):base.sha256(p) for p in
            (Path(__file__),Path(rules.__file__),CONFIG)}
    original = base.read_frozen_csv(source/'g4_bounded_exit/portfolio_trades.csv')
    days = base.read_frozen_csv(source/'g4_bounded_exit/daily_results.csv').day.tolist()
    prices = EvidencePrices(base.read(g3dir/'summary.json'),summary['price_sources'])
    feature_rows = []
    for count,row in enumerate(original.sort_values(['tradingsymbol','day']).to_dict('records'),1):
        raw,key = prices.frame(pd.Timestamp(row['day']).date(),row['tradingsymbol'])
        features = rules.confirmation_features(raw,row)
        path = prices.paths(pd.DataFrame([row]))[int(row['sid'])]
        features.update(rules.pending_invalidation(row,path,config['entry_expiry_minutes']))
        features.update(sid=int(row['sid']),price_source=key)
        feature_rows.append(features)
        if count%100==0:
            print(f'Computed causal entry checks: {count}/{len(original)}',flush=True)
    audit = original.merge(pd.DataFrame(feature_rows),on='sid',validate='one_to_one',suffixes=('','_guard'))
    if len(audit)!=len(original):
        raise ValueError('Guard feature audit lost orders')
    control = allocate(original)
    bounded.history._same_allocation(control,original)
    periods = {'all':days,'through_september':[x for x in days if x<'2026-10-01'],
               'october_before_15':[x for x in days if '2026-10-01'<=x<'2026-10-15'],
               'october_15':[x for x in days if x=='2026-10-15']}
    periods = {k:v for k,v in periods.items() if v}
    records=[]; changes=[]; ledgers={}; daily=[]
    for name,(limit,cancel) in expected.items():
        reject = pd.Series(False,index=audit.index)
        if limit is not None:
            reject |= audit.entry_veto_extension_atr.gt(limit)
        if cancel:
            reject |= audit.pending_cancelled.eq(True)
        audit['veto_'+name] = reject
        survivors = audit.loc[~reject,original.columns].copy()
        ledger = allocate(survivors)
        ledger['strategy_version'] = VERSION+'-'+name
        ledger['source_strategy_fingerprint'] = original.strategy_fingerprint.iloc[0]
        ledger['strategy_fingerprint'] = base.fingerprint(dict(config=config,scenario=name))
        ledgers[name] = ledger
        delta = trade_changes(original,ledger)
        delta['scenario'] = name
        changes.append(delta)
        for period,cohort in periods.items():
            selected = audit.day.isin(cohort)
            removed = audit.loc[selected & reject & audit.portfolio_executed.eq(True)]
            metrics = bounded.stats(ledger,cohort)
            before = bounded.stats(original,cohort)
            records.append(dict(scenario=name,period=period,**metrics,
                net_change_rs=metrics['net_pnl_rs']-before['net_pnl_rs'],
                vetoed_orders=int((selected & reject).sum()),
                removed_baseline_winners=int(removed.portfolio_net_profit_rupees.gt(0).sum()),
                removed_baseline_losers=int(removed.portfolio_net_profit_rupees.lt(0).sum()),
                removed_baseline_net_rs=float(removed.portfolio_net_profit_rupees.sum())))
        daily.extend(dict(scenario=name,day=day,**bounded.stats(ledger,[day])) for day in days)
        print(name,records[-len(periods)]['net_pnl_rs'],flush=True)
    if base.sha256(pointer_path)!=pointer_digest:
        raise ValueError('Default bounded result pointer changed during research')
    for path,digest in code.items():
        if base.sha256(Path(path))!=digest:
            raise ValueError('Guard research code changed during computation')
    for path,proof in prices.proof.items():
        if base.sha256(Path(path))!=proof['sha256']:
            raise ValueError('Input price changed during computation')
    output.mkdir(parents=True)
    comparison = pd.DataFrame(records)
    comparison.to_csv(output/'comparison.csv',index=False)
    pd.DataFrame(daily).to_csv(output/'daily_comparison.csv',index=False)
    pd.concat(changes,ignore_index=True).to_csv(output/'changed_executed_trades.csv',index=False)
    details = ['sid','day','setup_id','tradingsymbol','side','confirmation_ts','entry_ts','exit_ts',
               'confirmation_high','confirmation_low','trigger','filled','portfolio_executed',
               'exit_reason','native_target_pct','net_return_pct','portfolio_net_profit_rupees',
               'entry_veto_ema20','entry_veto_atr14','entry_veto_extension_atr','pending_cancelled',
               'pending_cancel_ts','pending_cancel_close']+['veto_'+name for name in expected]
    audit[details].to_csv(output/'guard_audit.csv',index=False)
    last = audit.loc[audit.day.eq(days[-1]),details].sort_values(['confirmation_ts','side'])
    last.to_csv(output/'last_day_guard_audit.csv',index=False)
    for name,book in ledgers.items():
        folder=output/name;folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv',index=False)
    result = dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        source_run=str(source),source_manifest_sha256=pointer['manifest_sha256'],
        first_date=days[0],last_date=days[-1],sessions=len(days),source_orders=len(original),
        original_accounting_reproduced=True,default_configuration_changed=False,
        methodology='Veto existing ranked orders without replacement; preserve native fills/exits for surviving orders and recalculate all portfolio allocations.',
        limitations=['The October 15 examples motivated these checks, so October 15 is a diagnostic case, not a holdout.',
          'Chronological subperiods have previously been examined during strategy development and are not untouched validation.',
          'One-minute close cancellation assumes action before the next bar opens; broker cancellation latency and fill/cancel races are not modeled.',
          'Fewer stops can result from taking fewer trades. Winner removals, lost profits and allocation changes are reported.',
          '5bps modeled costs and fixed Rs 500,000 exposure per allocated trade are retained.'],
        comparison=records,price_sources=prices.proof)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n',encoding='utf-8')
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    shown = ['scenario','period','trades','wins','losses','win_rate_pct','stop_exits',
             'profitable_days','losing_days','net_pnl_rs','net_change_rs',
             'removed_baseline_winners','removed_baseline_losers']
    lastcols = ['setup_id','tradingsymbol','side','entry_veto_extension_atr','pending_cancel_ts',
                'portfolio_net_profit_rupees']+['veto_'+x for x in expected if x!='current_g4']
    table=lambda f:f.to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    document='<!doctype html><html><head><meta charset="utf-8"><title>G4 entry veto research</title><style>body{background:#101927;color:#e4edf7;font:15px/1.5 system-ui;margin:30px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:8px;border-bottom:1px solid #40526b;text-align:left}th{background:#243449}a{color:#83cfff}.scroll{overflow:auto}</style></head><body><h1>G4 entry veto research</h1><p>52 sessions through 15 October 2026. Same 0.60-2.00% targets and 1.20% / 1.00% staged stop. No stock replacement after a veto. Every portfolio allocation is recalculated.</p><p>Stretch = directional distance of the completed confirmation close from EMA20, divided by ATR14. Pending invalidation = a completed pre-fill close below confirmation low for a long, or above confirmation high for a short. A same-minute trigger fill takes precedence over cancellation at its close.</p><h2>Full history and chronological subperiods</h2><div class="scroll">'+table(comparison[shown])+'</div><h2>15 October orders</h2><div class="scroll">'+table(last[lastcols])+'</div><p>These are retrospective experiments motivated by observed losses. None is an untouched forward test or a live execution guarantee. Defaults were not changed.</p><p><a href="comparison.csv">Comparison CSV</a> | <a href="guard_audit.csv">Every order and guard decision</a> | <a href="changed_executed_trades.csv">Profitable and losing trades removed or added</a> | <a href="daily_comparison.csv">Daywise results</a></p></body></html>'
    (output/'report.html').write_text(document,encoding='utf-8')
    manifest=dict(code_sha256=code,files={p.relative_to(output).as_posix():base.sha256(p)
        for p in output.rglob('*') if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    # Independent round-trip accounting after writing all ledgers.
    for name in expected:
        saved=base.read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,cohort in periods.items():
            observed=bounded.stats(saved,cohort)
            record=comparison.loc[comparison.scenario.eq(name)&comparison.period.eq(period)].iloc[0]
            for field,val in observed.items():
                if not np.isclose(val,record[field],rtol=0,atol=1e-6):
                    raise ValueError('Written guard results do not reconcile')
    return comparison


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args()
    output=args.output_dir or base.G4_ROOT/'research'/datetime.now().strftime('%Y%m%dT%H%M%S_entry_veto_v1')
    result=run(output)
    print(result.loc[result.period.isin(['all','october_15'])].to_string(index=False),flush=True)
    print(output,flush=True)


if __name__=='__main__':
    main()
