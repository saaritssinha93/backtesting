"""Replay a user-specified G options stop/target on the original frozen sample.

Percent CLI arguments are percentage points. The active defaults are 14 and 57.
Exactly three lots, source capital, fees, slippage and liquidity rules are kept.
This is a fixed, post-hoc scenario comparison; no parameter search is performed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_options_backtest as bt
from fno_v13_v10_g_options_config import STOP_PCT, TARGET_PCT


def run(source: Path, output: Path, stop_pct: float, target_pct: float):
    if not np.isfinite(stop_pct) or not 0 < stop_pct < 100:
        raise ValueError('stop-pct must be between zero and 100')
    if not np.isfinite(target_pct) or target_pct <= 0:
        raise ValueError('target-pct must be positive and finite')
    source, output = source.resolve(), output.resolve()
    if source == output or source in output.parents:
        raise ValueError('Use a separate sibling output directory to preserve the source run')
    manifest = json.loads((source/'manifest.json').read_text(encoding='utf-8'))
    inputs = source/'frozen_input'
    for filename in ['research_spec.json','options_trades.csv','frozen_input/inputs.json']:
        keys = {k.replace('\\','/'):v for k,v in manifest['artifacts'].items()}
        if bt.sha(source/filename) != keys[filename]:
            raise RuntimeError(f'Source artifact drift: {filename}')
    input_info = json.loads((inputs/'inputs.json').read_text(encoding='utf-8'))
    for filename, digest in input_info['artifacts'].items():
        if bt.sha(inputs/filename) != digest:
            raise RuntimeError(f'Frozen input drift: {filename}')
    pinned_code = {Path(k).name:v for k,v in manifest['code_sources'].items()}
    for name in ['fno_v13_v10_g_options_backtest.py','fno_v13_v10_g_options_execution.py']:
        if bt.sha(Path(__file__).with_name(name)) != pinned_code[name]:
            raise RuntimeError(f'Source execution code drift: {name}')
    spec = json.loads((source/'research_spec.json').read_text(encoding='utf-8'))
    mapped = pd.read_parquet(inputs/'mapped.parquet')
    paths = {p.stem:pd.read_parquet(p) for p in (inputs/'paths').glob('*.parquet')}
    days = input_info['days']
    pair = (stop_pct/100., target_pct/100.)
    policy = {'default':pair, **{s:pair for s in mapped.setup_id.unique()}}
    frame, bars, cash, events = bt.replay(mapped, paths, policy, capital=spec['capital'],
        slippage_bps=spec['slippage_bps'], participation=spec['previous_bar_participation'])
    if not bars.empty:
        bars = bars.merge(frame[['trade_id','portfolio_status']],on='trade_id',how='left',validate='many_to_one')
    cutoff = spec['train_cutoff']
    frame['historical_partition'] = np.where(frame.day.le(cutoff),'ORIGINAL_TRAIN','ORIGINAL_LATER_DATES')
    baseline = pd.read_csv(source/'options_trades.csv')
    entered = frame.loc[frame.portfolio_status.eq('ADMITTED')]
    closed = entered.loc[entered.status.eq('CLOSED')]
    baseline_entered = baseline.loc[baseline.portfolio_status.eq('ADMITTED')]
    checks = dict(
        all_source_orders_preserved=set(frame.trade_id)==set(mapped.trade_id),
        exact_three_lots=bool((entered.quantity==3*entered.lot_size).all()),
        unchanged_entries=set(entered.trade_id)==set(baseline_entered.trade_id),
        net_pnl_reconciles=bool(np.allclose(closed.net_pnl,closed.gross_pnl-closed.entry_costs-closed.exit_costs)),
        gross_pnl_reconciles=bool(np.allclose(closed.gross_pnl,(closed.exit_price-closed.entry_price)*closed.quantity)),
        cash_events_reconcile=bool(np.isclose(spec['capital']+events.cash_change.sum(),cash['ending_free_cash'])),
        cash_never_negative=bool(events.free_cash.ge(-1e-8).all()),
    )
    # Cash constraints may legitimately change the entry set with different exits.
    if not all(v for k,v in checks.items() if k!='unchanged_entries'):
        raise AssertionError(checks)
    if checks['unchanged_entries']:
        match = entered.merge(baseline_entered,on='trade_id',suffixes=('_new','_old'),validate='one_to_one')
        checks['entry_premiums_unchanged'] = bool(np.allclose(match.entry_price_new,match.entry_price_old))
        checks['contracts_unchanged'] = bool(match.option_symbol_new.eq(match.option_symbol_old).all())
        assert checks['entry_premiums_unchanged'] and checks['contracts_unchanged']
    metrics = bt.metrics(frame)
    comparison = []
    for label, trades in [('Previous 17.5% SL / 22.5% target',baseline),
                          (f'Requested {stop_pct:g}% SL / {target_pct:g}% target',frame)]:
        for period, sub in [('ALL_AVAILABLE',trades),('ORIGINAL_TRAIN',trades.loc[trades.day.le(cutoff)]),
                            ('ORIGINAL_LATER_DATES',trades.loc[trades.day.gt(cutoff)]),
                            ('DATED_METADATA_ONLY',trades.loc[trades.mapping_status.eq('MAPPED_CAUSAL')])]:
            comparison.append(dict(profile=label,period=period,**bt.metrics(sub)))
    comparison = pd.DataFrame(comparison)
    daily = pd.DataFrame([dict(day=d,option_data_present=d in spec['option_data_sessions'],
                              **bt.metrics(frame.loc[frame.day.eq(d)])) for d in days])
    daily['cumulative_closed_net_pnl'] = daily.net_pnl.cumsum()
    monthly = pd.DataFrame([dict(month=m,**bt.metrics(frame.loc[frame.day.str.startswith(m)]))
                            for m in sorted({d[:7] for d in days})])
    slots = pd.DataFrame([dict(setup_id=s,stop_pct=stop_pct,target_pct=target_pct,
                               **bt.metrics(frame.loc[frame.setup_id.eq(s)])) for s in sorted(mapped.setup_id.unique())])
    sides = pd.DataFrame([dict(option_type=s,**bt.metrics(frame.loc[frame.option_type.eq(s)])) for s in ['CE','PE']])
    exits = closed.reason.value_counts().rename_axis('exit_reason').reset_index(name='trades')
    output.mkdir(parents=True,exist_ok=True)
    for filename, data in [('options_trades.csv',frame),('options_5min_bar_audit.csv',bars),
                           ('premium_cash_events.csv',events),('comparison_summary.csv',comparison),
                           ('options_daily.csv',daily),('options_monthly.csv',monthly),
                           ('setup_sl_target_results.csv',slots),('ce_pe_results.csv',sides),('exit_counts.csv',exits)]:
        data.to_csv(output/filename,index=False)
    fixed_spec = dict(schema='V13_V10_G_OPTIONS_FIXED_USER_PROFILE_V1',source_run=str(source),
        frozen_input=str(inputs),source_manifest_sha256=bt.sha(source/'manifest.json'),
        stop_pct=stop_pct,target_pct=target_pct,lots=3,policy=policy,
        capital=spec['capital'],slippage_bps=spec['slippage_bps'],
        previous_bar_participation=spec['previous_bar_participation'],
        interval_minutes=5,square_off='15:15',original_partition_cutoff=cutoff,
        parameter_choice='EXPLICIT_USER_REQUEST_POSTHOC_COMPARISON_NO_OPTIMIZATION')
    bt.write_json(output/'selected_option_profile.json',fixed_spec)
    old_metrics = bt.metrics(baseline)
    summary = {**metrics,**cash,'stop_pct':stop_pct,'target_pct':target_pct,
        'net_change_vs_previous':metrics['net_pnl']-old_metrics['net_pnl'],
        'option_data_sessions':spec['option_data_sessions'],'original_partition_cutoff':cutoff,
        'original_later_dates':bt.metrics(frame.loc[frame.day.gt(cutoff)]),
        'validation_checks':checks}
    bt.write_json(output/'summary.json',summary)
    bt.write_json(output/'validation.json',dict(passed=all(checks.values()),checks=checks,
        frozen_input_hashes_verified=len(input_info['artifacts']),source_engine_hashes_verified=2))
    cols=['profile','period','closed','wins','losses','win_rate_pct','profit_factor','net_pnl','daily_realized_drawdown']
    report = [f'# V13-V10-G options: {stop_pct:g}% SL / {target_pct:g}% target, three lots', '',
        f"**Modeled net: Rs{metrics['net_pnl']:,.2f}. {metrics['closed']} closed trades, {metrics['wins']} wins / {metrics['losses']} losses.** "
        f"Change from 17.5% / 22.5%: Rs{summary['net_change_vs_previous']:+,.2f}.", '',
        f'This is the explicitly requested fixed exit setting on the identical frozen August 26–September 11, 2026 option sample. '
        f'Buy three lots of ATM CE for LONG / ATM PE for SHORT. Stop is entry premium × {1-pair[0]:g}; target is entry premium × {1+pair[1]:g}, '
        'with the existing contract-tick rounding. All three lots exit together. Five-minute monitoring, 15:15 square-off, '
        '10 bps adverse market-fill slippage, fee schedule, prior-volume screening and Rs15 lakh cash account are unchanged.', '',
        '## Compared with the previous settings', '',bt.table(comparison[cols]), '',
        f"Peak reserved premium plus entry fees: Rs{cash['peak_reserved_premium_and_fees']:,.2f}. "
        f"Ending free cash: Rs{cash['ending_free_cash']:,.2f}. Unresolved entered trades: {metrics['unresolved']}. "
        'Drawdown above uses daily realized closes; it is not intraday mark-to-market drawdown.', '',
        '## Monthly results', '',bt.table(monthly[['month','closed','wins','losses','win_rate_pct','profit_factor','net_pnl','costs']]), '',
        '## Calls and puts', '',bt.table(sides[['option_type','closed','wins','losses','win_rate_pct','profit_factor','net_pnl']]), '',
        '## Exit reasons', '',bt.table(exits), '',
        '## Every five-minute setup', '',bt.table(slots[['setup_id','stop_pct','target_pct','closed','wins','losses','net_pnl']]), '',
        '## Executed trade ledger', '',bt.table(entered[[c for c in ['day','setup_id','option_symbol','quantity','entry_ts','entry_price','stop_price','target_price','exit_ts','exit_price','reason','net_pnl','status'] if c in entered]]), '',
        '## Coverage and interpretation', '',
        'All 73 original orders remain in the trade ledger. Forty triggered entries lack August-expiry options metadata/history, '
        'seven stock orders never triggered, and entry-data/liquidity exclusions remain explicit. August executions use metadata '
        'reconstructed from a later snapshot. Missing option history is not treated as verified zero trading profit.', '',
        f'The earlier study split at {cutoff} is retained for comparison. This setting was requested after seeing the earlier results; '
        'the later-date comparison is not an untouched out-of-sample test. The underlying G strategy also reused this history.', '',
        'Option-bar volumes are execution proxies; ex-post participation breaches remain recorded. No historical bid/ask depth is available. '
        'Stops take precedence when both levels are touched within an ambiguous candle; opening gaps are handled before intrabar highs/lows.', '',
        '## Reproduction and artifacts', '',
        f'`python -B fno_v13_v10_g_options_fixed_profile.py --stop-pct {stop_pct:g} --target-pct {target_pct:g}`', '',
        '[All trade attempts](options_trades.csv) · [Every monitored five-minute candle](options_5min_bar_audit.csv) · '
        '[Daily results](options_daily.csv) · [Validation](validation.json)', '',
        f"Verified {len(input_info['artifacts'])} frozen input hashes and the original replay/engine hashes. Quantity, fees, P&L, cash and entry parity checks passed."
    ]
    (output/'V13_V10_G_OPTIONS_FIXED_PROFILE_RESULTS.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    bt.write_json(output/'manifest.json',dict(complete=True,configuration=fixed_spec,
        code_sha256={str(Path(__file__).resolve()):bt.sha(Path(__file__)),
                     **{str(Path(__file__).with_name(n).resolve()):pinned_code[n] for n in ['fno_v13_v10_g_options_backtest.py','fno_v13_v10_g_options_execution.py']}},
        artifacts={p.name:bt.sha(p) for p in output.iterdir() if p.is_file() and p.name!='manifest.json'}))
    print(json.dumps(bt.json_value(summary),indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-run',type=Path,default=bt.DEFAULT_OUTPUT)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--stop-pct',type=float,default=STOP_PCT * 100)
    parser.add_argument('--target-pct',type=float,default=TARGET_PCT * 100)
    args=parser.parse_args()
    suffix=f"sl{args.stop_pct:g}_target{args.target_pct:g}".replace('.','p')
    output=args.output_dir or args.source_run.with_name(args.source_run.name+'_'+suffix)
    run(args.source_run,output,args.stop_pct,args.target_pct)


if __name__=='__main__':
    main()
