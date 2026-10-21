"""Isolated G4 bounded-exit replay of an explicitly incomplete session.

Uses a sealed raw-input snapshot, genuine minute bars and the existing G4
selection/entry/exit/portfolio rules. No completed-history pointer is published.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
from research import g4_bounded_exit_backtest as bounded
from research import g4_complete_inventory_backtest as complete
from research import g4_g3_mirror_backtest as mirror
from research.g3_0950_long_backtest import normalize_signals
from research.g3_sl_target_simulator import simulate_exit


def partial_exit(path, index, entry, is_long, target):
    """A surviving truncated path is open, never a forced square-off."""
    try:
        result = simulate_exit(path,index,entry,is_long,1.2,target,
                               tighten_after_minutes=120,tightened_stop_pct=1.)
        result.update(position_open=False,mark_price=np.nan,mark_ts=pd.NaT)
        return result
    except ValueError as error:
        if str(error) != 'Position survives a path missing its native 15:15 square-off candle':
            raise
    last = pd.Timestamp(int(path['timestamp_ns'][-1]),tz='UTC').tz_convert(source.common.IST)
    first = pd.Timestamp(int(path['timestamp_ns'][index]),tz='UTC').tz_convert(source.common.IST)
    if last.strftime('%H:%M') >= '15:15':
        raise ValueError('Open partial path must end before square-off')
    sign = 1 if is_long else -1
    mark = float(path['close'][-1])
    active = 1. if last-pd.Timedelta(minutes=1) >= first+pd.Timedelta(minutes=120) else 1.2
    return dict(position_open=True,exit_reason='OPEN_AT_CUTOFF',exit_ts=pd.NaT,
        exit_price=np.nan,exit_path_index=None,exit_event='NONE',
        mark_price=mark,mark_ts=last,gross_return_pct=sign*(mark/entry-1)*100,
        active_stop_pct_at_exit=active,active_stop_price=entry*(1-sign*active/100),
        initial_stop_pct=1.2,stop_hit=False,target_hit=False,same_bar_ambiguous=False)


def replay_orders(orders, paths, cutoff):
    engine = bounded.history.simulator.g.v9
    records=[]
    for record in orders.to_dict('records'):
        row=dict(record)
        path=paths[int(row['sid'])]
        row.update(native_stop_pct=1.2,initial_stop_pct=1.2,
                   native_target_pct=float(np.clip(row['native_target_pct'],.6,2.)))
        found=engine.v5._entry(SimpleNamespace(**row),path,delay_bars=0,
            trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=10)
        row.update(filled=False,position_open=False,entry_ts=pd.NaT,exit_ts=pd.NaT,
            gross_return_pct=np.nan,net_return_pct=np.nan,cost_pct=np.nan,
            exit_reason='UNFILLED_EXPIRED' if cutoff>=pd.Timestamp(row['confirmation_ts'])+pd.Timedelta(minutes=10) else 'PENDING_AT_CUTOFF')
        if found is not None:
            index,entry,trigger,gap,overshoot=found
            row.update(filled=True,entry_path_index=index,entry_price=entry,trigger_used=trigger,
                entry_gap_through=gap,entry_overshoot_bps=overshoot,
                entry_ts=pd.Timestamp(int(path['timestamp_ns'][index]),tz='UTC').tz_convert(source.common.IST))
            row.update(partial_exit(path,index,entry,row['side']=='LONG',row['native_target_pct']))
            row.update(cost_pct=.05,net_return_pct=row['gross_return_pct']-.05)
        records.append(row)
    trades=pd.DataFrame(records)
    # The existing allocator requires a release timestamp. Open positions stay
    # reserved beyond every observed entry. This is not an invented price/exit.
    trades['allocation_release_ts']=pd.to_datetime(trades.exit_ts,utc=True).dt.tz_convert(source.common.IST)
    trades.loc[trades.position_open,'allocation_release_ts']=cutoff+pd.Timedelta(minutes=1)
    work=trades.copy()
    work['exit_ts']=work.allocation_release_ts
    settings=engine.V9Config(portfolio_capital_rupees=1000000.,capital_per_entry_rupees=100000.,
                            leverage_factor=5.,max_positions=10,cost_bps=5.)
    sized=engine.v5.apply_fixed_capital_model(work,100000.,5.)
    ledger,_=engine.v6.apply_portfolio_constraints(sized,settings.portfolio_config())
    opened=ledger.position_open.eq(True)&ledger.portfolio_executed.eq(True)
    closed=~ledger.position_open.eq(True)&ledger.portfolio_executed.eq(True)
    ledger['position_state']=np.select([opened,closed,ledger.portfolio_status.eq('REJECTED')],
        ['OPEN','CLOSED','PORTFOLIO_REJECTED'],default=ledger.exit_reason)
    ledger['valuation_net_rs']=ledger.portfolio_net_profit_rupees
    ledger['realized_net_rs']=np.where(closed,ledger.valuation_net_rs,0.)
    ledger['open_mtm_gross_rs']=np.where(opened,ledger.portfolio_gross_profit_rupees,0.)
    ledger['open_mtm_net_after_reserved_cost_rs']=np.where(opened,ledger.valuation_net_rs,0.)
    ledger.loc[ledger.position_open.eq(True),'exit_ts']=pd.NaT
    # Keep generic realized-accounting fields free of open-position valuation.
    for col in ['portfolio_net_profit_rupees','portfolio_gross_profit_rupees','portfolio_cost_rupees']:
        ledger.loc[~closed,col]=0.
    ledger['as_of_ist']=str(cutoff)
    ledger['strategy_version']=bounded.VERSION
    ledger['scope']='PARTIAL_DAY_BACKTEST_ONLY'
    ledger['execution_authority']=False
    return ledger


class SnapshotPrices:
    enrich=mirror.Prices.enrich

    def __init__(self,roots,day,cutoff):
        self.roots,self.day,self.cutoff=roots,day,cutoff
        self.last_key=None
        self.last_frame=None

    def frame(self,day,symbol):
        assert day==self.day
        key=self.roots.equity_1m/f'{symbol}_stocks_indicators_1min.parquet'
        if key!=self.last_key:
            problems=[]
            frame=source._load_minute(key,day,problems,symbol)
            if problems:
                raise ValueError(str(problems))
            self.last_key,self.last_frame=key,frame.loc[frame.ts.le(self.cutoff)].copy()
        return self.last_frame,str(key)

    def paths(self,orders):
        result={}
        for row in orders.itertuples():
            raw,_=self.frame(self.day,row.tradingsymbol)
            stamp=pd.Timestamp(row.confirmation_ts).tz_convert(self.cutoff.tz)
            frame=raw.loc[raw.ts.gt(stamp)&raw.ts.le(self.cutoff)]
            expected=pd.date_range(stamp+pd.Timedelta(minutes=1),self.cutoff,freq='min')
            if not np.array_equal(frame.ts.astype('int64').to_numpy(),expected.asi8):
                raise ValueError(f'Incomplete observed execution path {row.tradingsymbol}')
            result[int(row.sid)]={'timestamp_ns':frame.ts.astype('int64').to_numpy(),
                **{name:frame[name].to_numpy(float) for name in ['open','high','low','close']}}
        return result


def _feature_partition(args):
    day,roots,clocks=args
    result=source.build_day_features(day,roots=roots,signal_clocks=clocks)
    result.pop('minutes',None)
    return result


def build_features(day,roots,clocks,output,workers):
    if workers==1:
        return source.build_day_features(day,roots=roots,signal_clocks=clocks)
    # Each stock's features are independent. Partition only the copied dated
    # universe; preserve every original row and combine before any ranking.
    original=pd.read_parquet(roots.universe/f'near_month_{day}.parquet')
    stocks=original.loc[~original.is_index_future.fillna(False).astype(bool)]
    indexes=original.loc[original.is_index_future.fillna(False).astype(bool)]
    args=[]
    for i in range(workers):
        folder=output/'feature_partitions'/str(i)
        folder.mkdir(parents=True,exist_ok=True)
        subset=pd.concat([stocks.iloc[i::workers],indexes],ignore_index=True)
        subset.to_parquet(folder/f'near_month_{day}.parquet',index=False)
        args.append((day,source.DataRoots(folder,roots.equity_1m,roots.futures_5m,roots.opening_observer),clocks))
    with ProcessPoolExecutor(max_workers=workers) as executor:
        parts=list(executor.map(_feature_partition,args))
    result={'universe_count':sum(p['universe_count'] for p in parts)}
    if result['universe_count']!=len(stocks):
        raise ValueError('Parallel feature reconstruction lost universe members')
    for key in ('problems','coverage_rows','excluded_stocks','opening_0920_exclusions'):
        result[key]=[row for p in parts for row in p[key]]
    for key in ('signals','feature_ledger'):
        result[key]=pd.concat([p[key] for p in parts],ignore_index=True)
    signals=result['signals'].sort_values(['tradingsymbol','signal_ts','side'],kind='stable').reset_index(drop=True)
    signals['sid']=np.arange(len(signals))
    result['signals']=signals
    if result['feature_ledger'].duplicated(['tradingsymbol','signal_ts']).any():
        raise ValueError('Duplicate stock/clock across feature partitions')
    return result


def run(output,workers=1,reuse_reconstruction=False,config_path=complete.DEFAULT_CONFIG):
    output=Path(output).resolve()
    if not output.is_relative_to((bounded.G4_ROOT/'partial').resolve()) or (output/'manifest.json').exists():
        raise ValueError('An unsealed isolated G4 partial directory is required')
    reference=mirror.read(output/'snapshot_reference.json')
    snapshot=Path(reference['snapshot'])
    manifest=mirror.read(snapshot/'snapshot_manifest.json')
    source._verify_input_snapshot(snapshot,manifest)
    if manifest['problems']:
        raise ValueError('Snapshot capture incomplete')
    day=date.fromisoformat(manifest['session_date'])
    roots=source.DataRoots(snapshot/'universe',snapshot/'equity_1m',snapshot/'futures_5m',snapshot/'opening_observer')
    version=mirror.read(config_path).get('variant_id')
    expanded=version==complete.VERSION
    config=complete.configuration(config_path) if expanded else bounded.configuration(config_path)
    active_setups=complete.Identity(config).ACTIVE_SETUPS if expanded else mirror.frozen_donor.ACTIVE_SETUPS
    active_ids={s.setup_id for s in active_setups}
    donor=mirror.configuration()
    if donor['variant_id']!=config['donor_g4_variant']:
        raise ValueError('Unexpected entry profile')
    cutoffs=[]
    for item in manifest['sources']:
        if item['role']!='EQUITY_ONE_MINUTE':
            continue
        if not item['captured']:
            raise ValueError('Missing equity source')
        frame=pd.read_parquet(snapshot/item['snapshot_relative_path'],columns=['date'])
        stamps=source.hybrid._to_ist(frame.date)
        today=stamps.loc[stamps.dt.date.eq(day)]
        cutoffs.append(dict(symbol=Path(item['snapshot_relative_path']).name,
                            first=str(today.min()),last=str(today.max()),minutes=len(today)))
    available=pd.DataFrame(cutoffs)
    if available.minutes.eq(0).any():
        raise ValueError('Some universe stocks have no observed session data')
    cutoff=min(pd.Timestamp(x) for x in available['last']).tz_convert(source.common.IST)
    now=pd.Timestamp.now(tz=source.common.IST).floor('min')
    if cutoff>now or cutoff.strftime('%H:%M')>='15:15':
        raise ValueError('This runner is only for genuine pre-square-off partial sessions')
    clocks={s.signal_end:s.confirmation_end for s in active_setups
            if pd.Timestamp(source.slot_datetime(day,s.confirmation_end))<=cutoff}
    if not clocks:
        raise ValueError('No completed setup confirmations')
    print(f'Frozen common minute cutoff: {cutoff}; stocks: {len(available)}',flush=True)
    if reuse_reconstruction:
        proof=mirror.read(output/'reconstruction_manifest.json')
        if proof['snapshot_manifest_sha256']!=mirror.sha256(snapshot/'snapshot_manifest.json') or proof['cutoff']!=str(cutoff):
            raise ValueError('Reconstruction checkpoint belongs to another snapshot/cutoff')
        for name,digest in proof['files'].items():
            if mirror.sha256(output/name)!=digest:
                raise ValueError('Reconstruction checkpoint changed')
        prior=mirror.read(output/'coverage.json')
        dataset=dict(signals=pd.read_pickle(output/'reconstructed_signals.pkl'),
            feature_ledger=pd.read_pickle(output/'reconstructed_features.pkl'),
            problems=prior['source_problems'],excluded_stocks=prior['excluded_stocks'],
            universe_count=prior['universe_count'],coverage_rows=pd.read_csv(output/'coverage.csv').to_dict('records'))
    else:
        dataset=build_features(day,roots,sorted(clocks),output,workers)
        dataset['signals'].to_pickle(output/'reconstructed_signals.pkl')
        dataset['feature_ledger'].to_pickle(output/'reconstructed_features.pkl')
    unavailable_setup_ids=[]
    if any(x['reason']=='NO_VALID_0920_OPENING_INPUTS' for x in dataset['problems']):
        unavailable_setup_ids=[s.setup_id for s in active_setups if s.signal_end=='09:20']
    blockers=[]
    expected_future=[]
    for item in dataset['problems']:
        if item['reason']=='MISSING_REQUIRED_EQUITY_MINUTES':
            past=[t for t in item['timestamps'] if pd.Timestamp(t)<=cutoff]
            if past:
                blockers.append({**item,'timestamps':past,'count':len(past)})
            else:
                expected_future.append(item)
        elif item['reason'] not in source.EXCLUDABLE_COVERAGE_REASONS|{'NO_VALID_0920_OPENING_INPUTS'}:
            blockers.append(item)
    coverage=dict(cutoff=str(cutoff),universe_count=dataset['universe_count'],
        excluded_stocks=dataset['excluded_stocks'],blocking_problems=blockers,
        expected_unobserved_future_minutes=expected_future,
        source_problems=dataset['problems'],unavailable_setup_ids=unavailable_setup_ids,
        partial_selection_policy='Disclose unavailable opening setup and source stock exclusions; never fabricate inputs')
    (output/'coverage.json').write_text(json.dumps(source._json_ready(coverage),indent=2),encoding='utf-8')
    available.to_csv(output/'source_cutoffs.csv',index=False)
    pd.DataFrame(dataset['coverage_rows']).to_csv(output/'coverage.csv',index=False)
    checkpoint=dict(snapshot_manifest_sha256=mirror.sha256(snapshot/'snapshot_manifest.json'),cutoff=str(cutoff),
        files={n:mirror.sha256(output/n) for n in ['reconstructed_signals.pkl','reconstructed_features.pkl','coverage.json','coverage.csv']})
    (output/'reconstruction_manifest.json').write_text(json.dumps(checkpoint,indent=2),encoding='utf-8')
    if blockers:
        raise ValueError(f'Historical input gaps before cutoff: {len(blockers)}; see coverage.json')
    raw=normalize_signals(dataset['signals'])
    raw=raw.loc[pd.to_datetime(raw.confirmation_ts,utc=True).le(cutoff)].copy()
    raw.to_csv(output/'candidate_signals.csv',index=False)
    pool=mirror.donor.selection_audit(raw)
    pool=pool.loc[pool.setup_id.isin(active_ids)].copy()
    pool=pool.loc[~pool.setup_id.isin(unavailable_setup_ids)].copy()
    prices=SnapshotPrices(roots,day,cutoff)
    pool=prices.enrich(pool.sort_values(['tradingsymbol','confirmation_ts']).copy())
    scenario=donor['primary_scenario']
    if expanded:
        orders=complete.reselect(pool,config)
    else:
        orders=mirror.reselect(pool,donor['scenarios'][scenario],donor['five_minute_volume_ratio_overrides'][scenario])
        orders=mirror.Identity(donor,scenario).identify(orders)
    if orders.empty:
        raise ValueError('No selected orders; needs empty-result reporting')
    orders=orders.sort_values(['confirmation_ts','side']).reset_index(drop=True)
    ledger=replay_orders(orders,prices.paths(orders),cutoff)
    ledger['strategy_version']=config['variant_id']
    ledger['source_strategy_fingerprint']=ledger['strategy_fingerprint']
    ledger['strategy_fingerprint']=mirror.fingerprint(config)
    pool.to_csv(output/'selection_audit.csv',index=False)
    orders.to_csv(output/'selected_orders.csv',index=False)
    ledger.to_csv(output/'portfolio_trades.csv',index=False)
    filled=ledger.loc[ledger.portfolio_executed.eq(True)]
    closed=filled.loc[filled.position_state.eq('CLOSED')]
    opened=filled.loc[filled.position_state.eq('OPEN')]
    summary=dict(strategy_version=config['variant_id'],state='PARTIAL_DAY_WITH_EXCLUSIONS' if unavailable_setup_ids or dataset['excluded_stocks'] else 'PARTIAL_DAY',complete=False,
        session_date=day.isoformat(),as_of_ist=str(cutoff),created_at_ist=str(pd.Timestamp.now(tz=source.common.IST)),
        execution_authority=False,current_configuration_unchanged=True,selective_four_guards_enabled=False,
        universe_stocks=dataset['universe_count'],excluded_stocks=dataset['excluded_stocks'],
        active_setup_count=len(active_setups),unavailable_setup_ids=unavailable_setup_ids,available_setups=len(active_setups)-len(unavailable_setup_ids),
        selected_orders=len(orders),executed_entries=len(filled),closed_trades=len(closed),open_positions=len(opened),
        expired_unfilled=int(ledger.exit_reason.eq('UNFILLED_EXPIRED').sum()),
        pending_entries=int(ledger.exit_reason.eq('PENDING_AT_CUTOFF').sum()),
        portfolio_rejected=int(ledger.position_state.eq('PORTFOLIO_REJECTED').sum()),
        closed_wins=int(closed.realized_net_rs.gt(0).sum()),closed_losses=int(closed.realized_net_rs.lt(0).sum()),
        closed_trade_win_rate_pct=100*float(closed.realized_net_rs.gt(0).mean()) if len(closed) else None,
        stop_exits=int(closed.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum()),
        target_exits=int(closed.exit_reason.eq('TARGET').sum()),
        realized_net_rs=float(ledger.realized_net_rs.sum()),
        open_mtm_gross_rs=float(ledger.open_mtm_gross_rs.sum()),
        open_mtm_net_after_reserved_cost_rs=float(ledger.open_mtm_net_after_reserved_cost_rs.sum()),
        combined_net_including_open_mtm_rs=float(ledger.valuation_net_rs.sum()),
        modeled_round_trip_cost_per_full_trade_rs=250.,
        snapshot_manifest=str(snapshot/'snapshot_manifest.json'),snapshot_manifest_sha256=mirror.sha256(snapshot/'snapshot_manifest.json'),
        limitations=['Partial-day marked valuation is not final realized P&L.',
            'Open-trade net MTM reserves the full 5bps roundtrip cost, not an actual broker charge.',
            'One-minute OHLC replay with stop-first ambiguity and observed-open stop gaps.',
            'Historical availability does not certify live receipt latency.'])
    assert np.isclose(summary['combined_net_including_open_mtm_rs'],summary['realized_net_rs']+summary['open_mtm_net_after_reserved_cost_rs'],atol=1e-6)
    assert ledger.loc[ledger.position_open.eq(True),'exit_ts'].isna().all()
    assert not ledger.exit_reason.eq('TIME_EXIT_1515').any()
    (output/'summary.json').write_text(json.dumps(source._json_ready(summary),indent=2),encoding='utf-8')
    (output/'config.json').write_text(json.dumps(config,indent=2),encoding='utf-8')
    cols=['setup_id','tradingsymbol','side','entry_ts','entry_price','native_target_pct','position_state',
          'exit_reason','exit_ts','exit_price','mark_price','realized_net_rs','open_mtm_net_after_reserved_cost_rs','valuation_net_rs']
    details=ledger[cols]
    details.to_csv(output/'today_orders.csv',index=False)
    summary_table=pd.DataFrame([summary]).drop(columns=['excluded_stocks','limitations','snapshot_manifest','snapshot_manifest_sha256']).T.reset_index()
    summary_table.columns=['Measure','Value']
    doc='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 partial-day backtest</title><style>body{background:#101927;color:#edf4ff;font:15px/1.6 system-ui;margin:30px}h1,h2{color:#fff}a{color:#8ed6ff}.scroll{overflow:auto}table{border-collapse:collapse;width:100%}th,td{padding:8px;border-bottom:1px solid #476078;text-align:left;white-space:nowrap}th{background:#213b50}</style></head><body><h1>G4 partial-day backtest</h1>'''
    doc+=f'<p>{day} through {cutoff.strftime("%H:%M")} IST. Incomplete session. Current bounded-exit G4; the four selective research guards are not enabled.</p>'
    doc+=f'<p><strong>Coverage exclusions:</strong> unavailable setup IDs: {unavailable_setup_ids}; excluded stocks: {[x["symbol"] for x in dataset["excluded_stocks"]]}. These omissions reflect unavailable valid opening evidence or required futures OI. This is not a complete-universe final-day result.</p>'
    doc+='<p>Targets 0.60–2.00%; SL 1.20% tightening to 1.00% after 120 minutes. Open positions are marked at the last observed close, not force-exited. Their estimated net MTM reserves the full Rs250 roundtrip modeled cost per full trade.</p>'
    doc+=summary_table.to_html(index=False,border=0)+'<h2>Every selected order</h2><div class="scroll">'+details.to_html(index=False,border=0,na_rep='—',float_format=lambda x:f'{x:,.2f}')+'</div>'
    doc+='<p><a href="today_orders.csv">Order details CSV</a> · <a href="portfolio_trades.csv">Full ledger CSV</a> · <a href="coverage.json">Coverage</a> · <a href="summary.json">Summary and evidence</a></p></body></html>'
    (output/'report.html').write_text(doc,encoding='utf-8')
    proof=dict(files={p.name:mirror.sha256(p) for p in output.iterdir() if p.is_file()},
        code_sha256={str(Path(p).resolve()):mirror.sha256(Path(p)) for p in [__file__,bounded.__file__,complete.__file__,mirror.__file__,source.__file__,config_path]},
        feature_partition_sha256={p.relative_to(output).as_posix():mirror.sha256(p) for p in (output/'feature_partitions').rglob('*.parquet')})
    (output/'manifest.json').write_text(json.dumps(proof,indent=2),encoding='utf-8')
    print(json.dumps(source._json_ready(summary),indent=2),flush=True)
    print(details.to_string(index=False),flush=True)
    print(output,flush=True)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--workers',type=int,choices=range(1,9),default=1)
    parser.add_argument('--reuse-reconstruction',action='store_true')
    parser.add_argument('--config',type=Path,default=complete.DEFAULT_CONFIG)
    args=parser.parse_args()
    run(args.output_dir,args.workers,args.reuse_reconstruction,args.config)
