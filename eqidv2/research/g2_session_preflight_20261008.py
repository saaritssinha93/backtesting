"""Read-only pre-session evidence inventory. This is not a backtest report."""
from __future__ import annotations

import argparse
import html
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_backtest as g2


def timestamp(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize('Asia/Kolkata') if stamp.tz is None else stamp.tz_convert('Asia/Kolkata')


def inspect_prices(path, day):
    record = {'path':str(path),'file_exists':path.is_file(),'target_date_rows':None,
              'latest_timestamp_ist':None,'status':'FILE_MISSING'}
    if not path.is_file():
        return record
    try:
        parquet = pq.ParquetFile(path)
        names = parquet.schema.names
        field = next((f for f in ('date','timestamp','ts') if f in names),None)
        if field is None:
            record['status']='TIMESTAMP_FIELD_UNAVAILABLE'
            return record
        position=names.index(field)
        maxima=[]
        for group in range(parquet.metadata.num_row_groups):
            stats=parquet.metadata.row_group(group).column(position).statistics
            if stats is None or not stats.has_min_max:
                maxima=[]
                break
            maxima.append(timestamp(stats.max))
        if maxima:
            latest=max(maxima)
            record['latest_timestamp_ist']=latest.isoformat()
            if latest.date()<day:
                record.update(target_date_rows=0,status='NO_REQUESTED_SESSION_ROWS',
                              evidence='PARQUET_TIMESTAMP_MAX_BEFORE_REQUESTED_DATE')
                return record
        raw=pd.read_parquet(path,columns=[field])[field]
        values=pd.to_datetime(raw,errors='coerce')
        values=values.dt.tz_localize('Asia/Kolkata') if values.dt.tz is None else values.dt.tz_convert('Asia/Kolkata')
        count=int(values.dt.date.eq(day).sum())
        record.update(target_date_rows=count,latest_timestamp_ist=values.max().isoformat(),
                      status='REQUESTED_ROWS_REQUIRE_OHLCV_VALIDATION' if count else 'NO_REQUESTED_SESSION_ROWS',
                      evidence='TIMESTAMP_COLUMN_SCAN')
    except Exception as exc:
        record.update(status='UNREADABLE',error=f'{type(exc).__name__}: {exc}')
    return record


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date',default='2026-10-08')
    args=parser.parse_args()
    day=pd.Timestamp(args.date).date()
    now=pd.Timestamp.now(tz='Asia/Kolkata')
    requested=common.UNIVERSE_DIR/f'near_month_{day}.parquet'
    if requested.exists() or now>=pd.Timestamp(f'{day} 09:15',tz='Asia/Kolkata'):
        raise RuntimeError('This pre-session report is only for a not-yet-started session without its dated universe; run the full audit when inputs exist.')
    prior=sorted(p for p in common.UNIVERSE_DIR.glob('near_month_*.parquet')
                 if p.stem.replace('near_month_','')<str(day))[-1]
    universe=pd.read_parquet(prior)
    stocks=universe.loc[~universe.underlying.isin(common.INDEX_UNDERLYINGS)].copy()
    protected=[Path(g2.__file__),Path(g2.g.__file__),g2.DEFAULT_G_CONFIG,prior]
    before={str(p.resolve()):g2.sha256(p) for p in protected}
    source_bundle=g2.verify_bundle(g2.DEFAULT_SOURCE_BUNDLE)
    source=g2.read_json(g2.DEFAULT_G_CONFIG)
    frozen=g2.config(source)
    rows=[]
    for index,row in enumerate(stocks.itertuples()):
        symbol=str(row.equity_symbol)
        price_symbol=hybrid.resolve_backtest_equity_symbol(symbol,hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
        paths={'equity_1m':hybrid.equity_one_minute_path(price_symbol,hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR),
               'equity_5m':hybrid.equity_five_minute_path(symbol,hybrid.DEFAULT_BACKTEST_EQUITY_5M_DIR),
               'futures_5m_oi':common.RAW_CONTRACT_DIR/f'{common.safe_contract_stem(row.futures_tradingsymbol)}_5minute.parquet'}
        for role,path in paths.items():
            rows.append(dict(reference_universe=str(prior),reference_only_not_requested_universe=True,
                             symbol=symbol,role=role,**inspect_prices(path,day)))
        if index%50==0:
            print(f'Reference inventory: {index+1}/{len(stocks)} previous-universe symbols',flush=True)
    frame=pd.DataFrame(rows)
    live=common.FNO_ROOT/'v13_v10_g_live'
    artifacts={name:[str(p) for pattern in (str(day),str(day).replace('-',''))
                    for p in (live/name).rglob('*'+pattern+'*')] if (live/name).exists() else []
               for name in ('scanner_5m','confirmation_1m','signals','orders','order_events','evidence')}
    daily_root=Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs')
    report=dict(session_date=str(day),checked_at_ist=now.isoformat(),status='SESSION_NOT_STARTED',
        report_complete=False,movement_analysis_available=False,session_data_finalized=False,
        requested_dated_universe=str(requested),requested_dated_universe_exists=False,
        requested_universe_count=None,usable_requested_universe_count=None,
        previous_universe_reference=dict(path=str(prior),sha256=g2.sha256(prior),total_contracts=len(universe),
            stock_rows=len(stocks),unique_stock_symbols=int(stocks.equity_symbol.nunique()),
            index_rows=len(universe)-len(stocks),used_as_requested_universe=False),
        storage_inventory=frame.groupby(['role','status'],dropna=False).size().rename('files').reset_index().to_dict('records'),
        requested_rows_seen=int(frame.target_date_rows.fillna(0).sum()),
        recorded_artifacts=artifacts,requested_backtest_root=str(daily_root/str(day)),
        requested_backtest_exists=(daily_root/str(day)).exists(),actual_requested_backtest_run=None,
        latest_available_run_day=max(p.name for p in daily_root.iterdir() if p.is_dir()),
        standard_g2=dict(source_file=str(Path(g2.__file__).resolve()),source_sha256=g2.sha256(Path(g2.__file__)),
            retained_g_config=str(g2.DEFAULT_G_CONFIG),retained_g_config_sha256=g2.sha256(g2.DEFAULT_G_CONFIG),
            source_bundle=str(g2.DEFAULT_SOURCE_BUNDLE),source_bundle_verified=source_bundle.get('state')=='COMPLETE',
            settings=frozen,relaxed_0925_long_enabled=False),
        earlier_comparison_note='The earlier91-trade comparison enabled optional relaxed09:25LONG; it is a separate variant from standard frozenG2 and has not been silently substituted.',
        official_previous_close_status='NOT_VERIFIED_AS_EXCHANGE_OFFICIAL_CLOSE',
        requested_counts=dict(reached_plus_2pct=None,reached_minus_2pct=None,
            trough_between_minus_2pct_and_zero=None,close_between_minus_2pct_and_zero=None),
        selection_status='NOT_YET_EVALUATED_NO_RECORDED_REJECTION',
        counterfactual_status='UNAVAILABLE_NO_SESSION_PRICES_OR_TARGET_DATE_UNIVERSE',
        proposed_changes=[],historical_replay_status='NOT_RUN_NO_SESSION_DERIVED_PROPOSAL',
        open_requirements=['October8datedstock-universe snapshot and reconciled symbols',
            'Valid session price bars, futuresOI and timestamps',
            'Verified prior trading day official cash closing prices and current officialclose',
            'FrozenG2 slot reconstruction plus recorded scanner/confirmation/ranking/order evidence',
            'Whole-universe causal counterfactuals and historical validation before recommendations'],
        no_baseline_mutations=True,market_timing_source='https://www.nseindia.com/static/market-data/market-timings')
    if before!={path:g2.sha256(Path(path)) for path in before}:
        raise RuntimeError('Protected baseline changed during preflight')
    output=common.FNO_ROOT/'strategy_research'/'v13_g2_session_audit'/f'{day}_preflight_{datetime.now():%H%M%S}'
    output.mkdir(parents=True)
    frame.to_csv(output/'previous_universe_storage_inventory.csv',index=False)
    g2.dump_json(output/'preflight.json',report)
    text=f'''<!doctype html><html lang="en"><meta charset="utf-8"><title>G2 session preflight {day}</title>
<style>body{{font:16px system-ui;margin:40px;max-width:1000px;line-height:1.6}}table{{border-collapse:collapse}}td,th{{padding:8px;border:1px solid #ccc}}</style>
<h1>{day}: session has not started</h1><p>Checked at {html.escape(now.isoformat())}. This is a preflight evidence report, not the requested completed movement/backtest analysis.</p>
<p>The dated universe for {day} is absent. The prior dated snapshot contains {len(stocks)} stock rows and {len(universe)-len(stocks)} index contracts. Those stocks are reference inventory only; they have not been assumed to be today's universe.</p>
<p>No target-date backtest, scanner, confirmation, signal or order artifacts were found. Stocks are not yet evaluated. Counts of gainers, decliners, small losses, returns and selection failures are unavailable, not zero.</p>
<p>Stored prior closes have not been authenticated as official exchange closing prices. A last intraday candle close must not be silently substituted for the official reference.</p>
<p>Standard frozen G2 and its sealed source bundle were read and verified. Stops start at1.25% and tighten to1.00% after120minutes. Standard G2 keeps the original selection rules; the optional relaxed09:25LONG variant in earlier reports is separate.</p>
<p>Baseline source files are unchanged. No rule changes or historical counterfactuals were run because October8 entry opportunities do not exist yet.</p>
<h2>Reference storage inventory</h2>{pd.DataFrame(report['storage_inventory']).to_html(index=False,border=0)}
<p><a href="preflight.json">Exact sources and missing evidence</a> · <a href="previous_universe_storage_inventory.csv">Per-symbol source inventory</a></p></html>'''
    (output/'preflight_report.html').write_text(text,encoding='utf-8')
    print(json.dumps({k:report[k] for k in ('status','checked_at_ist','previous_universe_reference','storage_inventory','requested_rows_seen','latest_available_run_day')},indent=2),flush=True)
    print('REPORT: '+str(output),flush=True)


if __name__=='__main__':
    main()
