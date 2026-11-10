"""Read-only native Kite minute history for the advanced estimated-CVD study.

All files are isolated in this new package. Native historical candle starts
become completed ``minute_end`` by adding one minute. No missing bar is filled.
"""
from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import argparse
import contextlib
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = Path(__file__).resolve().parent
PRIOR = PACKAGE.parent / 'cvd_kite_proxy_20261102/kite_data_20261102'
IST = 'Asia/Kolkata'
FIELDS = ['open', 'high', 'low', 'close', 'volume']
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from g3_v18_20261102.cvd_kite_proxy_20261102.data_fetch import make_client, resolve_token, windows


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def checked(path: Path) -> Path:
    path = path.resolve()
    if path == PACKAGE or not path.is_relative_to(PACKAGE):
        raise ValueError('Output must be a child of the new advanced-CVD package')
    return path


def dump(path: Path, value: dict) -> None:
    checked(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False), encoding='utf-8')


def cutoff_ist(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError('Cutoff must include timezone')
    return stamp.tz_convert(IST)


def normalize(rows: list[dict], symbol: str, cutoff) -> pd.DataFrame:
    cutoff = cutoff_ist(cutoff)
    columns = ['symbol', 'minute_end', *FIELDS]
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows)
    if not {'date', *FIELDS}.issubset(frame):
        raise ValueError('Provider candle schema is incomplete')
    if any(pd.Timestamp(value).tzinfo is None for value in frame.date):
        raise ValueError('Provider timestamp is naive')
    frame['minute_end'] = pd.to_datetime(frame.date, utc=True).dt.tz_convert(IST) + pd.Timedelta(minutes=1)
    clock = frame.minute_end.dt.hour * 60 + frame.minute_end.dt.minute
    frame = frame.loc[(clock >= 556) & (clock <= 930) & frame.minute_end.le(cutoff)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    if (frame.minute_end.dt.second.ne(0) | frame.minute_end.dt.microsecond.ne(0)).any():
        raise ValueError('Provider timestamps are off the minute grid')
    if frame.minute_end.duplicated().any():
        raise ValueError('Provider timestamps are duplicated')
    values = frame[FIELDS].apply(pd.to_numeric, errors='coerce')
    if not np.isfinite(values.to_numpy(float)).all():
        raise ValueError('Nonfinite provider candle value')
    if (values[['open', 'high', 'low', 'close']] <= 0).any().any() or (values.volume < 0).any():
        raise ValueError('Invalid provider candle prices or volume')
    if ((values.high < values[['open', 'close', 'low']].max(axis=1)) |
        (values.low > values[['open', 'close', 'high']].min(axis=1))).any():
        raise ValueError('Inconsistent provider OHLC')
    frame[FIELDS] = values
    frame['symbol'] = symbol
    return frame[columns].sort_values('minute_end').reset_index(drop=True)


def aggregate_5m(minutes: pd.DataFrame) -> pd.DataFrame:
    """Keep partial groups visible and marked; do not manufacture absent minutes."""
    required = {'symbol', 'minute_end', *FIELDS}
    if not required.issubset(minutes):
        raise ValueError('Canonical minute fields are missing')
    if minutes.duplicated(['symbol', 'minute_end']).any():
        raise ValueError('Duplicate canonical minute identity')
    frame = minutes[['symbol', 'minute_end', *FIELDS]].copy().sort_values(['symbol', 'minute_end'])
    frame['bar_end'] = frame.minute_end.dt.ceil('5min')
    bars = frame.groupby(['symbol', 'bar_end'], sort=False, observed=True).agg(
        open=('open', 'first'), high=('high', 'max'), low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum'), source_1m_count=('minute_end', 'size'),
        first_minute_end=('minute_end', 'min'), last_minute_end=('minute_end', 'max')).reset_index()
    bars['bar_contiguous'] = (bars.source_1m_count.eq(5) & bars.last_minute_end.eq(bars.bar_end) &
        bars.first_minute_end.eq(bars.bar_end - pd.Timedelta(minutes=4)))
    bars['session_date'] = bars.bar_end.dt.strftime('%Y-%m-%d')
    return bars


def reconcile(bars: pd.DataFrame, reference: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare overlap only; new later candles are outside the old snapshot."""
    if bars.duplicated(['symbol', 'bar_end']).any() or reference.duplicated(['symbol', 'bar_end']).any():
        raise ValueError('Duplicate five-minute reconciliation identity')
    check = bars.merge(reference[['symbol', 'bar_end', *FIELDS]], on=['symbol', 'bar_end'],
        how='outer', validate='one_to_one', suffixes=('_minute', '_native'), indicator=True)
    check['session_date'] = check.bar_end.dt.strftime('%Y-%m-%d')
    check['both_sources'] = check._merge.eq('both')
    check['missing_minute_group'] = check._merge.eq('right_only')
    check['outside_reference_snapshot'] = check._merge.eq('left_only')
    match = check.both_sources & check.bar_contiguous.fillna(False).astype(bool)
    for field in FIELDS:
        tolerance = .5 if field == 'volume' else .005
        a, b = check[field+'_minute'], check[field+'_native']
        check[field+'_absolute_difference'] = (a-b).abs()
        check[field+'_mismatch'] = check.both_sources & ~np.isclose(a, b, atol=tolerance, rtol=1e-8)
        match &= ~check[field+'_mismatch']
    check['ohlcv_and_minutes_match'] = match
    summary = check.groupby('session_date').agg(
        reference_bars=('missing_minute_group', lambda x: int((~check.loc[x.index,'outside_reference_snapshot']).sum())),
        matched_groups=('both_sources', 'sum'), missing_minute_groups=('missing_minute_group', 'sum'),
        later_or_extra_minute_groups=('outside_reference_snapshot', 'sum'),
        ohlcv_and_minutes_match=('ohlcv_and_minutes_match', 'sum'),
        **{field+'_mismatch_groups': (field+'_mismatch','sum') for field in FIELDS},
        **{field+'_max_absolute_difference': (field+'_absolute_difference','max') for field in FIELDS}).reset_index()
    status = check.loc[~check.missing_minute_group,['symbol','bar_end','both_sources','ohlcv_and_minutes_match']].rename(
        columns={'both_sources':'native5m_reference_available','ohlcv_and_minutes_match':'native5m_reference_match'})
    return status, summary


class HistoricalReader:
    def __init__(self, kite, pause: float = .70):
        if pause < .5:
            raise ValueError('At most two historical requests per second are allowed')
        self.kite, self.pause, self.last = kite, pause, 0.

    def fetch(self, token, start, end):
        for attempt in range(3):
            time.sleep(max(0., self.pause - (time.monotonic()-self.last)))
            self.last = time.monotonic()
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    return self.kite.historical_data(token, start.to_pydatetime(), end.to_pydatetime(),
                        'minute', continuous=False, oi=False)
            except Exception as exc:
                if type(exc).__name__ in ('TokenException', 'PermissionException'):
                    raise RuntimeError(f'Kite authentication/permission failure: {type(exc).__name__}') from None
                if attempt == 2:
                    raise RuntimeError(f'Kite historical request failed: {type(exc).__name__}') from None
                time.sleep(3. * (attempt+1))
        raise AssertionError('Unreachable')


def run(output: Path, start='2026-07-29', through='2026-11-02') -> dict:
    from g3_v18_20261102.run import guard_writes
    guard_writes(PACKAGE)
    output = checked(output)
    output.mkdir(parents=True, exist_ok=True)
    source_manifest_path = PRIOR/'fetch_manifest.json'
    old = json.loads(source_manifest_path.read_text(encoding='utf-8'))
    source_bars, source_members = PRIOR/'bars_5m.parquet', PRIOR/'universe_membership.parquet'
    if digest(source_bars) != old['bars_sha256'] or digest(source_members) != old['membership_sha256']:
        raise ValueError('Immutable prior five-minute source hashes changed')
    member = pd.read_parquet(source_members)
    member = member.loc[member.session_date.between(start, through)].copy()
    symbols = sorted(member.symbol.unique())
    historic_tokens = {}
    for item in old['records']:
        if item.get('status') == 'FETCHED':
            historic_tokens.setdefault(item['symbol'], set()).add(int(item['instrument_token']))
    plan_path = output/'fetch_plan.json'
    if plan_path.exists():
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
        if plan['start'] != start or plan['through'] != through or plan['symbols'] != symbols:
            raise ValueError('Existing plan differs; use a fresh advanced-data output')
        if plan['reference_bars_sha256'] != old['bars_sha256']:
            raise ValueError('Reference snapshot changed during resume')
        cutoff = cutoff_ist(plan['completed_minute_cutoff_ist'])
    else:
        captured = pd.Timestamp.now(tz=IST)
        cutoff = min((captured-pd.Timedelta(seconds=5)).floor('min'), pd.Timestamp(f'{through} 15:30',tz=IST))
        plan = dict(start=start, through=through, symbols=symbols, captured_at_ist=str(captured),
            completed_minute_cutoff_ist=str(cutoff), interval='minute', chunk_calendar_days=60,
            requests_planned=len(symbols)*len(windows(date.fromisoformat(start),cutoff.date())),
            source='KITE_HISTORICAL_NATIVE_MINUTE', timestamp_policy='Kite candle start plus one minute',
            reference_manifest=str(source_manifest_path), reference_manifest_sha256=digest(source_manifest_path),
            reference_bars_path=str(source_bars), reference_bars_sha256=old['bars_sha256'],
            reference_membership_path=str(source_members), reference_membership_sha256=old['membership_sha256'],
            scope='BACKTEST_RESEARCH_ONLY', execution_authority=False,
            proxy_warning='Minute OHLCV estimates aggressor pressure; actual buyer/seller traded volume is unavailable')
        dump(plan_path,plan)
    member.to_parquet(output/'universe_membership.parquet',index=False)
    kite = make_client()
    try:
        with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
            instruments = kite.instruments('NSE')
    except Exception as exc:
        raise RuntimeError(f'Kite instrument lookup failed: {type(exc).__name__}') from None
    tokens = {str(r['tradingsymbol']):int(r['instrument_token']) for r in instruments
              if r.get('segment')=='NSE' and r.get('instrument_type')=='EQ'}
    reader = HistoricalReader(kite)
    chunks = output/'native_1m_chunks'
    chunks.mkdir(exist_ok=True)
    records,pieces=[],[]
    request_windows=windows(date.fromisoformat(start),cutoff.date())
    print(f'MINUTE_FETCH symbols={len(symbols)} requests={plan["requests_planned"]} cutoff={cutoff}',flush=True)
    for number,symbol in enumerate(symbols,1):
        token,provider_symbol,mapping_basis=resolve_token(symbol,tokens,historic_tokens.get(symbol,set()))
        if token is None:
            records.append(dict(symbol=symbol,status=mapping_basis))
            continue
        for left,right in request_windows:
            query_start=pd.Timestamp(f'{left} 09:15',tz=IST)
            query_end=min(pd.Timestamp(f'{right} 15:30',tz=IST),cutoff)
            path=chunks/f'{symbol}_{left}_{right}.parquet'
            receipt=path.with_suffix('.json')
            rec=dict(symbol=symbol,provider_symbol=provider_symbol,instrument_token=token,
                identity_mapping_basis=mapping_basis,provider='KITE_HISTORICAL_NATIVE_MINUTE',
                request_start_ist=str(query_start),request_end_ist=str(query_end),path=str(path))
            try:
                if path.exists() and receipt.exists():
                    cached=json.loads(receipt.read_text(encoding='utf-8'))
                    if cached['sha256']!=digest(path) or cached['request_end_ist']!=str(query_end) or cached['instrument_token']!=token:
                        raise ValueError('Cached minute receipt differs from frozen plan')
                    frame=pd.read_parquet(path)
                    rec.update(cached)
                    rec['cache_reused']=True
                else:
                    rows=reader.fetch(token,query_start,query_end)
                    frame=normalize(rows,symbol,cutoff)
                    frame=frame.loc[frame.minute_end.ge(query_start+pd.Timedelta(minutes=1)) & frame.minute_end.le(query_end)].copy()
                    frame.to_parquet(path,index=False)
                    rec.update(status='FETCHED',rows=len(frame),sha256=digest(path),cache_reused=False,
                        acquired_at_ist=str(pd.Timestamp.now(tz=IST)))
                    dump(receipt,rec)
                pieces.append(frame)
            except Exception as exc:
                rec.update(status='FAILED',error_type=type(exc).__name__)
                if isinstance(exc,RuntimeError):
                    rec['reason']=str(exc)
                if 'authentication/permission' in str(exc):
                    records.append(rec)
                    dump(output/'fetch_status.json',dict(operation_finished=False,records=records))
                    raise
            records.append(rec)
        if number%10==0 or number==len(symbols):
            print(f'MINUTE_FETCH {number}/{len(symbols)} chunks_ok={sum(r.get("status")=="FETCHED" for r in records)} failed={sum(r.get("status")!="FETCHED" for r in records)}',flush=True)
            dump(output/'fetch_status.json',dict(operation_finished=False,records=records))
    if not pieces:
        raise ValueError('No valid native minute history received')
    minutes=pd.concat(pieces,ignore_index=True).sort_values(['symbol','minute_end']).reset_index(drop=True)
    del pieces
    if minutes.duplicated(['symbol','minute_end']).any():
        raise ValueError('Duplicate consolidated minute identity')
    minutes['session_date']=minutes.minute_end.dt.strftime('%Y-%m-%d')
    minutes['provider']='KITE_HISTORICAL_NATIVE_MINUTE'
    minutes.to_parquet(output/'minutes_1m.parquet',index=False)
    print(f'MINUTE_FETCH canonical_saved rows={len(minutes)}; aggregating and reconciling',flush=True)
    bars=aggregate_5m(minutes)
    reference=pd.read_parquet(source_bars)
    statuses,reconciliation=reconcile(bars,reference)
    bars=bars.merge(statuses,on=['symbol','bar_end'],how='left',validate='one_to_one')
    bars.to_parquet(output/'aggregate_5m.parquet',index=False)
    reconciliation.to_csv(output/'reconciliation_5m.csv',index=False)
    coverage=[]
    groups={key:frame.minute_end for key,frame in minutes.groupby(['session_date','symbol'],sort=False)}
    observed_dates=set(minutes.session_date.unique())
    for row in member.itertuples(index=False):
        end=min(pd.Timestamp(f'{row.session_date} 15:30',tz=IST),cutoff)
        expected=pd.date_range(pd.Timestamp(f'{row.session_date} 09:16',tz=IST),end,freq='min')
        actual=pd.DatetimeIndex(groups.get((row.session_date,row.symbol),[]))
        missing=expected.difference(actual)
        strategy=expected[expected<=pd.Timestamp(f'{row.session_date} 13:30',tz=IST)]
        strategy_missing=strategy.difference(actual)
        coverage.append(dict(session_date=row.session_date,symbol=row.symbol,universe_basis=row.universe_basis,
            expected_minutes=len(expected),observed_minutes=len(actual),missing_minutes=len(missing),
            complete=not len(missing),first_missing=str(missing[0]) if len(missing) else None,
            expected_strategy_minutes=len(strategy),missing_strategy_minutes=len(strategy_missing),
            strategy_window_complete=not len(strategy_missing),any_symbol_observed_on_date=row.session_date in observed_dates))
    cov=pd.DataFrame(coverage)
    cov.to_csv(output/'coverage.csv',index=False)
    aliases=list({r['symbol']:dict(symbol=r['symbol'],provider_symbol=r['provider_symbol'],instrument_token=r['instrument_token'],
        mapping_basis=r['identity_mapping_basis']) for r in records if r.get('provider_symbol') and r['symbol']!=r['provider_symbol']}.values())
    mismatch=sum(int(reconciliation[field+'_mismatch_groups'].sum()) for field in FIELDS)
    summary=dict(operation_finished=True,all_requests_succeeded=all(r.get('status')=='FETCHED' for r in records),
        symbols_requested=len(symbols),symbols_observed=int(minutes.symbol.nunique()),minute_rows=len(minutes),
        first_minute_end_ist=str(minutes.minute_end.min()),last_minute_end_ist=str(minutes.minute_end.max()),
        completed_minute_cutoff_ist=str(cutoff),requests_planned=plan['requests_planned'],
        failed_chunks=sum(r.get('status')!='FETCHED' for r in records),aggregate_bars=len(bars),
        complete_five_minute_groups=int(bars.bar_contiguous.sum()),partial_five_minute_groups=int((~bars.bar_contiguous).sum()),
        overlapping_native5m_groups=int(reconciliation.matched_groups.sum()),
        absent_groups_from_native5m_snapshot=int(reconciliation.missing_minute_groups.sum()),
        overlapping_field_mismatches=mismatch,
        member_stock_days=len(cov),complete_member_stock_days=int(cov.complete.sum()),
        missing_member_minutes=int(cov.missing_minutes.sum()),
        strategy_complete_stock_days=int(cov.strategy_window_complete.sum()),
        missing_strategy_minutes=int(cov.missing_strategy_minutes.sum()),
        membership_dates_without_provider_minutes=sorted(set(member.session_date)-observed_dates),aliases=aliases,
        minutes_path=str(output/'minutes_1m.parquet'),minutes_sha256=digest(output/'minutes_1m.parquet'),
        bars_path=str(output/'aggregate_5m.parquet'),bars_sha256=digest(output/'aggregate_5m.parquet'),
        membership_path=str(output/'universe_membership.parquet'),membership_sha256=digest(output/'universe_membership.parquet'),
        reference_manifest=str(source_manifest_path),reference_manifest_sha256=digest(source_manifest_path),
        scope='RETROSPECTIVE_RESEARCH_ONLY',execution_authority=False,
        historical_universe_caveat='Dated or archived G3 cohorts as declared by prior snapshot; early archived cohorts are not fully point-in-time.',
        records=records)
    dump(output/'fetch_manifest.json',summary)
    dump(output/'fetch_status.json',{k:v for k,v in summary.items() if k!='records'})
    print(json.dumps({k:v for k,v in summary.items() if k!='records'},indent=2),flush=True)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=PACKAGE/'data_20261102')
    parser.add_argument('--start',default='2026-07-29')
    parser.add_argument('--through',default='2026-11-02')
    args=parser.parse_args()
    run(args.output,args.start,args.through)
