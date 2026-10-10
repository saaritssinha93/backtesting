"""Isolated full-session movement/coverage audit. Writes only requested research output.

Native parquet libraries are needed for the trading stores. CSV/JSON are analytical
intermediates, not an authored workbook. No strategy or store files are modified.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
import re
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

IST = 'Asia/Kolkata'
DATA = Path(r'C:\TradingData\eqidv2')
FLAGS = ['gap_filled', 'opening_snapshot', 'provisional_stale']
INDEX = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50', 'NIFTYFPI'}


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def dump(path, payload):
    def clean(v):
        if isinstance(v, dict): return {k: clean(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)): return [clean(x) for x in v]
        if isinstance(v, np.generic): return clean(v.item())
        if isinstance(v, float) and not np.isfinite(v): return None
        if isinstance(v, (pd.Timestamp, datetime, date, Path)): return str(v)
        return v
    path.write_text(json.dumps(clean(payload), indent=2, allow_nan=False), encoding='utf-8')


def bhav(day, out):
    name = f'BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip'
    url = 'https://nsearchives.nseindia.com/content/cm/' + name
    dest = out / name
    meta = dict(url=url, path=str(dest), requested_day=str(day))
    try:
        if not dest.exists():
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=40) as r:
                payload = r.read()
                meta.update(http_status=r.status, http_last_modified=r.headers.get('Last-Modified'))
            with zipfile.ZipFile(io.BytesIO(payload)) as z:
                assert len(z.namelist()) == 1
                member = z.namelist()[0]
                csvdata = z.read(member)
            frame = pd.read_csv(io.BytesIO(csvdata))
            assert set(frame.TradDt) == {str(day)}, 'Incorrect official session date'
            dest.write_bytes(payload)
            (out / Path(member).name).write_bytes(csvdata)
        else:
            with zipfile.ZipFile(dest) as z:
                frame = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])))
        frame = frame.loc[frame.SctySrs.eq('EQ')].copy()
        assert frame.TckrSymb.is_unique
        assert set(frame.TradDt) == {str(day)}
        meta.update(status='VERIFIED_OFFICIAL_FINAL_FILE', sha256=sha(dest), fetched_at_ist=str(pd.Timestamp.now(tz=IST)), rows=len(frame))
        return frame.set_index('TckrSymb'), meta
    except Exception as exc:
        meta.update(status='UNAVAILABLE', error=f'{type(exc).__name__}: {exc}')
        return pd.DataFrame(), meta


def cas_archive(day, out):
    page='https://www.nseindia.com/static/reports/closing-auction-session-historical-data'
    meta={'listing_url':page,'session_date':str(day)}
    try:
        req=urllib.request.Request(page,headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req,timeout=30) as response: html=response.read().decode('utf-8')
        links=re.findall(r'href="([^"]+Closing-Auction-Session-'+day.strftime('%d-%b-%Y')+r'_[^"]+\.csv)"',html)
        assert len(links)==1, 'Dated official CAS archive link unavailable or ambiguous'
        url=links[0]; dest=out/url.rsplit('/',1)[1]
        if not dest.exists():
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req,timeout=30) as response: payload=response.read()
            dest.write_bytes(payload)
        frame=pd.read_csv(dest)
        frame['SYMBOL']=frame.SYMBOL.str.strip()
        assert frame.SYMBOL.is_unique
        for field in ['FINAL PRICE','FINAL VOLUME','REFERENCE PRICE']:
            frame[field]=pd.to_numeric(frame[field].astype(str).str.replace(',','',regex=False),errors='coerce')
        meta.update(status='VERIFIED_OFFICIAL_CAS_ARCHIVE',url=url,path=str(dest),sha256=sha(dest),rows=len(frame),fetched_at_ist=str(pd.Timestamp.now(tz=IST)))
        return frame.set_index('SYMBOL'),meta
    except Exception as exc:
        meta.update(status='UNAVAILABLE',error=f'{type(exc).__name__}: {exc}')
        return pd.DataFrame(),meta


def read_day(path, day):
    if not path.exists(): return pd.DataFrame(), {'exists': False, 'path': str(path)}
    fields = pq.ParquetFile(path).schema.names
    cols = [c for c in ['date', 'open', 'high', 'low', 'close', 'volume', 'Prev_Day_Close', 'source_1m_count', *FLAGS] if c in fields]
    frame = pd.read_parquet(path, columns=cols)
    ts = pd.to_datetime(frame.date, errors='coerce', utc=True).dt.tz_convert(IST)
    frame = frame.loc[ts.dt.date.eq(day)].copy()
    frame['ts'] = ts.loc[frame.index]
    meta = dict(exists=True, path=str(path), sha256=sha(path), bytes=path.stat().st_size,
                modified_at_ist=str(pd.Timestamp(path.stat().st_mtime, unit='s', tz='UTC').tz_convert(IST)), flags_available=[c for c in FLAGS if c in cols])
    return frame.sort_values('ts').reset_index(drop=True), meta


def flag(frame, field):
    return frame[field].astype(str).str.lower().isin(['true', '1', '1.0', 'yes']) if field in frame else pd.Series(False, index=frame.index)


def validate(frame, day, freq):
    # NSE Closing Auction Session securities (the FnO-underlying stock universe)
    # end continuous trading at 15:15. Later padded candles are not CTS gaps.
    expected = pd.date_range(f'{day} 09:{16 if freq == 1 else 20}', f'{day} 15:15', freq=f'{freq}min', tz=IST)
    if frame.empty:
        return frame.copy(), dict(rows_session=0, expected_bars=len(expected), present_expected_bars=0, valid_bars=0,
            missing_timestamp_count=len(expected), unusable_expected_count=0, absent_or_unusable_count=len(expected),
            coverage_pct=0., missing_timestamps=';'.join(expected.strftime('%H:%M')), invalid_ohlcv_count=0,
            duplicate_timestamp_rows=0, zero_volume_count=0, negative_volume_count=0, quality_flagged_count=0,
            gap_filled_count=0, provisional_stale_count=0, opening_snapshot_count=0, first_valid_time=None, last_valid_time=None)
    nums = frame[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')
    numeric = np.isfinite(nums).all(axis=1)
    geometry = nums[['open', 'high', 'low', 'close']].gt(0).all(axis=1) & nums.high.ge(nums[['open', 'close', 'low']].max(axis=1)) & nums.low.le(nums[['open', 'close', 'high']].min(axis=1))
    duplicate = frame.ts.duplicated(keep=False)
    quality = pd.Series(False, index=frame.index)
    for field in FLAGS: quality |= flag(frame, field)
    insession = frame.ts.isin(expected)
    valid = insession & numeric & geometry & nums.volume.gt(0) & ~duplicate & ~quality
    good = frame.loc[valid].copy()
    present = pd.DatetimeIndex(frame.loc[insession, 'ts'].unique())
    stats = dict(rows_session=len(frame), expected_bars=len(expected), present_expected_bars=len(present), valid_bars=len(good),
        missing_timestamp_count=len(expected.difference(present)), unusable_expected_count=len(present) - len(good),
        absent_or_unusable_count=len(expected) - len(good), coverage_pct=100 * len(good)/len(expected),
        missing_timestamps=';'.join(expected.difference(present).strftime('%H:%M')),
        invalid_ohlcv_count=int((insession & ~(numeric & geometry)).sum()), duplicate_timestamp_rows=int((insession & duplicate).sum()),
        zero_volume_count=int((insession & nums.volume.eq(0)).sum()), negative_volume_count=int((insession & nums.volume.lt(0)).sum()),
        quality_flagged_count=int((insession & quality).sum()),
        **{f'{c}_count':int((insession & flag(frame,c)).sum()) for c in FLAGS},
        opening_snapshot_rows_outside_expected=int((~insession & flag(frame,'opening_snapshot')).sum()),
        post_cts_rows=int(frame.ts.gt(pd.Timestamp(f'{day} 15:15',tz=IST)).sum()),
        post_cts_quality_flagged_rows=int((frame.ts.gt(pd.Timestamp(f'{day} 15:15',tz=IST)) & quality).sum()),
        first_valid_time=str(good.ts.min()) if len(good) else None, last_valid_time=str(good.ts.max()) if len(good) else None)
    return good, stats


def extrema(frame, pc, prefix):
    if frame.empty or not np.isfinite(pc) or pc <= 0: return {}
    hi, lo = float(frame.high.max()), float(frame.low.min())
    hits = frame.loc[np.isclose(frame.high, hi, rtol=0, atol=.00001), 'ts']
    lows = frame.loc[np.isclose(frame.low, lo, rtol=0, atol=.00001), 'ts']
    return {f'{prefix}high':hi, f'{prefix}low':lo, f'{prefix}peak_pct':100*(hi/pc-1), f'{prefix}trough_pct':100*(lo/pc-1),
        f'{prefix}peak_first_bar_end':str(hits.iloc[0]), f'{prefix}peak_last_bar_end':str(hits.iloc[-1]),
        f'{prefix}trough_first_bar_end':str(lows.iloc[0]), f'{prefix}trough_last_bar_end':str(lows.iloc[-1])}


def price_tolerance(price):
    """Allow float32 storage rounding, never a blanket one-paise/tick match."""
    return float(abs(np.spacing(np.float32(price))) * .51 + 1e-8)


def analyze(contract, day, snapshot, previous, today, cas):
    symbol = str(contract.equity_symbol)
    paths = {'frozen_1m': snapshot/'equity_1m'/f'{symbol}_stocks_indicators_1min.parquet',
        'current_1m': DATA/'stocks_indicators_1min_eq'/f'{symbol}_stocks_indicators_1min.parquet',
        'current_5m': DATA/'stocks_indicators_5min_eq_live2'/f'{symbol}_stocks_indicators_5min.parquet',
        'live_5m': DATA/'stocks_indicators_5min_eq_live'/f'{symbol}_stocks_indicators_5min.parquet'}
    frames, good, prov, row = {}, {}, [], dict(symbol=symbol, futures_symbol=str(contract.tradingsymbol))
    pc = float(previous.loc[symbol, 'ClsPric']) if symbol in previous.index else float('nan')
    for key, path in paths.items():
        frame, meta = read_day(path, day)
        frames[key] = frame
        valid, stats = validate(frame, day, 1 if '1m' in key else 5)
        good[key] = valid
        row.update({f'{key}_{k}':v for k,v in stats.items()})
        row.update(extrema(valid, pc, key+'_'))
        row[key+'_source'] = str(path)
        row[key+'_source_exists'] = bool(meta['exists'])
        prov.append(dict(symbol=symbol, role=key, **meta))
    row['frozen_current_1m_file_hash_match'] = prov[0].get('sha256') == prov[1].get('sha256')
    row.update(official_previous_close=pc, official_prev_date=str(previous.loc[symbol,'TradDt']) if symbol in previous.index else None,
               official_current_available=symbol in today.index)
    for key, frame in frames.items():
        if not frame.empty and 'Prev_Day_Close' in frame:
            vals=pd.to_numeric(frame.Prev_Day_Close,errors='coerce').dropna().unique()
            row[key+'_cached_previous_close_values']=';'.join(str(x) for x in vals)
            row[key+'_cached_previous_close_match']=bool(len(vals) and all(abs(float(v)-pc)<=price_tolerance(pc) for v in vals))
    if not np.isfinite(pc) or pc<=0:
        row['movement_status']='OFFICIAL_PREVIOUS_CLOSE_UNAVAILABLE'
        return row, prov
    # Observed bars include trustworthy full-session coverage wherever it exists.
    one = good['current_1m']
    five = good['current_5m']
    observed = [(float(f.high.max()), float(f.low.min())) for f in [one,five] if not f.empty]
    if observed:
        row.update(observed_high=max(a for a,b in observed), observed_low=min(b for a,b in observed))
        row.update(observed_peak_pct=100*(row['observed_high']/pc-1), observed_trough_pct=100*(row['observed_low']/pc-1))
    if symbol in today.index:
        daily = today.loc[symbol]
        row.update(official_high=float(daily.HghPric), official_low=float(daily.LwPric), official_close=float(daily.ClsPric),
            official_day_previous_close=float(daily.PrvsClsgPric), official_previous_close_matches=abs(pc-float(daily.PrvsClsgPric))<.00001,
            official_peak_pct=100*(float(daily.HghPric)/pc-1), official_trough_pct=100*(float(daily.LwPric)/pc-1),
            close_return_pct=100*(float(daily.ClsPric)/pc-1), ranking_basis='OFFICIAL_NSE_FULL_SESSION_HIGH_LOW',
            movement_status='OFFICIAL_DAILY_USABLE_CTS_PARTIAL' if len(one)<360 else 'OFFICIAL_DAILY_USABLE_CTS_COMPLETE')
        row.update(peak_pct=row['official_peak_pct'], trough_pct=row['official_trough_pct'])
        if symbol in cas.index:
            row.update(cas_final_price=float(cas.loc[symbol,'FINAL PRICE']),cas_final_volume=float(cas.loc[symbol,'FINAL VOLUME']),
                       cas_reference_price=float(cas.loc[symbol,'REFERENCE PRICE']))
            row['cas_final_matches_official_close']=bool(abs(row['cas_final_price']-row['official_close'])<.00001)
        for kind, field in [('peak','high'),('trough','low')]:
            target=row[f'official_{field}']
            observed_val=row.get(f'observed_{field}', float('nan'))
            row[f'official_{field}_matches_observed']=bool(abs(target-observed_val)<=price_tolerance(target))
            row[f'observed_{field}_minus_official']=observed_val-target
            hits=one.loc[np.isclose(one[field],target,rtol=0,atol=price_tolerance(target)),'ts'] if len(one) else pd.Series(dtype='object')
            hits5=five.loc[np.isclose(five[field],target,rtol=0,atol=.00001),'ts'] if len(five) else pd.Series(dtype='object')
            row[f'{kind}_time']=str(hits.iloc[0]) if len(hits) else None
            row[f'{kind}_last_time']=str(hits.iloc[-1]) if len(hits) else None
            row[f'{kind}_time_precision']='1_MINUTE_BAR_END' if len(hits) else '5_MINUTE_INTERVAL' if len(hits5) else 'UNAVAILABLE_OFFICIAL_DAILY_ONLY'
            row[f'{kind}_time_interval_start']=str(hits.iloc[0]-pd.Timedelta(minutes=1)) if len(hits) else str(hits5.iloc[0]-pd.Timedelta(minutes=5)) if len(hits5) else None
            row[f'{kind}_time_interval_end']=str(hits.iloc[0]) if len(hits) else str(hits5.iloc[0]) if len(hits5) else None
            row[f'{kind}_time_note']='First observed occurrence; missing bars may contain earlier or later repeats. Bar end is not exact trade time.' if len(hits) or len(hits5) else 'Official high/low not observed in valid intraday bars; time cannot be established.'
            if not len(hits) and not len(hits5) and row.get('cas_final_volume',0)>0 and abs(row.get('cas_final_price',float('nan'))-target)<.00001:
                row[f'{kind}_time_precision']='CLOSING_AUCTION_SESSION_ONLY'
                row[f'{kind}_time_interval_start']=str(pd.Timestamp(f'{day} 15:28',tz=IST))
                row[f'{kind}_time_interval_end']=str(pd.Timestamp(f'{day} 15:35',tz=IST))
                row[f'{kind}_time_note']='Official extreme equals executed CAS final price. CSV has no execution timestamp. 15:28-15:35 is a session-rule bound: matching starts after random order-entry closure in 15:28-15:30; not an observed timestamp.'
    else:
        row.update(movement_status='OFFICIAL_CURRENT_UNAVAILABLE', ranking_basis='PARTIAL_INTRADAY_ONLY',
                   peak_pct=row.get('observed_peak_pct'),trough_pct=row.get('observed_trough_pct'))
    return row, prov


def stats(series):
    series=pd.to_numeric(series, errors='coerce').dropna()
    return dict(count=len(series), mean=float(series.mean()) if len(series) else None,
                median=float(series.median()) if len(series) else None,
                minimum=float(series.min()) if len(series) else None, maximum=float(series.max()) if len(series) else None,
                sum_descriptive_percentage_points=float(series.sum()) if len(series) else None)


def run(day, run_dir, out):
    out.mkdir(parents=True,exist_ok=True)
    official=out/'official_sources'; official.mkdir(exist_ok=True)
    manifest=json.loads((run_dir/'source_manifest.json').read_text())
    snapshot=Path(manifest['input_snapshot']['root'])
    uni_path=snapshot/'universe'/f'near_month_{day}.parquet'
    raw=pd.read_parquet(uni_path)
    universe=raw.loc[~raw.underlying.isin(INDEX)].copy()
    assert universe.equity_symbol.notna().all() and universe.equity_symbol.is_unique
    universe.to_csv(out/'dated_stock_universe.csv',index=False)
    today, tsrc=bhav(day,official)
    cas, csrc=cas_archive(day,official)
    previous, psrc=pd.DataFrame(), None
    for delta in range(1,8):
        previous, psrc=bhav(day-timedelta(days=delta),official)
        if len(previous): break
    print('Official:',tsrc['status'],psrc['status'],'Universe:',len(universe),flush=True)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results=list(pool.map(lambda c:analyze(c,day,snapshot,previous,today,cas), universe.itertuples()))
    moves=pd.DataFrame([row for row,_ in results]).sort_values('symbol')
    moves.to_csv(out/'universe_movements.csv',index=False)
    usable=moves.loc[moves.official_previous_close.gt(0) & moves.official_current_available]
    assert len(usable)==len(universe), 'Missing official close/day records require explicit incomplete report'
    assert usable.official_high.ge(usable.official_close).all() and usable.official_low.le(usable.official_close).all()
    assert usable.observed_high.le(usable.official_high+.011).all() and usable.observed_low.ge(usable.official_low-.011).all()
    usable.loc[~usable.official_high_matches_observed | ~usable.official_low_matches_observed].to_csv(out/'official_intraday_extreme_reconciliation.csv',index=False)
    usable.loc[usable.current_1m_zero_volume_count.gt(0)].to_csv(out/'zero_volume_minute_symbols.csv',index=False)
    gain=usable.loc[usable.peak_pct.ge(2)].sort_values(['peak_pct','symbol'],ascending=[False,True])
    loss=usable.loc[usable.trough_pct.le(-2)].sort_values(['trough_pct','symbol'])
    for name,df in [('gainers_ge_2pct',gain),('decliners_le_minus2pct',loss),
        ('top10_long',usable.sort_values(['peak_pct','symbol'],ascending=[False,True]).head(10)),
        ('top10_short',usable.sort_values(['trough_pct','symbol']).head(10)),
        ('smaller_intraday_losses',usable.loc[usable.trough_pct.gt(-2)&usable.trough_pct.lt(0)]),
        ('smaller_closing_losses',usable.loc[usable.close_return_pct.gt(-2)&usable.close_return_pct.lt(0)])]:
        df.to_csv(out/f'{name}.csv',index=False)
    groups={'gainers_ge_2pct':gain,'decliners_le_minus2pct':loss,
        'observed_cts_gainers_ge_2pct':usable.loc[usable.observed_peak_pct.ge(2)],
        'observed_cts_decliners_le_minus2pct':usable.loc[usable.observed_trough_pct.le(-2)],
        'smaller_intraday_loss_gt_minus2_lt0':usable.loc[usable.trough_pct.gt(-2)&usable.trough_pct.lt(0)],
        'smaller_closing_loss_gt_minus2_lt0':usable.loc[usable.close_return_pct.gt(-2)&usable.close_return_pct.lt(0)],
        'closing_loss_le_minus2':usable.loc[usable.close_return_pct.le(-2)],
        'closing_gain_ge2':usable.loc[usable.close_return_pct.ge(2)],
        'both_peak_ge2_trough_le_minus2':usable.loc[usable.peak_pct.ge(2)&usable.trough_pct.le(-2)]}
    summary=dict(session_date=str(day),dated_universe_total_rows=len(raw),dated_stock_universe=len(universe),
        index_rows_excluded=len(raw)-len(universe),usable_official_daily_universe=len(usable),
        unusable_symbols=moves.loc[~moves.symbol.isin(usable.symbol),'symbol'].tolist(),
        denominator_definition='All dated stock symbols with official previous-session close and current-session official NSE EQ daily OHLC.',
        ranking_basis='Official NSE full-session high/low divided by previous trading day official close. Timing only from matching valid intraday bars.',
        official_previous_day=str(previous.TradDt.iloc[0]) if len(previous) else None,
        official_previous_close_mismatch_symbols=usable.loc[~usable.official_previous_close_matches,'symbol'].tolist(),
        cached_previous_close_mismatch_count=int((~usable.current_1m_cached_previous_close_match).sum()),
        all_current_1m_files_match_frozen=bool(usable.frozen_current_1m_file_hash_match.all()),
        cas_final_close_matched_count=int(usable.cas_final_matches_official_close.fillna(False).sum()) if 'cas_final_matches_official_close' in usable else 0,
        cas_timed_high_count=int(usable.peak_time_precision.eq('CLOSING_AUCTION_SESSION_ONLY').sum()),
        cas_timed_low_count=int(usable.trough_time_precision.eq('CLOSING_AUCTION_SESSION_ONLY').sum()),
        equal_weight_official_close_to_close_return_pct=float(usable.close_return_pct.mean()) if len(usable) else None,
        groups={k:dict(count=len(v),pct_of_usable=100*len(v)/len(usable) if len(usable) else None,
                      peak_move_pct=stats(v.peak_pct),trough_move_pct=stats(v.trough_pct),closing_return_pct=stats(v.close_return_pct)) for k,v in groups.items()},
        universe_peak_move_pct=stats(usable.peak_pct),universe_trough_move_pct=stats(usable.trough_pct),universe_closing_return_pct=stats(usable.close_return_pct),
        sum_caution='Sums of individual peak/trough percentages are descriptive opportunity measures in percentage points, not realizable portfolio returns.',
        session_finalization='Official final daily bhavcopy and CAS final-price archive available. CTS price-bar validity is reported separately. CAS exact execution timestamps are not supplied by the final-price archive.',
        session_clock={'security_scope':'NSE FnO-underlying equities subject to Closing Auction Session',
            'continuous_session':'09:15-15:15 IST','expected_end_labelled_1m_bars':360,'expected_end_labelled_5m_bars':72,
            'closing_auction_session':'15:15-15:35 IST','source':'https://www.nseindia.com/static/products-services/closing-auction-session',
            'post_1515_candle_policy':'Not counted as missing CTS bars. Gap-filled five-minute records are not valid auction execution bars.'},
        timing_caution='End-labelled candle timestamps localize an extreme to a one-minute or five-minute interval, not its exact trade time. Missing bars can conceal repeats.',
        validation_policy='CTS bars must have finite positive geometrically valid OHLC, finite positive volume, a unique expected timestamp and no asserted gap_filled/opening_snapshot/provisional_stale flag. Zero-volume minutes are excluded as untraded observations, not automatically classified as feed failures. Flag columns absent from historical 1m source cannot independently certify live point-in-time readiness.',
        price_matching_tolerance='Historical 1m prices are float32: allow 0.51 float32 ULP plus 1e-8. Five-minute/official/CAS comparisons use 1e-5. A nearby price one tick away is not an exact extreme match.',
        coverage={k:{'missing_files':int((~moves[k+'_source_exists']).sum()),'total_expected_bars':int(moves[k+'_expected_bars'].sum()),
            'total_valid_bars':int(moves[k+'_valid_bars'].sum()),'missing_timestamp_count':int(moves[k+'_missing_timestamp_count'].sum()),
            'unusable_expected_count':int(moves[k+'_unusable_expected_count'].sum()),
            'symbols_complete_valid':int(moves[k+'_valid_bars'].eq(moves[k+'_expected_bars']).sum()),
            'valid_bars_min':int(moves[k+'_valid_bars'].min()),'valid_bars_max':int(moves[k+'_valid_bars'].max()),
            'zero_volume_count':int(moves[k+'_zero_volume_count'].sum()),'quality_flagged_count':int(moves[k+'_quality_flagged_count'].sum()),
            'duplicate_timestamp_rows':int(moves[k+'_duplicate_timestamp_rows'].sum()),'invalid_ohlcv_count':int(moves[k+'_invalid_ohlcv_count'].sum())}
            for k in ['frozen_1m','current_1m','current_5m','live_5m']},
        official_high_not_matched_count=int((~usable.official_high_matches_observed).sum()),
        official_low_not_matched_count=int((~usable.official_low_matches_observed).sum()))
    dump(out/'summary.json',summary)
    dump(out/'provenance.json',dict(g_run=str(run_dir),snapshot=str(snapshot),snapshot_fingerprint=manifest['input_snapshot']['snapshot_fingerprint'],
        source_manifest_path=str(run_dir/'source_manifest.json'),source_manifest_sha256=sha(run_dir/'source_manifest.json'),
        universe_path=str(uni_path),universe_sha256=sha(uni_path),official_sources=[psrc,tsrc,csrc],
        analysis_script=str(Path(__file__).resolve()),analysis_script_sha256=sha(Path(__file__)),
        session_clock_source='https://www.nseindia.com/static/products-services/closing-auction-session',
        observed_at_ist=str(pd.Timestamp.now(tz=IST)),price_sources=[s for _,sources in results for s in sources]))
    print(json.dumps({k:summary[k] for k in ['dated_stock_universe','usable_official_daily_universe','equal_weight_official_close_to_close_return_pct','coverage']},indent=2),flush=True)


if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--day',required=True); ap.add_argument('--run',type=Path,required=True); ap.add_argument('--out',type=Path,required=True)
    args=ap.parse_args(); run(date.fromisoformat(args.day),args.run,args.out)
