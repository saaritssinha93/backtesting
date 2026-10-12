"""Read-only historical-data recovery for the two omitted G3 candidate pools."""
from __future__ import annotations
import hashlib
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import pandas as pd
import fno_oi_common as common
import fno_v13_v10_g_3_contract as contract

DAY = date(2026, 10, 12)
BASE = Path(r'C:\TradingData\eqidv2\fno_oi')
OUT = BASE / 'strategy_research/v13_v10_g_3_missing_slots' / ('run_' + datetime.now(contract.IST).strftime('%Y%m%dT%H%M%S'))

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def write(path, data):
    path.write_text(json.dumps(data, indent=2, default=str, allow_nan=False) + '\n', encoding='utf-8')

def main():
    (OUT / 'source_scanner').mkdir(parents=True)
    (OUT / 'minutes').mkdir()
    (OUT / 'broker_responses').mkdir()
    sources, candidates = [], []
    for slot in ('0925', '0930'):
        path = BASE / 'v13_v10_g_3_live/scanner_5m' / DAY.isoformat() / f'slot_{slot}.json'
        original = path.read_bytes()
        copy = OUT / 'source_scanner' / path.name
        copy.write_bytes(original)
        packet = json.loads(original)
        assert packet['session_date'] == DAY.isoformat()
        assert packet['strategy_fingerprint'] == contract.strategy_fingerprint()
        sources.append(dict(path=str(path), snapshot=str(copy), sha256=sha(copy)))
        recovered = []
        for row in packet['feature_evaluations']:
            side = contract.base_signal_side(row, signal_end=packet['signal_end'], session_date=DAY,
                nifty_first_bar_return_pct=packet.get('index_context', {}).get('nifty_first_bar_return_pct'))
            recorded = bool(row.get('base_long_pass') or row.get('base_short_pass'))
            assert bool(side) == recorded, (slot, row['tradingsymbol'])
            if side:
                recovered.append({**row, 'side': side, 'signal_timestamp': row['signal_ts'],
                    'signal_close': row['close'], 'source_scanner_sha256': sha(copy)})
        candidates.extend(recovered)
        print(json.dumps(dict(slot=slot, recovered_base_candidates=len(recovered))), flush=True)
    assert len(candidates) == 20
    universe_path = common.UNIVERSE_DIR / f'near_month_{DAY.isoformat()}.parquet'
    assert universe_path.exists(), 'Dated universe required'
    universe = pd.read_parquet(universe_path)
    universe_copy = OUT / 'dated_universe.parquet'
    universe_copy.write_bytes(universe_path.read_bytes())
    sources.append(dict(path=str(universe_path), snapshot=str(universe_copy), sha256=sha(universe_copy)))
    write(OUT / 'base_candidates.json', candidates)
    now = datetime.now(contract.IST)
    cutoff = pd.Timestamp(now).floor('min') - pd.Timedelta(minutes=1)
    start = pd.Timestamp(datetime.combine(DAY - timedelta(days=7), datetime.min.time()), tz=contract.IST) + pd.Timedelta(hours=9, minutes=15)
    client = common.make_kite_client(common.discover_kite_credentials(max_apps=1)[0])
    results = []
    for symbol in sorted({r['tradingsymbol'] for r in candidates}):
        rows = universe.loc[universe.equity_symbol.eq(symbol)]
        assert len(rows) == 1, symbol
        token = int(rows.iloc[0].equity_instrument_token)
        record = dict(symbol=symbol, instrument_token=token, requested_from=start.isoformat(),
            completed_through=cutoff.isoformat(), source='KITE_HISTORICAL_MINUTE',
            fetched_retrospectively=True, available_in_original_live_confirmation=False)
        for attempt in range(3):
            try:
                raw = client.historical_data(token, start.to_pydatetime(), cutoff.to_pydatetime(),
                    'minute', continuous=False, oi=False)
                if not raw:
                    raise ValueError('No candles returned')
                raw_path = OUT / 'broker_responses' / f'{symbol}.json'
                write(raw_path, raw)
                frame = pd.DataFrame(raw)
                frame['candle_start'] = pd.to_datetime(frame['date'], utc=True).dt.tz_convert(contract.IST)
                frame['ts'] = frame.candle_start + pd.Timedelta(minutes=1)
                frame = frame.loc[frame.ts.le(cutoff) & frame.ts.dt.strftime('%H:%M').between('09:16', '15:30')].copy()
                assert not frame.ts.duplicated().any(), 'Duplicate broker minute'
                for field in ('open', 'high', 'low', 'close', 'volume'):
                    frame[field] = pd.to_numeric(frame[field], errors='raise')
                assert frame[['open','high','low','close']].gt(0).all().all()
                assert frame.volume.ge(0).all()
                assert (frame.high.ge(frame[['open','close']].max(axis=1)) & frame.low.le(frame[['open','close']].min(axis=1))).all()
                frame['source'] = 'KITE_HISTORICAL_MINUTE_RETROSPECTIVE'
                frame['fetched_at_ist'] = datetime.now(contract.IST).isoformat()
                frame.sort_values('ts', inplace=True)
                path = OUT / 'minutes' / f'{symbol}.parquet'
                frame.to_parquet(path, index=False)
                today = frame.loc[frame.ts.dt.date.eq(DAY)]
                record.update(state='FETCHED', rows=len(frame), today_rows=len(today),
                    first_today=today.ts.min().isoformat(), last_today=today.ts.max().isoformat(),
                    parquet=str(path), parquet_sha256=sha(path), raw_response=str(raw_path),
                    raw_response_sha256=sha(raw_path))
                break
            except Exception as exc:
                record.update(state='FAILED', error_type=type(exc).__name__)
                if attempt < 2:
                    time.sleep(1)
        results.append(record)
        write(OUT / 'recovery_manifest.json', dict(session_date=DAY.isoformat(),
            run_dir=str(OUT), execution_authority=False, role='COUNTERFACTUAL_MISSING_SLOT_RESEARCH',
            strategy_fingerprint=contract.strategy_fingerprint(), as_of_ist=cutoff.isoformat(),
            original_sources=sources, minute_sources=results,
            original_scanner_records_unchanged=all(sha(Path(s['path']))==s['sha256'] for s in sources)))
        print(json.dumps({k:record.get(k) for k in ('symbol','state','today_rows','last_today','error_type')}), flush=True)
        time.sleep(.55)
    write(Path(__file__).with_name('latest_run.json'), {'run_dir': str(OUT)})
    print('RUN_DIR=' + str(OUT), flush=True)
    if any(r['state']!='FETCHED' for r in results):
        raise SystemExit(2)

if __name__ == '__main__':
    main()
