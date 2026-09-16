"""Read-only Kite history backfill for an audited G-options missing-data plan.

Writes only this study's isolated data cache. No orders, account changes,
shared live statuses, credential outputs, or paid subscriptions are involved.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import pandas as pd
import fno_oi_common as common


def fetch(plan_path: Path, output_root: Path):
    plan = pd.read_csv(plan_path)
    credentials = common.discover_kite_credentials(max_apps=8)
    clients = [common.make_kite_client(x, timeout_sec=12.) for x in credentials]
    outcomes = []
    exhausted_auth = set()
    for index, row in enumerate(plan.to_dict('records'), 1):
        for interval, minutes, folder in [('minute',1,'raw_options_1m'),('5minute',5,'raw_options_5m')]:
            record = dict(option_symbol=row['option_symbol'],day=row['day'],interval=interval)
            data = None
            error_type = ''
            for client_index, client in enumerate(clients):
                if client_index in exhausted_auth:
                    continue
                time.sleep(.4)
                try:
                    data = client.historical_data(int(row['instrument_token']),
                        pd.Timestamp(row['from_date']).to_pydatetime(), pd.Timestamp(row['to_date']).to_pydatetime(),
                        interval, continuous=False, oi=True)
                    break
                except Exception as exc:
                    error_type = type(exc).__name__
                    if error_type == 'TokenException':
                        exhausted_auth.add(client_index)
                    # Only exception type is retained; broker errors can contain identifiers.
            if data:
                frame = pd.DataFrame(data).rename(columns={'date':'timestamp'})
                frame['timestamp'] = pd.to_datetime(frame.timestamp, utc=True).dt.tz_convert('Asia/Kolkata')
                frame['candle_start'] = frame.timestamp
                frame['candle_end'] = frame.timestamp + pd.Timedelta(minutes=minutes)
                frame['tradingsymbol'] = row['option_symbol']
                frame['instrument_token'] = int(row['instrument_token'])
                frame['underlying'] = str(row['underlying'])
                frame['expiry'] = pd.Timestamp(row['expiry']).normalize()
                frame['strike'] = float(row['strike'])
                frame['instrument_type'] = str(row['instrument_type'])
                frame['lot_size'] = int(row['lot_size'])
                frame['tick_size'] = float(row['tick_size'])
                frame['fetched_at'] = pd.Timestamp.now(tz='Asia/Kolkata')
                folder_path = output_root/folder
                folder_path.mkdir(parents=True,exist_ok=True)
                path = folder_path/f"{common.safe_contract_stem(row['option_symbol'])}_{'1minute' if minutes==1 else '5minute'}.parquet"
                if path.exists():
                    existing = pd.read_parquet(path)
                    if 'expiry' in existing:
                        existing['expiry'] = pd.to_datetime(existing['expiry'], errors='coerce')
                    frame = pd.concat([existing,frame],ignore_index=True)
                frame = frame.drop_duplicates('timestamp',keep='last').sort_values('timestamp')
                common.atomic_write_parquet(frame, path)
                record.update(state='WRITTEN',rows_returned=len(data),file=str(path.resolve()))
            else:
                record.update(state='FAILED' if data is None else 'NO_CANDLES',rows_returned=0,error_type=error_type)
            outcomes.append(record)
        print(f"[HISTORY] {index}/{len(plan)} {row['option_symbol']} {row['day']} {outcomes[-1]['state']}",flush=True)
        if len(exhausted_auth)==len(clients):
            print('[HISTORY] No authenticated history client is available; remaining data stays missing.',flush=True)
            break
    output_root.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(outcomes).to_csv(output_root/'fetch_outcomes.csv',index=False)
    (output_root/'fetch_manifest.json').write_text(json.dumps(dict(plan=str(plan_path.resolve()),
        intervals=['minute','5minute'], read_only_broker_calls=True, outcomes=outcomes),indent=2),encoding='utf-8')


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--output-root',type=Path,required=True)
    args=parser.parse_args()
    fetch(args.plan,args.output_root)
