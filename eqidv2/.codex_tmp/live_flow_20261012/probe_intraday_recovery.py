"""Bounded read-only broker probes; fetched bars go only to audit staging."""
import sys
import json
from pathlib import Path
import time
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_oi_common as common
from tools.audit_dashboard_flow_intraday import valid_rows, windows

OUT = Path('C:/TradingData/eqidv2/fno_oi/data_quality/intraday_20261012_verified/recovery_probe')
OUT.mkdir(parents=True, exist_ok=True)
client = common.make_kite_client(common.discover_kite_credentials(max_apps=1)[0])
universe = pd.read_parquet(common.UNIVERSE_DIR / 'latest_near_month.parquet')
plans = [('cash','RELIANCE','2026-10-09','15:10','15:30'),
         ('cash','360ONE','2026-08-03','15:10','15:30'),
         ('oi','ALKEM','2026-10-12','09:15','09:25'),
         ('oi','SAIL','2026-10-08','09:15','09:50'),
         ('oi','RELIANCE','2026-08-21','12:45','15:30'),
         ('oi','RELIANCE','2026-09-18','09:15','09:40')]
results = []
for kind,symbol,day,begin,end in plans:
    record = dict(kind=kind,symbol=symbol,day=day,request_start=begin,request_end=end)
    try:
        dated = common.UNIVERSE_DIR / f'near_month_{day}.parquet'
        mapping = pd.read_parquet(dated) if dated.exists() else universe
        row = mapping.loc[mapping.underlying.eq(symbol)].iloc[0]
        token = int(row.equity_instrument_token if kind=='cash' else row.instrument_token)
        record['contract'] = str(row.tradingsymbol) if kind=='oi' else symbol
        start,stop = [pd.Timestamp(f'{day} {value}',tz=common.IST).to_pydatetime() for value in (begin,end)]
        raw = client.historical_data(token,start,stop,'minute' if kind=='cash' else '5minute',continuous=False,oi=kind=='oi')
        frame = pd.DataFrame(raw)
        if not frame.empty:
            frame['ts'] = pd.to_datetime(frame.date,utc=True).dt.tz_convert(common.IST) + pd.Timedelta(minutes=1 if kind=='cash' else 5)
            if kind=='oi':
                frame['candle_start'] = frame.ts - pd.Timedelta(minutes=5)
            frame = frame.loc[frame.ts.le(stop)]
            good = valid_rows(frame,minute=kind=='cash',oi=kind=='oi')
            frame.to_parquet(OUT/f'{kind}_{symbol}_{day}.parquet',index=False)
            record['received_rows'] = len(frame)
            record['valid_rows'] = len(good)
            record['end_stamps'] = good.ts.dt.strftime('%H:%M').tolist()
        else:
            record.update(received_rows=0,valid_rows=0,end_stamps=[])
        record['status'] = 'READ_ONLY_STAGED'
    except Exception as exc:
        record['status'] = 'BROKER_ERROR_' + type(exc).__name__
    results.append(record)
    (OUT/'probe_results.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
    print(json.dumps(record),flush=True)
    time.sleep(0.5)
