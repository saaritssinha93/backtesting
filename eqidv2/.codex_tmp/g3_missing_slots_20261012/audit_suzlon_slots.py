"""Read-only SUZLON slot audit; write only isolated research artifacts."""
from __future__ import annotations
import hashlib
import html
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as config

DAY = date(2026, 10, 12)
BASE = Path(r'C:\TradingData\eqidv2\fno_oi')
RUN = Path(json.loads(Path(__file__).with_name('latest_run.json').read_text())['run_dir'])
MINUTE = RUN / 'minutes/SUZLON.parquet'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    frame = pd.read_parquet(MINUTE)
    assert frame.ts.is_unique and frame.ts.is_monotonic_increasing
    results = []
    for slot in contract.SIGNAL_TO_CONFIRMATION:
        scanner_path = BASE / 'v13_v10_g_3_live/scanner_5m' / DAY.isoformat() / f"slot_{slot.replace(':','')}.json"
        scanner = json.loads(scanner_path.read_text())
        assert scanner['strategy_fingerprint'] == contract.strategy_fingerprint()
        record = next(r for r in scanner['feature_evaluations'] if r['tradingsymbol'] == 'SUZLON')
        confirm_end = contract.SIGNAL_TO_CONFIRMATION[slot]
        stamp = pd.Timestamp(f'{DAY} {confirm_end}', tz=contract.IST)
        exact = frame.loc[frame.ts.eq(stamp)]
        assert len(exact) == 1
        bar = exact.iloc[0].to_dict()
        bar.update(timestamp=stamp.isoformat(), confirmation_history_complete=True)
        bar = config.annotate_confirmation_volume(bar, frame[['ts','volume']].rename(columns={'ts':'timestamp'}))
        assert bar['confirmation_prior_volume_count'] == 20
        confirmation_path = BASE / 'v13_v10_g_3_live/confirmation_1m' / DAY.isoformat() / f"slot_{confirm_end.replace(':','')}.json"
        confirmation = json.loads(confirmation_path.read_text())
        recorded = [r for r in confirmation['feature_evaluations'] if r['tradingsymbol'] == 'SUZLON']
        setups = []
        for side in ('LONG','SHORT'):
            setup = contract.setup_for(slot, side, session_date=DAY)
            if setup is None:
                continue
            base = {k:record.get(k) for k in ('tradingsymbol','signal_ts','signal_end','open','high','low','close',
                       'volume','ema9','ema20','ema50','price_change_pct','oi','prev_oi','oi_change_pct',
                       'volume_ratio','traded_value','nifty_first_bar_return_pct')}
            base.update(side=side, signal_timestamp=record['signal_ts'], signal_close=record['close'])
            metrics = config.confirmation_metrics(base,bar)
            trigger = metrics['confirm_high'] if side == 'LONG' else metrics['confirm_low']
            future = frame.loc[frame.ts.gt(stamp) & frame.ts.le(stamp+pd.Timedelta(minutes=10))]
            touched = future.loc[future.high.ge(trigger) if side == 'LONG' else future.low.le(trigger)]
            first_hit = touched.ts.iloc[0].isoformat() if len(touched) else None
            passed = contract.passes_selected_filters(metrics,setup,session_date=DAY)
            setups.append(dict(side=side, setup_id=setup.setup_id,
                exact_1m_close=metrics['confirm_close'], exact_1m_open=metrics['confirm_open'],
                confirmation_reason=metrics['confirmation_reason'], confirmation_volume_ratio=metrics['v9_1m_volume_ratio'],
                required_confirmation_volume_ratio=contract.confirmation_volume_minimum(side),
                body_ratio=metrics['body_ratio'], required_body_ratio=setup.body_ratio,
                adverse_wick_ratio=metrics['wick_ratio'], maximum_wick_ratio=setup.max_wick_ratio,
                required_price_change_pct=setup.price_change_pct, required_oi_change_pct=setup.oi_change_pct,
                required_5m_volume_ratio=setup.volume_ratio,
                current_filter_pass=passed, trigger=trigger, hypothetical_first_trigger_touch_ist=first_hit))
        results.append(dict(signal_slot=slot, confirmation_end=confirm_end, scanner_sha256=sha(scanner_path),
            confirmation_packet_sha256=sha(confirmation_path), scanner_state=scanner['state'],
            base_side=record['base_side'] or None, base_long_pass=record['base_long_pass'],
            base_short_pass=record['base_short_pass'],
            base_first_failed_gate=None if record['base_side'] else record['first_failed_gate'],
            price_change_pct=record['price_change_pct'], oi_change_pct=record['oi_change_pct'],
            five_minute_volume_ratio=record['volume_ratio'], signal_close=record['close'],
            configured_sides=[s['side'] for s in setups], recorded_confirmation_rows=len(recorded),
            recorded_confirmation_rejection=confirmation.get('candidate_rejections',{}).get('SUZLON'),
            recorded_entry_selected=any('SUZLON' in str(s) for s in confirmation.get('selected_signal_ids',[])),
            retrospective_setup_checks=setups))
    assert len(results)==9
    assert sum(bool(x['base_side']) for x in results)==2
    assert sum(x['recorded_confirmation_rows'] for x in results)==1
    assert not any(x['recorded_entry_selected'] for x in results)
    assert not any(s['current_filter_pass'] for x in results for s in x['retrospective_setup_checks'])
    data=dict(session_date=str(DAY),symbol='SUZLON',strategy_fingerprint=contract.strategy_fingerprint(),
        market_data_as_of_ist=json.loads((RUN/'recovery_manifest.json').read_text())['as_of_ist'],
        minute_source_sha256=sha(MINUTE),original_scanner_and_confirmation_packets_unchanged=True,
        scope='Read-only current-rules audit. Unrecorded 1m checks are retrospective, never live confirmation decisions.',
        slots=results)
    (RUN/'suzlon_all_slots.json').write_text(json.dumps(data,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    rows=[]
    for r in results:
        summaries=[]
        for s in r['retrospective_setup_checks']:
            summaries.append(f"{s['side']}: {s['confirmation_reason']}; volume {s['confirmation_volume_ratio']:.2f}x; "
                             f"body {s['body_ratio']:.2f}; trigger "
                             f"{s['hypothetical_first_trigger_touch_ist'][11:16] if s['hypothetical_first_trigger_touch_ist'] else 'not touched'}")
        rows.append([r['signal_slot'],r['base_side'] or 'REJECTED',r['base_first_failed_gate'],
                     ', '.join(r['configured_sides']),r['recorded_confirmation_rows'],
                     'YES' if r['recorded_entry_selected'] else 'NO',' / '.join(summaries)])
    headers=['Signal','5m base','First 5m failure','Configured sides','Recorded 1m rows','Recorded entry','Retrospective 1m checks and trigger touch']
    table='<table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'
    table+=''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows)+'</tbody></table>'
    page='<!doctype html><html lang="en"><head><meta charset="utf-8"><title>SUZLON G3 slots</title><style>body{font:15px/1.5 system-ui,sans-serif;margin:30px;color:#172033}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border:1px solid #d3dce7;padding:8px;vertical-align:top}th{background:#eaf0f8;text-align:left}tr:nth-child(even){background:#f7f9fc}</style></head><body>'
    page+='<h1>SUZLON across all G3 slots</h1><p>12 October 2026. No recorded entry in any slot. Saved 5-minute packets and recorded confirmation packets are compared with exact retrospectively retrieved 1-minute candles.</p>'
    page+=table+'<p>Trigger touch shows whether a later minute bar crossed the confirmation extreme within ten minutes. It does not imply selection or a broker fill. For slots without a recorded confirmation row, 1-minute checks are retrospective.</p></body></html>'
    (RUN/'suzlon_all_slots.html').write_text(page,encoding='utf-8')
    for r in results:
        print(json.dumps({k:r[k] for k in ('signal_slot','base_side','base_first_failed_gate','configured_sides','recorded_confirmation_rows','recorded_entry_selected')}))
    print(str(RUN/'suzlon_all_slots.html'))

if __name__=='__main__':
    main()
