"""Read-only market-input diagnostic; never enables orders or changes rules."""
from pathlib import Path
from datetime import datetime
import hashlib
import json
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dashboard_flow_live_data import load_session_market

DATA = Path('C:/TradingData/eqidv2')
ASOF = pd.Timestamp('2026-11-02 11:30:00', tz='Asia/Kolkata')
CLOCKS = ['09:25','09:30','09:50','10:25','10:45','11:20']


def spot_evidence():
    import io
    path = DATA / 'stocks_indicators_5min_eq_live/NIFTY50_INDEX_stocks_indicators_5min.parquet'
    meta_path = Path(str(path) + '.source.json')
    meta_raw, raw = meta_path.read_bytes(), path.read_bytes()
    meta = json.loads(meta_raw)
    if meta.get('source_symbol') != 'NIFTY 50' or meta.get('instrument_token') != 256265:
        raise ValueError('True spot-index source identity not verified')
    f = pd.read_parquet(io.BytesIO(raw))
    f['ts'] = pd.to_datetime(f.date)
    valid = f.ts.le(ASOF)
    for flag in ('opening_snapshot','gap_filled','provisional_stale','synthetic','is_synthetic','interpolated'):
        if flag in f:
            valid &= ~f[flag].astype(str).str.lower().isin(['true','1','1.0'])
    f = f.loc[valid].set_index('ts').sort_index()
    if f.index.duplicated().any():
        raise ValueError('Duplicate spot-index candle timestamp')
    f['causal_ema20'] = f.close.ewm(span=20, adjust=False, min_periods=100).mean()
    previous = float(f.loc[pd.Timestamp('2026-10-30 15:30',tz=ASOF.tz),'close'])
    today = f.loc[f.index.date == ASOF.date()]
    opening = float(today.loc[pd.Timestamp('2026-11-02 09:20',tz=ASOF.tz),'open'])
    rows = []
    for clock in CLOCKS:
        ts = pd.Timestamp(f'2026-11-02 {clock}',tz=ASOF.tz)
        price = float(today.loc[ts,'close'])
        prior_ts = ts-pd.Timedelta(minutes=15)
        prior = float(today.loc[prior_ts,'close']) if prior_ts in today.index else opening if prior_ts.strftime('%H:%M') == '09:15' else None
        rows.append(dict(clock=clock, spot_close=price, opening_gap_pct=(opening/previous-1)*100,
            return_vs_prior_close_pct=(price/previous-1)*100, return_vs_open_pct=(price/opening-1)*100,
            return_15m_pct=(price/prior-1)*100 if prior is not None else None,
            ema20=float(today.loc[ts,'causal_ema20']), above_ema20=bool(price > float(today.loc[ts,'causal_ema20']))))
    return dict(source=str(path), source_sha256=hashlib.sha256(raw).hexdigest(),
        source_metadata=meta, source_metadata_sha256=hashlib.sha256(meta_raw).hexdigest(),
        previous_session_last_bar_close=previous, opening_price=opening,
        valid_candle_policy='Flagged opening snapshots and synthetic/stale/gap rows excluded; causal EMA20 recomputed from valid rows, minimum100bars warmup. 09:30 15min return uses verified completed09:20bar open, never09:15snapshot close.',
        previous_close_kind='EXACT_PREVIOUS_SESSION_1530_SPOT_BAR_NOT_SEPARATELY_VERIFIED_OFFICIAL_CLOSE',rows=rows)


def main():
    path = DATA / 'fno_oi/raw_contracts_5m/NIFTY26NOVFUT_5minute.parquet'
    raw = path.read_bytes()
    import io
    f = pd.read_parquet(io.BytesIO(raw)).copy()
    f['ts'] = pd.to_datetime(f.timestamp)
    f = f.loc[f.ts.le(ASOF) & f.quality_state.eq('VALID')].sort_values('ts').set_index('ts')
    previous = float(f.loc[pd.Timestamp('2026-10-30 15:30',tz=ASOF.tz),'close'])
    today = f.loc[f.index.date == ASOF.date()]
    opening = float(today.loc[pd.Timestamp('2026-11-02 09:20',tz=ASOF.tz),'open'])
    market = load_session_market(DATA, ASOF.date(), now=ASOF)
    byclock = {s['time']:s for s in market['slots']}
    records = []
    for clock in CLOCKS:
        ts = pd.Timestamp(f'2026-11-02 {clock}',tz=ASOF.tz)
        price = float(today.loc[ts,'close'])
        prior_ts = ts-pd.Timedelta(minutes=15)
        prior = float(today.loc[prior_ts,'close']) if prior_ts in today.index else opening if prior_ts.strftime('%H:%M') == '09:15' else None
        # Completed opening bar gives the known opening price only after 09:20.
        rows = byclock[clock]['rows']
        available = [r for r in rows if r.get('price_pct') is not None]
        rising = [r for r in available if r['price_pct'] > 0]
        above_open = [r for r in rows if r.get('price') is not None and r.get('session_open') is not None and r['price'] > r['session_open']]
        open_available = [r for r in rows if r.get('price') is not None and r.get('session_open') is not None]
        records.append(dict(clock=clock, futures_close=price, opening_gap_pct=(opening/previous-1)*100,
            return_vs_prior_close_pct=(price/previous-1)*100, return_vs_open_pct=(price/opening-1)*100,
            return_15m_pct=(price/prior-1)*100 if prior is not None else None,
            universe=len(rows), daily_breadth_eligible=len(available), daily_advancers=len(rising),
            daily_breadth_pct=100*len(rising)/len(available) if available else None,
            open_breadth_eligible=len(open_available), above_open=len(above_open),
            open_breadth_pct=100*len(above_open)/len(open_available) if open_available else None))
    out = ROOT / 'outputs/g3_admission_20261102'
    out.mkdir(parents=True, exist_ok=True)
    result = dict(scope='READ_ONLY_RESEARCH_DIAGNOSTIC', execution_authority=False,
        asof=ASOF.isoformat(), index_instrument='NIFTY26NOVFUT', source=str(path),
        source_sha256=hashlib.sha256(raw).hexdigest(), prior_session='2026-10-30',
        previous_session_last_bar_close=previous, prior_close_kind='SAME_CONTRACT_1530_BAR_NOT_OFFICIAL_SETTLEMENT',
        opening_price=opening, breadth_universe_state=market['universe_state'],
        breadth_warnings=market['warnings'], rows=records,
        true_spot=spot_evidence(),
        note='No threshold optimization or P&L test. Historical file reconstruction does not prove live acquisition timing. No ETF/spot/futures references mixed. NIFTY/NIFTY50/NIFTY_50 filenames currently contain NIFTYBEES ETF; true spot is NIFTY50_INDEX with verified token256265.')
    (out / 'nifty_regime_diagnostic.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__ == '__main__':
    main()
