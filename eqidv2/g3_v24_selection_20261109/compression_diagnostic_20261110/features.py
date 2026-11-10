"""Causal compression/breakout labels; no admission or execution decisions."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import features as source_adapter
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import stock_features

VERSION = 'G3_V24_COMPRESSION_DIAGNOSTIC_1M_20_2_Q20_100_V1'
IST = 'Asia/Kolkata'
MINUTE_NS = 60_000_000_000
DAY_NS = 24*60*MINUTE_NS
PERIOD = 20
MULTIPLIER = 2.
REFERENCE_COUNT = 100
QUANTILE = .20
RANGE_BARS = 5
MIN_CURRENT_BARS = 25
OHLCV = ('open','high','low','close','volume')
FLAGS = ('gap_filled','opening_snapshot','provisional_stale','is_synthetic')
LABELS = ('COMPRESSION_BREAKOUT','EARLIER_BREAKOUT_CONTINUATION',
          'BREAKOUT_WITHOUT_COMPRESSION','OTHER_READY',
          'EARLY_SESSION_NOT_APPLICABLE','UNAVAILABLE')


def configuration():
    return dict(version=VERSION, scope='BACKTEST_ONLY_DIAGNOSTIC', execution_authority=False,
        timeframe='completed 1 minute', bollinger_period=PERIOD, standard_deviations=MULTIPLIER,
        standard_deviation_ddof=0, bandwidth='100*(upper-lower)/SMA',
        band_warmup='20 consecutive valid unflagged same-session bars; no overnight mixing',
        regular_session_end_labels='09:16 through 15:30 Asia/Kolkata',
        percentile=QUANTILE, percentile_history=REFERENCE_COUNT,
        percentile_policy='previous100 VALID BandWidth observations strictly before each bar; prior sessions allowed',
        current_session_required_bars=MIN_CURRENT_BARS, prior_range_bars=RANGE_BARS,
        primary='previous-minute compression AND current directional close beyond prior5 range AND expanding BandWidth',
        earlier_continuation='latest compressed anchor t0-5..t0-2; frozen5-bar range ending anchor; first either-side close break before t0 in G3 direction and current close remains beyond it',
        labels=list(LABELS), label_priority=list(LABELS[:4]),
        volume='Existing G3 admitted-volume qualification retained; no additional RVOL threshold',
        threshold_optimization=False, alters_admission=False, alters_entries=False)


def _stamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError('Compression confirmation must be timezone aware')
    stamp = stamp.tz_convert(IST)
    if stamp != stamp.floor('min'):
        raise ValueError('Compression confirmation must be a completed exact minute')
    return stamp


def _clock(value):
    return pd.Timestamp(int(value), tz='UTC').tz_convert(IST).isoformat()


def _blank(row):
    stamp = _stamp(row['confirmation_ts'])
    if row['side'] not in ('LONG','SHORT') or str(stamp.date()) != str(row['day'])[:10]:
        raise ValueError('Invalid compression candidate identity')
    value = dict(sid=int(row['sid']), day=str(stamp.date()), setup_id=row['setup_id'],
        tradingsymbol=row['tradingsymbol'], side=row['side'], confirmation_ts=stamp.isoformat(),
        available_at=stamp.isoformat(), feature_version=VERSION,
        compression_ready=False, compression_status='MISSING_CONFIRMATION',
        compression_label='UNAVAILABLE', is_primary=False,
        current_width_expanding=False, directional_range_break=False,
        previous_bar_compressed=False, anchor_ts=None, anchor_kind=None,
        first_break_ts=None, first_break_side=None, consecutive_valid_session_bars=0,
        volume_ratio=row.get('volume_ratio',np.nan),
        v9_1m_volume_ratio=row.get('v9_1m_volume_ratio',np.nan),
        volume_qualified_inherited=True)
    value.update({name:np.nan for name in ('current_bandwidth','prior_bandwidth','current_q20',
        'prior_q20','prior_range_high','prior_range_low','anchor_range_high','anchor_range_low',
        'anchor_bandwidth','anchor_q20','confirmation_close')})
    return value


def _prepared_values(history):
    """Forward-only vector calculations; no full-history date-string grouping."""
    column = next((key for key in ('ts','date','timestamp') if key in history),None)
    if column is None or not set(OHLCV).issubset(history):
        raise ValueError('Compression history requires timestamps and OHLCV')
    times = pd.DatetimeIndex(pd.to_datetime(history[column],utc=True)).tz_convert(IST).as_unit('ns')
    order = np.argsort(times.asi8,kind='stable')
    times = times[order]
    if times.hasnans or times.has_duplicates:
        raise ValueError('Missing or duplicate compression chronology')
    numeric = history.loc[:,list(OHLCV)].apply(pd.to_numeric,errors='coerce').to_numpy(float)[order]
    op,hi,lo,close,volume = numeric.T
    bad = (~np.isfinite(numeric).all(axis=1) | (numeric[:,:4]<=0).any(axis=1)
        | (volume<0) | (hi<np.maximum.reduce([op,lo,close])) | (lo>np.minimum.reduce([op,hi,close])))
    flagged = np.zeros(len(times),dtype=bool)
    for key in FLAGS:
        if key in history:
            values = history[key]
            if values.dtype == object or isinstance(values.dtype,pd.StringDtype):
                truthy = [x for x in values.dropna().unique()
                          if str(x).strip().lower() in ('true','1','1.0','yes')]
                marked = values.isin(truthy).to_numpy(bool)
            else:
                marked = values.fillna(False).to_numpy(bool)
            flagged |= marked[order]
    nanos = times.asi8
    local = times.tz_localize(None).asi8
    day = local//DAY_NS
    minute = (local%DAY_NS)//MINUTE_NS
    regular = (minute >= 9*60+16) & (minute <= 15*60+30) & (local%MINUTE_NS == 0)
    valid = ~bad & ~flagged & regular
    positions = np.arange(len(times),dtype=np.int64)
    boundary = np.r_[True,(np.diff(nanos)!=MINUTE_NS) | (day[1:]!=day[:-1])]
    restart = np.where(~valid,positions,np.where(boundary,positions-1,-1))
    run_length = positions-np.maximum.accumulate(restart)
    prices = pd.Series(np.where(np.isfinite(close),close,np.nan))
    mean = prices.rolling(PERIOD,min_periods=PERIOD).mean().to_numpy()
    sigma = prices.rolling(PERIOD,min_periods=PERIOD).std(ddof=0).to_numpy()
    width = np.divide(2*MULTIPLIER*100*sigma,mean,
        out=np.full(len(times),np.nan),where=(run_length>=PERIOD)&(mean>0)&np.isfinite(sigma))
    reference = np.full(len(times),np.nan)
    indices = np.flatnonzero(np.isfinite(width))
    reference[indices] = pd.Series(width[indices]).shift(1).rolling(
        REFERENCE_COUNT,min_periods=REFERENCE_COUNT).quantile(QUANTILE).to_numpy()
    return dict(timestamp_ns=nanos,day=day,minute=minute,valid=valid,
        run_length=run_length,bandwidth=width,q20=reference,
        **dict(zip(OHLCV,numeric.T)))


def _from_prepared(values,row):
    result = _blank(row)
    stamp = _stamp(row['confirmation_ts'])
    at = int(np.searchsorted(values['timestamp_ns'],stamp.value))
    if at >= len(values['timestamp_ns']) or values['timestamp_ns'][at] != stamp.value:
        return result
    for key in OHLCV:
        expected = float(row['confirmation_'+key])
        if not np.isfinite(expected) or not np.isclose(values[key][at],expected,rtol=0,atol=.001):
            raise ValueError(f'Compression source/confirmation {key} mismatch: {row["sid"]}')
    result['confirmation_close'] = float(values['close'][at])
    result['consecutive_valid_session_bars'] = int(values['run_length'][at])
    if values['minute'][at] < 9*60+40:
        result.update(compression_label='EARLY_SESSION_NOT_APPLICABLE',
                      compression_status='EARLY_SESSION_NOT_APPLICABLE')
        return result
    if values['run_length'][at] < MIN_CURRENT_BARS:
        result['compression_status'] = 'INCOMPLETE_OR_FLAGGED_CURRENT_25_BARS'
        return result
    prior = at-1
    if not np.isfinite(values['q20'][at-5:at+1]).all():
        result['compression_status'] = 'INSUFFICIENT_PRIOR_100_VALID_BANDWIDTHS'
        return result
    bw,old_bw = float(values['bandwidth'][at]),float(values['bandwidth'][prior])
    high = float(np.max(values['high'][at-5:at]))
    low = float(np.min(values['low'][at-5:at]))
    close = float(values['close'][at])
    is_long = row['side']=='LONG'
    directional_break = close>high if is_long else close<low
    expands = bw>old_bw
    compressed = old_bw <= values['q20'][prior]
    result.update(compression_ready=True,compression_status='READY',compression_label='OTHER_READY',
        current_bandwidth=bw,prior_bandwidth=old_bw,current_q20=float(values['q20'][at]),
        prior_q20=float(values['q20'][prior]),prior_range_high=high,prior_range_low=low,
        current_width_expanding=bool(expands),directional_range_break=bool(directional_break),
        previous_bar_compressed=bool(compressed))
    if compressed and directional_break and expands:
        result.update(compression_label='COMPRESSION_BREAKOUT',is_primary=True,
            anchor_ts=_clock(values['timestamp_ns'][prior]),anchor_kind='PREVIOUS_MINUTE',
            anchor_range_high=high,anchor_range_low=low,anchor_bandwidth=old_bw,
            anchor_q20=float(values['q20'][prior]),first_break_ts=stamp.isoformat(),first_break_side=row['side'])
        return result
    anchors = [j for j in range(at-5,at-1) if values['bandwidth'][j] <= values['q20'][j]]
    if anchors:
        anchor = anchors[-1]
        range_high = float(np.max(values['high'][anchor-4:anchor+1]))
        range_low = float(np.min(values['low'][anchor-4:anchor+1]))
        result.update(anchor_ts=_clock(values['timestamp_ns'][anchor]),anchor_kind='EARLIER_CANDIDATE',
            anchor_range_high=range_high,anchor_range_low=range_low,
            anchor_bandwidth=float(values['bandwidth'][anchor]),anchor_q20=float(values['q20'][anchor]))
        for j in range(anchor+1,at+1):
            side = 'LONG' if values['close'][j]>range_high else ('SHORT' if values['close'][j]<range_low else None)
            if side is not None:
                result.update(first_break_ts=_clock(values['timestamp_ns'][j]),first_break_side=side)
                remains = close>range_high if is_long else close<range_low
                if j<at and side==row['side'] and remains:
                    result['compression_label']='EARLIER_BREAKOUT_CONTINUATION'
                    return result
                break
    if directional_break and expands:
        result['compression_label']='BREAKOUT_WITHOUT_COMPRESSION'
    return result


def features_at(history,row):
    """Pure candidate API; observations after confirmation are excluded first."""
    column = next((key for key in ('ts','date','timestamp') if key in history),None)
    if column is None:
        raise ValueError('Compression history has no timestamp column')
    times = pd.to_datetime(history[column],utc=True)
    prefix = history.loc[times.le(_stamp(row['confirmation_ts']).tz_convert('UTC'))]
    if prefix.empty:
        return _blank(row)
    return _from_prepared(_prepared_values(prefix),row)


def build_features(orders):
    """Preserve every admitted SID; attach diagnostic labels and source proofs."""
    if orders.empty:
        return pd.DataFrame(columns=['sid','compression_ready','compression_label','is_primary']),[]
    grouped,source_proofs,missing,proofs = source_adapter._inventory_with_missing(orders)
    results = []
    lookup = orders.set_index('sid').to_dict('index')
    for value in missing:
        sid = int(value['sid'])
        row = _blank(dict(lookup[sid],sid=sid))
        row['compression_status']='MISSING_SEALED_SOURCE'
        results.append(row)
    proofs = [dict(proof,compression_feature_version=VERSION,
        policy='Unavailable diagnostic label only; retain admitted order') for proof in proofs]
    for count,(path,rows) in enumerate(grouped.items(),1):
        frame,proof = stock_features._prepared_frame(path,rows,source_proofs[path])
        values = _prepared_values(frame)
        for row in rows:
            value = _from_prepared(values,row)
            value.update(stock_source_path=str(path),stock_source_sha256=proof['stock_sha256'])
            results.append(value)
        proofs.append(dict(proof,compression_feature_version=VERSION,candidate_rows=len(rows),
            candidate_days=sorted({str(row['day'])[:10] for row in rows}),source_rows=len(frame),
            definition=configuration()))
        if count%100==0:
            print(f'Compression sources {count}/{len(grouped)}',flush=True)
    audit = pd.DataFrame(results).sort_values('sid',kind='stable').reset_index(drop=True)
    if len(audit)!=len(orders) or audit.sid.duplicated().any() or set(audit.sid)!=set(orders.sid):
        raise ValueError('Compression feature builder changed admitted inventory')
    if not audit.compression_label.isin(LABELS).all():
        raise ValueError('Unknown compression diagnostic label')
    return audit,proofs
