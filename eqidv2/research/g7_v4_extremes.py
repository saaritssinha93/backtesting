"""Causal G7 V4 features and entry policies. No production/broker dependencies.

Equity timestamps label candle END. A decision at confirmation can only use
that candle or earlier ones. Retest orders arm after the observation candle.
"""
from __future__ import annotations
from types import SimpleNamespace
import json

import numpy as np
import pandas as pd

from research.g4_vwap_rsi_features import _check_flags, wilder_rsi
from research.g8_exit_engine import _flags
from research.g3_sl_target_simulator import simulate_exit
import fno_v13_corrected_v5_backtest as native_engine

IST = 'Asia/Kolkata'
FIELDS = ['open', 'high', 'low', 'close', 'volume']
POLICIES = {
    'vwap25': (1, 'Location: directional trigger VWAP z > 2.5 after 30 session bars'),
    'historical95': (1, 'Location: trigger VWAP distance OR opening move exceeds prior same-clock 95th percentile'),
    'bollinger25': (1, 'Location: trigger z against last 20 session closes > 2.5'),
    'vwap25_rsi': (2, 'VWAP extreme AND directional RSI14 falls over three completed minutes'),
    'historical95_rejection': (2, 'Historical extreme AND confirmation wick/rejection'),
    'combined_exhaustion': (2, 'VWAP OR historical extreme AND RSI weakening OR price rejection'),
    'historical975_rejection': (2, 'Fixed stricter neighbour: historical 97.5th percentile AND rejection'),
    'time_adaptive': (2, 'Before 30 session bars: historical95; later: combined exhaustion'),
    'conditional_retest': (3, 'Only extreme candidates wait for completed breakout, later retest, then a subsequent trigger fill'),
}


def timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError('Missing timestamp')
    return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)


def checked_session(frame, stamp):
    start = stamp.normalize()+pd.Timedelta(hours=9, minutes=16)
    window = frame.loc[frame.ts.ge(start) & frame.ts.le(stamp)].sort_values('ts')
    if not pd.DatetimeIndex(window.ts).equals(pd.date_range(start, stamp, freq='min')):
        raise ValueError('Missing or duplicate completed minute')
    values = window[FIELDS].apply(pd.to_numeric, errors='coerce').to_numpy(float)
    if (not len(values) or not np.isfinite(values).all() or (values[:,:4] <= 0).any()
            or (values[:,4] < 0).any() or values[:,4].sum() <= 0
            or (values[:,1] < values[:,[0,2,3]].max(axis=1)).any()
            or (values[:,2] > values[:,[0,1,3]].min(axis=1)).any()):
        raise ValueError('Invalid OHLCV')
    _check_flags(window)
    if 'is_synthetic' in window and _flags(window.is_synthetic.where(window.is_synthetic.notna(),False),'synthetic').any():
        raise ValueError('Synthetic candle')
    return window


def statistics(window):
    tp = window[['high','low','close']].mean(axis=1).to_numpy(float)
    vol = window.volume.to_numpy(float)
    vwap = float(np.average(tp, weights=vol))
    sigma = float(np.sqrt(np.average((tp-vwap)**2, weights=vol)))
    return dict(vwap=vwap, sigma=sigma, opening=float(window.open.iloc[0]),
        high=float(window.high.iloc[-1]), low=float(window.low.iloc[-1]))


def feature_row(days, row, cache=None):
    """Same-clock historical quantiles use at most 60 prior sessions, >=20 valid.

    Historical references use that minute's high for LONG, low for SHORT, so
    comparison is trigger-to-trigger, not a trigger against historical closes.
    Invalid historical prefixes are excluded and counted; invalid current
    prefixes abort research rather than being scored as profitable rejections.
    """
    cache = {} if cache is None else cache
    stamp = timestamp(row['confirmation_ts'])
    if row['side'] not in ('LONG','SHORT'):
        raise ValueError('Invalid side')
    sign = 1 if row['side']=='LONG' else -1
    day, clock = str(stamp.date()), stamp.strftime('%H:%M')
    if stamp != stamp.floor('min') or not '09:16' <= clock <= '15:15':
        raise ValueError('Invalid confirmation clock')
    window = checked_session(days[day], stamp)
    for field in FIELDS[:4]:
        if not np.isclose(window[field].iloc[-1], float(row['confirmation_'+field]),atol=.001,rtol=0):
            raise ValueError('Confirmation OHLC differs from archived order')
    now = statistics(window)
    trigger = float(row['trigger'])
    if not np.isfinite(trigger) or trigger <= 0:
        raise ValueError('Invalid trigger')
    dist = sign*(trigger/now['vwap']-1)*100
    move = sign*(trigger/now['opening']-1)*100
    history, excluded = [], []
    for earlier in sorted(d for d in days if d < day)[-60:]:
        key = (earlier,clock)
        if key not in cache:
            try:
                cache[key] = statistics(checked_session(days[earlier],timestamp(earlier+' '+clock)))
            except ValueError as exc:
                cache[key] = str(exc)
        item = cache[key]
        if isinstance(item,str):
            excluded.append(dict(day=earlier, reason=item))
        else:
            edge = item['high'] if sign==1 else item['low']
            history.append((earlier, sign*(edge/item['vwap']-1)*100, sign*(edge/item['opening']-1)*100))
    rsi = wilder_rsi(window.close.to_numpy(float))
    rsi_ready = len(window)>=30 and np.isfinite(rsi[-1]) and np.isfinite(rsi[-4])
    current = window.iloc[-1]
    candle_range = float(current.high-current.low)
    wick = (current.high-max(current.open,current.close)) if sign==1 else (min(current.open,current.close)-current.low)
    wick_fraction = float(wick/candle_range) if candle_range>0 else 0.
    previous = window.iloc[:-1].tail(20)
    prior_edge = float(previous.high.max() if sign==1 else previous.low.min()) if len(previous)>=5 else np.nan
    broke = np.isfinite(prior_edge) and sign*(trigger-prior_edge)>0
    rejected = bool(broke and sign*(float(current.close)-prior_edge)<=0)
    closes = window.close.tail(20).to_numpy(float)
    bb_sd = float(np.std(closes,ddof=0))
    result = dict(sid=int(row['sid']), day=day, tradingsymbol=row['tradingsymbol'], side=row['side'],
        confirmation_ts=stamp.isoformat(), feature_available_at=stamp.isoformat(),
        session_bars=len(window), vwap=now['vwap'], volume_weighted_sigma=now['sigma'],
        signed_vwap_distance_pct=dist, signed_opening_move_pct=move,
        vwap_ready=len(window)>=30 and now['sigma']>0,
        signed_vwap_z=sign*(trigger-now['vwap'])/now['sigma'] if now['sigma']>0 else np.nan,
        bollinger_ready=len(window)>=20 and bb_sd>0,
        signed_bollinger_z=sign*(trigger-float(closes.mean()))/bb_sd if bb_sd>0 else np.nan,
        rsi_ready=bool(rsi_ready), rsi14=float(rsi[-1]) if np.isfinite(rsi[-1]) else np.nan,
        signed_rsi_delta3=sign*float(rsi[-1]-rsi[-4]) if rsi_ready else np.nan,
        directional_wick_fraction=wick_fraction, failed_prior20_breakout=rejected,
        rejection=bool(wick_fraction>=.25 or rejected),
        historical_ready=len(history)>=20, historical_samples=len(history),
        historical_last_day=history[-1][0] if history else '',
        historical_first_day=history[0][0] if history else '',
        excluded_historical_sessions=json.dumps(excluded,separators=(',',':')),
        historical_source_days=json.dumps([x[0] for x in history],separators=(',',':')))
    for tag,q in [('95',.95),('975',.975)]:
        for name,col in [('vwap',1),('move',2)]:
            result[name+'_q'+tag] = float(np.quantile([x[col] for x in history],q,method='linear')) if len(history)>=20 else np.nan
    return result


def flags(features):
    """Outcome fields are never read. Invalid advertised-ready values raise."""
    f=features
    available=pd.to_datetime(f.feature_available_at,utc=True)
    confirm=pd.to_datetime(f.confirmation_ts,utc=True)
    if available.isna().any() or confirm.isna().any() or (available>confirm).any():
        raise ValueError('Noncausal feature availability')
    ready={k:_flags(f[k+'_ready'],k+'_ready') for k in ('vwap','bollinger','rsi','historical')}
    for k,cols in dict(vwap=['signed_vwap_z'],bollinger=['signed_bollinger_z'],rsi=['signed_rsi_delta3'],
                      historical=['signed_vwap_distance_pct','signed_opening_move_pct','vwap_q95','move_q95','vwap_q975','move_q975']).items():
        if not np.isfinite(f.loc[ready[k],cols].to_numpy(float)).all():
            raise ValueError('Missing advertised-ready feature: '+k)
    if (f.loc[ready['historical'],'historical_samples']<20).any():
        raise ValueError('Historical warmup missing')
    last=pd.to_datetime(f.historical_last_day,errors='coerce')
    if (ready['historical'] & (last.isna() | last.ge(pd.to_datetime(f.day)))).any():
        raise ValueError('Historical reference includes candidate/future day')
    vw=ready['vwap'] & f.signed_vwap_z.gt(2.5)
    bb=ready['bollinger'] & f.signed_bollinger_z.gt(2.5)
    historical={}
    for tag in ('95','975'):
        historical[tag]=ready['historical'] & ((f.signed_vwap_distance_pct.gt(f['vwap_q'+tag]) & f.signed_vwap_distance_pct.gt(0))
            | (f.signed_opening_move_pct.gt(f['move_q'+tag]) & f.signed_opening_move_pct.gt(0)))
    weakening=ready['rsi'] & f.signed_rsi_delta3.lt(0)
    rejection=_flags(f.rejection,'rejection')
    extreme=vw|historical['95']
    return dict(vwap=vw,bollinger=bb,historical=historical['95'],extreme=extreme,
        vwap25=vw,historical95=historical['95'],bollinger25=bb,
        vwap25_rsi=vw & weakening,historical95_rejection=historical['95'] & rejection,
        combined_exhaustion=extreme & (weakening|rejection),
        historical975_rejection=historical['975'] & rejection,
        time_adaptive=(f.session_bars.lt(30)&historical['95']) | (f.session_bars.ge(30)&extreme&(weakening|rejection)),
        conditional_retest=extreme)


def retest_entry(row,path):
    """Breakout close, a LATER retest/hold close, then next-bar trigger order.

    Entry uses original trigger and original ten-minute deadline. Before arming,
    a completed close through the opposite confirmation edge cancels the setup.
    Same-bar high/low cannot cause a fill before that bar's close was observed.
    """
    side=row['side']; sign=1 if side=='LONG' else -1
    if side not in ('LONG','SHORT'): raise ValueError('Invalid side')
    stamp=timestamp(row['confirmation_ts'])
    n=min(10,len(path['close']))
    expected=pd.date_range(stamp+pd.Timedelta(minutes=1),periods=n,freq='min').tz_convert('UTC').asi8
    if n<10 or not np.array_equal(np.asarray(path['timestamp_ns'][:n],dtype='int64'),expected):
        raise ValueError('Incomplete retest entry window')
    a=np.column_stack([path[k][:n] for k in ('open','high','low','close')]).astype(float)
    if not np.isfinite(a).all() or (a<=0).any() or (a[:,1]<a[:,[0,2,3]].max(axis=1)).any() or (a[:,2]>a[:,[0,1,3]].min(axis=1)).any():
        raise ValueError('Invalid retest OHLC')
    trigger=float(row['trigger'])
    opposite=float(row['confirmation_low'] if sign==1 else row['confirmation_high'])
    broken=False
    for i in range(n-1):
        close=float(path['close'][i])
        if sign*(close-opposite)<0: return None,'INVALIDATED_BEFORE_ARM',i
        if not broken:
            broken=sign*(close-trigger)>=0
            continue
        touch=float(path['low'][i])<=trigger if sign==1 else float(path['high'][i])>=trigger
        if touch and sign*(close-trigger)>=0:
            found=native_engine._entry(SimpleNamespace(**row),path,delay_bars=i+1,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=9-i)
            return found,('RETEST_FILLED' if found else 'ARMED_EXPIRED'),i
    return None,'NO_COMPLETED_RETEST',None


def replay_retests(original,paths,extreme):
    if set(original.sid)!=set(extreme.index) or not extreme.index.is_unique:
        raise ValueError('Retest decision inventory differs')
    result=[]
    for row in original.to_dict('records'):
        sid=int(row['sid'])
        row['v4_retest_required']=bool(extreme.loc[sid])
        row['v4_retest_status']='NOT_REQUIRED'
        if extreme.loc[sid]:
            found,status,observed=retest_entry(row,paths[sid])
            row.update(v4_retest_status=status,v4_retest_observed_index=observed,
                filled=False,g8_baseline_filled=False,entry_ts=pd.NaT,entry_price=np.nan,
                entry_path_index=np.nan,exit_ts=pd.NaT,exit_price=np.nan,exit_path_index=np.nan,
                gross_return_pct=np.nan,net_return_pct=np.nan,cost_pct=np.nan,exit_reason='UNFILLED',
                holding_minutes=np.nan,mfe_pct=np.nan,mae_pct=np.nan,target_hit=False,stop_hit=False,
                same_bar_ambiguous=False,entry_gap_through=False,entry_overshoot_bps=np.nan)
            if found is not None:
                index,price,trigger,gap,overshoot=found
                row.update(filled=True,g8_baseline_filled=True,entry_path_index=index,entry_price=price,
                    trigger_used=trigger,entry_gap_through=gap,entry_overshoot_bps=overshoot,
                    entry_ts=pd.Timestamp(int(paths[sid]['timestamp_ns'][index]),tz='UTC').tz_convert(IST))
                outcome=simulate_exit(paths[sid],index,price,row['side']=='LONG',float(row['native_stop_pct']),
                    float(row['native_target_pct']),tighten_after_minutes=120,tightened_stop_pct=1.)
                row.update(outcome,cost_pct=.05,net_return_pct=outcome['gross_return_pct']-.05)
        result.append(row)
    return pd.DataFrame(result)
