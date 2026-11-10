from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from . import engine as e


def fixture(count=10):
    stamp = pd.Timestamp('2026-11-09 10:01:00', tz='Asia/Kolkata')
    times = stamp.tz_convert('UTC').value + np.arange(1, count+1)*e.MINUTE_NS
    path = dict(timestamp_ns=times, open=np.full(count, 100.), high=np.full(count, 100.5),
                low=np.full(count, 99.5), close=np.full(count, 100.))
    window = dict(sid=7, confirmation_ts=stamp, available=True, status='READY',
        timestamp_ns=stamp.tz_convert('UTC').value+np.arange(11)*e.MINUTE_NS,
        open=np.full(11,100.), high=np.full(11,100.5), low=np.full(11,99.5),
        close=np.full(11,100.), prior_close=np.full(11,100.),
        rsi14=np.full(11,50.), valid=np.ones(11,dtype=bool),
        bar_status=np.full(11,'READY',dtype=object))
    order = dict(sid=7, day='2026-11-09', setup_id='1001_LONG', tradingsymbol='TEST',
                 side='LONG', confirmation_ts=stamp, trigger=102., native_target_pct=.8)
    return order,path,window


def ready_long(count=10):
    order,path,window=fixture(count)
    window['prior_close'][0]=101.  # A completed price pullback and RSI reset.
    window['rsi14'][1:]=56.
    window['close'][1]=101.
    window['prior_close'][1]=100.
    window['high'][1]=101.5
    path['high'][0]=101.5
    path['close'][0]=101.
    if count>1:
        path['open'][1]=101.
        path['high'][1]=102.
        path['low'][1]=100.
        path['close'][1]=101.8
    return order,path,window


def test_requires_reset_then_later_recovery_and_subsequent_price_break():
    order,path,window=ready_long()
    found=e.find_entry(order,path,window)
    assert found['filled'] and found['reset_index']==0 and found['cross_index']==1
    assert found['entry_path_index']==1 and found['entry_price']==101.5
    assert found['entry_delay_minutes']==2
    assert pd.Timestamp(found['entry_ts']) > pd.Timestamp(found['cross_ts'])


def test_cross_candle_cannot_fill_its_own_high():
    order,path,window=ready_long()
    path['high'][1:]=101.
    path['open'][1:]=100.;path['close'][1:]=100.;path['low'][1:]=99.
    found=e.find_entry(order,path,window)
    assert not found['filled'] and found['status']=='RECOVERY_WITHOUT_BREAK'


def test_reset_requires_a_directional_price_pause_not_rsi_zone_alone():
    order,path,window=ready_long()
    window['prior_close'][0]=99.
    found=e.find_entry(order,path,window)
    assert not found['filled'] and found['status']=='NO_RESET_WITHIN_WINDOW'


def test_no_lookback_reset_or_stale_high_rsi_admission():
    order,path,window=ready_long()
    window['rsi14'][:]=65.
    assert e.find_entry(order,path,window)['status']=='NO_RESET_WITHIN_WINDOW'
    window['rsi14'][:]=50.
    assert e.find_entry(order,path,window)['status']=='RESET_WITHOUT_RECOVERY'


def test_original_expiry_includes_tenth_bar_but_excludes_eleventh():
    order,path,window=fixture(11)
    window['prior_close'][0]=101.
    window['rsi14'][9:]=56.
    window['high'][9]=101.
    path['high'][9]=101.
    assert e.find_entry(order,path,window)['entry_delay_minutes']==10
    path['high'][9]=100.5;path['high'][10]=101.
    assert not e.find_entry(order,path,window)['filled']
    window['rsi14'][9]=50.
    assert e.find_entry(order,path,window)['status']=='RESET_WITHOUT_RECOVERY'


def test_entry_uses_adverse_gap_open_and_control_preserves_original_trigger():
    order,path,window=ready_long()
    path['high'][1]=101.75;path['close'][1]=101.5
    primary=e.find_entry(order,path,window)
    control=e.find_entry(order,path,window,'rsi_reset_recovery_keep_trigger')
    assert primary['filled'] and primary['entry_price']==101.5
    assert not control['filled'] and control['trigger_used']==102.
    path['open'][1]=103.;path['high'][1]=104.;path['close'][1]=103.
    gap=e.find_entry(order,path,window)
    assert gap['entry_price']==103. and gap['entry_gap_through']


def test_first_trigger_stays_frozen_despite_later_rsi_weakening():
    order,path,window=ready_long()
    path['high'][1]=101.;path['open'][1]=100.;path['close'][1]=100.;path['low'][1]=99.
    window['rsi14'][2]=20.;window['rsi14'][3]=65.;window['high'][3]=105.
    path['high'][3]=102.
    found=e.find_entry(order,path,window)
    assert found['entry_path_index']==3 and found['trigger_used']==101.5
    assert found['cross_index']==1


def test_long_short_are_mirrored_in_entry_rules_and_control_trigger():
    order,path,window=ready_long()
    original=e.find_entry(order,path,window)
    short_order=deepcopy(order);short_order.update(side='SHORT',trigger=98.,setup_id='1001_SHORT')
    short_path=deepcopy(path);short_window=deepcopy(window)
    for target,source in ((short_path,path),(short_window,window)):
        for key in ('open','close'):
            target[key]=200.-source[key]
        target['high']=200.-source['low'];target['low']=200.-source['high']
    short_window['prior_close']=200.-window['prior_close']
    short_window['rsi14']=100.-window['rsi14']
    mirrored=e.find_entry(short_order,short_path,short_window)
    assert mirrored['entry_path_index']==original['entry_path_index']
    assert mirrored['entry_price']==200.-original['entry_price']
    assert e.find_entry(short_order,short_path,short_window,'rsi_reset_recovery_keep_trigger')['trigger_used']==98.


def test_future_observation_missing_or_mutated_cannot_veto_an_existing_fill():
    order,path,window=ready_long()
    expected=e.find_entry(order,path,window)
    window['valid'][2:]=False;window['rsi14'][2:]=np.nan
    window['bar_status'][2:]='MISSING_RSI'
    assert e.find_entry(order,path,window)==expected
    prefix={key:value[:2] for key,value in path.items()}
    assert e.find_entry(order,prefix,window)==expected


def test_missing_completed_observation_before_entry_cancels_only_order():
    order,path,window=ready_long()
    window['valid'][1]=False;window['bar_status'][1]='MISSING_MINUTE'
    found=e.find_entry(order,path,window)
    assert not found['filled'] and found['invalid_observation_index']==1
    assert found['status']=='MISSING_OBSERVATION_BEFORE_ENTRY'


def test_confirmation_clock_and_corrupt_ready_values_are_rejected():
    order,path,window=ready_long()
    window['timestamp_ns'][0]+=e.MINUTE_NS
    with pytest.raises(ValueError,match='clock'):
        e.find_entry(order,path,window)
    order,path,window=ready_long();window['rsi14'][0]=np.nan
    with pytest.raises(ValueError,match='READY'):
        e.find_entry(order,path,window)


def test_simulator_preserves_all_orders_and_reuses_frozen_exit_economics():
    stamp=pd.Timestamp('2026-11-09 10:01:00',tz='Asia/Kolkata')
    count=int((pd.Timestamp('2026-11-09 15:15:00',tz='Asia/Kolkata')-stamp).total_seconds()/60)
    order,path,window=ready_long(count)
    # Candidate crosses then fills at101.5; native0.8%target is hit nextbar.
    path['open'][2:]=101.5;path['close'][2:]=102.;path['low'][2:]=101.;path['high'][2:]=103.
    missing=deepcopy(order);missing['sid']=8
    trades,audit=e.simulate(pd.DataFrame([order,missing]),{7:path,8:path},{7:window},e.POLICIES[0])
    assert audit.sid.tolist()==[7,8] and audit.filled.tolist()==[True,False]
    assert len(trades)==1 and trades.iloc[0].exit_reason=='TARGET'
    assert trades.iloc[0].portfolio_net_profit_rupees==pytest.approx(3750.)
    expected=e.exits.replay(path,1,101.5,True,.8,e.SETTINGS,1.)
    assert trades.iloc[0].exit_price==expected['price']

\n