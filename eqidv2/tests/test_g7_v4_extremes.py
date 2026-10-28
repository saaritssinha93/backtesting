import json
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_extremes as v4
from research.g7_v4_backtest import screen
from research.g8_exit_engine import allocate_variant,replay_exits


def session(day,offset=0.):
    ts=pd.date_range(day+' 09:16',periods=50,freq='min',tz=v4.IST)
    close=100+offset+np.arange(50)*.02
    return pd.DataFrame(dict(ts=ts,open=close-.01,high=close+.04,low=close-.04,close=close,volume=np.arange(50)+10))


def features():
    days={str(d.date()):session(str(d.date()),i*.1) for i,d in enumerate(pd.bdate_range('2026-09-01',periods=30))}
    day=sorted(days)[-2]; bar=days[day].iloc[34]
    row=dict(sid=1,tradingsymbol='TEST',side='LONG',confirmation_ts=bar.ts,trigger=bar.high)
    row.update({'confirmation_'+k:bar[k] for k in ['open','high','low','close']})
    return days,row


def assert_features_equal(a,b):
    for key in a:
        if isinstance(a[key],float) and np.isnan(a[key]): assert np.isnan(b[key]),key
        else: assert a[key]==b[key],key


def test_current_future_and_later_sessions_cannot_change_features():
    days,row=features(); before=v4.feature_row(days,row)
    for day,frame in days.items():
        mask=frame.ts.gt(row['confirmation_ts'])
        frame.loc[mask,v4.FIELDS]=np.nan
    assert_features_equal(before,v4.feature_row(days,row))
    assert before['historical_samples']==28
    assert before['historical_last_day']<before['day']


def test_historical_reference_uses_same_clock_prefix_and_excludes_bad_prefix():
    days,row=features(); before=v4.feature_row(days,row)
    earlier=sorted(days)[5]
    days[earlier].loc[days[earlier].ts.dt.strftime('%H:%M').gt('09:50'),v4.FIELDS]=np.nan
    assert_features_equal(before,v4.feature_row(days,row))
    days[earlier]=days[earlier].drop(index=4)
    after=v4.feature_row(days,row)
    assert after['historical_samples']==27
    assert json.loads(after['excluded_historical_sessions'])[0]['day']==earlier


@pytest.mark.parametrize('bad',['missing','duplicate','synthetic','ohlc','confirmation'])
def test_corrupt_current_data_aborts_instead_of_becoming_a_profitable_filter(bad):
    days,row=features(); day=str(row['confirmation_ts'].date())
    if bad=='missing': days[day]=days[day].drop(index=3)
    if bad=='duplicate': days[day]=pd.concat([days[day],days[day].iloc[[3]]])
    if bad=='synthetic': days[day]['is_synthetic']=False; days[day].loc[3,'is_synthetic']=True
    if bad=='ohlc': days[day].loc[3,'high']=1
    if bad=='confirmation': row['confirmation_close']+=1
    with pytest.raises(ValueError): v4.feature_row(days,row)


def test_early_warmup_does_not_use_previous_session_rsi_or_future_bars():
    days,row=features(); day=str(row['confirmation_ts'].date()); bar=days[day].iloc[5]
    row.update(confirmation_ts=bar.ts,trigger=bar.high)
    row.update({'confirmation_'+k:bar[k] for k in ['open','high','low','close']})
    result=v4.feature_row(days,row)
    assert result['historical_ready'] and not result['vwap_ready'] and not result['rsi_ready'] and not result['bollinger_ready']
    assert np.isnan(result['rsi14'])
    current={day:days[day]}; result=v4.feature_row(current,row)
    assert not result['historical_ready']
    assert not any(s.iloc[0] for k,s in v4.flags(pd.DataFrame([result])).items())


def test_vwap_is_volume_weighted_and_percentile_uses_directional_trigger():
    days,row=features(); result=v4.feature_row(days,row)
    w=days[result['day']].iloc[:35]
    expected=np.average(w[['high','low','close']].mean(axis=1),weights=w.volume)
    assert result['vwap']==pytest.approx(expected)
    row['side']='SHORT'; row['trigger']=float(w.low.iloc[-1])
    short=v4.feature_row(days,row)
    assert short['signed_vwap_distance_pct']==pytest.approx((1-row['trigger']/expected)*100)
    assert short['historical_samples']==result['historical_samples']


def test_outcomes_cannot_change_rejection_and_invalid_availability_raises():
    days,row=features(); f=pd.DataFrame([v4.feature_row(days,row)])
    a=v4.flags(f); b=v4.flags(f.assign(exit_reason='STOP',net_profit_rupees=-1e12))
    for k in a: pd.testing.assert_series_equal(a[k],b[k])
    f['feature_available_at']=str(row['confirmation_ts']+pd.Timedelta(minutes=1))
    with pytest.raises(ValueError): v4.flags(f)


def path_and_order(short=False):
    ts=pd.date_range('2026-10-26 10:01','2026-10-26 15:15',freq='min',tz=v4.IST)
    p=dict(timestamp_ns=ts.asi8,open=np.full(len(ts),100.6),high=np.full(len(ts),100.8),low=np.full(len(ts),100.5),close=np.full(len(ts),100.7))
    bars=[(100.,100.4,99.9,100.3),(100.2,100.3,99.95,100.1),(100.6,100.8,100.5,100.7),(100.7,101.5,100.6,101.)]
    for i,bar in enumerate(bars):
        for k,x in zip(('open','high','low','close'),bar):p[k][i]=x
    r=dict(sid=1,day='2026-10-26',setup_id='1000_LONG',tradingsymbol='TEST',side='LONG',trigger=100.,
        confirmation_ts='2026-10-26 10:00+05:30',confirmation_low=99.,confirmation_high=100.,
        filled=True,g8_baseline_filled=True,entry_ts=ts[0],entry_price=100.,entry_path_index=0,
        native_stop_pct=1.2,native_target_pct=.6,picker='max_liquidity',traded_value=1e6)
    if short:
        p={**p,'open':200-p['open'],'close':200-p['close'],'high':200-p['low'],'low':200-p['high']}
        r.update(side='SHORT',setup_id='1000_SHORT',confirmation_low=100.,confirmation_high=101.)
    return p,r


@pytest.mark.parametrize('short',[False,True])
def test_retest_arms_next_bar_and_reprices_gap_entry_and_exit(short):
    p,r=path_and_order(short)
    fill,status,observed=v4.retest_entry(r,p)
    assert status=='RETEST_FILLED' and observed==1 and fill[0]==2
    assert fill[1]==pytest.approx(99.4 if short else 100.6)
    replay=v4.replay_retests(pd.DataFrame([r]),{1:p},pd.Series({1:True})).iloc[0]
    assert replay.entry_path_index==2 and replay.exit_reason=='TARGET'
    assert replay.exit_price==pytest.approx(fill[1]*(.994 if short else 1.006))
    assert replay.entry_ts>pd.Timestamp(r['confirmation_ts'])+pd.Timedelta(minutes=observed+1)
    p['close'][10:]=50 # Future closes cannot alter already executable entry.
    assert v4.retest_entry(r,p)[0]==fill


def test_retest_requires_separate_breakout_and_retest_and_never_extends_deadline():
    p,r=path_and_order()
    for k in ('open','high','low','close'): p[k][:10]=99.5
    p['high'][0]=101.; p['low'][0]=99.4 # Intrabar breakout without holding close is insufficient.
    p['open'][8],p['high'][8],p['low'][8],p['close'][8]=99.8,100.4,99.7,100.2
    p['open'][9],p['high'][9],p['low'][9],p['close'][9]=100.2,100.3,99.9,100.1
    assert v4.retest_entry(r,p)[0] is None


def test_retest_invalidates_before_arming_and_detects_missing_bars():
    p,r=path_and_order(); p['low'][0]=98.;p['close'][0]=98.5
    assert v4.retest_entry(r,p)[1]=='INVALIDATED_BEFORE_ARM'
    p['timestamp_ns'][1]+=60_000_000_000
    with pytest.raises(ValueError):v4.retest_entry(r,p)


def test_nonextreme_entries_preserve_all_original_fields():
    p,r=path_and_order(); r['gross_return_pct']=.6
    source=pd.DataFrame([r]); result=v4.replay_retests(source,{1:p},pd.Series({1:False}))
    pd.testing.assert_frame_equal(result[source.columns],source)


def test_one_active_symbol_blocks_overlap_but_allows_after_exit():
    p,r=path_and_order(); ts=pd.to_datetime(p['timestamp_ns'],utc=True).tz_convert(v4.IST)
    # Target at minute 4 closes the first position before the third candidate.
    rows=[]
    for sid,index in [(1,0),(2,1),(3,5)]:
        row=dict(r,sid=sid,setup_id=str(sid)+'_LONG',entry_path_index=index,entry_ts=ts[index],entry_price=100.)
        rows.append(row)
    config=dict(initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
        capital_per_entry_rs=100000.,leverage=5.,portfolio_capital_rs=1000000.,max_positions_per_symbol=1)
    raw=replay_exits(pd.DataFrame(rows),{1:p,2:p,3:p},1.2,.6,config)
    book=allocate_variant(raw,pd.Series({1:True,2:True,3:True}),config,'test')
    assert book.portfolio_executed.tolist()==[True,False,True]


def test_training_selection_ignores_every_later_result_and_missing_effect_fails():
    records=[]
    for period in ['development','validation','assessment','final_oct27']:
        for policy in ['g7_v2','candidate']:
            records.append(dict(policy=policy,period=period,net_pnl_rs=10000,negative_trade_sum_abs_rs=1000 if policy=='g7_v2' else 900,
                trades=100,wins=80,daily_close_drawdown_rs=100,changed_executions=10,changed_days=4,loss_amount_reduction_rs=0 if policy=='g7_v2' else 100))
    table=pd.DataFrame(records); table['net_pnl_rs']=table.net_pnl_rs.astype(float); chosen=screen(table,['candidate'])
    assert chosen[0]=='candidate'
    table.loc[~table.period.eq('development'),'net_pnl_rs']=-1e20
    assert screen(table,['candidate'])==chosen
    table.loc[table.policy.eq('candidate') & table.period.eq('development'),'negative_trade_sum_abs_rs']=960
    assert screen(table,['candidate'])[0] is None


def test_round2_requires_actual_failed_breakout_and_keeps_outcomes_out():
    from research.g7_v4_round2_backtest import triggers
    days,row=features(); record=v4.feature_row(days,row)
    f=pd.DataFrame([record,record])
    f['signed_vwap_z']=3.;f['signed_bollinger_z']=3.
    f['signed_vwap_distance_pct']=5.;f['vwap_q95']=1.;f['vwap_q975']=2.
    f['signed_rsi_delta3']=-1.;f['rejection']=True
    f['failed_prior20_breakout']=[False,True]
    result=triggers(f)
    assert result['vwap_failed_breakout'].tolist()==[False,True]
    assert result['historical975_failed_breakout'].tolist()==[False,True]
    assert result['bollinger_rsi'].all() and result['dual_extreme_rsi'].all()
    other=triggers(f.assign(net_profit_rupees=[1e10,-1e10],exit_reason='TARGET'))
    for k in result:pd.testing.assert_series_equal(result[k],other[k])
