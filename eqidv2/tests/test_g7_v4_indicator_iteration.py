"""Prefix causality, bounded selection and protected delayed-entry integration."""
from copy import deepcopy
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_indicator_iteration as study
from research.g8_exit_engine import allocate_variant


def session(count=60):
    ts = pd.date_range('2026-10-26 09:16', periods=count, freq='min', tz='Asia/Kolkata')
    close = 100 + np.arange(count)*.02 + np.sin(np.arange(count))*.05
    raw = pd.DataFrame(dict(ts=ts, open=close-.01, high=close+.05, low=close-.05, close=close, volume=1000.+np.arange(count)))
    point = min(count-1, 39)
    now = raw.iloc[point]
    row = dict(sid=1, day='2026-10-26', tradingsymbol='TEST', side='LONG', confirmation_ts=now.ts,
        trigger=now.high, **{'confirmation_'+k:now[k] for k in ('open','high','low','close')})
    return raw, row


def full_features():
    raw, row = session()
    old = study.features_v4.feature_row({'2026-10-26':raw},row)
    new = study.indicator_row(raw,row)
    f = pd.DataFrame([{**old, **new, 'allowed_g7_v2':True}]).set_index('sid',drop=False)
    return raw,row,f


def test_indicators_use_completed_prefix_and_ignore_all_later_prices():
    raw,row = session()
    first = study.indicator_row(raw,row)
    later = raw.ts.gt(row['confirmation_ts'])
    for name in ('open','high','low','close'):raw.loc[later,name]=500.
    raw.loc[later,'volume']=1e12
    assert study.indicator_row(raw,row)==first
    assert 0<=first['efficiency10']<=1
    assert np.isfinite(first['chop14'])


def test_feature_builder_preserves_sid_from_archived_parquet(tmp_path):
    raw,row=session()
    path=tmp_path/'stock.parquet'
    raw.rename(columns={'ts':'date'}).to_parquet(path,index=False)
    original=study.features_v4.feature_row({'2026-10-26':raw},row)
    original.update(stock_source_path=str(path),stock_source_sha256=study.sha(path))
    f=pd.DataFrame([original]).set_index('sid',drop=False)
    result=study.build_features(f,pd.DataFrame([row]))
    assert result.index.tolist()==[1] and result.sid.tolist()==[1]
    assert result.loc[1,'efficiency10']==study.indicator_row(raw,row)['efficiency10']


def test_efficiency_and_choppiness_have_known_monotonic_path_values():
    raw,row = session()
    raw['close']=100+np.arange(len(raw))
    raw['open']=raw.close-.5;raw['high']=raw.close+.5;raw['low']=raw.close-.5
    point=raw.loc[raw.ts.eq(row['confirmation_ts'])].iloc[0]
    row.update(trigger=point.high,**{'confirmation_'+k:point[k] for k in ('open','high','low','close')})
    result=study.indicator_row(raw,row)
    assert result['efficiency10']==pytest.approx(1.)
    assert result['chop14']==pytest.approx(100*np.log10(21/14)/np.log10(14))


def test_early_warmup_is_not_a_profitable_missing_data_veto():
    raw,row=session(10)
    new=study.indicator_row(raw,row)
    assert not new['indicator_ready'] and np.isnan(new['efficiency10'])
    old=study.features_v4.feature_row({'2026-10-26':raw},row)
    f=pd.DataFrame([{**old,**new}]).set_index('sid',drop=False)
    for name, mask in study.decisions(f).items():assert not mask.iloc[0]


def test_current_missing_minute_or_flagged_input_aborts():
    raw,row=session()
    with pytest.raises(ValueError,match='Missing or duplicate'):
        study.indicator_row(raw.drop(index=15),row)
    raw['provisional_stale']=False;raw.loc[15,'provisional_stale']=True
    with pytest.raises(ValueError,match='Flagged'):
        study.indicator_row(raw,row)


def test_decisions_ignore_outcomes_and_validate_feature_availability():
    _,_,f=full_features()
    f['signed_vwap_z']=3.;f['signed_rsi_delta3']=-1.
    old=study.decisions(f)
    changed=study.decisions(f.assign(net_profit_rupees=-1e12,exit_reason='STOP'))
    for key in old:pd.testing.assert_series_equal(old[key],changed[key])
    f['indicator_available_at']=pd.to_datetime(f.confirmation_ts,utc=True)+pd.Timedelta(minutes=1)
    with pytest.raises(ValueError,match='Noncausal'):study.decisions(f)


def test_consensus_requires_two_independent_warnings_and_extreme():
    _,_,f=full_features()
    f['signed_vwap_z']=3.;f['signed_rsi_delta3']=-1.
    f['price_rsi_divergence']=False;f['chop14']=20.;f['efficiency10']=1.;f['volume_ratio5']=2.
    assert not study.decisions(f)['consensus2'].iloc[0]
    f['price_rsi_divergence']=True
    assert study.decisions(f)['consensus2'].iloc[0]
    f['signed_vwap_z']=0.
    assert not study.decisions(f)['consensus2'].iloc[0]


CONFIG=dict(initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
    capital_per_entry_rs=100000.,leverage=5.,portfolio_capital_rs=1000000.,model_cost_bps_round_trip=5.)
SETTINGS=dict(variant_id='V13-V10-G-7-V4-PROFIT-PROTECTION',scope='BACKTEST_ONLY',execution_authority=False,
    selection_basis='USER_REQUEST',max_positions_per_symbol=1,exit_policy=dict(
    protect_after_close_pct=.6,protected_profit_pct=.1,activation='COMPLETED_FULL_POST_ENTRY_CLOSE',effective='NEXT_MINUTE_OPEN'))


def entry_fixture(short=False):
    ts=pd.date_range('2026-10-26 10:01','2026-10-26 15:15',freq='min',tz='Asia/Kolkata')
    p=dict(timestamp_ns=ts.asi8,open=np.full(len(ts),100.6),high=np.full(len(ts),100.8),low=np.full(len(ts),100.5),close=np.full(len(ts),100.7))
    bars=[(100.,100.4,99.9,100.2),(100.6,100.8,100.5,100.7),(100.8,101.5,100.7,101.4),(101.1,101.2,100.6,100.8)]
    for i,bar in enumerate(bars):
        for k,v in zip(('open','high','low','close'),bar):p[k][i]=v
    row=dict(sid=1,day='2026-10-26',setup_id='1000_LONG',tradingsymbol='TEST',side='LONG',filled=True,
        g8_baseline_filled=True,entry_ts=ts[0],entry_price=100.,entry_path_index=0,native_stop_pct=1.2,native_target_pct=2.,
        confirmation_ts='2026-10-26 10:00+05:30',confirmation_low=99.,confirmation_high=100.,trigger=100.,
        picker='max_liquidity',traded_value=1e6)
    if short:
        p={**p,'open':200-p['open'],'high':200-p['low'],'low':200-p['high'],'close':200-p['close']}
        row.update(side='SHORT',setup_id='1000_SHORT',confirmation_high=101.,confirmation_low=100.)
    return pd.DataFrame([row]),{1:p}


@pytest.mark.parametrize('short',[False,True])
def test_delayed_entry_observes_first_close_then_reprices_gap_and_protection(short):
    native,paths=entry_fixture(short)
    delayed=study.confirm_next(native,paths,pd.Series({1:True}))
    r=delayed.iloc[0]
    assert r.entry_path_index==1
    assert r.entry_price==pytest.approx(99.4 if short else 100.6)
    assert r.indicator_entry_status=='DELAYED_FILLED'
    raw=study.selected.replay(delayed,paths,CONFIG,SETTINGS)
    assert raw.iloc[0].protection_armed_path_index==2
    assert raw.iloc[0].exit_path_index==3
    assert raw.iloc[0].exit_reason=='PROTECTED_STOP'
    b=allocate_variant(raw,pd.Series({1:True}),dict(CONFIG,max_positions_per_symbol=1),'test')
    assert b.iloc[0].portfolio_net_profit_rupees==pytest.approx(250.)


def test_failed_first_close_cancels_even_when_later_prices_recover():
    native,paths=entry_fixture()
    paths[1]['close'][0]=99.95
    delayed=study.confirm_next(native,paths,pd.Series({1:True}))
    assert not delayed.iloc[0].filled
    assert delayed.iloc[0].indicator_entry_status=='FIRST_MINUTE_FAILED'
    assert np.isnan(delayed.iloc[0].entry_price)


def test_delay_keeps_original_deadline_and_requires_complete_entry_window():
    native,paths=entry_fixture()
    p=paths[1]
    for k in ('open','high','low','close'):p[k][1:10]=99.9
    # A later recovery after minute10 cannot create a new entry.
    assert not study.confirm_next(native,paths,pd.Series({1:True})).iloc[0].filled
    p['timestamp_ns'][3]+=60_000_000_000
    with pytest.raises(ValueError,match='Missing original'):study.confirm_next(native,paths,pd.Series({1:True}))


def test_nonrisk_orders_preserve_original_entry_fields():
    native,paths=entry_fixture()
    result=study.confirm_next(native,paths,pd.Series({1:False}))
    pd.testing.assert_frame_equal(result[native.columns],native)


def selection_table():
    return pd.DataFrame([dict(policy=n,period=p,net_pnl_rs=10000.,negative_trade_sum_abs_rs=900. if n!='g7_v4' else 1000.,
        trades=100,wins=81 if n!='g7_v4' else 80,losses=19 if n!='g7_v4' else 20,
        win_rate_pct=81. if n!='g7_v4' else 80.,daily_close_drawdown_rs=100.,changed_executions=6,changed_days=3)
        for p in ('development','validation','assessment') for n in ['g7_v4',*study.POLICIES]])


def test_selection_reads_only_named_slice_and_cannot_choose_validation_replacement():
    t=selection_table();expected=study.choose(t,list(study.POLICIES))
    assert expected[0] is not None
    t.loc[~t.period.eq('development') & t.policy.ne('g7_v4'),'net_pnl_rs']=-1e20
    assert study.choose(t,list(study.POLICIES))==expected
    assert study.choose(t,[expected[0]],'validation')[0] is None


@pytest.mark.parametrize('field,value',[('net_pnl_rs',9999.),('negative_trade_sum_abs_rs',951.),('losses',20),
    ('win_rate_pct',79.),('wins',75),('trades',79),('daily_close_drawdown_rs',101.),('changed_executions',4)])
def test_every_acceptance_floor_is_enforced(field,value):
    t=selection_table();t.loc[t.policy.eq('vwap_rsi') & t.period.eq('development'),field]=value
    assert study.choose(t,['vwap_rsi'])[0] is None
