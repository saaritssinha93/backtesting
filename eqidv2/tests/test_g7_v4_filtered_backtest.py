"""The selected entry veto is combined with protection before full allocation."""
from copy import deepcopy
import numpy as np
import pandas as pd
import pytest
from research import g7_v4_filtered_backtest as chosen

CONFIG=dict(initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
    capital_per_entry_rs=100000.,leverage=5.,portfolio_capital_rs=1000000.,model_cost_bps_round_trip=5.)
SETTINGS=dict(variant_id=chosen.VERSION,scope='BACKTEST_ONLY',execution_authority=False,
    selection_basis='USER_REQUEST',research_qualification=False,max_positions_per_symbol=1,
    entry_filter=deepcopy(chosen.FILTER),exit_policy=deepcopy(chosen.PROTECTION))


def example(short=False):
    ts=pd.date_range('2026-10-26 10:00','2026-10-26 15:15',freq='min',tz='Asia/Kolkata')
    p=dict(timestamp_ns=ts.asi8,open=np.full(len(ts),100.2),high=np.full(len(ts),100.3),
        low=np.full(len(ts),100.15),close=np.full(len(ts),100.2))
    for i,bar in enumerate([(100.,100.8,99.9,100.7),(100.2,100.8,99.8,100.7),(100.5,100.6,100.05,100.2)]):
        for k,v in zip(('open','high','low','close'),bar):p[k][i]=v
    row=dict(sid=1,day='2026-10-26',setup_id='0959_LONG',tradingsymbol='TEST',side='LONG',filled=True,
        entry_ts=ts[0],entry_price=100.,entry_path_index=0,native_stop_pct=1.2,native_target_pct=2.,
        picker='max_liquidity',traded_value=1e6)
    f=dict(sid=1,day='2026-10-26',confirmation_ts='2026-10-26 09:59+05:30',
        feature_available_at='2026-10-26 09:59+05:30',indicator_available_at='2026-10-26 09:59+05:30',
        vwap_ready=True,bollinger_ready=False,rsi_ready=True,historical_ready=False,
        historical_samples=0,historical_last_day='',signed_vwap_z=3.,signed_bollinger_z=np.nan,
        signed_rsi_delta3=-1.,signed_vwap_distance_pct=1.,signed_opening_move_pct=1.,
        vwap_q95=np.nan,move_q95=np.nan,vwap_q975=np.nan,move_q975=np.nan,
        indicator_ready=True,efficiency10=.2,directional_close_position=.8,price_rsi_divergence=False,
        chop14=40.,volume_ratio5=1.,directional_wick_fraction=.1,rejection=False,session_bars=44,
        allowed_g7_v2=True)
    if short:
        p={**p,'open':200-p['open'],'high':200-p['low'],'low':200-p['high'],'close':200-p['close']}
        row.update(side='SHORT',setup_id='0959_SHORT')
    return pd.DataFrame([row]),{1:p},pd.DataFrame([f]).set_index('sid',drop=False)


@pytest.mark.parametrize('short',[False,True])
def test_extreme_weak_rsi_low_efficiency_veto_is_applied_to_both_sides(short):
    native,paths,f=example(short)
    b=chosen.make_book(native,paths,f,CONFIG,SETTINGS)
    assert not b.portfolio_executed.any()
    assert b.iloc[0].portfolio_reject_reason=='LOW_EFFICIENCY_EXTREME_RSI'
    assert b.iloc[0].v4_indicator_veto
    assert b.iloc[0].g8_baseline_filled


@pytest.mark.parametrize('column,value',[('efficiency10',.3),('signed_rsi_delta3',0.),('signed_vwap_z',2.5),('indicator_ready',False)])
def test_all_three_conditions_are_required_and_warmup_preserves_entries(column,value):
    native,paths,f=example()
    f[column]=value
    b=chosen.make_book(native,paths,f,CONFIG,SETTINGS)
    assert b.iloc[0].portfolio_executed
    assert b.iloc[0].exit_reason=='PROTECTED_STOP'
    assert b.iloc[0].protection_armed_path_index==1
    assert b.iloc[0].protection_effective_path_index==2
    assert b.iloc[0].portfolio_net_profit_rupees==pytest.approx(250.)
    assert b.iloc[0].strategy_version==chosen.VERSION
    assert b.iloc[0].selection_basis=='USER_REQUEST'
    assert not b.iloc[0].execution_authority
    assert not b.iloc[0].research_qualification


def test_removing_first_entry_releases_capacity_for_later_same_symbol():
    native,paths,f=example()
    row=dict(native.iloc[0],sid=2,setup_id='1000_LONG',entry_path_index=1,
        entry_ts=pd.Timestamp(paths[1]['timestamp_ns'][1],tz='UTC'))
    native=pd.concat([native,pd.DataFrame([row])],ignore_index=True)
    paths[2]=paths[1]
    second=dict(f.iloc[0],sid=2,efficiency10=.5)
    f=pd.concat([f.reset_index(drop=True),pd.DataFrame([second])],ignore_index=True).set_index('sid',drop=False)
    b=chosen.make_book(native,paths,f,CONFIG,SETTINGS)
    assert b.portfolio_executed.tolist()==[False,True]
    assert b.sid.tolist()==[1,2]


def test_nifty_permission_is_retained_even_without_an_indicator_veto():
    native,paths,f=example()
    f['efficiency10']=.5;f['allowed_g7_v2']=False
    b=chosen.make_book(native,paths,f,CONFIG,SETTINGS)
    assert not b.portfolio_executed.any()


def test_v2_reference_rebuilds_permission_metadata_for_cost_replays():
    native,paths,f=example()
    # Older cached comparison ledgers may have this column absent or missing.
    native['g8_direction_allowed']=np.nan
    f['allowed_g7_v2']=False
    reference=chosen.reference_book(native,paths,f,CONFIG)
    allowed=chosen._flags(reference.set_index('sid').g8_direction_allowed,'allowed')
    assert allowed.tolist()==[False]
    stressed=chosen.allocate_variant(reference,allowed,CONFIG,'g7_v2',cost_bps=15.)
    assert not stressed.portfolio_executed.any()


@pytest.mark.parametrize('field,value',[('scope','LIVE'),('execution_authority',True),('research_qualification',True),('max_positions_per_symbol',2)])
def test_selection_cannot_enable_execution_or_claim_research_qualification(field,value):
    s=deepcopy(SETTINGS);s[field]=value
    with pytest.raises(ValueError):chosen.validate_settings(s)


def test_changed_parameters_require_a_new_explicit_variant_definition():
    s=deepcopy(SETTINGS);s['entry_filter']['efficiency_below']=.4
    with pytest.raises(ValueError):chosen.validate_settings(s)
    s=deepcopy(SETTINGS);s['exit_policy']['protect_after_close_pct']=.7
    with pytest.raises(ValueError):chosen.validate_settings(s)
