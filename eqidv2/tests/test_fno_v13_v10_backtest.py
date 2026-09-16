from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_backtest as e
import fno_v13_v10_research as r


def market(side='LONG'):
    confirmation=pd.Timestamp('2026-08-03 09:26',tz='Asia/Kolkata')
    stamps=pd.date_range(confirmation+pd.Timedelta(minutes=1),confirmation.normalize()+pd.Timedelta(hours=15,minutes=15),freq='min')
    row=dict(sid=1,day=confirmation.date(),tradingsymbol='TEST',side=side,trigger=100.,
        confirmation_ts=confirmation,signal_ts=confirmation-pd.Timedelta(minutes=1),
        setup_id='0926_'+side,picker='max_liquidity',traded_value=1e9)
    path=dict(timestamp_ns=stamps.asi8,open=np.full(len(stamps),100.),high=np.full(len(stamps),100.1),
        low=np.full(len(stamps),99.9),close=np.full(len(stamps),100.))
    return pd.DataFrame([row]),{1:path}


def set_bar(p,index,opening,high,low,close):
    for field,value in zip(['open','high','low','close'],[opening,high,low,close]):
        p[field][index]=value


@pytest.mark.parametrize('kwargs',[{'initial_stop_pct':1.5001},{'runner_target_pct':2.01},
    {'first_target_pct':float('nan')},{'partial_pct':0},{'runner_stop':'TRAIL_UNKNOWN'},
    {'partial_pct':1,'first_target_pct':.5,'runner_target_pct':1}])
def test_caps_and_exit_contract_fail_closed(kwargs):
    with pytest.raises(ValueError):
        e.V10Config(**kwargs).validate()


@pytest.mark.parametrize('fraction',[.1,.25,.5,.75])
@pytest.mark.parametrize('side',['LONG','SHORT'])
def test_partial_be_replay_is_exact_native(fraction,side):
    orders,paths=market(side)
    set_bar(paths[1],1,100.1,101.2,100.1,101.1)
    set_bar(paths[1],2,101.1,102.1,100.8,101.8)
    if side=='SHORT':
        old={k:v.copy() for k,v in paths[1].items()}
        for key in ['open','close']:
            paths[1][key]=200-old[key]
        paths[1]['high'],paths[1]['low']=200-old['low'],200-old['high']
    cfg=e.V10Config(partial_pct=fraction)
    expected=e.v9.v5.simulate_scaleout(orders,paths,cfg.exit_spec(),cost_bps=5,max_entry_delay_minutes=10)
    actual=e.simulate(orders,paths,cfg)
    pd.testing.assert_frame_equal(expected,actual)


def test_original_stop_runner_can_survive_a_breakeven_retest():
    orders,paths=market()
    set_bar(paths[1],1,100.2,100.6,100.1,100.5)
    set_bar(paths[1],2,100.3,100.4,99.8,100.2)
    set_bar(paths[1],3,100.2,101.2,100.1,101.0)
    cfg=e.V10Config(initial_stop_pct=1,first_target_pct=.5,runner_target_pct=1,partial_pct=.25)
    be=e.simulate(orders,paths,cfg).iloc[0]
    original=e.simulate(orders,paths,replace(cfg,runner_stop='INITIAL')).iloc[0]
    assert be.exit_reason=='T1_THEN_BREAKEVEN'
    assert original.exit_reason=='RUNNER_TARGET'
    assert original.gross_return_pct==pytest.approx(.875)
    assert original.exit_path_index==3


@pytest.mark.parametrize('side',['LONG','SHORT'])
def test_full_exit_books_whole_target_without_same_bar_be(side):
    orders,paths=market(side)
    set_bar(paths[1],1,100.,102.1,99.8,101.8) if side=='LONG' else set_bar(paths[1],1,100.,100.2,97.9,98.2)
    cfg=e.V10Config(first_target_pct=2,runner_target_pct=2,partial_pct=1,runner_stop='INITIAL')
    row=e.simulate(orders,paths,cfg).iloc[0]
    assert row.exit_reason=='FULL_TARGET'
    assert row.gross_return_pct==pytest.approx(2.)
    assert row.exit_path_index==1


def test_same_bar_initial_stop_precedes_full_target():
    orders,paths=market()
    set_bar(paths[1],1,100.,102.1,98.,100.)
    cfg=e.V10Config(first_target_pct=2,runner_target_pct=2,partial_pct=1,runner_stop='INITIAL')
    row=e.simulate(orders,paths,cfg).iloc[0]
    assert row.exit_reason=='FULL_STOP'
    assert row.same_bar_ambiguous
    assert row.gross_return_pct==pytest.approx(-1.5)


def test_original_stop_gap_after_partial_uses_actual_adverse_open():
    orders,paths=market()
    set_bar(paths[1],1,100.2,100.7,100.1,100.5)
    set_bar(paths[1],2,98.,99.1,97.8,98.5)
    cfg=e.V10Config(initial_stop_pct=1,first_target_pct=.5,runner_target_pct=1,partial_pct=.25,runner_stop='INITIAL')
    row=e.simulate(orders,paths,cfg).iloc[0]
    assert row.exit_reason=='T1_THEN_INITIAL_STOP'
    assert row.exit_gap_through
    assert row.exit_price==98
    assert row.gross_return_pct==pytest.approx(.25*.5-.75*2)


def test_missing_path_minute_fails_before_portfolio_replay():
    orders,paths=market()
    for key in paths[1]:
        paths[1][key]=paths[1][key][1:]
    with pytest.raises(RuntimeError,match='Non-continuous'):
        e.evaluate_orders(orders,paths,e.V10Config())


def test_grid_is_unique_and_all_caps_and_families_are_represented():
    rows=r.grid()
    configs=[r.config(row) for row in rows]
    assert len(configs)==5294
    assert len(set(configs))==len(configs)
    for cfg in configs:
        cfg.validate()
    assert {cfg.partial_pct for cfg in configs}=={.1,.25,.5,.75,1.}
    assert {cfg.runner_stop for cfg in configs}=={'INITIAL','BREAKEVEN'}


def test_zero_profit_roundoff_is_not_an_improved_win():
    ledger=pd.DataFrame([dict(day='2026-08-03',portfolio_executed=True,gross_return_pct=.05+1e-15,exposure_per_entry_rupees=500000)])
    m=r.metric(ledger,[pd.Timestamp('2026-08-03').date()])
    assert m['wins']==0
    assert m['breakeven']==1


@pytest.mark.parametrize('side',['LONG','SHORT'])
def test_single_exit_matches_independent_native_fixed_bracket(side):
    orders,paths=market(side)
    rng=np.random.default_rng(1010)
    path=paths[1]
    close=100+np.cumsum(rng.normal(0,.12,len(path['close'])))
    opening=np.r_[100.,close[:-1]]
    path.update(open=opening,close=close,high=np.maximum(opening,close)+.05,
                low=np.minimum(opening,close)-.05)
    for stop,target in [(.25,.5),(.75,1.),(1.5,2.)]:
        native_orders=orders.assign(native_stop_pct=stop,native_target_pct=target)
        native=e.v9.v5.simulate_native(native_orders,paths,cost_bps=5,max_entry_delay_minutes=10)
        cfg=e.V10Config(initial_stop_pct=stop,first_target_pct=target,runner_target_pct=target,
                         partial_pct=1,runner_stop='INITIAL')
        actual=e.simulate(native_orders,paths,cfg)
        for col in ['filled','entry_ts','exit_ts','entry_price','exit_price','net_return_pct','same_bar_ambiguous']:
            if pd.api.types.is_numeric_dtype(native[col]):
                np.testing.assert_allclose(actual[col],native[col],rtol=0,atol=1e-10)
            else:
                pd.testing.assert_series_equal(actual[col],native[col],check_names=False)
