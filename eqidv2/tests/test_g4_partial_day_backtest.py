import numpy as np
import pandas as pd
import pytest

from research.g4_partial_day_backtest import partial_exit, replay_orders, SnapshotPrices


def path(rows, start='2026-10-16 10:01'):
    values=np.asarray(rows,float)
    times=pd.date_range(start,periods=len(rows),freq='min',tz='Asia/Kolkata')
    return dict(timestamp_ns=times.asi8,**{name:values[:,i] for i,name in enumerate(('open','high','low','close'))})


@pytest.mark.parametrize('long',[True,False])
def test_unclosed_position_is_marked_without_inventing_an_exit(long):
    r=partial_exit(path([[100,100.1,99.9,100],[100,100.4,99.9,100.3]]),0,100,long,2.)
    assert r['position_open'] and r['exit_reason']=='OPEN_AT_CUTOFF'
    assert pd.isna(r['exit_ts']) and np.isnan(r['exit_price'])
    assert r['gross_return_pct']==pytest.approx(.3 if long else -.3)


def test_partial_path_keeps_stop_first_and_observed_gap_fill():
    r=partial_exit(path([[100,100.1,99.9,100],[98,103,97.9,100]]),0,100,True,2.)
    assert not r['position_open']
    assert r['exit_reason']=='STOP' and r['exit_price']==98


def test_invalid_path_is_not_reclassified_as_open():
    p=path([[100,100.1,99.9,100]]*3)
    p['timestamp_ns'][2]+=60_000_000_000
    with pytest.raises(ValueError,match='contiguous'):
        partial_exit(p,0,100,True,2.)


def test_equal_instants_with_timezone_aliases_have_valid_execution_paths():
    prices=object.__new__(SnapshotPrices)
    prices.day=pd.Timestamp('2026-10-16').date()
    prices.cutoff=pd.Timestamp('2026-10-16 10:03:00+05:30')
    frame=pd.DataFrame(dict(ts=pd.date_range('2026-10-16 10:01',periods=3,freq='min',tz='Asia/Kolkata'),
        open=100.,high=100.2,low=99.9,close=100.1))
    prices.frame=lambda day,symbol:(frame,'synthetic')
    orders=pd.DataFrame([dict(sid=1,tradingsymbol='A',confirmation_ts=pd.Timestamp('2026-10-16 10:00',tz='Asia/Calcutta'))])
    assert len(prices.paths(orders)[1]['close'])==3


def test_scheduled_tightening_uses_next_bar_open():
    p=path([[100,100.1,99.9,100]]*121)
    assert partial_exit(p,0,100,True,2.)['active_stop_pct_at_exit']==1.2
    p=path([[100,100.1,99.9,100]]*122)
    assert partial_exit(p,0,100,True,2.)['active_stop_pct_at_exit']==1.


def test_open_positions_reserve_capital_and_never_count_as_realized_winners():
    cutoff=pd.Timestamp('2026-10-16 10:15',tz='Asia/Kolkata')
    orders=[]; paths={}
    for i in range(11):
        stamp=pd.Timestamp('2026-10-16 10:00',tz='Asia/Kolkata')+pd.Timedelta(minutes=i)
        orders.append(dict(sid=i,day=stamp.date(),side='LONG',tradingsymbol=f'S{i}',
            confirmation_ts=stamp,setup_id=f'{i}_LONG',trigger=100.,native_target_pct=2.,picker='max_liquidity',traded_value=1000.))
        paths[i]=path([[100,100.2,99.9,100.1]]*int((cutoff-stamp).total_seconds()/60),str(stamp.tz_localize(None)+pd.Timedelta(minutes=1)))
    ledger=replay_orders(pd.DataFrame(orders),paths,cutoff)
    assert ledger.portfolio_executed.sum()==10
    assert ledger.position_state.eq('PORTFOLIO_REJECTED').sum()==1
    assert ledger.realized_net_rs.sum()==0
    assert ledger.portfolio_net_profit_rupees.sum()==0
    assert ledger.exit_ts.isna().all()
    assert ledger.open_mtm_net_after_reserved_cost_rs.sum()==pytest.approx(2500.)
