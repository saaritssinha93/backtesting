import numpy as np
import pandas as pd
import pytest

from research.g6_persistence_exit_research import trigger_index, early_result


def path(closes, *, minute=1, start='2026-10-21 10:01', opens=None, spread=.01):
    closes=np.asarray(closes,float)
    opens=np.r_[closes[0],closes[:-1]] if opens is None else np.asarray(opens,float)
    return dict(timestamp_ns=pd.date_range(start,periods=len(closes),freq=f'{minute}min',tz='Asia/Kolkata').asi8,
        open=opens,high=np.maximum(opens,closes)+spread,low=np.minimum(opens,closes)-spread,close=closes)


def test_stock_opposition_waits_for_full_window_and_is_long_short_symmetric():
    p=path([100,99.98,99.96,99.94,99.92,99.88,99.85,99.80])
    assert trigger_index(p,0,100,True,p,5,'opposite',.1,1)==5
    short={k:(200-v if k in ('open','close') else v.copy()) for k,v in p.items()}
    short['high']=200-p['low'];short['low']=200-p['high']
    assert trigger_index(short,0,100,False,short,5,'opposite',.1,1)==5


def test_entry_bar_cannot_supply_window_and_profitable_position_is_protected():
    p=path([100]*8)
    assert trigger_index(p,0,100,True,p,5,'sideways',.2,1)==5
    profitable=path([100.5]*8)
    assert trigger_index(profitable,0,100,True,profitable,5,'sideways',.2,1) is None


def test_flat_window_does_not_mean_merely_returning_to_start_after_large_swing():
    p=path([100,100.4,99.6,100.3,99.7,100,100])
    assert trigger_index(p,0,100,True,p,5,'sideways',.2,1) is None


def test_detection_cannot_see_later_prices():
    p=path([100,99.98,99.96,99.94,99.92,99.88,99.85,99.80])
    changed={k:v.copy() for k,v in p.items()}
    for field in ('open','high','low','close'):
        changed[field][6:]*=1.2
    assert trigger_index(changed,0,100,True,changed,5,'opposite',.1,1)==5


def test_nifty_candle_overlapping_entry_is_not_a_completed_post_entry_window():
    stock=path([100]*20,start='2026-10-21 10:03')
    index=path([100,99.9,99.8,99.7],minute=5,start='2026-10-21 10:05')
    # Entry at 10:03: 10:00-10:05 overlaps entry; 10:05-10:10 is the first full bar.
    found=trigger_index(stock,0,100,True,index,5,'opposite',.03,5)
    assert pd.Timestamp(stock['timestamp_ns'][found],tz='UTC').tz_convert('Asia/Kolkata').strftime('%H:%M')=='10:10'


def row(exit_index=7,event='INTRABAR'):
    return dict(entry_path_index=0,entry_price=100.,side='LONG',target_pct=2.,exit_path_index=exit_index,exit_event=event)


def test_early_exit_uses_next_open_before_later_intrabar_stop():
    p=path([100,99.98,99.96,99.94,99.92,99.88,98.,98.],opens=[100,100,99.98,99.96,99.94,99.92,99.70,98.])
    result=early_result(row(exit_index=6),p,5,'EARLY_TEST')
    assert result['exit_price']==99.70
    assert result['exit_path_index']==6
    assert result['holding_minutes']==5
    assert result['mae_pct'] == pytest.approx(-.3)  # Open fill counts; later low of 98 does not.


@pytest.mark.parametrize('exit_index,event',[(5,'INTRABAR'),(6,'OPEN')])
def test_native_exit_before_or_at_decision_execution_keeps_precedence(exit_index,event):
    p=path([100,99.98,99.96,99.94,99.92,99.88,99.85,99.80])
    assert early_result(row(exit_index,event),p,5,'EARLY_TEST') is None


def test_open_gap_through_original_stop_uses_existing_stop_exit():
    p=path([100,99.98,99.96,99.94,99.92,99.88,98.,98.],opens=[100,100,99.98,99.96,99.94,99.92,98.,98.])
    assert early_result(row(exit_index=6),p,5,'EARLY_TEST') is None


def test_partial_nifty_duration_is_rejected():
    p=path([100]*8)
    with pytest.raises(ValueError,match='multiple'):
        trigger_index(p,0,100,True,p,3,'sideways',.1,5)
