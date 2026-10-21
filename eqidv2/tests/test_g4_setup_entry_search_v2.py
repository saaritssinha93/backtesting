from copy import deepcopy

import pandas as pd
import pytest

from research import g4_setup_entry_engine_v2 as entry
from research import g4_setup_search_v2 as search
from research import g4_complete_inventory_backtest as complete


def config():
    return complete.configuration_payload()


def row(symbol='A', sid=1, price=.35, value=1000000., volume=1.3):
    signal=pd.Timestamp('2026-10-16 09:25',tz='Asia/Kolkata')
    confirm=signal+pd.Timedelta(minutes=1)
    return dict(day='2026-10-16',setup_id='0926_LONG',side='LONG',signal_end='09:25',
        signal_timestamp=signal,confirmation_timestamp=confirm,v9_1m_feature_ts=confirm,
        v9_5m_feature_ts=signal,v9_feature_available_ts=confirm,ema9=103.,ema20=102.,ema50=101.,
        price_change_pct=price,oi=100200.,prev_oi=100000.,oi_change_pct=.2,volume_ratio=volume,
        source_1m_count=5,v9_exact_confirmation_present=True,confirm_open=100.,
        confirm_high=100.3,confirm_low=99.9,confirm_close=100.1,signal_close=100.,
        body_ratio=.8,wick_ratio=.2,traded_value=value,v9_1m_volume_ratio=2.,
        tradingsymbol=symbol,sid=sid)


def test_relaxation_admits_previously_rejected_candidate_before_ranking():
    selector=entry.PreparedSelection(pd.DataFrame([row(),row('B',2,price=.24,value=2000000.)]),config(),{})
    grid=selector.variants['0926_LONG']
    baseline=selector.select('0926_LONG',grid[0])
    relaxed=selector.select('0926_LONG',next(v for v in grid if v['name']=='price_change_pct_x0.75'))
    assert baseline.tradingsymbol.tolist()==['A']
    assert relaxed.tradingsymbol.tolist()==['B']
    assert relaxed.v10_g_f_core.tolist()==[True]


def test_tightening_replaces_rejected_top_stock_and_never_reads_outcomes():
    a,b=row(value=2000000.),row('B',2,volume=2.)
    a['future_profit']=999999999
    b['future_profit']=-999999999
    selector=entry.PreparedSelection(pd.DataFrame([a,b]),config(),{})
    variant=next(v for v in selector.variants['0926_LONG'] if v['name']=='volume_ratio_x1.25')
    assert selector.select('0926_LONG',variant).tradingsymbol.tolist()==['B']


def test_relaxation_preserves_shared_volume_floor_and_timestamp_validity():
    a,b=row(volume=.79),row('B',2,volume=3.)
    b['v9_feature_available_ts']+=pd.Timedelta(minutes=1)
    selector=entry.PreparedSelection(pd.DataFrame([a,b]),config(),{})
    for variant in selector.variants['0926_LONG']:
        assert selector.select('0926_LONG',variant).empty


def test_quota_and_clock_cannot_be_tuned_implicitly():
    selector=entry.PreparedSelection(pd.DataFrame([row()]),config(),{})
    variant=deepcopy(selector.variants['0926_LONG'][0])
    variant['active']['max_entries']=3
    with pytest.raises(ValueError,match='immutable'):
        selector.select('0926_LONG',variant)


def ledger(values):
    return pd.DataFrame([dict(sid=sid,day='2026-10-16',setup_id='0926_LONG',
        filled=True,portfolio_executed=True,portfolio_net_profit_rupees=pnl,exit_reason='TARGET')
        for sid,pnl in values])


def test_removed_original_winners_count_zero_replacement_does_not_hide_damage():
    result=search.metrics(ledger([(1,100.),(2,-50.)]),ledger([(3,1000.)]),['2026-10-16'])
    assert result['net_delta_rs']==950.
    assert result['winner_retention_pct']==0.
    assert result['original_winners_removed']==1
    assert not search.economic(result)


def test_winner_flip_is_reported_but_combined_retention_is_the_constraint():
    result=search.metrics(ledger([(1,100.),(2,100.)]),ledger([(1,200.),(2,-10.)]),['2026-10-16'])
    assert result['winner_retention_pct']==95.
    assert result['original_winners_turned_losses']==1


def test_training_selection_identity_ignores_october_changes():
    a=pd.DataFrame([dict(sid=1,day='2026-09-18',v10_g_f_core=True),
                    dict(sid=2,day='2026-10-16',v10_g_f_core=True)])
    b=a.copy();b.loc[b.day.eq('2026-10-16'),'sid']=3
    assert search.selection_key(a,['2026-09-18'])==search.selection_key(b,['2026-09-18'])
    assert search.selection_key(a)!=search.selection_key(b)
