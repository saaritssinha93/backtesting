import pandas as pd
import numpy as np
import pytest

from research import g4_vwap_rsi_validation as study
from tests.test_g4_g3_mirror_backtest import candidates


def test_filter_precedes_quota_and_keeps_existing_atr_guard():
    pool=candidates()
    pool['vwap_rsi_reject']=[True,False]
    assert study.select(pool,{},False).tradingsymbol.tolist()==['A']
    assert study.select(pool,{},True).tradingsymbol.tolist()==['B']
    assert study.select(pool,{'1236_SHORT':2.9},True).empty


def test_not_applicable_warmup_keeps_baseline_selection():
    pool=candidates()
    pool['vwap_rsi_reject']=False
    pool['vwap_rsi_status']='NOT_APPLICABLE'
    assert study.select(pool,{},True).tradingsymbol.tolist()==['A']


def test_changed_trade_accounting_includes_shared_allocations():
    def frame(values):
        return pd.DataFrame([dict(sid=sid,portfolio_executed=True,portfolio_net_profit_rupees=pnl,
            day='2026-10-14',setup_id='1236_SHORT',tradingsymbol=str(sid),side='SHORT',exit_reason='SQUARE_OFF')
            for sid,pnl in values])
    old=frame([(1,-100),(2,200)])
    new=frame([(2,150),(3,300)])
    changes=study.trade_changes(old,new)
    assert set(changes.action)=={'REMOVED_EXECUTED','ADDED_EXECUTED','ALLOCATION_CHANGED'}
    assert changes.delta_net_pnl_rs.sum()==350
    assert study.trade_changes(old,old).empty


def test_archived_trigger_precision_preserves_one_ulp_and_new_candidates():
    frame = pd.DataFrame([dict(sid=sid,day='2026-08-14',setup_id='1016_LONG',
        tradingsymbol=str(sid),side='LONG',trigger=14.100000381469727) for sid in (1,2)])
    prior = frame.iloc[:1].copy()
    prior['trigger'] = np.nextafter(prior.trigger, np.inf)
    prior['entry_ts'] = 'must never be copied'
    actual, audit = study.pin_trigger_precision(frame,[('g4',prior),('g3',frame.iloc[:1])])
    assert actual.loc[0,'trigger'] == prior.loc[0,'trigger']
    assert actual.loc[1,'trigger'] == frame.loc[1,'trigger']
    assert 'entry_ts' not in actual
    assert len(audit) == 1 and audit[0]['ulp_difference'] == 1 and audit[0]['source'] == 'g4'
    prior['trigger'] = prior.trigger + 0.01
    with pytest.raises(ValueError,match='one ULP'):
        study.pin_trigger_precision(frame,[('g4',prior)])


def test_archived_trigger_rejects_wrong_candidate_identity():
    frame = pd.DataFrame([dict(sid=1,day='2026-08-14',setup_id='1016_LONG',
        tradingsymbol='A',side='LONG',trigger=100.)])
    prior = frame.assign(tradingsymbol='B')
    with pytest.raises(ValueError,match='identity'):
        study.pin_trigger_precision(frame,[('g4',prior)])
