import pandas as pd
import pytest

from research import g4_rejection_validation as study
from tests.test_g4_g3_mirror_backtest import candidates


def sample(side='LONG'):
    stamp=pd.Timestamp('2026-10-14 12:36',tz=study.base.production.IST)
    raw=pd.DataFrame(dict(ts=pd.date_range(stamp-pd.Timedelta(minutes=10),periods=12,freq='min'),
        open=100.,high=101.,low=99.,close=100.))
    if side=='LONG':
        raw.loc[10,'high']=102.
    else:
        raw.loc[10,'low']=98.
    row=pd.Series(dict(side=side,confirmation_ts=stamp,
        **{'confirmation_'+name:raw.loc[10,name] for name in ['open','high','low','close']}))
    return raw,row


@pytest.mark.parametrize('side',['LONG','SHORT'])
def test_rejection_is_symmetric_and_uses_only_completed_prior_bars(side):
    raw,row=sample(side)
    first=study.rejection_features(raw,row)
    assert first['rejection_inputs_valid'] and first['rejection_flag']
    assert first['prior_10_high']==101 and first['prior_10_low']==99
    raw.loc[11,['open','high','low','close']]=1e9
    assert study.rejection_features(raw,row)==first


@pytest.mark.parametrize('side,boundary',[('LONG',101.),('SHORT',99.)])
def test_close_back_at_boundary_or_breakout_held_does_not_reject(side,boundary):
    raw,row=sample(side)
    row['confirmation_close']=boundary;raw.loc[10,'close']=boundary
    assert not study.rejection_features(raw,row)['rejection_flag']
    field='high' if side=='LONG' else 'low'
    row['confirmation_'+field]=boundary;raw.loc[10,field]=boundary
    row['confirmation_close']=100.;raw.loc[10,'close']=100.
    assert not study.rejection_features(raw,row)['rejection_flag']


def test_missing_or_flagged_prior_bar_is_unavailable_and_ohlc_drift_raises():
    raw,row=sample()
    assert not study.rejection_features(raw.drop(index=4),row)['rejection_inputs_valid']
    raw['gap_filled']=False;raw.loc[3,'gap_filled']=True
    assert not study.rejection_features(raw,row)['rejection_inputs_valid']
    raw.loc[10,'close']=999
    with pytest.raises(ValueError,match='OHLC drift'):
        study.rejection_features(raw,row)


def test_rejection_runs_before_ranking_and_fail_closed_inputs():
    pool=candidates()
    pool['rejection_inputs_valid']=True
    pool['rejection_flag']=[True,False]
    assert study.select(pool,{},False).tradingsymbol.tolist()==['A']
    assert study.select(pool,{},True).tradingsymbol.tolist()==['B']
    pool.loc[1,'rejection_inputs_valid']=False
    assert study.select(pool,{},True).empty
