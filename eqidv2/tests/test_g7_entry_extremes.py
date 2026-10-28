import numpy as np
import pandas as pd
import pytest

from research.g7_entry_extremes_20261026 import causal_features, stretch_decisions, path_diagnostics


def sample():
    ts = pd.date_range('2026-10-26 09:16',periods=40,freq='min',tz='Asia/Kolkata')
    c = np.arange(40)*.1+100
    raw = pd.DataFrame(dict(ts=ts,open=c-.1,high=c+.2,low=c-.2,close=c,volume=100))
    b = raw.iloc[24]
    row = dict(sid=1,side='LONG',confirmation_ts=b.ts,trigger=b.high)
    row.update({'confirmation_'+k:b[k] for k in ['open','high','low','close']})
    return raw,row


def test_future_prices_cannot_change_entry_features():
    raw,row = sample()
    before = causal_features(raw,row)
    raw.loc[raw.ts.gt(row['confirmation_ts']),['open','high','low','close','volume']] = np.nan
    assert causal_features(raw,row) == before


@pytest.mark.parametrize('problem',['missing','duplicate','confirmation','synthetic'])
def test_bad_evidence_is_not_a_profitable_filter(problem):
    raw,row=sample()
    if problem=='missing': raw=raw.drop(index=3)
    if problem=='duplicate': raw=pd.concat([raw,raw.iloc[[3]]])
    if problem=='confirmation': row['confirmation_close']+=.1
    if problem=='synthetic': raw['is_synthetic']=False; raw.loc[20,'is_synthetic']=True
    with pytest.raises(ValueError): causal_features(raw,row)


def test_symmetric_side_and_known_trigger():
    raw,row=sample()
    long=causal_features(raw,row)
    row['side']='SHORT'
    short=causal_features(raw,row)
    assert short['trigger_extension_atr']==-long['trigger_extension_atr']
    assert long['trigger_extension_atr']>long['close_extension_atr']


def test_fixed_limit_boundary_and_index_gate():
    f=pd.DataFrame(dict(allowed_g7_v2=[True]*4+[False],trigger_extension_atr=[3,3.001,4,4.001,1],
        feature_at=['2026-10-26 10:00+05:30']*5,confirmation_ts=['2026-10-26 10:00+05:30']*5))
    d=stretch_decisions(f)
    assert d['trigger_max3atr'].tolist()==[True,False,False,False,False]
    assert d['trigger_max4atr'].tolist()==[True,True,True,False,False]
    f.loc[0,'trigger_extension_atr']=np.nan
    with pytest.raises(ValueError): stretch_decisions(f)


def test_outcome_diagnostics_exclude_ambiguous_entry_exit_bars():
    raw,row=sample()
    row.update(entry_ts=raw.ts.iloc[25],exit_ts=raw.ts.iloc[29],entry_price=102.5)
    result=path_diagnostics(raw,row)
    assert result['best_completed_close_time']==raw.ts.iloc[28]
    assert np.isnan(result['completed_close_return_after_5m_pct'])
    assert result['completed_close_return_after_1m_pct']>0


@pytest.mark.parametrize('stamp',['2026-10-26 10:01+05:30',None])
def test_feature_timestamp_must_exist_and_not_look_ahead(stamp):
    f=pd.DataFrame(dict(allowed_g7_v2=[True],trigger_extension_atr=[1],
        feature_at=[stamp],confirmation_ts=['2026-10-26 10:00+05:30']))
    with pytest.raises(ValueError): stretch_decisions(f)
