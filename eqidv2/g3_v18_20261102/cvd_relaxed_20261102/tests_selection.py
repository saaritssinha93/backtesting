"""Causal relaxed OR gates, explicit warmup and native replacement checks."""
import numpy as np
import pandas as pd
import pytest
from g3_v18_20261102.cvd_advanced_20261102.tests_selection import row as native_row
from . import selection as s


def row(symbol="A",sid=1,side="SHORT",p=.3,a=.2,**changes):
    sign=1 if side=="LONG" else -1
    values=native_row(symbol,sid,side=side,acceleration=sign*a)
    values['adv_recent15_pressure']=sign*p
    values.update(changes)
    return values


def test_original_parity_and_no_mutation():
    raw=pd.DataFrame([row(),row('B',2,traded_value=900.)]); before=raw.copy(deep=True)
    expected=s.native.select(raw,'control'); actual=s.select_all(raw)
    for a,b in zip(actual['base_control'],expected):
        pd.testing.assert_frame_equal(a,b)
    assert actual['available_control'][0].sid.tolist()==expected[0].sid.tolist()
    pd.testing.assert_frame_equal(raw,before)


@pytest.mark.parametrize('side',['LONG','SHORT'])
@pytest.mark.parametrize('variant,p,a,expected',[
    ('relaxed_and',0,-.05,True),('relaxed_and',-.001,.8,False),
    ('either_nonnegative',-.8,0,True),('either_nonnegative',0,-.8,True),('either_nonnegative',-.001,-.001,False),
    ('either_soft',-.05,-.8,True),('either_soft',-.051,-.051,False),
    ('opposition_veto',-.10,-.05,False),('opposition_veto',-.099,-.8,True),
    ('strong_opposition_veto',-.15,-.10,False),('strong_opposition_veto',-.16,-.099,True),
    ('pressure_tolerance',-.10,-1.8,True),('pressure_tolerance',-.101,1.8,False),
])
def test_signed_rules_exact_boundaries(side,variant,p,a,expected):
    raw=pd.DataFrame([row(side=side,p=p,a=a)])
    assert bool(s.gate(raw,variant)[0].iloc[0]) is expected


@pytest.mark.parametrize('field,value',[
    ('adv_ready',False),('adv_recent15_pressure',np.nan),('adv_recent15_pressure',np.inf),
    ('adv_acceleration',np.nan),('adv_acceleration',2.01),
    ('adv_feature_ts',pd.Timestamp('2026-10-23 09:55',tz='Asia/Kolkata')),
    ('side','UNKNOWN'),('signal_ts',pd.NaT),
])
def test_or_cannot_bypass_missing_late_input(field,value):
    raw=pd.DataFrame([row(p=.8,a=.8)])
    raw[field]=value
    assert s.gate(raw,'base_control')[0].all()
    for variant in s.VARIANTS[1:]:
        assert not s.gate(raw,variant)[0].any()


def test_early_bypass_retains_native_filters_and_source_fields():
    raw=pd.DataFrame([native_row('GOOD',1,clock='09:30',adv_ready=False,adv_acceleration=np.nan,adv_feature_ts=pd.NaT),
                      native_row('BAD_OI',2,clock='09:30',oi_change_pct=-1.,adv_ready=False,adv_acceleration=np.nan)])
    original=s.native.select(raw,'control')[0]
    assert original.tradingsymbol.tolist()==['GOOD']
    result=s.select_all(raw)
    for variant in s.VARIANTS[1:]:
        orders,audit=result[variant]
        assert orders.tradingsymbol.tolist()==['GOOD']
        assert orders.relaxed_gate_reason.eq('NATIVE_G3_WARMUP_BYPASS').all()
        assert not audit.loc[audit.tradingsymbol.eq('BAD_OI'),'v9_filter_pass'].any()


@pytest.mark.parametrize('stamp',['2026-10-23 09:19:00','2026-10-23 09:31:00','2026-10-23 09:30:00.000000001'])
def test_warmup_does_not_bypass_invalid_clock(stamp):
    raw=pd.DataFrame([row(signal_ts=pd.Timestamp(stamp,tz='Asia/Kolkata'),adv_ready=False)])
    assert not s.gate(raw,'either_soft')[0].any()


def test_0945_requires_full_readiness():
    raw=pd.DataFrame([native_row(clock='09:45',adv_ready=False,adv_recent15_pressure=-.1)])
    for variant in s.VARIANTS[1:]:
        assert not s.gate(raw,variant)[0].any()


def test_rejection_precedes_native_ranking_and_admits_replacement():
    raw=pd.DataFrame([row('LEADER',1,p=-.8,a=-.8),row('NEXT',2,traded_value=900.)])
    result=s.select_all(raw)
    assert result['base_control'][0].tradingsymbol.tolist()==['LEADER']
    for variant in s.RULES:
        orders,audit=result[variant]
        assert orders.tradingsymbol.tolist()==['NEXT']
        leader=audit.set_index('tradingsymbol').loc['LEADER']
        assert leader.relaxed_original_selected and leader.relaxed_native_filter_pass
        assert not leader.relaxed_gate_pass and not leader.v9_selected


def test_native_core_priority_is_preserved():
    raw=pd.DataFrame([row('CORE',1,traded_value=900.),row('EXTRA',2,price_change_pct=-.15)])
    assert s.select(raw,'either_soft')[0].tradingsymbol.tolist()==['CORE']
    raw.loc[0,['adv_recent15_pressure','adv_acceleration']]=[.8,.8]
    assert s.select(raw,'either_soft')[0].tradingsymbol.tolist()==['EXTRA']


def test_empty_duplicate_and_unknown_variants():
    raw=pd.DataFrame([row()])
    assert all(orders.empty for orders,_ in s.select_all(raw.iloc[:0]).values())
    with pytest.raises(ValueError):
        s.select_all(pd.concat([raw,raw],ignore_index=True))
    with pytest.raises(ValueError):
        s.gate(raw,'invented')
