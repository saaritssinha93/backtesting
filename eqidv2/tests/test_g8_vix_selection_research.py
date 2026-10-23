import copy

import numpy as np
import pandas as pd
import pytest

from research import g8_vix_selection_research as r
from research.g8_sources import load_sources
from research.g8_vix_data import VixData
from research.g8_vix_selection_combinations import discover_pairs
from research.g8_vix_selection_review import frozen_score


CONFIG=dict(capital_per_entry_rs=100000.,leverage=5.,portfolio_capital_rs=1000000.,initial_stop_pct=1.2)


def orders():
    rows=[]
    for sid,minute,exit_,priority in ((1,1,5,3.),(2,1,3,2.),(3,4,7,1.)):
        rows.append(dict(sid=sid,day="2026-10-01",tradingsymbol="TEST"+str(sid),setup_id="1000_LONG",
            side="LONG",filled=True,entry_price=100.,entry_path_index=0,
            entry_ts=f"2026-10-01 10:0{minute}:00+05:30",exit_ts=f"2026-10-01 10:0{exit_}:00+05:30",
            confirmation_ts="2026-10-01 10:00:00+05:30",gross_return_pct=.6,
            net_return_pct=.55,cost_pct=.05,initial_stop_pct=1.2,picker="max_liquidity",traded_value=priority))
    return pd.DataFrame(rows)


def test_fast_allocator_matches_full_priority_and_capital_release():
    config={**CONFIG,"portfolio_capital_rs":100000.}
    original=orders()
    fast=r.FastSelection(original,config)
    mask=pd.Series({3:True,1:True,2:True})
    book=fast.full_book(mask,"all")
    assert book.portfolio_executed.tolist()==[True,False,False]
    mask.loc[1]=False
    # The second trade now receives capital and exits before trade 3 enters.
    assert fast.full_book(mask,"veto_first").portfolio_executed.tolist()==[False,True,True]
    m=fast.stats(fast.execution(mask),["2026-10-01"],10.)
    assert m["net_pnl_rs"]==pytest.approx(5000.)


def test_fast_allocator_includes_same_timestamp_exit_release_and_unfilled():
    original=orders()
    original.loc[0,"exit_ts"]="2026-10-01 10:04:00+05:30"
    original.loc[1,"filled"]=False
    fast=r.FastSelection(original,{**CONFIG,"portfolio_capital_rs":100000.})
    assert fast.full_book(pd.Series({1:True,2:True,3:True}),"same_clock").portfolio_executed.tolist()==[True,False,True]
    with pytest.raises(ValueError,match="Missing selection"):
        fast.execution(pd.Series({1:True}))


def test_pair_logic_and_base_gate_remain_causal():
    f=pd.DataFrame(dict(nifty_base_allowed=[True,True,False],side_sign=[1,-1,1],
        nifty_both_aligned=[False,True,True],vix_15_pct=[2.,2.,2.],vix_session_pct=[2.,2.,2.],
        vix_previous_close_pct=[0.,-3.,0.]))
    a=dict(id="a",mode="direction",window=15,threshold=1.)
    b=dict(id="b",mode="overnight_direction",threshold=1.)
    both=dict(mode="combined",join="both_filters",first=a,second=b)
    consensus=dict(both,join="consensus_veto")
    assert r.allowed_for(f,both).tolist()==[False,False,False]
    assert r.allowed_for(f,consensus).tolist()==[True,True,False]
    changed=f.assign(future_profit=[1e9,-1e9,0])
    pd.testing.assert_series_equal(r.allowed_for(f,consensus),r.allowed_for(changed,consensus))


def record(name=r.BASELINE,**changes):
    return dict(dict(policy=name,period="development",net_pnl_rs=10000.,win_rate_pct=75.,trades=100,
        changed_executions=10,changed_days=5,negative_trade_sum_abs_rs=3000.,losing_days=3,
        daily_close_drawdown_rs=1000.),**changes)


def test_select_requires_both_profit_and_win_improvement_and_excludes_old_winners():
    records=[record(),record("win_only",net_pnl_rs=9000.,win_rate_pct=90.),
        record("profit_only",net_pnl_rs=12000.,win_rate_pct=74.),
        record("previous_historical_best",net_pnl_rs=999999.,win_rate_pct=99.),
        record("candidate",net_pnl_rs=11000.,win_rate_pct=78.)]
    assert r.train_choice(pd.DataFrame(records))=="candidate"
    records[-1]["changed_days"]=2
    assert r.train_choice(pd.DataFrame(records))==r.BASELINE


def test_zero_activity_validation_cannot_pass():
    base=record()
    c=record("candidate",net_pnl_rs=12000.,win_rate_pct=85.,changed_executions=0)
    assert not r.validation_eligible(c,base)
    c.update(changed_executions=3,changed_days=2)
    assert r.validation_eligible(c,base)
    c["losing_days"]=4
    assert not r.validation_eligible(c,base)


def test_pair_discovery_is_training_only_and_within_budget():
    specs=[dict(id="a",family="trend",mode="direction"),dict(id="b",family="level",mode="level"),
        dict(id="c",family="overnight",mode="overnight_direction")]
    training=[record(),record("a",net_pnl_rs=10500.),record("b",net_pnl_rs=10200.),record("c",net_pnl_rs=10100.)]
    future=[record("a",period="final_test",net_pnl_rs=-1e9),record("b",period="all",net_pnl_rs=1e9)]
    pairs=discover_pairs(specs,pd.DataFrame(training+future))
    assert len(pairs)==6
    assert pairs==discover_pairs(specs,pd.DataFrame(training))
    assert len(pairs)<=12


def test_frozen_model_scoring_matches_exported_logistic_transform():
    f=pd.DataFrame(dict(first=[np.nan,2.],second=[4.,6.]),index=[10,20])
    model=dict(columns=["first","second"],imputer_median=[1.,4.],
        scaler_mean=[1.,4.],scaler_scale=[1.,2.],coefficient=[2.,-1.],intercept=0.)
    score=frozen_score(f,model)
    assert score.loc[10]==pytest.approx(.5)
    assert score.loc[20]==pytest.approx(1./(1.+np.exp(-1.)))
    pd.testing.assert_series_equal(score,frozen_score(f.assign(future_outcome=[-1e9,1e9]),model))


def test_model_fits_only_training_outcomes():
    rng=np.random.default_rng(31)
    n=240
    names=list(dict.fromkeys(r.VIX_FEATURES+r.CONTEXT_FEATURES))
    f=pd.DataFrame(rng.normal(size=(n,len(names))),columns=names,index=np.arange(1,n+1))
    f["sid"]=f.index
    f["day"]=np.repeat(pd.bdate_range("2026-07-01",periods=24).strftime("%Y-%m-%d"),10)
    f["nifty_base_allowed"]=True
    original=pd.DataFrame(dict(sid=f.index,filled=True,net_return_pct=np.where(np.arange(n)%4==0,-1.,1.)))
    train=sorted(f.day.unique())[:15]
    m,p=r.model_masks(f,original,train)
    altered=original.copy()
    altered.loc[150:,"net_return_pct"]*=-1000
    m2,p2=r.model_masks(f,altered,train)
    assert p==p2
    for name in m:
        pd.testing.assert_series_equal(m[name],m2[name])


def test_real_vix_features_ignore_future_bars_and_lag_current_day():
    source=load_sources()
    selected=source["unfiltered"].loc[source["unfiltered"].day.eq("2026-10-22")].iloc[[0]]
    source={**source,"unfiltered":selected,"audit":source["audit"].loc[source["audit"].sid.isin(selected.sid)]}
    vix=VixData()
    original=r.build_features(source,vix)
    stamp=pd.Timestamp(selected.iloc[0].confirmation_ts)
    changed=copy.deepcopy(vix)
    frame=changed.days["2026-10-22"]
    frame.loc[frame.available_at.gt(stamp),["open","high","low","close"]]=1000.
    pd.testing.assert_frame_equal(original,r.build_features(source,changed))
    assert original.vix_level_last_reference_day.iloc[0] < "2026-10-22"


def test_cutoff_rejected_before_writes(tmp_path):
    output=tmp_path/"no_output"
    with pytest.raises(ValueError,match="cutoff"):
        r.run("2026-10-23",output)
    assert not output.exists()
