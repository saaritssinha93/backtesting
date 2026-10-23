import numpy as np
import pandas as pd
import pytest

from research import g8_regime_exit_research as r
from research.g8_exit_engine import allocate_variant


def features():
    return pd.DataFrame(dict(vix_level_ratio=[1.2,1.2,1.2,.9,.9,.9],
        vix_5m_pct=[1.,.2,.2,.2,.2,0.],signed_nifty_slot=[.1,.1,0.,.1,0.,-.05],
        signed_nifty_session=[.2,.2,.2,.2,0.,.10],vix_level_ready=[True]*6,
        baseline_stop_pct=[1.2]*6,baseline_target_pct=[.8]*6),index=[1,2,3,4,5,6])


def test_regime_precedence_and_boundary_values():
    f=features()
    assert r.classify(f).tolist()==[r.REGIMES[0],r.REGIMES[1],r.REGIMES[2],r.REGIMES[3],r.REGIMES[4],r.REGIMES[0]]
    f.loc[1,"vix_5m_pct"]=-1.
    assert r.classify(f).loc[1]==r.REGIMES[0]
    f.loc[1,"vix_level_ratio"]=np.nan
    with pytest.raises(ValueError,match="Missing causal"):
        r.classify(f)


def test_wider_stop_reduces_size_and_no_tighter_stop_leverage_increase():
    f=features()
    p=r.settings(f,dict(actions={r.REGIMES[1]:r.action(1.25,.75),r.REGIMES[0]:r.action(.75,.5)}))
    assert p.loc[2,"stop_pct"]==pytest.approx(1.5)
    assert p.loc[2,"quantity_scale"]==pytest.approx(.8)
    assert p.loc[1,"quantity_scale"]==1.
    assert p.loc[1,"target_pct"]==pytest.approx(.4)


def source():
    times=pd.date_range("2026-10-01 10:01","2026-10-01 15:15",freq="min",tz="Asia/Kolkata")
    p=dict(timestamp_ns=times.as_unit("ns").asi8,open=np.full(len(times),100.),
        high=np.full(len(times),100.1),low=np.full(len(times),99.9),close=np.full(len(times),100.))
    p["high"][0]=100.6; p["close"][0]=100.5
    p["high"][1]=100.5; p["close"][1]=100.4
    for k,v in dict(open=99.8,high=99.9,low=99.7,close=99.8).items():p[k][2]=v
    config=dict(initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
        capital_per_entry_rs=100000.,leverage=5.,portfolio_capital_rs=1000000.,model_cost_bps_round_trip=5.)
    row=dict(sid=1,setup_id="1000_LONG",day="2026-10-01",tradingsymbol="TEST",side="LONG",
        filled=True,entry_price=100.,entry_path_index=0,entry_ts=times[0],confirmation_ts=times[0]-pd.Timedelta(minutes=1),
        exit_ts=times[-1],gross_return_pct=0.,net_return_pct=-.05,cost_pct=.05,
        initial_stop_pct=1.2,native_target_pct=.8,picker="max_liquidity",traded_value=100000.)
    return dict(unfiltered=pd.DataFrame([row]),paths={1:p},config=config)


def test_profit_lock_uses_next_bar_and_gap_can_still_lose():
    s=source(); f=features().loc[[1]]
    p=r.settings(f,dict(actions={r.REGIMES[0]:r.action(protect=True)}))
    raw=r.replay(s,p)
    assert raw.loc[0,"protection_armed_path_index"]==1
    assert raw.loc[0,"exit_path_index"]==2
    assert raw.loc[0,"exit_event"]=="OPEN"
    assert raw.loc[0,"gross_return_pct"]==pytest.approx(-.2)
    book=allocate_variant(raw,pd.Series({1:True}),s["config"],"test")
    assert book.loc[0,"portfolio_net_profit_rupees"]==pytest.approx(-1250.)


def test_baseline_does_not_arm_protection_on_entry_candle():
    s=source(); f=features().loc[[1]]
    raw=r.replay(s,r.settings(f,dict(actions={})))
    assert raw.loc[0,"exit_reason"]=="TIME_EXIT_1515"
    assert not bool(raw.loc[0,"protection_applied"])


def test_selection_never_reads_assessment_outcomes():
    b=dict(net_pnl_rs=100.,negative_trade_sum_abs_rs=20.,win_rate_pct=75.,losing_days=2,
        daily_close_drawdown_rs=10.,trades=100,changed_trades=0,changed_days=0,losing_day_sum_abs_rs=15.)
    c={**b,"net_pnl_rs":99.,"negative_trade_sum_abs_rs":18.,"changed_trades":6,"changed_days":3,"losing_day_sum_abs_rs":12.}
    rows=[dict(policy=name,period=period,**row) for period in ("development","validation","assessment") for name,row in ((r.BASE,b),("candidate",c))]
    trial=pd.DataFrame(rows)
    assert r.select(trial)==("candidate","candidate")
    trial.loc[trial.period.eq("assessment"),"net_pnl_rs"]=-1e9
    assert r.select(trial)==("candidate","candidate")
    trial.loc[trial.period.eq("validation")&trial.policy.eq("candidate"),"changed_days"]=1
    assert r.select(trial)==("candidate",r.BASE)
