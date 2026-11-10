"""Availability, fixed-score bounds and complete OR-path checks."""
import numpy as np
import pandas as pd
import pytest

from g3_indicator_lab_20261103 import features as f


def bars(day="2026-10-23", volumes=(100.,200.,300.,400.)):
    stamps=pd.date_range(day+" 09:20",periods=len(volumes),freq="5min",tz="Asia/Kolkata")
    return pd.DataFrame(dict(tradingsymbol="A",signal_ts=stamps,volume=volumes,
        high=np.arange(len(volumes))+101.,low=np.arange(len(volumes))+99.,
        ema20=np.arange(len(volumes))*.1+100.,source_1m_count=5))


def test_same_clock_reference_needs_five_earlier_sessions_and_excludes_current():
    history={}
    for day in pd.bdate_range("2026-10-12",periods=5).strftime("%Y-%m-%d"):
        out=f.add_day(bars(day),history)
        assert out.lab_tod_rvol.isna().all()
    sixth=f.add_day(bars(volumes=(1000.,200.,300.,400.)),history)
    assert sixth.lab_tod_count.tolist()==[5,5,5,5]
    np.testing.assert_allclose(sixth.lab_tod_rvol,[10.,1.,1.,1.])
    # The denominator belongs to this stock/clock; another clock cannot leak in.
    assert list(history[("A","09:20")])==[100.,100.,100.,100.,100.,1000.]


def test_opening_range_available_only_after_three_completed_bars():
    raw=bars(volumes=(100.,200.,300.,400.,500.))
    out=f.add_day(raw,{})
    assert out.lab_or_high.iloc[:3].isna().all()
    assert out.lab_or_low.iloc[:3].isna().all()
    assert out.lab_or_high.iloc[3:].eq(103.).all()
    assert out.lab_or_low.iloc[3:].eq(99.).all()
    changed=raw.copy()
    changed.loc[4,"high"]=1_000_000.
    changed.loc[4,"low"]=.001
    later=f.add_day(changed,{})
    pd.testing.assert_series_equal(out.lab_or_high,later.lab_or_high)
    pd.testing.assert_series_equal(out.lab_or_low,later.lab_or_low)


def test_incomplete_opening_range_never_receives_future_substitute():
    raw=bars().drop(index=1)
    out=f.add_day(raw,{})
    assert out.lab_or_high.isna().all()
    assert out.lab_or_low.isna().all()


def market_row(side="LONG",**changes):
    row=dict(side=side,v18_stock_ready=True,v18_market_ready=True,v18_breadth_coverage_pct=100.,
        v18_breadth_above_open_pct=65.,v18_breadth_below_open_pct=65.,
        v18_nifty_from_open_pct=.4 if side=="LONG" else -.4,
        v18_relative_strength_pct=.2 if side=="LONG" else -.2,volume_ratio=1.2)
    row.update(changes)
    return row


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_market_path_is_symmetric_and_requires_all_conditions(side):
    assert f.market_path(pd.DataFrame([market_row(side)])).iloc[0]
    for changes in ({"v18_stock_ready":False},{"v18_market_ready":False},
                    {"v18_breadth_coverage_pct":89.},{"volume_ratio":1.19},
                    {"v18_nifty_from_open_pct":0.},{"v18_relative_strength_pct":np.nan},
                    {"v18_breadth_above_open_pct":20.,"v18_breadth_below_open_pct":20.}):
        assert not f.market_path(pd.DataFrame([market_row(side,**changes)])).iloc[0]


def test_grouped_score_bounded_and_missing_group_inputs_neutral():
    raw=pd.DataFrame([dict(side="LONG",hhmm_int=1000),dict(side="SHORT",hhmm_int=1200)])
    out=f.grouped_scores(raw)
    columns=["score_market","score_strength","score_participation","score_entry","group_score"]
    assert out[columns].eq(.5).all().all()
    raw=raw.assign(v18_nifty_from_open_pct=1_000.,v18_breadth_above_open_pct=100.,
        v18_breadth_below_open_pct=0.,v18_relative_strength_pct=1_000.,volume_ratio=1_000.,
        oi_change_pct=1_000.,body_ratio=1.,wick_ratio=0.,confirm_close=100.,
        v9_1m_ema20=99.,v9_1m_atr14=.1)
    out=f.grouped_scores(raw)
    assert out[columns].ge(0).all().all() and out[columns].le(1).all().all()
    np.testing.assert_allclose(out.group_score,out[columns[:-1]].mean(axis=1))
