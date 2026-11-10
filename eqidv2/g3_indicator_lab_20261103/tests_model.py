"""Whole-day train/test separation and transformation causality checks."""
import numpy as np
import pandas as pd
import pytest

from g3_indicator_lab_20261103 import model as m


def synthetic_pool(n_days=25, per_day=6):
    days=pd.bdate_range("2026-07-01",periods=n_days).strftime("%Y-%m-%d").tolist()
    rng=np.random.default_rng(71)
    rows=[]
    for day_index,day in enumerate(days):
        for stock in range(per_day):
            sid=day_index*per_day+stock
            signal=pd.Timestamp(day+" 10:00",tz="Asia/Kolkata")+pd.Timedelta(minutes=stock*5)
            rows.append(dict(sid=sid,day=day,signal_ts=signal,side="LONG" if stock%2==0 else "SHORT",
                filled=True,net_return_pct=.5 if stock%2==0 else -.4,
                entry_ts=signal+pd.Timedelta(minutes=2),exit_ts=signal+pd.Timedelta(minutes=20),
                v18_nifty_from_open_pct=float(rng.normal()),v18_breadth_above_open_pct=55.,
                v18_breadth_below_open_pct=45.,v18_breadth_coverage_pct=100.,
                v18_relative_strength_pct=float(rng.normal()),v18_vwap_distance_pct=float(rng.normal()),
                v18_vwap_slope_pct=float(rng.normal()),v18_efficiency_6=float(rng.random()),
                oi_change_pct=float(rng.random()),volume_ratio=1.+float(rng.random()),
                v9_1m_volume_ratio=1.+float(rng.random()),lab_tod_rvol=1.+float(rng.random()),
                body_ratio=.4+.1*stock,wick_ratio=.2,lab_extension_atr=.5,hhmm_int=1000+5*stock))
    return pd.DataFrame(rows),days


@pytest.fixture(scope="module")
def fitted_pair():
    pool,days=synthetic_pool()
    first=m.fit_predict_walkforward(pool,days,m.PACKAGE/"unused_test_output",write_reports=False)
    altered=pool.copy()
    future=altered.day.isin(days[20:])
    altered.loc[future,"net_return_pct"]=-altered.loc[future,"net_return_pct"]
    second=m.fit_predict_walkforward(altered,days,m.PACKAGE/"unused_test_output",write_reports=False)
    return pool,days,first,second


def test_chronological_readiness_and_mature_label_boundary(fitted_pair):
    pool,days,predictions,_=fitted_pair
    earlier=predictions.day.isin(days[:20])
    assert not predictions.loc[earlier,"model_ready"].any()
    assert predictions.loc[earlier,list(m.MODELS)].isna().all().all()
    assert predictions.loc[~earlier,"model_ready"].all()
    folds=predictions.attrs["diagnostics"]["folds"]
    assert len(folds)==1
    assert folds[0]["train_last_day"]==days[19]
    assert folds[0]["test_first_day"]==days[20]
    assert pd.Timestamp(folds[0]["max_train_exit"])<pd.Timestamp(folds[0]["first_test_signal"])
    assert folds[0]["train_filled_labels"]==120


def test_evaluation_labels_never_change_training_or_scores(fitted_pair):
    _,_,original,altered=fitted_pair
    pd.testing.assert_frame_equal(original,altered,check_flags=False)
    assert original.attrs["diagnostics"]["training_transforms"]==altered.attrs["diagnostics"]["training_transforms"]
    # Predictive metrics must respond, even though fitted scores cannot.
    before=original.attrs["diagnostics"]["metrics"][0]["brier"]
    after=altered.attrs["diagnostics"]["metrics"][0]["brier"]
    assert before!=after


def test_imputer_and_scaler_are_fit_only_on_training_rows(fitted_pair):
    pool,days,predictions,_=fitted_pair
    x=m.feature_matrix(pool.loc[pool.day.isin(days[:20])])
    medians=x.median().fillna(0).to_numpy()
    imputed=x.fillna(x.median()).fillna(0)
    for transform in predictions.attrs["diagnostics"]["training_transforms"]:
        np.testing.assert_allclose(transform["imputer_medians"],medians)
        np.testing.assert_allclose(transform["scaler_means"],imputed.mean().to_numpy())


def test_feature_contract_excludes_outcomes_and_identity():
    pool,_=synthetic_pool(n_days=1)
    original=m.feature_matrix(pool)
    altered=pool.assign(net_return_pct=9999.,target_pct=9999.,signal_id="WINNER",tradingsymbol="AFTER_OUTCOME",
                        portfolio_net_profit_rupees=9999.)
    pd.testing.assert_frame_equal(original,m.feature_matrix(altered))
    assert tuple(original.columns)==m.FEATURES
    assert len(original.columns)==16
    altered.loc[0,"oi_change_pct"]=np.inf
    assert np.isnan(m.feature_matrix(altered).loc[0,"oi_change_pct"])


def test_unfilled_and_unmatured_labels_are_excluded():
    pool,days=synthetic_pool()
    pool.loc[0,"filled"]=False
    pool.loc[1,"exit_ts"]=pd.Timestamp(days[20]+" 14:00",tz="Asia/Kolkata")
    predictions=m.fit_predict_walkforward(pool,days,m.PACKAGE/"unused_test_output",write_reports=False)
    fold=predictions.attrs["diagnostics"]["folds"][0]
    assert fold["train_filled_labels"]==118
    assert fold["purged_unmatured_labels"]==1
    assert predictions.model_ready.sum()==30


def test_insufficient_history_is_flagged_without_fake_predictions():
    pool,days=synthetic_pool(per_day=2)
    predictions=m.fit_predict_walkforward(pool,days,m.PACKAGE/"unused_test_output",write_reports=False)
    assert not predictions.model_ready.any()
    assert predictions[list(m.MODELS)].isna().all().all()
    assert predictions.attrs["diagnostics"]["folds"][0]["status"]=="INSUFFICIENT_PRIOR_LABELS"


def test_zero_trade_days_count_as_sessions_and_calendar_must_be_sorted():
    pool,days=synthetic_pool(per_day=2)
    # Empty observed session retains its place in the fixed-day clock.
    pool=pool.loc[~pool.day.eq(days[5])]
    predictions=m.fit_predict_walkforward(pool,days,m.PACKAGE/"unused_test_output",write_reports=False)
    assert predictions.attrs["diagnostics"]["folds"][0]["train_sessions"]==20
    with pytest.raises(ValueError,match="chronologically"):
        m.fit_predict_walkforward(pool,days[::-1],m.PACKAGE/"unused_test_output",write_reports=False)


def test_one_class_evaluation_metrics_are_defined_except_auc():
    result=m._metrics(np.array([1,1]),np.array([.6,.8]),.5,model="example")
    assert np.isnan(result["roc_auc"])
    assert np.isfinite(result["log_loss"])
    assert np.isfinite(result["brier"])
    assert sum(row["count"] for row in m._calibration([1,1],[.6,.8],model="example"))==2
