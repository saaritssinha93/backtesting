"""Protect the causal/economic boundary of the exit feasibility dataset."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_exit_dataset as dataset
from research import g3_trade_monitor_parameter_search as search
from tests.test_g3_trade_monitor_parameter_search import DAY, _inputs


def _fixture(*, count=1, side="LONG"):
    _, paths, baseline = _inputs(count=count, side=side)
    return baseline, search.build_feature_cache(baseline, paths)


def test_predictors_exclude_future_execution_and_outcome_information():
    baseline, features = _fixture()
    predictors, labels, _ = dataset.build_dataset(baseline, features)

    forbidden = {
        "next_bar_open_price", "next_bar_open_at_ist", "modeled_execution_at_ist",
        "execution_path_index", "original_exit_at_ist", "exit_ts", "exit_execution_ts",
        "exit_price", "exit_reason", "portfolio_net_profit_rupees",
    }
    assert not forbidden.intersection(predictors.columns)
    assert not any(column.startswith("label_") for column in predictors)
    assert set(predictors.completed_bar_count) == {6, 15, 30}
    assert set(labels.sid) == set(predictors.sid)

    revised_features = features.copy()
    revised_features["next_bar_open_price"] += 0.4
    revised_features["future_oracle_answer"] = 1.0
    revised_baseline = baseline.copy()
    revised_baseline["exit_reason"] = "TARGET"
    revised_baseline["portfolio_net_profit_rupees"] += 1000.0
    revised_predictors, revised_labels, _ = dataset.build_dataset(revised_baseline, revised_features)
    pd.testing.assert_frame_equal(predictors, revised_predictors)
    assert not np.allclose(labels.label_exit_value_after_cost_rs, revised_labels.label_exit_value_after_cost_rs)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("extra_bps", [0.0, 10.0])
def test_label_uses_direction_original_notional_base_cost_and_extra_exit_cost(side, extra_bps):
    baseline, features = _fixture(side=side)
    # Vary position size and base cost to catch accidental hard-coded 500k/250.
    baseline["exposure_per_entry_rupees"] = 200_000.0
    baseline["portfolio_cost_rupees"] = 80.0
    baseline["portfolio_net_profit_rupees"] = -1500.0
    _, labels, _ = dataset.build_dataset(baseline, features, extra_exit_cost_bps=extra_bps)

    sign = 1.0 if side == "LONG" else -1.0
    for row in labels.itertuples(index=False):
        feature = features.loc[
            features.sid.eq(row.sid) & features.completed_bar_count.eq(row.completed_bar_count)
        ].iloc[0]
        trade = baseline.loc[baseline.sid.eq(row.sid)].iloc[0]
        gross = sign * (feature.next_bar_open_price / trade.entry_price - 1.0) * 200_000.0
        expected_delta = gross - 80.0 - (-1500.0) - 200_000.0 * extra_bps / 10_000.0
        assert row.label_native_net_pnl_rs == -1500.0
        assert row.label_exit_value_after_cost_rs == pytest.approx(expected_delta)


def test_target_protection_is_distinct_from_other_profitable_native_exits():
    orders, paths, _ = _inputs(count=2)
    # Trade zero reaches its target after all three checkpoints. Trade one
    # finishes profitable at the scheduled close without reaching its target.
    paths[0]["high"][60] = 102.0
    for field, value in {"open": 100.6, "close": 100.6, "high": 100.61, "low": 100.59}.items():
        paths[1][field][-1] = value
    baseline = search.history._simulate(orders, paths, [DAY], search.history.contract)
    assert baseline.set_index("sid").loc[0, "exit_reason"] == "TARGET"
    assert baseline.set_index("sid").loc[1, "exit_reason"] == "TIME_EXIT_1515"
    assert baseline.portfolio_net_profit_rupees.gt(0).all()
    features = search.build_feature_cache(baseline, paths)
    _, labels, _ = dataset.build_dataset(baseline, features)

    target = labels.loc[labels.sid.eq(0)]
    other_winner = labels.loc[labels.sid.eq(1)]
    assert target.label_native_target.all()
    assert target.label_target_damage_rs.gt(0).all()
    assert not other_winner.label_native_target.any()
    assert other_winner.label_target_damage_rs.eq(0).all()
    assert other_winner.label_exit_value_after_cost_rs.lt(0).all()


def test_missing_landmarks_preserve_executed_cohort_in_coverage():
    baseline, features = _fixture(count=11)
    accepted = set(baseline.loc[baseline.portfolio_executed, "sid"])
    assert len(accepted) == 10
    features = features.loc[features.sid.ne(0)].copy()
    predictors, labels, coverage = dataset.build_dataset(baseline, features)

    assert set(coverage.sid) == accepted
    assert len(coverage) == len(accepted) * 3
    assert not coverage.loc[coverage.sid.eq(0), "eligible"].any()
    assert 0 not in set(predictors.sid)
    assert 10 not in set(predictors.sid)
    assert set(labels.sid) == accepted - {0}


def test_labels_conservatively_mature_at_native_bar_end():
    baseline, features = _fixture()
    actual = pd.Timestamp(f"{DAY} 15:14", tz="Asia/Kolkata")
    baseline["exit_execution_ts"] = actual
    # The archived intrabar fill time is not necessarily precisely observed;
    # use its completed native bar as the conservative label maturity time.
    _, labels, _ = dataset.build_dataset(baseline, features)
    expected = pd.Timestamp(baseline.iloc[0].exit_ts).tz_convert("UTC")
    assert pd.to_datetime(labels.label_available_at_ist, utc=True).eq(expected).all()


def test_initial_risk_normalization_uses_entry_stop_not_later_tightened_stop():
    baseline, features = _fixture()
    baseline["initial_stop_pct"] = 1.25
    baseline["active_stop_pct_at_exit"] = 0.10
    predictors, _, _ = dataset.build_dataset(baseline, features)
    np.testing.assert_allclose(predictors.signed_move_initial_r, predictors.signed_move_pct / 1.25)
    np.testing.assert_allclose(predictors.mfe_initial_r, predictors.mfe_pct / 1.25)
    np.testing.assert_allclose(predictors.momentum_5m_initial_r, predictors.momentum_5m_pct / 1.25)


def test_duplicate_trade_identity_is_rejected():
    baseline, features = _fixture()
    duplicated = pd.concat([baseline, baseline], ignore_index=True)
    with pytest.raises(ValueError):
        dataset.build_dataset(duplicated, features)


def test_duplicate_requested_checkpoint_is_rejected():
    baseline, features = _fixture()
    duplicated = pd.concat([features, features.loc[features.completed_bar_count.eq(6)]], ignore_index=True)
    with pytest.raises(ValueError):
        dataset.build_dataset(baseline, duplicated)


def test_unknown_trade_identity_is_rejected():
    baseline, features = _fixture()
    features.loc[features.completed_bar_count.eq(6), "sid"] = 9999
    with pytest.raises(ValueError):
        dataset.build_dataset(baseline, features)


def test_multiple_checkpoints_do_not_give_longer_observed_trades_more_weight():
    baseline, features = _fixture(count=2)
    features = features.loc[~(features.sid.eq(0) & features.completed_bar_count.eq(30))].copy()
    predictors, _, _ = dataset.build_dataset(baseline, features)
    assert predictors.groupby("sid").size().to_dict() == {0: 2, 1: 3}
    np.testing.assert_allclose(predictors.groupby("sid").sample_weight.sum(), 1.0)


@pytest.mark.parametrize("field,value", [("tradingsymbol", "WRONG_SYMBOL"), ("side", "SHORT"), ("day", "2026-10-12"), ("source_signal_id", "WRONG_SIGNAL")])
def test_mismatched_trade_identity_is_rejected(field, value):
    baseline, features = _fixture()
    features.loc[features.completed_bar_count.eq(6), field] = value
    with pytest.raises(ValueError):
        dataset.build_dataset(baseline, features)


def test_build_does_not_mutate_inputs():
    baseline, features = _fixture()
    before_baseline, before_features = baseline.copy(deep=True), features.copy(deep=True)
    dataset.build_dataset(baseline, features)
    pd.testing.assert_frame_equal(baseline, before_baseline)
    pd.testing.assert_frame_equal(features, before_features)
