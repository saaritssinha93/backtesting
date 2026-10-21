"""Causality, accounting and portable model checks for isolated G5 research."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research import g5_ml_backtest as g5
from research import g5_ml_shadow as shadow


@pytest.fixture
def config():
    return json.loads(g5.DEFAULT_CONFIG.read_text(encoding="utf-8"))


def context_fixture(day="2026-10-15"):
    stamp = pd.Timestamp(day + " 10:00", tz="Asia/Kolkata")
    context = pd.DataFrame(dict(
        ts=[stamp - pd.Timedelta(minutes=1), stamp, stamp + pd.Timedelta(minutes=1)],
        open=[99., 100., 101.], high=[101., 102., 103.],
        low=[98., 99., 100.], close=[100., 101., 102.],
        atr14=[1., 1., 1.], ema20=[99., 100., 101.],
        session_vwap=[99., 100., 101.], roc3_atr=[1., 1., 1.],
        ema_slope3_atr=[.2, .2, .2], relvol20=[1., 2., 3.], relvol_time=[2., 2., 2.],
        upper_wick_ratio=[.1, .2, .3], lower_wick_ratio=[.3, .2, .1],
        body_ratio=[.5, .5, .5], gap_filled=[False, False, False]))
    row = dict(sid=1, confirmation_ts=stamp, side="LONG", native_target_pct=1.2,
               confirmation_open=100., confirmation_high=102., confirmation_low=99.,
               confirmation_close=101.)
    return pd.DataFrame([row]), {1: context}


def synthetic_features():
    rng = np.random.default_rng(3)
    days = pd.bdate_range("2026-07-01", periods=40).strftime("%Y-%m-%d").tolist()
    frame = pd.DataFrame(rng.normal(size=(400, len(g5.FEATURES))), columns=g5.FEATURES)
    frame["sid"] = np.arange(400)
    frame["day"] = np.repeat(days, 10)
    frame["feature_valid"] = True
    labels = pd.Series(np.where(np.arange(400) % 3 == 0, -1., .7), index=frame.index)
    return frame, labels, days


def test_features_cannot_see_later_bars_or_outcome_columns():
    orders, contexts = context_fixture()
    before = g5.build_entry_features(orders, contexts)
    contexts[1].loc[2, ["close", "atr14", "relvol20"]] = 1e9
    orders["net_return_pct"] = -1e9
    orders["mfe_pct"] = 1e9
    after = g5.build_entry_features(orders, contexts)
    pd.testing.assert_frame_equal(before, after)
    assert before.loc[1, "ema_stretch_atr"] == 1.
    assert before.loc[1, "feature_ts"] == before.loc[1, "decision_ts"]


def test_missing_confirmation_or_mismatch_rejected():
    orders, contexts = context_fixture()
    contexts[1] = contexts[1].drop(index=1)
    with pytest.raises(ValueError, match="Exact confirmation"):
        g5.build_entry_features(orders, contexts)
    orders, contexts = context_fixture()
    orders.loc[0, "confirmation_high"] = 999
    with pytest.raises(ValueError, match="differs"):
        g5.build_entry_features(orders, contexts)


@pytest.mark.parametrize("field,value", [("atr14", 0.), ("relvol20", np.nan), ("gap_filled", True)])
def test_bad_feature_input_defaults_to_baseline(field, value):
    orders, contexts = context_fixture()
    contexts[1].loc[1, field] = value
    features = g5.build_entry_features(orders, contexts)
    assert not features.loc[1, "feature_valid"]


def test_future_labels_and_features_cannot_change_prior_fit(config):
    features, labels, days = synthetic_features()
    model, audit = g5.fit_model(features, labels, days[:30], config)
    changed = features.copy()
    changed.loc[changed.day.isin(days[30:]), list(g5.FEATURES)] = 1e12
    future = labels.copy()
    future.loc[features.day.isin(days[30:])] = -1e12
    other, other_audit = g5.fit_model(changed, future, days[:30], config)
    assert audit == other_audit
    np.testing.assert_allclose(model[-1].coef_, other[-1].coef_, atol=1e-12, rtol=0)
    np.testing.assert_allclose(model[0].mean_, other[0].mean_, atol=1e-12, rtol=0)
    np.testing.assert_allclose(model[0].mean_, features.loc[:299, list(g5.FEATURES)].mean())


def test_numeric_model_matches_sklearn_and_is_label_free(config):
    features, labels, days = synthetic_features()
    model, audit = g5.fit_model(features, labels, days[:30], config)
    saved = g5.serialize_model(model, audit, days[:30])
    result = g5.score_saved_model(features.iloc[300:], saved)
    np.testing.assert_allclose(result, model.predict(features.iloc[300:][list(g5.FEATURES)]), atol=1e-12)
    saved = json.loads(json.dumps(saved, allow_nan=False))
    assert saved["training_end"] == days[29]
    invalid = features.iloc[300:].copy()
    invalid.loc[300, "feature_valid"] = False
    assert np.isnan(g5.score_saved_model(invalid, saved).loc[300])


def test_insufficient_history_and_disabled_models_cannot_skip(config):
    features, labels, days = synthetic_features()
    model, audit = g5.fit_model(features, labels, days[:3], config)
    assert model is None
    saved = g5.serialize_model(model, audit, days[:3])
    assert g5.score_saved_model(features, saved).isna().all()


def test_day_grouped_walkforward_has_no_overlap_or_warmup_evaluation():
    days = pd.bdate_range("2026-07-01", periods=53).strftime("%Y-%m-%d").tolist()
    split = list(g5.folds(days))
    assert [len(test) for _, _, test in split] == [5, 5, 5, 5, 3]
    assert [day for _, _, test in split for day in test] == days[30:]
    for _, train, test in split:
        assert train[-1] < test[0]
        assert not set(train) & set(test)
    with pytest.raises(ValueError):
        list(g5.folds(days[::-1]))


def native_fixture():
    return pd.DataFrame([
        dict(sid=1, day="2026-10-15", tradingsymbol="ABC", side="LONG", filled=True,
             confirmation_ts="2026-10-15 09:59:00+05:30", entry_ts="2026-10-15 10:00:00+05:30",
             exit_ts="2026-10-15 10:10:00+05:30", gross_return_pct=1., net_return_pct=.95,
             cost_pct=.05, initial_stop_pct=1.2, picker="max_move", price_change_pct=1.,
             exit_reason="TARGET", entry_price=100., exit_price=101.),
        dict(sid=2, day="2026-10-15", tradingsymbol="XYZ", side="SHORT", filled=True,
             confirmation_ts="2026-10-15 10:00:00+05:30", entry_ts="2026-10-15 10:01:00+05:30",
             exit_ts="2026-10-15 10:11:00+05:30", gross_return_pct=-1.2, net_return_pct=-1.25,
             cost_pct=.05, initial_stop_pct=1.2, picker="max_move", price_change_pct=1.,
             exit_reason="STOP", entry_price=100., exit_price=101.2),
    ])


def test_variable_sizing_scales_capital_exposure_costs_and_replays_capacity(config):
    native = native_fixture()
    config = {**config, "portfolio_capital_rs": 100000.}
    full, _ = g5.allocate(native, pd.Series({1: 1., 2: 1.}), config)
    assert full.portfolio_executed.tolist() == [True, False]
    half, _ = g5.allocate(native, pd.Series({1: .5, 2: .5}), config)
    assert half.portfolio_executed.all()
    np.testing.assert_allclose(half.capital_per_entry_rupees, [50000., 50000.])
    np.testing.assert_allclose(half.portfolio_cost_rupees, [125., 125.])
    np.testing.assert_allclose(half.portfolio_net_profit_rupees, [2375., -3125.])
    skip, _ = g5.allocate(native, pd.Series({1: 0., 2: 1.}), config)
    assert skip.portfolio_executed.tolist() == [False, True]
    assert skip.iloc[0].portfolio_status == "G5_SKIPPED"
    stressed, _ = g5.allocate(native, pd.Series({1: .5, 2: .5}), config, 15.)
    np.testing.assert_allclose(stressed.portfolio_cost_rupees, [375., 375.])


def test_minute_equity_converges_to_final_net_and_records_cost_at_entry(config):
    native = native_fixture()
    book, _ = g5.allocate(native, pd.Series({1: 1., 2: 0.}), config)
    grid = pd.date_range("2026-10-15 10:00", "2026-10-15 15:15", freq="min", tz="Asia/Kolkata")
    paths = {1: {"timestamp_ns": grid.as_unit("ns").asi8, "close": np.full(len(grid), 100.)}}
    curve = g5.minute_equity(book, paths, ["2026-10-15"])
    assert curve.iloc[-1].equity_pnl_rs == 4750.
    assert curve.loc[curve.timestamp.eq("2026-10-15 10:00:00+05:30"), "session_pnl_rs"].iloc[0] == -250.


def test_exit_choice_uses_only_past_book_rows(config):
    config = {**config, "minimum_training_fills": 1,
              "exit_selection": {**config["exit_selection"], "minimum_changed_trades": 1}}
    baseline, _ = g5.allocate(native_fixture(), pd.Series({1: 1., 2: 1.}), config)
    candidate = baseline.copy()
    candidate.loc[candidate.sid.eq(2), "portfolio_net_profit_rupees"] = -1000.
    books = {"baseline": baseline, "protective": candidate}
    selected, _ = g5.choose_exit(books, ["2026-10-15"], config)
    assert selected == "protective"
    future = candidate.iloc[:1].copy()
    future["sid"], future["day"], future["portfolio_net_profit_rupees"] = 3, "2026-10-16", -1e9
    books["protective"] = pd.concat([candidate, future], ignore_index=True)
    assert g5.choose_exit(books, ["2026-10-15"], config)[0] == selected


def test_shadow_refuses_training_period_and_allows_only_later_sessions(config):
    orders, contexts = context_fixture()
    frame = contexts[1].assign(sid=1)
    saved = dict(features=list(g5.FEATURES), enabled=False, training_end="2026-10-15",
                 next_block_exit_policy="baseline")
    with pytest.raises(ValueError, match="strictly after"):
        shadow.score(saved, orders, frame)
    saved["training_end"] = "2026-10-14"
    scored = shadow.score(saved, orders, frame)
    assert scored.suggested_size_multiplier.eq(1).all()
    assert not scored.suggested_skip.any()
    assert not scored.execution_authority.any()


def test_bootstrap_paired_constant_difference_is_exact(config):
    result = g5.block_interval(np.full(23, -10.), config)
    assert result["lower_95_delta_rs"] == result["upper_95_delta_rs"] == -230.


def test_acceptance_cannot_call_profit_sacrifice_growth(config):
    baseline = dict(net_pnl_rs=100., negative_trade_sum_abs_rs=50., daily_close_drawdown_rs=20.)
    candidate = dict(net_pnl_rs=90., negative_trade_sum_abs_rs=10., daily_close_drawdown_rs=5.)
    assert g5.acceptance(candidate, baseline, config) == "DOES_NOT_PASS"
    candidate["net_pnl_rs"] = 96.
    assert g5.acceptance(candidate, baseline, config) == "HISTORICAL_DEFENSIVE_SCREEN_PASS"


def protective_path(side, entry_index=0, gap=False):
    stamps = pd.date_range("2026-10-15 15:10", "2026-10-15 15:15", freq="min", tz="Asia/Kolkata")
    sign = 1 if side == "LONG" else -1
    bars = []
    for index in range(len(stamps)):
        opening = 100. if index == entry_index else 100. - sign * .4
        close = 100. - sign * .4
        if index == entry_index + 2:
            opening = 100. - sign * (2. if gap else .5)
            close = opening
        bars.append((opening, max(opening, close) + .1, min(opening, close) - .1, close))
    return dict(timestamp_ns=stamps.as_unit("ns").asi8,
                **{name: np.asarray([bar[j] for bar in bars])
                   for j, name in enumerate(("open", "high", "low", "close"))})


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_g5_protective_exit_uses_two_closes_and_following_open(side):
    path = protective_path(side)
    result = g5.protective_outcome(dict(side=side, native_target_pct=2.), path, 0, 100.)
    assert result["exit_path_index"] == 2
    assert result["exit_event"] == "OPEN"
    assert result["exit_reason"] == "G5_PROTECTIVE_EXIT"
    assert result["exit_price"] == path["open"][2]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_g5_protective_exit_preserves_native_gap_stop_priority(side):
    path = protective_path(side, gap=True)
    result = g5.protective_outcome(dict(side=side, native_target_pct=2.), path, 0, 100.)
    assert result["exit_reason"] == "STOP"
    assert result["exit_price"] == path["open"][2]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_prefill_adverse_closes_cannot_count_towards_filled_trade_exit(side):
    path = protective_path(side, entry_index=4)
    result = g5.protective_outcome(dict(side=side, native_target_pct=2.), path, 4, 100.)
    assert result["exit_reason"] == "TIME_EXIT_1515"
