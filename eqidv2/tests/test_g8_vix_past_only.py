"""Independent causal-boundary checks for G8's rolling model experiment."""
from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from research import g8_vix_past_only as target


def synthetic_source(session_count=42, trades_per_session=8):
    rng = np.random.default_rng(160723)
    days = pd.bdate_range("2026-06-01", periods=session_count).strftime("%Y-%m-%d").tolist()
    n = len(days) * trades_per_session
    sid = np.arange(2000, 2000 + n)
    f = pd.DataFrame(rng.normal(size=(n, len(target.CONTEXT + target.VIX))),
                     columns=target.CONTEXT + target.VIX)
    f["sid"] = sid
    f["day"] = np.repeat(days, trades_per_session)
    f["setup_id"] = "1011_LONG"
    f["side"] = "LONG"
    f["nifty_base_allowed"] = sid % 7 != 0
    f.loc[f.index % 13 == 0, target.CONTEXT[0]] = np.nan
    f = f.set_index("sid", drop=False)
    returns = np.where(sid % 3 == 0, -.9, .6) + rng.normal(scale=.03, size=n)
    original = pd.DataFrame(dict(sid=sid, filled=sid % 11 != 0,
                                 net_return_pct=returns))
    return f, original, days


def test_fit_forecasts_ignores_all_future_outcomes_and_fill_labels():
    f, original, days = synthetic_source()
    training = days[:21]
    expected_scores, expected_evidence = target.fit_forecasts(f, original, training)
    changed = original.copy()
    future = ~changed.sid.isin(f.loc[f.day.isin(training), "sid"])
    changed.loc[future, "net_return_pct"] = np.arange(future.sum()) * 1000. - 100000.
    changed.loc[future, "filled"] = ~changed.loc[future, "filled"]
    actual_scores, actual_evidence = target.fit_forecasts(f, changed, training)
    assert actual_evidence == expected_evidence
    for name in expected_scores:
        pd.testing.assert_series_equal(actual_scores[name], expected_scores[name])
    eligible = original.set_index("sid").loc[f.index].filled & f.nifty_base_allowed & f.day.isin(training)
    assert expected_evidence["native_training_trades"] == int(eligible.sum())
    assert expected_evidence["train_through"] == days[20]


def test_future_features_do_not_fit_imputer_scaler_or_coefficients():
    f, original, days = synthetic_source()
    training = days[:21]
    scores, evidence = target.fit_forecasts(f, original, training)
    poisoned = f.copy()
    future = ~poisoned.day.isin(training)
    poisoned.loc[future, target.CONTEXT + target.VIX] = 1e7
    future_scores, future_evidence = target.fit_forecasts(poisoned, original, training)
    assert future_evidence == evidence
    for name in scores:
        pd.testing.assert_series_equal(future_scores[name].loc[~future], scores[name].loc[~future])


def mask_source():
    f = pd.DataFrame(dict(
        nifty_base_allowed=[False, True, True, True, True],
        side_sign=[1, 1, 1, -1, -1],
        vix_session_pct=[0., 3., 0., -3., 0.],
        vix_30_pct=[0., 3., 0., -3., 0.],
        vix_15_pct=[0., 3., 0., -3., 0.],
        stock_relative5_pct=[0., -.2, 0., -.2, 0.],
        stock_relative_session_pct=[0., -.2, 0., -.2, 0.]), index=[30, 31, 32, 33, 34])
    scores = {
        "binary": pd.Series([.9, .9, .1, .1, .9], index=f.index),
        "payoff_weighted": pd.Series([.1, .1, .1, .5, .9], index=f.index),
        "context_only": pd.Series([.9, .9, .1, .1, .9], index=f.index),
        "ridge_10.0": pd.Series([-.1, -.1, .1, -.1, .1], index=f.index),
        "ridge_100.0": pd.Series([-.1, -.1, .1, -.1, .1], index=f.index),
    }
    return f, scores


def test_consensus_veto_requires_both_bad_forecasts_and_keeps_nifty_gate():
    f, scores = mask_source()
    decisions = target.model_mask(f, dict(mode="consensus", p=.5, r=0.), scores)
    # Row31 has both poor; row32 has good expected return; row33 reaches p cutoff.
    assert decisions.tolist() == [False, False, True, True, True]


def test_vix_model_consensus_and_combined_are_union_and_intersection_within_base_gate():
    f, scores = mask_source()
    union = target.model_mask(f, dict(mode="vix_model_consensus"), scores)
    intersection = target.model_mask(f, dict(mode="vix_model_combined"), scores)
    assert union.tolist() == [False, True, True, False, True]
    assert intersection.tolist() == [False, False, False, False, True]


@pytest.mark.parametrize("spec", target.specifications(), ids=lambda value: value["id"])
def test_every_candidate_preserves_base_nifty_rejection(spec):
    f, scores = mask_source()
    if "window" in spec:
        decisions = target.model_mask(f, spec, scores)
    else:
        decisions = target.rule_mask(f, spec)
    assert decisions.index.equals(f.index)
    assert not decisions.loc[~f.nifty_base_allowed].any()


def policy_trial():
    baseline = dict(policy=target.core.BASELINE, net_pnl_rs=100., win_rate_pct=70., trades=100,
                    losing_days=3, daily_close_drawdown_rs=20., changed_executions=0, changed_days=0)
    candidate = dict(policy="eligible_candidate", net_pnl_rs=120., win_rate_pct=72., trades=90,
                     losing_days=2, daily_close_drawdown_rs=18., changed_executions=10, changed_days=4)
    ineligible = dict(candidate, policy="ineligible_candidate", net_pnl_rs=1000., changed_days=1)
    rows = []
    for period in ("past_forecast_development", "locked_assessment", "all_past_forecasts"):
        rows.extend(dict(row, period=period) for row in (baseline, candidate, ineligible))
    return pd.DataFrame(rows)


def test_policy_selection_ignores_locked_assessment_and_all_history_results():
    trial = policy_trial()
    assert target.select_policy(trial) == "eligible_candidate"
    assessment = trial.period.ne("past_forecast_development")
    changed = trial.copy()
    changed.loc[assessment, "net_pnl_rs"] = np.arange(assessment.sum()) * 1e9
    changed.loc[assessment, "win_rate_pct"] = 0.
    changed.loc[assessment, "changed_days"] = 999
    assert target.select_policy(changed) == "eligible_candidate"


def test_policy_selection_falls_back_when_only_assessment_has_improvement():
    trial = policy_trial()
    development = trial.period.eq("past_forecast_development") & trial.policy.ne(target.core.BASELINE)
    trial.loc[development, "net_pnl_rs"] = -1000.
    assert target.select_policy(trial) == target.core.BASELINE


def test_weekly_forecasts_use_only_prior_days_and_only_apply_to_their_fold(monkeypatch):
    f, original, days = synthetic_source(session_count=56)
    # Only model candidates matter to this split check; baseline is the model warmup.
    specs = [dict(id=target.core.BASELINE, mode="baseline"),
             dict(id="expanding", mode="binary", window="expanding", cutoff=.5),
             dict(id="rolling", mode="binary", window="rolling28", cutoff=.5)]
    monkeypatch.setattr(target, "specifications", lambda: deepcopy(specs))
    calls = []

    def stub_fit(features, source, training):
        call = len(calls)
        calls.append(list(training))
        # Alternating allow/reject makes each fold's application observable.
        value = .9 if call % 4 < 2 else .1
        score = pd.Series(value, index=features.index)
        return {"binary": score}, dict(train_from=min(training), train_through=max(training))

    monkeypatch.setattr(target, "fit_forecasts", stub_fit)
    masks, proofs, predictions = target.past_only_masks(f, original, days)
    assert len(calls) == 10
    assert (predictions.train_through < predictions.day).all()
    assert not predictions.duplicated(["sid", "window"]).any()
    warmup = f.day.isin(days[:21])
    for name in ("expanding", "rolling"):
        pd.testing.assert_series_equal(masks[name].loc[warmup], f.nifty_base_allowed.loc[warmup])
    for block, start in enumerate(range(21, 56, 7)):
        selected = f.day.isin(days[start:start + 7])
        expected = f.nifty_base_allowed.loc[selected] & (block % 2 == 0)
        for name in ("expanding", "rolling"):
            pd.testing.assert_series_equal(masks[name].loc[selected], expected)
        assert calls[2 * block] == days[:start]
        assert calls[2 * block + 1] == days[max(0, start - 28):start]
        for proof in proofs[2 * block:2 * block + 2]:
            assert proof["train_through"] < proof["test_from"]
