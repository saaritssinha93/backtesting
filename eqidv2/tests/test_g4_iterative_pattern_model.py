"""Chronological and winner-retention safeguards for adaptive G4 research."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_pattern_model as model


def fixture(days_count=30):
    days = pd.bdate_range("2026-07-01", periods=days_count).strftime("%Y-%m-%d").tolist()
    records, rows = [], []
    for n, day in enumerate(days):
        for j in range(5):
            sid = n * 5 + j
            records.append(dict(sid=sid, day=day, setup_id="setup_a", side="LONG",
                                confirmation_ts=f"{day} 10:00:00+05:30"))
            rows.append(dict(sid=sid, baseline=100. if j < 3 else -100.,
                             safer=98. if j < 3 else 20. if j == 3 else -40.))
    return pd.DataFrame(records), pd.DataFrame(rows).set_index("sid"), days


def test_only_prior_sessions_select_each_block_even_if_test_outcomes_change():
    meta, values, days = fixture()
    normal = model.select_adaptive(meta, values, days)
    altered_values = values.copy()
    altered_values.loc[meta.loc[meta.day.isin(days[20:]), "sid"], "safer"] = -1e9
    altered = model.select_adaptive(meta, altered_values, days)
    first_test_ids = meta.loc[meta.day.isin(days[20:25]), "sid"]
    assert normal["policy_for_sid"].loc[first_test_ids].eq("safer").all()
    pd.testing.assert_series_equal(normal["policy_for_sid"].loc[first_test_ids],
                                   altered["policy_for_sid"].loc[first_test_ids])
    assert altered["policy_for_sid"].loc[meta.loc[meta.day.isin(days[25:]), "sid"]].eq("baseline").all()
    assert normal["policy_for_sid"].iloc[:100].eq("baseline").all()
    assert normal["folds"].train_end.lt(normal["folds"].test_start).all()


def test_policy_choice_cannot_see_future_candidate_columns_ranking():
    meta, values, days = fixture()
    values["future_oracle"] = values.baseline
    values.loc[100:, "future_oracle"] = 1e9
    result = model.select_adaptive(meta, values, days)
    assert result["policy_for_sid"].loc[100:124].eq("safer").all()


def test_rejects_loss_saving_that_exceeds_original_winner_profit_budget():
    _, values, _ = fixture()
    values["too_costly"] = np.where(values.baseline > 0, 94., 100.)
    policy, audit = model._fit(values[["baseline", "too_costly"]])
    assert policy == "baseline"
    assert audit["reason"] == "no_qualified_prior_policy"
    values["budget_edge"] = np.where(values.baseline > 0, 95., 100.)
    assert model._fit(values[["baseline", "budget_edge"]])[0] == "budget_edge"


def test_improving_loss_amount_alone_cannot_qualify():
    _, values, _ = fixture()
    values["same_loss_count"] = np.where(values.baseline > 0, 100., -10.)
    assert model._fit(values[["baseline", "same_loss_count"]])[0] == "baseline"
    values["only_two_changes"] = values.baseline
    values.loc[[3, 4], "only_two_changes"] = 10.
    assert model._fit(values[["baseline", "only_two_changes"]])[0] == "baseline"
    values["losses_to_flat"] = np.where(values.baseline > 0, 100., 0.)
    assert model._fit(values[["baseline", "losses_to_flat"]])[0] == "baseline"


def test_binary_roundoff_at_cost_covering_exit_does_not_create_wins_or_losses():
    baseline = pd.Series([100., -100., 5e-11, -5e-11])
    candidate = pd.Series([-5e-11, 5e-11, -100., 100.])
    stats = model._metrics(baseline, candidate)
    assert stats["baseline_wins"] == stats["baseline_losses"] == 1
    assert stats["candidate_wins"] == stats["candidate_losses"] == 1
    assert stats["original_winner_profit"] == 100.
    assert stats["retained_original_winner_profit"] == -5e-11
    assert stats["winners_turned_losses"] == stats["losers_turned_winners"] == 0
    _, values, _ = fixture()
    values["roundoff_protection"] = np.where(values.baseline > 0, 100., 5e-11)
    policy, audit = model._fit(values[["baseline", "roundoff_protection"]])
    assert policy == "baseline"
    assert audit["reason"] == "no_qualified_prior_policy"


@pytest.mark.parametrize("baseline", [
    [100.] * 9 + [5e-11] * 10 + [-100.] * 21,
    [100.] * 31 + [-5e-11] * 2 + [-100.] * 7,
])
def test_roundoff_cannot_satisfy_minimum_training_winners_or_losers(baseline):
    values = pd.DataFrame({"baseline": baseline, "candidate": 100.})
    policy, audit = model._fit(values)
    assert policy == "baseline"
    assert audit["reason"] == "insufficient_prior_trades"


def test_deterministic_ties_and_row_order_alignment():
    meta, values, days = fixture()
    values["aardvark"] = values.safer
    result = model.select_adaptive(meta, values.sample(frac=1, random_state=5), days)
    assert result["policy_for_sid"].iloc[100:].eq("aardvark").all()
    for sid, policy in result["policy_for_sid"].items():
        assert result["pnl"].loc[sid] == values.loc[sid, policy]


def test_global_fallback_enforces_winner_retention_for_each_sparse_group():
    days = pd.bdate_range("2026-07-01", periods=21).strftime("%Y-%m-%d").tolist()
    rows, records = [], []
    for i, day in enumerate(days):
        for j, side in enumerate(["LONG", "SHORT"]):
            sid = 2 * i + j
            baseline = -100. if i < 8 else 100.
            candidate = 90. if i < 8 else 80. if side == "LONG" else 120.
            records.append(dict(sid=sid, day=day, setup_id=side, side=side,
                                confirmation_ts=f"{day} 12:00:00+05:30"))
            rows.append(dict(sid=sid, baseline=baseline, candidate=candidate))
    result = model.select_adaptive(pd.DataFrame(records), pd.DataFrame(rows).set_index("sid"), days)
    assert result["policy_for_sid"].loc[40] == "baseline"
    assert result["policy_for_sid"].loc[41] == "candidate"
    audits = result["folds"].set_index("group")
    assert audits.loc["all", "policy"] == "candidate"
    assert audits.loc["LONG:11_to_13", "reason"] == "global_fallback_group_retention_rejected"
    assert audits.loc["SHORT:11_to_13", "reason"] == "global_fallback_group_retention_pass"


def test_time_groups_are_point_in_time_and_market_timezone_aware():
    meta, values, days = fixture(1)
    meta["confirmation_ts"] = [f"{days[0]} {time}" for time in [
        "10:59:00+05:30", "11:00:00+05:30", "12:59:00+05:30", "13:00:00+05:30", "07:30:00+00:00"]]
    result = model.select_adaptive(meta, values, days)
    assert result["metadata"].group.tolist() == ["LONG:before_11", "LONG:11_to_13",
                                               "LONG:11_to_13", "LONG:from_13", "LONG:from_13"]


def test_last_partial_block_and_empty_session_are_preserved():
    meta, values, days = fixture(32)
    empty_sid = meta.loc[meta.day.eq(days[21]), "sid"]
    meta = meta.loc[~meta.sid.isin(empty_sid)]
    values = values.drop(empty_sid)
    result = model.select_adaptive(meta, values, days)
    folds = result["folds"].query("group == 'all'")
    assert folds.test_sessions.tolist() == [5, 5, 2]
    assert folds.iloc[-1].test_end == days[-1]
    assert len(result["pnl"]) == len(meta)


def test_sparse_setup_remains_baseline_and_holdout_never_used_for_fit():
    meta, values, days = fixture(40)
    meta["setup_id"] = [f"setup_{j % 25}" for j in range(len(meta))]
    result = model.select_setup_holdout(meta, values, days)
    assert result["policy_for_sid"].eq("baseline").all()
    assert result["folds"].reason.eq("insufficient_prior_trades").all()
    meta["setup_id"] = "one_pooled_setup"
    result = model.select_setup_holdout(meta, values, days)
    assert result["policy_for_sid"].iloc[175:].eq("safer").all()
    changed = values.copy()
    changed.loc[175:, "safer"] = -1e9
    altered = model.select_setup_holdout(meta, changed, days)
    pd.testing.assert_series_equal(result["policy_for_sid"], altered["policy_for_sid"])


@pytest.mark.parametrize("mutation", ["duplicate", "missing_sid", "nan", "bad_day", "unordered_days", "bad_side"])
def test_rejects_ambiguous_or_invalid_input(mutation):
    meta, values, days = fixture()
    if mutation == "duplicate":
        meta = pd.concat([meta, meta.iloc[[0]]])
    elif mutation == "missing_sid":
        values = values.iloc[1:]
    elif mutation == "nan":
        values.loc[0, "safer"] = np.nan
    elif mutation == "bad_day":
        meta.loc[0, "confirmation_ts"] = "2000-01-01 10:00"
    elif mutation == "unordered_days":
        days = list(reversed(days))
    elif mutation == "bad_side":
        meta.loc[0, "side"] = "BUY"
    with pytest.raises(ValueError):
        model.select_adaptive(meta, values, days)
