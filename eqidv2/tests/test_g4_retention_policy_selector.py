"""Configurable retention without changing historical selection causality."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_pattern_model as original
from research import g4_retention_policy_selector as selector


def fixture(days_count=40, winner_return=92.):
    days = pd.bdate_range("2026-07-01", periods=days_count).strftime("%Y-%m-%d").tolist()
    records, rows = [], []
    for day_index, day in enumerate(days):
        for j in range(5):
            sid = day_index * 5 + j
            records.append(dict(sid=sid, day=day, setup_id="setup_a", side="LONG",
                                confirmation_ts=f"{day} 10:00:00+05:30"))
            rows.append(dict(sid=sid, baseline=100. if j < 3 else -100.,
                             candidate=winner_return if j < 3 else 20. if j == 3 else -40.))
    return pd.DataFrame(records), pd.DataFrame(rows).set_index("sid"), days


@pytest.mark.parametrize("method,warmup", [("select_adaptive", 20), ("select_setup_holdout", 35)])
def test_90_percent_admits_candidate_rejected_at_95_percent(method, warmup):
    meta, values, days = fixture()
    call = getattr(selector, method)
    strict = call(meta, values, days)
    flexible = call(meta, values, days, minimum_winner_profit_retention=.90)
    assert strict["policy_for_sid"].eq("baseline").all()
    assert flexible["policy_for_sid"].iloc[:warmup * 5].eq("baseline").all()
    assert flexible["policy_for_sid"].iloc[warmup * 5:].eq("candidate").all()
    assert flexible["parameters"]["minimum_winner_profit_retention"] == .90
    assert strict["parameters"]["minimum_winner_profit_retention"] == .95
    assert selector.PARAMETERS["minimum_winner_profit_retention"] == .95
    assert original.PARAMETERS["minimum_winner_profit_retention"] == .95


@pytest.mark.parametrize("method", ["select_adaptive", "select_setup_holdout"])
@pytest.mark.parametrize("winner_return", [92., 98.])
def test_default_95_has_exact_original_selection_audit_and_pnl_parity(method, winner_return):
    meta, values, days = fixture(winner_return=winner_return)
    old = getattr(original, method)(meta, values, days)
    new = getattr(selector, method)(meta, values, days)
    for key in ("pnl", "policy_for_sid"):
        pd.testing.assert_series_equal(old[key], new[key])
    for key in ("assignments", "folds", "metadata"):
        pd.testing.assert_frame_equal(old[key], new[key])
    assert old["parameters"] == new["parameters"]


@pytest.mark.parametrize("long_winner,expected", [(92., "candidate"), (89., "baseline")])
def test_sparse_group_fallback_uses_explicit_90_percent_guard(long_winner, expected):
    days = pd.bdate_range("2026-07-01", periods=21).strftime("%Y-%m-%d").tolist()
    rows, records = [], []
    for i, day in enumerate(days):
        for j, side in enumerate(["LONG", "SHORT"]):
            sid = 2 * i + j
            base = -100. if i < 8 else 100.
            candidate = 90. if i < 8 else long_winner if side == "LONG" else 200. - long_winner
            records.append(dict(sid=sid, day=day, setup_id=side, side=side,
                                confirmation_ts=f"{day} 12:00:00+05:30"))
            rows.append(dict(sid=sid, baseline=base, candidate=candidate))
    meta, values = pd.DataFrame(records), pd.DataFrame(rows).set_index("sid")
    strict = selector.select_adaptive(meta, values, days)
    loose = selector.select_adaptive(meta, values, days, minimum_winner_profit_retention=.90)
    assert strict["policy_for_sid"].loc[40] == "baseline"
    assert loose["policy_for_sid"].loc[40] == expected
    assert loose["policy_for_sid"].loc[41] == "candidate"
    assert loose["folds"].set_index("group").loc["all", "policy"] == "candidate"


@pytest.mark.parametrize("method,train_count,test_count", [
    ("select_adaptive", 20, 5), ("select_setup_holdout", 35, 5),
])
def test_future_outcomes_cannot_change_fitted_policy_at_90(method, train_count, test_count):
    meta, values, days = fixture()
    call = getattr(selector, method)
    before = call(meta, values, days, minimum_winner_profit_retention=.90)
    changed = values.copy()
    changed.loc[meta.loc[meta.day.isin(days[train_count:]), "sid"], "candidate"] = -1e12
    after = call(meta, changed, days, minimum_winner_profit_retention=.90)
    ids = meta.loc[meta.day.isin(days[train_count:train_count + test_count]), "sid"]
    assert before["policy_for_sid"].loc[ids].eq("candidate").all()
    pd.testing.assert_series_equal(before["policy_for_sid"].loc[ids], after["policy_for_sid"].loc[ids])


@pytest.mark.parametrize("retention", [0., -.1, 1.01, np.inf, -np.inf, np.nan, True, False, None])
@pytest.mark.parametrize("method", ["select_adaptive", "select_setup_holdout"])
def test_invalid_retention_is_rejected_before_selection(method, retention):
    meta, values, days = fixture(1)
    with pytest.raises(ValueError, match="minimum_winner_profit_retention"):
        getattr(selector, method)(meta, values, days, minimum_winner_profit_retention=retention)


def test_lower_retention_does_not_relax_strict_win_requirement_or_roundoff_tolerance():
    _, values, _ = fixture()
    values["candidate"] = np.where(values.baseline > 0, 100., 5e-11)
    policy, audit = selector._fit(values, minimum_winner_profit_retention=.90)
    assert policy == "baseline" and audit["reason"] == "no_qualified_prior_policy"


def test_wide_metadata_is_reduced_without_mutation_or_outcome_leakage():
    meta, values, days = fixture()
    wide = pd.concat([meta, pd.DataFrame({f"future_field_{i}": np.arange(len(meta)) * (i + 1)
                                        for i in range(120)})], axis=1)
    before = wide.copy()
    narrow = selector.select_adaptive(meta, values, days, minimum_winner_profit_retention=.90)
    result = selector.select_adaptive(wide, values, days, minimum_winner_profit_retention=.90)
    pd.testing.assert_series_equal(result["policy_for_sid"], narrow["policy_for_sid"])
    pd.testing.assert_frame_equal(result["metadata"], narrow["metadata"])
    pd.testing.assert_frame_equal(wide, before)
    assert set(result["metadata"].columns) == {"day", "setup_id", "side", "confirmation_ts", "group"}
