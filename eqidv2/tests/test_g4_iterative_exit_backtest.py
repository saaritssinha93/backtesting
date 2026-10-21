"""Matched-cohort objectives and causal execution for iterative G4 research."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_exit_backtest as runner


def qualifying_metrics(**changes):
    result = dict(original_winner_profit_retention_pct=95., net_change_rs=0.,
        losses=2, baseline_losses=3, wins=5, baseline_wins=4,
        loss_saved_rs=100., changed_trades=3)
    result.update(changes)
    return result


@pytest.mark.parametrize("change", [
    {"original_winner_profit_retention_pct": 94.99},
    {"original_winner_profit_retention_pct": np.nan},
    {"net_change_rs": -1.}, {"losses": 3}, {"wins": 4},
    {"loss_saved_rs": 0.}, {"changed_trades": 2},
])
def test_qualification_requires_every_objective_and_strictly_more_wins(change):
    assert runner.qualifies(qualifying_metrics())
    assert not runner.qualifies(qualifying_metrics(**change))


def test_sid_alignment_and_original_winner_profit_include_harmed_winners():
    meta = pd.DataFrame({"sid": [30, 10, 20], "day": ["2026-10-02", "2026-10-01", "2026-10-01"]}).set_index("sid")
    before = pd.Series([100., -100., 200.], index=[10, 20, 30])
    after = pd.Series([-10., 195., 90.], index=[20, 30, 10])
    result = runner.metrics(before, after, meta, ["2026-10-01", "2026-10-02"])
    assert result["trades"] == 3
    assert result["original_winner_profit_rs"] == 300.
    assert result["original_winner_profit_after_rs"] == 285.
    assert result["original_winner_profit_retention_pct"] == 95.
    assert result["loss_saved_rs"] == 90.
    assert result["net_change_rs"] == 75.
    assert result["losing_days"] == 0
    after.loc[10], after.loc[30] = -10., 285.
    harmed = runner.metrics(before, after, meta, ["2026-10-01", "2026-10-02"])
    assert harmed["original_winner_profit_after_rs"] == 275.
    assert harmed["original_winners_turned_losses"] == 1


def returns_fixture():
    days = pd.bdate_range("2026-07-01", periods=40).strftime("%Y-%m-%d").tolist()
    meta, values = [], []
    for day_index, day in enumerate(days):
        for n in range(4):
            sid = 1000 + day_index * 4 + n
            meta.append(dict(sid=sid, day=day, setup_id="SYNTHETIC", side="LONG"))
            values.append(dict(sid=sid, baseline=[100., 100., -100., -100.][n],
                safer=[98., 98., 20., -40.][n], future_oracle=[95., 95., -20., -20.][n]))
    catalog = [dict(id=name, family=name) for name in ("baseline", "safer", "future_oracle")]
    return pd.DataFrame(meta).set_index("sid"), pd.DataFrame(values).set_index("sid"), days, catalog


def test_first35_selection_is_unchanged_by_arbitrary_later_results():
    meta, values, days, catalog = returns_fixture()
    normal = runner.comparison_table(values, meta, catalog, days)
    assert runner.choose(normal.loc[normal.period.eq("development35")]) == "safer"
    later = meta.day.isin(days[35:])
    values.loc[later, "future_oracle"] = 1e12
    values.loc[later, "safer"] = -1e12
    changed = runner.comparison_table(values, meta, catalog, days)
    assert runner.choose(changed.loc[changed.period.eq("development35")]) == "safer"
    pd.testing.assert_frame_equal(normal.loc[normal.period.eq("development35")].reset_index(drop=True),
                                  changed.loc[changed.period.eq("development35")].reset_index(drop=True))


def test_choose_uses_declared_ranking_and_baseline_fallback():
    table = pd.DataFrame([dict(policy="z", **qualifying_metrics(loss_saved_rs=200.)),
                          dict(policy="a", **qualifying_metrics(loss_saved_rs=200.)),
                          dict(policy="large_net", **qualifying_metrics(loss_saved_rs=100., net_change_rs=1e6))])
    assert runner.choose(table) == "a"
    table["original_winner_profit_retention_pct"] = 94.
    assert runner.choose(table) == "baseline"


def native_fixture():
    rows = [[100., 100.1, 99.9, 100.]] * 10
    rows[-1] = [100., 101.1, 99.9, 100.]
    values = np.asarray(rows)
    stamps = pd.date_range("2026-10-16 10:01", periods=10, freq="min", tz="Asia/Kolkata")
    path = dict(timestamp_ns=stamps.asi8,
        **{field: values[:, i] for i, field in enumerate(("open", "high", "low", "close"))})
    selected = pd.DataFrame([dict(sid=1, day="2026-10-16", signal_id="synthetic", setup_id="1000_LONG",
        tradingsymbol="SYNTHETIC", side="LONG", confirmation_ts=stamps[0] - pd.Timedelta(minutes=1),
        trigger=100., native_target_pct=1., native_stop_pct=1.2, initial_stop_pct=1.2)])
    native = runner.sealed.replay(selected, {1: path}, "current_g4")
    return native, path


def event(path, *, index=3):
    end = pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC").tz_convert("Asia/Kolkata")
    decision = pd.Timestamp(int(path["timestamp_ns"][index - 1]), tz="UTC").tz_convert("Asia/Kolkata")
    return dict(execution_path_index=index, modeled_execution_at_ist=end.isoformat(),
        next_bar_open_at_ist=(end - pd.Timedelta(minutes=1)).isoformat(),
        decision_at_ist=decision.isoformat(), next_bar_open_price=float(path["open"][index]),
        flags=json.dumps(["synthetic"]))


@pytest.mark.parametrize("corruption", ["entry_bar", "price", "decision", "reservation", "open", "native_exit", "out_of_bounds"])
def test_event_validation_rejects_unknown_or_noncausal_prices_and_times(corruption):
    native, path = native_fixture()
    row = native.iloc[0].to_dict()
    request = event(path)
    if corruption == "entry_bar":
        request = event(path, index=1)
    elif corruption == "price":
        request["next_bar_open_price"] += .1
    elif corruption == "decision":
        request["decision_at_ist"] = (pd.Timestamp(request["decision_at_ist"]) + pd.Timedelta(seconds=1)).isoformat()
    elif corruption == "reservation":
        request["modeled_execution_at_ist"] = request["decision_at_ist"]
    elif corruption == "open":
        request["next_bar_open_at_ist"] = request["modeled_execution_at_ist"]
    elif corruption == "native_exit":
        row["exit_ts"] = pd.Timestamp(request["decision_at_ist"])
    else:
        request["execution_path_index"] = len(path["timestamp_ns"])
    with pytest.raises(ValueError, match="Noncausal|inconsistent"):
        runner.validate_event(row, path, request)


def test_selected_ledger_preserves_native_control_and_matches_return_matrix():
    native, path = native_fixture()
    baseline = runner.selected_ledger(native, {1: path}, {}, "baseline")
    runner.sealed.parity(baseline, native)
    request = event(path)
    value = runner.event_net(native.iloc[0].to_dict(), path, request)
    changed = runner.selected_ledger(native, {1: path}, {1: request}, "synthetic")
    assert value == pytest.approx(-250.)
    assert changed.iloc[0].net_profit_rupees == pytest.approx(value)
    assert changed.iloc[0].portfolio_net_profit_rupees == pytest.approx(value)
    assert changed.iloc[0].exit_execution_ts == request["next_bar_open_at_ist"]
    assert changed.iloc[0].exit_ts == pd.Timestamp(request["modeled_execution_at_ist"])
    assert changed.iloc[0].active_stop_pct_at_exit == pytest.approx(1.2)


def test_combinations_use_only_development_shortlist_and_earliest_member_event():
    native, path = native_fixture()
    catalog = [dict(id="baseline", family="baseline"), dict(id="early", family="a"),
               dict(id="late", family="b"), dict(id="not_shortlisted", family="c")]
    returns = pd.DataFrame({"baseline": [4750.], "early": [-250.], "late": [-250.], "not_shortlisted": [1e12]}, index=[1])
    events = dict(baseline={}, early={1: event(path, index=3)}, late={1: event(path, index=4)}, not_shortlisted={})
    development = pd.DataFrame([
        dict(policy="early", family="a", **qualifying_metrics(loss_saved_rs=200.)),
        dict(policy="late", family="b", **qualifying_metrics(loss_saved_rs=100.)),
        dict(policy="not_shortlisted", family="c", **qualifying_metrics(original_winner_profit_retention_pct=94.)),
    ])
    combined, audits, extra, shortlisted = runner.combine_candidates(catalog, returns, events, native, {1: path}, development)
    assert shortlisted == ["early", "late"]
    assert len(extra) == 1 and extra[0]["params"]["members"] == ["early", "late"]
    assert audits[extra[0]["id"]][1] == events["early"][1]
    assert combined.loc[1, extra[0]["id"]] == pytest.approx(-250.)
