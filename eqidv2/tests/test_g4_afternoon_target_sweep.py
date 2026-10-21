"""Pinned afternoon cohorts, empty setups and actual-allocation qualification."""
import copy
import json

import pandas as pd
import pytest

from research import g4_extended_target_sweep as study


def target_config(tmp_path, **changes):
    value = study.configuration(study.MIDDAY_CONFIG)
    value.update(variant_id=study.AFTERNOON_VERSION,
        entry_policy="FREEZE_ALL_24_PREVIOUSLY_SELECTED_FILTERS_AND_PINNED_ORDERS",
        target_min_pct=.06, target_max_pct=10., target_step_pct=.01)
    value.update(changes)
    path = tmp_path / "afternoon.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return study.configuration(path)


def cohort():
    version = "V13-V10-G-4-BACKTEST-1235-1330-V6"
    clocks = tuple(pd.date_range("2000-01-01 12:35", "2000-01-01 13:30", freq="5min").strftime("%H:%M"))
    summary = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        sessions=49, training_sessions=38, holdout_sessions=11, new_setups=24,
        first_date="2026-07-29", last_date="2026-10-12", signal_ends=list(clocks),
        selected_orders=2, potential_fills=1)
    specs = []
    for clock in clocks:
        confirm = (pd.Timestamp("2000-01-01 " + clock) + pd.Timedelta(minutes=1)).strftime("%H:%M")
        for side in ("LONG", "SHORT"):
            specs.append(dict(setup_id=confirm.replace(":", "") + "_" + side,
                signal_end=clock, confirmation_end=confirm, side=side, target_pct=1.,
                initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
                max_entries=1, picker="max_liquidity"))
    config = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False, setups=specs)
    # Most setups intentionally have no orders: their configured membership is
    # still pinned and they must remain explicit nonpassing results.
    orders = pd.DataFrame([dict(sid=7_000_000 + n, signal_id=f"s{n}", day="2026-07-29",
        setup_id=s["setup_id"], signal_end=s["signal_end"], side=s["side"],
        native_target_pct=1., initial_stop_pct=1.25, filled=n == 0)
        for n, s in enumerate(specs[:2])])
    baseline = pd.DataFrame([dict(sid=1, signal_id="BASE", day="2026-07-29",
        setup_id="0951_LONG", filled=True)])
    saved = pd.concat([baseline, orders], ignore_index=True)
    dates = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2026-07-29", "2026-10-09")
             if d.strftime("%Y-%m-%d") != "2026-10-01"][:48] + ["2026-10-12"]
    return summary, config, orders, saved, baseline, dates


def test_afternoon_target_grid_preserves_cost_floor_1percent_reference_and_fixed_execution(tmp_path):
    config = target_config(tmp_path)
    assert config["targets_pct"] == [n / 100 for n in range(6, 1001)]
    assert len(config["targets_pct"]) == 995 and 1. in config["targets_pct"]
    assert config["scope"] == "BACKTEST_ONLY" and config["execution_authority"] is False
    assert (config["initial_stop_pct"], config["tightened_stop_pct"],
        config["tighten_after_minutes"], config["square_off"]) == (1.25, 1., 120, "15:15")
    assert study.PROFILES[study.AFTERNOON_VERSION]["require_additions"] is True


@pytest.mark.parametrize("change", [
    {"execution_authority": True}, {"scope": "LIVE"}, {"entry_policy": "REFIT_ENTRIES"},
    {"target_min_pct": .05}, {"target_max_pct": 10.01}, {"target_step_pct": .1},
    {"initial_stop_pct": 1.}, {"tighten_after_minutes": 60}, {"required_win_rate_pct": 69.},
    {"training_sessions": 39}, {"minimum_historical_fills": 1}, {"round_trip_cost_bps": 0.},
])
def test_afternoon_target_configuration_rejects_protocol_drift(tmp_path, change):
    with pytest.raises(ValueError):
        target_config(tmp_path, **change)


def test_pinned_cohort_retains_all_24_labels_even_when_22_have_no_orders():
    values = cohort()
    labels, fills = study.validate_afternoon_cohort(*values)
    assert len(labels) == 24 and fills == 1
    assert labels["1236_LONG"] == "12:35_LONG"
    assert labels["1301_SHORT"] == "13:00_SHORT"
    assert labels["1331_SHORT"] == "13:30_SHORT"


@pytest.mark.parametrize("mutation", [
    "authority", "version", "wrong_clock_set", "wrong_date_end", "short_history",
    "missing_setup", "duplicate_setup", "one_side_only", "target", "stop", "confirmation",
    "wrong_order_count", "wrong_fill_count", "duplicate_sid", "baseline_sid_overlap",
    "missing_saved_order", "wrong_order_clock", "wrong_order_target", "excluded_date",
])
def test_frozen_cohort_validation_rejects_membership_policy_and_count_drift(mutation):
    summary, config, orders, saved, baseline, days = cohort()
    if mutation == "authority": summary["execution_authority"] = True
    elif mutation == "version": config["variant_id"] = "V5"
    elif mutation == "wrong_clock_set": summary["signal_ends"][-1] = "13:35"
    elif mutation == "wrong_date_end": summary["last_date"] = "2026-10-13"
    elif mutation == "short_history": days.pop()
    elif mutation == "missing_setup": config["setups"].pop()
    elif mutation == "duplicate_setup": config["setups"][-1] = copy.deepcopy(config["setups"][0])
    elif mutation == "one_side_only": config["setups"][1]["side"] = "LONG"
    elif mutation == "target": config["setups"][0]["target_pct"] = .99
    elif mutation == "stop": config["setups"][0]["tightened_stop_pct"] = .5
    elif mutation == "confirmation": config["setups"][0]["confirmation_end"] = "12:40"
    elif mutation == "wrong_order_count": summary["selected_orders"] += 1
    elif mutation == "wrong_fill_count": summary["potential_fills"] += 1
    elif mutation == "duplicate_sid": orders.loc[1, "sid"] = orders.loc[0, "sid"]
    elif mutation == "baseline_sid_overlap": baseline.loc[0, "sid"] = orders.loc[0, "sid"]
    elif mutation == "missing_saved_order": saved = saved.iloc[:-1]
    elif mutation == "wrong_order_clock": orders.loc[0, "signal_end"] = "13:35"
    elif mutation == "wrong_order_target": orders.loc[0, "native_target_pct"] = 2.
    elif mutation == "excluded_date": days[1] = "2026-10-01"
    with pytest.raises(ValueError):
        study.validate_afternoon_cohort(summary, config, orders, saved, baseline, days)


def test_strong_combined_baseline_cannot_hide_unqualified_added_trades():
    common = dict(all_win_rate_pct=95., all_net_pnl_rs=100000.,
        added_all_win_rate_pct=69., added_all_net_pnl_rs=100.)
    assert study.common_eligible(common, "all", require_additions=False)
    assert not study.common_eligible(common, "all", require_additions=True)
    common["added_all_win_rate_pct"] = 70.
    common["added_all_net_pnl_rs"] = 0.
    assert not study.common_eligible(common, "all", require_additions=True)


def test_portfolio_ranking_cannot_hide_one_setup_below_70_after_capital_allocation():
    rows = []
    for name, pnl in [("higher_profit_weak_setup", 200.), ("all_setups_pass", 100.)]:
        for scope in ("combined", "additions"):
            rows.append(dict(scenario=name, scope=scope, period="all", executed_trades=20,
                wins=16, win_rate_pct=80., net_pnl_rs=pnl, baseline_trades_changed=0))
    setups = pd.DataFrame([
        dict(scenario="higher_profit_weak_setup", period="all", setup_id="A", observed_70_positive=True),
        dict(scenario="higher_profit_weak_setup", period="all", setup_id="B", observed_70_positive=False),
        dict(scenario="all_setups_pass", period="all", setup_id="A", observed_70_positive=True),
    ])
    ranking = study.scenario_ranking(pd.DataFrame(rows), setups).set_index("scenario")
    assert ranking.loc["higher_profit_weak_setup", "portfolio_70_positive"]
    assert not ranking.loc["higher_profit_weak_setup", "every_setup_70_positive"]
    assert ranking.loc["all_setups_pass", "every_setup_70_positive"]


def test_zero_fill_setups_are_explicit_nonpassing_decisions_instead_of_disappearing(tmp_path, monkeypatch):
    _, _, _, _, _, days = cohort()
    labels = {s["setup_id"]: f'{s["signal_end"]}_{s["side"]}' for s in cohort()[1]["setups"]}
    row = {column: None for column in study.RAW_COLUMNS}
    row.update(sid=7_000_000, setup_id="1236_LONG", signal_end="12:35", side="LONG",
        day=days[0], filled=False, net_return_pct=float("nan"))
    raw = pd.DataFrame([row])
    monkeypatch.setattr(study, "replay_target", lambda *args: raw.copy())
    monkeypatch.setattr(study, "assert_same_entries", lambda *args: None)
    monkeypatch.setattr(study, "allocate", lambda frame: frame)
    monkeypatch.setattr(study, "_stats", lambda *args: dict(executed_trades=0, wins=0,
        win_rate_pct=None, net_pnl_rs=0.))
    monkeypatch.setattr(study, "portfolio_changed", lambda *args: 0)
    monkeypatch.setattr(study, "_WORKER", {})
    study._worker_init(raw, {}, raw.iloc[:0], raw, days, labels)
    _, _, rows, _ = study._trial(1.)
    assert len(rows) == 24
    assert all(r["all_fills"] == 0 and r["all_win_rate_pct"] is None for r in rows)
    decisions, historical, training, chronological = study.decisions_for(rows, labels, target_config(tmp_path))
    assert len(decisions) == 24 and not historical and not training and not chronological
    assert not decisions.historical_pass.any()
    assert decisions.best_observed_win_rate_pct.isna().all()
    assert decisions.target_at_best_observed_win_rate_pct.isna().all()


def test_qualification_reallocates_after_removal_before_accepting_remaining_setups(monkeypatch):
    # Removing A releases capacity and admits new losing B fills; B must then
    # be reevaluated rather than retaining its obsolete passing statistics.
    raw = pd.DataFrame([dict(sid=i, setup_id=setup, native_target_pct=1.)
                        for i, setup in enumerate("ABC", 1)])
    calls = []

    def allocate(frame):
        member = frozenset(frame.setup_id)
        calls.append(member)
        result = frame.copy()
        result["failure"] = result.setup_id.map(
            {"A": member == frozenset("ABC"), "B": member == frozenset("BC"), "C": False})
        return result

    def stats(frame, _days):
        failed = bool(frame.failure.any())
        return dict(executed_trades=10, wins=6 if failed else 8,
            win_rate_pct=60. if failed else 80., net_pnl_rs=100.)

    monkeypatch.setattr(study, "allocate", allocate)
    monkeypatch.setattr(study, "_stats", stats)
    targets = dict.fromkeys("ABC", 1.)
    retained, removals = study.portfolio_qualified_subset({1.: raw}, targets,
        raw.iloc[:0], raw, ["2026-07-29"])
    assert retained == {"C": 1.} and targets == dict.fromkeys("ABC", 1.)
    assert calls == [frozenset("ABC"), frozenset("BC"), frozenset("C")]
    assert [(r["setup_id"], r["iteration"]) for r in removals] == [("A", 1), ("B", 2)]
