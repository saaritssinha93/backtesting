"""Frozen midday target choices and capital-adjusted qualification checks."""
import copy
import json

import numpy as np
import pandas as pd
import pytest

from research import g4_extended_target_sweep as sweep


def test_frozen_csv_preserves_exact_short_trigger_boundary_fill(tmp_path):
    # This price is exactly representable in the sealed float32 minute path.
    # The default fast CSV parser formerly rounded it down by one float64 ULP,
    # incorrectly turning the equal-price SHORT trigger into an unfilled order.
    trigger = 988.4000244140625
    path = tmp_path / "orders.csv"
    path.write_text("sid,side,trigger,native_target_pct\n1,SHORT,988.4000244140625,1.0\n", encoding="utf-8")
    orders = sweep.read_frozen_csv(path)
    assert float(orders.trigger.iloc[0]).hex() == trigger.hex()
    minute = {"timestamp_ns": np.array([pd.Timestamp("2026-09-24 12:02", tz="Asia/Kolkata").value]),
              "open": np.array([trigger + .5]), "high": np.array([trigger + 1.]),
              "low": np.array([trigger]), "close": np.array([trigger + .2])}
    actual = sweep.simulator.simulate_staged(orders, {1: minute}, cost_bps=5., max_entry_delay_minutes=10)
    assert bool(actual.iloc[0].filled)
    assert actual.iloc[0].entry_path_index == 0
    assert float(actual.iloc[0].entry_price).hex() == trigger.hex()


def test_midday_target_profile_keeps_legacy_default_and_frozen_entry_policy():
    old = sweep.configuration()
    new = sweep.configuration(sweep.MIDDAY_CONFIG)
    assert old["variant_id"] == sweep.VERSION
    assert new["variant_id"] == sweep.MIDDAY_VERSION
    assert new["reference_run_relative"] == "runs/2026-10-13/20261013_midday_1135_1230_v4"
    assert new["reference_manifest_sha256"] == "ebc265ff4ff7489ccdacd6809f41e6c8d913b8d4df1006df8ad6ab608a837cd6"
    assert new["entry_policy"] == "FREEZE_ALL_24_PREVIOUSLY_SELECTED_FILTERS_AND_345_ORDERS"
    assert new["scope"] == "BACKTEST_ONLY" and new["execution_authority"] is False
    assert (new["initial_stop_pct"], new["tightened_stop_pct"], new["tighten_after_minutes"]) == (1.25, 1., 120)
    assert new["required_win_rate_pct"] == 70.
    assert new["round_trip_cost_bps"] == 5.
    assert 1. in new["targets_pct"]
    assert len(new["targets_pct"]) == len(set(new["targets_pct"]))
    profile = sweep.PROFILES[sweep.MIDDAY_VERSION]
    assert (profile["setups"], profile["orders"], profile["fills"]) == (24, 345, 287)
    assert profile["reference_version"] == "V13-V10-G-4-BACKTEST-1135-1230-V4"
    assert profile["ledger"] == "all_24_diagnostic_trades.csv"


@pytest.mark.parametrize("change", [
    {"entry_policy": "FREEZE_ALL_22_PREVIOUSLY_SELECTED_FILTERS_AND_278_ORDERS"},
    {"required_win_rate_pct": 69.},
    {"round_trip_cost_bps": 0.},
    {"tightened_stop_pct": .75},
    {"execution_authority": True},
    {"target_min_pct": 1.01},
    {"target_max_pct": .99},
])
def test_midday_target_config_rejects_protocol_drift_or_grid_without_reference(tmp_path, change):
    config = json.loads(sweep.MIDDAY_CONFIG.read_text(encoding="utf-8"))
    config.update(change)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        sweep.configuration(path)


def common_trial(*, combined_win_rate=80., added_win_rate=75., combined_profit=1000., added_profit=100.):
    result = {"target_pct": .8}
    for period in ("all", "train", "holdout"):
        result.update({f"{period}_win_rate_pct": combined_win_rate,
                       f"{period}_net_pnl_rs": combined_profit,
                       f"{period}_executed_trades": 20,
                       f"added_{period}_win_rate_pct": added_win_rate,
                       f"added_{period}_net_pnl_rs": added_profit,
                       f"added_{period}_executed_trades": 8})
    return result


def test_common_target_cannot_hide_midday_failure_in_profitable_baseline():
    candidate = common_trial(added_win_rate=60.)
    assert sweep.common_eligible(candidate, "all", require_additions=False)
    assert not sweep.common_eligible(candidate, "all", require_additions=True)
    assert sweep.common_eligible(common_trial(added_win_rate=70.), "all", require_additions=True)


@pytest.mark.parametrize("changes", [
    {"combined_win_rate": 69.99}, {"combined_profit": 0.},
    {"added_win_rate": 69.99}, {"added_win_rate": None},
    {"added_profit": 0.}, {"added_profit": -100.},
])
def test_common_target_needs_positive_realized_profit_and_both_win_rates(changes):
    assert not sweep.common_eligible(common_trial(**changes), "all", require_additions=True)


def test_common_target_training_eligibility_ignores_later_and_full_results():
    row = common_trial()
    changed = copy.deepcopy(row)
    changed.update(all_net_pnl_rs=-1e9, added_all_net_pnl_rs=-1e9,
                   holdout_win_rate_pct=0., added_holdout_win_rate_pct=0.)
    assert sweep.common_eligible(changed, "train", require_additions=True)
    assert not sweep.common_eligible(changed, "all", require_additions=True)


def scenario_frames():
    portfolios, setups = [], []
    for name, profit, rate, count, each_pass in (
        ("baseline_masks_weak_additions", 1000., 60., 10, False),
        ("portfolio_only_pass", 900., 70., 10, False),
        ("all_setups_pass", 800., 80., 10, True),
        ("too_few_executions", 700., 100., 4, True),
    ):
        for scope in ("combined", "additions"):
            portfolios.append(dict(scenario=name, period="all", scope=scope,
                executed_trades=count if scope == "additions" else 100 + count,
                win_rate_pct=rate if scope == "additions" else 80.,
                net_pnl_rs=profit - 500. if scope == "additions" else profit,
                baseline_trades_changed=0))
        setups.extend([dict(scenario=name, period="all", setup_id="A", observed_70_positive=True),
                       dict(scenario=name, period="all", setup_id="B", observed_70_positive=each_pass)])
    return pd.DataFrame(portfolios), pd.DataFrame(setups)


def test_ranking_separates_aggregate_win_rate_from_each_executed_setup():
    portfolios, setups = scenario_frames()
    ranking = sweep.scenario_ranking(portfolios, setups)
    by_name = ranking.set_index("scenario")
    assert not by_name.loc["baseline_masks_weak_additions", "portfolio_70_positive"]
    assert by_name.loc["portfolio_only_pass", "portfolio_70_positive"]
    assert not by_name.loc["portfolio_only_pass", "every_setup_70_positive"]
    assert by_name.loc["all_setups_pass", "every_setup_70_positive"]
    assert not by_name.loc["too_few_executions", "portfolio_70_positive"]
    assert ranking.loc[ranking.portfolio_70_positive].iloc[0].scenario == "portfolio_only_pass"
    assert ranking.loc[ranking.every_setup_70_positive].iloc[0].scenario == "all_setups_pass"


def test_ranking_uses_actual_execution_qualification_and_full_period_only():
    portfolios, setups = scenario_frames()
    setup_changed = setups.copy()
    setup_changed.loc[setup_changed.scenario.eq("all_setups_pass"), "observed_70_positive"] = False
    extra_period = portfolios.assign(period="holdout", net_pnl_rs=1e9, win_rate_pct=100.)
    ranking = sweep.scenario_ranking(pd.concat([portfolios, extra_period], ignore_index=True), setup_changed)
    chosen = ranking.set_index("scenario").loc["all_setups_pass"]
    assert chosen.combined_net_pnl_rs == 800.
    assert chosen.portfolio_70_positive
    assert not chosen.every_setup_70_positive
