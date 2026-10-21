"""Integration of proportional partial cashflows and actual resting protection."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_protection_refinement as refinement


def minute_path(*, start="2026-10-16 10:01", protect=False):
    first = pd.Timestamp(start, tz="Asia/Kolkata")
    count = int((pd.Timestamp("2026-10-16 10:10", tz="Asia/Kolkata") - first).total_seconds() / 60) + 1
    values = np.tile([100., 100.1, 99.9, 100.], (count, 1))
    values[-1] = [100., 101.1, 99.9, 100.]
    if first.strftime("%H:%M") == "10:01":
        if protect:
            values[1:4] = [[100., 100.6, 99.9, 100.5],
                           [100.5, 100.6, 100.3, 100.4],
                           [100.2, 100.3, 100.05, 100.2]]
        else:
            values[3] = [100.4, 100.5, 100.3, 100.4]
    stamps = pd.date_range(first, periods=count, freq="min")
    return dict(timestamp_ns=stamps.asi8,
        **{field: values[:, i] for i, field in enumerate(("open", "high", "low", "close"))})


def native_book(paths):
    records = []
    for sid, path in paths.items():
        first = pd.Timestamp(int(path["timestamp_ns"][0]), tz="UTC").tz_convert("Asia/Kolkata")
        records.append(dict(sid=sid, day="2026-10-16", signal_id=f"synthetic-{sid}",
            setup_id=f"{sid:04d}_LONG", tradingsymbol=f"SYNTHETIC{sid}", side="LONG",
            confirmation_ts=first - pd.Timedelta(minutes=1), trigger=100.,
            native_target_pct=1., native_stop_pct=1.2, initial_stop_pct=1.2))
    return refinement.core.sealed.replay(pd.DataFrame(records), paths, "current_g4")


def close_event(path):
    end = pd.Timestamp(int(path["timestamp_ns"][3]), tz="UTC").tz_convert("Asia/Kolkata")
    decision = end - pd.Timedelta(minutes=1)
    return dict(execution_path_index=3, modeled_execution_at_ist=end.isoformat(),
        next_bar_open_at_ist=decision.isoformat(), decision_at_ist=decision.isoformat(),
        next_bar_open_price=float(path["open"][3]), flags=json.dumps(["parent"]), family="target_giveback")


def partial_policy(fraction=.5):
    return dict(id="partial", family="partial_target_giveback", kind="partial_exit",
                params=dict(parent="parent", fraction=fraction))


@pytest.mark.parametrize("fraction,expected_net", [(.25, 4000.), (.5, 3250.), (.75, 2500.)])
def test_partial_ledger_reconciles_real_two_leg_cost_and_preserves_native_reservation(fraction, expected_net):
    paths = {1: minute_path()}
    native = native_book(paths)
    policy = partial_policy(fraction)
    events = {"parent": {1: close_event(paths[1])}}
    book = refinement.ledger(native, paths, "partial", {"partial": policy}, events, {})
    row, original = book.iloc[0], native.iloc[0]
    legs = json.loads(row.partial_leg_audit)
    assert len(legs) == 2
    assert sum(leg["cost_rs"] for leg in legs) == pytest.approx(250.)
    assert sum(leg["net_profit_rs"] for leg in legs) == pytest.approx(expected_net)
    assert row.net_profit_rupees == pytest.approx(expected_net)
    assert row.portfolio_net_profit_rupees == pytest.approx(expected_net)
    assert row.portfolio_cost_rupees == pytest.approx(250.)
    assert row.exit_ts == original.exit_ts
    assert row.exit_execution_ts == original.exit_execution_ts
    assert row.holding_minutes == original.holding_minutes
    assert row.portfolio_trade_capital_rupees == pytest.approx(100000.)
    assert row.portfolio_trade_exposure_rupees == pytest.approx(500000.)
    details = refinement.descriptor(original, 1, "partial", {"partial": policy}, events, {}, paths)
    assert details["extra_cost_5bps"] == pytest.approx(250. * fraction)
    assert pd.Timestamp(details["final_exit"]) == original.exit_ts


def test_partial_sale_cannot_admit_an_order_rejected_by_full_original_margin():
    paths = {sid: minute_path() for sid in range(10)}
    paths[10] = minute_path(start="2026-10-16 10:05")
    native = native_book(paths)
    assert native.portfolio_executed.sum() == 10
    policy = partial_policy(.75)
    result = refinement.ledger(native, paths, "partial", {"partial": policy},
        {"parent": {0: close_event(paths[0])}}, {}).set_index("sid")
    assert result.portfolio_executed.sum() == 10
    assert not result.loc[10, "portfolio_executed"]
    assert result.loc[0, "exit_ts"] == pd.Timestamp("2026-10-16 10:10", tz="Asia/Kolkata")
    assert result.loc[10, "portfolio_reserved_capital_before_rupees"] == pytest.approx(1000000.)


def stop_policy():
    choices = refinement.stops.catalog()
    baseline = next(item for item in choices if item["id"] == "baseline")
    protected = next(item for item in choices if item["family"] == "profit_floor"
        and item["params"]["activation_floor_pct"] == .4
        and item["params"]["target_fraction"] == .25
        and item["params"]["protected_profit_pct"] == .1)
    return baseline, dict(protected, kind="resting_stop")


def test_resting_outcome_matrix_and_full_ledger_match_actual_simulator(monkeypatch):
    paths = {1: minute_path(protect=True)}
    native = native_book(paths)
    baseline, protected = stop_policy()
    monkeypatch.setattr(refinement.stops, "catalog", lambda: [baseline, protected])
    initial = pd.DataFrame({"baseline": native.set_index("sid").net_return_pct * 5000.})
    matrix, outcomes = refinement.refine(native, paths, initial, {"baseline": {}}, [protected])
    result = refinement.ledger(native, paths, protected["id"], {protected["id"]: protected}, {}, outcomes)
    actual = outcomes[protected["id"]][1]
    assert actual["exit_reason"] == "PROTECTED_STOP"
    assert actual["exit_price"] == pytest.approx(100.1)
    assert matrix.loc[1, protected["id"]] == pytest.approx(250.)
    assert result.iloc[0].net_profit_rupees == pytest.approx(matrix.loc[1, protected["id"]])
    assert result.iloc[0].exit_ts == actual["exit_ts"]
    assert result.iloc[0].exit_execution_ts == actual["exit_execution_ts"]
    assert result.iloc[0].exit_price == pytest.approx(actual["exit_price"])
    assert result.iloc[0].portfolio_cost_rupees == pytest.approx(250.)


def test_full_stop_exit_can_admit_new_order_while_primary_cohort_remains_fixed(monkeypatch):
    paths = {sid: minute_path() for sid in range(10)}
    paths[0] = minute_path(protect=True)
    paths[10] = minute_path(start="2026-10-16 10:05")
    native = native_book(paths)
    baseline, protected = stop_policy()
    monkeypatch.setattr(refinement.stops, "catalog", lambda: [baseline, protected])
    initial = pd.DataFrame({"baseline": native.set_index("sid").net_return_pct * 5000.})
    matrix, outcomes = refinement.refine(native, paths, initial, {"baseline": {}}, [protected])
    full = refinement.ledger(native, paths, protected["id"], {protected["id"]: protected}, {}, outcomes)
    assert native.portfolio_executed.sum() == 10
    assert full.portfolio_executed.sum() == 11
    original = native.loc[native.portfolio_executed].set_index("sid")
    metric = refinement.core.metrics(matrix.baseline.loc[original.index], matrix.loc[original.index, protected["id"]], original, ["2026-10-16"])
    assert metric["trades"] == 10
    assert full.set_index("sid").loc[10, "portfolio_executed"]


def test_corrupt_native_control_is_rejected_before_research_metrics(monkeypatch):
    paths = {1: minute_path(protect=True)}
    native = native_book(paths)
    baseline, protected = stop_policy()
    monkeypatch.setattr(refinement.stops, "catalog", lambda: [baseline, protected])
    real = refinement.stops.outcomes_for_trade

    def corrupted(row, path, policies):
        outcomes = real(row, path, policies)
        outcomes["baseline"]["exit_price"] += .01
        return outcomes

    monkeypatch.setattr(refinement.stops, "outcomes_for_trade", corrupted)
    initial = pd.DataFrame({"baseline": native.set_index("sid").net_return_pct * 5000.})
    with pytest.raises(ValueError, match="native control differs"):
        refinement.refine(native, paths, initial, {"baseline": {}}, [protected])


def test_tradeoff_selection_permits_declining_net_but_never_relaxes_winner_floor():
    common = dict(wins=9, baseline_wins=10, losses=5, baseline_losses=6, changed_trades=3)
    development = pd.DataFrame([
        dict(policy="tradeoff", original_winner_profit_retention_pct=95., net_change_rs=-100., loss_saved_rs=200., **common),
        dict(policy="too_costly", original_winner_profit_retention_pct=94.9, net_change_rs=1000., loss_saved_rs=1000., **common),
    ])
    assert refinement.best_tradeoff(development) == "tradeoff"
    assert refinement.core.choose(development) == "baseline"
    development.loc[development.policy.eq("tradeoff"), "original_winner_profit_retention_pct"] = 94.9
    assert refinement.best_tradeoff(development) == "baseline"
