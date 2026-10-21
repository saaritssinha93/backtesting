"""The target cutoff replays executions and reallocates released capital."""
from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research import g3_target_cutoff_backtest as history


DAY = date(2026, 10, 13)


def test_current_profile_restores_exact_three_targets_and_keeps_ten_disabled():
    allowed, removed = history._validate_profile()
    expected = {"0951_LONG": .75, "1006_LONG": .56, "1016_SHORT": .68}
    assert len(allowed) == 51 and len(removed) == 10
    assert {s.setup_id: s.target_pct for s in history.contract.ACTIVE_SETUPS if s.target_pct <= .75} == expected
    assert removed == {s.setup_id for s in history.production.ACTIVE_SETUPS
                       if s.target_pct <= .75 and s.setup_id not in expected}


def _inputs(count=11):
    confirmation = pd.Timestamp(f"{DAY} 12:36", tz="Asia/Kolkata")
    timestamps = pd.date_range(confirmation + pd.Timedelta(minutes=1),
        pd.Timestamp(f"{DAY} 15:15", tz="Asia/Kolkata"), freq="min")
    path = dict(timestamp_ns=timestamps.asi8, open=np.full(len(timestamps), 100.),
        high=np.full(len(timestamps), 100.1), low=np.full(len(timestamps), 99.9), close=np.full(len(timestamps), 100.))
    orders = pd.DataFrame([dict(sid=i, day=str(DAY), signal_id=f"old-{i}", setup_id="1236_LONG" if i == 0 else "1331_LONG",
        tradingsymbol=f"EXAMPLE{i:02}", side="LONG", confirmation_ts=confirmation, trigger=100.,
        picker="max_liquidity", traded_value=1000-i, native_target_pct=.37 if i == 0 else 1.17,
        native_stop_pct=1.25) for i in range(count)])
    return orders, {i: {k: v.copy() for k, v in path.items()} for i in range(count)}


def test_filter_before_simulation_releases_capital_for_previously_blocked_fill():
    orders, paths = _inputs()
    baseline = history._simulate(orders, paths, [DAY], history.production)
    kept = orders.loc[orders.native_target_pct.gt(.75)].copy()
    kept["source_signal_id"] = kept.signal_id
    kept["signal_id"] = "new-" + kept.sid.astype(str)
    filtered = history._simulate(kept, paths, [DAY], history.contract)
    history._same_native(filtered, baseline.loc[baseline.native_target_pct.gt(.75)])
    assert baseline.portfolio_executed.sum() == 10
    assert not baseline.loc[baseline.sid.eq(10), "portfolio_executed"].item()
    assert filtered.portfolio_executed.sum() == 10
    assert filtered.loc[filtered.sid.eq(10), "portfolio_executed"].item()
    assert filtered.portfolio_cost_rupees.sum() == 2500
    assert filtered.strategy_version.eq(history.contract.STRATEGY_VERSION).all()
    assert baseline.strategy_version.eq(history.production.STRATEGY_VERSION).all()
    portfolio, _, _, admitted, rejected = history._comparison(baseline, filtered, [DAY], {"1331_LONG"})
    assert admitted.sid.tolist() == [10]
    assert rejected.empty
    assert portfolio.loc[portfolio.scope.eq("retained_at_original_allocation"), "executed_trades"].item() == 9


@pytest.mark.parametrize("empty", [False, True])
def test_unfilled_or_empty_day_retains_zero_trade_accounting(empty):
    orders, paths = _inputs(1)
    orders["trigger"] = 200.
    if empty:
        orders = orders.iloc[:0]
    actual = history._simulate(orders, paths, [DAY], history.contract)
    daily = history._daily(actual, [DAY])
    assert daily.trades.tolist() == [0]
    assert daily.net_pnl.tolist() == [0.]
    assert daily.cost.tolist() == [0.]


def test_native_timestamp_drift_is_rejected_even_with_unchanged_profit():
    orders, paths = _inputs(1)
    saved = history._simulate(orders, paths, [DAY], history.production)
    changed = saved.copy()
    changed["exit_ts"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="execution timestamp"):
        history._same_native(changed, saved)


def test_realized_ledger_cannot_be_used_as_selected_order_inputs():
    orders, paths = _inputs(1)
    ledger = history._simulate(orders, paths, [DAY], history.production)
    with pytest.raises(ValueError, match="pre-execution"):
        history._check_order_inputs(ledger)


def test_source_reference_rejects_escape_and_manifest_drift(tmp_path):
    with pytest.raises(ValueError, match="escaped"):
        history._child(tmp_path, "../outside")
    (tmp_path / "manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="manifest changed"):
        history._checked_reference(tmp_path, dict(run_dir=".", manifest_sha256="wrong"))


def test_unused_historical_candidate_path_cannot_collide_with_later_selected_order():
    orders, actual_paths = _inputs(2)
    historical_orders = orders.iloc[:1]
    daily_order = orders.iloc[1:]
    stale_candidate = {key: value.copy() for key, value in actual_paths[1].items()}
    # This unused historical candidate has the same local SID as a daily order
    # but different prices. Retaining it could either block the merge or silently
    # supply the wrong execution path to the selected later order.
    for field in ("open", "high", "low", "close"):
        stale_candidate[field] += 50.
    historical_pool = {0: actual_paths[0], 1: stale_candidate}
    selected = history._selected_path_subset(historical_pool, historical_orders)
    assert set(selected) == {0}
    assert selected[0] is actual_paths[0]
    assert 1 in historical_pool  # pruning must not mutate the source cache
    selected.update({1: actual_paths[1]})
    daily_result = history._simulate(daily_order, selected, [DAY], history.production)
    assert daily_result.entry_price.item() == 100.
    assert daily_result.exit_price.item() == 100.


def test_path_pruning_rejects_missing_and_duplicated_selected_ids():
    orders, paths = _inputs(2)
    with pytest.raises(ValueError, match="paths missing"):
        history._selected_path_subset({0: paths[0]}, orders)
    with pytest.raises(ValueError, match="identifiers are duplicated"):
        history._selected_path_subset(paths, pd.concat([orders.iloc[:1], orders.iloc[:1]]))


def test_package_artifact_tamper_fails_before_accounting(tmp_path):
    manifest = dict(schema_version=history.SCHEMA, execution_authority=False,
        files={name: "wrong" for name in history.FILES})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    for name in history.FILES:
        (tmp_path / name).write_text("changed")
    with pytest.raises(ValueError, match="artifact changed"):
        history.verified_package(tmp_path, tmp_path)


def _without_new_slot_candidates(monkeypatch, orders):
    """Keep synthetic portfolio fixtures isolated from the real market archive."""
    from research import g3_two_slot_backtest as new_slots
    proof = dict(setup_ids=sorted(new_slots.SETUP_IDS), outcomes_used_for_selection=False,
        candidate_rows=0, selected_orders=0,
        selected_by_setup={setup_id: 0 for setup_id in sorted(new_slots.SETUP_IDS)}, opening_coverage={})
    audit = pd.DataFrame(columns=["day", "setup_id", "tradingsymbol", "sid", "v9_selected"])
    monkeypatch.setattr(new_slots, "reselect", lambda *args: (orders.iloc[:0].copy(), {}, proof, audit))


def test_complete_package_roundtrip_reconciles_comparisons_and_rejects_resealed_drift(tmp_path, monkeypatch):
    orders, paths = _inputs()
    orders["strategy_version"] = history.baseline_contract.STRATEGY_VERSION
    orders["strategy_fingerprint"] = history.baseline_contract.strategy_fingerprint()
    saved = history._simulate(orders, paths, [DAY], history.baseline_contract)
    proof = dict(run_dir="pinned", manifest_sha256="known")
    monkeypatch.setattr(history, "_baseline", lambda root: (orders, saved, [DAY], [], proof))
    monkeypatch.setattr(history, "_historical_paths", lambda days: (paths, {}))
    monkeypatch.setattr(history, "_revised_0941_orders", lambda *args: (orders.iloc[:0].copy(), {}, {}))
    _without_new_slot_candidates(monkeypatch, orders)
    monkeypatch.setattr(history, "_provenance", lambda: {"test": "stable"})
    monkeypatch.setattr(history.replay, "require_closed_session", lambda through: None)
    from types import SimpleNamespace
    output = tmp_path / "backtests/runs/2026-10-13/run"
    result = history.build(tmp_path, output, DAY, roots=SimpleNamespace(universe=tmp_path / "universe"))
    assert result["newly_admitted_fills"] == 1
    assert result["comparison_sessions"] == 1
    history.verified_package(output, tmp_path)
    comparison = output / "portfolio_comparison.csv"
    values = pd.read_csv(comparison)
    scope = f"filtered_{len(history.contract.ACTIVE_SETUPS)}_reallocated"
    values.loc[values.scope.eq(scope), "net_pnl_rs"] += 1
    values.to_csv(comparison, index=False)
    manifest = json.loads((output / "manifest.json").read_text())
    manifest["files"][comparison.name] = history.sha256(comparison)
    (output / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(AssertionError):
        history.verified_package(output, tmp_path)


def test_fresh_build_retains_all_three_low_target_exceptions(tmp_path, monkeypatch):
    orders, paths = _inputs(5)
    identities = ["0951_LONG", "1006_LONG", "1016_SHORT", "1236_LONG", "1331_LONG"]
    for index, setup_id in enumerate(identities):
        spec = history.production._ACTIVE_BY_ID[setup_id]
        orders.loc[index, ["setup_id", "side", "native_target_pct"]] = [setup_id, spec.side, spec.target_pct]
    orders["strategy_version"] = history.baseline_contract.STRATEGY_VERSION
    orders["strategy_fingerprint"] = history.baseline_contract.strategy_fingerprint()
    saved = history._simulate(orders, paths, [DAY], history.baseline_contract)
    monkeypatch.setattr(history, "_baseline", lambda root: (orders, saved, [DAY], [], {"run_dir": "pinned"}))
    monkeypatch.setattr(history, "_historical_paths", lambda days: (paths, {}))
    monkeypatch.setattr(history, "_revised_0941_orders", lambda *args: (orders.iloc[:0].copy(), {}, {}))
    _without_new_slot_candidates(monkeypatch, orders)
    monkeypatch.setattr(history, "_provenance", lambda: {"test": "stable"})
    monkeypatch.setattr(history.replay, "require_closed_session", lambda through: None)
    from types import SimpleNamespace
    output = tmp_path / "backtests/runs/2026-10-13/restored"
    summary = history.build(tmp_path, output, DAY, roots=SimpleNamespace(universe=tmp_path / "universe"))
    selected = pd.read_csv(output / "selected_orders.csv")
    ledger = pd.read_csv(output / "portfolio_trades.csv")
    assert set(selected.setup_id) == set(identities) - {"1236_LONG"}
    assert set(ledger.setup_id) == set(selected.setup_id)
    assert summary["active_setup_count"] == 51
    assert summary["disabled_setup_count"] == 10
    assert summary["restored_setup_ids"] == sorted(history.RESTORED_TARGETS)
    assert ledger.loc[ledger.setup_id.isin(history.RESTORED_TARGETS), "portfolio_executed"].all()


def test_archived_v8_package_keeps_original_identity_and_cannot_replace_current_pointer():
    root = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3")
    directory = root / "backtests/runs/2026-10-13/20261013_target_gt075_v8_normal_cli_rerun"
    if not directory.exists():
        pytest.skip("Archived V8 integration evidence is not available on this host")
    before = history.sha256(directory / "manifest.json")
    summary, _ = history.verified_package(directory, root)
    assert summary["strategy_version"] == "V13-V10-G-3-BACKTEST-TARGET-GT075-V8"
    assert summary["active_setup_count"] == 46 and summary["disabled_setup_count"] == 13
    assert summary["current_g3"]["executed_trades"] == 428
    with pytest.raises(ValueError, match="archived backtest"):
        history.publish(root, directory)
    assert history.sha256(directory / "manifest.json") == before
