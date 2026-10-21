"""Bounded whole-setup target/add/delete refinement of the frozen G4 afternoon book.

This is retrospective local optimization. Entry decisions and stops are fixed;
individual trade removal is never an available search operation.
"""
from __future__ import annotations

import argparse
import heapq
import html
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from research import g3_v2_contract as contract
from research.g3_0920_backtest import allocate, _stats, _config
from research.g3_0920_target_sweep import assert_same_entries
from research.g3_g4_late_combined_backtest import checked_allocate, same_raw, verified_run
from research.g3_g4_late_target_sweep import load_paths, replay_target
from research.g4_extended_slot_backtest import portfolio_changed
from research.g4_extended_target_sweep import BACKTEST_ROOT, ROOT, read_frozen_csv
from research.g4_late_slot_source import sha256

VERSION = "V13-V10-G-4-1235-1330-PORTFOLIO-TARGET-REFINED-V4"
DUAL70_VERSION = "V13-V10-G-4-1235-1330-DUAL70-TARGET-REFINED-V5"
SOURCE_VERSION = "V13-V10-G-4-1235-1330-TARGET-SWEEP-V3"
ENTRY_VERSION = "V13-V10-G-4-BACKTEST-1235-1330-V6"
DEFAULT_SOURCE = BACKTEST_ROOT / "target_sweeps/2026-10-13/20261013_afternoon_1235_1330_targets_v3"
DEFAULT_OUTPUT = BACKTEST_ROOT / "target_sweeps/2026-10-13/20261013_afternoon_portfolio_targets_v4"


def passes(count, wins, pnl, *, minimum=5):
    """Use exact integer arithmetic at the inclusive 70% boundary."""
    return bool(count >= minimum and 10 * wins >= 7 * count and np.isfinite(pnl) and pnl > 0)


def heap_accept(entries, exits, ordered_indices, *, capacity=10):
    """Release exits at/before an entry, using already-established native priority."""
    if capacity < 1:
        raise ValueError("Capacity must be positive")
    accepted = []
    active = []
    for index in ordered_indices:
        entry = entries[index]
        while active and active[0] <= entry:
            heapq.heappop(active)
        if len(active) < capacity:
            accepted.append(index)
            heapq.heappush(active, exits[index])
    return np.asarray(accepted, dtype=np.int64)


class FastBook:
    """Fixed-entry matrix of all native target outcomes with a small heap allocator."""

    def __init__(self, baseline, cache, days, *, require_native70=False):
        self.require_native70 = bool(require_native70)
        policy = _config()
        limits = policy.portfolio_config()
        if (policy.capital_per_entry_rupees != 100_000 or policy.leverage_factor != 5
                or limits.portfolio_capital_rupees != 1_000_000
                or limits.max_positions not in (None, 10)
                or any(getattr(limits, key) is not None for key in
                       ("max_positions_per_symbol", "max_gross_exposure_rupees", "max_open_risk_rupees"))):
            raise ValueError("Fast allocator supports only the pinned G4 fixed-capital policy")
        self.days = list(days)
        self.targets = sorted(cache.target_pct.unique())
        self.target_index = {round(float(value), 2): i for i, value in enumerate(self.targets)}
        first = cache.loc[cache.target_pct.eq(self.targets[0])].sort_values("sid").reset_index(drop=True)
        if first.sid.duplicated().any() or baseline.sid.duplicated().any() or set(first.sid) & set(baseline.sid):
            raise ValueError("Native cohort identities overlap")
        self.setups = sorted(first.setup_id.unique())
        self.baseline_count = len(baseline)
        self.frame = pd.concat([baseline, first], ignore_index=True)
        self.frame["day"] = pd.to_datetime(self.frame.day).dt.strftime("%Y-%m-%d")
        self.day_index = self.frame.day.map({day: i for i, day in enumerate(days)}).to_numpy(int)
        self.setup_index = self.frame.setup_id.map({key: i for i, key in enumerate(self.setups)}).fillna(-1).to_numpy(int)
        self.new_indices = {key: np.flatnonzero(self.setup_index == i) for i, key in enumerate(self.setups)}
        self.filled = self.frame.filled.eq(True).to_numpy(bool)
        self.entries = pd.to_datetime(self.frame.entry_ts, utc=True).astype("int64").to_numpy()
        priority = simulator.g.v9.v6.prepare_source_ledger(self.frame)
        priority["_confirmation"] = priority.confirmation_ts.fillna(priority.entry_ts)
        priority["_setup"] = priority.setup_id.fillna("").astype(str)
        self.order = priority.loc[self.filled].sort_values(
            ["entry_ts", "_confirmation", "_setup", "portfolio_priority_value", "tradingsymbol", "sid"],
            ascending=[True, True, True, False, True, True], kind="stable").index.to_numpy(int)
        self.expected_baseline = baseline.portfolio_executed.eq(True).to_numpy()
        self.base_exits = pd.to_datetime(self.frame.exit_ts, utc=True).astype("int64").to_numpy()
        self.base_net = pd.to_numeric(self.frame.net_return_pct).to_numpy(float) * 5000.
        self.base_gross = pd.to_numeric(self.frame.gross_return_pct).to_numpy(float) * 5000.
        self.base_cost = pd.to_numeric(self.frame.cost_pct).to_numpy(float) * 5000.
        self.exit_matrix = np.empty((len(self.targets), len(first)), dtype=np.int64)
        self.net_matrix = np.empty((len(self.targets), len(first)))
        self.gross_matrix = np.empty_like(self.net_matrix)
        self.cost_matrix = np.empty_like(self.net_matrix)
        invariant = ["sid", "setup_id", "signal_id", "filled", "entry_price", "entry_ts",
                     "confirmation_ts", "picker", "traded_value", "volume_ratio", "price_change_pct"]
        # CSV/parquet timestamps can differ in representation but their instants must agree.
        original = first[invariant].copy()
        for key in ("entry_ts", "confirmation_ts"):
            original[key] = pd.to_datetime(original[key], utc=True)
        for target, part in cache.groupby("target_pct", sort=True):
            part = part.sort_values("sid").reset_index(drop=True)
            observed = part[invariant].copy()
            for key in ("entry_ts", "confirmation_ts"):
                observed[key] = pd.to_datetime(observed[key], utc=True)
            pd.testing.assert_frame_equal(original, observed, check_dtype=False, check_exact=True)
            index = self.target_index[round(float(target), 2)]
            self.exit_matrix[index] = pd.to_datetime(part.exit_ts, utc=True).astype("int64")
            self.net_matrix[index] = pd.to_numeric(part.net_return_pct).to_numpy(float) * 5000.
            self.gross_matrix[index] = pd.to_numeric(part.gross_return_pct).to_numpy(float) * 5000.
            self.cost_matrix[index] = pd.to_numeric(part.cost_pct).to_numpy(float) * 5000.
        if not np.isfinite(self.net_matrix[:, first.filled.eq(True)]).all():
            raise ValueError("Nonfinite executed candidate profit")
        # Standalone qualification uses every native fill of the fixed entry
        # cohort. Capital competition cannot hide losing fills from this gate.
        self.native_qualification = np.zeros((len(self.targets), len(self.setups)), dtype=bool)
        self.native_counts = np.zeros(len(self.setups), dtype=int)
        self.native_wins = np.zeros_like(self.native_qualification, dtype=int)
        self.native_pnl = np.zeros_like(self.native_qualification, dtype=float)
        for setup_index, setup in enumerate(self.setups):
            rows = self.new_indices[setup]
            columns = rows[self.filled[rows]] - self.baseline_count
            values = self.net_matrix[:, columns]
            self.native_counts[setup_index] = len(columns)
            self.native_wins[:, setup_index] = (values > 0).sum(axis=1)
            self.native_pnl[:, setup_index] = values.sum(axis=1)
            self.native_qualification[:, setup_index] = (
                (len(columns) >= 5) & (10 * self.native_wins[:, setup_index] >= 7 * len(columns))
                & np.isfinite(values).all(axis=1) & (self.native_pnl[:, setup_index] > 0))
        self.setup_number = {setup: i for i, setup in enumerate(self.setups)}

    def state(self, target_map):
        if set(target_map) - set(self.setups):
            raise ValueError("Unknown setup")
        selected = np.zeros(len(self.frame), dtype=bool)
        selected[:self.baseline_count] = True
        exits, net, gross, cost = (arr.copy() for arr in
                                 (self.base_exits, self.base_net, self.base_gross, self.base_cost))
        for setup, target in target_map.items():
            index = self.target_index[round(float(target), 2)]
            rows = self.new_indices[setup]
            columns = rows - self.baseline_count
            selected[rows] = True
            exits[rows] = self.exit_matrix[index, columns]
            net[rows] = self.net_matrix[index, columns]
            gross[rows] = self.gross_matrix[index, columns]
            cost[rows] = self.cost_matrix[index, columns]
        accepted = heap_accept(self.entries, exits, self.order[selected[self.order]])
        accepted_mask = np.zeros(len(self.frame), dtype=bool)
        accepted_mask[accepted] = True
        return selected, accepted, accepted_mask, net, gross, cost

    def evaluate(self, target_map, *, detailed=False):
        selected, accepted, accepted_mask, net, gross, cost = self.state(target_map)
        added = accepted[accepted >= self.baseline_count]
        count, wins, pnl = len(accepted), int((net[accepted] > 0).sum()), float(net[accepted].sum())
        add_count, add_wins, add_pnl = len(added), int((net[added] > 0).sum()), float(net[added].sum())
        counts = np.bincount(self.setup_index[added], minlength=len(self.setups))
        win_counts = np.bincount(self.setup_index[added], weights=net[added] > 0, minlength=len(self.setups))
        profits = np.bincount(self.setup_index[added], weights=net[added], minlength=len(self.setups))
        changed = int(np.count_nonzero(accepted_mask[:self.baseline_count] != self.expected_baseline))
        each = bool(target_map) and all(passes(counts[i], win_counts[i], profits[i])
                    for i, setup in enumerate(self.setups) if setup in target_map)
        native_each = bool(target_map) and all(self.native_qualification[
            self.target_index[round(float(target), 2)], self.setup_number[setup]]
            for setup, target in target_map.items())
        result = dict(eligible=bool(each and passes(count, wins, pnl) and passes(add_count, add_wins, add_pnl)
                                   and changed == 0 and (native_each or not self.require_native70)),
            combined_net_pnl_rs=pnl, combined_executed_trades=count, combined_wins=wins,
            combined_win_rate_pct=100 * wins / count if count else None,
            added_net_pnl_rs=add_pnl, added_executed_trades=add_count, added_wins=add_wins,
            added_win_rate_pct=100 * add_wins / add_count if add_count else None,
            every_setup_70_positive=each, native_every_setup_70_positive=bool(native_each),
            baseline_trades_changed=changed)
        if detailed:
            result["periods"] = {}
            for period, day_ids in (("train", np.arange(38)), ("holdout", np.arange(38, len(self.days))),
                                    ("all", np.arange(len(self.days)))):
                for scope, is_added in (("", False), ("added_", True)):
                    mask = selected & np.isin(self.day_index, day_ids)
                    if is_added:
                        mask[:self.baseline_count] = False
                    taken = accepted_mask & mask
                    daily = np.bincount(self.day_index[taken], weights=net[taken], minlength=len(self.days))[day_ids]
                    curve = np.r_[0., daily.cumsum()]
                    stats = dict(selected_orders=int(mask.sum()), native_fills=int((mask & self.filled).sum()),
                        executed_trades=int(taken.sum()), wins=int((net[taken] > 0).sum()), losses=int((net[taken] < 0).sum()),
                        win_rate_pct=float((net[taken] > 0).mean() * 100) if taken.any() else None,
                        gross_pnl_rs=float(gross[taken].sum()), costs_rs=float(cost[taken].sum()),
                        net_pnl_rs=float(net[taken].sum()), max_drawdown_rs=float((np.maximum.accumulate(curve)-curve).max()))
                    result["periods"].update({f"{scope}{period}_{key}": value for key, value in stats.items()})
        return result


def coordinate_refine(initial, universe, targets, evaluate, *, max_passes=5):
    """Best improvement across one whole-setup add/change/delete per bounded pass."""
    if not 1 <= max_passes <= 100 or not initial or set(initial) - set(universe):
        raise ValueError("Invalid coordinate-search starting map or pass bound")
    current = dict(initial)
    best = evaluate(current)
    if not best["eligible"]:
        raise ValueError("Initial map does not qualify on actual executions")
    trials = [dict(iteration=0, operation="initial", setup_id="", target_pct=None, accepted=True,
                   improvement_rs=0., target_map=json.dumps(current, sort_keys=True), **best)]
    accepted_maps = [dict(iteration=0, targets=dict(current), **best)]
    converged = False
    for iteration in range(1, max_passes + 1):
        winner = None
        for setup in sorted(universe):
            options = [("change" if setup in current else "add", float(target)) for target in sorted(set(targets))
                       if current.get(setup) != target]
            if setup in current and len(current) > 1:
                options.append(("delete", None))
            for operation, target in options:
                candidate = dict(current)
                if target is None:
                    del candidate[setup]
                else:
                    candidate[setup] = target
                metrics = evaluate(candidate)
                row = dict(iteration=iteration, operation=operation, setup_id=setup, target_pct=target,
                           accepted=False, improvement_rs=metrics["combined_net_pnl_rs"]-best["combined_net_pnl_rs"],
                           target_map=json.dumps(candidate, sort_keys=True), **metrics)
                trials.append(row)
                if metrics["eligible"] and row["improvement_rs"] > 1e-6:
                    key = (-round(metrics["combined_net_pnl_rs"], 8), setup, operation, -1 if target is None else target)
                    if winner is None or key < winner[0]:
                        winner = (key, row, candidate, metrics)
        if winner is None:
            converged = True
            break
        _, row, current, best = winner
        row["accepted"] = True
        accepted_maps.append(dict(iteration=iteration, operation=row["operation"], setup_id=row["setup_id"],
                                  target_pct=row["target_pct"], targets=dict(current), **best))
        print(f"Coordinate pass {iteration}: {row['operation']} {row['setup_id']} target={row['target_pct']}; net={best['combined_net_pnl_rs']:.2f}", flush=True)
    return current, trials, accepted_maps, converged


def validate_common_targets(book, common):
    checks = []
    if list(np.round(common.target_pct.to_numpy(float), 2)) != list(np.round(book.targets, 2)):
        raise ValueError("Common-target validation omitted target levels")
    for row in common.to_dict("records"):
        fast = book.evaluate({setup: row["target_pct"] for setup in book.setups}, detailed=True)
        for key, value in {**fast["periods"], "baseline_trades_changed": fast["baseline_trades_changed"]}.items():
            wanted = row[key]
            equal = ((value is None and pd.isna(wanted)) or
                     (value is not None and np.isclose(value, wanted, rtol=0, atol=1e-6)))
            checks.append(dict(target_pct=row["target_pct"], metric=key, passed=bool(equal)))
            if not equal:
                raise ValueError(f"Fast/native common-target mismatch {row['target_pct']} {key}: {value} != {wanted}")
    return pd.DataFrame(checks)


def standalone_stats(frame):
    """Report all native fills before portfolio capital acceptance."""
    filled = frame.loc[frame.filled.eq(True)]
    pnl = pd.to_numeric(filled.net_return_pct, errors="raise") * 5000.
    return dict(standalone_native_fills=len(filled), standalone_native_wins=int(pnl.gt(0).sum()),
        standalone_native_win_rate_pct=100 * float(pnl.gt(0).mean()) if len(filled) else None,
        standalone_native_net_pnl_rs=float(pnl.sum()))


def run(source=DEFAULT_SOURCE, output=DEFAULT_OUTPUT, *, source_manifest_sha256, max_passes=5,
        initial_config=None, require_native70=False):
    version = DUAL70_VERSION if require_native70 else VERSION
    source, output = Path(source).resolve(), Path(output).resolve()
    if not source.is_relative_to(BACKTEST_ROOT.resolve()) or sha256(source / "manifest.json") != source_manifest_sha256:
        raise ValueError("Source target manifest drift")
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Output must be new and within G4 research root")
    modules = [Path(__file__), ROOT / "research/g4_extended_target_sweep.py", ROOT / "research/g4_extended_slot_backtest.py",
        ROOT / "research/g3_0920_backtest.py", ROOT / "research/g3_0920_target_sweep.py",
        ROOT / "research/g3_g4_late_target_sweep.py", ROOT / "research/g3_g4_late_combined_backtest.py",
        ROOT / "research/g3_v2_contract.py", Path(simulator.__file__), Path(simulator.g.v9.v5.__file__), Path(simulator.g.v9.v6.__file__)]
    code_hashes = {str(path.relative_to(ROOT)): sha256(path) for path in modules}
    proof = verified_run(source, SOURCE_VERSION)
    summary_source = json.loads((source / "summary.json").read_text())
    if (summary_source["setups"], summary_source["target_levels"], summary_source["target_min_pct"], summary_source["target_max_pct"]) != (24, 995, .06, 10.):
        raise ValueError("Expected all 995 afternoon target levels")
    reference = Path(summary_source["source"]["run_dir"])
    if sha256(reference / "manifest.json") != summary_source["source"]["manifest_sha256"]:
        raise ValueError("Entry-source manifest drift")
    entry_proof = verified_run(reference, ENTRY_VERSION)
    days = sorted(read_frozen_csv(source / "daily_results.csv").day.unique())
    if len(days) != 49:
        raise ValueError("Expected 49 frozen sessions")
    configs = json.loads((source / "scenario_configs.json").read_text())
    initial_name = summary_source["best_tested_every_setup70_scenario"]
    if not initial_name:
        raise ValueError("No qualifying starting target scenario")
    initial = {item["setup_id"]: item["target_pct"] for item in configs["scenarios"][initial_name]}
    external = None
    if initial_config:
        path = Path(initial_config).resolve()
        external = dict(path=str(path), sha256=sha256(path))
        payload = json.loads(path.read_text())
        if payload.get("scope") != "BACKTEST_ONLY" or payload.get("execution_authority") is not False:
            raise ValueError("Starting configuration must be offline research")
        initial = {item["setup_id"]: item["target_pct"] for item in payload["setups"]}
        initial_name = "provided_research_config"
    orders = read_frozen_csv(reference / "selected_orders.csv")
    baseline_saved = read_frozen_csv(reference / "baseline_g4_trades.csv")
    baseline = checked_allocate(baseline_saved, baseline_saved)
    cache = pd.read_parquet(source / "minute_replay_all_targets.parquet")
    book = FastBook(baseline, cache, days, require_native70=require_native70)
    parity = validate_common_targets(book, read_frozen_csv(source / "common_target_sweep.csv"))
    print(f"Fast allocator validated: {len(parity)} aggregate comparisons across all 995 targets", flush=True)
    final_map, trials, accepted_maps, converged = coordinate_refine(initial, book.setups, book.targets, book.evaluate, max_passes=max_passes)
    for field in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        orders[field] = pd.to_datetime(orders[field], utc=True).dt.tz_convert(contract.IST)
    wanted = orders.loc[orders.setup_id.isin(final_map)].copy()
    paths, path_proof = load_paths(wanted, json.loads((reference / "summary.json").read_text()))
    fresh = replay_target(wanted, paths, final_map)
    cached_final = pd.concat([cache.loc[cache.setup_id.eq(setup) & cache.target_pct.eq(target)] for setup, target in sorted(final_map.items())], ignore_index=True)
    same_raw(fresh, cached_final)
    assert_same_entries(fresh, cached_final)
    refined = allocate(pd.concat([baseline, fresh], ignore_index=True))
    if portfolio_changed(baseline, refined):
        raise ValueError("Final replay changes baseline executions")
    final_metrics = book.evaluate(final_map)
    for key, actual in (("combined_net_pnl_rs", _stats(refined, days)["net_pnl_rs"]),
                        ("combined_executed_trades", _stats(refined, days)["executed_trades"])):
        if not np.isclose(final_metrics[key], actual, rtol=0, atol=1e-6):
            raise ValueError("Fresh final replay differs from fast search")
    accepted_ids = set(book.frame.iloc[book.state(final_map)[1]].sid)
    if accepted_ids != set(refined.loc[refined.portfolio_executed.eq(True), "sid"]):
        raise ValueError("Fresh native acceptance differs from fast search")
    output.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(trials).to_csv(output / "refinement_trials.csv", index=False)
    parity.to_csv(output / "fast_allocator_validation.csv", index=False)
    (output / "accepted_maps.json").write_text(json.dumps(accepted_maps, indent=2) + "\n")
    result_rows, setup_rows, daily_rows, monthly_rows = [], [], [], []
    initial_raw = pd.concat([cache.loc[cache.setup_id.eq(setup) & cache.target_pct.eq(target)] for setup, target in sorted(initial.items())], ignore_index=True)
    initial_ledger = allocate(pd.concat([baseline, initial_raw], ignore_index=True))
    for name, ledger, targets in (("baseline_g4", baseline, {}), ("initial_qualified", initial_ledger, initial), ("refined_qualified", refined, final_map)):
        added = ledger.loc[ledger.setup_id.isin(targets)]
        for period, cohort in (("train", days[:38]), ("holdout", days[38:]), ("all", days)):
            for scope, frame in (("combined", ledger), ("additions", added)):
                if targets or scope == "combined":
                    result_rows.append(dict(scenario=name, scope=scope, period=period, **_stats(frame.loc[frame.day.isin(cohort)], cohort)))
            for setup, target in sorted(targets.items()):
                part = added.loc[added.setup_id.eq(setup) & added.day.isin(cohort)]
                stats, native = _stats(part, cohort), standalone_stats(part)
                signal = orders.loc[orders.setup_id.eq(setup)].iloc[0]
                minimum = 3 if period == "holdout" else 5
                setup_rows.append(dict(scenario=name, setup_id=setup, signal_end=signal.signal_end,
                    side=signal.side, setup=f"{signal.signal_end}_{signal.side}", target_pct=target, period=period,
                    **stats, **native,
                    observed_70_positive=passes(stats["executed_trades"], stats["wins"], stats["net_pnl_rs"], minimum=minimum),
                    standalone_native_70_positive=passes(native["standalone_native_fills"], native["standalone_native_wins"],
                        native["standalone_native_net_pnl_rs"], minimum=minimum)))
        for day in days:
            daily_rows.append(dict(scenario=name, day=day, **_stats(ledger.loc[ledger.day.eq(day)], [day])))
        for month in sorted({day[:7] for day in days}):
            cohort = [day for day in days if day.startswith(month)]
            monthly_rows.append(dict(scenario=name, month=month, **_stats(ledger.loc[ledger.day.isin(cohort)], cohort)))
        ledger.to_csv(output / f"{name}_trades.csv", index=False)
    fresh.to_csv(output / "refined_added_raw_trades.csv", index=False)
    results, setups = pd.DataFrame(result_rows), pd.DataFrame(setup_rows)
    final_setup_rows = setups.loc[setups.scenario.eq("refined_qualified") & setups.period.eq("all")]
    if (not final_metrics["eligible"] or not final_setup_rows.observed_70_positive.all()
            or (require_native70 and not final_setup_rows.standalone_native_70_positive.all())):
        raise ValueError("Native final per-setup qualification failed")
    for name, frame in (("portfolio_results", results), ("scenario_setup_results", setups),
                        ("daily_results", pd.DataFrame(daily_rows)), ("monthly_results", pd.DataFrame(monthly_rows))):
        frame.to_csv(output / f"{name}.csv", index=False)
    original_setups = json.loads((reference / "selected_config.json").read_text())["setups"]
    portable = [{**item, "target_pct": final_map[item["setup_id"]]} for item in original_setups if item["setup_id"] in final_map]
    if {item["setup_id"]: item["target_pct"] for item in portable} != final_map:
        raise ValueError("Portable target configuration is incomplete")
    for item in portable:
        for key in ("training_pass", "chronological_check_pass"):
            if key in item:
                item["prior_entry_" + key] = item.pop(key)
    config = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        source=proof, fixed_policy=configs["fixed_policy"], setups=portable,
        require_native70=bool(require_native70),
        selection_policy=("BOUNDED_WHOLE_SETUP_TARGET_ADD_CHANGE_DELETE_MAX_COMBINED_PNL_EVERY_SETUP_NATIVE_AND_ACTUAL_70"
            if require_native70 else "BOUNDED_WHOLE_SETUP_TARGET_ADD_CHANGE_DELETE_MAX_COMBINED_PNL_EVERY_SETUP_70"))
    (output / "refined_config.json").write_text(json.dumps(config, indent=2) + "\n")
    summary = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), source=proof, entry_source=entry_proof,
        initial_scenario=initial_name, external_initial_config=external,
        sessions=49, first_date=days[0], last_date=days[-1], initial_setups=len(initial), final_setups=len(final_map),
        initial=book.evaluate(initial), final=final_metrics, initial_targets=initial, targets=final_map,
        net_pnl_improvement_rs=final_metrics["combined_net_pnl_rs"]-book.evaluate(initial)["combined_net_pnl_rs"],
        tested_configurations=len(trials)-1, accepted_changes=len(accepted_maps)-1,
        max_passes=max_passes, local_coordinate_convergence=converged, pass_limit_reached=not converged,
        all_995_common_targets_validated=True, allocator_validation_checks=len(parity),
        baseline_trades_changed=0, entries_and_stops_unchanged=True, fresh_native_replay_matches_cache=True,
        every_remaining_setup_70_positive_actual_executions=True, minute_path_evidence=path_proof,
        require_native70=bool(require_native70),
        every_remaining_setup_native70_positive=bool(final_setup_rows.standalone_native_70_positive.all()),
        historical_selection_is_in_sample=True, later_period_previously_used=True, untouched_out_of_sample=False,
        optimum_scope="BOUNDED_LOCAL_COORDINATE_TARGET_ADD_CHANGE_DELETE; NOT_GLOBAL_COMBINATORIAL_OPTIMUM",
        all_scenarios=json.loads(results.to_json(orient="records")))
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    page = '<!doctype html><html><meta charset="utf-8"><title>G4 afternoon portfolio target refinement</title><style>body{font:15px system-ui;margin:30px;color:#19324a;background:#f4f6fa}table{border-collapse:collapse;background:white}td,th{padding:8px;border:1px solid #ddd;text-align:right}.note{background:#fff0cb;padding:18px}a{color:#075ead}</style><h1>G4 12:35–13:30 portfolio target refinement</h1>'
    page += f'<p>{days[0]} through {days[-1]}; 49 sessions. {len(trials)-1:,} whole-setup target/add/delete trials, {len(accepted_maps)-1} accepted changes, {len(final_map)} retained setups. Local convergence: {converged}.</p>'
    page += '<p class="note">Retrospective historical optimization, including the previously-used later 11 sessions. This is neither an untouched out-of-sample result nor a global optimum. Every retained setup has at least five actual executions, at least 70% profitable trades after costs, and positive net P&amp;L. No individual trade was removed.</p>'
    if require_native70:
        page += '<p>The same minimum of five fills, 70% net wins and positive P&amp;L is also required for every setup using all its native fills before portfolio capital restrictions. This prevents rejected losing fills from creating a passing standalone configuration.</p>'
    page += '<p>Entry filters, orders, 1.25% stop tightened to 1.00% after 120 minutes, next-minute confirmation, ten-minute trigger expiry and 15:15 square-off are fixed. ₹100,000 margin per trade, 5× exposure, ₹1,000,000 portfolio capital and 5 bps round-trip costs. Existing baseline executions unchanged.</p>'
    for title, table in (("Full-period portfolio comparison", results.loc[results.period.eq("all")]),
                          ("Final setups: standalone and portfolio executions", final_setup_rows[[
                              "setup", "target_pct", "standalone_native_fills", "standalone_native_wins",
                              "standalone_native_win_rate_pct", "standalone_native_net_pnl_rs",
                              "executed_trades", "wins", "win_rate_pct", "net_pnl_rs",
                              "standalone_native_70_positive", "observed_70_positive"]]),
                          ("Later 11 sessions", results.loc[results.period.eq("holdout")]),
                          ("Accepted changes", pd.DataFrame(accepted_maps).drop(columns="targets"))):
        page += f'<h2>{html.escape(title)}</h2>' + table.to_html(index=False, border=0, float_format=lambda x: f"{x:,.2f}")
    page += '<p>' + ' · '.join(f'<a href="{name}">{name}</a>' for name in ("refined_config.json", "refinement_trials.csv", "accepted_maps.json", "daily_results.csv", "monthly_results.csv", "scenario_setup_results.csv", "refined_qualified_trades.csv", "fast_allocator_validation.csv", "summary.json", "manifest.json")) + '</p></html>'
    (output / "report.html").write_text(page, encoding="utf-8")
    if code_hashes != {str(path.relative_to(ROOT)): sha256(path) for path in modules}:
        raise ValueError("Refinement code changed while running")
    if sha256(source / "manifest.json") != source_manifest_sha256 or (external and sha256(Path(external["path"])) != external["sha256"]):
        raise ValueError("Source or initial configuration changed while running")
    manifest = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False, source=proof,
        code_sha256=code_hashes, files={path.name: sha256(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: summary[key] for key in ("initial", "final", "accepted_changes", "local_coordinate_convergence")}, indent=2), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-manifest-sha256", required=True)
    parser.add_argument("--max-passes", type=int, default=5)
    parser.add_argument("--initial-config", type=Path)
    parser.add_argument("--require-native70", action="store_true",
        help="Also require every setup to pass 70%%, five fills and positive P&L before capital allocation (V5)")
    args = parser.parse_args()
    run(args.source, args.output, source_manifest_sha256=args.source_manifest_sha256,
        max_passes=args.max_passes, initial_config=args.initial_config, require_native70=args.require_native70)


if __name__ == "__main__":
    main()
