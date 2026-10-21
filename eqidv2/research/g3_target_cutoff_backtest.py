"""Replay G3 backtests with the target cutoff and three restored setups.

The pinned 59-setup orders are pre-execution evidence. Their minute paths are
verified and replayed first; the retained orders are then replayed and allocated
again. PAPER/LIVE decision rules are never changed by this module.
"""
from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_ROOT, runtime_dir
import fno_v13_replay_source as source
import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as production
from research.archived_g3_v7 import contract as baseline_contract
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_backtest_profile import contract, CONFIG_PATH
from research.g3_0920_backtest import new_execution_paths, _stats
from research.g3_g4_late_combined_backtest import G3_RUN, same_raw
from research.g4_late_slot_backtest import _g3_baseline, configuration as old_config, DEFAULT_CONFIG
from research.g4_late_slot_source import load_late_source, sha256
from research.g4_extended_slot_backtest import (
    CLOCKS, DEFAULT_CACHE, MIDDAY_CACHE, MIDDAY_CLOCKS, AFTERNOON_CACHE, AFTERNOON_CLOCKS,
)
from research.g4_extended_target_sweep import read_frozen_csv


SCHEMA = "fno_g3_target_cutoff_backtest_v1"
BASELINE_RELATIVE = "backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli"
BASELINE_MANIFEST_SHA256 = "05ed1a712e0fc80d31bb76084ae24b502e91a9f471595374a0fc649a069ba6d0"
BASELINE_VERSION = "V13-V10-G-3-PRODUCTION-V7"
BASELINE_FINGERPRINT = "68e842bb99002c950304cc40a41aa40e93a6394b131199ade872ec5e852a281f"
TARGET_CUTOFF = .75
RESTORED_TARGETS = {"0951_LONG": .75, "1006_LONG": .56, "1016_SHORT": .68}
FILES = {"config.json", "summary.json", "daily_results.csv", "portfolio_trades.csv", "baseline_trades.csv",
    "selected_orders.csv", "disabled_setups.csv", "newly_admitted_trades.csv", "newly_rejected_trades.csv",
    "portfolio_comparison.csv", "daily_comparison.csv", "setup_comparison.csv", "guarded_1035_selection.csv", "setup_results.csv",
    "monthly_results.csv", "period_results.csv", "report.html", "enabled_two_slots_selection.csv",
    "revised_0926_selection.csv"}


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _child(root, relative):
    path = (Path(root) / relative).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        raise ValueError("Historical source escaped its result root")
    return path


def _check_files(directory, manifest, *, daily=False):
    records = manifest.get("artifacts", {}) if daily else manifest.get("files", {})
    if not records:
        raise ValueError("Historical source has no sealed artifact inventory")
    for name, record in records.items():
        relative, digest = (record["path"], record["sha256"]) if daily else (name, record)
        path = _child(directory, relative)
        if not path.is_file() or sha256(path) != digest:
            raise ValueError(f"Historical source artifact changed: {relative}")


def _checked_reference(root, record):
    directory = _child(root, record["run_dir"])
    path = directory / record.get("manifest_name", "manifest.json")
    if sha256(path) != record["manifest_sha256"]:
        raise ValueError("Historical source manifest changed")
    return directory, _read(path)


def _validate_profile():
    contract.load_contract(CONFIG_PATH)
    if (baseline_contract.STRATEGY_VERSION != BASELINE_VERSION
            or baseline_contract.strategy_fingerprint() != BASELINE_FINGERPRINT
            or production.confirmation_volume_minimum("LONG", setup_id="0941_LONG") != .8):
        raise ValueError("Pinned PAPER/LIVE baseline identity changed")
    allowed, removed = set(contract._ACTIVE_BY_ID), set(contract.DISABLED_IDS)
    expected = {s.setup_id for s in production.ACTIVE_SETUPS if s.target_pct > TARGET_CUTOFF} | set(RESTORED_TARGETS)
    if (allowed != expected or removed != set(production._ACTIVE_BY_ID) - allowed
            or any(contract._ACTIVE_BY_ID[key].target_pct != target for key, target in RESTORED_TARGETS.items())):
        raise ValueError("Backtest must retain targets above 0.75% and exactly the three approved exceptions")
    return allowed, removed


def _check_identity(frame, identity):
    for key, expected in (("strategy_version", identity.STRATEGY_VERSION),
                          ("strategy_fingerprint", identity.strategy_fingerprint())):
        if key not in frame or not frame[key].eq(expected).all():
            raise ValueError("Historical ledger mixes strategy identities")


def _check_order_inputs(orders):
    if any(name in orders for name in ("entry_price", "exit_price", "net_return_pct", "filled")):
        raise ValueError("Execution inputs must be pre-execution selected orders")
    if orders.signal_id.isna().any() or orders.signal_id.duplicated().any():
        raise ValueError("Selected order identities are missing or duplicated")
    for row in orders.itertuples(index=False):
        spec = production._ACTIVE_BY_ID.get(row.setup_id)
        if spec is None or float(row.native_target_pct) != spec.target_pct:
            raise ValueError("Pinned selected order target differs from its original contract")


def _baseline(root):
    directory = _child(root, BASELINE_RELATIVE)
    if sha256(directory / "manifest.json") != BASELINE_MANIFEST_SHA256:
        raise ValueError("Pinned 59-setup baseline manifest changed")
    manifest, summary = _read(directory / "manifest.json"), _read(directory / "summary.json")
    _check_files(directory, manifest)
    if (summary.get("strategy_version") != BASELINE_VERSION
            or summary.get("strategy_fingerprint") != BASELINE_FINGERPRINT
            or summary.get("sessions") != 50 or summary.get("active_setup_count") != 59
            or summary.get("execution_authority") is not False
            or _read(directory / "config.json") != baseline_contract.strategy_payload()):
        raise ValueError("Pinned historical baseline is not the complete original 59-setup cohort")
    base, base_manifest = _checked_reference(root, summary["base_source"])
    _check_files(base, base_manifest)
    if _read(base / "config.json") != baseline_contract.strategy_payload():
        raise ValueError("Pinned historical base contract changed")
    base_orders = read_frozen_csv(base / "selected_orders.csv")
    _check_order_inputs(base_orders)
    _check_identity(base_orders, baseline_contract)
    members = []
    for record in summary["daily_sources"]:
        member, payload = _checked_reference(root, record)
        _check_files(member, payload, daily=True)
        if (payload.get("strategy_version") != BASELINE_VERSION
                or payload.get("strategy_fingerprint") != BASELINE_FINGERPRINT
                or payload.get("execution_authority") is not False or payload.get("complete") is not True):
            raise ValueError("Pinned daily source is not the original complete production revision")
        orders = read_frozen_csv(member / payload["artifacts"]["selected_orders"]["path"])
        _check_order_inputs(orders)
        _check_identity(orders, baseline_contract)
        members.append((member, payload, orders))
    saved = read_frozen_csv(directory / "portfolio_trades.csv")
    _check_identity(saved, baseline_contract)
    orders = pd.concat([base_orders] + [m[2] for m in members], ignore_index=True, sort=False)
    _check_order_inputs(orders)
    if set(orders.signal_id) != set(saved.signal_id) or orders.sid.duplicated().any():
        raise ValueError("Pinned baseline order and ledger identities disagree")
    days = [date.fromisoformat(d) for d in summary["days"]]
    _compare_stats(summary["current_g3"], _stats(saved, days))
    proof = dict(run_dir=BASELINE_RELATIVE, manifest_name="manifest.json",
        manifest_sha256=BASELINE_MANIFEST_SHA256, historical_base=summary["base_source"],
        daily_sources=summary["daily_sources"])
    return orders, saved, days, members, proof


def _historical_paths(expected_days):
    _, late_paths, days, late_proof, snapshots = load_late_source(verify_snapshots=True)
    if days != expected_days:
        raise ValueError("Original historical source calendar changed")
    _, paths, base_proof = _g3_baseline(days, snapshots, old_config(DEFAULT_CONFIG))
    opening = read_frozen_csv(G3_RUN / "added_0920_orders.csv")
    opening_paths, opening_proof = new_execution_paths(opening, Path(RUNTIME_ROOT))
    groups = [late_paths, opening_paths]
    proof = dict(late=late_proof, opening=opening_proof, original=base_proof)
    for label, cache, clocks, sid_base in (
            ("selected_ten", DEFAULT_CACHE, CLOCKS, 5_000_000),
            ("midday_twelve", MIDDAY_CACHE, MIDDAY_CLOCKS, 6_000_000),
            ("afternoon_fifteen", AFTERNOON_CACHE, AFTERNOON_CLOCKS, 7_000_000)):
        _, group, group_days, source_proof, _ = load_late_source(
            cache, clocks=clocks, sid_base=sid_base, verify_snapshots=True)
        if group_days != expected_days:
            raise ValueError("Historical minute source calendars differ")
        groups.append(group)
        proof[label] = source_proof
    for group in groups:
        if set(group) & set(paths):
            raise ValueError("Historical minute-path identifiers overlap")
        paths.update(group)
    return paths, proof


def _daily_paths(directory, payload, orders):
    provenance = _read(directory / payload["artifacts"]["source_manifest"]["path"])
    if (provenance.get("source_kind") != "IMMUTABLE_RAW_MARKET_SNAPSHOT"
            or provenance.get("completed_trade_outputs_consumed") is not False):
        raise ValueError("Daily source lacks original raw snapshot provenance")
    manifest_path = Path(provenance["snapshot_manifest"]).resolve()
    if not manifest_path.is_relative_to(directory.resolve()) or sha256(manifest_path) != provenance["snapshot_manifest_sha256"]:
        raise ValueError("Daily immutable snapshot manifest changed or escaped its run")
    manifest = _read(manifest_path)
    source._verify_input_snapshot(manifest_path.parent, manifest)
    day = date.fromisoformat(payload["session_date"])
    minutes, problems, evidence = {}, [], []
    for symbol in sorted(set(orders.tradingsymbol)):
        path = manifest_path.parent / "equity_1m" / f"{symbol}_stocks_indicators_1min.parquet"
        minutes[symbol] = source._load_minute(path, day, problems, symbol)
        evidence.append(dict(path=str(path), sha256=sha256(path)))
    if problems:
        raise ValueError("Pinned daily selected execution minutes are invalid")
    stamps = orders.copy()
    stamps["confirmation_ts"] = pd.to_datetime(stamps.confirmation_ts, utc=True).dt.tz_convert(production.IST)
    paths = source.selected_paths(stamps, minutes, day)
    return paths, dict(run_dir=str(directory), snapshot_manifest=str(manifest_path),
        snapshot_manifest_sha256=sha256(manifest_path), selected_minute_files=evidence)


def _revised_0941_orders(original_orders, days, members):
    """Reselect 09:41 LONG from sealed candidate evidence under the 0.80x gate."""
    from fno_v13_v10_g_3_backtest import OCTOBER12_SOURCE
    from research import g3_0950_long_backtest as inputs

    signals, historical_days, sources, proof = inputs.load_sources(
        inputs.DEFAULT_HISTORY_PROVENANCE, inputs.DEFAULT_RECENT_PROVENANCE,
        OCTOBER12_SOURCE)
    if historical_days != days[:49]:
        raise ValueError("Revised 09:41 candidate calendar differs from pinned baseline")
    pool = signals.loc[signals.signal_end.eq("09:40") & signals.side.eq("LONG")].copy()
    member_map = {}
    for directory, payload, _ in members:
        candidates = read_frozen_csv(directory / payload["artifacts"]["candidate_signals"]["path"])
        candidates = inputs.normalize_signals(candidates)
        candidates = candidates.loc[candidates.signal_end.eq("09:40") & candidates.side.eq("LONG")]
        pool = pd.concat([pool, candidates], ignore_index=True, sort=False)
        member_map[payload["session_date"]] = (directory, payload)
    if pool.duplicated(["day", "tradingsymbol"]).any():
        raise ValueError("Duplicate 09:41 LONG candidate in sealed inputs")
    pool["input_sid"] = pool.sid
    pool["sid"] = np.arange(90_000_000, 90_000_000 + len(pool))
    original = baseline_contract.selection_audit(pool)
    revised = production.selection_audit(pool)
    old_keys = set(zip(original.loc[original.v9_selected, "day"].astype(str),
                       original.loc[original.v9_selected, "tradingsymbol"]))
    saved_slot = original_orders.loc[original_orders.setup_id.eq("0941_LONG")]
    saved_keys = set(zip(saved_slot.day.astype(str), saved_slot.tradingsymbol))
    if old_keys != saved_keys:
        raise ValueError("Original 09:41 selection does not reproduce pinned orders")
    selected = revised.loc[revised.v9_selected].copy()
    selected_keys = set(zip(selected.day.astype(str), selected.tradingsymbol))
    if not old_keys.issubset(selected_keys):
        raise ValueError("0.80x gate displaced an original 09:41 selection")
    added = selected.loc[~selected.apply(
        lambda row: (str(row.day), row.tradingsymbol) in old_keys, axis=1)].copy()
    if added.empty:
        return added, {}, dict(candidate_source=proof, newly_selected=[])
    native = added.copy()
    native["sid"] = native.input_sid.astype(int)
    hist_mask = pd.to_datetime(native.day).dt.date.isin(days[:49])
    historical_paths = inputs.selected_paths(native.loc[hist_mask], sources)
    paths = {int(row.sid): historical_paths[int(row.input_sid)]
             for row in added.loc[hist_mask].itertuples()}
    daily_proof = {}
    for day_text, (directory, payload) in member_map.items():
        daily_mask = pd.to_datetime(native.day).dt.strftime("%Y-%m-%d").eq(day_text)
        if not daily_mask.any():
            continue
        daily_paths, evidence = _daily_paths(directory, payload, native.loc[daily_mask])
        daily_proof[day_text] = evidence
        for row in added.loc[daily_mask].itertuples():
            paths[int(row.sid)] = daily_paths[int(row.input_sid)]
    if set(paths) != set(added.sid.astype(int)):
        raise ValueError("Revised 09:41 order paths are incomplete")
    return added, paths, dict(candidate_source=proof, daily_sources=daily_proof,
                              newly_selected=[dict(day=str(row.day), symbol=row.tradingsymbol,
                                                   volume_ratio=float(row.v9_1m_volume_ratio))
                                              for row in added.itertuples()])


def _selected_path_subset(paths, orders):
    """Keep only this cohort's paths; candidate IDs are local to their source.

    An unused historical candidate can share its integer SID with a selected
    order from a later daily source. It must not reserve that identifier or
    supply execution prices to that later order.
    """
    needed = set(orders.sid.astype(int))
    if len(needed) != len(orders):
        raise ValueError("Selected execution path identifiers are duplicated")
    missing = needed - set(paths)
    if missing:
        raise ValueError(f"Selected historical execution paths missing: {sorted(missing)}")
    return {sid: paths[sid] for sid in sorted(needed)}


def _simulate(orders, paths, days, identity):
    """Simulate explicitly under the supplied identity, never reuse old P&L."""
    base = simulator.g.v9.V9Config(portfolio_capital_rupees=identity.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=identity.CAPITAL_PER_ENTRY_RS, leverage_factor=identity.LEVERAGE,
        max_positions=identity.MAX_POSITIONS, cost_bps=identity.ROUND_TRIP_COST_BPS)
    selected_paths = {int(sid): paths[int(sid)] for sid in orders.sid}
    simulator.g.v9.validate_paths(orders, selected_paths)
    trades = simulator.simulate_staged(orders, selected_paths, cost_bps=base.cost_bps,
        max_entry_delay_minutes=identity.ENTRY_EXPIRY_MINUTES) if len(orders) else orders.copy()
    for name, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                          ("gross_return_pct", np.nan), ("net_return_pct", np.nan), ("cost_pct", np.nan)):
        if name not in trades:
            trades[name] = default
    trades = simulator.g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = simulator.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    ledger["strategy"] = "V13-V10-G-3"
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    ledger["strategy_version"] = identity.STRATEGY_VERSION
    ledger["strategy_fingerprint"] = identity.strategy_fingerprint()
    ledger["day"] = pd.to_datetime(ledger.day).dt.strftime("%Y-%m-%d")
    if not set(ledger.day).issubset({str(d) for d in days}):
        raise ValueError("Simulation escaped its verified session calendar")
    return ledger


def _same_native(actual, expected):
    actual = actual.copy()
    actual["signal_id"] = actual.sid.map(expected.set_index("sid").signal_id)
    same_raw(actual, expected)
    left, right = (f.set_index("sid").sort_index() for f in (actual, expected))
    for column in ("entry_ts", "exit_ts"):
        if not pd.to_datetime(left[column], utc=True).equals(pd.to_datetime(right[column], utc=True)):
            raise ValueError(f"Fresh baseline execution timestamp differs: {column}")
    for column in ("gross_return_pct", "cost_pct"):
        if not np.allclose(left[column], right[column], rtol=0, atol=1e-8, equal_nan=True):
            raise ValueError(f"Fresh native execution differs: {column}")


def _same_allocation(actual, expected):
    left, right = (f.set_index("sid").sort_index() for f in (actual, expected))
    if not left.portfolio_executed.equals(right.portfolio_executed):
        raise ValueError("Fresh baseline allocation differs from the pinned portfolio")
    for column in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
        if not np.allclose(left[column], right[column], rtol=0, atol=1e-6):
            raise ValueError("Fresh baseline portfolio accounting differs")


def _daily(ledger, days):
    rows = []
    for day in days:
        stats = _stats(ledger.loc[ledger.day.astype(str).eq(str(day))], [day])
        rows.append(dict(day=str(day), trades=stats["executed_trades"], wins=stats["wins"],
            losses=stats["losses"], net_pnl=stats["net_pnl_rs"], gross_pnl=stats["gross_pnl_rs"], cost=stats["costs_rs"]))
    result = pd.DataFrame(rows)
    result["cumulative_net_pnl"] = result.net_pnl.cumsum()
    return result


def _compare_stats(expected, actual):
    for key, value in actual.items():
        if value is None and expected[key] is None:
            continue
        if not np.isclose(value, expected[key], atol=1e-6, rtol=0):
            raise ValueError(f"Historical summary accounting differs: {key}")


def _comparison(baseline, filtered, days, allowed, *, profile=None):
    profile = profile or contract
    kept = baseline.loc[baseline.setup_id.isin(allowed)]
    rows = [dict(scope=label, **_stats(frame, days)) for label, frame in (
        ("original_59", baseline), ("retained_at_original_allocation", kept),
        (f"filtered_{len(profile.ACTIVE_SETUPS)}_reallocated", filtered))]
    setups = []
    saved_specs = {s.setup_id: s for s in (*profile.ACTIVE_SETUPS, *profile.DISABLED_SETUPS)}
    for current in production.ACTIVE_SETUPS:
        spec = saved_specs.get(current.setup_id)
        if spec is None:
            continue
        record = dict(setup_id=spec.setup_id, signal_end=spec.signal_end, side=spec.side,
            target_pct=spec.target_pct, disabled=spec.setup_id not in allowed)
        for prefix, frame in (("before", baseline), ("after", filtered)):
            record.update({prefix + "_" + k: v for k, v in _stats(frame.loc[frame.setup_id.eq(spec.setup_id)], days).items()})
        setups.append(record)
    before = baseline.set_index("signal_id")
    changed = filtered.copy()
    changed["baseline_executed"] = changed.source_signal_id.map(before.portfolio_executed).fillna(False).astype(bool)
    admitted = changed.loc[changed.portfolio_executed & ~changed.baseline_executed].copy()
    rejected = changed.loc[~changed.portfolio_executed & changed.baseline_executed].copy()
    daily = _daily(baseline, days).merge(_daily(filtered, days), on="day", suffixes=("_before", "_after"))
    daily["net_pnl_delta"] = daily.net_pnl_after - daily.net_pnl_before
    return pd.DataFrame(rows), pd.DataFrame(setups), daily, admitted, rejected


def _provenance():
    project = Path(__file__).resolve().parents[1]
    modules = (contract, production, replay, source, simulator, simulator.g, simulator.g.v9,
        simulator.g.v9.v5, simulator.g.v9.v6, simulator.g.r, production.late_setups,
        production.selected_setups, production.midday_setups, production.afternoon_setups)
    paths = {Path(__file__).resolve(), Path(CONFIG_PATH).resolve(), *(Path(m.__file__).resolve() for m in modules)}
    paths.update(project / name for name in ("research/g3_backtest_profile.py", "research/g3_backtest_report.py",
        "research/g3_1035_extension_backtest.py", "g3_one_minute_extension.py",
        "research/g3_two_slot_backtest.py", "research/g3_0920_strategy.py", "research/g3_0920_parameter_search.py",
        "research/g3_0920_history.py", "research/g3_0920_feature_export.py",
        "research/g3_0920_backtest.py", "research/g3_g4_late_combined_backtest.py", "research/g3_0950_long_backtest.py",
        "research/g4_late_slot_source.py", "research/g4_late_slot_backtest.py", "research/g4_extended_slot_backtest.py",
        "research/g3_0926_volume_backtest.py", "research/g4_g3_mirror_backtest.py",
        "research/g4_extended_target_sweep.py", "backtesting_result_v13_v10_g_3_daily.py", "fno_v13_v10_g_3_opening_inputs.py"))
    return {str(path): sha256(path) for path in sorted(paths)}


def build(root, output, through, *, base=None, roots=None):
    """Fresh historical replay plus any same-configuration daily extensions."""
    root, output = Path(root).resolve(), Path(output).resolve()
    replay.require_closed_session(through)
    if output.exists() or not output.is_relative_to(root):
        raise ValueError("Target-cutoff replay requires a new directory inside the G3 result root")
    if base is not None and Path(base).resolve() != _child(root, BASELINE_RELATIVE):
        raise ValueError("The target-cutoff replay uses its pinned 59-setup baseline only")
    allowed, removed = _validate_profile()
    code_proof = _provenance()
    original_orders, saved, days, members, baseline_proof = _baseline(root)
    if through < days[-1]:
        raise ValueError("Requested cutoff precedes the pinned complete baseline")
    paths, historical_proof = _historical_paths(days[:49])
    historical_orders = original_orders.loc[pd.to_datetime(original_orders.day).dt.date.isin(days[:49])]
    candidate_path_count = len(paths)
    paths = _selected_path_subset(paths, historical_orders)
    historical_proof["execution_path_selection"] = dict(candidate_paths=candidate_path_count,
        selected_paths=len(paths), unused_candidate_paths_omitted=candidate_path_count - len(paths))
    daily_proofs = []
    for member, payload, member_orders in members:
        group, proof = _daily_paths(member, payload, member_orders)
        if set(group) & set(paths):
            raise ValueError("Original daily path identifiers overlap historical paths")
        paths.update(group)
        daily_proofs.append(proof)
    baseline = _simulate(original_orders, paths, days, baseline_contract)
    _same_native(baseline, saved)
    _same_allocation(baseline, saved)
    print(f"Verified fresh59 baseline: {len(baseline)} selected orders; all native and capital outcomes unchanged", flush=True)
    from research.g3_0926_volume_backtest import reselect as reselect_0926
    revised_0926_orders, revised_0926_paths, volume_proof, volume_audit = reselect_0926(original_orders, days)
    for sid, path in revised_0926_paths.items():
        if sid in paths:
            original_path = paths[sid]
            if set(path) != set(original_path) or any(not np.array_equal(path[key], original_path[key]) for key in path):
                raise ValueError("Pinned 09:26 execution path differs from candidate evidence")
        else:
            paths[sid] = path
    historical_proof["revised_0926_long_volume"] = volume_proof
    revised_orders, revised_paths, revision_proof = _revised_0941_orders(original_orders, days, members)
    if set(revised_paths) & set(paths):
        raise ValueError("Revised 09:41 paths collide with pinned paths")
    paths.update(revised_paths)
    historical_proof["revised_0941_long"] = revision_proof
    if original_orders.setup_id.isin({"1036_LONG", "1036_SHORT"}).any():
        from research.g3_1035_extension_backtest import reselect as reselect_1035
        guarded_orders, guarded_paths, guard_proof, guard_audit = reselect_1035(original_orders, days, members)
    else:
        guarded_orders, guarded_paths = original_orders.iloc[:0].copy(), {}
        guard_proof = dict(original_selected=0, revised_selected=0)
        guard_audit = pd.DataFrame(columns=["day", "setup_id", "tradingsymbol", "decision", "v9_1m_extension_atr"])
    if set(guarded_paths) & set(paths):
        raise ValueError("10:35 extension paths collide with pinned paths")
    paths.update(guarded_paths)
    historical_proof["revised_1035_extension"] = guard_proof
    from research.g3_two_slot_backtest import reselect as select_two_slots
    new_orders, new_paths, new_slot_proof, new_slot_audit = select_two_slots(original_orders, days, members)
    if set(new_paths) & set(paths):
        raise ValueError("New-slot paths collide with existing execution paths")
    paths.update(new_paths)
    historical_proof["enabled_two_slots"] = new_slot_proof
    orders = original_orders.loc[original_orders.setup_id.isin(allowed)
                                 & ~original_orders.setup_id.isin({"0926_LONG", "1036_LONG", "1036_SHORT"})].copy()
    orders = pd.concat([orders, revised_0926_orders, revised_orders, guarded_orders, new_orders], ignore_index=True, sort=False)
    orders["source_sid"] = orders.sid
    source_ids = orders.sid.map(baseline.set_index("sid").signal_id)
    orders["source_signal_id"] = (source_ids.combine_first(orders.source_signal_id).combine_first(orders.signal_id)
                                  if "source_signal_id" in orders else source_ids.combine_first(orders.signal_id))
    for index, row in orders.iterrows():
        spec = contract._ACTIVE_BY_ID[row.setup_id]
        orders.loc[index, "signal_id"] = contract.canonical_signal_id(contract.STRATEGY_VERSION,
            date.fromisoformat(str(row.day)), spec.signal_end, spec.confirmation_end, spec.side, row.tradingsymbol)
    orders["strategy_version"] = contract.STRATEGY_VERSION
    orders["strategy_fingerprint"] = contract.strategy_fingerprint()
    filtered = _simulate(orders, paths, days, contract)
    unchanged_sids = set(filtered.sid) & set(original_orders.sid)
    _same_native(filtered.loc[filtered.sid.isin(unchanged_sids)],
                 saved.loc[saved.sid.isin(unchanged_sids)])
    portfolio, setups, daily_compare, admitted, rejected = _comparison(baseline, filtered, days, allowed)
    baseline_days = list(days)
    frames, order_frames, extension_records = [filtered], [orders], []
    roots = roots or replay.DataRoots()
    available = sorted(path.stem.removeprefix("near_month_") for path in roots.universe.glob("near_month_*.parquet")
                       if str(days[-1]) < path.stem.removeprefix("near_month_") <= str(through))
    if through > days[-1] and (not available or available[-1] != str(through)):
        raise ValueError("Requested cutoff has no dated market universe; history was not advanced")
    if available:
        from backtesting_result_v13_v10_g_3_daily import _verified_daily, _run_id
        occupied = set(orders.sid.astype(int))
        for day_text in available:
            day = date.fromisoformat(day_text)
            member = _verified_daily(root, day)
            if member is None:
                directory = root / "production/runs" / day_text / _run_id()
                payload = replay.replay_day(day, directory, roots=roots)
                member = directory, payload
            directory, payload = member
            if (payload.get("complete") is not True or payload.get("strategy_fingerprint") != contract.strategy_fingerprint()):
                raise ValueError("Incomplete or wrong-revision daily extension")
            _check_files(directory, payload, daily=True)
            extra = read_frozen_csv(directory / payload["artifacts"]["selected_orders"]["path"])
            if not set(extra.setup_id).issubset(allowed):
                raise ValueError("Daily extension includes a disabled setup")
            _check_identity(extra, contract)
            extra_paths, proof = _daily_paths(directory, payload, extra)
            extra["source_sid"], extra["source_signal_id"] = extra.sid, extra.signal_id
            for index in extra.index:
                sid = int(extra.loc[index, "sid"])
                if sid in occupied:
                    replacement = 90_000_000 + len(occupied)
                    while replacement in occupied:
                        replacement += 1
                    extra.loc[index, "sid"] = replacement
                    extra_paths[replacement] = extra_paths.pop(sid)
                occupied.add(int(extra.loc[index, "sid"]))
            frames.append(_simulate(extra, extra_paths, [day], contract))
            order_frames.append(extra)
            daily_proofs.append(proof)
            extension_records.append(dict(run_dir=directory.relative_to(root).as_posix(),
                manifest_name="run_manifest.json", manifest_sha256=sha256(directory / "run_manifest.json")))
            days.append(day)
    ledger = pd.concat(frames, ignore_index=True, sort=False)
    all_orders = pd.concat(order_frames, ignore_index=True, sort=False)
    daily = _daily(ledger, days)
    stats = _stats(ledger, days)
    disabled = pd.DataFrame([dict(setup_id=s.setup_id, signal_end=s.signal_end, side=s.side,
        target_pct=s.target_pct, reason="TARGET_LESS_THAN_OR_EQUAL_TO_0.75_PERCENT")
        for s in production.ACTIVE_SETUPS if s.setup_id in removed])
    summary = dict(schema_version=SCHEMA, scope="BACKTEST_ONLY", execution_authority=False,
        strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
        active_setup_count=len(allowed), disabled_setup_count=len(removed), target_cutoff_pct=TARGET_CUTOFF,
        restored_setup_ids=sorted(RESTORED_TARGETS),
        created_at_ist=datetime.now(contract.IST).isoformat(), first_date=str(days[0]), last_date=str(days[-1]),
        sessions=len(days), days=[str(d) for d in days], current_g3=stats,
        comparison_first_date=str(baseline_days[0]), comparison_last_date=str(baseline_days[-1]),
        comparison_sessions=len(baseline_days),
        baseline_g3_v7=_stats(baseline, baseline_days),
        retained_at_original_allocation=_stats(baseline.loc[baseline.setup_id.isin(allowed)], baseline_days),
        filtered_reallocated_original50=_stats(filtered, baseline_days),
        net_pnl_delta_rs=float(filtered.portfolio_net_profit_rupees.sum() - baseline.portfolio_net_profit_rupees.sum()),
        newly_admitted_fills=len(admitted), newly_rejected_fills=len(rejected),
        capital_reallocation_net_pnl_rs=float(filtered.portfolio_net_profit_rupees.sum()
            - baseline.loc[baseline.setup_id.isin(allowed), "portfolio_net_profit_rupees"].sum()),
        normal_minute_replay_verified=True, baseline_native_replay_verified=True,
        baseline_allocation_verified=True, retained_native_outcomes_unchanged=True,
        paper_live_configuration_changed=True, historical_targets_selected_after_inspection=True,
        independent_holdout_qualification=False, baseline_source=baseline_proof,
        source_evidence=historical_proof, daily_path_evidence=daily_proofs, daily_sources=extension_records,
        guarded_1035=dict(maximum_extension_atr=3.0,
                          original_selected=guard_proof["original_selected"],
                          revised_selected=guard_proof["revised_selected"],
                          source=guard_proof),
        enabled_two_slots=new_slot_proof, revised_0926_long_volume=volume_proof,
        coverage=dict(excluded_sessions=[dict(day="2026-10-01", reason="Incomplete immutable historical input")],
            history_scope="FULL_CURRENT_STRATEGY_HISTORY_WITH_DAILY_EXTENSIONS",
            opening_0920_short=new_slot_proof["opening_coverage"],
            selection_method="PINNED_PRE_EXECUTION_ORDERS_PLUS_SEALED_0926_0941_1035_RESELECTION_AND_FULL_NEW_SLOT_CANDIDATE_SELECTION_BEFORE_FRESH_MINUTE_REPLAY_AND_ALLOCATION"))
    summary["comparison_delta"] = {key: summary["filtered_reallocated_original50"][key]
        - summary["baseline_g3_v7"][key] for key in ("selected_orders", "native_fills", "executed_trades",
            "wins", "losses", "win_rate_pct", "gross_pnl_rs", "costs_rs", "net_pnl_rs", "max_drawdown_rs")}
    from research.g3_backtest_report import write
    if _provenance() != code_proof:
        raise ValueError("Backtest execution/source code changed during replay")
    output.mkdir(parents=True)
    for name, frame in dict(portfolio_trades=ledger, baseline_trades=baseline, selected_orders=all_orders,
            disabled_setups=disabled, newly_admitted_trades=admitted, newly_rejected_trades=rejected,
            daily_results=daily, guarded_1035_selection=guard_audit, enabled_two_slots_selection=new_slot_audit,
            revised_0926_selection=volume_audit,
            portfolio_comparison=portfolio, daily_comparison=daily_compare,
            setup_comparison=setups).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    replay.atomic_json(output / "config.json", contract.strategy_payload())
    replay.atomic_json(output / "summary.json", summary)
    write(output, summary, ledger, daily)
    if _provenance() != code_proof:
        raise ValueError("Backtest execution/source code changed during artifact generation")
    replay.atomic_json(output / "manifest.json", dict(schema_version=SCHEMA, execution_authority=False,
        files={name: sha256(output / name) for name in sorted(FILES)}, code_sha256=code_proof))
    verified_package(output, root)
    return summary


def verified_package(directory, root):
    directory, root = Path(directory).resolve(), Path(root).resolve()
    manifest = _read(directory / "manifest.json")
    manifest_files = set(manifest.get("files", {}))
    legacy_files = FILES - {"revised_0926_selection.csv"}
    if (manifest.get("schema_version") != SCHEMA or manifest.get("execution_authority") is not False
            or manifest_files not in (FILES, legacy_files, legacy_files - {"enabled_two_slots_selection.csv"},
                legacy_files - {"guarded_1035_selection.csv", "enabled_two_slots_selection.csv"})):
        raise ValueError("Invalid target-cutoff historical manifest")
    _check_files(directory, manifest)
    summary = _read(directory / "summary.json")
    profile = contract.contract_for_identity(summary.get("strategy_version"), summary.get("strategy_fingerprint"))
    if profile is None:
        raise ValueError("Unknown target-cutoff historical strategy identity")
    if profile is contract and "guarded_1035_selection.csv" not in manifest_files:
        raise ValueError("Current G3 history lacks the 10:35 extension decision audit")
    if profile is contract and "revised_0926_selection.csv" not in manifest_files:
        raise ValueError("Current G3 history lacks the 09:26 volume selection audit")
    if {"0921_SHORT", "1011_LONG"}.issubset(profile._ACTIVE_BY_ID) and "enabled_two_slots_selection.csv" not in manifest_files:
        raise ValueError("Current G3 history lacks both new-slot candidate selections")
    if (_read(directory / "config.json") != profile.strategy_payload()
            or summary.get("strategy_version") != profile.STRATEGY_VERSION
            or summary.get("strategy_fingerprint") != profile.strategy_fingerprint()
            or summary.get("active_setup_count") != len(profile.ACTIVE_SETUPS)
            or summary.get("disabled_setup_count") != len(profile.DISABLED_IDS)
            or set(summary.get("restored_setup_ids", [])) != {s.setup_id for s in profile.ACTIVE_SETUPS if s.target_pct <= TARGET_CUTOFF}
            or summary.get("execution_authority") is not False
            or summary.get("normal_minute_replay_verified") is not True
            or summary.get("baseline_native_replay_verified") is not True
            or summary.get("baseline_allocation_verified") is not True):
        raise ValueError("Target-cutoff package differs from its fixed backtest contract")
    days = [date.fromisoformat(d) for d in summary["days"]]
    if (days != sorted(set(days)) or len(days) != summary["sessions"]
            or str(days[0]) != summary["first_date"] or str(days[-1]) != summary["last_date"]):
        raise ValueError("Target-cutoff session calendar is invalid")
    _, saved_baseline, baseline_days, _, baseline_proof = _baseline(root)
    if (summary.get("baseline_source") != baseline_proof
            or summary.get("comparison_first_date") != str(baseline_days[0])
            or summary.get("comparison_last_date") != str(baseline_days[-1])
            or summary.get("comparison_sessions") != len(baseline_days)):
        raise ValueError("Historical comparison source or calendar changed")
    for record in summary["daily_sources"]:
        member, payload = _checked_reference(root, record)
        _check_files(member, payload, daily=True)
        if payload.get("strategy_fingerprint") != profile.strategy_fingerprint() or payload.get("complete") is not True:
            raise ValueError("Target-cutoff daily source identity changed")
    ledger = read_frozen_csv(directory / "portfolio_trades.csv")
    orders = read_frozen_csv(directory / "selected_orders.csv")
    _check_identity(ledger, profile)
    _check_identity(orders, profile)
    expected_targets = ledger.setup_id.map({s.setup_id: s.target_pct for s in profile.ACTIVE_SETUPS})
    if (ledger.signal_id.duplicated().any() or ledger.sid.duplicated().any()
            or set(ledger.signal_id) != set(orders.signal_id)
            or not set(ledger.setup_id).issubset(profile._ACTIVE_BY_ID)
            or not np.allclose(pd.to_numeric(ledger.native_target_pct), expected_targets, atol=1e-12, rtol=0)
            or not set(ledger.day.astype(str)).issubset(summary["days"])):
        raise ValueError("Target-cutoff ledger contains disabled, duplicate, or invalid orders")
    if not np.allclose(ledger.portfolio_gross_profit_rupees - ledger.portfolio_cost_rupees,
                       ledger.portfolio_net_profit_rupees, atol=1e-6, rtol=0):
        raise ValueError("Target-cutoff gross/cost/net accounting mismatch")
    _compare_stats(summary["current_g3"], _stats(ledger, days))
    daily = read_frozen_csv(directory / "daily_results.csv")
    pd.testing.assert_frame_equal(daily, _daily(ledger, days), check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    disabled = read_frozen_csv(directory / "disabled_setups.csv")
    expected_removed = set(profile.DISABLED_IDS)
    if set(disabled.setup_id) != expected_removed:
        raise ValueError("Disabled setup inventory differs from the 0.75% cutoff")
    baseline = read_frozen_csv(directory / "baseline_trades.csv")
    _check_identity(baseline, baseline_contract)
    _same_native(baseline, saved_baseline)
    _same_allocation(baseline, saved_baseline)
    comparison_ledger = ledger.loc[ledger.day.astype(str).isin({str(d) for d in baseline_days})].copy()
    if "enabled_two_slots_selection.csv" in manifest_files:
        _verify_two_slot_audit(directory, summary, comparison_ledger)
    if "revised_0926_selection.csv" in manifest_files:
        _verify_0926_audit(directory, summary, comparison_ledger)
    allowed = set(profile._ACTIVE_BY_ID)
    retained_baseline = baseline.loc[baseline.setup_id.isin(allowed)]
    prior = comparison_ledger.loc[comparison_ledger.sid.isin(retained_baseline.sid)]
    _same_native(prior, retained_baseline.loc[retained_baseline.sid.isin(prior.sid)])
    expected_source_ids = prior.sid.map(retained_baseline.set_index("sid").signal_id)
    if not prior.source_signal_id.equals(expected_source_ids):
        raise ValueError("Retained orders lost their original production signal identities")
    expected_comparisons = _comparison(baseline, comparison_ledger, baseline_days, allowed, profile=profile)
    for name, expected in zip(("portfolio_comparison", "setup_comparison", "daily_comparison",
                              "newly_admitted_trades", "newly_rejected_trades"), expected_comparisons):
        actual = read_frozen_csv(directory / (name + ".csv"))
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True),
            check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    _compare_stats(summary["baseline_g3_v7"], _stats(baseline, baseline_days))
    _compare_stats(summary["retained_at_original_allocation"], _stats(retained_baseline, baseline_days))
    _compare_stats(summary["filtered_reallocated_original50"], _stats(comparison_ledger, baseline_days))
    delta = {key: summary["filtered_reallocated_original50"][key] - summary["baseline_g3_v7"][key]
             for key in summary["comparison_delta"]}
    _compare_stats(summary["comparison_delta"], delta)
    admitted, rejected = expected_comparisons[-2:]
    allocation_gain = float(comparison_ledger.portfolio_net_profit_rupees.sum() - retained_baseline.portfolio_net_profit_rupees.sum())
    if (summary["newly_admitted_fills"] != len(admitted) or summary["newly_rejected_fills"] != len(rejected)
            or not np.isclose(summary["capital_reallocation_net_pnl_rs"], allocation_gain, atol=1e-6, rtol=0)
            or not np.isclose(summary["net_pnl_delta_rs"], delta["net_pnl_rs"], atol=1e-6, rtol=0)):
        raise ValueError("Capital reallocation comparison accounting differs")
    return summary, manifest


def _verify_0926_audit(directory, summary, comparison_ledger):
    """Bind the 1.2x pre-quota selection to its sealed candidates and trade ledger."""
    from research import g3_0926_volume_backtest as volume
    source_dir = runtime_dir("backtesting_result_v13_v10_g_4") / volume.SOURCE_RELATIVE
    if (sha256(source_dir / "manifest.json") != volume.MANIFEST_SHA256
            or sha256(source_dir / "candidate_extension_audit.csv") != volume.POOL_SHA256):
        raise ValueError("09:26 volume source evidence changed")
    pool = read_frozen_csv(source_dir / "candidate_extension_audit.csv")
    expected = production.selection_audit(pool.loc[pool.setup_id.eq(volume.SETUP_ID)
        & pool.day.astype(str).isin(set(summary["days"][:summary["comparison_sessions"]]))].copy())
    audit = read_frozen_csv(Path(directory) / "revised_0926_selection.csv")
    if (len(expected) != len(audit) or not expected.sid.astype(int).equals(audit.sid.astype(int))
            or not expected.v9_selected.astype(bool).equals(audit.v9_selected.astype(bool))):
        raise ValueError("09:26 volume audit differs from fresh contract selection")
    selected = audit.loc[audit.v9_selected.eq(True)]
    actual = comparison_ledger.loc[comparison_ledger.setup_id.eq(volume.SETUP_ID)]
    def keys(frame):
        return set(zip(frame.day.astype(str).str[:10], frame.tradingsymbol, frame.sid.astype(int)))
    proof = summary.get("revised_0926_long_volume", {})
    if (keys(selected) != keys(actual) or proof.get("candidate_rows") != len(audit)
            or proof.get("revised_selected") != len(selected)
            or proof.get("threshold_5m_volume_ratio") != 1.2
            or proof.get("outcomes_used_for_selection") is not False
            or proof.get("source_manifest_sha256") != volume.MANIFEST_SHA256
            or proof.get("source_pool_sha256") != volume.POOL_SHA256):
        raise ValueError("09:26 volume selections differ from historical ledger or sealed source")


def _verify_two_slot_audit(directory, summary, comparison_ledger):
    """Bind the new-slot selection audit to the freshly simulated historical cohort."""
    from research.g3_two_slot_backtest import SETUP_IDS
    audit = read_frozen_csv(Path(directory) / "enabled_two_slots_selection.csv")
    proof = summary.get("enabled_two_slots", {})
    if (set(proof.get("setup_ids", [])) != SETUP_IDS
            or proof.get("outcomes_used_for_selection") is not False
            or proof.get("candidate_rows") != len(audit)
            or audit.sid.duplicated().any() or set(audit.setup_id) - SETUP_IDS):
        raise ValueError("New-slot candidate audit provenance is invalid")
    selected = audit.loc[audit.v9_selected.eq(True)]
    actual = comparison_ledger.loc[comparison_ledger.setup_id.isin(SETUP_IDS)]
    def keys(frame):
        return set(zip(frame.day.astype(str), frame.setup_id, frame.tradingsymbol, frame.sid.astype(int)))
    if keys(selected) != keys(actual) or proof.get("selected_orders") != len(selected):
        raise ValueError("New-slot selections differ from the historical trade ledger")
    expected_counts = {setup_id: int(selected.setup_id.eq(setup_id).sum()) for setup_id in sorted(SETUP_IDS)}
    if proof.get("selected_by_setup") != expected_counts:
        raise ValueError("New-slot per-setup selection counts differ")
    if not np.allclose(pd.to_numeric(actual.native_target_pct), 1.5, atol=1e-12, rtol=0):
        raise ValueError("New-slot historical targets must be 1.50 percent")


def publish(root, directory):
    root, directory = Path(root).resolve(), Path(directory).resolve()
    summary, _ = verified_package(directory, root)
    if summary["strategy_fingerprint"] != contract.strategy_fingerprint():
        raise ValueError("An archived backtest cannot replace the current strategy pointer")
    relative = directory.relative_to(root)
    if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4 or relative.parts[2] != summary["last_date"]:
        raise ValueError("Target-cutoff history is outside its canonical destination")
    current = root / "latest/current_backtest.json"
    if current.exists():
        prior = _read(current)
        if prior.get("strategy_fingerprint") == summary["strategy_fingerprint"] and prior.get("session_date", "") > summary["last_date"]:
            raise ValueError("An older history cannot replace the latest backtest")
        if prior.get("strategy_fingerprint") != summary["strategy_fingerprint"]:
            replay.atomic_json(root / "latest/archived" / prior["strategy_version"] / "current_backtest.json", prior)
    pointer = dict(schema_version=SCHEMA, role="backtest_history", execution_authority=False,
        run_dir=relative.as_posix(), manifest_sha256=sha256(directory / "manifest.json"),
        strategy_version=summary["strategy_version"], strategy_fingerprint=summary["strategy_fingerprint"],
        session_date=summary["last_date"], created_at_ist=summary["created_at_ist"])
    replay.atomic_json(current, pointer)
    return pointer
