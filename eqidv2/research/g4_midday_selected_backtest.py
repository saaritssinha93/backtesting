"""G4: preserve the existing 37 setups and add exactly twelve selected midday setups.

Rebuild selected entries and replay all 49 setups against verified minute paths.
Frozen targets/filters come from the preceding historical target study.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from research import g3_v2_contract as contract
from research.g3_0920_backtest import allocate
from research.g3_g4_late_combined_backtest import checked_allocate, same_raw
from research.g3_g4_late_target_sweep import load_paths, replay_target
from research.g4_extended_slot_backtest import (
    ROOT, BACKTEST_ROOT, MIDDAY_CLOCKS, MIDDAY_CACHE, MIDDAY_VERSION as ENTRY_VERSION,
    portfolio_changed, screen_source_liquidity,
)
from research.g4_extended_target_sweep import MIDDAY_VERSION as TARGET_VERSION, read_frozen_csv
from research.g4_late_slot_backtest import (
    configuration as original_configuration, DEFAULT_CONFIG as ORIGINAL_CONFIG, _g3_baseline,
)
from research.g4_late_slot_source import load_late_source, sha256
from research.g4_selected_backtest import (
    VERSION as BASELINE_VERSION, SELECTED_TARGETS as MORNING_TARGETS,
    selected_setups, assert_previous_outcomes, metrics, write_report,
    verify_reference as _verify_reference,
)

VERSION = "V13-V10-G-4-SELECTED-MIDDAY-TWELVE-V5"
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json"
SELECTED_TARGETS = {
    "1136_LONG": 7.15, "1136_SHORT": .87, "1141_LONG": 4.03, "1141_SHORT": 1.19,
    "1146_SHORT": 4.95, "1206_LONG": 1.40, "1211_SHORT": .79,
    "1216_LONG": 1.24, "1216_SHORT": 3.49, "1226_SHORT": 2.13,
    "1231_LONG": 2.53, "1231_SHORT": 1.85,
}
THRESHOLDS = {"price_change_pct", "oi_change_pct", "five_minute_volume_ratio_min",
              "one_minute_body_ratio_min", "one_minute_volume_ratio_min", "one_minute_wick_ratio_max"}


def configuration(path=DEFAULT_CONFIG):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    required = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        preserve_existing_g4_setups=True, allow_unlisted_additions=False,
        initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
        confirmation_offset_minutes=1, entry_expiry_minutes=10, square_off="15:15",
        round_trip_cost_bps=5., capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.)
    if any(value.get(key) != expected for key, expected in required.items()):
        raise ValueError("Selected midday G4 authority, baseline or execution policy changed")
    setups = value.get("setups", [])
    if len(setups) != 12 or {s.get("setup_id") for s in setups} != set(SELECTED_TARGETS):
        raise ValueError("G4 midday profile must contain exactly the twelve requested additions")
    for s in setups:
        if (s.get("enabled") is not True or s["target_pct"] != SELECTED_TARGETS[s["setup_id"]]
                or s["side"] not in ("LONG", "SHORT")
                or s["setup_id"] != s["confirmation_end"].replace(":", "") + "_" + s["side"]
                or s["confirmation_end"] != (pd.Timestamp("2000-01-01 " + s["signal_end"]) + pd.Timedelta(minutes=1)).strftime("%H:%M")
                or (s["initial_stop_pct"], s["tightened_stop_pct"], s["tighten_after_minutes"], s["max_entries"], s["picker"]) != (1.25, 1., 120, 1, "max_liquidity")
                or s["stage"] not in ("strict", "relax_1m", "relax_5m")):
            raise ValueError("Selected midday setup has an incorrect clock, target, quota or stop")
        if set(s["thresholds"]) != THRESHOLDS or any(
                not np.isfinite(float(v)) or float(v) < 0 for v in s["thresholds"].values()):
            raise ValueError("Selected midday thresholds must be complete, finite and nonnegative")
    return value


def verify_reference(relative, digest, version, required_artifacts=()):
    folder, proof = _verify_reference(relative, digest, version)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if not set(required_artifacts).issubset(manifest["files"]):
        raise ValueError("Required reference artifacts are missing from the pinned manifest")
    return folder, proof


def _baseline_replay(entry_summary, days, snapshots):
    proof = entry_summary["baseline"]
    folder, proof = verify_reference(proof["run_dir"], proof["manifest_sha256"], BASELINE_VERSION,
        ("summary.json", "combined_trades.csv", "enabled_setups.csv"))
    summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    saved = read_frozen_csv(folder / "combined_trades.csv")
    _, paths, _ = _g3_baseline(days, snapshots, original_configuration(ORIGINAL_CONFIG))
    old = summary["baseline_study"]
    original_dir, _ = verify_reference(old["run_dir"], old["manifest_sha256"],
        "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1", ("g4_added_orders.csv", "summary.json"))
    old_orders = read_frozen_csv(original_dir / "g4_added_orders.csv")
    old_summary = json.loads((original_dir / "summary.json").read_text(encoding="utf-8"))
    old_paths, old_proofs = load_paths(old_orders, old_summary)
    ten = saved.loc[saved.setup_id.isin(MORNING_TARGETS)]
    ten_paths, ten_proofs = load_paths(ten, {"source_evidence": summary["source_proof"]})
    for more in (old_paths, ten_paths):
        if set(paths) & set(more):
            raise ValueError("Existing G4 minute-path identities overlap")
        paths.update(more)
    if len(saved) != 318 or saved.setup_id.nunique() != 37 or set(paths) != set(saved.sid):
        raise ValueError("Existing 37-setup G4 source inventory changed")
    raw = simulator.simulate_staged(saved, paths, cost_bps=5., max_entry_delay_minutes=10)
    same_raw(raw, saved)
    baseline = checked_allocate(raw, saved)
    return raw, baseline, proof, old_proofs + ten_proofs, read_frozen_csv(folder / "enabled_setups.csv")


def run(config_path=DEFAULT_CONFIG, output_dir=None, *, cache=MIDDAY_CACHE):
    config = configuration(config_path)
    now = datetime.now(contract.IST)
    output = Path(output_dir or BACKTEST_ROOT / "runs" / f"{now:%Y-%m-%d}" /
                  f"{now:%Y%m%dT%H%M%S%f}_selected_midday_twelve").resolve()
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Output must be new or empty within the G4 backtest result root")
    entry_dir, entry_proof = verify_reference(config["entry_source_run_relative"],
        config["entry_source_manifest_sha256"], ENTRY_VERSION,
        ("summary.json", "selected_orders.csv", "baseline_g4_trades.csv", "daily_results.csv"))
    target_dir, target_proof = verify_reference(config["target_source_run_relative"],
        config["target_source_manifest_sha256"], TARGET_VERSION,
        ("summary.json", "scenario_configs.json", "historical_profitable70_only_added_raw_trades.csv"))
    source_config = json.loads((target_dir / "scenario_configs.json").read_text(encoding="utf-8"))
    expected = {s["setup_id"]: {k: v for k, v in s.items() if not k.startswith("prior_entry_")}
                for s in source_config["scenarios"]["historical_profitable70_only"]}
    for spec in config["setups"]:
        if {k: v for k, v in spec.items() if k != "enabled"} != expected[spec["setup_id"]]:
            raise ValueError("Selected filters or targets differ from the pinned target study")
    target_summary = json.loads((target_dir / "summary.json").read_text(encoding="utf-8"))
    if target_summary["source"] != entry_proof:
        raise ValueError("Target study does not reference the configured entry study")
    candidates, paths, days, source_proof, snapshots = load_late_source(
        cache, clocks=MIDDAY_CLOCKS, sid_base=6_000_000, verify_snapshots=True)
    candidates, rejected = screen_source_liquidity(candidates)
    with selected_setups(config):
        audit = contract.selection_audit(candidates)
        orders = audit.loc[audit.v9_selected.eq(True)].copy()
    frozen_orders = read_frozen_csv(entry_dir / "selected_orders.csv")
    frozen_orders = frozen_orders.loc[frozen_orders.setup_id.isin(SELECTED_TARGETS)]
    if (set(orders.sid) != set(frozen_orders.sid) or set(orders.setup_id) != set(SELECTED_TARGETS)
            or orders.duplicated(["day", "setup_id"]).any()):
        raise ValueError("Midday entries differ from the frozen selected entry filters")
    orders["source_signal_id"] = orders.sid.map(frozen_orders.set_index("sid").signal_id)
    raw_added = replay_target(orders, {int(s): paths[int(s)] for s in orders.sid}, SELECTED_TARGETS)
    frozen_raw = read_frozen_csv(target_dir / "historical_profitable70_only_added_raw_trades.csv")
    frozen_raw = frozen_raw.loc[frozen_raw.setup_id.isin(SELECTED_TARGETS)]
    assert_previous_outcomes(raw_added, frozen_raw)
    assert_previous_outcomes(raw_added, frozen_raw, entries_only=True)
    print(f"Rebuilt exactly 12 midday additions / {len(orders)} orders; fresh replay matches frozen outcomes", flush=True)
    entry_summary = json.loads((entry_dir / "summary.json").read_text(encoding="utf-8"))
    dates = [str(d) for d in days]
    if len(dates) != 49 or dates != sorted(read_frozen_csv(entry_dir / "daily_results.csv").day.unique()):
        raise ValueError("Frozen session cohort changed")
    baseline_raw, baseline, baseline_proof, path_proofs, old_inventory = _baseline_replay(entry_summary, days, snapshots)
    same_raw(baseline, read_frozen_csv(entry_dir / "baseline_g4_trades.csv"))
    if set(baseline.sid) & set(orders.sid) or set(baseline.setup_id) & set(SELECTED_TARGETS):
        raise ValueError("Requested midday IDs overlap the existing G4 baseline")
    combined = allocate(pd.concat((baseline_raw, raw_added), ignore_index=True, sort=False))
    fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    combined["g4_profile_version"] = VERSION
    combined["g4_profile_fingerprint"] = fingerprint
    added = combined.loc[combined.sid.isin(orders.sid)].copy()
    changed = portfolio_changed(baseline, combined)
    same_raw(combined.loc[combined.sid.isin(baseline.sid)], baseline)
    scopes = dict(baseline=baseline, additions=added, combined=combined)
    daily_rows, monthly_rows, period_rows, setup_rows = [], [], [], []
    for scope, ledger in scopes.items():
        for day in dates:
            daily_rows.append(dict(day=day, scope=scope, **metrics(ledger.loc[ledger.day.eq(day)], [day])))
        for month in sorted({d[:7] for d in dates}):
            cohort = [d for d in dates if d.startswith(month)]
            monthly_rows.append(dict(month=month, scope=scope, **metrics(ledger.loc[ledger.day.isin(cohort)], cohort)))
        for period, cohort in (("training_38", dates[:38]), ("later_11", dates[38:]), ("all_49", dates)):
            period_rows.append(dict(period=period, scope=scope, **metrics(ledger.loc[ledger.day.isin(cohort)], cohort)))
    for setup_id, frame in combined.groupby("setup_id", sort=True):
        row = frame.iloc[0]
        setup_rows.append(dict(setup_id=setup_id, signal_end=row.signal_end, side=row.side,
            native_target_pct=float(row.native_target_pct), addition=setup_id in SELECTED_TARGETS,
            **metrics(frame, dates)))
    daily = pd.DataFrame(daily_rows)
    for scope in scopes:
        ix = daily.index[daily.scope.eq(scope)]
        curve = daily.loc[ix, "net_pnl_rs"].cumsum()
        daily.loc[ix, "cumulative_net_pnl_rs"] = curve
        daily.loc[ix, "daily_drawdown_rs"] = curve.cummax().clip(lower=0) - curve
    monthly, periods, setups = map(pd.DataFrame, (monthly_rows, period_rows, setup_rows))
    if len(setups) != 49 or set(added.setup_id) != set(SELECTED_TARGETS):
        raise ValueError("Enabled inventory must be existing 37 plus exactly twelve midday setups")
    columns = ["setup_id", "signal_end", "side", "native_target_pct"]
    actual_old = setups.loc[~setups.addition, columns].sort_values("setup_id").reset_index(drop=True)
    expected_old = old_inventory[columns].sort_values("setup_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_old, expected_old, check_dtype=False, check_exact=True)
    add_stats = setups.loc[setups.addition]
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=now.isoformat(), first_date=dates[0], last_date=dates[-1], sessions=len(days),
        added_setups=12, original_setups=37, total_setups=49, selected_targets_pct=SELECTED_TARGETS,
        baseline_g4=metrics(baseline, dates), selected_additions=metrics(added, dates), combined_g4=metrics(combined, dates),
        incremental_net_pnl_rs=float(combined.portfolio_net_profit_rupees.sum()-baseline.portfolio_net_profit_rupees.sum()),
        baseline_trades_changed=changed, all_requested_additions_observed_70=bool(add_stats.win_rate_pct.ge(70).all()),
        all_requested_additions_profitable=bool(add_stats.net_pnl_rs.gt(0).all()),
        all_requested_additions_minimum_five_fills=bool(add_stats.executed_trades.ge(5).all()),
        selection_matches_frozen_entry_study=True, all_49_setups_minute_replay_verified=True,
        source_proof=source_proof, source_rejected_rows=len(rejected), entry_study=entry_proof,
        target_study=target_proof, baseline_study=baseline_proof, baseline_late_path_evidence=path_proofs,
        config_path=str(Path(config_path).resolve()), config_sha256=sha256(config_path), config_fingerprint=fingerprint,
        period_results=json.loads(periods.to_json(orient="records")),
        excluded_session="2026-10-01: incomplete immutable input",
        historical_targets_and_subset_selected_after_inspection=True, untouched_out_of_sample=False,
        capital=dict(per_entry_rs=100_000., leverage=5., exposure_rs=500_000., portfolio_rs=1_000_000.),
        stops=dict(initial_pct=1.25, tightened_pct=1., tighten_after_minutes=120), cost_bps=5.)
    inventory = setups[[*columns, "addition"]].copy()
    inventory["enabled"] = True
    output.mkdir(parents=True, exist_ok=True)
    artifacts = dict(selection_audit=audit, source_rejections=rejected, selected_orders=orders,
        all_selected_orders=pd.concat((baseline_raw, orders), ignore_index=True, sort=False),
        baseline_trades=baseline, added_raw_trades=raw_added, added_trades=added, combined_trades=combined,
        daily_results=daily, monthly_results=monthly, period_results=periods, setup_results=setups,
        enabled_setups=inventory)
    for name, frame in artifacts.items():
        frame.to_csv(output / f"{name}.csv", index=False)
    (output / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    write_report(output, summary, setups, monthly, periods, daily)
    code = [Path(__file__), ROOT / "fno_v13_v10_g_4_backtest.py", Path(contract.__file__), Path(simulator.__file__),
        Path(simulator.g.v9.v5.__file__), Path(simulator.g.v9.v6.__file__), ROOT / "research/g4_late_slot_source.py",
        ROOT / "research/g4_late_slot_backtest.py", ROOT / "research/g4_selected_backtest.py",
        ROOT / "research/g4_extended_slot_backtest.py", ROOT / "research/g4_extended_target_sweep.py",
        ROOT / "research/g3_0920_backtest.py", ROOT / "research/g3_g4_late_target_sweep.py",
        ROOT / "research/g3_g4_late_combined_backtest.py"]
    manifest = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        code_sha256={str(p.relative_to(ROOT)): sha256(p) for p in code},
        files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(add_stats[["setup_id", "native_target_pct", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs"]].to_string(index=False), flush=True)
    return summary
