"""Default G4 backtest: original 27 setups plus the ten user-selected additions.

Frozen filters and targets are checked against the previous verified research.
Selections and all 37 setups' exits are replayed from the original minute paths.
This module has no production, broker, scheduler or dashboard write authority.
"""
from __future__ import annotations

import copy
import hashlib
import html
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from research import g3_v2_contract as contract
from research.g3_0920_backtest import allocate, _stats
from research.g3_0920_target_sweep import assert_same_entries
from research.g3_g4_late_combined_backtest import checked_allocate, same_raw, verified_run
from research.g3_g4_late_target_sweep import load_paths, replay_target
from research.g4_extended_slot_backtest import (
    ROOT, BACKTEST_ROOT, CLOCKS, DEFAULT_CACHE, VERSION as ENTRY_VERSION, portfolio_changed,
)
from research.g4_extended_target_sweep import VERSION as TARGET_VERSION
from research.g4_late_slot_backtest import configuration as old_configuration, DEFAULT_CONFIG as OLD_CONFIG, _g3_baseline
from research.g4_late_slot_source import load_late_source, sha256

VERSION = "V13-V10-G-4-SELECTED-TEN-V3"
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_selected_v3.json"
SELECTED_TARGETS = {
    "1036_LONG": 2.12, "1036_SHORT": 2.86, "1041_LONG": 1.71,
    "1046_LONG": .79, "1046_SHORT": 1.01, "1051_LONG": .77,
    "1056_SHORT": 1.53, "1111_LONG": 1.76, "1111_SHORT": 1.84, "1126_SHORT": 4.20,
}


def configuration(path=DEFAULT_CONFIG):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    required = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        preserve_existing_g4_setups=True, allow_unlisted_additions=False,
        initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
        confirmation_offset_minutes=1, entry_expiry_minutes=10, square_off="15:15",
        round_trip_cost_bps=5., capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.)
    if any(value.get(key) != expected for key, expected in required.items()):
        raise ValueError("Selected G4 profile changed its authority, base or execution policy")
    setups = value["setups"]
    if len(setups) != 10 or {s["setup_id"] for s in setups} != set(SELECTED_TARGETS):
        raise ValueError("G4 selected profile must contain exactly the ten requested time/side additions")
    for s in setups:
        if (s.get("enabled") is not True or s["target_pct"] != SELECTED_TARGETS[s["setup_id"]]
                or s["setup_id"] != s["confirmation_end"].replace(":", "") + "_" + s["side"]
                or s["confirmation_end"] != (pd.Timestamp("2000-01-01 " + s["signal_end"]) + pd.Timedelta(minutes=1)).strftime("%H:%M")
                or (s["initial_stop_pct"], s["tightened_stop_pct"], s["tighten_after_minutes"], s["max_entries"], s["picker"]) != (1.25, 1., 120, 1, "max_liquidity")):
            raise ValueError("Selected G4 setup has an incorrect clock, target, quota or stop")
        for field, v in s["thresholds"].items():
            if not np.isfinite(float(v)):
                raise ValueError(f"Nonfinite selected threshold: {field}")
    return value


def verify_reference(relative, digest, version):
    folder = (BACKTEST_ROOT / relative).resolve()
    if not folder.is_relative_to(BACKTEST_ROOT.resolve()) or sha256(folder / "manifest.json") != digest:
        raise ValueError("Selected G4 reference manifest changed or escaped its result root")
    return folder, verified_run(folder, version)


@contextmanager
def selected_setups(config):
    """Install only the selected additions in the archived offline selector."""
    names = ("CORE_SETUPS", "ACTIVE_SETUPS", "_CORE_BY_ID", "_ACTIVE_BY_ID",
        "MIN_CONFIRMATION_VOLUME_BY_SETUP", "SIGNAL_TO_CONFIRMATION",
        "STRATEGY_PROFILE", "STRATEGY_VERSION", "ACCEPTED_VARIANT")
    old = {k: getattr(contract, k) for k in names}
    specs, volumes = [], {}
    try:
        for row in config["setups"]:
            level = row["thresholds"]
            donor = contract.setup_for("09:50", row["side"])
            spec = replace(donor, signal_end=row["signal_end"], confirmation_end=row["confirmation_end"],
                max_entries=1, picker="max_liquidity", price_change_pct=level["price_change_pct"],
                oi_change_pct=level["oi_change_pct"], volume_ratio=level["five_minute_volume_ratio_min"],
                body_ratio=level["one_minute_body_ratio_min"], max_wick_ratio=level["one_minute_wick_ratio_max"],
                stop_pct=1.25, target_pct=row["target_pct"], source_version=config["variant_id"])
            specs.append(spec)
            volumes[spec.setup_id] = level["one_minute_volume_ratio_min"]
        contract.CORE_SETUPS = contract.ACTIVE_SETUPS = tuple(specs)
        contract._CORE_BY_ID = {s.setup_id: s for s in specs}
        contract._ACTIVE_BY_ID = dict(contract._CORE_BY_ID)
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP = volumes
        contract.SIGNAL_TO_CONFIRMATION = {s.signal_end: s.confirmation_end for s in specs}
        contract.STRATEGY_PROFILE = contract.STRATEGY_VERSION = contract.ACCEPTED_VARIANT = config["variant_id"]
        yield
    finally:
        for name, before in old.items():
            setattr(contract, name, before)


def assert_previous_outcomes(actual, previous, *, entries_only=False):
    """Changing the G4 profile version changes its canonical signal ID only."""
    left = actual.copy()
    prior_ids = previous.set_index("sid").signal_id
    left["signal_id"] = left.sid.map(prior_ids)
    if entries_only:
        assert_same_entries(left, previous)
    else:
        same_raw(left, previous)


def metrics(frame, days):
    result = _stats(frame, days)
    executed = frame.loc[frame.portfolio_executed.eq(True)]
    pnl = executed.portfolio_net_profit_rupees.astype(float)
    positive, negative = float(pnl.clip(lower=0).sum()), float(-pnl.clip(upper=0).sum())
    dates = frame.groupby(frame.day.astype(str)).portfolio_net_profit_rupees.sum().reindex([str(d) for d in days], fill_value=0.)
    result.update(profit_factor=positive / negative if negative else None,
        average_net_pnl_per_trade_rs=float(pnl.mean()) if len(pnl) else None,
        winning_sessions=int(dates.gt(0).sum()), losing_sessions=int(dates.lt(0).sum()),
        flat_sessions=int(dates.eq(0).sum()), sessions=len(days),
        portfolio_return_pct=result["net_pnl_rs"] / 1_000_000 * 100,
        capital_rejected_fills=result["native_fills"] - result["executed_trades"])
    return result


def write_report(output, summary, setups, monthly, periods, daily):
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 selected ten full backtest</title>
<style>body{font:15px/1.5 system-ui,sans-serif;background:#f4f7fa;color:#172a3d;margin:30px}h1,h2{line-height:1.2}.note{background:#fff0ce;padding:16px;border-left:4px solid #b78000}.scroll{overflow:auto;max-height:650px;margin:18px 0 30px}table{border-collapse:collapse;background:white;width:100%}th,td{padding:9px;border-bottom:1px solid #d8e0e8;text-align:right;white-space:nowrap}th{background:#243d57;color:white;position:sticky;top:0}td:first-child,th:first-child{text-align:left}a{color:#075db6}p{max-width:1150px}</style></head><body><h1>G4: the ten selected additions</h1>'''
    page = page.replace("G4 selected ten full backtest", "G4 selected additions full backtest").replace("G4: the ten selected additions", f"G4: {summary['original_setups']} existing + {summary['added_setups']} selected additions")
    page += f"<p>{summary['first_date']} to {summary['last_date']}: {summary['sessions']} sessions. The existing {summary['original_setups']} G4 setups plus exactly {summary['added_setups']} selected additions; no new threshold or target optimization in this run.</p>"
    page += '<div class=note>Targets and this subset were selected after inspecting historical results. Full-period and later-period outcomes are descriptive, not untouched out-of-sample validation. October 1 is excluded because immutable input was incomplete.</div>'
    page += '<p>New entries use exact next-minute confirmation and the highest-liquidity qualifying stock, at most once per setup/session. Target percentages are those selected in the preceding target study. Stops remain 1.25%, tightening to 1.00% after 120 minutes; entry expiry is ten minutes and square-off is 15:15.</p>'
    page += '<p>Capital is Rs 100,000 per entry at five-times exposure, with Rs 1,000,000 portfolio capital. P&amp;L deducts five basis points round-trip cost. Wins mean positive net P&amp;L, including profitable time exits. Drawdown is measured from daily realized P&amp;L. Extra slippage, market impact and broker-specific charges are not modeled.</p>'
    totals = pd.DataFrame([dict(scope=label, **summary[label]) for label in ("baseline_g4", "selected_additions", "combined_g4")])
    headline_cols = ["scope", "selected_orders", "executed_trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs", "profit_factor"]
    setup_cols = ["setup_id", "signal_end", "side", "native_target_pct", "executed_trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]
    tables = [("Portfolio totals", totals[headline_cols]),
        ("Selected additions: actual portfolio executions", setups.loc[setups.addition, setup_cols]),
        ("Monthly results", monthly[["month", "scope", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]]),
        ("Original training and later periods", periods[["period", "scope", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs"]]),
        (f"All {summary['total_setups']} configured setups", setups[setup_cols]),
        ("Combined daily results", daily.loc[daily.scope.eq("combined"), ["day", "executed_trades", "wins", "losses", "net_pnl_rs", "cumulative_net_pnl_rs", "daily_drawdown_rs"]])]
    for title, frame in tables:
        page += f'<h2>{html.escape(title)}</h2><div class=scroll>{frame.to_html(index=False,border=0,na_rep="—",float_format=lambda v:f"{v:,.2f}")}</div>'
    page += f"<p>Existing baseline trades changed by portfolio allocation: {summary['baseline_trades_changed']}. Added net P&amp;L: Rs {summary['incremental_net_pnl_rs']:,.2f}. The original baseline is the pinned G4 book used in the preceding studies, not current G3 production.</p>"
    page += '<p>' + ' · '.join(f'<a href="{name}">{name}</a>' for name in ("config.json", "enabled_setups.csv", "daily_results.csv", "monthly_results.csv", "setup_results.csv", "period_results.csv", "combined_trades.csv", "added_trades.csv", "selected_orders.csv", "summary.json", "manifest.json")) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(config_path=DEFAULT_CONFIG, output_dir=None, *, cache=DEFAULT_CACHE):
    config = configuration(config_path)
    now = datetime.now(contract.IST)
    output = Path(output_dir or BACKTEST_ROOT / "runs" / f"{now:%Y-%m-%d}" / f"{now:%Y%m%dT%H%M%S%f}_selected_ten").resolve()
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Output must be new or empty within the G4 backtest result root")
    entry_dir, entry_proof = verify_reference(config["entry_source_run_relative"], config["entry_source_manifest_sha256"], ENTRY_VERSION)
    target_dir, target_proof = verify_reference(config["target_source_run_relative"], config["target_source_manifest_sha256"], TARGET_VERSION)
    target_config = json.loads((target_dir / "scenario_configs.json").read_text(encoding="utf-8"))
    expected_setups = {s["setup_id"]: s for s in target_config["scenarios"]["historical_profitable70_only"]}
    for spec in config["setups"]:
        expected = {k: v for k, v in expected_setups[spec["setup_id"]].items() if not k.startswith("prior_entry_")}
        if {k:v for k,v in spec.items() if k != "enabled"} != expected:
            raise ValueError("Selected filters or targets differ from the preceding verified study")
    candidates, paths, days, source_proof, snapshots = load_late_source(cache, clocks=CLOCKS, sid_base=5_000_000, verify_snapshots=True)
    with selected_setups(config):
        audit = contract.selection_audit(candidates)
        orders = audit.loc[audit.v9_selected.eq(True)].copy()
    frozen_orders = pd.read_csv(entry_dir / "selected_orders.csv")
    frozen_orders = frozen_orders.loc[frozen_orders.setup_id.isin(SELECTED_TARGETS)]
    if (set(orders.sid) != set(frozen_orders.sid) or set(orders.setup_id) != set(SELECTED_TARGETS)
            or orders.duplicated(["day", "setup_id"]).any()):
        raise ValueError("Requested setup selection differs from the frozen selected entry filters")
    orders["source_signal_id"] = orders.sid.map(frozen_orders.set_index("sid").signal_id)
    selected_paths = {int(s): paths[int(s)] for s in orders.sid}
    raw_added = replay_target(orders, selected_paths, SELECTED_TARGETS)
    frozen_raw = pd.read_csv(target_dir / "historical_profitable70_only_added_raw_trades.csv")
    frozen_raw = frozen_raw.loc[frozen_raw.setup_id.isin(SELECTED_TARGETS)]
    assert_previous_outcomes(raw_added, frozen_raw)
    assert_previous_outcomes(raw_added, frozen_raw, entries_only=True)
    print(f"Selected exactly {len(SELECTED_TARGETS)} additions / {len(orders)} orders; native replay matches frozen target outcomes", flush=True)
    # Replay all original G4 exits too. Its 15 G3 V2 setups and original twelve
    # late setups use separate verified historical path inventories.
    old_config = old_configuration(OLD_CONFIG)
    g3_orders, baseline_paths, _ = _g3_baseline(days, snapshots, old_config)
    entry_summary = json.loads((entry_dir / "summary.json").read_text(encoding="utf-8"))
    baseline_proof = entry_summary["baseline"]
    prior_dir, _ = verify_reference(str(Path(baseline_proof["run_dir"]).relative_to(BACKTEST_ROOT)),
        baseline_proof["manifest_sha256"], "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1")
    original_late = pd.read_csv(prior_dir / "g4_added_orders.csv")
    original_summary = json.loads((prior_dir / "summary.json").read_text(encoding="utf-8"))
    original_paths, old_path_proofs = load_paths(original_late, original_summary)
    if set(baseline_paths) & set(original_paths):
        raise ValueError("Original G4 path identifiers overlap")
    baseline_paths.update(original_paths)
    baseline_saved = pd.read_csv(entry_dir / "baseline_g4_trades.csv")
    if set(baseline_saved.sid) != set(baseline_paths) or len(baseline_saved) != 212:
        raise ValueError("Original G4 baseline order/path inventory changed")
    baseline_raw = simulator.simulate_staged(baseline_saved, baseline_paths,
        cost_bps=5., max_entry_delay_minutes=10)
    same_raw(baseline_raw, baseline_saved)
    baseline = checked_allocate(baseline_raw, baseline_saved)
    if set(baseline.sid) & set(orders.sid):
        raise ValueError("New setup IDs overlap original G4 orders")
    combined = allocate(pd.concat((baseline_raw, raw_added), ignore_index=True, sort=False))
    fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    combined["g4_profile_version"] = VERSION
    combined["g4_profile_fingerprint"] = fingerprint
    added = combined.loc[combined.sid.isin(orders.sid)].copy()
    changed = portfolio_changed(baseline, combined)
    scopes = dict(baseline=baseline, additions=added, combined=combined)
    dates = [str(d) for d in days]
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
    if len(setups) != 37 or set(added.setup_id) != set(SELECTED_TARGETS):
        raise ValueError("Enabled setup inventory is not original 27 plus selected ten")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=now.isoformat(), first_date=dates[0], last_date=dates[-1], sessions=len(days),
        added_setups=10, original_setups=27, total_setups=37, selected_targets_pct=SELECTED_TARGETS,
        baseline_g4=metrics(baseline, dates), selected_additions=metrics(added, dates), combined_g4=metrics(combined, dates),
        incremental_net_pnl_rs=float(combined.portfolio_net_profit_rupees.sum()-baseline.portfolio_net_profit_rupees.sum()),
        baseline_trades_changed=changed, all_requested_additions_observed_70=bool(setups.loc[setups.addition].win_rate_pct.ge(70).all()),
        selection_matches_frozen_entry_study=True, all_37_setups_minute_replay_verified=True,
        source_proof=source_proof, entry_study=entry_proof, target_study=target_proof,
        baseline_study=baseline_proof, baseline_late_path_evidence=old_path_proofs,
        config_path=str(Path(config_path).resolve()), config_sha256=sha256(config_path), config_fingerprint=fingerprint,
        period_results=json.loads(periods.to_json(orient="records")),
        excluded_session="2026-10-01: incomplete immutable input",
        historical_targets_and_subset_selected_after_inspection=True, untouched_out_of_sample=False,
        capital=dict(per_entry_rs=100_000., leverage=5., exposure_rs=500_000., portfolio_rs=1_000_000.),
        stops=dict(initial_pct=1.25, tightened_pct=1., tighten_after_minutes=120), cost_bps=5.)
    output.mkdir(parents=True, exist_ok=True)
    inventory = setups[["setup_id", "signal_end", "side", "native_target_pct", "addition"]].copy()
    inventory["enabled"] = True
    all_orders = pd.concat((g3_orders, original_late, orders), ignore_index=True, sort=False)
    artifacts = dict(selection_audit=audit, selected_orders=orders, all_selected_orders=all_orders,
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
        ROOT / "research/g3_0920_backtest.py", ROOT / "research/g3_g4_late_target_sweep.py"]
    manifest = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        code_sha256={str(p.relative_to(ROOT)): sha256(p) for p in code},
        files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(setups.loc[setups.addition, ["setup_id", "native_target_pct", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs"]].to_string(index=False), flush=True)
    return summary
