"""Reconstruct the 49-session G3 base with its ten selected G4 additions."""
from datetime import date, datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_ROOT, runtime_dir
import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_daily_replay as replay
from research.archived_g3_v5.backtest_profile import contract, configuration, ADDED_IDS, VERSION, CONFIG_PATH
from research.g3_current_backtest import verified_package as verify_v4
from research.g3_0920_backtest import new_execution_paths, _stats
from research.g3_g4_late_combined_backtest import G3_RUN, verified_run, same_raw, checked_allocate
from research.g4_late_slot_backtest import _g3_baseline, configuration as old_config, DEFAULT_CONFIG
from research.g4_late_slot_source import load_late_source, sha256
from research.g4_extended_slot_backtest import CLOCKS, DEFAULT_CACHE
from research.g4_selected_backtest import assert_previous_outcomes

ROOT = runtime_dir("backtesting_result_v13_v10_g_3")
FILES = {"config.json", "summary.json", "variant_trades.csv", "baseline_trades.csv", "added_trades.csv",
         "selected_orders.csv", "selection_audit.csv", "daily_results.csv", "setup_results.csv", "report.html"}


def verified_package(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "manifest.json").read_text())
    if (manifest.get("variant_id") != VERSION or manifest.get("execution_authority") is not False
            or set(manifest.get("files", {})) != FILES):
        raise ValueError("Invalid selected-ten G3 historical manifest")
    for name, digest in manifest["files"].items():
        if sha256(directory / name) != digest:
            raise ValueError(f"Selected G3 historical artifact changed: {name}")
    summary = json.loads((directory / "summary.json").read_text())
    if (json.loads((directory / "config.json").read_text()) != contract.strategy_payload()
            or not contract.supported_identity(summary["strategy_version"], summary["strategy_fingerprint"])
            or summary.get("normal_minute_replay_verified") is not True or summary["sessions"] != 49
            or summary.get("execution_authority") is not False):
        raise ValueError("Selected G3 historical profile does not match its verified replay")
    ledger = pd.read_csv(directory / "variant_trades.csv")
    days = pd.read_csv(directory / "daily_results.csv").day.astype(str).tolist()
    if (days != sorted(set(days)) or len(days) != 49 or summary["first_date"] != days[0]
            or summary["last_date"] != days[-1] or ledger.signal_id.duplicated().any()
            or set(ledger.setup_id) != set(contract._ACTIVE_BY_ID)):
        raise ValueError("Selected G3 inventory/calendar mismatch")
    for field, value in (("strategy_version", contract.STRATEGY_VERSION),
                          ("strategy_fingerprint", contract.strategy_fingerprint())):
        if not ledger[field].eq(value).all():
            raise ValueError("Selected G3 base mixes strategy identities")
    for key, value in _stats(ledger, [date.fromisoformat(day) for day in days]).items():
        if not np.isclose(summary["current_g3"][key], value, atol=1e-6, rtol=0):
            raise ValueError("Selected G3 summary accounting mismatch")
    return summary, manifest


def run(output):
    output = Path(output).resolve()
    if not output.is_relative_to(ROOT.resolve()) or output.exists():
        raise ValueError("Selected G3 replay needs a new directory in the G3 result root")
    config = configuration()
    baseline_dir = (ROOT / config["historical_base_run_relative"]).resolve()
    if (not baseline_dir.is_relative_to(ROOT.resolve())
            or sha256(baseline_dir / "manifest.json") != config["historical_base_manifest_sha256"]):
        raise ValueError("Pinned G3 V4 baseline manifest changed")
    verify_v4(baseline_dir)
    saved = pd.read_csv(baseline_dir / "variant_trades.csv")
    # Recompute the ten selections from the complete sealed pre-quota pools.
    candidates, extra_paths, days, extra_proof, snapshots = load_late_source(
        DEFAULT_CACHE, clocks=CLOCKS, sid_base=5_000_000, verify_snapshots=True)
    audit = contract.selection_audit(candidates)
    extra_orders = audit.loc[audit.v9_selected.eq(True)].copy()
    if set(extra_orders.setup_id) != ADDED_IDS or extra_orders.duplicated(["day", "setup_id"]).any():
        raise ValueError("Selected G3 pool does not contain exactly the ten requested setups")
    g4root = runtime_dir("backtesting_result_v13_v10_g_4")
    target_dir = g4root / config["target_source_run_relative"]
    if sha256(target_dir / "manifest.json") != config["target_source_manifest_sha256"]:
        raise ValueError("Selected target evidence changed")
    target_proof = verified_run(target_dir, "V13-V10-G-4-1035-1130-TARGET-SWEEP-V1")
    expected = pd.read_csv(target_dir / "historical_profitable70_only_added_raw_trades.csv")
    expected = expected.loc[expected.setup_id.isin(ADDED_IDS)]
    extra_raw = simulator.simulate_staged(extra_orders, extra_paths, cost_bps=5., max_entry_delay_minutes=10)
    assert_previous_outcomes(extra_raw, expected)
    assert_previous_outcomes(extra_raw, expected, entries_only=True)
    # Replay the original 22 G3 setups using their verified minute paths.
    _, late_paths, old_days, late_proof, old_snapshots = load_late_source(verify_snapshots=True)
    if old_days != days:
        raise ValueError("G3 and selected G4 session calendars differ")
    _, paths, _ = _g3_baseline(days, old_snapshots, old_config(DEFAULT_CONFIG))
    opening = pd.read_csv(G3_RUN / "added_0920_orders.csv")
    opening_paths, opening_proof = new_execution_paths(opening, Path(RUNTIME_ROOT))
    for group in (opening_paths, late_paths, extra_paths):
        if set(paths) & set(group):
            raise ValueError("Historical minute path IDs overlap")
        paths.update(group)
    baseline_raw = simulator.simulate_staged(saved, paths, cost_bps=5., max_entry_delay_minutes=10)
    same_raw(baseline_raw, saved)
    baseline = checked_allocate(baseline_raw, saved)
    original_orders = pd.read_csv(baseline_dir / "selected_orders.csv")
    if set(original_orders.sid) != set(saved.sid):
        raise ValueError("Pinned G3 order/ledger identities disagree")
    orders = pd.concat((original_orders, extra_orders), ignore_index=True, sort=False)
    orders["source_signal_id"] = orders.signal_id
    for index, row in orders.iterrows():
        spec = contract._ACTIVE_BY_ID[row.setup_id]
        orders.loc[index, "signal_id"] = contract.canonical_signal_id(VERSION,
            date.fromisoformat(str(row.day)), spec.signal_end, spec.confirmation_end, spec.side, row.tradingsymbol)
    orders["strategy_version"] = VERSION
    orders["strategy_fingerprint"] = contract.strategy_fingerprint()
    variant, _ = replay.simulate(orders, {int(sid): paths[int(sid)] for sid in orders.sid}, days)
    variant["day"] = pd.to_datetime(variant.day).dt.strftime("%Y-%m-%d")
    baseline["day"] = pd.to_datetime(baseline.day).dt.strftime("%Y-%m-%d")
    old = variant.loc[variant.sid.isin(saved.sid)].copy()
    old["signal_id"] = old.sid.map(saved.set_index("sid").signal_id)
    same_raw(old, saved)
    old = old.set_index("sid").sort_index()
    original = baseline.set_index("sid").sort_index()
    changed = int((old.portfolio_executed.ne(original.portfolio_executed)
        | old.portfolio_net_profit_rupees.sub(original.portfolio_net_profit_rupees).abs().gt(1e-6)).sum())
    added = variant.loc[variant.setup_id.isin(ADDED_IDS)].copy()
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        strategy_version=VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
        first_date=str(days[0]), last_date=str(days[-1]), active_setup_count=32,
        baseline_g3_v4=_stats(baseline, days), current_g3=_stats(variant, days), added_ten=_stats(added, days),
        original_trades_changed_by_allocation=changed, normal_minute_replay_verified=True,
        historical_targets_selected_after_inspection=True, independent_holdout_qualification=False,
        baseline_source=dict(run_dir=str(baseline_dir), manifest_sha256=sha256(baseline_dir / "manifest.json")),
        target_source=target_proof, new_source_evidence=extra_proof, late_source_evidence=late_proof,
        opening_path_evidence=opening_proof, excluded_session="2026-10-01: incomplete immutable input")
    from research.g3_current_history import _daily_table
    daily = _daily_table(variant, [str(day) for day in days])
    setups = pd.DataFrame([dict(setup_id=setup, target_pct=contract._ACTIVE_BY_ID[setup].target_pct,
        added=setup in ADDED_IDS, **_stats(frame, days)) for setup, frame in variant.groupby("setup_id")])
    output.mkdir(parents=True)
    for name, frame in dict(variant_trades=variant, baseline_trades=baseline, added_trades=added,
            selected_orders=orders, selection_audit=audit, daily_results=daily, setup_results=setups).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    replay.atomic_json(output / "config.json", contract.strategy_payload())
    replay.atomic_json(output / "summary.json", summary)
    totals = pd.DataFrame([dict(scope=k, **summary[k]) for k in ("baseline_g3_v4", "added_ten", "current_g3")])
    page = '<!doctype html><html><meta charset="utf-8"><title>G3 32-setup historical backtest</title><style>body{font:15px system-ui;margin:25px}td,th{padding:7px;text-align:right}div{overflow:auto}</style><h1>G3: 32 setups</h1>'
    page += '<p>49 sessions through October 12, 2026. Existing 22 plus the ten selected setups. Historical targets were fitted after inspecting results; this is not independent holdout validation. October 1 is excluded. Opening OI is retrospective. Stops: 1.25%, tightened to 1% after 120 minutes. Five bps round-trip cost; Rs100,000 per entry, 5x exposure; Rs1,000,000 portfolio.</p>'
    for frame in (totals, setups, daily):
        page += '<div>' + frame.to_html(index=False, float_format=lambda v: f'{v:,.2f}') + '</div>'
    (output / "report.html").write_text(page + '</html>', encoding="utf-8")
    replay.atomic_json(output / "manifest.json", dict(variant_id=VERSION, execution_authority=False,
        files={name: sha256(output / name) for name in sorted(FILES)},
        code_sha256={str(path): sha256(path) for path in (Path(__file__), Path(replay.__file__), CONFIG_PATH,
            Path(contract.__file__), Path(__file__).with_name('g3_backtest_profile.py'))}))
    verified_package(output)
    print(json.dumps({key: summary[key] for key in ('baseline_g3_v4', 'added_ten', 'current_g3', 'original_trades_changed_by_allocation')}, indent=2), flush=True)
    return summary
