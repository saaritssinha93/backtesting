"""Normal G3 history: retained V3 selections plus six approved late setups.

Every exit is replayed from verified minute inputs. The prior 131 selected
orders are sealed evidence for unchanged entry rules; late selections are
recomputed from the complete G4 candidate pools using the shared G3 rules.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
import copy
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.archived_g3_v4 import contract
import fno_v13_v10_g_3_late_setups as late
import fno_v13_v10_g_3_daily_replay as replay
import fno_v13_v10_g_2_backtest as simulator
from eqidv2_runtime_paths import RUNTIME_ROOT
from research.g3_0920_backtest import allocate, _stats, new_execution_paths
from research.g3_g4_late_combined_backtest import G3_RUN, G4_RUN, ROOT, verified_run, same_raw, sha256, daily_table
from research.g4_late_slot_source import load_late_source
from research.g4_late_slot_backtest import configuration, DEFAULT_CONFIG, _g3_baseline

VARIANT = "V13-V10-G-3-BT-CURRENT-V4"


@contextmanager
def current_contract():
    """Stage the exact forthcoming contract before production promotion."""
    names = ("CORE_SETUPS", "ACTIVE_SETUPS", "_CORE_BY_ID", "_ACTIVE_BY_ID",
             "MIN_CONFIRMATION_VOLUME_BY_SETUP", "SIGNAL_TO_CONFIRMATION", "STRATEGY_VERSION", "ACCEPTED_VARIANT")
    before = {name: copy.deepcopy(getattr(contract, name)) for name in names}
    if contract.STRATEGY_VERSION != late.VERSION:
        if len(contract.ACTIVE_SETUPS) != 16 or late.SETUP_IDS & set(contract._ACTIVE_BY_ID):
            raise ValueError("Expected original G3 V3 or exact promoted V4")
        extras = tuple(replace(contract._make_setup(row), source_version=late.VERSION) for row in late.ROWS)
        contract.CORE_SETUPS = tuple(replace(s, source_version=late.VERSION) for s in contract.CORE_SETUPS) + extras
        contract.ACTIVE_SETUPS = tuple(replace(s, source_version=late.VERSION) for s in contract.ACTIVE_SETUPS) + extras
        contract._CORE_BY_ID = {s.setup_id: s for s in contract.CORE_SETUPS}
        contract._ACTIVE_BY_ID = {s.setup_id: s for s in contract.ACTIVE_SETUPS}
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP = {**contract.MIN_CONFIRMATION_VOLUME_BY_SETUP, **late.CONFIRMATION_VOLUME}
        contract.SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in contract.ACTIVE_SETUPS}.items()))
        contract.STRATEGY_VERSION = late.VERSION
        contract.ACCEPTED_VARIANT = late.ACCEPTED_VARIANT
    try:
        if len(contract.ACTIVE_SETUPS) != 22 or {s.setup_id: s.target_pct for s in contract.ACTIVE_SETUPS if s.setup_id in late.SETUP_IDS} != late.TARGETS:
            raise ValueError("Current history requires the exact six approved additions")
        yield
    finally:
        for name, value in before.items():
            setattr(contract, name, value)


def verified_package(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != VARIANT or manifest.get("execution_authority") is not False
            or summary.get("variant_id") != VARIANT or summary.get("sessions") != 49
            or summary.get("existing_g3_trades_unchanged") is not True
            or summary.get("normal_minute_replay_verified") is not True):
        raise ValueError("Invalid current G3 historical result")
    for name, digest in manifest["files"].items():
        if Path(name).name != name or sha256(directory / name) != digest:
            raise ValueError("Current G3 backtest artifact verification failed")
    config = json.loads((directory / "config.json").read_text(encoding="utf-8"))
    with current_contract():
        if config != contract.strategy_payload() or summary["strategy_fingerprint"] != contract.strategy_fingerprint():
            raise ValueError("Current historical rules differ from approved configuration")
    return summary, manifest


def publish(directory, root=ROOT):
    directory, root = Path(directory).resolve(), Path(root).resolve()
    relative = directory.relative_to(root)
    summary, _ = verified_package(directory)
    if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4 or relative.parts[2] != summary["last_date"]:
        raise ValueError("Invalid current-history destination")
    pointer = dict(schema_version="fno_g3_current_backtest_v2", role="backtest_history",
        run_dir=relative.as_posix(), variant_id=VARIANT, manifest_sha256=sha256(directory / "manifest.json"),
        strategy_version=summary["strategy_version"], strategy_fingerprint=summary["strategy_fingerprint"],
        session_date=summary["last_date"], execution_authority=False, created_at_ist=summary["created_at_ist"])
    replay.atomic_json(root / "latest/current_backtest.json", pointer)
    return pointer


def run(output):
    output = Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Historical output must be new or empty")
    evidence = dict(g3=verified_run(G3_RUN, "V13-V10-G-3-BT-0920-LONG-T300-V1"),
                    g4=verified_run(G4_RUN, "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1"))
    candidates, late_paths, days, source_evidence, snapshots = load_late_source()
    prior_orders, prior_paths, _ = _g3_baseline(days, snapshots, configuration(DEFAULT_CONFIG))
    opening = pd.read_csv(G3_RUN / "added_0920_orders.csv")
    opening_paths, opening_proofs = new_execution_paths(opening, Path(RUNTIME_ROOT))
    baseline_orders = pd.concat((prior_orders, opening), ignore_index=True, sort=False)
    for field in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        baseline_orders[field] = pd.to_datetime(baseline_orders[field], utc=True).dt.tz_convert(contract.IST)
    saved = pd.read_csv(G3_RUN / "variant_trades.csv")
    all_paths = {**prior_paths, **opening_paths, **late_paths}
    baseline_raw = simulator.simulate_staged(baseline_orders, all_paths,
        cost_bps=contract.ROUND_TRIP_COST_BPS, max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    same_raw(baseline_raw, saved)
    baseline = allocate(baseline_raw)
    expected = pd.read_csv(G4_RUN / "g4_added_orders.csv")
    expected = expected.loc[expected.setup_id.isin(late.SETUP_IDS)]
    with current_contract():
        audit = contract.selection_audit(candidates)
        added_orders = audit.loc[audit.v9_selected.eq(True)].copy()
        if set(added_orders.sid) != set(expected.sid) or set(added_orders.setup_id) != late.SETUP_IDS:
            raise ValueError("G3 shared selection differs from the six selected G4 pools")
        orders = pd.concat((baseline_orders, added_orders), ignore_index=True, sort=False)
        for field in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
            orders[field] = pd.to_datetime(orders[field], utc=True).dt.tz_convert(contract.IST)
        raw = simulator.simulate_staged(orders, all_paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
                                       max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
        variant = allocate(raw)
        variant["strategy"] = "V13-V10-G-3"
        variant["strategy_version"] = contract.STRATEGY_VERSION
        variant["strategy_fingerprint"] = contract.strategy_fingerprint()
        config = contract.strategy_payload()
        fingerprint = contract.strategy_fingerprint()
    old = variant.loc[variant.sid.isin(saved.sid)].set_index("sid").sort_index()
    prior = baseline.set_index("sid").sort_index()
    same_raw(old.reset_index(), prior.reset_index())
    if (not old.portfolio_executed.equals(prior.portfolio_executed)
            or not np.allclose(old.portfolio_net_profit_rupees, prior.portfolio_net_profit_rupees, atol=1e-6, rtol=0)):
        raise ValueError("The new historical portfolio altered existing G3 trades")
    added = variant.loc[variant.sid.isin(added_orders.sid)].copy()
    summary = dict(variant_id=VARIANT, scope="BACKTEST_ONLY", execution_authority=False,
        strategy_version=late.VERSION, strategy_fingerprint=fingerprint,
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
        first_date=str(days[0]), last_date=str(days[-1]), baseline_g3_v3=_stats(baseline, days),
        current_g3=_stats(variant, days), added_six=_stats(added, days), active_setup_count=22,
        added_targets_pct=late.TARGETS, normal_minute_replay_verified=True, existing_g3_trades_unchanged=True,
        entry_selection_method="HASH_VERIFIED_UNCHANGED_V3_SELECTIONS_PLUS_SHARED_CONTRACT_LATE_POOL_RESELECTION",
        baseline_opening_oi_retrospective=True, independent_holdout_qualification=False,
        excluded_session="2026-10-01: incomplete immutable input", sources=evidence,
        source_evidence=source_evidence, opening_minute_path_evidence=opening_proofs)
    daily = daily_table(days, baseline, variant).rename(columns={
        "g3_plus_late_12_selected": "variant_selected", "g3_plus_late_12_fills": "variant_fills",
        "g3_plus_late_12_wins": "variant_wins", "g3_plus_late_12_net_pnl_rs": "variant_net_pnl_rs"})
    setup_results = pd.DataFrame([dict(setup_id=setup, target_pct=late.TARGETS[setup], **_stats(part, days))
                                  for setup, part in added.groupby("setup_id")])
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in dict(variant_trades=variant, baseline_trades=baseline, added_six_trades=added,
            selected_orders=orders, added_six_orders=added_orders, selection_audit=audit,
            daily_results=daily, setup_results=setup_results).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    (output / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    comparison = pd.DataFrame([dict(portfolio=label, **summary[key]) for label, key in (
        ("Previous G3 V3", "baseline_g3_v3"), ("Current G3 + selected six", "current_g3"), ("Added six", "added_six"))])
    page = '<!doctype html><html><head><meta charset="utf-8"><title>Current G3 historical backtest</title><style>body{font:15px/1.5 system-ui;margin:28px}table{border-collapse:collapse}td,th{padding:8px;border:1px solid #ddd}div{overflow:auto}</style></head><body>'
    page += '<h1>V13-V10-G-3: normal historical backtest</h1><p>49 sessions, 2026-07-29 through 2026-10-12. Six approved additions; 22 total setups. Every exit replayed from verified minute inputs.</p>'
    for title, frame in (("Portfolio", comparison), ("Added setups", setup_results), ("Daily results", daily)):
        page += f'<h2>{html.escape(title)}</h2><div>{frame.to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}")}</div>'
    page += '<p>Targets were selected retrospectively. Original strict holdout qualification was not met. Sizing: Rs100,000 per entry, 5x exposure; Rs1,000,000 portfolio; 5 bps costs. October 1 is excluded. Opening OI is retrospective.</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")
    manifest = dict(variant_id=VARIANT, execution_authority=False,
        code_sha256={str(p.resolve()): sha256(p) for p in (Path(__file__), Path(late.__file__), Path(simulator.__file__))},
        files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    verified_package(output)
    return summary
