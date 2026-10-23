"""Isolated G4 history comparison for a 10:11 SHORT price-move threshold.

Reselects the complete pre-quota 10:11 candidate pool, then replays all selected
orders and the shared portfolio. The published G4 configuration is read-only.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import json
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from research import g3_target_cutoff_contract as donor
from research import g3_0950_long_backtest as morning
from research import g4_complete_inventory_backtest as g4
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_late_slot_source import load_late_source


SETUP = "1011_SHORT"
BASE_THRESHOLD = 0.13
TEST_THRESHOLD = 0.11
SCHEMA = "g4_1011_short_threshold_comparison_v1"
ROOT = g4.bounded.G4_ROOT / "research"


def _keys(frame):
    return set(zip(frame.day.astype(str).str[:10], frame.setup_id, frame.tradingsymbol))


def _candidate_pool(source_summary):
    historical, _, historical_days, historical_proof, _ = load_late_source(
        verify_snapshots=True)
    if [str(day) for day in historical_days] != source_summary["days"][:49]:
        raise ValueError("Historical candidate calendar differs")
    parts = [historical.loc[historical.hhmm_int.eq(1010) &
                            historical.side.eq("SHORT")].copy()]
    evidence = []
    for item in source_summary["daily_path_evidence"]:
        directory = Path(item["run_dir"]).resolve()
        if not directory.is_relative_to(g4.bounded.G3_ROOT.resolve()):
            raise ValueError("Daily source escaped G3 root")
        payload_path = directory / "run_manifest.json"
        payload = g4.mirror.read(payload_path)
        day = payload["session_date"]
        if day not in source_summary["days"][49:]:
            raise ValueError("Unexpected appended candidate day")
        if (Path(item["snapshot_manifest"]).resolve() !=
                (directory / "input_snapshots" /
                 Path(item["snapshot_manifest"]).parent.name /
                 "snapshot_manifest.json").resolve() or
                g4.mirror.sha256(Path(item["snapshot_manifest"])) !=
                item["snapshot_manifest_sha256"]):
            raise ValueError("Daily snapshot provenance differs")
        record = payload["artifacts"]["candidate_signals"]
        path = (directory / record["path"]).resolve()
        if (not path.is_relative_to(directory) or
                g4.mirror.sha256(path) != record["sha256"]):
            raise ValueError("Daily candidate source changed")
        raw = morning.normalize_signals(read_frozen_csv(path))
        parts.append(raw.loc[raw.hhmm_int.eq(1010) & raw.side.eq("SHORT")].copy())
        evidence.append(dict(day=day, run_manifest=str(payload_path),
                             run_manifest_sha256=g4.mirror.sha256(payload_path),
                             candidate_path=str(path), candidate_sha256=record["sha256"]))
    if {str(day) for day in source_summary["days"][49:]} != {x["day"] for x in evidence}:
        raise ValueError("Missing appended candidate day")
    pool = pd.concat(parts, ignore_index=True, sort=False)
    if pool.duplicated(["day", "tradingsymbol"]).any():
        raise ValueError("Duplicate 10:11 candidate")
    pool["sid"] = np.arange(200_000_000, 200_000_000 + len(pool))
    audit = donor.selection_audit(pool)
    if not audit.setup_id.eq(SETUP).all():
        raise ValueError("10:11 candidate mapped to another setup")
    old_setup = replace(donor._ACTIVE_BY_ID[SETUP], price_change_pct=BASE_THRESHOLD)
    old_keys = set()
    for day, group in audit.groupby("day", sort=True):
        chosen = donor.rank_candidates(group.to_dict("records"), old_setup,
                                       session_date=pd.Timestamp(day).date())
        old_keys.update((str(day)[:10], SETUP, str(row["tradingsymbol"]))
                        for row in chosen)
    audit["selected_013"] = [key in old_keys for key in zip(
        audit.day.astype(str).str[:10], audit.setup_id, audit.tradingsymbol)]
    return audit, dict(historical=historical_proof, appended=evidence)


def _select_changed(audit, baseline_orders):
    old = baseline_orders.loc[baseline_orders.setup_id.eq(SETUP)]
    original = audit.loc[audit.selected_013.eq(True)]
    if _keys(original) != _keys(old):
        raise ValueError("Unchanged 10:11 selection does not reproduce saved G4")
    setup = replace(donor._ACTIVE_BY_ID[SETUP], price_change_pct=TEST_THRESHOLD)
    chosen = []
    for day, group in audit.groupby("day", sort=True):
        selected = donor.rank_candidates(group.to_dict("records"), setup,
                                           session_date=pd.Timestamp(day).date())
        chosen.extend(dict(row, v9_selected=True, v9_filter_pass=True,
                           v9_rank_in_setup_day=rank, v9_decision="SELECTED")
                      for rank, row in enumerate(selected, 1))
    selected = pd.DataFrame(chosen)
    if selected.empty:
        raise ValueError("No 10:11 selections under revised threshold")
    changed_keys = _keys(selected)
    old_keys = _keys(old)
    retained = old.loc[[key in changed_keys for key in zip(
        old.day.astype(str).str[:10], old.setup_id, old.tradingsymbol)]].copy()
    additions = selected.loc[[key not in old_keys for key in zip(
        selected.day.astype(str).str[:10], selected.setup_id, selected.tradingsymbol)]].copy()
    if len(additions):
        additions = additions.assign(
            native_stop_pct=1.2, stop_pct=1.2,
            initial_stop_pct=1.2, tightened_stop_pct=1.0,
            tighten_after_minutes=120,
            native_target_pct=donor._ACTIVE_BY_ID[SETUP].target_pct,
            target_pct=donor._ACTIVE_BY_ID[SETUP].target_pct)
        additions = additions.reindex(columns=baseline_orders.columns)
    other = baseline_orders.loc[baseline_orders.setup_id.ne(SETUP)].copy()
    orders = pd.concat([other, retained, additions], ignore_index=True, sort=False)
    if orders.sid.duplicated().any() or orders.duplicated(
            ["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Revised selection has duplicate identity")
    if _keys(orders.loc[orders.setup_id.ne(SETUP)]) != _keys(other):
        raise ValueError("Other G4 setup selection changed")
    audit = audit.copy()
    audit["passes_011"] = [donor.passes_selected_filters(row, setup,
        session_date=pd.Timestamp(row["day"]).date())
        for row in audit.to_dict("records")]
    audit["selected_011"] = [key in changed_keys for key in zip(
        audit.day.astype(str).str[:10], audit.setup_id, audit.tradingsymbol)]
    changes = pd.DataFrame(
        [dict(day=d, setup_id=s, tradingsymbol=t, action="ADDED")
         for d, s, t in sorted(changed_keys-old_keys)] +
        [dict(day=d, setup_id=s, tradingsymbol=t, action="REMOVED")
         for d, s, t in sorted(old_keys-changed_keys)],
        columns=["day", "setup_id", "tradingsymbol", "action"])
    return orders, audit, changes


def _report(summary, periods, changes, trades):
    cols = ["period", "sessions", "selected_orders", "trades", "wins", "losses",
            "profitable_days", "losing_days", "net_pnl_rs", "negative_trade_sum_abs_rs",
            "stop_exits", "daily_close_drawdown_rs"]
    display = lambda frame: frame.to_html(index=False, border=0,
        float_format=lambda value: f"{value:,.2f}")
    return ('<!doctype html><html><meta charset="utf-8"><title>G4 10:11 SHORT threshold</title>'
        '<style>body{font:15px/1.5 system-ui;margin:28px;color:#13243b;background:#f6f8fc}'
        'table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #ccd6e2}'
        'th{background:#e5edf8}a{color:#175bb7}</style>'
        '<h1>G4 10:11 SHORT: 0.13% versus 0.11%</h1>'
        f'<p>{summary["first_date"]} through {summary["last_date"]}; {summary["sessions"]} sessions. '
        'All other entry and exit settings remain as in the current 51-setup G4 profile. '
        'Each policy reranks the complete 10:11 candidate pool and replays the whole portfolio.</p>'
        '<h2>Overall and recent periods</h2>' + display(periods[cols + ["policy"]]) +
        '<h2>Selection changes</h2>' + display(changes) +
        '<h2>Changed 10:11 trades</h2>' + display(trades) +
        '<p><a href="daily_comparison.csv">Daywise comparison</a> | '
        '<a href="candidate_audit.csv">Candidate audit</a> | '
        '<a href="selected_orders_011.csv">Revised selected orders</a> | '
        '<a href="summary.json">Summary and provenance</a></p></html>')


def run(output, baseline_run):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(ROOT.resolve()):
        raise ValueError("Require a fresh G4 research directory")
    baseline_run = Path(baseline_run).resolve()
    if not baseline_run.is_relative_to(g4.bounded.G4_ROOT.resolve()):
        raise ValueError("Baseline is outside G4 root")
    baseline = g4.verify(baseline_run)
    if baseline["last_date"] != "2026-10-21" or baseline["active_setups"] != 51:
        raise ValueError("Require complete G4 V13 through 21 October")
    config = g4.configuration(g4.PREVIOUS_CONFIG)
    source_summary = g4.mirror.read(Path(baseline["current_g3_source_run"]) / "summary.json")
    if source_summary["days"] != baseline["days"]:
        raise ValueError("Source and G4 calendars differ")
    baseline_orders = read_frozen_csv(baseline_run / "selected_orders.csv")
    baseline_book = read_frozen_csv(baseline_run / "g4_bounded_exit" / "portfolio_trades.csv")
    audit, candidate_sources = _candidate_pool(source_summary)
    orders, audit, changes = _select_changed(audit, baseline_orders)
    print(f"Verified {len(audit)} pre-quota 10:11 SHORT candidates; "
          f"{len(changes)} changed selections", flush=True)
    revised = json.loads(json.dumps(config))
    for item in revised["active_setups"]:
        if item["setup_id"] == SETUP:
            if item["price_change_pct"] != BASE_THRESHOLD:
                raise ValueError("Baseline threshold differs")
            item["price_change_pct"] = TEST_THRESHOLD
    base_identity, changed_identity = g4.Identity(config), g4.Identity(revised)
    base_orders = base_identity.identify(baseline_orders)
    changed_orders = changed_identity.identify(orders)
    prices = g4.Prices(source_summary)
    paths = prices.paths(changed_orders.sort_values(["tradingsymbol", "day", "confirmation_ts"]))
    baseline_ids = set(baseline_orders.sid.astype(int))
    if not baseline_ids.issubset(paths):
        raise ValueError("Baseline execution path missing")
    baseline_native = g4.bounded.history._simulate(base_orders, paths,
        [date.fromisoformat(day) for day in baseline["days"]], base_identity)
    baseline_rebuilt = g4._bounded(baseline_native, paths, config)
    g4.bounded.history._same_native(baseline_rebuilt, baseline_book)
    g4.bounded.history._same_allocation(baseline_rebuilt, baseline_book)
    changed_native = g4.bounded.history._simulate(changed_orders, paths,
        [date.fromisoformat(day) for day in baseline["days"]], changed_identity)
    changed_book = g4._bounded(changed_native, paths, revised)
    periods = {"all_55": baseline["days"],
               "first_49": baseline["days"][:49],
               "appended_6": baseline["days"][49:],
               "october": [day for day in baseline["days"] if day >= "2026-10-01"],
               "2026-10-21": ["2026-10-21"]}
    period_rows = [dict(period=name, policy=policy, **g4.bounded.stats(book, days))
        for name, days in periods.items()
        for policy, book in (("baseline_013", baseline_rebuilt), ("test_011", changed_book))]
    period_results = pd.DataFrame(period_rows)
    day_rows = []
    for day in baseline["days"]:
        before = g4.bounded.stats(baseline_rebuilt, [day])
        after = g4.bounded.stats(changed_book, [day])
        day_rows.append(dict(day=day, baseline_trades=before["trades"], test_trades=after["trades"],
            baseline_wins=before["wins"], test_wins=after["wins"],
            baseline_losses=before["losses"], test_losses=after["losses"],
            baseline_net_pnl_rs=before["net_pnl_rs"], test_net_pnl_rs=after["net_pnl_rs"],
            delta_net_pnl_rs=after["net_pnl_rs"]-before["net_pnl_rs"]))
    daily = pd.DataFrame(day_rows)
    if not np.isclose(daily.delta_net_pnl_rs.sum(),
            period_results.loc[period_results.period.eq("all_55") &
                period_results.policy.eq("test_011"), "net_pnl_rs"].iloc[0] -
            period_results.loc[period_results.period.eq("all_55") &
                period_results.policy.eq("baseline_013"), "net_pnl_rs"].iloc[0],
            rtol=0, atol=1e-6):
        raise ValueError("Daily comparison does not reconcile")
    changed_days = set(changes.day.astype(str))
    selected_trades = pd.concat([
        book.loc[book.setup_id.eq(SETUP) & book.day.isin(changed_days)].assign(policy=policy)
        for policy, book in (("baseline_013", baseline_rebuilt), ("test_011", changed_book))],
        ignore_index=True, sort=False)
    trade_cols = ["policy", "day", "tradingsymbol", "setup_id", "portfolio_status",
        "entry_ts", "exit_ts", "exit_reason", "portfolio_net_profit_rupees"]
    summary = dict(schema_version=SCHEMA, scope="BACKTEST_ONLY", execution_authority=False,
        baseline_variant=g4.VERSION, first_date=baseline["first_date"],
        last_date=baseline["last_date"], sessions=baseline["sessions"],
        setup_id=SETUP, baseline_price_change_pct=BASE_THRESHOLD,
        test_price_change_pct=TEST_THRESHOLD, baseline_run=str(baseline_run),
        baseline_manifest_sha256=g4.mirror.sha256(baseline_run / "manifest.json"),
        source_g3_run=baseline["current_g3_source_run"],
        source_g3_manifest_sha256=baseline["current_g3_manifest_sha256"],
        candidate_sources=candidate_sources, selected_changes=changes.to_dict("records"),
        price_sources=prices.proof,
        period_results=period_rows,
        daily_delta_sum_rs=float(daily.delta_net_pnl_rs.sum()),
        limitations=["Retrospective threshold test on a historically researched setup.",
            "After-close minute data can differ from the live candle used at the decision time.",
            "Execution uses complete one-minute paths and model costs, not broker fills."])
    for path, proof in prices.proof.items():
        if g4.mirror.sha256(Path(path)) != proof["sha256"]:
            raise ValueError("Minute path source changed")
    if g4.mirror.sha256(baseline_run / "manifest.json") != summary["baseline_manifest_sha256"]:
        raise ValueError("Baseline changed during replay")
    output.mkdir(parents=True)
    for name, frame in {
        "candidate_audit": audit,
        "selection_changes": changes,
        "selected_orders_011": changed_orders,
        "portfolio_baseline_013": baseline_rebuilt,
        "portfolio_test_011": changed_book,
        "period_results": period_results,
        "daily_comparison": daily,
        "changed_setup_trades": selected_trades[trade_cols],
    }.items():
        frame.to_csv(output / f"{name}.csv", index=False)
    (output / "config_test_011.json").write_text(json.dumps(revised, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    (output / "report.html").write_text(_report(summary, period_results, changes,
        selected_trades[trade_cols]), encoding="utf-8")
    (output / "manifest.json").write_text(json.dumps(dict(schema_version=SCHEMA,
        files={p.relative_to(output).as_posix():g4.mirror.sha256(p)
               for p in output.iterdir() if p.is_file()}), indent=2) + "\n")
    return summary


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-run", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.baseline_run is None:
        pointer = g4.mirror.read(g4.bounded.G4_ROOT / "latest" / "bounded_exit.json")
        baseline_run = g4.bounded.G4_ROOT / pointer["run_dir"]
    else:
        baseline_run = args.baseline_run
    now = datetime.now(g4.mirror.production.IST)
    output = args.output_dir or ROOT / (f"{now:%Y%m%dT%H%M%S%f}_"
        f"{secrets.token_hex(4)}_1011_short_011")
    summary = run(output, baseline_run)
    print(json.dumps(dict(sessions=summary["sessions"], selected_changes=summary["selected_changes"],
        period_results=summary["period_results"]), indent=2, default=str))
    print(output)


if __name__ == "__main__":
    main()
