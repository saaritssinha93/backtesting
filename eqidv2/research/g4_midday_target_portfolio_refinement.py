"""Greedy portfolio refinement of the frozen midday historical target choices.

Only whole setups may be removed. Entry filters, selected orders, targets and
stops remain fixed; every candidate is reallocated with the existing baseline.
"""
from __future__ import annotations

import argparse
import html
import json
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
from research.g4_extended_slot_backtest import portfolio_changed
from research.g4_extended_target_sweep import (
    BACKTEST_ROOT, MIDDAY_VERSION, ROOT, read_frozen_csv,
)
from research.g4_late_slot_source import sha256


VERSION = "V13-V10-G-4-1135-1230-TARGET-PORTFOLIO-REFINED-V1"
DEFAULT_SOURCE = BACKTEST_ROOT / "target_sweeps/2026-10-13/20261013_midday_1135_1230_targets_10pct_v2"
DEFAULT_OUTPUT = BACKTEST_ROOT / "target_sweeps/2026-10-13/20261013_midday_targets_portfolio_refined_v1"


def qualifies(combined, added, *, minimum=5):
    """Require positive net profit and 70% wins for the portfolio and each setup."""
    if added.empty or not minimum >= 1 or added.setup_id.isna().any():
        return False
    for frame in (combined, added):
        executed = frame.loc[frame.portfolio_executed.eq(True)]
        pnl = executed.portfolio_net_profit_rupees
        if len(executed) < minimum or not np.isfinite(pnl).all() or pnl.sum() <= 0 or pnl.gt(0).mean() < .7:
            return False
    for _, frame in added.groupby("setup_id", sort=True):
        executed = frame.loc[frame.portfolio_executed.eq(True)]
        pnl = executed.portfolio_net_profit_rupees
        if len(executed) < minimum or pnl.sum() <= 0 or pnl.gt(0).mean() < .7:
            return False
    return True


def greedy_refine(target_map, evaluate):
    """Deterministically accept the best qualifying whole-setup deletion per pass."""
    current = dict(target_map)
    if not current:
        raise ValueError("The refinement needs a nonempty qualifying starting map")
    baseline = evaluate(current)
    if not baseline["eligible"]:
        raise ValueError("The starting map does not qualify on actual portfolio executions")
    trials = [dict(iteration=0, removed_setup=None, setups=len(current),
                   accepted=False, improvement_rs=0., **baseline)]
    iteration = 0
    while len(current) > 1:
        iteration += 1
        candidates = []
        for setup in sorted(current):
            candidate = {key: value for key, value in current.items() if key != setup}
            stats = evaluate(candidate)
            row = dict(iteration=iteration, removed_setup=setup, setups=len(candidate),
                accepted=False, improvement_rs=stats["combined_net_pnl_rs"] - baseline["combined_net_pnl_rs"],
                **stats)
            trials.append(row)
            if row["eligible"] and row["improvement_rs"] > 1e-6:
                candidates.append((row, candidate))
        if not candidates:
            break
        chosen, current = sorted(candidates, key=lambda pair: (
            -pair[0]["combined_net_pnl_rs"], pair[0]["removed_setup"]))[0]
        chosen["accepted"] = True
        baseline = {key: chosen[key] for key in baseline}
    return current, trials


def _write_report(output, summary, results, setup_results, trials):
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 midday target portfolio refinement</title>
<style>body{font:15px/1.5 system-ui,sans-serif;margin:30px;background:#f4f6fa;color:#172a3d}h1,h2{line-height:1.2}p{max-width:1100px}.notice{background:#fff0ce;padding:16px;border-left:4px solid #b98105}.scroll{overflow:auto;margin-bottom:28px}table{border-collapse:collapse;background:white;width:100%}td,th{padding:8px;border-bottom:1px solid #d9e0e8;white-space:nowrap;text-align:right}th{background:#213c57;color:white}td:first-child,th:first-child{text-align:left}a{color:#065dbc}</style></head><body><h1>G4 midday: portfolio refinement</h1>'''
    page += (f"<p>Started with {summary['initial_setups']} qualifying target configurations from the 11:35–12:30 study. "
             f"Reallocated each whole-setup removal with the existing baseline, retaining {summary['final_setups']} setups "
             f"after {summary['deletion_trials']} tested removals. Every retained setup has at least five executed trades, "
             "70% net wins and positive net profit in the full historical period.</p>")
    page += '<div class="notice">Targets and subset membership use all 49 historical sessions. The later 11 sessions were already used in research. These results are an in-sample fit, not an independent test. The greedy deletion search is bounded and does not establish a global optimum over every target/subset combination.</div>'
    page += '<p>Entry filters and targets are frozen from the exhaustive 0.06%–10.00% target study. Stops remain 1.25%, tightening to 1.00% after 120 minutes; confirmation, trigger expiry and 15:15 square-off are unchanged. Capital is Rs 100,000 per entry, 5× exposure and Rs 1,000,000 total, with five basis points round-trip cost. Wins include profitable time exits.</p>'
    removed = ", ".join(summary["removed_setups"]) or "None"
    page += f"<p>Removed setup IDs: {html.escape(removed)}. Existing baseline executions changed: {summary['baseline_trades_changed']}.</p>"
    cols = ["scenario", "scope", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]
    setup_cols = ["setup", "target_pct", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "observed_70_positive"]
    tables = [("Full 49-session comparison", results.loc[results.period.eq("all"), cols]),
              ("Retained setup results", setup_results.loc[setup_results.scenario.eq("refined_qualified") & setup_results.period.eq("all"), setup_cols]),
              ("Later 11-session comparison", results.loc[results.period.eq("holdout"), cols]),
              ("Accepted refinements", trials.loc[trials.accepted])]
    for title, frame in tables:
        page += f'<h2>{html.escape(title)}</h2><div class="scroll">{frame.to_html(index=False,border=0,na_rep="—",float_format=lambda x:f"{x:,.2f}")}</div>'
    page += '<p>' + ' · '.join(f'<a href="{name}">{name}</a>' for name in (
        "portfolio_results.csv", "scenario_setup_results.csv", "refinement_trials.csv", "daily_results.csv",
        "monthly_results.csv", "refined_config.json", "summary.json", "manifest.json")) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(source=DEFAULT_SOURCE, output=DEFAULT_OUTPUT, *, source_manifest_sha256):
    source, output = Path(source).resolve(), Path(output).resolve()
    if not source.is_relative_to(BACKTEST_ROOT.resolve()) or sha256(source / "manifest.json") != source_manifest_sha256:
        raise ValueError("Target source path or pinned manifest changed")
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Refinement output must be new and within the G4 backtest root")
    proof = verified_run(source, MIDDAY_VERSION)
    summary_source = json.loads((source / "summary.json").read_text(encoding="utf-8"))
    if (summary_source["setups"], summary_source["selected_orders"], summary_source["potential_fills"],
            summary_source["target_min_pct"], summary_source["target_max_pct"], summary_source["target_levels"]) != (24, 345, 287, .06, 10., 995):
        raise ValueError("Expected the completed 24-setup exhaustive 0.06%-10% study")
    reference = Path(summary_source["source"]["run_dir"])
    if sha256(reference / "manifest.json") != summary_source["source"]["manifest_sha256"]:
        raise ValueError("Frozen entry study changed")
    entry_proof = verified_run(reference, "V13-V10-G-4-BACKTEST-1135-1230-V4")
    target_map = summary_source["historical_targets"]
    orders = read_frozen_csv(reference / "selected_orders.csv")
    baseline_saved = read_frozen_csv(reference / "baseline_g4_trades.csv")
    baseline = checked_allocate(baseline_saved, baseline_saved)
    full_saved = read_frozen_csv(source / "historical_profitable70_only_trades.csv")
    full_raw = read_frozen_csv(source / "historical_profitable70_only_added_raw_trades.csv")
    cache = pd.read_parquet(source / "minute_replay_all_targets.parquet",
                           filters=[("target_pct", "in", sorted(set(target_map.values())))])
    cached = pd.concat([cache.loc[cache.setup_id.eq(setup) & cache.target_pct.eq(target)]
                       for setup, target in sorted(target_map.items())], ignore_index=True)
    same_raw(cached, full_raw)
    assert_same_entries(cached, full_raw)
    if set(cached.setup_id.unique()) != set(target_map):
        raise ValueError("Cached target map is incomplete")
    days = sorted(read_frozen_csv(source / "daily_results.csv").day.unique())
    if len(days) != 49:
        raise ValueError("Frozen session list changed")
    full = checked_allocate(pd.concat([baseline, cached], ignore_index=True), full_saved)
    evaluations = {}

    def evaluate(candidate):
        key = tuple(sorted(candidate))
        if key not in evaluations:
            if any(target_map[setup] != target for setup, target in candidate.items()):
                raise ValueError("Refinement may remove setups but cannot change frozen targets")
            raw = cached.loc[cached.setup_id.isin(candidate)].copy()
            ledger = allocate(pd.concat([baseline, raw], ignore_index=True))
            added = ledger.loc[ledger.sid.isin(raw.sid)]
            combined_stats, added_stats = _stats(ledger, days), _stats(added, days)
            evaluations[key] = dict(eligible=qualifies(ledger, added),
                combined_net_pnl_rs=combined_stats["net_pnl_rs"],
                combined_win_rate_pct=combined_stats["win_rate_pct"],
                added_net_pnl_rs=added_stats["net_pnl_rs"], added_win_rate_pct=added_stats["win_rate_pct"],
                added_executed_trades=added_stats["executed_trades"],
                baseline_trades_changed=portfolio_changed(baseline, ledger))
        return dict(evaluations[key])

    refined_map, trial_rows = greedy_refine(target_map, evaluate)
    print(f"Greedy refinement: {len(target_map)} -> {len(refined_map)} setups; {len(trial_rows)-1} deletion trials", flush=True)
    # Native minute replay is independent of the cached outcomes used in search.
    for field in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        orders[field] = pd.to_datetime(orders[field], utc=True).dt.tz_convert(contract.IST)
    original_summary = json.loads((reference / "summary.json").read_text(encoding="utf-8"))
    wanted = orders.loc[orders.setup_id.isin(refined_map)].copy()
    paths, path_proof = load_paths(wanted, original_summary)
    fresh_raw = replay_target(wanted, paths, refined_map)
    refined_cached = cached.loc[cached.setup_id.isin(refined_map)]
    same_raw(fresh_raw, refined_cached)
    assert_same_entries(fresh_raw, refined_cached)
    fresh_raw["research_exit_variant"] = VERSION
    refined = allocate(pd.concat([baseline, fresh_raw], ignore_index=True))
    added_refined = refined.loc[refined.sid.isin(fresh_raw.sid)]
    if not qualifies(refined, added_refined):
        raise ValueError("Fresh replay failed the final qualification")
    final_stats = _stats(refined, days)
    if abs(final_stats["net_pnl_rs"] - evaluate(refined_map)["combined_net_pnl_rs"]) > 1e-6:
        raise ValueError("Fresh replay and cached search disagree")
    same_raw(refined.loc[refined.sid.isin(baseline.sid)], baseline)
    output.mkdir(parents=True, exist_ok=True)
    periods = dict(train=days[:38], holdout=days[38:], all=days)
    result_rows, setup_rows, daily_rows, monthly_rows = [], [], [], []
    for name, ledger, targets in (("baseline_g4", baseline, {}), ("full_qualified", full, target_map),
                                  ("refined_qualified", refined, refined_map)):
        added = ledger.loc[ledger.setup_id.isin(targets)] if targets else ledger.iloc[:0]
        # Baseline setup IDs cannot collide with new midday setups.
        if set(baseline.setup_id) & set(targets):
            raise ValueError("Baseline and midday setup identities overlap")
        for period, cohort in periods.items():
            for scope, frame in (("combined", ledger), ("additions", added)):
                if not targets and scope == "additions":
                    continue
                result_rows.append(dict(scenario=name, scope=scope, period=period,
                    **_stats(frame.loc[frame.day.isin(cohort)], cohort)))
            for setup, target in sorted(targets.items()):
                frame = added.loc[added.setup_id.eq(setup) & added.day.isin(cohort)]
                stats = _stats(frame, cohort)
                signal = orders.loc[orders.setup_id.eq(setup)].iloc[0]
                passing = (stats["executed_trades"] >= (3 if period == "holdout" else 5)
                    and stats["win_rate_pct"] is not None and stats["win_rate_pct"] >= 70
                    and stats["net_pnl_rs"] > 0)
                setup_rows.append(dict(scenario=name, setup_id=setup, setup=f"{signal.signal_end}_{signal.side}",
                    target_pct=target, period=period, **stats, observed_70_positive=bool(passing)))
        for day in days:
            daily_rows.append(dict(scenario=name, day=day, **_stats(ledger.loc[ledger.day.eq(day)], [day]),
                added_net_pnl_rs=float(added.loc[added.day.eq(day)].portfolio_net_profit_rupees.sum())))
        for month in sorted({day[:7] for day in days}):
            cohort = [day for day in days if day.startswith(month)]
            monthly_rows.append(dict(scenario=name, month=month,
                **_stats(ledger.loc[ledger.day.isin(cohort)], cohort),
                added_net_pnl_rs=float(added.loc[added.day.isin(cohort)].portfolio_net_profit_rupees.sum())))
        ledger.to_csv(output / f"{name}_trades.csv", index=False)
    fresh_raw.to_csv(output / "refined_added_raw_trades.csv", index=False)
    results, setups, trials = pd.DataFrame(result_rows), pd.DataFrame(setup_rows), pd.DataFrame(trial_rows)
    for filename, frame in (("portfolio_results", results), ("scenario_setup_results", setups),
                            ("refinement_trials", trials), ("daily_results", pd.DataFrame(daily_rows)),
                            ("monthly_results", pd.DataFrame(monthly_rows))):
        frame.to_csv(output / f"{filename}.csv", index=False)
    source_configs = json.loads((source / "scenario_configs.json").read_text(encoding="utf-8"))
    portable = [setup for setup in source_configs["scenarios"]["historical_profitable70_only"]
                if setup["setup_id"] in refined_map]
    if {setup["setup_id"]: setup["target_pct"] for setup in portable} != refined_map:
        raise ValueError("Portable refinement config changed targets or membership")
    (output / "refined_config.json").write_text(json.dumps(dict(variant_id=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, source=proof, fixed_policy=source_configs["fixed_policy"],
        setups=portable, selection_policy="GREEDY_WHOLE_SETUP_DELETION_MAX_COMBINED_NET_PNL_EVERY_SETUP_70"),
        indent=2) + "\n", encoding="utf-8")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), source=proof, entry_source=entry_proof,
        sessions=49, training_sessions=38, later_sessions=11, first_date=days[0], last_date=days[-1],
        initial_setups=len(target_map), final_setups=len(refined_map), deletion_trials=len(trial_rows)-1,
        accepted_deletions=int(trials.loc[trials.iteration.gt(0), "accepted"].sum()),
        removed_setups=sorted(set(target_map)-set(refined_map)), targets=refined_map,
        initial=evaluate(target_map), final=evaluate(refined_map),
        net_pnl_improvement_rs=evaluate(refined_map)["combined_net_pnl_rs"]-evaluate(target_map)["combined_net_pnl_rs"],
        baseline_trades_changed=portfolio_changed(baseline, refined), minute_path_evidence=path_proof,
        every_remaining_setup_70_positive_actual_executions=True,
        fresh_native_replay_matches_cache=True, entries_and_targets_unchanged=True,
        historical_selection_is_in_sample=True, later_period_previously_used=True, untouched_out_of_sample=False,
        optimum_scope="GREEDY_DELETE_ONE_SETUP_AT_A_TIME_FIXED_TARGETS; LOCAL_SUBSET_OPTIMUM_NOT_GLOBAL",
        all_scenarios=json.loads(results.to_json(orient="records")))
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    _write_report(output, summary, results, setups, trials)
    modules = [Path(__file__), ROOT / "research/g4_extended_target_sweep.py",
        ROOT / "research/g3_g4_late_target_sweep.py", ROOT / "research/g3_g4_late_combined_backtest.py",
        ROOT / "research/g3_0920_backtest.py", Path(simulator.__file__),
        Path(simulator.g.v9.v5.__file__), Path(simulator.g.v9.v6.__file__)]
    manifest = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        source=proof, code_sha256={str(path.relative_to(ROOT)): sha256(path) for path in modules},
        files={path.name: sha256(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: summary[key] for key in ("removed_setups", "net_pnl_improvement_rs", "initial", "final")}, indent=2), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--source-manifest-sha256", required=True)
    args = parser.parse_args()
    run(args.source, args.output, source_manifest_sha256=args.source_manifest_sha256)


if __name__ == "__main__":
    main()
