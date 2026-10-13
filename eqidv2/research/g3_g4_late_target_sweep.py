"""Target-only sweep of the twelve frozen G4 late setups against G3 V3.

Run with python -m research.g3_g4_late_target_sweep. Historical-fit winners are
reported separately from targets chosen using only the first 38 sessions.
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
import fno_v13_v10_g_3_contract as contract
from research.g3_0920_backtest import allocate, _stats, _config
from research.g3_0920_target_sweep import assert_same_entries
from research.g3_g4_late_combined_backtest import (
    G3_RUN, G4_RUN, ROOT, verified_run, sha256, same_raw, checked_allocate,
)
from research.g4_late_slot_source import _archive_paths

VERSION = "G3_V3_G4_LATE_12_TARGET_SWEEP_V1"
TARGETS = tuple(round(n / 100, 2) for n in range(6, 501))


def load_paths(orders, summary):
    evidence = summary["source_evidence"]
    manifest_path = Path(evidence["sealed_dataset_manifest"])
    if sha256(manifest_path) != evidence["sealed_dataset_manifest_sha256"]:
        raise ValueError("Sealed source manifest changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sealed_days = set(manifest["days"])
    archive = manifest_path.parent / "paths.npz"
    if sha256(archive) != manifest["output_sha256"]["paths.npz"]:
        raise ValueError("Sealed minute paths changed")
    paths = _archive_paths(archive, set(orders.loc[orders.day.isin(sealed_days)].sid.astype(int)))
    proofs = [{"path": str(archive), "sha256": manifest["output_sha256"]["paths.npz"]}]
    cache = Path(evidence["late_cache"])
    for day, digest in evidence["recent_day_manifests"].items():
        path = cache / day / "manifest.json"
        if sha256(path) != digest:
            raise ValueError(f"Late source manifest changed: {day}")
        value = json.loads(path.read_text(encoding="utf-8"))
        archive = path.parent / "paths.npz"
        if value.get("complete") is not True or sha256(archive) != value["paths_sha256"]:
            raise ValueError(f"Late minute paths changed: {day}")
        required = set(orders.loc[orders.day.eq(day)].sid.astype(int))
        loaded = _archive_paths(archive, required)
        if set(paths) & set(loaded):
            raise ValueError("Minute path IDs overlap")
        paths.update(loaded)
        proofs.append({"path": str(archive), "sha256": value["paths_sha256"]})
    if set(paths) != set(orders.sid.astype(int)):
        raise ValueError("Selected minute paths incomplete")
    simulator.g.v9.validate_paths(orders, paths)
    return paths, proofs


def replay_target(orders, paths, targets):
    work = orders.copy()
    work["native_target_pct"] = work.setup_id.map(targets) if isinstance(targets, dict) else targets
    work["target_pct"] = work.native_target_pct
    if work.native_target_pct.isna().any():
        raise ValueError("Missing target")
    return simulator.simulate_staged(work, paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
                                     max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)


def raw_stats(raw, days):
    frame = raw.loc[raw.filled.eq(True) & raw.day.isin(days)]
    pnl = frame.net_return_pct.to_numpy(float) * contract.TARGET_EXPOSURE_RS / 100
    daily = pd.Series(pnl, index=frame.day).groupby(level=0).sum().reindex(days, fill_value=0)
    curve = np.r_[0., daily.cumsum().to_numpy(float)]
    return dict(fills=len(frame), wins=int((pnl > 0).sum()), losses=int((pnl < 0).sum()),
                win_rate_pct=float((pnl > 0).mean() * 100) if len(frame) else None,
                net_pnl_rs=float(pnl.sum()), max_drawdown_rs=float((np.maximum.accumulate(curve)-curve).max()))


def choose(rows, period, minimum):
    eligible = [r for r in rows if r[f"{period}_fills"] >= minimum
                and r[f"{period}_win_rate_pct"] is not None and r[f"{period}_win_rate_pct"] >= 70]
    return max(eligible, key=lambda r: (round(r[f"{period}_net_pnl_rs"], 6),
                 -round(r[f"{period}_max_drawdown_rs"], 6), r[f"{period}_win_rate_pct"],
                 -r["target_pct"])) if eligible else None


def run(output):
    output = Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Output must be new or empty")
    sources = dict(g3=verified_run(G3_RUN, "V13-V10-G-3-BT-0920-LONG-T300-V1"),
                   g4=verified_run(G4_RUN, "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1"))
    baseline_saved = pd.read_csv(G3_RUN / "variant_trades.csv")
    late_saved = pd.read_csv(G4_RUN / "g4_added_trades.csv")
    orders = pd.read_csv(G4_RUN / "g4_added_orders.csv")
    g4_summary = json.loads((G4_RUN / "summary.json").read_text(encoding="utf-8"))
    days = sorted(pd.read_csv(G4_RUN / "daily_results.csv").day.unique())
    if len(days) != 49 or len(orders) != 89 or orders.setup_id.nunique() != 12:
        raise ValueError("Frozen session/selection cohort changed")
    for field in ("confirmation_ts", "signal_ts", "v9_1m_feature_ts"):
        orders[field] = pd.to_datetime(orders[field], utc=True).dt.tz_convert(contract.IST)
    paths, path_proofs = load_paths(orders, g4_summary)
    original_targets = orders.groupby("setup_id").native_target_pct.first().to_dict()
    reference = replay_target(orders, paths, original_targets)
    same_raw(reference, late_saved)
    checked_allocate(reference, late_saved)
    baseline = checked_allocate(baseline_saved, baseline_saved)
    # A target can change only exits. Even if every fill stays open all day,
    # nine fills/day fit into the ten-entry capital budget; no other cap exists.
    maximum_daily_fills = int(pd.concat((baseline_saved, late_saved)).loc[
        lambda x: x.filled.eq(True)].groupby("day").size().max())
    portfolio = _config().portfolio_config()
    if (maximum_daily_fills * contract.CAPITAL_PER_ENTRY_RS > contract.PORTFOLIO_CAPITAL_RS
            or any(getattr(portfolio, key) is not None for key in (
                "max_positions", "max_positions_per_symbol", "max_gross_exposure_rupees", "max_open_risk_rupees"))):
        raise ValueError("Independent setup optimization requires full portfolio enumeration")
    print(f"Verified reference replay: {len(orders)} selections, {int(reference.filled.sum())} fills; "
          f"maximum {maximum_daily_fills} fills/day fits capital", flush=True)
    periods = {"train": days[:38], "holdout": days[38:], "all": days}
    rows, raw_by_target = [], {}
    for index, target in enumerate(TARGETS, 1):
        raw = replay_target(orders, paths, target)
        assert_same_entries(raw, reference)
        raw_by_target[target] = raw
        for setup, frame in raw.groupby("setup_id", sort=True):
            signal = frame.signal_end.iloc[0]
            record = dict(setup=f"{signal}_{frame.side.iloc[0]}", setup_id=setup, target_pct=target)
            for period, cohort in periods.items():
                record.update({f"{period}_{key}": value for key, value in raw_stats(frame, cohort).items()})
            rows.append(record)
        if index % 25 == 0 or index == len(TARGETS):
            print(f"Replayed {index}/{len(TARGETS)} target levels", flush=True)
    table = pd.DataFrame(rows)
    choices, historical, train_map, training_eligible, historical_pass = [], {}, {}, {}, {}
    for setup in sorted(original_targets):
        trials = [r for r in rows if r["setup_id"] == setup]
        training = choose(trials, "train", 5)
        # Lock the training choice before evaluating its holdout pass.
        train_map[setup] = training["target_pct"] if training else original_targets[setup]
        if training:
            training_eligible[setup] = training["target_pct"]
        observed = choose(trials, "all", 1)
        fallback = max(trials, key=lambda r: (r["all_win_rate_pct"], round(r["all_net_pnl_rs"], 6), -r["target_pct"]))
        chosen = observed or fallback
        historical[setup] = chosen["target_pct"]
        if observed:
            historical_pass[setup] = chosen["target_pct"]
        strict = bool(training and training["holdout_fills"] >= 3
                      and training["holdout_win_rate_pct"] >= 70 and training["all_win_rate_pct"] >= 70)
        choices.append(dict(setup=chosen["setup"], setup_id=setup,
            original_target_pct=original_targets[setup], historical_target_pct=chosen["target_pct"],
            historical_70_pass=observed is not None, historical_small_sample=chosen["all_fills"] < 5,
            historical_wins=chosen["all_wins"], historical_fills=chosen["all_fills"],
            historical_win_rate_pct=chosen["all_win_rate_pct"], historical_net_pnl_rs=chosen["all_net_pnl_rs"],
            historical_holdout_wins=chosen["holdout_wins"], historical_holdout_fills=chosen["holdout_fills"],
            historical_holdout_win_rate_pct=chosen["holdout_win_rate_pct"],
            train_selected_target_pct=training["target_pct"] if training else None,
            train_wins=training["train_wins"] if training else None,
            train_fills=chosen["train_fills"],
            train_selected_holdout_wins=training["holdout_wins"] if training else None,
            holdout_fills=chosen["holdout_fills"],
            train_selected_holdout_win_rate_pct=training["holdout_win_rate_pct"] if training else None,
            strict_qualification_pass=strict,
            training_status="TRAINING_70_PASS" if training else (
                "INSUFFICIENT_TRAINING_FILLS" if chosen["train_fills"] < 5 else "NO_TRAINING_TARGET_REACHED_70")))
    profitable_pass = {c["setup_id"]: c["historical_target_pct"] for c in choices
                       if c["historical_70_pass"] and c["historical_net_pnl_rs"] > 0}
    plans = {"original_all_12": original_targets, "historical_best_all_12": historical,
             "historical_70_setups_only": historical_pass,
             "historical_profitable_70_only": profitable_pass,
             "training_selected_all_12": train_map, "training_70_setups_only": training_eligible}
    scenario_summaries, ledgers, daily_rows, scenario_setup_rows = {}, {}, [], []
    for name, targets in plans.items():
        parts = [raw_by_target[target].loc[raw_by_target[target].setup_id.eq(setup)]
                 for setup, target in targets.items()]
        late = pd.concat(parts, ignore_index=True) if parts else reference.iloc[:0].copy()
        combined = allocate(pd.concat((baseline, late), ignore_index=True, sort=False))
        added = combined.loc[combined.sid.isin(orders.sid)]
        old = combined.loc[combined.sid.isin(baseline.sid)]
        same_raw(old, baseline)
        if (not old.portfolio_executed.eq(old.filled).all()
                or not combined.portfolio_executed.eq(combined.filled).all()):
            raise ValueError("Unexpected capital rejection invalidates independent optimization")
        metric = _stats(combined, days)
        base_pnl = _stats(baseline, days)["net_pnl_rs"]
        if not np.isclose(metric["net_pnl_rs"]-base_pnl, _stats(added, days)["net_pnl_rs"], atol=1e-6):
            raise ValueError("Combined P&L reconciliation failed")
        scenario_summaries[name] = dict(targets_pct=targets, combined=metric, added=_stats(added, days),
            delta_vs_g3_rs=metric["net_pnl_rs"]-base_pnl, periods={period: {
                "combined": _stats(combined.loc[combined.day.isin(cohort)], cohort),
                "added": _stats(added.loc[added.day.isin(cohort)], cohort)} for period, cohort in periods.items()})
        ledgers[name] = combined
        for day in days:
            daily_rows.append(dict(scenario=name, day=day, **_stats(combined.loc[combined.day.eq(day)], [day])))
        for setup, part in added.groupby("setup_id"):
            scenario_setup_rows.append(dict(scenario=name, setup_id=setup, target_pct=targets[setup], **_stats(part, days)))
    summary = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=49, first_date=days[0], last_date=days[-1],
        training_sessions=38, holdout_sessions=11, target_grid_pct=list(TARGETS),
        target_searches=len(rows), selected_orders=len(orders), reference_fills=int(reference.filled.sum()),
        objective="MAXIMUM_NET_PNL_SUBJECT_TO_AT_LEAST_70_PERCENT_NET_WIN_RATE_WITHIN_TESTED_GRID",
        historical_selection="ALL_49_SESSIONS_IN_SAMPLE; NOT_AN_INDEPENDENT_VALIDATION",
        training_selection="FIRST_38_SESSIONS_ONLY; MINIMUM_5_FILLS; MAX_NET_PNL_AMONG_70_PERCENT_TARGETS",
        strict_qualification="TRAINING>=5_FILLS_AND_70_PERCENT; HOLDOUT>=3_FILLS_AND_70_PERCENT; FULL>=70_PERCENT",
        previously_inspected_holdout=True, prospective_validation=False,
        capital=dict(per_entry_rs=contract.CAPITAL_PER_ENTRY_RS, exposure_rs=contract.TARGET_EXPOSURE_RS,
                     portfolio_rs=contract.PORTFOLIO_CAPITAL_RS, maximum_daily_fills=maximum_daily_fills),
        fixed_exit=dict(initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
                        square_off="15:15", cost_bps=contract.ROUND_TRIP_COST_BPS),
        baseline=_stats(baseline, days), scenarios=scenario_summaries, choices=choices,
        historical_70_setup_count=len(historical_pass), historical_profitable_70_setup_count=len(profitable_pass),
        training_70_setup_count=len(training_eligible),
        strict_qualified_setup_count=sum(c["strict_qualification_pass"] for c in choices),
        baseline_opening_oi_is_retrospective=True, excluded_session=g4_summary["excluded_session"],
        sources=sources, minute_path_evidence=path_proofs, reference_replay_parity=True)
    output.mkdir(parents=True, exist_ok=True)
    table.to_csv(output / "target_sweep.csv", index=False)
    pd.DataFrame(choices).to_csv(output / "best_targets.csv", index=False)
    pd.DataFrame(daily_rows).to_csv(output / "daily_results.csv", index=False)
    pd.DataFrame(scenario_setup_rows).to_csv(output / "scenario_setup_results.csv", index=False)
    for name, ledger in ledgers.items():
        ledger.to_csv(output / f"{name}_trades.csv", index=False)
    raw_columns = ["sid", "day", "signal_id", "setup_id", "side", "tradingsymbol", "native_target_pct",
                   "filled", "entry_price", "entry_ts", "exit_price", "exit_ts", "exit_reason", "net_return_pct"]
    pd.concat([r[raw_columns] for r in raw_by_target.values()], ignore_index=True).to_csv(
        output / "minute_replay_all_targets.csv", index=False)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    write_report(output, summary)
    code = [Path(__file__), Path(simulator.__file__), Path(simulator.g.v9.v5.__file__),
            Path(simulator.g.v9.v6.__file__)]
    manifest = dict(version=VERSION, execution_authority=False,
                    code_sha256={str(p.resolve()): sha256(p) for p in code},
                    files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"historical_70": len(historical_pass), "strict_qualified": summary["strict_qualified_setup_count"],
                      "combined_historical_best": scenario_summaries["historical_best_all_12"]["combined"]}, indent=2), flush=True)
    print(output, flush=True)
    return summary


def write_report(output, summary):
    comparison = [{"Scenario": "G3 V3 baseline", **summary["baseline"]}]
    comparison += [{"Scenario": name, **info["combined"]} for name, info in summary["scenarios"].items()]
    portfolio_columns = ["Scenario", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]
    choices = pd.DataFrame(summary["choices"])
    historical_columns = ["setup", "original_target_pct", "historical_target_pct", "historical_wins",
                          "historical_fills", "historical_win_rate_pct", "historical_net_pnl_rs", "historical_70_pass"]
    validation_columns = ["setup", "train_selected_target_pct", "train_wins", "train_fills",
                          "train_selected_holdout_wins", "holdout_fills", "train_selected_holdout_win_rate_pct",
                          "strict_qualification_pass"]
    tables = [("Portfolio comparison", pd.DataFrame(comparison)[portfolio_columns]),
              ("Best historical targets (fitted on all 49 sessions)", choices[historical_columns]),
              ("Targets selected on training data and checked on holdout", choices[validation_columns]),
              ("Daily results by scenario", pd.read_csv(output / "daily_results.csv"))]
    intro = ("Target-only sweep, 0.06% to 5.00% in 0.01% steps; fixed entries and staged stops. "
             "Best means maximum net profit subject to 70% net winning trades. "
             "Historical best uses all 49 sessions and is an in-sample fit. Training choices use only the first 38 sessions. "
             "The later 11 sessions have already been inspected in earlier research; they are not fresh prospective validation. "
             "If no training-qualified target exists, the original target is retained in the training_selected_all_12 diagnostic. "
             "A historical setup without a 70% target uses its maximum-win-rate target in historical_best_all_12, and is omitted from historical_70_setups_only.")
    lines = ["# G3 V3 + G4 twelve setups: target sweep", "", intro, "",
             f"Historical 70% setups: {summary['historical_70_setup_count']}/12; original strict qualification: {summary['strict_qualified_setup_count']}/12.", ""]
    page = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>G3 late setups target sweep</title><style>body{font:15px/1.5 system-ui;margin:28px;background:#101827;color:#eef3fa}p{max-width:1100px;color:#c4d0df}table{border-collapse:collapse;background:#1b293b}td,th{padding:8px;border-bottom:1px solid #41516a;text-align:right;white-space:nowrap}th{background:#29405a;position:sticky;top:0}.scroll{overflow:auto;max-height:550px;margin-bottom:30px}a{color:#a5d6ff}</style></head><body>')
    page += '<h1>G3 V3 + G4: individual target sweep</h1><p>' + html.escape(intro) + '</p>'
    for title, frame in tables:
        page += f'<h2>{html.escape(title)}</h2><div class="scroll">{frame.to_html(index=False, border=0, na_rep="—", float_format=lambda x: f"{x:,.2f}")}</div>'
    page += '<p>Amounts use Rs 100,000 per entry, 5x exposure, Rs 1,000,000 portfolio and 5 bps round-trip cost. October 1 is excluded. Baseline 09:20 opening OI remains retrospective.</p></body></html>'
    for choice in summary["choices"]:
        lines.append(f"- {choice['setup']}: historical target {choice['historical_target_pct']:.2f}%; "
                     f"{choice['historical_wins']}/{choice['historical_fills']} wins "
                     f"({choice['historical_win_rate_pct']:.2f}%); net Rs {choice['historical_net_pnl_rs']:,.2f}; "
                     f"historical pass={choice['historical_70_pass']}; strict pass={choice['strict_qualification_pass']}.")
    (output / "report.html").write_text(page, encoding="utf-8")
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    folder = args.output_dir or ROOT / "backtests/runs/2026-10-12" / (
        datetime.now(contract.IST).strftime("%Y%m%dT%H%M%S%f") + "_late_12_target_sweep")
    run(folder)
