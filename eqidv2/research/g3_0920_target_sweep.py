"""Backtest-only LONG/SHORT target sweep on the fixed best 09:20 entry profile.

Reselect the sealed dated universe, replay real minute paths with staged stops,
and reallocate the existing portfolio at every target pair. Historical target
rankings are exploratory; source availability is retrospective.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_parameter_search as coarse
from research import g3_0920_joint_70_search as joint
from research.g3_0920_strategy import (
    FiveMinuteSelection, OneMinuteSelection, SelectionProfile, SideSelectionProfile,
    selection_audit_0920,
)

SEARCH_VERSION = "G3_0920_FIXED_ENTRY_LONG_SHORT_TARGET_SWEEP_V1"
DEFAULT_REFERENCE = Path(RUNTIME_ROOT) / "backtesting_result_v13_v10_g_3" / "research_0920" / "runs" / "2026-10-12" / "joint70_target1_20261012T2355"
TARGETS = tuple(sorted({round(n / 100, 2) for n in range(10, 301, 5)} | {.97, 2.67}))


def read_verified_reference(folder):
    folder = Path(folder).resolve()
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("search_version") != joint.SEARCH_VERSION or manifest.get("execution_authority") is not False:
        raise ValueError("Expected the sealed target-1% joint research run")
    if not {"summary.json", "best_observed_70_orders.csv", "best_observed_70_variant_trades.csv"}.issubset(manifest["files"]):
        raise ValueError("Reference manifest omits required selection artifacts")
    for name, digest in manifest["files"].items():
        if Path(name).name != name or paired.sha256(folder / name) != digest:
            raise ValueError(f"Changed reference artifact: {name}")
    summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    chosen = summary["winners"]["best_observed_70"]
    if chosen is None:
        raise ValueError("Reference has no fixed best-observed entry profile")
    sides = [SideSelectionProfile(FiveMinuteSelection(**chosen["profile"][side]["five_minute"]),
                                  OneMinuteSelection(**chosen["profile"][side]["one_minute"]))
             for side in ("long", "short")]
    return summary, SelectionProfile(*sides)


def apply_targets(orders, long_target, short_target):
    values = (long_target, short_target)
    if any(not np.isfinite(value) or not .10 <= value <= 3.0 for value in values):
        raise ValueError("Target percentages must be finite and within 0.10 to 3.00")
    if not orders.side.isin(("LONG", "SHORT")).all():
        raise ValueError("Unexpected side in research orders")
    result = orders.copy()
    result["native_target_pct"] = result.side.map({"LONG": long_target, "SHORT": short_target})
    result["target_pct"] = result.native_target_pct
    result["research_exit_variant"] = SEARCH_VERSION
    return result


def assert_same_entries(actual, expected):
    left, right = (frame.set_index("sid").sort_index() for frame in (actual, expected))
    if not left.index.equals(right.index) or not left.signal_id.equals(right.signal_id):
        raise RuntimeError("Target sweep changed order identity")
    for col in ("filled", "entry_path_index", "entry_price", "entry_ts"):
        a, b = left[col], right[col]
        if col == "entry_ts":
            equal = pd.to_datetime(a, utc=True, errors="coerce").equals(pd.to_datetime(b, utc=True, errors="coerce"))
        elif col in ("entry_path_index", "entry_price"):
            equal = np.allclose(pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce"),
                                rtol=0, atol=1e-9, equal_nan=True)
        else:
            equal = a.astype(str).equals(b.astype(str))
        if not equal:
            raise RuntimeError(f"Target sweep changed entry field: {col}")


def periods_for(baseline_stats, variant, added_ids, days):
    parts = {"discovery": [day for day in days if day <= coarse.DISCOVERY_LAST],
             "holdout": [day for day in days if day >= coarse.HOLDOUT_FIRST], "all": days}
    added = variant.loc[variant.sid.isin(added_ids)]
    result = {}
    for label, subset in parts.items():
        combined = coarse._period_stats(variant, subset)
        result[label] = dict(baseline=baseline_stats[label], combined=combined,
                             added_0920=coarse._period_stats(added, subset),
                             added_long=coarse._period_stats(added.loc[added.side.eq("LONG")], subset),
                             added_short=coarse._period_stats(added.loc[added.side.eq("SHORT")], subset),
                             incremental_pnl_rs=combined["net_pnl_rs"] - baseline_stats[label]["net_pnl_rs"])
    return result


def pair_passes(period, *, minimum=10, each_side=False, side_minimum=1):
    added = period["added_0920"]
    ok = (added["executed_trades"] >= minimum and added["win_rate_pct"] is not None
          and added["win_rate_pct"] >= 70 and period["combined"]["win_rate_pct"] >= 70
          and period["incremental_pnl_rs"] > 0)
    if each_side:
        ok = ok and all(period[key]["executed_trades"] >= side_minimum
                        and period[key]["win_rate_pct"] is not None
                        and period[key]["win_rate_pct"] >= 70
                        and period[key]["net_pnl_rs"] > 0 for key in ("added_long", "added_short"))
    return bool(ok)


def choose_pair(trials, period="all", *, minimum=10, each_side=False, side_minimum=1):
    eligible = [trial for trial in trials if pair_passes(trial["periods"][period], minimum=minimum,
                                                        each_side=each_side, side_minimum=side_minimum)]
    return max(eligible, key=lambda trial: (
        round(trial["periods"][period]["incremental_pnl_rs"], 6),
        -round(trial["periods"][period]["combined"]["max_drawdown_rs"], 6),
        trial["periods"][period]["added_0920"]["win_rate_pct"],
        -abs(trial["long_target_pct"] - 1) - abs(trial["short_target_pct"] - 1),
        -trial["long_target_pct"], -trial["short_target_pct"])) if eligible else None


def choose_side(trials, side, period="all", *, minimum=5, require_70=True):
    key = "added_" + side.lower()
    # Side sweep holds the opposite side at the current 1% reference.
    opposite = "short_target_pct" if side == "LONG" else "long_target_pct"
    own = "long_target_pct" if side == "LONG" else "short_target_pct"
    candidates = [trial for trial in trials if trial[opposite] == 1.0
                  and trial["periods"][period][key]["executed_trades"] >= minimum]
    if require_70:
        candidates = [trial for trial in candidates
                      if trial["periods"][period][key]["win_rate_pct"] is not None
                      and trial["periods"][period][key]["win_rate_pct"] >= 70
                      and trial["periods"][period][key]["net_pnl_rs"] > 0]
    return max(candidates, key=lambda trial: (
        round(trial["periods"][period][key]["net_pnl_rs"], 6),
        -round(trial["periods"][period][key]["max_drawdown_rs"], 6),
        trial["periods"][period][key]["win_rate_pct"], -abs(trial[own] - 1), -trial[own])) if candidates else None


def flatten(trial):
    row = {key: trial[key] for key in ("long_target_pct", "short_target_pct")}
    for period, detail in trial["periods"].items():
        row[period + "_incremental_pnl_rs"] = detail["incremental_pnl_rs"]
        for name in ("added_0920", "added_long", "added_short", "combined"):
            for field in ("executed_trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"):
                row[f"{period}_{name}_{field}"] = detail[name][field]
    row.update(full_period_pooled_70=pair_passes(trial["periods"]["all"]),
               full_period_both_sides_observed_70=pair_passes(trial["periods"]["all"], each_side=True),
               full_period_both_sides_min5_70=pair_passes(trial["periods"]["all"], each_side=True, side_minimum=5))
    return row


def run(reference, output_dir, *, pointer_path=paired.DEFAULT_POINTER, data_root=RUNTIME_ROOT):
    reference, output_dir = Path(reference).resolve(), Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Research output must be new or empty: {output_dir}")
    source_summary, profile = read_verified_reference(reference)
    code = [Path(__file__).resolve(), Path(paired.__file__), Path(joint.__file__), Path(coarse.__file__),
            Path(contract.__file__), Path(simulator.__file__), Path(__file__).with_name("g3_0920_strategy.py")]
    code_hashes = {p.name: paired.sha256(p) for p in code}
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                targets_pct=TARGETS, pair_count=len(TARGETS) ** 2, fixed_entry_profile=asdict(profile),
                source_reference=str(reference), source_manifest_sha256=paired.sha256(reference / "manifest.json"),
                initial_stop_pct=contract.INITIAL_STOP_PCT, tightened_stop_pct=contract.TIGHTENED_STOP_PCT,
                tighten_after_minutes=contract.TIGHTEN_AFTER_MINUTES, square_off=contract.SQUARE_OFF,
                target_win_rate_pct=70, code_sha256=code_hashes)
    (output_dir / "search_plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    published, days, baseline_evidence = paired.load_published(pointer_path)
    if baseline_evidence["pointer_sha256"] != source_summary["baseline_evidence"]["pointer_sha256"]:
        raise RuntimeError("Published baseline differs from the fixed entry search")
    features_path = Path(source_summary["feature_path"])
    if paired.sha256(features_path) != source_summary["feature_sha256"]:
        raise RuntimeError("Fixed-entry feature export changed")
    all_features, coverage, feature_evidence = paired.load_feature_evidence(features_path)
    universe_evidence = paired.validate_feature_universe(all_features, data_root, coverage)
    features = all_features.loc[all_features.session_date.astype(str).isin({str(d) for d in days})].copy()
    audit = selection_audit_0920(features, profile=profile)
    orders = audit.loc[audit.v9_selected].reset_index(drop=True)
    expected_orders = pd.read_csv(reference / "best_observed_70_orders.csv")
    keys = ["sid", "signal_id", "v10_g_f_core", "v9_rank_in_setup_day"]
    pd.testing.assert_frame_equal(orders[keys], expected_orders[keys], check_dtype=False)
    if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
        raise RuntimeError("Research identities overlap baseline")
    paths, path_evidence = paired.new_execution_paths(orders, Path(data_root))
    print(f"Fixed {len(orders)} entries: {orders.side.value_counts().to_dict()}; "
          f"{len(TARGETS)} targets per side / {len(TARGETS) ** 2} target pairs", flush=True)
    raw_by_target = {}
    for index, target in enumerate(TARGETS, 1):
        trial_orders = apply_targets(orders, target, target)
        raw = simulator.simulate_staged(trial_orders, paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
                                        max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
        raw["scope"], raw["execution_authority"] = "BACKTEST_ONLY", False
        if raw_by_target:
            assert_same_entries(raw, next(iter(raw_by_target.values())))
        if not raw.native_target_pct.eq(target).all():
            raise RuntimeError("Simulator target drift")
        raw_by_target[target] = raw
        if index % 10 == 0 or index == len(TARGETS):
            print(f"Simulated target {index}/{len(TARGETS)}: {target:.2f}%", flush=True)
    baseline = paired.allocate(published)
    day_sets = {"discovery": [d for d in days if d <= coarse.DISCOVERY_LAST],
                "holdout": [d for d in days if d >= coarse.HOLDOUT_FIRST], "all": days}
    baseline_stats = {label: coarse._period_stats(baseline, subset) for label, subset in day_sets.items()}
    added_ids = set(orders.sid.astype(int))
    source_1pct = pd.read_csv(reference / "best_observed_70_variant_trades.csv")

    def replay_pair(long_target, short_target):
        raw = pd.concat((raw_by_target[long_target].loc[raw_by_target[long_target].side.eq("LONG")],
                         raw_by_target[short_target].loc[raw_by_target[short_target].side.eq("SHORT")]),
                        ignore_index=True).sort_values(["day", "hhmm_int", "side", "tradingsymbol"], kind="stable")
        return paired.allocate(pd.concat((published, raw), ignore_index=True, sort=False))

    paired._same_replayed_baseline(source_1pct, replay_pair(1.0, 1.0))
    print("Reference 1% portfolio parity passed", flush=True)
    trials = []
    for long_target in TARGETS:
        for short_target in TARGETS:
            variant = replay_pair(long_target, short_target)
            detail = periods_for(baseline_stats, variant, added_ids, days)
            # Raw old outcomes are unchanged; portfolio changes must reconcile.
            old = variant.loc[~variant.sid.isin(added_ids)]
            old_delta = float(old.portfolio_net_profit_rupees.sum() - baseline.portfolio_net_profit_rupees.sum())
            if not np.isclose(detail["all"]["incremental_pnl_rs"],
                              detail["all"]["added_0920"]["net_pnl_rs"] + old_delta, rtol=0, atol=1e-5):
                raise RuntimeError("Target-pair incremental P&L did not reconcile")
            trials.append(dict(long_target_pct=long_target, short_target_pct=short_target,
                               old_trade_net_change_rs=old_delta, periods=detail))
        print(f"Portfolio sweep {len(trials)}/{len(TARGETS)**2}: LONG target {long_target:.2f}%", flush=True)
    winners = {
        "best_pair_pooled_70": choose_pair(trials),
        "best_pair_both_sides_observed_70": choose_pair(trials, each_side=True),
        "best_pair_both_sides_min5_70": choose_pair(trials, each_side=True, side_minimum=5),
        "best_discovery_pair_min10_70": choose_pair(trials, "discovery"),
        "diagnostic_discovery_pair_small_sample_70": choose_pair(trials, "discovery", minimum=1),
        "best_long_min5_70": choose_side(trials, "LONG"),
        "best_short_min5_70": choose_side(trials, "SHORT"),
        "best_short_small_sample_70": choose_side(trials, "SHORT", minimum=1),
        "best_long_net_unconstrained": choose_side(trials, "LONG", minimum=1, require_70=False),
        "best_short_net_unconstrained": choose_side(trials, "SHORT", minimum=1, require_70=False),
    }
    # Save full ledger checks for every finalist, not just the winning sum.
    for label, trial in winners.items():
        if trial is None:
            continue
        long_target, short_target = trial["long_target_pct"], trial["short_target_pct"]
        variant = replay_pair(long_target, short_target)
        comparison, daily = paired.compare_ledgers(baseline, variant, days, days, added_ids)
        trial["old_trade_effect"] = comparison["old_trade_effect"]
        variant.to_csv(output_dir / (label + "_variant_trades.csv"), index=False)
        daily.to_csv(output_dir / (label + "_daily_comparison.csv"), index=False)
        apply_targets(orders, long_target, short_target).to_csv(output_dir / (label + "_orders.csv"), index=False)
    reference_trial = next(t for t in trials if t["long_target_pct"] == t["short_target_pct"] == 1.)
    summary = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
                   first_date=str(days[0]), last_date=str(days[-1]), targets_pct=TARGETS,
                   pair_count=len(trials), selected_entry_counts={k: int(v) for k, v in orders.side.value_counts().items()},
                   fixed_entry_profile=asdict(profile), fixed_entry_profile_id=source_summary["winners"]["best_observed_70"]["profile_id"],
                   fixed_exit=dict(initial_stop_pct=contract.INITIAL_STOP_PCT, tightened_stop_pct=contract.TIGHTENED_STOP_PCT,
                                   tighten_after_minutes=contract.TIGHTEN_AFTER_MINUTES, square_off=contract.SQUARE_OFF),
                   sizing=dict(capital_per_entry_rs=contract.CAPITAL_PER_ENTRY_RS, leverage=contract.LEVERAGE,
                               portfolio_capital_rs=contract.PORTFOLIO_CAPITAL_RS, cost_bps=contract.ROUND_TRIP_COST_BPS),
                   winners=winners, reference_1pct=reference_trial,
                   pooled_70_pairs=sum(pair_passes(t["periods"]["all"]) for t in trials),
                   both_sides_observed_70_pairs=sum(pair_passes(t["periods"]["all"], each_side=True) for t in trials),
                   both_sides_min5_70_pairs=sum(pair_passes(t["periods"]["all"], each_side=True, side_minimum=5) for t in trials),
                   max_abs_old_trade_net_change_rs=max(abs(t["old_trade_net_change_rs"]) for t in trials),
                   source_reference=str(reference), source_manifest_sha256=plan["source_manifest_sha256"],
                   baseline_evidence=baseline_evidence, feature_evidence=feature_evidence,
                   dated_universe_evidence=universe_evidence, source_path_evidence=path_evidence,
                   earlier_results_already_inspected=True, retrospective_opening_oi=True,
                   live_tradability_verified=False, approved_for_live=False, reference_1pct_parity=True)
    table = pd.DataFrame([flatten(t) for t in trials])
    table.to_csv(output_dir / "target_pairs.csv", index=False)
    table.loc[table.short_target_pct.eq(1.)].to_csv(output_dir / "long_target_sweep.csv", index=False)
    table.loc[table.long_target_pct.eq(1.)].to_csv(output_dir / "short_target_sweep.csv", index=False)
    pd.concat([raw.assign(sweep_target_pct=target) for target, raw in raw_by_target.items()],
              ignore_index=True).to_csv(output_dir / "raw_trades_all_targets.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    if paired.sha256(Path(baseline_evidence["pointer"])) != baseline_evidence["pointer_sha256"]:
        raise RuntimeError("Published pointer changed during sweep")
    if any(paired.sha256(p) != code_hashes[p.name] for p in code):
        raise RuntimeError("Research code changed during sweep")
    manifest = dict(search_version=SEARCH_VERSION, execution_authority=False, code_sha256=code_hashes,
                    files={p.name: paired.sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, default=paired.DEFAULT_POINTER)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    args = parser.parse_args(argv)
    summary = run(args.reference, args.output_dir, pointer_path=args.pointer, data_root=args.data_root)
    print(json.dumps({key: summary[key] for key in ("pair_count", "pooled_70_pairs",
        "both_sides_observed_70_pairs", "both_sides_min5_70_pairs", "winners")}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
