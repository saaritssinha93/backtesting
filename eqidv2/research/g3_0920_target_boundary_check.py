"""Extend the observed boundary targets without changing the sealed first sweep.

LONG: check 3.05..10.00% in 0.05-point increments. SHORT: refine 0.06..0.20%
in 0.01-point increments. Existing entry filters/stops remain fixed. Targets
above 3% are explicit research overrides, outside the production contract.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g3_0920_target_sweep as sweep
from research import g3_0920_backtest as paired
from research import g3_0920_parameter_search as coarse
from research.g3_0920_strategy import selection_audit_0920
import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract

LONG_EXTRA = tuple(round(n / 100, 2) for n in range(305, 1001, 5))
SHORT_EXTRA = tuple(round(n / 100, 2) for n in range(6, 21) if round(n / 100, 2) not in sweep.TARGETS)
VERSION = "G3_0920_TARGET_BOUNDARY_RESEARCH_V1"


def override_targets(orders, long_target, short_target):
    if any(not np.isfinite(t) or not .06 <= t <= 10 for t in (long_target, short_target)):
        raise ValueError("Boundary research targets must be between 0.06 and 10%")
    result = orders.copy()
    result["native_target_pct"] = result.side.map({"LONG": long_target, "SHORT": short_target})
    result["target_pct"] = result.native_target_pct
    result["research_exit_variant"] = VERSION
    return result


def run(parent, output):
    parent, output = Path(parent).resolve(), Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Boundary output must be new or empty")
    manifest = json.loads((parent / "manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["files"].items():
        if Path(name).name != name or paired.sha256(parent / name) != digest:
            raise ValueError(f"Changed parent artifact: {name}")
    old = json.loads((parent / "summary.json").read_text(encoding="utf-8"))
    original_reference = Path(old["source_reference"])
    source_summary, profile = sweep.read_verified_reference(original_reference)
    published, days, baseline_evidence = paired.load_published()
    if baseline_evidence["pointer_sha256"] != old["baseline_evidence"]["pointer_sha256"]:
        raise RuntimeError("Published comparison changed")
    code = [Path(__file__).resolve(), Path(sweep.__file__), Path(paired.__file__),
            Path(simulator.__file__), Path(contract.__file__)]
    code_hashes = {p.name: paired.sha256(p) for p in code}
    output.mkdir(parents=True, exist_ok=True)
    plan = dict(search_version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                parent=str(parent), parent_manifest_sha256=paired.sha256(parent / "manifest.json"),
                new_long_targets=LONG_EXTRA, new_short_targets=SHORT_EXTRA,
                native_target_upper_bound=3., research_target_upper_bound=10.,
                entry_profile_id=old["fixed_entry_profile_id"], code_sha256=code_hashes)
    (output / "search_plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    path = Path(source_summary["feature_path"])
    if paired.sha256(path) != source_summary["feature_sha256"]:
        raise RuntimeError("Feature export changed")
    features, coverage, _ = paired.load_feature_evidence(path)
    paired.validate_feature_universe(features, sweep.RUNTIME_ROOT, coverage)
    features = features.loc[features.session_date.astype(str).isin({str(d) for d in days})]
    audit = selection_audit_0920(features, profile=profile)
    orders = audit.loc[audit.v9_selected].reset_index(drop=True)
    expected = pd.read_csv(original_reference / "best_observed_70_orders.csv")
    pd.testing.assert_frame_equal(orders[["sid", "signal_id"]], expected[["sid", "signal_id"]], check_dtype=False)
    paths, path_evidence = paired.new_execution_paths(orders, sweep.RUNTIME_ROOT)
    baseline = paired.allocate(published)
    sets = {"all": days, "discovery": [d for d in days if d <= coarse.DISCOVERY_LAST],
            "holdout": [d for d in days if d >= coarse.HOLDOUT_FIRST]}
    base_stats = {k: coarse._period_stats(baseline, v) for k, v in sets.items()}
    added_ids = set(orders.sid.astype(int))
    cached_raw = pd.read_csv(parent / "raw_trades_all_targets.csv", low_memory=False)
    raw = {(side, float(target)): group.copy() for (side, target), group in cached_raw.groupby(["side", "sweep_target_pct"])}
    added_raw = []
    for side, targets in (("LONG", LONG_EXTRA), ("SHORT", SHORT_EXTRA)):
        side_orders = orders.loc[orders.side.eq(side)]
        for index, target in enumerate(targets, 1):
            new = simulator.simulate_staged(override_targets(side_orders, target, target), paths,
                                            cost_bps=contract.ROUND_TRIP_COST_BPS,
                                            max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
            sweep.assert_same_entries(new, raw[(side, 1.)])
            raw[side, target] = new
            added_raw.append(new.assign(sweep_target_pct=target))
        print(f"Simulated {side}: {len(targets)} additional targets", flush=True)

    replayed = {}
    def evaluate(long, short):
        key = (long, short)
        if key in replayed:
            return replayed[key][0]
        added = pd.concat((raw["LONG", long], raw["SHORT", short]), ignore_index=True)
        added["day"] = pd.to_datetime(added.day).dt.date
        added = added.sort_values(["day", "hhmm_int", "side", "tradingsymbol"], kind="stable")
        variant = paired.allocate(pd.concat((published, added), ignore_index=True, sort=False))
        periods = sweep.periods_for(base_stats, variant, added_ids, days)
        trial = dict(long_target_pct=long, short_target_pct=short, periods=periods)
        replayed[key] = (trial, variant)
        return trial

    # Seed each old winner, plus earlier-period axis choices, to preserve the
    # entire first sweep's relevant alternatives without repeating its 3721 pairs.
    seeds = {(1., 1.), (.97, 2.67)}
    for label in ("best_long_min5_70", "best_short_small_sample_70",
                  "best_long_net_unconstrained", "best_short_net_unconstrained"):
        trial = old["winners"][label]
        if trial:
            seeds.add((trial["long_target_pct"], trial["short_target_pct"]))
    for side in ("long", "short"):
        table = pd.read_csv(parent / f"{side}_target_sweep.csv")
        prefix = f"discovery_added_{side}_"
        eligible = table.loc[table[prefix + "win_rate_pct"].ge(70) & table[prefix + "net_pnl_rs"].gt(0)]
        if not eligible.empty:
            row = eligible.sort_values(prefix + "net_pnl_rs", ascending=False).iloc[0]
            seeds.add((float(row.long_target_pct), float(row.short_target_pct)))
    for long, short in sorted(seeds):
        evaluate(long, short)
    paired._same_replayed_baseline(pd.read_csv(original_reference / "best_observed_70_variant_trades.csv"),
                                   replayed[(1., 1.)][1])
    for i, target in enumerate(LONG_EXTRA, 1):
        evaluate(target, 1.)
        if i % 35 == 0:
            print(f"Checked LONG extension {i}/{len(LONG_EXTRA)}", flush=True)
    for target in SHORT_EXTRA:
        evaluate(1., target)
    trials = [value[0] for value in replayed.values()]
    long_best = sweep.choose_side(trials, "LONG")
    short_best = sweep.choose_side(trials, "SHORT", minimum=1)
    chosen = evaluate(long_best["long_target_pct"], short_best["short_target_pct"]) if long_best and short_best else None
    early_long = sweep.choose_side(trials, "LONG", "discovery", minimum=1)
    early_short = sweep.choose_side(trials, "SHORT", "discovery", minimum=1)
    early = evaluate(early_long["long_target_pct"], early_short["short_target_pct"]) if early_long and early_short else None
    winners = dict(best_long_min5_70=long_best, best_short_small_sample_70=short_best,
                   best_short_min5_70=sweep.choose_side(trials, "SHORT"),
                   best_pair_both_sides_observed_70=chosen,
                   diagnostic_discovery_pair_small_sample_70=early,
                   best_long_net_unconstrained=sweep.choose_side(trials, "LONG", minimum=1, require_70=False))
    for label, trial in winners.items():
        if trial is None:
            continue
        key = (trial["long_target_pct"], trial["short_target_pct"])
        variant = replayed[key][1]
        comparison, daily = paired.compare_ledgers(baseline, variant, days, days, added_ids)
        trial["old_trade_effect"] = comparison["old_trade_effect"]
        variant.to_csv(output / (label + "_variant_trades.csv"), index=False)
        daily.to_csv(output / (label + "_daily_comparison.csv"), index=False)
        override_targets(orders, *key).to_csv(output / (label + "_orders.csv"), index=False)
    table = pd.DataFrame([sweep.flatten(t[0]) for t in replayed.values()])
    table.to_csv(output / "additional_target_pairs.csv", index=False)
    for side, opposite in (("long", "short"), ("short", "long")):
        old_axis = pd.read_csv(parent / f"{side}_target_sweep.csv")
        axis = pd.concat((old_axis, table.loc[table[opposite + "_target_pct"].eq(1.)]), ignore_index=True)
        axis = axis.drop_duplicates(["long_target_pct", "short_target_pct"], keep="last").sort_values(side + "_target_pct")
        axis.to_csv(output / f"{side}_target_sweep.csv", index=False)
    pd.concat(added_raw, ignore_index=True).to_csv(output / "additional_raw_trades.csv", index=False)
    summary = dict(search_version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), first_date=str(days[0]), last_date=str(days[-1]),
                   sessions=len(days), parent=str(parent), parent_manifest_sha256=plan["parent_manifest_sha256"],
                   parent_pair_count=old["pair_count"], additional_pair_count=len(replayed),
                   long_target_count=len(set(sweep.TARGETS) | set(LONG_EXTRA)),
                   short_target_count=len(set(sweep.TARGETS) | set(SHORT_EXTRA)),
                   selected_entry_counts=old["selected_entry_counts"], fixed_entry_profile=old["fixed_entry_profile"],
                   fixed_entry_profile_id=old["fixed_entry_profile_id"], fixed_exit=old["fixed_exit"], sizing=old["sizing"],
                   reference_1pct=old["reference_1pct"], winners=winners,
                   long_10pct_target_hits=int(raw["LONG", 10.].exit_reason.eq("TARGET").sum()),
                   long_10pct_max_favorable_excursion=float(raw["LONG", 10.].mfe_pct.max()),
                   final_pair_pooled_70=bool(chosen and sweep.pair_passes(chosen["periods"]["all"])),
                   final_pair_both_sides_min5_70=bool(chosen and sweep.pair_passes(chosen["periods"]["all"], each_side=True, side_minimum=5)),
                   baseline_evidence=baseline_evidence, source_path_evidence=path_evidence,
                   research_override_above_native_3pct=True, retrospective_opening_oi=True,
                   earlier_results_already_inspected=True, approved_for_live=False)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    if paired.sha256(Path(baseline_evidence["pointer"])) != baseline_evidence["pointer_sha256"]:
        raise RuntimeError("Published pointer changed")
    if any(paired.sha256(p) != code_hashes[p.name] for p in code):
        raise RuntimeError("Boundary research source changed")
    final_manifest = dict(search_version=VERSION, execution_authority=False, code_sha256=code_hashes,
                          files={p.name: paired.sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(final_manifest, indent=2) + "\n", encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.parent, args.output_dir)
    print(json.dumps({"winners": result["winners"], "long_10pct_max_favorable_excursion": result["long_10pct_max_favorable_excursion"]}, default=str))
