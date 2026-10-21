"""Independent G4 afternoon accounting and choice audit; no production engines."""
from pathlib import Path
import argparse
import importlib.util
import json

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
spec = importlib.util.spec_from_file_location("independent_heap", HERE.parent / "g4_midday_selected12_20261013/subset_audit.py")
heap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(heap)
read = lambda p: pd.read_csv(p, float_precision="round_trip")
load = lambda p: json.loads(p.read_text(encoding="utf-8-sig"))
CHECKS = []


def check(name, value):
    CHECKS.append(dict(name=name, passed=bool(value)))
    assert value, name


def near(name, a, b, *, atol=1e-6):
    check(name, (pd.isna(a) and pd.isna(b)) or np.isclose(float(a), float(b), rtol=0, atol=atol))


def verify(folder, *, code=True):
    manifest = load(folder / "manifest.json")
    for name, digest in manifest["files"].items():
        check("artifact " + str(folder / name), heap.sha(folder / name) == digest)
    if code:
        for name, digest in {**manifest.get("code_hashes", {}), **manifest.get("code_sha256", {})}.items():
            check("code " + name, heap.sha(WORKSPACE / name) == digest)
    return manifest


def independent_ledger(saved, label):
    expected = heap.independent_allocate(saved)
    check(label + " executions", np.array_equal(expected.portfolio_executed, saved.portfolio_executed))
    for metric, col in [("net", "net_profit"), ("gross", "gross_profit"), ("cost", "cost")]:
        check(label + " " + metric, np.allclose(expected["audit_" + metric].astype(float),
            saved["portfolio_" + col + "_rupees"].astype(float), rtol=0, atol=1e-6))
    return expected


def compare_stats(row, frame, days, label):
    stats = heap.stats(frame, days)
    for name, expected in stats.items():
        if name in row:
            near(label + " " + name, row[name], expected)
    return stats


def portfolio_audit(folder, days, *, baseline_ids, summary, baseline):
    periods = dict(train=days[:38], holdout=days[38:], all=days)
    results = read(folder / "portfolio_results.csv")
    daily = read(folder / "daily_results.csv")
    monthly = read(folder / "monthly_results.csv")
    setup_file = folder / "scenario_setup_results.csv"
    setups = read(setup_file) if setup_file.exists() else None
    scenarios = {}
    for scenario, rows in results.groupby("scenario", sort=True):
        ledger = independent_ledger(read(folder / f"{scenario}_trades.csv"), scenario)
        scenarios[scenario] = ledger
        added = ledger.loc[~ledger.sid.isin(baseline_ids)]
        old = baseline.set_index("sid").sort_index()
        kept = ledger.loc[ledger.sid.isin(baseline_ids)].set_index("sid").sort_index()
        changed = 0
        if len(kept):
            check(scenario + " baseline identities", kept.index.equals(old.index))
            for col in ("native_target_pct", "entry_price", "exit_price", "net_return_pct"):
                check(scenario + " baseline raw " + col, np.allclose(kept[col], old[col], rtol=0, atol=1e-9, equal_nan=True))
            changed = int((kept.portfolio_executed.ne(old.portfolio_executed) |
                ~np.isclose(kept.audit_net, old.portfolio_net_profit_rupees, rtol=0, atol=1e-6)).sum())
        for row in rows.to_dict("records"):
            scope = row.get("scope", "combined")
            selected = added if scope == "additions" else ledger
            cohort = periods[row["period"]]
            compare_stats(row, selected.loc[selected.day.isin(cohort)], cohort, scenario + " " + scope + " " + row["period"])
            if "baseline_trades_changed" in row:
                check(scenario + " baseline changes reported " + row["period"] + scope, changed == row["baseline_trades_changed"])
        for row in daily.loc[daily.scenario.eq(scenario)].to_dict("records"):
            compare_stats(row, ledger.loc[ledger.day.eq(row["day"])], [row["day"]], scenario + " " + row["day"])
            if "added_net_pnl_rs" in row:
                near(scenario + " daily additions " + row["day"], row["added_net_pnl_rs"], added.loc[added.day.eq(row["day"])].audit_net.sum())
        for row in monthly.loc[monthly.scenario.eq(scenario)].to_dict("records"):
            cohort = [d for d in days if d.startswith(row["month"])]
            compare_stats(row, ledger.loc[ledger.day.isin(cohort)], cohort, scenario + " " + row["month"])
            if "added_net_pnl_rs" in row:
                near(scenario + " monthly additions " + row["month"], row["added_net_pnl_rs"], added.loc[added.day.isin(cohort)].audit_net.sum())
        if setups is not None:
            for row in setups.loc[setups.scenario.eq(scenario)].to_dict("records"):
                cohort = periods[row["period"]]
                frame = added.loc[added.setup_id.eq(row["setup_id"]) & added.day.isin(cohort)]
                stats = compare_stats(row, frame, cohort, scenario + " " + row["setup_id"] + " " + row["period"])
                passes = (stats["executed_trades"] >= (3 if row["period"] == "holdout" else 5)
                    and stats["win_rate_pct"] is not None and stats["win_rate_pct"] >= 70 and stats["net_pnl_rs"] > 0)
                check(scenario + " actual70 " + row["setup_id"] + " " + row["period"], passes == row["observed_70_positive"])
    return scenarios


def all_entry_trial_statistics(raw, trials, days):
    """Recompute every filter-grid trial with NumPy, before seeing any fills."""
    periods = dict(train=np.arange(38), holdout=np.arange(38, 49), all=np.arange(49))
    columns = {}
    for period in periods:
        for metric in ("selections", "fills", "wins", "losses", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"):
            columns[period + "_" + metric] = []
    pools = {}
    for (clock, side), frame in raw.groupby(["hhmm_int", "side"]):
        work = frame.sort_values(["day", "traded_value", "tradingsymbol"], ascending=[True, False, True], kind="stable")
        pools[(clock, side)] = dict(day=work.day.map({d: i for i, d in enumerate(days)}).to_numpy(int),
            price=work.price_change_pct.to_numpy(float) * (1 if side == "LONG" else -1),
            oi=work.oi_change_pct.to_numpy(float), volume=work.volume_ratio.to_numpy(float),
            body=work.body_ratio.to_numpy(float), wick=work.wick_ratio.to_numpy(float),
            one_volume=work.v9_1m_volume_ratio.to_numpy(float), filled=work.filled.to_numpy(bool),
            pnl=work.net_return_pct.to_numpy(float) * 5000.)
    for row in trials.to_dict("records"):
        level = json.loads(row["thresholds"])
        pool = pools.get((int(row["signal_end"].replace(":", "")), row["side"]))
        if pool is None:
            day = np.array([], dtype=int)
            filled = np.array([], dtype=bool)
            pnl = np.array([], dtype=float)
        else:
            matching = np.flatnonzero((pool["price"] >= level["price_change_pct"])
                & (pool["oi"] >= level["oi_change_pct"]) & (pool["volume"] >= level["five_minute_volume_ratio_min"])
                & (pool["body"] >= level["one_minute_body_ratio_min"]) & (pool["wick"] <= level["one_minute_wick_ratio_max"])
                & (pool["one_volume"] >= level["one_minute_volume_ratio_min"]))
            _, first = np.unique(pool["day"][matching], return_index=True)
            chosen = matching[first]
            day, filled, pnl = pool["day"][chosen], pool["filled"][chosen], pool["pnl"][chosen]
        for period, cohort in periods.items():
            in_period = np.isin(day, cohort)
            mask = in_period & filled
            profit = pnl[mask]
            daily = np.bincount(day[mask], weights=profit, minlength=49)[cohort]
            curve = np.r_[0., daily.cumsum()]
            stats = dict(selections=int(in_period.sum()), fills=len(profit), wins=int((profit > 0).sum()),
                losses=int((profit < 0).sum()), win_rate_pct=float(100 * (profit > 0).mean()) if len(profit) else np.nan,
                net_pnl_rs=profit.sum(), max_drawdown_rs=(np.maximum.accumulate(curve) - curve).max())
            for metric, value in stats.items():
                columns[period + "_" + metric].append(value)
    for key, expected in columns.items():
        check("all6168trials independent " + key, np.allclose(trials[key], expected, rtol=0, atol=1e-6, equal_nan=True))


def entry_audit(folder):
    verify(folder)
    summary = load(folder / "summary.json")
    config = load(folder / "selected_config.json")
    days = sorted(read(folder / "daily_results.csv").day.unique())
    check("entry49days", len(days) == 49 and days[0] == "2026-07-29" and days[-1] == "2026-10-12" and "2026-10-01" not in days)
    check("entry24setups", len(config["setups"]) == 24 and summary["new_setups"] == 24)
    clocks = list(pd.date_range("2000-01-01 12:35", "2000-01-01 13:30", freq="5min").strftime("%H:%M"))
    check("entryexactclocks", summary["signal_ends"] == clocks)
    prior = Path(summary["baseline"]["run_dir"])
    check("entrybaselinepin", heap.sha(prior / "manifest.json") == summary["baseline"]["manifest_sha256"])
    verify(prior, code=False)
    baseline = read(folder / "baseline_g4_trades.csv")
    old = read(prior / "combined_trades.csv").set_index("sid").sort_index()
    kept = baseline.set_index("sid").sort_index()
    check("entry49baseline", baseline.setup_id.nunique() == 49 and kept.index.equals(old.index))
    for field in ("native_target_pct", "entry_price", "exit_price", "net_return_pct", "portfolio_net_profit_rupees"):
        check("entrybaseline " + field, np.allclose(kept[field], old[field], rtol=0, atol=1e-9, equal_nan=True))
    source = summary["source_evidence"]
    check("sealedmanifest", heap.sha(Path(source["sealed_dataset_manifest"])) == source["sealed_dataset_manifest_sha256"])
    cache = Path(source["late_cache"])
    for day, digest in source["recent_day_manifests"].items():
        manifest_path = cache / day / "manifest.json"
        check("source day " + day, heap.sha(manifest_path) == digest)
        manifest = load(manifest_path)
        for key, filename in [("candidate_sha256", "candidates.parquet"), ("paths_sha256", "paths.npz")]:
            check("source " + day + " " + filename, heap.sha(cache / day / filename) == manifest[key])
    trials = read(folder / "strictness_sweep.csv")
    check("entry6168trials", len(trials) == summary["tested_configurations"] == 6168)
    for filename, period, minimum in [("setup_results.csv", "train", 5), ("historical_fit_results.csv", "all", 8), ("stage_decisions.csv", "train", 5)]:
        chosen = read(folder / filename)
        for row in chosen.to_dict("records"):
            pool = trials.loc[trials.setup_id.eq(row["setup_id"])].copy()
            stages = [row["tested_stage"]] if "tested_stage" in row else ["strict", "relax_1m", "relax_5m"]
            expected = None
            for stage in stages:
                eligible = pool.loc[pool.stage.eq(stage) & pool[f"{period}_fills"].ge(minimum)
                    & pool[f"{period}_win_rate_pct"].ge(70) & pool[f"{period}_net_pnl_rs"].gt(0)].copy()
                if len(eligible):
                    eligible["rounded_pnl"] = eligible[f"{period}_net_pnl_rs"].round(6)
                    eligible["rounded_dd"] = eligible[f"{period}_max_drawdown_rs"].round(6)
                    expected = eligible.sort_values(["rounded_pnl", "rounded_dd", f"{period}_fills", "index"], ascending=[False, True, False, True]).iloc[0]
                    break
            passed = expected is not None
            if not passed:
                eligible = pool.loc[pool.stage.isin(stages) & pool[f"{period}_fills"].ge(minimum)]
                expected = (eligible.sort_values([f"{period}_win_rate_pct", f"{period}_net_pnl_rs", "index"], ascending=[False, False, True]).iloc[0]
                    if len(eligible) else pool.loc[pool.stage.isin(stages)].iloc[0])
            check(filename + " chosen index " + row["setup_id"] + " " + str(stages), int(row["index"]) == int(expected["index"]))
            check(filename + " passed " + row["setup_id"] + " " + str(stages), row["historical_pass" if period == "all" else "training_pass"] == passed)
    raw = read(folder / "candidate_outcomes.csv")
    all_entry_trial_statistics(raw, trials, days)
    decisions = read(folder / "setup_results.csv")
    selected_ids = []
    for row in decisions.to_dict("records"):
        level = json.loads(row["thresholds"])
        pool = raw.loc[raw.hhmm_int.eq(int(row["signal_end"].replace(":", ""))) & raw.side.eq(row["side"])].copy()
        pool["directional_price"] = pool.price_change_pct * (1 if row["side"] == "LONG" else -1)
        for col, limit, upper in [("directional_price", "price_change_pct", False), ("oi_change_pct", "oi_change_pct", False),
            ("volume_ratio", "five_minute_volume_ratio_min", False), ("body_ratio", "one_minute_body_ratio_min", False),
            ("wick_ratio", "one_minute_wick_ratio_max", True), ("v9_1m_volume_ratio", "one_minute_volume_ratio_min", False)]:
            pool = pool.loc[pool[col].le(level[limit]) if upper else pool[col].ge(level[limit])]
        wanted = pool.sort_values(["day", "traded_value", "tradingsymbol"], ascending=[True, False, True], kind="stable").drop_duplicates("day")
        selected_ids.extend(wanted.sid)
        for period, cohort in dict(train=days[:38], holdout=days[38:], all=days).items():
            filled = wanted.loc[wanted.day.isin(cohort) & wanted.filled.eq(True)]
            pnl = filled.net_return_pct * 5000.
            near(row["setup_id"] + period + " fills", row[f"{period}_fills"], len(filled))
            near(row["setup_id"] + period + " wins", row[f"{period}_wins"], pnl.gt(0).sum())
            near(row["setup_id"] + period + " pnl", row[f"{period}_net_pnl_rs"], pnl.sum())
    orders = read(folder / "selected_orders.csv")
    check("entryselectedidsindependent", set(selected_ids) == set(orders.sid) and len(selected_ids) == len(orders))
    scenarios = portfolio_audit(folder, days, baseline_ids=set(baseline.sid), summary=summary, baseline=baseline)
    return dict(status="PASS", kind="entry", run=str(folder), configured_setups=24, strictness_trials=len(trials),
        selected_orders=len(orders), native_fills=int(orders.sid.isin(raw.loc[raw.filled].sid).sum()), scenarios=len(scenarios))


def targets_audit(folder):
    verify(folder)
    summary = load(folder / "summary.json")
    reference = Path(summary["source"]["run_dir"])
    check("targetentrypin", heap.sha(reference / "manifest.json") == summary["source"]["manifest_sha256"])
    orders = read(reference / "selected_orders.csv")
    baseline = read(reference / "baseline_g4_trades.csv")
    cache = pd.read_parquet(folder / "minute_replay_all_targets.parquet")
    trials = read(folder / "target_sweep.csv")
    choices = read(folder / "best_targets.csv")
    levels = summary["target_levels"]
    check("995targetlevels", levels == 995 and summary["target_min_pct"] == .06 and summary["target_max_pct"] == 10.)
    check("targetgridrows", len(trials) == 24 * levels and len(cache) == len(orders) * levels)
    check("nativefillsfixed", cache.groupby("target_pct").filled.sum().eq(summary["potential_fills"]).all())
    check("nativeidsfixed", cache.groupby("sid").size().eq(levels).all())
    for col in ("filled", "entry_path_index", "entry_price", "entry_ts"):
        check("entryinvariance " + col, cache.groupby("sid")[col].nunique(dropna=False).le(1).all())
    for row in choices.to_dict("records"):
        pool = trials.loc[trials.setup_id.eq(row["setup_id"])].copy()
        for label, period in [("historical", "all"), ("training", "train")]:
            eligible = pool.loc[pool[f"{period}_fills"].ge(5) & pool[f"{period}_win_rate_pct"].ge(70) & pool[f"{period}_net_pnl_rs"].gt(0)].copy()
            if len(eligible):
                eligible["rounded_pnl"] = eligible[f"{period}_net_pnl_rs"].round(6)
                eligible["rounded_dd"] = eligible[f"{period}_max_drawdown_rs"].round(6)
                chosen = eligible.sort_values(["rounded_pnl", "rounded_dd", f"{period}_win_rate_pct", "target_pct"], ascending=[False, True, False, True]).iloc[0]
                near(row["setup_id"] + label + " besttarget", row[label + "_target_pct"], chosen.target_pct)
            else:
                check(row["setup_id"] + label + " nopass", pd.isna(row[label + "_target_pct"]))
    days = sorted(read(folder / "daily_results.csv").day.unique())
    for period, cohort in dict(train=days[:38], holdout=days[38:], all=days).items():
        filled = cache.loc[cache.day.isin(cohort) & cache.filled.eq(True)].copy()
        filled["audit_net"] = filled.net_return_pct * 5000.
        grouped = filled.groupby(["setup_id", "target_pct"]).audit_net.agg(["size", "sum", lambda x: x.gt(0).sum()])
        grouped.columns = ["fills", "pnl", "wins"]
        expected = trials[["setup_id", "target_pct"]].merge(grouped, how="left", on=["setup_id", "target_pct"]).fillna(0)
        for actual, metric in [("fills", "fills"), ("net_pnl_rs", "pnl"), ("wins", "wins")]:
            check("alltargetrows " + period + actual, np.allclose(trials[period + "_" + actual], expected[metric], atol=1e-6, rtol=0))
    scenarios = portfolio_audit(folder, days, baseline_ids=set(baseline.sid), summary=summary, baseline=baseline)
    ranking = read(folder / "scenario_comparison.csv")
    setup_results = read(folder / "scenario_setup_results.csv")
    for row in ranking.to_dict("records"):
        ledger = scenarios[row["scenario"]]
        added = ledger.loc[~ledger.sid.isin(baseline.sid)]
        full_stats, add_stats = heap.stats(ledger, days), heap.stats(added, days)
        portfolio_pass = add_stats["executed_trades"] >= 5 and add_stats["win_rate_pct"] >= 70 and add_stats["net_pnl_rs"] > 0 and full_stats["win_rate_pct"] >= 70 and full_stats["net_pnl_rs"] > 0
        per_setup = setup_results.loc[setup_results.scenario.eq(row["scenario"]) & setup_results.period.eq("all")]
        check(row["scenario"] + " rankportfolio", bool(row["portfolio_70_positive"]) == bool(portfolio_pass))
        check(row["scenario"] + " rankeverysetup", bool(row["every_setup_70_positive"]) == bool(portfolio_pass and len(per_setup) and per_setup.observed_70_positive.all()))
    for flag, key in [("portfolio_70_positive", "best_tested_portfolio70_scenario"), ("every_setup_70_positive", "best_tested_every_setup70_scenario")]:
        eligible = ranking.loc[ranking[flag]].sort_values(["combined_net_pnl_rs", "scenario"], ascending=[False, True])
        check("best scenario " + flag, summary[key] == (eligible.iloc[0].scenario if len(eligible) else None))
    return dict(status="PASS", kind="targets", run=str(folder), target_levels=levels, target_trials=len(trials), scenarios=len(scenarios), best_every_setup70=summary["best_tested_every_setup70_scenario"])


def refinement_audit(folder):
    verify(folder)
    summary = load(folder / "summary.json")
    source = Path(summary["source"]["run_dir"])
    check("refinementsourcepin", heap.sha(source / "manifest.json") == summary["source"]["manifest_sha256"])
    baseline = read(folder / "baseline_g4_trades.csv")
    raw = read(source / (summary["initial_scenario"] + "_added_raw_trades.csv"))
    trials = read(folder / "refinement_trials.csv")
    days = sorted(read(folder / "daily_results.csv").day.unique())
    active = set(raw.setup_id)
    previous = summary["initial"]["combined_net_pnl_rs"]
    for iteration, batch in trials.loc[trials.iteration.gt(0)].groupby("iteration", sort=True):
        check("every whole-setup deletion evaluated " + str(iteration), set(batch.removed_setup) == active)
        eligible = []
        for row in batch.to_dict("records"):
            remaining = active - {row["removed_setup"]}
            wanted = raw.loc[raw.setup_id.isin(remaining)]
            combined = heap.independent_allocate(pd.concat([baseline, wanted], ignore_index=True))
            added = combined.loc[combined.sid.isin(wanted.sid)]
            full_stats, add_stats = heap.stats(combined, days), heap.stats(added, days)
            name = str(iteration) + " remove " + row["removed_setup"]
            near(name + " combined", row["combined_net_pnl_rs"], full_stats["net_pnl_rs"])
            near(name + " added", row["added_net_pnl_rs"], add_stats["net_pnl_rs"])
            near(name + " improvement", row["improvement_rs"], full_stats["net_pnl_rs"] - previous)
            def qualifies(stats):
                return stats["executed_trades"] >= 5 and stats["win_rate_pct"] >= 70 and stats["net_pnl_rs"] > 0
            passing = qualifies(full_stats) and qualifies(add_stats)
            for setup_id in remaining:
                passing = passing and qualifies(heap.stats(added.loc[added.setup_id.eq(setup_id)], days))
            check(name + " eligibility", bool(passing) == bool(row["eligible"]))
            if passing and full_stats["net_pnl_rs"] - previous > 1e-6:
                eligible.append(row)
        accepted = batch.loc[batch.accepted.eq(True)]
        if eligible:
            best = sorted(eligible, key=lambda row: (-row["combined_net_pnl_rs"], row["removed_setup"]))[0]
            check("largest eligible deletion " + str(iteration), len(accepted) == 1 and accepted.iloc[0].removed_setup == best["removed_setup"])
            active.remove(best["removed_setup"])
            previous = best["combined_net_pnl_rs"]
        else:
            check("no further improving deletion " + str(iteration), accepted.empty)
    check("final selected members", active == set(summary["targets"]))
    scenarios = portfolio_audit(folder, days, baseline_ids=set(baseline.sid), summary=summary, baseline=baseline)
    final = scenarios["refined_qualified"]
    added = final.loc[final.setup_id.isin(active)].set_index("sid").sort_index()
    frozen = raw.loc[raw.setup_id.isin(active)].set_index("sid").sort_index()
    check("refined entries unchanged", added.index.equals(frozen.index))
    for field in ("entry_price", "exit_price", "native_target_pct", "net_return_pct", "gross_return_pct", "cost_pct"):
        check("refined raw " + field, np.allclose(added[field], frozen[field], rtol=0, atol=1e-9, equal_nan=True))
    final_config = load(folder / "refined_config.json")
    source_config = load(source / "scenario_configs.json")
    frozen_specs = {s["setup_id"]: s for s in source_config["scenarios"][summary["initial_scenario"]]}
    check("portable config targetmap", {s["setup_id"]: s["target_pct"] for s in final_config["setups"]} == summary["targets"])
    for setup in final_config["setups"]:
        check("portable unchanged " + setup["setup_id"], setup == frozen_specs[setup["setup_id"]])
    kept = final.loc[final.sid.isin(baseline.sid)].set_index("sid").sort_index()
    old = baseline.set_index("sid").sort_index()
    check("baseline identities retained", kept.index.equals(old.index))
    for field in ("native_target_pct", "entry_price", "exit_price", "net_return_pct"):
        check("refined baseline raw " + field, np.allclose(kept[field], old[field], rtol=0, atol=1e-9, equal_nan=True))
    changes = int((kept.portfolio_executed.ne(old.portfolio_executed) |
        ~np.isclose(kept.audit_net, old.portfolio_net_profit_rupees, atol=1e-6, rtol=0)).sum())
    check("baseline changes reported", changes == summary["baseline_trades_changed"])
    return dict(status="PASS", kind="refinement", run=str(folder), final_setups=len(active),
        independently_recomputed_deletion_trials=len(trials)-1, combined=heap.stats(final, days),
        additions=heap.stats(final.loc[final.setup_id.isin(active)], days), baseline_trades_changed=changes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["entry", "targets", "refinement"])
    parser.add_argument("folder", type=Path)
    args = parser.parse_args()
    result = {"entry": entry_audit, "targets": targets_audit, "refinement": refinement_audit}[args.kind](args.folder)
    result["checks"] = len(CHECKS)
    result["all_checks"] = CHECKS
    destination = HERE / ("independent_" + args.kind + "_validation.json")
    destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "all_checks"}, indent=2))


if __name__ == "__main__":
    main()
