"""Independent complete coordinate-grid replay with a separately built event heap."""
from pathlib import Path
import heapq
import json
import sys

import numpy as np
import pandas as pd

import independent_audit as audit


class IndependentEvents:
    def __init__(self, baseline, cache):
        self.targets = sorted(cache.target_pct.unique())
        self.target_rows = {target: i for i, target in enumerate(self.targets)}
        first = cache.loc[cache.target_pct.eq(self.targets[0])].sort_values("sid").reset_index(drop=True)
        self.setups = sorted(first.setup_id.unique())
        self.setup_codes = {name: i for i, name in enumerate(self.setups)}
        self.count_base = len(baseline)
        frame = audit.heap.independent_allocate(pd.concat([baseline, first], ignore_index=True))
        self.entries = pd.to_datetime(frame.entry_ts, utc=True).astype("int64").to_numpy()
        self.base_exits = pd.to_datetime(baseline.exit_ts, utc=True).astype("int64").to_numpy()
        self.base_profit = baseline.net_return_pct.to_numpy(float) * 5000.
        self.base_acceptance = baseline.portfolio_executed.to_numpy(bool)
        self.code_for_new = first.setup_id.map(self.setup_codes).to_numpy(int)
        self.new_filled = first.filled.to_numpy(bool)
        self.new_columns = np.arange(len(first))
        self.order = frame.loc[frame.filled].sort_values(
            ["entry_ts", "audit_confirmation", "setup_id", "audit_priority", "tradingsymbol", "sid"],
            ascending=[True, True, True, False, True, True], kind="stable").index.to_numpy(int)
        self.fill_exits = []
        self.fill_profit = []
        for target in self.targets:
            sample = cache.loc[cache.target_pct.eq(target)].sort_values("sid")
            audit.check("coordinate cache identities " + str(target), np.array_equal(sample.sid, first.sid))
            self.fill_exits.append(pd.to_datetime(sample.exit_ts, utc=True).astype("int64").to_numpy())
            self.fill_profit.append(sample.net_return_pct.to_numpy(float) * 5000.)
        self.fill_exits = np.array(self.fill_exits)
        self.fill_profit = np.array(self.fill_profit)

    def evaluate(self, targets, *, require_native70=False):
        rows_by_setup = np.full(len(self.setups), -1, dtype=int)
        for setup, target in targets.items():
            rows_by_setup[self.setup_codes[setup]] = self.target_rows[target]
        rows = rows_by_setup[self.code_for_new]
        selected = np.r_[np.ones(self.count_base, dtype=bool), rows >= 0]
        lookup = np.maximum(rows, 0)
        exits = np.r_[self.base_exits, self.fill_exits[lookup, self.new_columns]]
        profit = np.r_[self.base_profit, self.fill_profit[lookup, self.new_columns]]
        active = []
        accepted = []
        for index in self.order[selected[self.order]]:
            entry = self.entries[index]
            while active and active[0] <= entry:
                heapq.heappop(active)
            if len(active) < 10:
                accepted.append(index)
                heapq.heappush(active, exits[index])
        accepted = np.array(accepted, dtype=int)
        added = accepted[accepted >= self.count_base]
        base_taken = np.zeros(self.count_base, dtype=bool)
        base_taken[accepted[accepted < self.count_base]] = True
        changed = int(np.count_nonzero(base_taken != self.base_acceptance))
        counts = np.bincount(self.code_for_new[added - self.count_base], minlength=len(self.setups))
        wins = np.bincount(self.code_for_new[added - self.count_base], weights=(profit[added] > 0), minlength=len(self.setups))
        pnl = np.bincount(self.code_for_new[added - self.count_base], weights=profit[added], minlength=len(self.setups))
        combined_profit = float(profit[accepted].sum())
        added_profit = float(profit[added].sum())
        combined_wins, added_wins = int((profit[accepted] > 0).sum()), int((profit[added] > 0).sum())
        setup_pass = bool(targets) and all(counts[self.setup_codes[s]] >= 5
            and 10 * wins[self.setup_codes[s]] >= 7 * counts[self.setup_codes[s]]
            and pnl[self.setup_codes[s]] > 0 for s in targets)
        eligible = (setup_pass and changed == 0 and len(added) >= 5 and added_profit > 0
            and 10 * added_wins >= 7 * len(added) and combined_profit > 0 and 10 * combined_wins >= 7 * len(accepted))
        result = dict(eligible=eligible, every_setup_70_positive=setup_pass, baseline_trades_changed=changed,
            combined_executed_trades=len(accepted), combined_wins=combined_wins,
            combined_net_pnl_rs=combined_profit, added_executed_trades=len(added), added_wins=added_wins,
            added_net_pnl_rs=added_profit)
        if require_native70:
            native = self.new_filled & selected[self.count_base:]
            codes = self.code_for_new[native]
            native_pnl = profit[self.count_base:][native]
            counts = np.bincount(codes, minlength=len(self.setups))
            wins = np.bincount(codes, weights=native_pnl > 0, minlength=len(self.setups))
            pnl = np.bincount(codes, weights=native_pnl, minlength=len(self.setups))
            native_pass = bool(targets) and all(counts[self.setup_codes[s]] >= 5
                and 10 * wins[self.setup_codes[s]] >= 7 * counts[self.setup_codes[s]]
                and pnl[self.setup_codes[s]] > 0 for s in targets)
            result["native_every_setup_70_positive"] = native_pass
            result["eligible"] = bool(eligible and native_pass)
        return result


def main(folder):
    folder = Path(folder)
    audit.verify(folder)
    summary = audit.load(folder / "summary.json")
    require_native70 = bool(summary.get("require_native70", False))
    source = Path(summary["source"]["run_dir"])
    audit.check("coordinate source pin", audit.heap.sha(source / "manifest.json") == summary["source"]["manifest_sha256"])
    entry = Path(summary["entry_source"]["run_dir"])
    audit.check("coordinate entry pin", audit.heap.sha(entry / "manifest.json") == summary["entry_source"]["manifest_sha256"])
    baseline = audit.read(folder / "baseline_g4_trades.csv")
    cache = pd.read_parquet(source / "minute_replay_all_targets.parquet")
    events = IndependentEvents(baseline, cache)
    trials = audit.read(folder / "refinement_trials.csv")
    current = dict(summary["initial_targets"])
    original = trials.loc[trials.iteration.eq(0)].iloc[0]
    audit.check("coordinate initial map", json.loads(original.target_map) == current)
    initial = events.evaluate(current, require_native70=require_native70)
    audit.check("coordinate initial eligible", initial["eligible"])
    previous = initial["combined_net_pnl_rs"]
    replayed = 0
    accepted_count = 0
    for iteration, batch in trials.loc[trials.iteration.gt(0)].groupby("iteration", sort=True):
        audit.check("coordinate complete neighborhood " + str(iteration), len(batch) == len(events.setups) * len(events.targets))
        actual = []
        for row in batch.to_dict("records"):
            targets = json.loads(row["target_map"])
            expected = dict(current)
            if row["operation"] == "delete":
                assert row["setup_id"] in expected and pd.isna(row["target_pct"])
                expected.pop(row["setup_id"])
            else:
                assert row["operation"] == ("change" if row["setup_id"] in current else "add")
                assert row["target_pct"] in events.targets and current.get(row["setup_id"]) != row["target_pct"]
                expected[row["setup_id"]] = row["target_pct"]
            assert targets == expected, (iteration, row["setup_id"], row["target_pct"])
            result = events.evaluate(targets, require_native70=require_native70)
            result["improvement_rs"] = result["combined_net_pnl_rs"] - previous
            actual.append(result)
        expected_table = pd.DataFrame(actual)
        for col in expected_table:
            if col in ("eligible", "every_setup_70_positive", "native_every_setup_70_positive"):
                audit.check("coordinate all trials " + str(iteration) + col, np.array_equal(batch[col], expected_table[col]))
            else:
                audit.check("coordinate all trials " + str(iteration) + col,
                    np.allclose(batch[col], expected_table[col], rtol=0, atol=1e-6))
        eligible = batch.loc[batch.eligible & batch.improvement_rs.gt(1e-6)].copy()
        accepted = batch.loc[batch.accepted]
        if len(eligible):
            eligible["rounded_pnl"] = eligible.combined_net_pnl_rs.round(8)
            eligible["target_rank"] = eligible.target_pct.fillna(-1)
            winner = eligible.sort_values(["rounded_pnl", "setup_id", "operation", "target_rank"],
                ascending=[False, True, True, True]).iloc[0]
            audit.check("coordinate greatest eligible improvement " + str(iteration), len(accepted) == 1
                and accepted.iloc[0].target_map == winner.target_map)
            current = json.loads(winner.target_map)
            previous = winner.combined_net_pnl_rs
            accepted_count += 1
        else:
            audit.check("coordinate local convergence " + str(iteration), accepted.empty)
        replayed += len(batch)
        print(f"Independently replayed coordinate pass {iteration}: {len(batch)} target/add/delete candidates", flush=True)
    audit.check("coordinate final map", current == summary["targets"])
    audit.check("coordinate total tested", replayed == summary["tested_configurations"])
    audit.check("coordinate accepted count", accepted_count == summary["accepted_changes"])
    days = sorted(audit.read(folder / "daily_results.csv").day.unique())
    scenarios = audit.portfolio_audit(folder, days, baseline_ids=set(baseline.sid), baseline=baseline, summary=summary)
    final = scenarios["refined_qualified"]
    added = final.loc[~final.sid.isin(baseline.sid)]
    selected_cache = pd.concat([cache.loc[cache.setup_id.eq(setup) & cache.target_pct.eq(target)]
        for setup, target in current.items()]).set_index("sid").sort_index()
    ordered = added.set_index("sid").sort_index()
    audit.check("coordinate selected entry IDs", ordered.index.equals(selected_cache.index))
    for field in ("entry_price", "exit_price", "native_target_pct", "net_return_pct", "gross_return_pct", "cost_pct"):
        audit.check("coordinate final cache " + field, np.allclose(ordered[field], selected_cache[field], rtol=0, atol=1e-9, equal_nan=True))
    for field in ("entry_ts", "exit_ts", "confirmation_ts"):
        audit.check("coordinate final cache time " + field,
            pd.to_datetime(ordered[field], utc=True, errors="coerce").equals(pd.to_datetime(selected_cache[field], utc=True, errors="coerce")))
    setup_results = audit.read(folder / "scenario_setup_results.csv")
    if require_native70:
        periods = dict(train=days[:38], holdout=days[38:], all=days)
        for row in setup_results.to_dict("records"):
            frame = scenarios[row["scenario"]]
            native = frame.loc[frame.setup_id.eq(row["setup_id"]) & frame.day.isin(periods[row["period"]]) & frame.filled]
            pnl = native.net_return_pct * 5000.
            wins = int(pnl.gt(0).sum())
            name = row["scenario"] + row["setup_id"] + row["period"]
            for metric, value in [("fills", len(pnl)), ("wins", wins),
                ("win_rate_pct", 100 * wins / len(pnl) if len(pnl) else None), ("net_pnl_rs", pnl.sum())]:
                audit.near("coordinate standalone native " + name + metric, row["standalone_native_" + metric], value)
            passing = len(pnl) >= (3 if row["period"] == "holdout" else 5) and 10 * wins >= 7 * len(pnl) and pnl.sum() > 0
            audit.check("coordinate standalone native pass " + name, bool(row["standalone_native_70_positive"]) == bool(passing))
        final_setups = setup_results.loc[setup_results.scenario.eq("refined_qualified") & setup_results.period.eq("all")]
        audit.check("coordinate every final setup passes both screens", final_setups.standalone_native_70_positive.all() and final_setups.observed_70_positive.all())
    final_config = audit.load(folder / "refined_config.json")
    entry_config = audit.load(entry / "selected_config.json")
    original_specs = {s["setup_id"]: s for s in entry_config["setups"]}
    audit.check("coordinate portable targets", {s["setup_id"]: s["target_pct"] for s in final_config["setups"]} == current)
    if require_native70:
        audit.check("coordinate portable native guard", final_config["require_native70"] is True)
    for row in final_config["setups"]:
        prior = original_specs[row["setup_id"]]
        for key in ("thresholds", "signal_end", "confirmation_end", "side", "stage", "initial_stop_pct",
                    "tightened_stop_pct", "tighten_after_minutes", "max_entries", "picker"):
            audit.check("coordinate fixed rule " + row["setup_id"] + key, row[key] == prior[key])
    receipt = dict(status="PASS", kind="coordinate", run=str(folder), checks=len(audit.CHECKS),
        require_native70=require_native70,
        all_coordinate_candidates_independently_replayed=replayed, final_setups=len(current),
        combined=audit.heap.stats(final, days), additions=audit.heap.stats(added, days),
        accepted_changes=accepted_count, local_coordinate_convergence=summary["local_coordinate_convergence"],
        targets=current, all_checks=audit.CHECKS)
    filename = "independent_coordinate_dual70_validation.json" if require_native70 else "independent_coordinate_validation.json"
    (audit.HERE / filename).write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in receipt.items() if key != "all_checks"}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
