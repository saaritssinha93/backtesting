from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

import run_exit_robustness as ex


OUT = Path(__file__).resolve().parent
REJECTED_PATH = ex.V3_DIR / "fno_v13_corrected_v3_rejected_v13_v2_trades.csv"
TRAIN_END_EXCLUSIVE = date(2026, 8, 14)
VALIDATION_END = date(2026, 8, 26)


def split_days(days: list[date]) -> dict[str, list[date]]:
    return {
        "train": [d for d in days if d < TRAIN_END_EXCLUSIVE],
        "validation": [d for d in days if TRAIN_END_EXCLUSIVE <= d <= VALIDATION_END],
        "pseudo_test": [d for d in days if d > VALIDATION_END],
        "development": [d for d in days if d <= VALIDATION_END],
        "all": list(days),
    }


def load_pool() -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], list[date], dict[str, Any]]:
    selected, paths, days, audit = ex.load_frozen()
    rejected = pd.read_csv(REJECTED_PATH)
    rejected["day"] = pd.to_datetime(rejected["day"]).dt.date
    rejected["sid"] = rejected["sid"].astype(int)
    rejected["hhmm_int"] = rejected["hhmm_int"].astype(int)
    rejected["native_stop_pct"] = rejected["stop_pct"].astype(float)
    rejected["native_target_pct"] = rejected["target_pct"].astype(float)
    common = sorted(set(selected.columns) & set(rejected.columns))
    required = ["sid", "day", "hhmm_int", "tradingsymbol", "side", "setup_id", "trigger", "native_stop_pct", "native_target_pct", "nifty_first_bar_return_pct"]
    for col in required:
        if col not in common:
            raise RuntimeError(f"Missing required pool column: {col}")
    pool = pd.concat([selected[common], rejected[common]], ignore_index=True)
    pool = pool.drop_duplicates("sid", keep="first").sort_values(["day", "hhmm_int", "side", "sid"]).reset_index(drop=True)
    if len(pool) != len(selected) + len(rejected):
        raise RuntimeError("Unexpected duplicate sid between selected and rejected pools")
    pool["nifty_first_bar_return_pct"] = pd.to_numeric(pool["nifty_first_bar_return_pct"], errors="coerce")
    if pool["nifty_first_bar_return_pct"].isna().any():
        raise RuntimeError("Missing NIFTY first-bar context in candidate pool")
    per_day_nunique = pool.groupby("day")["nifty_first_bar_return_pct"].nunique(dropna=False)
    if int(per_day_nunique.max()) != 1:
        raise RuntimeError("NIFTY first-bar context is not constant within day")
    audit.update(
        {
            "rejected_path": str(REJECTED_PATH),
            "rejected_sha256": ex.sha256(REJECTED_PATH),
            "ungated_pool_orders": len(pool),
            "published_selected_orders": len(selected),
            "published_rejected_orders": len(rejected),
            "split_policy": {name: [str(d) for d in values] for name, values in split_days(days).items()},
        }
    )
    return pool, paths, days, audit


def current_mask(pool: pd.DataFrame, threshold: float = 0.05) -> pd.Series:
    applies = pool["hhmm_int"].eq(925) & pool["side"].eq("SHORT")
    return (~applies) | pool["nifty_first_bar_return_pct"].le(-threshold)


def side_alignment(pool: pd.DataFrame, slot: int, side: str, threshold: float, *, anti_opposition: bool = False) -> pd.Series:
    applies = pool["hhmm_int"].eq(slot) & pool["side"].eq(side)
    ret = pool["nifty_first_bar_return_pct"]
    if anti_opposition:
        passes = ret.ge(-threshold) if side == "LONG" else ret.le(threshold)
    else:
        passes = ret.ge(threshold) if side == "LONG" else ret.le(-threshold)
    return (~applies) | passes


def symmetric_alignment(pool: pd.DataFrame, slot: int, threshold: float, *, anti_opposition: bool = False) -> pd.Series:
    return side_alignment(pool, slot, "LONG", threshold, anti_opposition=anti_opposition) & side_alignment(pool, slot, "SHORT", threshold, anti_opposition=anti_opposition)


def experiment_masks(pool: pd.DataFrame) -> list[dict[str, Any]]:
    experiments: list[dict[str, Any]] = []

    def add(name: str, family: str, description: str, mask: pd.Series, params: dict[str, Any]) -> None:
        experiments.append({"experiment": name, "family": family, "description": description, "mask": mask.astype(bool), "params": params})

    baseline = current_mask(pool, 0.05)
    add("CURRENT_0925S_REQ_0.050", "BASELINE", "Published V13-v3: only 09:25 SHORT requires NIFTY 09:15-09:20 return <= -0.05%", baseline, {"slot": 925, "side": "SHORT", "threshold_pct": 0.05, "mode": "STRICT_ALIGNMENT"})
    add("ABLATE_0925S_OFF", "ABLATION", "Remove the published first-bar gate", pd.Series(True, index=pool.index), {"mode": "OFF"})

    # Replace only the published threshold; no other gate changes.
    for threshold in [0.00, 0.025, 0.075, 0.10, 0.15]:
        add(f"REPLACE_0925S_REQ_{threshold:.3f}", "0925_SHORT_THRESHOLD", f"Replace 09:25 SHORT required bearish alignment with {threshold:.3f}%", current_mask(pool, threshold), {"slot": 925, "side": "SHORT", "threshold_pct": threshold, "mode": "STRICT_ALIGNMENT"})

    # Add one gate at a time on top of the published policy.
    for threshold in [0.00, 0.025, 0.05, 0.075, 0.10]:
        mask = baseline & side_alignment(pool, 925, "LONG", threshold)
        add(f"ADD_0925L_REQ_{threshold:.3f}", "0925_LONG_SYMMETRY", f"Keep published 09:25 SHORT gate; add 09:25 LONG bullish alignment {threshold:.3f}%", mask, {"slot": 925, "side": "LONG", "threshold_pct": threshold, "mode": "STRICT_ALIGNMENT"})

    slots = [930, 935, 940, 945, 955, 1000]
    thresholds = [0.00, 0.025, 0.05, 0.075, 0.10]
    for slot in slots:
        for side in ["LONG", "SHORT"]:
            if not ((pool["hhmm_int"].eq(slot)) & pool["side"].eq(side)).any():
                continue
            for threshold in thresholds:
                mask = baseline & side_alignment(pool, slot, side, threshold)
                add(f"ADD_{slot:04d}_{side}_REQ_{threshold:.3f}", "ONE_SLOT_SIDE_STRICT", f"Keep published policy; add strict first-bar alignment to {slot:04d} {side} only", mask, {"slot": slot, "side": side, "threshold_pct": threshold, "mode": "STRICT_ALIGNMENT"})
            # Anti-opposition is a weaker causal alternative: reject only a strongly opposing first bar.
            for threshold in [0.025, 0.05, 0.10]:
                mask = baseline & side_alignment(pool, slot, side, threshold, anti_opposition=True)
                add(f"ADD_{slot:04d}_{side}_ANTI_{threshold:.3f}", "ONE_SLOT_SIDE_ANTI_OPPOSITION", f"Keep published policy; for {slot:04d} {side}, reject only NIFTY first bars opposing by more than {threshold:.3f}%", mask, {"slot": slot, "side": side, "threshold_pct": threshold, "mode": "ANTI_OPPOSITION"})

    for slot in [925, 930, 935, 940, 945]:
        for threshold in [0.025, 0.05, 0.10]:
            mask = baseline & symmetric_alignment(pool, slot, threshold)
            add(f"ADD_{slot:04d}_SYMMETRIC_REQ_{threshold:.3f}", "ONE_SLOT_SYMMETRIC", f"Keep published policy; require sign-aligned first bar on both sides at {slot:04d}", mask, {"slot": slot, "side": "BOTH", "threshold_pct": threshold, "mode": "STRICT_ALIGNMENT"})
            anti = baseline & symmetric_alignment(pool, slot, threshold, anti_opposition=True)
            add(f"ADD_{slot:04d}_SYMMETRIC_ANTI_{threshold:.3f}", "ONE_SLOT_SYMMETRIC_ANTI_OPPOSITION", f"Keep published policy; reject only opposing first bars on both sides at {slot:04d}", anti, {"slot": slot, "side": "BOTH", "threshold_pct": threshold, "mode": "ANTI_OPPOSITION"})

    for threshold in [0.025, 0.05, 0.10]:
        strict = baseline.copy()
        anti = baseline.copy()
        for slot in sorted(pool["hhmm_int"].unique()):
            strict &= symmetric_alignment(pool, int(slot), threshold)
            anti &= symmetric_alignment(pool, int(slot), threshold, anti_opposition=True)
        add(f"ADD_ALL_SYMMETRIC_REQ_{threshold:.3f}", "GLOBAL_SYMMETRIC", "Keep published policy; require strict side-aligned first bar for every setup", strict, {"slot": "ALL", "side": "BOTH", "threshold_pct": threshold, "mode": "STRICT_ALIGNMENT"})
        add(f"ADD_ALL_SYMMETRIC_ANTI_{threshold:.3f}", "GLOBAL_SYMMETRIC_ANTI_OPPOSITION", "Keep published policy; reject opposing first bars for every setup", anti, {"slot": "ALL", "side": "BOTH", "threshold_pct": threshold, "mode": "ANTI_OPPOSITION"})
    return experiments


def metrics_for(audit: pd.DataFrame, split: dict[str, list[date]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for period_name, days in split.items():
        for key, value in ex.metric(audit, days).items():
            out[f"{period_name}_{key}"] = value
    return out


def evaluate(pool: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], days: list[date], experiments: list[dict[str, Any]]) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    native_pool = ex.simulate_fixed(pool, paths, label="V3_NATIVE")
    v4_pool = ex.simulate_scaleout(pool, paths, ex.Scaleout(), label="V4")
    by_engine = {"V3_NATIVE": native_pool, "V4": v4_pool}
    rows: list[dict[str, Any]] = []
    splits = split_days(days)
    for exp in experiments:
        keep_sids = set(pool.loc[exp["mask"], "sid"].astype(int))
        for engine, full_audit in by_engine.items():
            audit = full_audit.loc[full_audit["sid"].astype(int).isin(keep_sids)].copy()
            rows.append(
                {
                    "experiment": exp["experiment"],
                    "family": exp["family"],
                    "description": exp["description"],
                    "exit_engine": engine,
                    "params_json": json.dumps(exp["params"], sort_keys=True),
                    "ungated_pool_orders": len(pool),
                    "kept_orders": len(keep_sids),
                    "removed_orders": len(pool) - len(keep_sids),
                    **metrics_for(audit, splits),
                }
            )
    return pd.DataFrame(rows), by_engine


def development_ranking(results: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    # Deliberately drop pseudo-test/all fields before pivoting or ranking.
    forbidden = [c for c in results.columns if c.startswith("pseudo_test_") or c.startswith("all_")]
    dev = results.drop(columns=forbidden).copy()
    metric_cols = [
        "train_fills", "train_win_rate_pct", "train_pf", "train_net_pct", "train_max_drawdown_pct",
        "validation_fills", "validation_win_rate_pct", "validation_pf", "validation_net_pct", "validation_max_drawdown_pct",
        "development_fills", "development_win_rate_pct", "development_pf", "development_net_pct", "development_max_drawdown_pct",
    ]
    identity = ["experiment", "family", "description", "params_json", "ungated_pool_orders", "kept_orders", "removed_orders"]
    wide_parts = []
    for engine, suffix in [("V3_NATIVE", "v3"), ("V4", "v4")]:
        part = dev.loc[dev["exit_engine"].eq(engine), identity + metric_cols].copy()
        part = part.rename(columns={col: f"{suffix}_{col}" for col in metric_cols})
        wide_parts.append(part)
    wide = wide_parts[0].merge(wide_parts[1], on=identity, how="inner", validate="one_to_one")
    baseline = wide.loc[wide["experiment"].eq("CURRENT_0925S_REQ_0.050")].iloc[0]
    for suffix in ["v3", "v4"]:
        for period in ["train", "validation", "development"]:
            for metric_name in ["fills", "win_rate_pct", "pf", "net_pct", "max_drawdown_pct"]:
                col = f"{suffix}_{period}_{metric_name}"
                wide[f"delta_{col}"] = wide[col].astype(float) - float(baseline[col])
    wide["min_train_validation_pf"] = wide[["v3_train_pf", "v3_validation_pf", "v4_train_pf", "v4_validation_pf"]].min(axis=1)
    wide["min_train_validation_net"] = wide[["v3_train_net_pct", "v3_validation_net_pct", "v4_train_net_pct", "v4_validation_net_pct"]].min(axis=1)
    wide["mean_development_pf"] = wide[["v3_development_pf", "v4_development_pf"]].mean(axis=1)
    wide["min_development_fills"] = wide[["v3_development_fills", "v4_development_fills"]].min(axis=1)
    baseline = wide.loc[wide["experiment"].eq("CURRENT_0925S_REQ_0.050")].iloc[0]
    wide["beats_baseline_pf_both_engines_splits"] = (
        wide["delta_v3_train_pf"].ge(0)
        & wide["delta_v3_validation_pf"].ge(0)
        & wide["delta_v4_train_pf"].ge(0)
        & wide["delta_v4_validation_pf"].ge(0)
    )
    wide["beats_baseline_pf_wr_fills_development_both_engines"] = (
        wide["delta_v3_development_pf"].ge(0)
        & wide["delta_v4_development_pf"].ge(0)
        & wide["delta_v3_development_win_rate_pct"].ge(0)
        & wide["delta_v4_development_win_rate_pct"].ge(0)
        & wide["delta_v3_development_fills"].ge(0)
        & wide["delta_v4_development_fills"].ge(0)
    )
    eligible = wide.loc[
        ~wide["family"].eq("BASELINE")
        & wide["v3_train_net_pct"].gt(0)
        & wide["v3_validation_net_pct"].gt(0)
        & wide["v4_train_net_pct"].gt(0)
        & wide["v4_validation_net_pct"].gt(0)
        & wide["min_development_fills"].ge(float(baseline["min_development_fills"]) * 0.80)
    ].copy()
    robust = eligible.sort_values(["min_train_validation_pf", "mean_development_pf", "min_development_fills"], ascending=False)
    count = eligible.loc[
        eligible["v3_development_pf"].ge(float(baseline["v3_development_pf"]))
        & eligible["v4_development_pf"].ge(float(baseline["v4_development_pf"]))
    ].sort_values(["min_development_fills", "mean_development_pf"], ascending=False)
    selected: list[str] = ["CURRENT_0925S_REQ_0.050", "ABLATE_0925S_OFF"]
    if len(robust):
        selected.append(str(robust.iloc[0]["experiment"]))
    if len(count):
        selected.append(str(count.iloc[0]["experiment"]))
    selected = list(dict.fromkeys(selected))
    wide["development_selected_for_pseudo_reveal"] = wide["experiment"].isin(selected)
    wide = wide.sort_values(["development_selected_for_pseudo_reveal", "min_train_validation_pf", "mean_development_pf"], ascending=[False, False, False])
    return wide, selected


def make_pseudo_reveal(results: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    cols = ["experiment", "family", "description", "exit_engine", "kept_orders"] + [c for c in results.columns if c.startswith("pseudo_test_")]
    reveal = results.loc[results["experiment"].isin(selected), cols].copy()
    reveal["selection_basis"] = "Experiment names frozen using TRAIN+VALIDATION table before pseudo-test columns were joined"
    return reveal


def family_champions(dev: pd.DataFrame) -> pd.DataFrame:
    """Freeze one representative per requested family using development columns only."""
    baseline = dev.loc[dev["experiment"].eq("CURRENT_0925S_REQ_0.050")].iloc[0]
    requested = [
        "0925_LONG_SYMMETRY",
        "0925_SHORT_THRESHOLD",
        "ONE_SLOT_SIDE_STRICT",
        "ONE_SLOT_SIDE_ANTI_OPPOSITION",
        "ONE_SLOT_SYMMETRIC",
        "ONE_SLOT_SYMMETRIC_ANTI_OPPOSITION",
        "GLOBAL_SYMMETRIC",
        "GLOBAL_SYMMETRIC_ANTI_OPPOSITION",
    ]
    rows: list[pd.Series] = []
    for family in requested:
        candidates = dev.loc[
            dev["family"].eq(family)
            & dev["min_train_validation_net"].gt(0)
            & dev["min_development_fills"].ge(float(baseline["min_development_fills"]) * 0.80)
        ].copy()
        if not len(candidates):
            continue
        candidates = candidates.sort_values(["min_train_validation_pf", "mean_development_pf", "min_development_fills"], ascending=False)
        rows.append(candidates.iloc[0])
    if not rows:
        return dev.iloc[0:0].copy()
    out = pd.DataFrame(rows).reset_index(drop=True)
    out["freeze_basis"] = "Highest cross-engine TRAIN/VALIDATION PF floor subject to positive split net and >=80% of baseline development fills; no pseudo/all columns available"
    return out


def day_audit(pool: pd.DataFrame, experiments: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    day_ret = pool.groupby("day")["nifty_first_bar_return_pct"].first()
    selected_names = {"CURRENT_0925S_REQ_0.050", "ABLATE_0925S_OFF"}
    for exp in experiments:
        if exp["experiment"] not in selected_names and exp["family"] not in {"0925_SHORT_THRESHOLD", "0925_LONG_SYMMETRY"}:
            continue
        kept = pool.loc[exp["mask"]].groupby("day").size()
        removed = pool.loc[~exp["mask"]].groupby("day").size()
        for day, ret in day_ret.items():
            rows.append({"experiment": exp["experiment"], "day": day, "period": "TRAIN" if day < TRAIN_END_EXCLUSIVE else ("VALIDATION" if day <= VALIDATION_END else "PSEUDO_TEST"), "nifty_first_bar_return_pct": ret, "kept_orders": int(kept.get(day, 0)), "removed_orders": int(removed.get(day, 0))})
    return pd.DataFrame(rows)


def rejected_trade_audit(pool: pd.DataFrame, by_engine: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rejected = pool.loc[~current_mask(pool, 0.05), ["sid", "day", "setup_id", "side", "tradingsymbol", "nifty_first_bar_return_pct"]].copy()
    rejected["period"] = rejected["day"].map(lambda d: "TRAIN" if d < TRAIN_END_EXCLUSIVE else ("VALIDATION" if d <= VALIDATION_END else "PSEUDO_TEST"))
    for engine, prefix in [("V3_NATIVE", "v3"), ("V4", "v4")]:
        cols = by_engine[engine].loc[:, ["sid", "filled", "net_return_pct", "objective_hit", "exit_reason"]].copy()
        cols = cols.rename(columns={c: f"{prefix}_{c}" for c in cols.columns if c != "sid"})
        rejected = rejected.merge(cols, on="sid", how="left", validate="one_to_one")
    return rejected.sort_values(["day", "sid"]).reset_index(drop=True)


def report(results: pd.DataFrame, dev: pd.DataFrame, selected: list[str], reveal: pd.DataFrame, champions: pd.DataFrame, audit: dict[str, Any]) -> str:
    baseline = results.loc[results["experiment"].eq("CURRENT_0925S_REQ_0.050")]
    off = results.loc[results["experiment"].eq("ABLATE_0925S_OFF")]
    lines = [
        "# Causal NIFTY First-Bar Alignment Ablation",
        "",
        "## Design",
        "",
        f"- Candidate pool: {audit['ungated_pool_orders']} orders = {audit['published_selected_orders']} published V13-v3 orders plus {audit['published_rejected_orders']} contemporaneously rejected 09:25 SHORT orders.",
        "- Context is the completed 09:15-09:20 near-month NIFTY futures bar, already available before each tested signal/confirmation. The value is constant within each session.",
        "- Every extension changes only one slot/side rule on top of the published 09:25 SHORT <= -0.05% gate. Native V3 and published V4 exits are both replayed.",
        "- Selection code physically drops ALL and PSEUDO_TEST columns before ranking. The pseudo-test is still not truly untouched historically because V13-v3 itself was designed after viewing this 25-session history.",
        "",
        "## Published gate versus ablation",
        "",
        "| Exit | Rule | Dev fills | Dev win % | Dev PF | Dev net % | Pseudo fills | Pseudo win % | Pseudo PF | Pseudo net % |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for engine in ["V3_NATIVE", "V4"]:
        for label, frame in [("current", baseline), ("gate off", off)]:
            row = frame.loc[frame["exit_engine"].eq(engine)].iloc[0]
            lines.append(f"| {engine} | {label} | {int(row['development_fills'])} | {row['development_win_rate_pct']:.3f} | {row['development_pf']:.3f} | {row['development_net_pct']:.3f} | {int(row['pseudo_test_fills'])} | {row['pseudo_test_win_rate_pct']:.3f} | {row['pseudo_test_pf']:.3f} | {row['pseudo_test_net_pct']:.3f} |")

    lines += [
        "",
        "## Development-frozen reveal set",
        "",
        "The following experiment names were frozen from TRAIN+VALIDATION only before pseudo-test metrics were exposed:",
        "",
    ]
    for name in selected:
        lines.append(f"- `{name}`")
    lines += [
        "",
        "## Development-only family representatives",
        "",
        "| Family | Frozen representative | Kept orders | V3 dev PF | V4 dev PF | Cross-engine/split PF floor |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in champions.itertuples(index=False):
        lines.append(f"| {row.family} | {row.experiment} | {int(row.kept_orders)} | {float(row.v3_development_pf):.3f} | {float(row.v4_development_pf):.3f} | {float(row.min_train_validation_pf):.3f} |")
    def v4row(name: str) -> pd.Series:
        return results.loc[results["experiment"].eq(name) & results["exit_engine"].eq("V4")].iloc[0]
    current_v4 = v4row("CURRENT_0925S_REQ_0.050")
    off_v4 = v4row("ABLATE_0925S_OFF")
    strict_v4 = v4row("REPLACE_0925S_REQ_0.150")
    long_v4 = v4row("ADD_0925L_REQ_0.075")
    slot_v4 = v4row("ADD_0930_LONG_REQ_0.050")
    anti_v4 = v4row("ADD_0940_LONG_ANTI_0.100")
    global_v4 = v4row("ADD_ALL_SYMMETRIC_REQ_0.050")
    lines += [
        "",
        "## Decision readout",
        "",
        f"- Turning the gate off adds 5 fills, but all five added trades lose under both exit engines. With V4, all-history fills rise {int(current_v4['all_fills'])}->{int(off_v4['all_fills'])}, while win rate falls {current_v4['all_win_rate_pct']:.3f}%->{off_v4['all_win_rate_pct']:.3f}%, PF {current_v4['all_pf']:.3f}->{off_v4['all_pf']:.3f}, net {current_v4['all_net_pct']:.3f}%->{off_v4['all_net_pct']:.3f}%, and DD {current_v4['all_max_drawdown_pct']:.3f}%->{off_v4['all_max_drawdown_pct']:.3f}%.",
        f"- The development winner, stricter 09:25 SHORT 0.15%, cuts total orders to {int(strict_v4['all_orders'])}; on pseudo-test it underperforms current V4 (PF {strict_v4['pseudo_test_pf']:.3f} vs {current_v4['pseudo_test_pf']:.3f}, net {strict_v4['pseudo_test_net_pct']:.3f}% vs {current_v4['pseudo_test_net_pct']:.3f}%). Keep 0.05%, do not tighten.",
        f"- Adding symmetric 09:25 LONG >=+0.075% is not compelling under V4: all-history fills {int(long_v4['all_fills'])}, WR {long_v4['all_win_rate_pct']:.3f}%, PF {long_v4['all_pf']:.3f}, net {long_v4['all_net_pct']:.3f}% versus current {int(current_v4['all_fills'])}/{current_v4['all_win_rate_pct']:.3f}%/{current_v4['all_pf']:.3f}/{current_v4['all_net_pct']:.3f}%.",
        f"- Development-only hypotheses `ADD_0930_LONG_REQ_0.050` (V4 all PF {slot_v4['all_pf']:.3f}, net {slot_v4['all_net_pct']:.3f}%) and `ADD_0940_LONG_ANTI_0.100` (PF {anti_v4['all_pf']:.3f}, net {anti_v4['all_net_pct']:.3f}%) make no different decision in pseudo-test, so that segment provides zero confirmation.",
        f"- Global strict symmetric alignment is destructive to count: at 0.05% it keeps only {int(global_v4['all_orders'])} of 84 ungated orders and {int(global_v4['development_fills'])} development fills versus {int(current_v4['development_fills'])} current.",
    ]
    lines += [
        "",
        "Full exact metrics are in `nifty_firstbar_ablation_all.csv`; the ranking and family-champion tables contain no pseudo-test/all columns. Pseudo-test results for both the core reveal set and development-frozen family representatives are stored separately.",
        "",
        "## Interpretation cautions",
        "",
        "- Strict alignment gates can improve PF/win rate only by removing trades; they cannot increase trade count. The gate-off ablation is the only tested first-bar change that can restore the five excluded orders.",
        "- Slot/side cells are tiny. Even one-factor rules face a large multiple-testing burden across thresholds and time slots; development winners are hypotheses, not deployable evidence.",
        "- NIFTY alignment may improve win rate when market direction is causally informative, but there is no general guarantee. Require neighborhood stability and genuinely new forward sessions.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    pool, paths, days, audit = load_pool()
    experiments = experiment_masks(pool)
    results, by_engine = evaluate(pool, paths, days, experiments)
    dev, selected = development_ranking(results)
    reveal = make_pseudo_reveal(results, selected)
    champions = family_champions(dev)
    family_names = champions["experiment"].astype(str).tolist()
    family_reveal = make_pseudo_reveal(results, family_names)
    daily = day_audit(pool, experiments)
    rejected_details = rejected_trade_audit(pool, by_engine)

    # Exact baseline parity against the exit replay.
    current_v3 = results.loc[(results["experiment"].eq("CURRENT_0925S_REQ_0.050")) & results["exit_engine"].eq("V3_NATIVE")].iloc[0]
    current_v4 = results.loc[(results["experiment"].eq("CURRENT_0925S_REQ_0.050")) & results["exit_engine"].eq("V4")].iloc[0]
    parity = {
        "v3_orders": int(current_v3["all_orders"]),
        "v3_fills": int(current_v3["all_fills"]),
        "v3_net_pct": float(current_v3["all_net_pct"]),
        "v4_orders": int(current_v4["all_orders"]),
        "v4_fills": int(current_v4["all_fills"]),
        "v4_net_pct": float(current_v4["all_net_pct"]),
        "passed": int(current_v3["all_orders"]) == 79 and int(current_v3["all_fills"]) == 78 and abs(float(current_v3["all_net_pct"]) - 46.03449112112943) < 1e-10 and abs(float(current_v4["all_net_pct"]) - 42.000023532190255) < 1e-10,
    }
    if not parity["passed"]:
        raise RuntimeError(f"First-bar baseline parity failed: {parity}")
    audit["baseline_parity"] = parity
    audit["experiment_count"] = len(experiments)
    audit["development_selected_names"] = selected

    results.to_csv(OUT / "nifty_firstbar_ablation_all.csv", index=False)
    dev.to_csv(OUT / "nifty_firstbar_development_ranking.csv", index=False)
    reveal.to_csv(OUT / "nifty_firstbar_pseudotest_reveal.csv", index=False)
    champions.to_csv(OUT / "nifty_firstbar_family_champions_development.csv", index=False)
    family_reveal.to_csv(OUT / "nifty_firstbar_family_champions_pseudotest.csv", index=False)
    daily.to_csv(OUT / "nifty_firstbar_day_audit.csv", index=False)
    rejected_details.to_csv(OUT / "nifty_firstbar_published_gate_rejections.csv", index=False)
    ex.write_json(OUT / "nifty_firstbar_metadata.json", audit)
    (OUT / "NIFTY_FIRSTBAR_ABLATION.md").write_text(report(results, dev, selected, reveal, champions, audit), encoding="utf-8")

    # Refresh isolated manifest after adding this bounded add-on.
    manifest = {"generated_by": [str((OUT / "run_exit_robustness.py").resolve()), str(Path(__file__).resolve())], "rng_seed": ex.RNG_SEED, "outputs": []}
    for path in sorted(OUT.iterdir()):
        if path.is_file() and path.name != "artifact_manifest.json":
            manifest["outputs"].append({"path": str(path), "bytes": path.stat().st_size, "sha256": ex.sha256(path)})
    ex.write_json(OUT / "artifact_manifest.json", manifest)
    print(json.dumps({"experiments": len(experiments), "rows": len(results), "selected_before_pseudo": selected, "parity": parity}, indent=2))


if __name__ == "__main__":
    main()
