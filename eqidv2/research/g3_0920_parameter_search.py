"""Bounded, backtest-only 09:20 G-3 filter search with a chronological holdout.

Five-minute gates are swept first while the 09:21 gates remain strict. The
09:21 gates are then relaxed for each five-minute tier. Only discovery-period
outcomes rank profiles; the later period is reported as a holdout. No live
configuration, order route, or published backtest pointer is modified.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import date, datetime
import json
import math
import os
from pathlib import Path

import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research.g3_0920_strategy import (
    RESEARCH_VERSION, FiveMinuteSelection, OneMinuteSelection,
    SelectionProfile, SideSelectionProfile, reference_profile,
    selection_audit_0920,
)


DEFAULT_FEATURES = (Path(__file__).resolve().parents[1] / "outputs" /
                    "g3_0920_research_full_run_20261012T2320" /
                    "2026-07-29_through_2026-10-12" / "features.parquet")
DISCOVERY_LAST = date(2026, 9, 30)
HOLDOUT_FIRST = date(2026, 10, 5)
SEARCH_VERSION = "G3_0920_5M_THEN_1M_SEARCH_V1"

# All numbers are percent units except dimensionless volume/body/wick ratios.
# These tiers were fixed before this 16-profile sweep; the earlier reference
# 09:20 outcomes were already known and the late dates are not pristine data.
FIVE_TIERS = {
    "strict": {
        "LONG": FiveMinuteSelection(.65, .65, .25, 5.0),
        "SHORT": FiveMinuteSelection(.45, .60, .20, 3.0),
    },
    "balanced": {
        "LONG": FiveMinuteSelection(.45, .45, .15, 4.0),
        "SHORT": FiveMinuteSelection(.30, .40, .10, 2.25),
    },
    "reference": {
        "LONG": FiveMinuteSelection(.30, .30, .10, 3.0),
        "SHORT": FiveMinuteSelection(.13, .20, .05, 1.5),
    },
    "relaxed": {
        "LONG": FiveMinuteSelection(.10, .20, .05, .80),
        "SHORT": FiveMinuteSelection(.10, .15, .05, .80),
    },
}
ONE_TIERS = {
    "strict": {
        "LONG": OneMinuteSelection(.75, .35, 2.0),
        "SHORT": OneMinuteSelection(.65, .35, 2.0),
    },
    "balanced": {
        "LONG": OneMinuteSelection(.68, .45, 1.6),
        "SHORT": OneMinuteSelection(.52, .45, 1.7),
    },
    "reference": {
        "LONG": OneMinuteSelection(.60, .60, 1.10),
        "SHORT": OneMinuteSelection(.40, .60, 1.20),
    },
    "relaxed": {
        "LONG": OneMinuteSelection(.35, .75, 1.10),
        "SHORT": OneMinuteSelection(.25, .75, 1.20),
    },
}


def profile_for(five_tier: str, one_tier: str) -> SelectionProfile:
    return SelectionProfile(
        long=SideSelectionProfile(FIVE_TIERS[five_tier]["LONG"], ONE_TIERS[one_tier]["LONG"]),
        short=SideSelectionProfile(FIVE_TIERS[five_tier]["SHORT"], ONE_TIERS[one_tier]["SHORT"]),
    )


def search_grid() -> list[tuple[str, str, SelectionProfile]]:
    """First loosen 5m with strict 1m; then loosen 1m at every 5m tier."""
    order = [(five, "strict") for five in FIVE_TIERS]
    order += [(five, one) for one in list(ONE_TIERS)[1:] for five in FIVE_TIERS]
    return [(five, one, profile_for(five, one)) for five, one in order]


def _period_stats(frame: pd.DataFrame, days: list[date]) -> dict:
    return paired._stats(frame.loc[frame.day.astype(str).isin({str(d) for d in days})], days)


def _trial_stats(baseline: pd.DataFrame, variant: pd.DataFrame,
                 new_sids: set[int], days: list[date]) -> dict:
    added = variant.loc[variant.sid.isin(new_sids)]
    discovery = [day for day in days if day <= DISCOVERY_LAST]
    holdout = [day for day in days if day >= HOLDOUT_FIRST]
    if discovery + holdout != days:
        raise ValueError("Unclassified published G-3 sessions in chronological split")
    result = {}
    for name, subset in (("discovery", discovery), ("holdout", holdout), ("all", days)):
        base_stats = _period_stats(baseline, subset)
        var_stats = _period_stats(variant, subset)
        add_stats = _period_stats(added, subset)
        result[name] = dict(sessions=len(subset), baseline=base_stats, combined=var_stats,
                            added_0920=add_stats,
                            incremental_pnl_rs=var_stats["net_pnl_rs"] - base_stats["net_pnl_rs"],
                            added_long=_period_stats(added.loc[added.side.eq("LONG")], subset),
                            added_short=_period_stats(added.loc[added.side.eq("SHORT")], subset))
    return result


def _qualification(trial: dict) -> bool:
    """The 70% target applies to added trades and the combined strategy."""
    part = trial["periods"]["discovery"]
    early, combined = part["added_0920"], part["combined"]
    if (early["executed_trades"] < 10 or early["win_rate_pct"] is None
            or early["win_rate_pct"] < 70 or combined["win_rate_pct"] < 70
            or part["incremental_pnl_rs"] <= 0):
        return False
    # A weak side cannot hide behind the other when it has enough observations.
    for side in ("added_long", "added_short"):
        stats = part[side]
        if stats["executed_trades"] >= 5 and stats["win_rate_pct"] < 70:
            return False
    return True


def choose_on_discovery(trials: list[dict]) -> tuple[dict, str]:
    eligible = [item for item in trials if _qualification(item)]
    if eligible:
        return max(eligible, key=lambda item: (
            item["periods"]["discovery"]["added_0920"]["executed_trades"],
            item["periods"]["discovery"]["incremental_pnl_rs"],
            -item["step"])), "DISCOVERY_70_PCT_AND_POSITIVE_PNL"
    # Still provide a concrete diagnostic candidate, without labelling it approved.
    candidates = [item for item in trials if item["periods"]["discovery"]["added_0920"]["executed_trades"] >= 10]
    if candidates:
        return max(candidates, key=lambda item: (
            item["periods"]["discovery"]["added_0920"]["win_rate_pct"],
            item["periods"]["discovery"]["incremental_pnl_rs"],
            item["periods"]["discovery"]["added_0920"]["executed_trades"],
            -item["step"])), "EXPLORATORY_NEAREST_DISCOVERY_WIN_RATE_MIN_10"
    return max(trials, key=lambda item: (
        item["periods"]["discovery"]["added_0920"]["executed_trades"],
        item["periods"]["discovery"]["incremental_pnl_rs"],
        -item["step"])), "EXPLORATORY_NO_PROFILE_WITH_10_DISCOVERY_FILLS"


def _flatten(trial: dict) -> dict:
    row = dict(step=trial["step"], five_tier=trial["five_tier"],
               one_tier=trial["one_tier"], profile_id=trial["profile_id"],
               discovery_qualified=trial["discovery_qualified"])
    for side in ("long", "short"):
        five = trial["profile"][side]["five_minute"]
        one = trial["profile"][side]["one_minute"]
        for key, value in five.items():
            row[f"{side}_5m_{key}"] = value
        for key, value in one.items():
            row[f"{side}_1m_{key}"] = value
    for period, detail in trial["periods"].items():
        for key in ("added_0920", "added_long", "added_short", "combined", "baseline"):
            for field in ("selected_orders", "native_fills", "executed_trades", "wins", "losses",
                          "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"):
                row[f"{period}_{key}_{field}"] = detail[key][field]
        row[f"{period}_incremental_pnl_rs"] = detail["incremental_pnl_rs"]
    effect = trial["old_trade_effect"]
    for field in ("displaced_count", "newly_accepted_count", "net_pnl_change_rs"):
        row[f"old_trade_{field}"] = effect[field]
    return row


def _side_status(stats: dict, *, minimum: int = 5) -> str:
    if stats["executed_trades"] < minimum:
        return "INSUFFICIENT_SAMPLE"
    return "OBSERVED_70_PCT" if stats["win_rate_pct"] >= 70 else "BELOW_70_PCT"


def run(features_path: Path, output_dir: Path, *, pointer_path: Path = paired.DEFAULT_POINTER,
        data_root: Path = RUNTIME_ROOT) -> dict:
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Research output must be new or empty: {output_dir}")
    published, days, baseline_evidence = paired.load_published(pointer_path)
    features_path = Path(features_path).resolve()
    all_features, coverage, feature_evidence = paired.load_feature_evidence(features_path)
    universe_evidence = paired.validate_feature_universe(all_features, data_root, coverage)
    exported_extra = sorted(set(paired._feature_days(all_features)) - set(days))
    if not set(days).issubset(paired._feature_days(all_features)):
        raise ValueError("Sealed opening features do not cover every published session")
    features = all_features.loc[all_features.session_date.astype(str).isin({str(d) for d in days})].copy()
    baseline = paired.allocate(published)
    if profile_for("reference", "reference") != reference_profile():
        raise RuntimeError("Search reference gates differ from the copied 09:25 rules")

    records = []
    union_orders: dict[int, dict] = {}
    for step, (five, one, profile) in enumerate(search_grid(), 1):
        audit = selection_audit_0920(features, profile=profile)
        orders = audit.loc[audit.v9_selected.eq(True)].copy().reset_index(drop=True)
        if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
            raise ValueError("Search 09:20 order IDs overlap the published baseline")
        for row in orders.to_dict("records"):
            sid = int(row["sid"])
            if sid in union_orders and union_orders[sid]["signal_id"] != row["signal_id"]:
                raise RuntimeError("Search order ID changed between profiles")
            union_orders.setdefault(sid, row)
        records.append(dict(step=step, five_tier=five, one_tier=one,
                            profile_id=f"5m_{five}__1m_{one}", profile=asdict(profile),
                            orders=orders, filter_pass=int(audit.v9_filter_pass.sum())))
        print(f"{step:02d}/{len(FIVE_TIERS)*len(ONE_TIERS)} {five}/{one}: "
              f"{len(orders)} selected, {records[-1]['filter_pass']} filter-pass", flush=True)

    union = pd.DataFrame(list(union_orders.values()))
    if union.empty:
        raise ValueError("No verified 09:20 order passed any predeclared profile")
    union = union.sort_values("sid").reset_index(drop=True)
    paths, path_evidence = paired.new_execution_paths(union, Path(data_root))
    trade_pool = simulator.simulate_staged(
        union, paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
        max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    trade_pool["strategy"] = RESEARCH_VERSION
    trade_pool["strategy_version"] = RESEARCH_VERSION
    trade_pool["scope"] = "BACKTEST_ONLY"
    trade_pool["execution_authority"] = False
    trade_by_sid = trade_pool.set_index("sid", drop=False)

    evaluated = []
    for record in records:
        orders = record.pop("orders")
        new_sids = set(orders.sid.astype(int))
        fresh = trade_by_sid.loc[orders.sid.astype(int).tolist()].copy().reset_index(drop=True)
        for column in ("v10_g_f_core", "v9_rank_in_setup_day", "v9_decision"):
            fresh[column] = orders[column].to_numpy()
        variant = paired.allocate(pd.concat((published, fresh), ignore_index=True, sort=False))
        comparison, daily = paired.compare_ledgers(baseline, variant, days, days, new_sids)
        trial = {**record, "periods": _trial_stats(baseline, variant, new_sids, days),
                 "old_trade_effect": comparison["old_trade_effect"]}
        trial["discovery_qualified"] = _qualification(trial)
        evaluated.append(trial)
        if record["five_tier"] == "reference" and record["one_tier"] == "reference":
            if (len(orders) != 19 or comparison["added_0920"]["executed_trades"] != 19
                    or comparison["added_0920"]["wins"] != 8
                    or not math.isclose(comparison["delta_net_pnl_rs"], -4699.549817,
                                        rel_tol=0, abs_tol=.02)):
                raise RuntimeError("Reference search does not reproduce the sealed 09:20 comparison")
        record["_variant"] = variant
        record["_daily"] = daily
        record["_orders"] = orders

    chosen, reason = choose_on_discovery(evaluated)
    chosen_record = next(r for r in records if r["step"] == chosen["step"])
    full_70 = [t for t in evaluated if (
        t["periods"]["all"]["added_0920"]["executed_trades"] >= 5
        and t["periods"]["all"]["added_0920"]["win_rate_pct"] >= 70
        and t["periods"]["all"]["combined"]["win_rate_pct"] >= 70)]
    observed_70 = max(full_70, key=lambda t: (
        t["periods"]["all"]["added_0920"]["executed_trades"],
        t["periods"]["all"]["incremental_pnl_rs"], -t["step"])) if full_70 else None
    observed_record = (next(r for r in records if r["step"] == observed_70["step"])
                       if observed_70 else None)
    holdout = chosen["periods"]["holdout"]
    holdout_early = holdout["added_0920"]
    holdout_observed_70 = (holdout_early["win_rate_pct"] is not None
                           and holdout_early["win_rate_pct"] >= 70
                           and holdout["combined"]["win_rate_pct"] >= 70)
    holdout_supported = (holdout_early["executed_trades"] >= 10
                         and holdout_observed_70 and holdout["incremental_pnl_rs"] > 0)
    side_status = {period: {side: _side_status(chosen["periods"][period][f"added_{side.lower()}"])
                            for side in ("LONG", "SHORT")}
                   for period in ("discovery", "holdout", "all")}
    approved = bool(chosen["discovery_qualified"] and holdout_supported
                    and all(side_status[period][side] == "OBSERVED_70_PCT"
                            for period in ("discovery", "holdout")
                            for side in ("LONG", "SHORT")))
    summary = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY",
                   execution_authority=False, created_at_ist=datetime.now(contract.IST).isoformat(),
                   sessions=len(days), period_start=str(days[0]), period_end=str(days[-1]),
                   discovery_last=str(DISCOVERY_LAST), holdout_first=str(HOLDOUT_FIRST),
                   trial_count=len(evaluated), ranking_uses_holdout=False,
                   historical_profile_70_selection_is_exploratory=True,
                   prior_reference_outcomes_previously_inspected=True,
                   selection_reason=reason, selected_profile_id=chosen["profile_id"],
                   selected_profile=chosen["profile"], discovery_qualified=chosen["discovery_qualified"],
                   best_observed_full_history_70_profile_id=(observed_70["profile_id"] if observed_70 else None),
                   best_observed_full_history_70_profile=(observed_70["profile"] if observed_70 else None),
                   best_observed_full_history_70_periods=(observed_70["periods"] if observed_70 else None),
                   holdout_observed_70_pct=holdout_observed_70,
                   holdout_minimum_10_fills_and_positive_incremental_pnl=holdout_supported,
                   approved_70_pct=approved, selected_periods=chosen["periods"],
                   side_status=side_status,
                   maximum_holdout_selected_orders_in_grid=max(
                       t["periods"]["holdout"]["added_0920"]["selected_orders"] for t in evaluated),
                   selected_old_trade_effect=chosen["old_trade_effect"],
                   baseline_evidence=baseline_evidence, feature_evidence=feature_evidence,
                   dated_universe_evidence=universe_evidence,
                   feature_path=str(features_path), feature_sha256=paired.sha256(features_path),
                   exported_dates_excluded=[str(d) for d in exported_extra],
                   complete_research_stock_days=int(features.research_feature_status.eq("COMPLETE").sum()),
                   valid_opening_oi_stock_days=int(features.oi_change_pct.notna().sum()),
                   retrospective_opening_oi=True, live_tradability_verified=False,
                   source_path_evidence=path_evidence)
    output_dir.mkdir(parents=True, exist_ok=True)
    membership = pd.concat([
        r["_orders"].assign(profile_id=r["profile_id"], step=r["step"])[
            ["profile_id", "step", "sid", "signal_id", "day", "tradingsymbol", "side",
             "v10_g_f_core", "v9_rank_in_setup_day"]]
        for r in records], ignore_index=True)
    artifact_frames = [
        ("trials.csv", pd.DataFrame([_flatten(t) for t in evaluated])),
        ("selected_membership_all_profiles.csv", membership),
        ("union_raw_trades.csv", trade_pool),
        ("selected_orders.csv", chosen_record["_orders"]),
        ("selected_variant_trades.csv", chosen_record["_variant"]),
        ("selected_daily_comparison.csv", chosen_record["_daily"]),
    ]
    if observed_record is not None:
        artifact_frames += [
            ("best_observed_70_orders.csv", observed_record["_orders"]),
            ("best_observed_70_variant_trades.csv", observed_record["_variant"]),
            ("best_observed_70_daily_comparison.csv", observed_record["_daily"]),
        ]
    for name, frame in artifact_frames:
        temporary = output_dir / ("." + name + ".part")
        frame.to_csv(temporary, index=False)
        os.replace(temporary, output_dir / name)
    public_trials = evaluated
    (output_dir / "trials.json").write_text(json.dumps(public_trials, indent=2, allow_nan=False,
                                                       default=str) + "\n", encoding="utf-8")
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False,
                                                default=str) + "\n", encoding="utf-8")
    code = [Path(__file__).resolve(), Path(__file__).with_name("g3_0920_strategy.py"),
            Path(__file__).with_name("g3_0920_backtest.py")]
    manifest = dict(search_version=SEARCH_VERSION, execution_authority=False,
                    code_sha256={p.name: paired.sha256(p) for p in code},
                    files={p.name: paired.sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, default=paired.DEFAULT_POINTER)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    args = parser.parse_args(argv)
    summary = run(args.features, args.output_dir,
                  pointer_path=args.pointer, data_root=args.data_root)
    print(json.dumps({key: summary[key] for key in (
        "selected_profile_id", "selection_reason", "approved_70_pct", "selected_periods")},
        indent=2, default=str))
    print(str(args.output_dir.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
