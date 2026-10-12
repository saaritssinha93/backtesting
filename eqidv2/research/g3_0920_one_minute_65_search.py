"""Backtest-only 09:21 confirmation relaxation with fixed 09:20 G-3 signal gates.

Every dated universe is reranked at each step. This is exploratory historical
research: 09:16 OI inputs were assembled retrospectively, and prior 09:20
results were already inspected. No production setup or backtest pointer moves.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, replace
from datetime import datetime
import json
import math
import os
from pathlib import Path

import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_parameter_search as previous
from research.g3_0920_strategy import (
    RESEARCH_VERSION, OneMinuteSelection, SelectionProfile,
    reference_profile, selection_audit_0920,
)


SEARCH_VERSION = "G3_0920_FIXED_5M_STEPWISE_1M_65_V1"
MIN_EXPLORATORY_FILLS = 10
TARGET_WIN_RATE_PCT = 65.0


@dataclass(frozen=True)
class Stage:
    name: str
    branch: str
    long: OneMinuteSelection
    short: OneMinuteSelection


def _stage(name: str, branch: str, body_l: float, body_s: float, wick: float,
           volume_l: float, volume_s: float) -> Stage:
    return Stage(name, branch, OneMinuteSelection(body_l, wick, volume_l),
                 OneMinuteSelection(body_s, wick, volume_s))


# Fixed before the new sweep, after reviewing the earlier coarse-grid result.
# The cumulative ladder relaxes 09:21 volume, body, then wick. Widening wick
# while body remains strict is mathematically redundant (body + wicks = 1),
# so wick changes start only after body has been relaxed. Probes test body at
# stricter volume. Five-minute gates
# never vary, and the native LONG 1.10 / SHORT 1.20 1m volume floors remain.
STAGES = (
    _stage("strict", "cumulative", .75, .65, .35, 2.0, 2.0),
    _stage("volume_190", "cumulative", .75, .65, .35, 1.9, 1.9),
    _stage("volume_180", "cumulative", .75, .65, .35, 1.8, 1.8),
    _stage("volume_160_170", "cumulative", .75, .65, .35, 1.6, 1.7),
    _stage("volume_140_150", "cumulative", .75, .65, .35, 1.4, 1.5),
    _stage("volume_native", "cumulative", .75, .65, .35, 1.1, 1.2),
    _stage("body_072_062", "cumulative", .72, .62, .35, 1.1, 1.2),
    _stage("body_070_060", "cumulative", .70, .60, .35, 1.1, 1.2),
    _stage("body_068_055", "cumulative", .68, .55, .35, 1.1, 1.2),
    _stage("body_065_050", "cumulative", .65, .50, .35, 1.1, 1.2),
    _stage("body_060_045", "cumulative", .60, .45, .35, 1.1, 1.2),
    _stage("body_060_040", "cumulative", .60, .40, .35, 1.1, 1.2),
    _stage("wick_040", "cumulative", .60, .40, .40, 1.1, 1.2),
    _stage("wick_045", "cumulative", .60, .40, .45, 1.1, 1.2),
    _stage("wick_050", "cumulative", .60, .40, .50, 1.1, 1.2),
    _stage("reference_1m", "cumulative", .60, .40, .60, 1.1, 1.2),
    _stage("probe_body_072_062", "one_axis_probe", .72, .62, .35, 2.0, 2.0),
    _stage("probe_body_070_060", "one_axis_probe", .70, .60, .35, 2.0, 2.0),
    _stage("probe_body_065_050", "one_axis_probe", .65, .50, .35, 2.0, 2.0),
    _stage("probe_volume_180_body_072", "two_axis_probe", .72, .62, .35, 1.8, 1.8),
    _stage("probe_volume_180_body_070", "two_axis_probe", .70, .60, .35, 1.8, 1.8),
    _stage("probe_volume_160_body_070", "two_axis_probe", .70, .60, .35, 1.6, 1.7),
    _stage("probe_volume_180_body_065", "two_axis_probe", .65, .50, .35, 1.8, 1.8),
    _stage("probe_volume_160_body_065", "two_axis_probe", .65, .50, .35, 1.6, 1.7),
)


def profile_for_stage(stage: Stage) -> SelectionProfile:
    reference = previous.profile_for("reference", "reference")
    return replace(reference,
                   long=replace(reference.long, one_minute=stage.long),
                   short=replace(reference.short, one_minute=stage.short))


def _qualifies(period: dict, *, minimum: int = MIN_EXPLORATORY_FILLS) -> bool:
    early = period["added_0920"]
    return bool(early["executed_trades"] >= minimum
                and early["win_rate_pct"] is not None
                and early["win_rate_pct"] >= TARGET_WIN_RATE_PCT
                and period["combined"]["win_rate_pct"] >= TARGET_WIN_RATE_PCT
                and period["incremental_pnl_rs"] > 0)


def choose_largest_65(trials: list[dict], period: str) -> dict | None:
    eligible = [trial for trial in trials if _qualifies(trial["periods"][period])]
    return max(eligible, key=lambda trial: (
        trial["periods"][period]["added_0920"]["executed_trades"],
        trial["periods"][period]["incremental_pnl_rs"],
        -trial["step"])) if eligible else None


def _flatten(trial: dict) -> dict:
    row = previous._flatten(trial)
    row["branch"] = trial["branch"]
    row["full_history_observed_65"] = _qualifies(trial["periods"]["all"])
    row["discovery_observed_65"] = _qualifies(trial["periods"]["discovery"])
    return row


def run(features_path: Path, output_dir: Path, *, pointer_path: Path = paired.DEFAULT_POINTER,
        data_root: Path = RUNTIME_ROOT) -> dict:
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Research output must be new or empty: {output_dir}")
    published, days, baseline_evidence = paired.load_published(pointer_path)
    features_path = Path(features_path).resolve()
    all_features, coverage, feature_evidence = paired.load_feature_evidence(features_path)
    universe_evidence = paired.validate_feature_universe(all_features, data_root, coverage)
    if not set(days).issubset(previous.paired._feature_days(all_features)):
        raise ValueError("Sealed 09:20 feature export omitted published G-3 days")
    features = all_features.loc[all_features.session_date.astype(str).isin({str(d) for d in days})].copy()
    excluded_days = sorted(set(paired._feature_days(all_features)) - set(days))
    baseline = paired.allocate(published)
    fixed_five = previous.profile_for("reference", "reference")
    if fixed_five != reference_profile():
        raise RuntimeError("Reference 09:20 profile drifted from copied G-3 rules")

    records = []
    union_orders: dict[int, dict] = {}
    for step, stage in enumerate(STAGES, 1):
        profile = profile_for_stage(stage)
        if (profile.long.five_minute != fixed_five.long.five_minute
                or profile.short.five_minute != fixed_five.short.five_minute):
            raise RuntimeError("09:20 five-minute gates changed in a one-minute-only search")
        audit = selection_audit_0920(features, profile=profile)
        orders = audit.loc[audit.v9_selected.eq(True)].copy().reset_index(drop=True)
        if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
            raise ValueError("Research order IDs overlap the published baseline")
        for row in orders.to_dict("records"):
            sid = int(row["sid"])
            if sid in union_orders and union_orders[sid]["signal_id"] != row["signal_id"]:
                raise RuntimeError("Candidate identity changed between one-minute profiles")
            union_orders.setdefault(sid, row)
        records.append(dict(step=step, five_tier="reference", one_tier=stage.name,
                            branch=stage.branch, profile_id=f"5m_reference__1m_{stage.name}",
                            profile=asdict(profile), orders=orders,
                            filter_pass=int(audit.v9_filter_pass.sum())))
        print(f"{step:02d}/{len(STAGES)} {stage.name}: {len(orders)} selected, "
              f"{records[-1]['filter_pass']} filter-pass", flush=True)

    if not union_orders:
        raise ValueError("No genuine 09:20 research order selected")
    union = pd.DataFrame(list(union_orders.values())).sort_values("sid").reset_index(drop=True)
    paths, path_evidence = paired.new_execution_paths(union, Path(data_root))
    pool = simulator.simulate_staged(union, paths,
                                     cost_bps=contract.ROUND_TRIP_COST_BPS,
                                     max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    pool["strategy"] = RESEARCH_VERSION
    pool["strategy_version"] = RESEARCH_VERSION
    pool["scope"] = "BACKTEST_ONLY"
    pool["execution_authority"] = False
    trade_by_sid = pool.set_index("sid", drop=False)

    trials = []
    for record in records:
        orders = record.pop("orders")
        new_sids = set(orders.sid.astype(int))
        fresh = trade_by_sid.loc[orders.sid.astype(int).tolist()].copy().reset_index(drop=True)
        for field in ("v10_g_f_core", "v9_rank_in_setup_day", "v9_decision"):
            fresh[field] = orders[field].to_numpy()
        variant = paired.allocate(pd.concat((published, fresh), ignore_index=True, sort=False))
        comparison, daily = paired.compare_ledgers(baseline, variant, days, days, new_sids)
        trial = {**record, "periods": previous._trial_stats(baseline, variant, new_sids, days),
                 "old_trade_effect": comparison["old_trade_effect"]}
        trial["discovery_qualified"] = _qualifies(trial["periods"]["discovery"])
        trials.append(trial)
        if record["one_tier"] == "strict":
            if (len(orders) != 9 or comparison["added_0920"]["wins"] != 7
                    or not math.isclose(comparison["delta_net_pnl_rs"], 23153.427113,
                                        rel_tol=0, abs_tol=.02)):
                raise RuntimeError("Strict 1m replay does not match the prior sealed search")
        if record["one_tier"] == "reference_1m":
            if (len(orders) != 19 or comparison["added_0920"]["wins"] != 8
                    or not math.isclose(comparison["delta_net_pnl_rs"], -4699.549812,
                                        rel_tol=0, abs_tol=.02)):
                raise RuntimeError("Reference 1m replay does not match the prior sealed search")
        record["_orders"], record["_variant"], record["_daily"] = orders, variant, daily

    best_all = choose_largest_65(trials, "all")
    best_discovery = choose_largest_65(trials, "discovery")
    best_all_record = next((r for r in records if best_all and r["step"] == best_all["step"]), None)
    best_discovery_record = next((r for r in records if best_discovery and r["step"] == best_discovery["step"]), None)
    max_recent_orders = max(t["periods"]["holdout"]["added_0920"]["selected_orders"] for t in trials)
    best_recent = best_discovery["periods"]["holdout"] if best_discovery else None
    recent_validated = bool(best_recent and _qualifies(best_recent))
    summary = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
                   period_start=str(days[0]), period_end=str(days[-1]),
                   discovery_last=str(previous.DISCOVERY_LAST),
                   recent_first=str(previous.HOLDOUT_FIRST),
                   trial_count=len(trials), fixed_five_minute_profile={
                       "long": asdict(fixed_five.long.five_minute),
                       "short": asdict(fixed_five.short.five_minute)},
                   target_added_0920_win_rate_pct=TARGET_WIN_RATE_PCT,
                   minimum_exploratory_fills=MIN_EXPLORATORY_FILLS,
                   full_history_observed_65_count=sum(_qualifies(t["periods"]["all"]) for t in trials),
                   best_full_history_observed_65_profile_id=(best_all["profile_id"] if best_all else None),
                   best_full_history_observed_65_profile=(best_all["profile"] if best_all else None),
                   best_full_history_observed_65_periods=(best_all["periods"] if best_all else None),
                   best_discovery_profile_id=(best_discovery["profile_id"] if best_discovery else None),
                   best_discovery_periods=(best_discovery["periods"] if best_discovery else None),
                   best_discovery_recent_meets_65=recent_validated,
                   max_recent_selected_orders_in_grid=max_recent_orders,
                   no_recent_10_fill_validation_possible=max_recent_orders < MIN_EXPLORATORY_FILLS,
                   historical_tuning_only=True, previous_reference_outcomes_inspected=True,
                   retrospective_opening_oi=True, live_tradability_verified=False,
                   baseline_evidence=baseline_evidence, feature_evidence=feature_evidence,
                   dated_universe_evidence=universe_evidence,
                   feature_path=str(features_path), feature_sha256=paired.sha256(features_path),
                   excluded_exported_days=[str(d) for d in excluded_days],
                   complete_research_stock_days=int(features.research_feature_status.eq("COMPLETE").sum()),
                   source_path_evidence=path_evidence)
    output_dir.mkdir(parents=True, exist_ok=True)
    membership = pd.concat([
        r["_orders"].assign(profile_id=r["profile_id"], step=r["step"])[
            ["profile_id", "step", "sid", "signal_id", "day", "tradingsymbol", "side",
             "v10_g_f_core", "v9_rank_in_setup_day"]]
        for r in records], ignore_index=True)
    artifacts = [("trials.csv", pd.DataFrame([_flatten(t) for t in trials])),
                 ("selected_membership_all_profiles.csv", membership),
                 ("union_raw_trades.csv", pool)]
    for label, record in (("best_observed_65", best_all_record),
                          ("best_discovery", best_discovery_record)):
        if record is not None:
            artifacts += [(f"{label}_orders.csv", record["_orders"]),
                          (f"{label}_variant_trades.csv", record["_variant"]),
                          (f"{label}_daily_comparison.csv", record["_daily"])]
    for name, frame in artifacts:
        temporary = output_dir / ("." + name + ".part")
        frame.to_csv(temporary, index=False)
        os.replace(temporary, output_dir / name)
    (output_dir / "trials.json").write_text(json.dumps(trials, indent=2, allow_nan=False,
                                                       default=str) + "\n", encoding="utf-8")
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False,
                                                default=str) + "\n", encoding="utf-8")
    code = [Path(__file__).resolve(), Path(previous.__file__).resolve(),
            Path(__file__).with_name("g3_0920_strategy.py"),
            Path(__file__).with_name("g3_0920_backtest.py"),
            Path(contract.__file__).resolve(), Path(simulator.__file__).resolve()]
    manifest = dict(search_version=SEARCH_VERSION, execution_authority=False,
                    code_sha256={p.name: paired.sha256(p) for p in code},
                    files={p.name: paired.sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=previous.DEFAULT_FEATURES)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, default=paired.DEFAULT_POINTER)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    args = parser.parse_args(argv)
    summary = run(args.features, args.output_dir, pointer_path=args.pointer,
                  data_root=args.data_root)
    print(json.dumps({key: summary[key] for key in (
        "best_full_history_observed_65_profile_id", "best_full_history_observed_65_periods",
        "best_discovery_profile_id", "best_discovery_recent_meets_65",
        "no_recent_10_fill_validation_possible")}, indent=2, default=str))
    print(str(args.output_dir.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
