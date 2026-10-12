"""Backtest-only 09:20 first-bar relaxation with fixed 09:21 G-3 confirmation.

The completed five-minute price, OI, and volume filters are relaxed in a
predeclared bounded sweep. Every dated universe is reranked, then compared
against the hash-verified published G-3 baseline using unchanged exits,
sizing, costs, and portfolio allocation. No live setup or pointer changes.
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
from research import g3_0920_parameter_search as first_search
from research import g3_0920_one_minute_65_search as one_search
from research.g3_0920_strategy import (
    FiveMinuteSelection, OneMinuteSelection, RESEARCH_VERSION,
    reference_profile, selection_audit_0920,
)


SEARCH_VERSION = "G3_0920_FIXED_1M_STEPWISE_5M_65_V1"
FIXED_LONG_1M = OneMinuteSelection(.75, .35, 1.6)
FIXED_SHORT_1M = OneMinuteSelection(.65, .35, 1.7)


@dataclass(frozen=True)
class Stage:
    name: str
    branch: str
    long: FiveMinuteSelection
    short: FiveMinuteSelection


def _stage(name: str, branch: str, price_l: float, oi_l: float, volume_l: float,
           active_price_s: float, core_price_s: float, volume_s: float) -> Stage:
    return Stage(name, branch,
                 FiveMinuteSelection(price_l, price_l, oi_l, volume_l),
                 FiveMinuteSelection(active_price_s, core_price_s, .05, volume_s))


# Fixed before this 24-profile run, after reviewing the earlier 1m-only sweep.
# Cumulative steps relax 5m volume, then price, then LONG OI. SHORT OI already
# equals the contractual .05% floor. One/two-axis probes retain stricter values
# on other gates. The 09:21 rules above never vary.
STAGES = (
    _stage("reference_5m", "cumulative", .30, .10, 3.0, .13, .20, 1.5),
    _stage("volume_275_145", "cumulative", .30, .10, 2.75, .13, .20, 1.45),
    _stage("volume_250_140", "cumulative", .30, .10, 2.5, .13, .20, 1.4),
    _stage("volume_200_125", "cumulative", .30, .10, 2.0, .13, .20, 1.25),
    _stage("volume_150_110", "cumulative", .30, .10, 1.5, .13, .20, 1.1),
    _stage("volume_100_100", "cumulative", .30, .10, 1.0, .13, .20, 1.0),
    _stage("volume_base", "cumulative", .30, .10, .80, .13, .20, .80),
    _stage("price_025_010_018", "cumulative", .25, .10, .80, .10, .18, .80),
    _stage("price_020_010_015", "cumulative", .20, .10, .80, .10, .15, .80),
    _stage("price_015_010_013", "cumulative", .15, .10, .80, .10, .13, .80),
    _stage("price_base", "cumulative", .10, .10, .80, .10, .10, .80),
    _stage("oi_long_0075", "cumulative", .10, .075, .80, .10, .10, .80),
    _stage("oi_base", "cumulative", .10, .05, .80, .10, .10, .80),
    _stage("probe_price_025", "one_axis_probe", .25, .10, 3.0, .10, .18, 1.5),
    _stage("probe_price_020", "one_axis_probe", .20, .10, 3.0, .10, .15, 1.5),
    _stage("probe_price_015", "one_axis_probe", .15, .10, 3.0, .10, .13, 1.5),
    _stage("probe_oi_0075", "one_axis_probe", .30, .075, 3.0, .13, .20, 1.5),
    _stage("probe_oi_base", "one_axis_probe", .30, .05, 3.0, .13, .20, 1.5),
    _stage("probe_volume250_price025", "two_axis_probe", .25, .10, 2.5, .10, .18, 1.4),
    _stage("probe_volume200_price020", "two_axis_probe", .20, .10, 2.0, .10, .15, 1.25),
    _stage("probe_volume150_price020", "two_axis_probe", .20, .10, 1.5, .10, .15, 1.1),
    _stage("probe_volume250_oi0075", "two_axis_probe", .30, .075, 2.5, .13, .20, 1.4),
    _stage("probe_volume200_oi0075", "two_axis_probe", .30, .075, 2.0, .13, .20, 1.25),
    _stage("probe_price025_oi0075", "two_axis_probe", .25, .075, 3.0, .10, .18, 1.5),
)


def profile_for_stage(stage: Stage):
    base = reference_profile()
    return replace(base,
                   long=replace(base.long, five_minute=stage.long,
                                one_minute=FIXED_LONG_1M),
                   short=replace(base.short, five_minute=stage.short,
                                 one_minute=FIXED_SHORT_1M))


def _flatten(trial: dict) -> dict:
    row = first_search._flatten(trial)
    row["branch"] = trial["branch"]
    row["full_history_observed_65"] = one_search._qualifies(trial["periods"]["all"])
    row["discovery_observed_65"] = one_search._qualifies(trial["periods"]["discovery"])
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
    if not set(days).issubset(paired._feature_days(all_features)):
        raise ValueError("Sealed 09:20 feature export omitted published G-3 days")
    features = all_features.loc[all_features.session_date.astype(str).isin({str(d) for d in days})].copy()
    excluded_days = sorted(set(paired._feature_days(all_features)) - set(days))
    baseline = paired.allocate(published)
    fixed_1m = one_search.profile_for_stage(next(
        stage for stage in one_search.STAGES if stage.name == "volume_160_170"))
    if (fixed_1m.long.one_minute != FIXED_LONG_1M
            or fixed_1m.short.one_minute != FIXED_SHORT_1M
            or fixed_1m.long.five_minute != reference_profile().long.five_minute
            or fixed_1m.short.five_minute != reference_profile().short.five_minute):
        raise RuntimeError("Fixed 09:21 research configuration drifted")

    records = []
    union_orders: dict[int, dict] = {}
    for step, stage in enumerate(STAGES, 1):
        profile = profile_for_stage(stage)
        if (profile.long.one_minute != FIXED_LONG_1M
                or profile.short.one_minute != FIXED_SHORT_1M):
            raise RuntimeError("09:21 one-minute gates changed in five-minute-only search")
        audit = selection_audit_0920(features, profile=profile)
        orders = audit.loc[audit.v9_selected.eq(True)].copy().reset_index(drop=True)
        if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
            raise ValueError("Research order IDs overlap the published baseline")
        for row in orders.to_dict("records"):
            sid = int(row["sid"])
            if sid in union_orders and union_orders[sid]["signal_id"] != row["signal_id"]:
                raise RuntimeError("Candidate identity changed between five-minute profiles")
            union_orders.setdefault(sid, row)
        records.append(dict(step=step, five_tier=stage.name,
                            one_tier="fixed_volume_160_170", branch=stage.branch,
                            profile_id=f"5m_{stage.name}__1m_fixed_160_170",
                            profile=asdict(profile), orders=orders,
                            filter_pass=int(audit.v9_filter_pass.sum())))
        print(f"{step:02d}/{len(STAGES)} {stage.name}: {len(orders)} selected, "
              f"{records[-1]['filter_pass']} filter-pass", flush=True)

    if not union_orders:
        raise ValueError("No genuine 09:20 research orders selected")
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
        trial = {**record, "periods": first_search._trial_stats(baseline, variant, new_sids, days),
                 "old_trade_effect": comparison["old_trade_effect"]}
        trial["discovery_qualified"] = one_search._qualifies(trial["periods"]["discovery"])
        trials.append(trial)
        if record["five_tier"] == "reference_5m":
            if (len(orders) != 12 or comparison["added_0920"]["wins"] != 8
                    or not math.isclose(comparison["delta_net_pnl_rs"], 21509.379494,
                                        rel_tol=0, abs_tol=.02)):
                raise RuntimeError("Reference 5m replay does not match prior sealed 1m search")
        record["_orders"], record["_variant"], record["_daily"] = orders, variant, daily

    best_all = one_search.choose_largest_65(trials, "all")
    best_discovery = one_search.choose_largest_65(trials, "discovery")
    best_all_record = next((r for r in records if best_all and r["step"] == best_all["step"]), None)
    best_discovery_record = next((r for r in records if best_discovery and r["step"] == best_discovery["step"]), None)
    max_recent_orders = max(t["periods"]["holdout"]["added_0920"]["selected_orders"] for t in trials)
    best_recent = best_discovery["periods"]["holdout"] if best_discovery else None
    recent_validated = bool(best_recent and one_search._qualifies(best_recent))
    summary = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
                   period_start=str(days[0]), period_end=str(days[-1]),
                   discovery_last=str(first_search.DISCOVERY_LAST),
                   recent_first=str(first_search.HOLDOUT_FIRST),
                   trial_count=len(trials), fixed_one_minute_profile={
                       "long": asdict(FIXED_LONG_1M), "short": asdict(FIXED_SHORT_1M)},
                   target_added_0920_win_rate_pct=one_search.TARGET_WIN_RATE_PCT,
                   minimum_exploratory_fills=one_search.MIN_EXPLORATORY_FILLS,
                   full_history_observed_65_count=sum(one_search._qualifies(t["periods"]["all"]) for t in trials),
                   best_full_history_observed_65_profile_id=(best_all["profile_id"] if best_all else None),
                   best_full_history_observed_65_profile=(best_all["profile"] if best_all else None),
                   best_full_history_observed_65_periods=(best_all["periods"] if best_all else None),
                   best_discovery_profile_id=(best_discovery["profile_id"] if best_discovery else None),
                   best_discovery_periods=(best_discovery["periods"] if best_discovery else None),
                   best_discovery_recent_meets_65=recent_validated,
                   max_recent_selected_orders_in_grid=max_recent_orders,
                   no_recent_10_fill_validation_possible=max_recent_orders < one_search.MIN_EXPLORATORY_FILLS,
                   historical_tuning_only=True, previous_0920_outcomes_inspected=True,
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
    code = [Path(__file__).resolve(), Path(one_search.__file__).resolve(),
            Path(first_search.__file__).resolve(), Path(__file__).with_name("g3_0920_strategy.py"),
            Path(__file__).with_name("g3_0920_backtest.py"),
            Path(contract.__file__).resolve(), Path(simulator.__file__).resolve()]
    manifest = dict(search_version=SEARCH_VERSION, execution_authority=False,
                    code_sha256={p.name: paired.sha256(p) for p in code},
                    files={p.name: paired.sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=first_search.DEFAULT_FEATURES)
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
