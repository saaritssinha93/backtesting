"""Joint 09:20/09:21 G-3 entry-filter research with a 70% historical screen.

The grid is declared before replay. Both new 09:20 targets are fixed at 1%.
The existing staged stop and portfolio rules are unchanged.
Selection starts from the entire dated universe, using a reusable broad audit
and the original contract predicates. No runtime configuration is published.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path

import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_parameter_search as coarse
from research import g3_0920_five_minute_65_search as five_search
from research import g3_0920_one_minute_65_search as one_search
from research.g3_0920_strategy import (
    RESEARCH_VERSION, FiveMinuteSelection, OneMinuteSelection,
    SelectionProfile, SideSelectionProfile, selection_audit_0920, setup_specs,
)

SEARCH_VERSION = "G3_0920_JOINT_5M_1M_70_TARGET1_V1"
TARGET_WIN_RATE = 70.0
TARGET_PRICE_PCT = 1.0
MIN_FILLS = 10
MIN_SIDE_FILLS = 5
PARITY_COLUMNS = ["sid", "signal_id", "v9_rank_in_setup_day", "v10_g_f_core",
                  "v9_decision", "native_stop_pct", "native_target_pct", "trigger"]


def _unique(items):
    seen = set()
    result = []
    for name, long, short in items:
        key = (long, short)
        if key not in seen:
            result.append((name, long, short))
            seen.add(key)
    return result


def five_tiers():
    # Strict to contractual floors, plus pre-existing bounded axis probes.
    return _unique([
        (name, coarse.FIVE_TIERS[name]["LONG"], coarse.FIVE_TIERS[name]["SHORT"])
        for name in ("strict", "balanced")
    ] + [(s.name, s.long, s.short) for s in five_search.STAGES]
      + [("coarse_relaxed", coarse.FIVE_TIERS["relaxed"]["LONG"],
          coarse.FIVE_TIERS["relaxed"]["SHORT"])])


def one_tiers():
    return _unique([(s.name, s.long, s.short) for s in one_search.STAGES] + [
        ("coarse_" + name, coarse.ONE_TIERS[name]["LONG"], coarse.ONE_TIERS[name]["SHORT"])
        for name in ("balanced", "relaxed")])


def search_grid():
    # Complete the 5m sweep at strict 1m, then repeat at each 1m relaxation.
    return [(five, one, SelectionProfile(SideSelectionProfile(fl, ol),
                                         SideSelectionProfile(fs, os)))
            for one, ol, os in one_tiers() for five, fl, fs in five_tiers()]


def broad_profile():
    five = FiveMinuteSelection(contract.BASE_PRICE_CHANGE_PCT,
                               contract.BASE_PRICE_CHANGE_PCT,
                               contract.BASE_OI_CHANGE_PCT, contract.BASE_VOLUME_RATIO)
    return SelectionProfile(*[
        SideSelectionProfile(five, OneMinuteSelection(
            0., 1., contract.confirmation_volume_minimum(side, setup_id=f"0921_{side}")))
        for side in ("LONG", "SHORT")])


class PreparedSelection:
    """Reuse source/date/identity checks, retaining native filter and picker logic.

    The broad audit is a superset of every valid numeric profile. It includes
    ALL filter passes, not merely its daily selected orders. A stricter profile
    can therefore choose a previously ranked-out stock. Profile validation
    prevents weakening any gate below the broad audit's contract floors.
    """
    def __init__(self, features):
        self.audit = selection_audit_0920(features, profile=broad_profile())
        self.pools = {}
        for row in self.audit.loc[self.audit.v9_filter_pass].to_dict("records"):
            self.pools.setdefault((row["day"], row["side"]), []).append(row)

    def select(self, profile):
        specs = setup_specs(profile)  # Validate all thresholds against native floors.
        selected = []
        filter_pass = 0
        for (day, side), rows in self.pools.items():
            core, active = specs[side]
            def key(row, setup):
                return (-contract.picker_value(row, setup.picker),
                        -float(row["traded_value"]), str(row["tradingsymbol"]))
            eligible = sorted((row for row in rows
                               if contract.passes_selected_filters(row, active, session_date=day)
                               and float(row["v9_1m_volume_ratio"]) >=
                               profile.for_side(side).one_minute.confirmation_volume_ratio),
                              key=lambda row: key(row, active))
            core_eligible = sorted((row for row in eligible
                                    if contract.passes_selected_filters(row, core, session_date=day)),
                                   key=lambda row: key(row, core))
            core_chosen = core_eligible[:core.max_entries]
            core_ids = {row["sid"] for row in core_chosen}
            chosen = core_chosen + [row for row in eligible if row["sid"] not in core_ids][
                :active.max_entries - len(core_chosen)]
            ranks = {row["sid"]: rank for rank, row in enumerate(eligible, 1)}
            filter_pass += len(eligible)
            selected += [dict(row, v10_g_f_core=row["sid"] in core_ids,
                              v9_filter_pass=True, v9_selected=True,
                              v9_rank_in_setup_day=ranks[row["sid"]],
                              v9_decision="SELECTED") for row in chosen]
        if not selected:
            return self.audit.iloc[:0].copy(), filter_pass
        orders = pd.DataFrame(selected).sort_values(
            ["day", "hhmm_int", "side", "tradingsymbol"], kind="stable").reset_index(drop=True)
        return orders, filter_pass

    def verify(self, features, profile):
        legacy = selection_audit_0920(features, profile=profile)
        expected = legacy.loc[legacy.v9_selected].reset_index(drop=True)
        actual, passes = self.select(profile)
        pd.testing.assert_frame_equal(expected[PARITY_COLUMNS], actual[PARITY_COLUMNS], check_dtype=False)
        if passes != int(legacy.v9_filter_pass.sum()):
            raise RuntimeError("Prepared selector changed filter-pass membership")
        return dict(selected=len(actual), filter_pass=passes)


def qualifies(period, *, sides=False):
    added = period["added_0920"]
    if (added["executed_trades"] < MIN_FILLS or added["win_rate_pct"] is None
            or added["win_rate_pct"] < TARGET_WIN_RATE
            or period["combined"]["win_rate_pct"] < TARGET_WIN_RATE
            or period["incremental_pnl_rs"] <= 0):
        return False
    return not sides or all(
        period[key]["executed_trades"] >= MIN_SIDE_FILLS
        and period[key]["win_rate_pct"] is not None
        and period[key]["win_rate_pct"] >= TARGET_WIN_RATE
        for key in ("added_long", "added_short"))


def choose(trials, period, *, sides=False):
    eligible = [trial for trial in trials if qualifies(trial["periods"][period], sides=sides)]
    return max(eligible, key=lambda trial: (
        trial["periods"][period]["added_0920"]["executed_trades"],
        trial["periods"][period]["incremental_pnl_rs"], -trial["step"])) if eligible else None


def selection_key(orders):
    # Core status and rank are retained because portfolio allocation uses metadata.
    return tuple((int(row.sid), bool(row.v10_g_f_core), int(row.v9_rank_in_setup_day))
                 for row in orders.itertuples())


def with_research_targets(orders):
    result = orders.copy()
    result["native_target_pct"] = TARGET_PRICE_PCT
    result["target_pct"] = TARGET_PRICE_PCT
    result["research_exit_variant"] = "0920_LONG_SHORT_TARGET_1PCT"
    return result


def _flatten(trial):
    row = coarse._flatten(trial)
    row.update(filter_pass=trial["filter_pass"],
               all_observed_70=qualifies(trial["periods"]["all"]),
               all_both_sides_70=qualifies(trial["periods"]["all"], sides=True),
               recent_observed_70=qualifies(trial["periods"]["holdout"]))
    return row


def run(features_path, output_dir, *, pointer_path=paired.DEFAULT_POINTER, data_root=RUNTIME_ROOT):
    output_dir = Path(output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Research output must be new or empty: {output_dir}")
    code = [Path(__file__).resolve(), Path(coarse.__file__), Path(five_search.__file__),
            Path(one_search.__file__), Path(paired.__file__), Path(contract.__file__),
            Path(simulator.__file__), Path(__file__).with_name("g3_0920_strategy.py")]
    code_hashes = {p.name: paired.sha256(p) for p in code}
    grid = search_grid()
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                target_win_rate_pct=TARGET_WIN_RATE, minimum_fills=MIN_FILLS,
                new_0920_target_pct=dict(LONG=TARGET_PRICE_PCT, SHORT=TARGET_PRICE_PCT),
                minimum_fills_per_side=MIN_SIDE_FILLS, code_sha256=code_hashes,
                trial_count=len(grid), profiles=[dict(five=f, one=o, profile=asdict(p)) for f, o, p in grid])
    (output_dir / "search_plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    published, days, baseline_evidence = paired.load_published(pointer_path)
    features_path = Path(features_path).resolve()
    all_features, coverage, feature_evidence = paired.load_feature_evidence(features_path)
    universe_evidence = paired.validate_feature_universe(all_features, data_root, coverage)
    if not set(days).issubset(paired._feature_days(all_features)):
        raise ValueError("Opening export omits published G-3 sessions")
    features = all_features.loc[all_features.session_date.astype(str).isin({str(d) for d in days})].copy()
    baseline = paired.allocate(published)
    prepared = PreparedSelection(features)
    print(f"Grid: {len(five_tiers())} five-minute x {len(one_tiers())} one-minute = {len(grid)} profiles; "
          f"{int(prepared.audit.v9_filter_pass.sum())} broad native filter passes", flush=True)
    parity = {}
    checkpoints = {("strict", "strict"), ("reference_5m", "strict"),
                   ("reference_5m", "reference_1m"), ("oi_base", "coarse_relaxed"),
                   ("coarse_relaxed", "coarse_balanced")}
    for five, one, profile in grid:
        if (five, one) in checkpoints:
            parity[f"{five}/{one}"] = prepared.verify(features, profile)
    print(f"Native selector parity passed for {len(parity)} full-universe checkpoints", flush=True)
    records, union_orders = [], {}
    for step, (five, one, profile) in enumerate(grid, 1):
        orders, passes = prepared.select(profile)
        orders = with_research_targets(orders)
        if set(orders.sid.astype(int)) & set(published.sid.astype(int)):
            raise RuntimeError("09:20 signal IDs overlap published orders")
        for row in orders.to_dict("records"):
            sid = int(row["sid"])
            if sid in union_orders and union_orders[sid]["signal_id"] != row["signal_id"]:
                raise RuntimeError("Order identity differs across profiles")
            union_orders.setdefault(sid, row)
        records.append(dict(step=step, five_tier=five, one_tier=one,
                            profile_id=f"5m_{five}__1m_{one}", profile=asdict(profile),
                            filter_pass=passes, orders=orders, key=selection_key(orders)))
        if step % len(five_tiers()) == 0:
            print(f"Selected {step}/{len(grid)} profiles; 1m tier {one}; union {len(union_orders)} orders", flush=True)
    if not union_orders:
        raise RuntimeError("No valid 09:20 candidate selected")
    union = pd.DataFrame(list(union_orders.values())).sort_values("sid").reset_index(drop=True)
    print(f"Replaying {len(union)} unique candidate paths", flush=True)
    paths, path_evidence = paired.new_execution_paths(union, Path(data_root))
    pool = simulator.simulate_staged(union, paths, cost_bps=contract.ROUND_TRIP_COST_BPS,
                                     max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    if not pool.native_target_pct.eq(TARGET_PRICE_PCT).all():
        raise RuntimeError("09:20 replay did not use the requested 1% targets")
    pool["strategy"], pool["strategy_version"] = RESEARCH_VERSION, RESEARCH_VERSION
    pool["scope"], pool["execution_authority"] = "BACKTEST_ONLY", False
    by_sid = pool.set_index("sid", drop=False)
    replay_cache, trials = {}, []
    for record in records:
        orders, key = record["orders"], record["key"]
        if key not in replay_cache:
            new_sids = set(orders.sid.astype(int))
            fresh = by_sid.loc[orders.sid.astype(int).tolist()].copy().reset_index(drop=True)
            for col in ("v10_g_f_core", "v9_rank_in_setup_day", "v9_decision"):
                fresh[col] = orders[col].to_numpy()
            variant = paired.allocate(pd.concat((published, fresh), ignore_index=True, sort=False))
            comparison, daily = paired.compare_ledgers(baseline, variant, days, days, new_sids)
            replay_cache[key] = dict(variant=variant, daily=daily, old_trade_effect=comparison["old_trade_effect"],
                                     periods=coarse._trial_stats(baseline, variant, new_sids, days))
        replay = replay_cache[key]
        trial = {k: v for k, v in record.items() if k not in ("orders", "key")}
        trial.update(periods=replay["periods"], old_trade_effect=replay["old_trade_effect"])
        trial["discovery_qualified"] = qualifies(trial["periods"]["discovery"])
        trials.append(trial)
        if record["five_tier"] == "reference_5m" and record["one_tier"] == "reference_1m":
            whole = trial["periods"]["all"]
            if whole["added_0920"]["selected_orders"] != 19:
                raise RuntimeError("Reference selection differs from sealed prior comparison")
    winners = {"best_observed_70": choose(trials, "all"),
               "best_discovery_70": choose(trials, "discovery"),
               "best_both_sides_70": choose(trials, "all", sides=True)}
    # Verify selected finalists against the original full-universe implementation.
    for label, trial in winners.items():
        if trial is not None:
            profile = grid[trial["step"] - 1][2]
            parity[label] = prepared.verify(features, profile)
    discovery = winners["best_discovery_70"]
    statistical_screen = bool(discovery and qualifies(discovery["periods"]["discovery"], sides=True)
                              and qualifies(discovery["periods"]["holdout"], sides=True))
    summary = dict(search_version=SEARCH_VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
                   period_start=str(days[0]), period_end=str(days[-1]), trial_count=len(trials),
                   five_minute_tiers=len(five_tiers()), one_minute_tiers=len(one_tiers()),
                   unique_candidate_paths=len(union), unique_selected_portfolios=len(replay_cache),
                   target_win_rate_pct=TARGET_WIN_RATE, minimum_fills=MIN_FILLS,
                   new_0920_exit=dict(target_pct_by_side=dict(LONG=TARGET_PRICE_PCT, SHORT=TARGET_PRICE_PCT),
                                      initial_stop_pct=contract.INITIAL_STOP_PCT,
                                      tightened_stop_pct=contract.TIGHTENED_STOP_PCT,
                                      tighten_after_minutes=contract.TIGHTEN_AFTER_MINUTES,
                                      square_off=contract.SQUARE_OFF),
                   minimum_side_fills=MIN_SIDE_FILLS,
                   full_history_observed_70_count=sum(qualifies(t["periods"]["all"]) for t in trials),
                   full_history_both_sides_70_count=sum(qualifies(t["periods"]["all"], sides=True) for t in trials),
                   discovery_observed_70_count=sum(qualifies(t["periods"]["discovery"]) for t in trials),
                   recent_observed_70_count=sum(qualifies(t["periods"]["holdout"]) for t in trials),
                   maximum_recent_selected_orders=max(t["periods"]["holdout"]["added_0920"]["selected_orders"] for t in trials),
                   discovery_last=str(coarse.DISCOVERY_LAST), recent_first=str(coarse.HOLDOUT_FIRST),
                   winners=winners, chronological_both_sides_70_screen_pass=statistical_screen,
                   historical_tuning_only=True, earlier_outcomes_already_inspected=True,
                   retrospective_opening_oi=True, live_tradability_verified=False,
                   approved_for_live=False, selector_parity_checks=parity,
                   baseline_evidence=baseline_evidence, feature_evidence=feature_evidence,
                   dated_universe_evidence=universe_evidence, feature_path=str(features_path),
                   feature_sha256=paired.sha256(features_path),
                   exported_dates_excluded=[str(d) for d in sorted(set(paired._feature_days(all_features)) - set(days))],
                   complete_research_stock_days=int(features.research_feature_status.eq("COMPLETE").sum()),
                   source_path_evidence=path_evidence)
    frames = [("trials.csv", pd.DataFrame([_flatten(t) for t in trials])),
              ("union_raw_trades.csv", pool),
              ("selected_membership_all_profiles.csv", pd.concat([
                  r["orders"].assign(profile_id=r["profile_id"], step=r["step"])[
                      ["profile_id", "step", "sid", "signal_id", "day", "tradingsymbol", "side",
                       "v10_g_f_core", "v9_rank_in_setup_day"]] for r in records], ignore_index=True))]
    for label, trial in winners.items():
        if trial is not None:
            record = records[trial["step"] - 1]
            replay = replay_cache[record["key"]]
            frames.extend([(label + "_orders.csv", record["orders"]),
                           (label + "_variant_trades.csv", replay["variant"]),
                           (label + "_daily_comparison.csv", replay["daily"])])
    for name, frame in frames:
        frame.to_csv(output_dir / name, index=False)
    for name, obj in (("trials.json", trials), ("summary.json", summary)):
        (output_dir / name).write_text(json.dumps(obj, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    if paired.sha256(Path(baseline_evidence["pointer"])) != baseline_evidence["pointer_sha256"]:
        raise RuntimeError("Published pointer changed during research")
    if any(paired.sha256(p) != code_hashes[p.name] for p in code):
        raise RuntimeError("Research source changed during replay")
    manifest = dict(search_version=SEARCH_VERSION, execution_authority=False, code_sha256=code_hashes,
                    files={p.name: paired.sha256(p) for p in output_dir.iterdir() if p.is_file()})
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Complete: {len(trials)} profiles, {len(replay_cache)} distinct portfolio replays", flush=True)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=coarse.DEFAULT_FEATURES)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pointer", type=Path, default=paired.DEFAULT_POINTER)
    parser.add_argument("--data-root", type=Path, default=RUNTIME_ROOT)
    args = parser.parse_args(argv)
    summary = run(args.features, args.output_dir, pointer_path=args.pointer, data_root=args.data_root)
    print(json.dumps({k: summary[k] for k in (
        "trial_count", "full_history_observed_70_count", "full_history_both_sides_70_count",
        "discovery_observed_70_count", "recent_observed_70_count", "maximum_recent_selected_orders",
        "winners")}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
