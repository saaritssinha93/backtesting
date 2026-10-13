"""Isolated G-4 late-clock backtest and chronological confirmation sweep.

G-3 source/selection/exit logic is reused without modifying G-3 files. All 12
new clock/side setups are evaluated. Thresholds are selected on the first 38
sessions only; the remaining 11 sessions are a chronological holdout. The run
never publishes a dashboard or production pointer.
"""
from __future__ import annotations

import hashlib
import json
import math
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_2_extend_results as extension
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_0950_long_backtest import metrics
from research.g4_late_slot_source import (DEFAULT_CACHE, LATE_CLOCKS,
                                           _archive_paths, load_late_source, sha256)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_v1.json"
VERSION = "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1"


def configuration(path: Path) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if (value.get("variant_id") != VERSION or value.get("scope") != "BACKTEST_ONLY"
            or value.get("execution_authority") is not False
            or value.get("preserve_g3_setups") is not True
            or value.get("base_strategy") != "V13-V10-G-3-PRODUCTION-V2"
            or value.get("base_g3_run_relative") !=
                "backtests/runs/2026-10-12/20261012T185109507457_a9213404_0950_long_t075"
            or value.get("base_g3_manifest_sha256") !=
                "cc4760fbd58bf76cc6042d1a5b0e14f930c2ea5cc3d85b3a7f42c7e6913d3015"
            or value.get("signal_ends") != list(LATE_CLOCKS)
            or value.get("sides") != ["LONG", "SHORT"]
            or value.get("confirmation_offset_minutes") != 1
            or value.get("max_entries_per_setup_day") != 1
            or value.get("picker") != "max_liquidity"
            or value.get("initial_stop_pct") != contract.INITIAL_STOP_PCT
            or value.get("tightened_stop_pct") != contract.TIGHTENED_STOP_PCT
            or value.get("tighten_after_minutes") != contract.TIGHTEN_AFTER_MINUTES
            or value.get("round_trip_cost_bps") != contract.ROUND_TRIP_COST_BPS
            or value.get("training_sessions") != 38
            or value.get("holdout_sessions") != 11
            or value.get("required_win_rate_pct_per_setup") != 70.0):
        raise ValueError("G4 study config must preserve the specified isolated profile")
    targets = value["target_pct_by_side"]
    if targets != {"LONG": .75, "SHORT": .93}:
        raise ValueError("G4 late setups must retain the G3 09:50 side-specific targets")
    fields = ("price_change_pct", "oi_change_pct", "five_minute_volume_ratio_min",
              "one_minute_body_ratio_min", "one_minute_wick_ratio_max",
              "one_minute_volume_ratio_min")
    for side in ("LONG", "SHORT"):
        levels = value["levels"][side]
        if len(levels) < 2:
            raise ValueError("G4 requires an iterative strictness ladder")
        for n, level in enumerate(levels):
            if set(level) != set(fields) or any(not math.isfinite(float(level[key]))
                                                for key in fields):
                raise ValueError("G4 threshold level is incomplete/nonfinite")
            if (level["price_change_pct"] < .10 or level["oi_change_pct"] < .05
                    or level["five_minute_volume_ratio_min"] < .8
                    or not 0 <= level["one_minute_body_ratio_min"] <= 1
                    or not 0 <= level["one_minute_wick_ratio_max"] <= 1
                    or level["one_minute_volume_ratio_min"] < 0):
                raise ValueError("G4 threshold would violate the native base gates")
            if n:
                prior = levels[n - 1]
                for key in fields:
                    if key == "one_minute_wick_ratio_max":
                        if level[key] > prior[key]:
                            raise ValueError("G4 wick limit must tighten monotonically")
                    elif level[key] < prior[key]:
                        raise ValueError("G4 threshold must tighten monotonically")
    one_gate_steps = (("price_change_pct", (.05, .10, .20)),
                      ("oi_change_pct", (.05, .10)),
                      ("five_minute_volume_ratio_min", (.20, .40)),
                      ("one_minute_body_ratio_min", (.10, .20)),
                      ("one_minute_wick_ratio_max", (-.10, -.20)),
                      ("one_minute_volume_ratio_min", (.20, .40)))
    variants = {}
    for side in ("LONG", "SHORT"):
        entries = [dict(name=f"joint_level_{n}", thresholds=dict(level))
                   for n, level in enumerate(value["levels"][side])]
        base = value["levels"][side][0]
        for field, deltas in one_gate_steps:
            for delta in deltas:
                threshold = dict(base)
                threshold[field] = round(threshold[field] + delta, 8)
                if field == "one_minute_wick_ratio_max" and threshold[field] < 0:
                    continue
                if threshold not in [entry["thresholds"] for entry in entries]:
                    entries.append(dict(name=f"one_gate_{field}_{delta:+.2f}", thresholds=threshold))
        variants[side] = entries
    value["candidate_variants"] = variants
    return value


def _confirmation(clock: str) -> str:
    return (datetime.strptime(clock, "%H:%M") + timedelta(minutes=1)).strftime("%H:%M")


@contextmanager
def g4_setups(config: dict, selected_levels: dict[str, int] | None = None,
              *, permissive: bool = False):
    """Temporarily add G4 specs in this offline process only."""
    old = (contract.CORE_SETUPS, contract.ACTIVE_SETUPS,
           dict(contract._CORE_BY_ID), dict(contract._ACTIVE_BY_ID),
           dict(contract.MIN_CONFIRMATION_VOLUME_BY_SETUP),
           dict(contract.SIGNAL_TO_CONFIRMATION),
           contract.STRATEGY_PROFILE, contract.STRATEGY_VERSION, contract.ACCEPTED_VARIANT)
    extras = []
    for clock in config["signal_ends"]:
        for side in config["sides"]:
            key = f"{clock}_{side}"
            level = ({"price_change_pct": .10, "oi_change_pct": .05,
                      "five_minute_volume_ratio_min": .8,
                      "one_minute_body_ratio_min": 0., "one_minute_wick_ratio_max": 1.,
                      "one_minute_volume_ratio_min": 0.}
                     if permissive else config["candidate_variants"][side][selected_levels[key]]["thresholds"])
            donor = contract.setup_for("09:50", side)
            if donor is None:
                raise RuntimeError("G3 09:50 donor setup missing")
            setup = replace(donor, signal_end=clock, confirmation_end=_confirmation(clock),
                            max_entries=1000 if permissive else 1,
                            picker="max_liquidity", price_change_pct=float(level["price_change_pct"]),
                            oi_change_pct=float(level["oi_change_pct"]),
                            volume_ratio=float(level["five_minute_volume_ratio_min"]),
                            body_ratio=float(level["one_minute_body_ratio_min"]),
                            max_wick_ratio=float(level["one_minute_wick_ratio_max"]),
                            target_pct=float(config["target_pct_by_side"][side]),
                            source_version=VERSION)
            extras.append(setup)
            contract.MIN_CONFIRMATION_VOLUME_BY_SETUP[setup.setup_id] = float(level["one_minute_volume_ratio_min"])
    contract.CORE_SETUPS = (*contract.CORE_SETUPS, *extras)
    contract.ACTIVE_SETUPS = (*contract.ACTIVE_SETUPS, *extras)
    contract._CORE_BY_ID.update({setup.setup_id: setup for setup in extras})
    contract._ACTIVE_BY_ID.update({setup.setup_id: setup for setup in extras})
    contract.SIGNAL_TO_CONFIRMATION = dict(sorted({**contract.SIGNAL_TO_CONFIRMATION,
        **{clock: _confirmation(clock) for clock in config["signal_ends"]}}.items()))
    contract.STRATEGY_PROFILE = "V13_V10_G_4_BACKTEST"
    contract.STRATEGY_VERSION = VERSION
    contract.ACCEPTED_VARIANT = VERSION
    try:
        yield tuple(extras)
    finally:
        (contract.CORE_SETUPS, contract.ACTIVE_SETUPS, core, active, volume,
         clocks, contract.STRATEGY_PROFILE, contract.STRATEGY_VERSION,
         contract.ACCEPTED_VARIANT) = old
        contract._CORE_BY_ID.clear(); contract._CORE_BY_ID.update(core)
        contract._ACTIVE_BY_ID.clear(); contract._ACTIVE_BY_ID.update(active)
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP.clear(); contract.MIN_CONFIRMATION_VOLUME_BY_SETUP.update(volume)
        contract.SIGNAL_TO_CONFIRMATION = clocks


def _label_candidates(candidates: pd.DataFrame, paths: dict[int, dict], config: dict) -> pd.DataFrame:
    with g4_setups(config, permissive=True):
        audit = contract.selection_audit(candidates)
        orders = audit.loc[audit.v9_selected.eq(True)].copy()
    if set(orders.sid.astype(int)) != set(candidates.sid.astype(int)):
        missing = set(candidates.sid.astype(int)) - set(orders.sid.astype(int))
        raise RuntimeError(f"G4 permissive audit dropped {len(missing)} strict source candidates")
    simulator.g.v9.validate_paths(orders, paths)
    trades = simulator.simulate_staged(orders, paths,
                                       cost_bps=contract.ROUND_TRIP_COST_BPS,
                                       max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    trades["research_win"] = trades.filled.eq(True) & trades.net_return_pct.gt(0)
    return trades


def _qualify_rows(frame: pd.DataFrame, side: str, level: dict) -> pd.DataFrame:
    signed_price = frame.price_change_pct if side == "LONG" else -frame.price_change_pct
    mask = (signed_price.ge(level["price_change_pct"])
            & frame.oi_change_pct.ge(level["oi_change_pct"])
            & frame.volume_ratio.ge(level["five_minute_volume_ratio_min"])
            & frame.body_ratio.ge(level["one_minute_body_ratio_min"])
            & frame.wick_ratio.le(level["one_minute_wick_ratio_max"])
            & frame.v9_1m_volume_ratio.ge(level["one_minute_volume_ratio_min"]))
    return (frame.loc[mask].sort_values(["day", "traded_value", "tradingsymbol"],
                                        ascending=[True, False, True], kind="stable")
            .drop_duplicates("day", keep="first"))


def _score(selected: pd.DataFrame) -> dict:
    filled = selected.loc[selected.filled.eq(True)]
    wins = int(filled.research_win.sum())
    return dict(selections=len(selected), fills=len(filled), wins=wins,
                losses=len(filled) - wins,
                win_rate_pct=100 * wins / len(filled) if len(filled) else None,
                net_return_pct_sum=float(filled.net_return_pct.sum()))


def sweep(trades: pd.DataFrame, days: list[date], config: dict) -> tuple[pd.DataFrame, dict[str, int], pd.DataFrame]:
    training = set(days[:config["training_sessions"]])
    holdout = set(days[config["training_sessions"]:])
    if len(training) != 38 or len(holdout) != 11 or max(training) >= min(holdout):
        raise RuntimeError("G4 chronological training/holdout split changed")
    rows = []
    chosen: dict[str, int] = {}
    choices = []
    for clock in config["signal_ends"]:
        for side in config["sides"]:
            key = f"{clock}_{side}"
            pool = trades.loc[trades.hhmm_int.eq(int(clock.replace(":", ""))) & trades.side.eq(side)].copy()
            scored = []
            for index, option in enumerate(config["candidate_variants"][side]):
                level = option["thresholds"]
                selected = _qualify_rows(pool, side, level)
                train = _score(selected.loc[pd.to_datetime(selected.day).dt.date.isin(training)])
                test = _score(selected.loc[pd.to_datetime(selected.day).dt.date.isin(holdout)])
                whole = _score(selected)
                record = dict(setup=key, signal_end=clock, side=side, level=index,
                              variant=option["name"],
                              **{f"train_{k}": v for k, v in train.items()},
                              **{f"holdout_{k}": v for k, v in test.items()},
                              **{f"all_{k}": v for k, v in whole.items()},
                              thresholds=json.dumps(level, sort_keys=True))
                rows.append(record)
                scored.append(record)
            eligible = [x for x in scored if x["train_fills"] >= config["minimum_training_fills"]]
            passing = [x for x in eligible if x["train_win_rate_pct"] >= config["required_win_rate_pct_per_setup"]]
            if passing:
                selected_level = max(passing, key=lambda x: (x["train_fills"],
                                                             x["train_net_return_pct_sum"],
                                                             -x["level"]))["level"]
                selection_reason = "MOST_TRAINING_FILLS_AT_OR_ABOVE_70_WITH_MINIMUM_FILLS"
            elif eligible:
                selected_level = max(eligible, key=lambda x: (x["train_win_rate_pct"],
                                                              x["train_fills"], -x["level"]))["level"]
                selection_reason = "NO_TRAINING_LEVEL_REACHED_70"
            else:
                selected_level = 0
                selection_reason = "INSUFFICIENT_TRAINING_FILLS_AT_ALL_LEVELS"
            chosen[key] = selected_level
            record = next(x for x in scored if x["level"] == selected_level)
            holdout_supported = (record["holdout_fills"] >= config["minimum_holdout_fills_for_validation"]
                                 and record["holdout_win_rate_pct"] is not None
                                 and record["holdout_win_rate_pct"] >= config["required_win_rate_pct_per_setup"])
            choices.append(dict(**record, selection_reason=selection_reason,
                                holdout_supported=holdout_supported,
                                qualified=bool(passing and holdout_supported
                                               and record["all_win_rate_pct"] is not None
                                               and record["all_win_rate_pct"] >= config["required_win_rate_pct_per_setup"])))
    return pd.DataFrame(rows), chosen, pd.DataFrame(choices)


def _daily(ledger: pd.DataFrame, days: list[date], name: str) -> pd.DataFrame:
    dates = pd.to_datetime(ledger.day).dt.date
    return pd.DataFrame([dict(day=str(day), scenario=name, **metrics(ledger.loc[dates.eq(day)]))
                         for day in days])


def _g3_baseline(days: list[date], snapshots: dict[date, Path],
                 config: dict) -> tuple[pd.DataFrame, dict[int, dict], Path]:
    root = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3")
    g3_run = (root / config["base_g3_run_relative"]).resolve()
    manifest_path = g3_run / "manifest.json"
    if (not g3_run.is_relative_to(root.resolve())
            or config["base_g3_manifest_sha256"] != sha256(manifest_path)
            or days[-1] != date(2026, 10, 12)):
        raise RuntimeError("G4 pinned G3 baseline manifest mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name in ("baseline_orders.csv", "added_0950_orders.csv", "variant_trades.csv"):
        if sha256(g3_run / name) != manifest["files"][name]:
            raise RuntimeError(f"G4 current G3 baseline artifact drift: {name}")
    baseline_orders = pd.concat((pd.read_csv(g3_run / "baseline_orders.csv"),
                                 pd.read_csv(g3_run / "added_0950_orders.csv")),
                                ignore_index=True, sort=False)
    if baseline_orders.sid.duplicated().any() or len(baseline_orders) != 123:
        raise RuntimeError("G4 baseline G3 selections changed")
    sealed_days = set(days[:38])
    historical = baseline_orders.loc[pd.to_datetime(baseline_orders.day).dt.date.isin(sealed_days)]
    sealed_root = replay.DEFAULT_SEALED_SOURCE
    paths = _archive_paths(sealed_root / "paths.npz", set(historical.sid.astype(int)))
    for day in days[38:]:
        subset = baseline_orders.loc[pd.to_datetime(baseline_orders.day).dt.date.eq(day)]
        if len(subset):
            day_paths = extension._selected_paths(subset, day, snapshots[day])
            if set(paths) & set(day_paths):
                raise RuntimeError("G4 G3 baseline path ID overlap")
            paths.update(day_paths)
    if set(paths) != set(baseline_orders.sid.astype(int)):
        raise RuntimeError("G4 G3 baseline execution paths incomplete")
    return baseline_orders, paths, g3_run


def run(config_path: Path, output_dir: Path, *, cache: Path = DEFAULT_CACHE) -> dict:
    config = configuration(config_path)
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"G4 output must be new or empty: {output}")
    candidates, candidate_paths, days, evidence, snapshots = load_late_source(cache)
    print(f"G4 source: {len(candidates)} strict late candidates over {len(days)} sessions", flush=True)
    baseline_orders, baseline_paths, g3_run = _g3_baseline(days, snapshots, config)
    baseline, _ = replay.simulate(baseline_orders, baseline_paths, days)
    # The frozen proof applies to the original G3 selections; 09:50 LONG was
    # added later, so compare the full baseline to the pinned G3 V2 ledger.
    recorded = pd.read_csv(g3_run / "variant_trades.csv")
    actual = baseline.set_index("sid").sort_index()
    expected = recorded.set_index("sid").sort_index()
    if not actual.index.equals(expected.index):
        raise RuntimeError("G4 G3 baseline selections do not match current G3 history")
    for field in ("filled", "portfolio_executed", "exit_reason"):
        if not actual[field].astype(str).equals(expected[field].astype(str)):
            raise RuntimeError(f"G4 G3 baseline trade drift: {field}")
    if not np.allclose(actual.portfolio_net_profit_rupees.astype(float),
                       expected.portfolio_net_profit_rupees.astype(float), rtol=0, atol=1e-6):
        raise RuntimeError("G4 G3 baseline P&L drift")
    print("G4 baseline matches pinned G3 V2 saved trades", flush=True)
    labeled = _label_candidates(candidates, candidate_paths, config)
    ladder, chosen, decisions = sweep(labeled, days, config)
    print("G4 training sweep completed for all 12 clock/side setups", flush=True)
    with g4_setups(config, chosen) as extras:
        audit = contract.selection_audit(candidates)
        new_orders = audit.loc[audit.v9_selected.eq(True)].copy()
    if new_orders.sid.duplicated().any():
        raise RuntimeError("G4 selected duplicate source IDs")
    expected_ids = set()
    for clock in config["signal_ends"]:
        for side in config["sides"]:
            key = f"{clock}_{side}"
            part = labeled.loc[labeled.hhmm_int.eq(int(clock.replace(":", ""))) & labeled.side.eq(side)]
            expected_ids.update(_qualify_rows(part, side,
                config["candidate_variants"][side][chosen[key]]["thresholds"]).sid.astype(int))
    if set(new_orders.sid.astype(int)) != expected_ids:
        raise RuntimeError("G4 sweep selection differs from production-contract selection")
    if set(new_orders.sid.astype(int)) & set(baseline_orders.sid.astype(int)):
        raise RuntimeError("G4 added candidate ID overlaps G3")
    combined_orders = pd.concat((baseline_orders, new_orders), ignore_index=True, sort=False)
    combined_paths = {**baseline_paths, **{int(sid): candidate_paths[int(sid)]
                                          for sid in new_orders.sid.astype(int)}}
    variant, _ = replay.simulate(combined_orders, combined_paths, days)
    variant["strategy"] = "V13-V10-G-4"
    variant["strategy_version"] = VERSION
    variant["strategy_fingerprint"] = hashlib.sha256(json.dumps(
        {"config": config, "selected_levels": chosen}, sort_keys=True).encode()).hexdigest()
    added = variant.loc[variant.sid.isin(new_orders.sid)].copy()
    added_days = pd.to_datetime(added.day).dt.date
    train_days = set(days[:config["training_sessions"]])
    holdout_days = set(days[config["training_sessions"]:])
    for index, row in decisions.iterrows():
        part = added.loc[added.hhmm_int.eq(int(row.signal_end.replace(":", "")))
                         & added.side.eq(row.side)]
        part_days = added_days.loc[part.index]
        for period, subset in (("train", part.loc[part_days.isin(train_days)]),
                               ("holdout", part.loc[part_days.isin(holdout_days)]),
                               ("all", part)):
            realized = metrics(subset)
            for field in ("fills", "wins", "win_rate_pct", "net_pnl_rs"):
                decisions.loc[index, f"portfolio_{period}_{field}"] = realized[field]
    decisions["portfolio_qualified"] = (
        decisions.qualified
        & decisions.portfolio_train_fills.ge(config["minimum_training_fills"])
        & decisions.portfolio_train_win_rate_pct.ge(config["required_win_rate_pct_per_setup"])
        & decisions.portfolio_holdout_fills.ge(config["minimum_holdout_fills_for_validation"])
        & decisions.portfolio_holdout_win_rate_pct.ge(config["required_win_rate_pct_per_setup"])
        & decisions.portfolio_all_win_rate_pct.ge(config["required_win_rate_pct_per_setup"])
    )
    baseline_metric, variant_metric, added_metric = metrics(baseline), metrics(variant), metrics(added)
    old = variant.loc[variant.sid.isin(baseline_orders.sid)].set_index("sid").sort_index()
    original = baseline.set_index("sid").sort_index()
    if not old.index.equals(original.index):
        raise RuntimeError("G4 baseline order identity changed in combined portfolio")
    changed_g3 = int((old.portfolio_executed.astype(str).ne(original.portfolio_executed.astype(str))
                      | ~np.isclose(old.portfolio_net_profit_rupees.astype(float),
                                    original.portfolio_net_profit_rupees.astype(float),
                                    rtol=0, atol=1e-6, equal_nan=True)).sum())
    by_day_baseline = pd.to_datetime(baseline.day).dt.date
    by_day_variant = pd.to_datetime(variant.day).dt.date
    period_metrics = dict(training=dict(
        baseline_g3=metrics(baseline.loc[by_day_baseline.isin(train_days)]),
        g4_all_12=metrics(variant.loc[by_day_variant.isin(train_days)])),
        holdout=dict(
        baseline_g3=metrics(baseline.loc[by_day_baseline.isin(holdout_days)]),
        g4_all_12=metrics(variant.loc[by_day_variant.isin(holdout_days)])))
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(),
                   sessions=len(days), first_date=str(days[0]), last_date=str(days[-1]),
                   training_sessions=config["training_sessions"], holdout_sessions=config["holdout_sessions"],
                   excluded_session="2026-10-01: incomplete immutable input",
                   baseline_g3=baseline_metric, g4_all_12=variant_metric, added_late_setups=added_metric,
                   net_pnl_delta_rs=variant_metric["net_pnl_rs"] - baseline_metric["net_pnl_rs"],
                   g3_baseline_trades_changed_by_portfolio=changed_g3,
                   period_metrics=period_metrics,
                   chosen_levels=chosen,
                   research_qualified_setups=decisions.loc[decisions.qualified, "setup"].tolist(),
                   qualified_setups=decisions.loc[decisions.portfolio_qualified, "setup"].tolist(),
                   all_12_production_ready=bool(decisions.portfolio_qualified.all()),
                   result_status="EXPLORATORY_ALL_12" if not decisions.portfolio_qualified.all()
                                 else "ALL_12_OBSERVED_QUALIFICATION_MET",
                   candidate_variants_per_side={side: len(config["candidate_variants"][side])
                                                for side in config["sides"]},
                   qualification_rule="Standalone and combined-portfolio: training >=5 fills and >=70% wins; later 11 sessions >=3 fills and >=70% wins; full observed >=70%",
                   per_setup_observed_win_rate_is_not_future_guarantee=True,
                   source_evidence=evidence, config_path=str(Path(config_path).resolve()),
                   config_sha256=sha256(config_path), g3_pinned_v2_backtest_run=str(g3_run))
    output.mkdir(parents=True, exist_ok=True)
    artifacts = dict(candidate_outcomes=labeled, strictness_sweep=ladder,
                     setup_decisions=decisions, selection_audit=audit,
                     g3_baseline_orders=baseline_orders, g4_added_orders=new_orders,
                     g3_baseline_trades=baseline, g4_all_12_trades=variant,
                     g4_added_trades=added,
                     daily_results=pd.concat((_daily(baseline, days, "G3_BASELINE"),
                                              _daily(variant, days, "G4_ALL_12")), ignore_index=True))
    for name, frame in artifacts.items():
        frame.to_csv(output / f"{name}.csv", index=False)
    selected_config = dict(config, chosen_levels=chosen,
                           qualification={row.setup: bool(row.portfolio_qualified)
                                          for row in decisions.itertuples(index=False)})
    (output / "selected_config.json").write_text(json.dumps(selected_config, indent=2) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    manifest = dict(variant_id=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                    code=str(Path(__file__).resolve()), code_sha256=sha256(Path(__file__)),
                    files={path.name: sha256(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary
