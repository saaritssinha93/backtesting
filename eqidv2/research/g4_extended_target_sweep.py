"""Target-only sweeps for frozen G4 morning, midday and afternoon configurations.

Every target uses the native minute simulator and a fresh capital allocation.
Historical best targets are explicitly separated from training-only choices.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from research import g3_v2_contract as contract
from research.g3_0920_backtest import allocate, _stats
from research.g3_0920_target_sweep import assert_same_entries
from research.g3_g4_late_combined_backtest import checked_allocate, same_raw, verified_run
from research.g3_g4_late_target_sweep import load_paths, replay_target, raw_stats
from research.g4_extended_slot_backtest import ROOT, BACKTEST_ROOT, VERSION as REFERENCE_VERSION, portfolio_changed
from research.g4_late_slot_source import sha256

VERSION = "V13-V10-G-4-1035-1130-TARGET-SWEEP-V1"
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_target_sweep_v1.json"
MIDDAY_VERSION = "V13-V10-G-4-1135-1230-TARGET-SWEEP-V2"
MIDDAY_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_target_sweep_1135_1230_v2.json"
AFTERNOON_VERSION = "V13-V10-G-4-1235-1330-TARGET-SWEEP-V3"
AFTERNOON_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_target_sweep_1235_1330_v3.json"
AFTERNOON_CLOCKS = ("12:35", "12:40", "12:45", "12:50", "12:55", "13:00",
                    "13:05", "13:10", "13:15", "13:20", "13:25", "13:30")
PROFILES = {
    VERSION: dict(reference_version=REFERENCE_VERSION, setups=22, orders=278, fills=245,
        ledger="all_22_diagnostic_trades.csv", require_additions=False,
        entry_policy="FREEZE_ALL_22_PREVIOUSLY_SELECTED_FILTERS_AND_278_ORDERS"),
    MIDDAY_VERSION: dict(reference_version="V13-V10-G-4-BACKTEST-1135-1230-V4",
        setups=24, orders=345, fills=287, ledger="all_24_diagnostic_trades.csv", require_additions=True,
        entry_policy="FREEZE_ALL_24_PREVIOUSLY_SELECTED_FILTERS_AND_345_ORDERS"),
    AFTERNOON_VERSION: dict(reference_version="V13-V10-G-4-BACKTEST-1235-1330-V6",
        setups=24, ledger="all_24_diagnostic_trades.csv", require_additions=True,
        entry_policy="FREEZE_ALL_24_PREVIOUSLY_SELECTED_FILTERS_AND_PINNED_ORDERS"),
}
RAW_COLUMNS = ["sid", "day", "signal_id", "setup_id", "signal_end", "side", "tradingsymbol",
    "confirmation_ts", "signal_ts", "hhmm_int", "picker", "traded_value", "volume_ratio",
    "price_change_pct", "filled", "entry_path_index", "entry_price", "entry_ts", "exit_price",
    "exit_ts", "exit_reason", "native_target_pct", "target_pct", "net_return_pct", "gross_return_pct",
    "cost_pct", "initial_stop_pct", "active_stop_pct_at_exit", "holding_minutes", "target_hit", "stop_hit"]
_WORKER = {}


def read_frozen_csv(path):
    """Preserve float trigger boundaries exactly when restoring frozen orders."""
    return pd.read_csv(path, float_precision="round_trip")


def configuration(path=DEFAULT_CONFIG):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    profile = PROFILES.get(value.get("variant_id"))
    if profile is None:
        raise ValueError("Unknown G4 target-sweep profile")
    expected = dict(scope="BACKTEST_ONLY", execution_authority=False,
        required_win_rate_pct=70., minimum_historical_fills=5, minimum_training_fills=5,
        minimum_later_fills=3, training_sessions=38, later_sessions=11, initial_stop_pct=1.25,
        tightened_stop_pct=1., tighten_after_minutes=120, entry_expiry_minutes=10, square_off="15:15",
        round_trip_cost_bps=5., capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.,
        entry_policy=profile["entry_policy"])
    if any(value.get(k) != v for k, v in expected.items()):
        raise ValueError("Target study must retain the frozen entry, stop and execution policies")
    numbers = [float(value[k]) for k in ("target_min_pct", "target_max_pct", "target_step_pct")]
    if (not all(np.isfinite(numbers)) or not .05 < numbers[0] <= 1. <= numbers[1] <= 10
            or numbers[2] != .01 or any(abs(v * 100 - round(v * 100)) > 1e-8 for v in numbers)):
        raise ValueError("Targets must include 1%, exceed modeled cost, use 0.01% steps and remain <=10%")
    value["targets_pct"] = [n / 100 for n in range(round(numbers[0] * 100), round(numbers[1] * 100) + 1)]
    return value


def validate_afternoon_cohort(summary, selected_config, orders, saved, baseline, days):
    """Validate the new cohort against its hash-pinned entry-study artifacts.

    Empty individual setups remain part of the 24-setup experiment. Counts are
    attested by the entry manifest rather than invented before that run exists.
    """
    version = PROFILES[AFTERNOON_VERSION]["reference_version"]
    expected = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        sessions=49, training_sessions=38, holdout_sessions=11, new_setups=24,
        first_date="2026-07-29", last_date="2026-10-12", signal_ends=list(AFTERNOON_CLOCKS))
    if any(summary.get(k) != v for k, v in expected.items()):
        raise ValueError("Frozen afternoon reference metadata changed")
    if (selected_config.get("variant_id") != version
            or selected_config.get("scope") != "BACKTEST_ONLY"
            or selected_config.get("execution_authority") is not False):
        raise ValueError("Frozen afternoon entry configuration identity changed")
    expected_setups = {f"{clock[:2]}{int(clock[-2:]) + 1:02d}_{side}": (clock, side)
        for clock in AFTERNOON_CLOCKS for side in ("LONG", "SHORT")}
    setups = selected_config.get("setups", [])
    if len(setups) != 24 or {s.get("setup_id") for s in setups} != set(expected_setups):
        raise ValueError("Frozen afternoon setup membership changed")
    labels = {}
    for setup in setups:
        clock, side = expected_setups[setup["setup_id"]]
        expected_rule = dict(signal_end=clock, side=side,
            confirmation_end=f"{clock[:2]}:{int(clock[-2:]) + 1:02d}", target_pct=1.,
            initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
            max_entries=1, picker="max_liquidity")
        if any(setup.get(k) != v for k, v in expected_rule.items()):
            raise ValueError("Frozen afternoon reference rule changed")
        labels[setup["setup_id"]] = f"{clock}_{side}"
    if (len(days) != 49 or days[0] != "2026-07-29" or days[-1] != "2026-10-12"
            or "2026-10-01" in days or len(set(days)) != 49
            or not set(orders.day).issubset(days) or not set(orders.setup_id).issubset(labels)
            or orders.sid.duplicated().any() or orders.signal_id.duplicated().any()
            or orders.sid.isna().any() or orders.signal_id.isna().any()
            or set(orders.sid) & set(baseline.sid)
            or saved.sid.duplicated().any() or set(saved.sid) != set(orders.sid) | set(baseline.sid)):
        raise ValueError("Frozen afternoon orders or session cohort changed")
    counts = (summary.get("selected_orders"), summary.get("potential_fills"))
    if (any(type(n) is not int or n < 0 for n in counts)
            or counts[0] != len(orders) or not len(orders)
            or counts[1] > counts[0]
            or counts[1] != int(saved.loc[saved.sid.isin(orders.sid), "filled"].eq(True).sum())):
        raise ValueError("Frozen afternoon order/fill counts changed")
    for setup_id, frame in orders.groupby("setup_id"):
        clock, side = expected_setups[setup_id]
        if (not frame.signal_end.eq(clock).all() or not frame.side.eq(side).all()
                or not frame.native_target_pct.eq(1.).all()
                or not frame.initial_stop_pct.eq(1.25).all()):
            raise ValueError("Frozen afternoon order policy changed")
    return labels, counts[1]


def choose(rows, period, minimum, *, required=70.):
    eligible = [r for r in rows if r[f"{period}_fills"] >= minimum
        and r[f"{period}_win_rate_pct"] is not None
        and r[f"{period}_win_rate_pct"] >= required and r[f"{period}_net_pnl_rs"] > 0]
    return max(eligible, key=lambda r: (round(r[f"{period}_net_pnl_rs"], 6),
        -round(r[f"{period}_max_drawdown_rs"], 6), r[f"{period}_win_rate_pct"], -r["target_pct"])) if eligible else None


def eligible_period(row, period, minimum):
    return bool(row is not None and row[f"{period}_fills"] >= minimum
        and row[f"{period}_win_rate_pct"] is not None and row[f"{period}_win_rate_pct"] >= 70
        and row[f"{period}_net_pnl_rs"] > 0)


def common_eligible(row, period, *, require_additions=False):
    """A profitable baseline cannot hide weak new additions in the midday study."""
    prefixes = ("", "added_") if require_additions else ("",)
    return all(row.get(f"{prefix}{period}_win_rate_pct") is not None
        and row[f"{prefix}{period}_win_rate_pct"] >= 70
        and row[f"{prefix}{period}_net_pnl_rs"] > 0 for prefix in prefixes)


def scenario_ranking(portfolio, setup_results):
    """Rank actual executions, distinguishing portfolio-wide and per-setup 70%."""
    rows = []
    full = portfolio.loc[portfolio.period.eq("all")]
    for name, group in full.groupby("scenario", sort=True):
        combined = group.loc[group.scope.eq("combined")].iloc[0]
        added = group.loc[group.scope.eq("additions")].iloc[0]
        setups = setup_results.loc[setup_results.scenario.eq(name) & setup_results.period.eq("all")]
        passing = (len(setups) > 0 and added.executed_trades >= 5
            and added.win_rate_pct >= 70 and added.net_pnl_rs > 0
            and combined.win_rate_pct >= 70 and combined.net_pnl_rs > 0)
        rows.append(dict(scenario=name, setups=len(setups),
            added_executed_trades=int(added.executed_trades), added_win_rate_pct=added.win_rate_pct,
            added_net_pnl_rs=added.net_pnl_rs, combined_win_rate_pct=combined.win_rate_pct,
            combined_net_pnl_rs=combined.net_pnl_rs,
            baseline_trades_changed=int(combined.baseline_trades_changed),
            portfolio_70_positive=bool(passing),
            every_setup_70_positive=bool(passing and setups.observed_70_positive.all())))
    return pd.DataFrame(rows).sort_values(["combined_net_pnl_rs", "scenario"], ascending=[False, True])


def decisions_for(rows, setup_ids, config):
    decisions, historical, training, chronological = [], {}, {}, {}
    for setup_id in sorted(setup_ids):
        trials = [r for r in rows if r["setup_id"] == setup_id]
        fit = choose(trials, "all", config["minimum_historical_fills"])
        train = choose(trials, "train", config["minimum_training_fills"])
        reference = next(r for r in trials if r["target_pct"] == 1.)
        observed = [r for r in trials if r["all_win_rate_pct"] is not None]
        best_rate = max(observed, key=lambda r: (r["all_win_rate_pct"], r["all_net_pnl_rs"],
            -r["target_pct"])) if observed else None
        later_pass = (eligible_period(train, "holdout", config["minimum_later_fills"])
                      and eligible_period(train, "all", config["minimum_historical_fills"]))
        if fit:
            historical[setup_id] = fit["target_pct"]
        if train:
            training[setup_id] = train["target_pct"]
        if later_pass:
            chronological[setup_id] = train["target_pct"]
        row = dict(setup=reference["setup"], setup_id=setup_id,
            original_target_pct=1., original_fills=reference["all_fills"],
            original_win_rate_pct=reference["all_win_rate_pct"], original_net_pnl_rs=reference["all_net_pnl_rs"],
            historical_pass=fit is not None, historical_target_pct=fit["target_pct"] if fit else None,
            training_pass=train is not None, training_target_pct=train["target_pct"] if train else None,
            chronological_check_pass=later_pass, small_sample=reference["all_fills"] < 10,
            best_observed_win_rate_pct=best_rate["all_win_rate_pct"] if best_rate else None,
            target_at_best_observed_win_rate_pct=best_rate["target_pct"] if best_rate else None)
        for label, selected in (("historical", fit), ("training", train)):
            for period in ("train", "holdout", "all"):
                for field in ("fills", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"):
                    row[f"{label}_{period}_{field}"] = selected[f"{period}_{field}"] if selected else None
        decisions.append(row)
    return pd.DataFrame(decisions), historical, training, chronological


def _worker_init(orders, paths, baseline, reference, days, setup_labels=None):
    labels = setup_labels or {setup_id: f"{frame.signal_end.iloc[0]}_{frame.side.iloc[0]}"
        for setup_id, frame in orders.groupby("setup_id", sort=True)}
    _WORKER.update(orders=orders, paths=paths, baseline=baseline, reference=reference,
                   periods=dict(train=days[:38], holdout=days[38:], all=days), setup_labels=labels)


def _trial(target):
    raw = replay_target(_WORKER["orders"], _WORKER["paths"], target)
    assert_same_entries(raw, _WORKER["reference"])
    rows = []
    for setup_id, label in sorted(_WORKER["setup_labels"].items()):
        frame = raw.loc[raw.setup_id.eq(setup_id)]
        row = dict(setup=label, setup_id=setup_id, target_pct=target)
        for period, days in _WORKER["periods"].items():
            row.update({f"{period}_{k}": v for k, v in raw_stats(frame, days).items()})
        rows.append(row)
    combined = allocate(pd.concat((_WORKER["baseline"], raw), ignore_index=True, sort=False))
    added = combined.loc[combined.sid.isin(raw.sid)]
    common = dict(target_pct=target)
    for period, days in _WORKER["periods"].items():
        part = combined.loc[combined.day.isin(days)]
        common.update({f"{period}_{k}": v for k, v in _stats(part, days).items()})
        common.update({f"added_{period}_{k}": v for k, v in _stats(added.loc[added.day.isin(days)], days).items()})
    common["baseline_trades_changed"] = portfolio_changed(_WORKER["baseline"], combined)
    return target, raw[RAW_COLUMNS].copy(), rows, common


def mix_cached(raw_by_target, targets, reference):
    parts = [raw_by_target[target].loc[raw_by_target[target].setup_id.eq(setup)] for setup, target in sorted(targets.items())]
    return pd.concat(parts, ignore_index=True) if parts else reference.iloc[:0].copy()


def portfolio_qualified_subset(raw_by_target, targets, baseline, reference, days, *, minimum=5):
    """Remove failing setups and reallocate until every retained setup qualifies.

    This deterministic historical screen is descriptive, not a global optimum.
    Removing positions may admit different fills, so a single pruning pass is
    insufficient. Every removal and its actual allocated statistics is recorded.
    """
    retained, removals = dict(targets), []
    for iteration in range(len(targets) + 1):
        raw = mix_cached(raw_by_target, retained, reference)
        ledger = allocate(pd.concat((baseline, raw), ignore_index=True, sort=False))
        added = ledger.loc[ledger.sid.isin(raw.sid)]
        failed = []
        for setup_id, target in sorted(retained.items()):
            stats = _stats(added.loc[added.setup_id.eq(setup_id)], days)
            if (stats["executed_trades"] < minimum or stats["win_rate_pct"] is None
                    or stats["win_rate_pct"] < 70 or stats["net_pnl_rs"] <= 0):
                failed.append(setup_id)
                removals.append(dict(iteration=iteration + 1, setup_id=setup_id,
                    target_pct=target, **stats))
        if not failed:
            return retained, removals
        for setup_id in failed:
            retained.pop(setup_id)
    raise RuntimeError("Portfolio qualification did not converge")


def write_report(output, summary, decisions, portfolio, scenario_setups):
    intro = (f"Targets {summary['target_min_pct']:.2f}% to {summary['target_max_pct']:.2f}% in 0.01% steps; "
        f"{summary['setups']} fixed entry configurations, {summary['selected_orders']} selected orders and {summary['potential_fills']} potential fills. "
        "49 sessions from 29 July to 12 October 2026; October 1 excluded. "
        "Best means greatest positive net P&L among tested targets with at least 70% net winning trades. "
        "Historical target choices use all 49 sessions and are an in-sample fit. "
        "Training choices use the first 38 sessions; the later 11 were already used in previous research.")
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 target optimization</title>
<style>body{font:15px/1.5 system-ui,sans-serif;margin:30px;background:#f4f6fa;color:#172a3d}h1,h2{line-height:1.2}p{max-width:1150px}.notice{background:#fff0ce;padding:16px;border-left:4px solid #b98105}.scroll{overflow:auto;max-height:700px;margin-bottom:28px}table{border-collapse:collapse;background:white;width:100%}td,th{padding:8px;border-bottom:1px solid #d9e0e8;white-space:nowrap;text-align:right}th{background:#213c57;color:white;position:sticky;top:0}td:first-child,th:first-child{text-align:left}a{color:#065dbc}</style></head><body><h1>G4: target sweep for all SETUP_COUNT setups</h1>'''
    page = page.replace('SETUP_COUNT', str(summary['setups']))
    page += f'<p>{html.escape(intro)}</p>'
    page += '<div class=notice>Historical best is descriptive, not independent validation. A target selected using later outcomes cannot claim those outcomes as an unseen test. Small samples and many tested targets make 70% historical wins uncertain evidence for future trading.</div>'
    page += '<p>Fixed exits: 1.25% initial stop, tightening to 1.00% after 120 minutes; exact next-minute confirmation; ten-minute trigger expiry; 15:15 square-off. Capital: Rs 100,000 per entry × 5 exposure; Rs 1,000,000 total capital. Five basis points round-trip cost; no additional market impact or broker-specific charges. Wins mean positive net P&amp;L, including time exits. Drawdown uses daily realized P&amp;L.</p>'
    page += '<p>Every tested common target and every reported scenario is replayed through portfolio capital constraints. The pinned G4 baseline and its targets are retained. The independent per-setup optimum maximizes each setup’s standalone P&amp;L; it is not proof of a global optimum across all possible interacting target combinations.</p>'
    all_stats = portfolio.loc[portfolio.period.eq("all"), ["scenario", "scope", "selected_orders", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs", "baseline_trades_changed"]]
    hist_cols = ["setup", "original_target_pct", "original_win_rate_pct", "original_net_pnl_rs",
        "historical_target_pct", "historical_all_fills", "historical_all_wins", "historical_all_win_rate_pct",
        "historical_all_net_pnl_rs", "historical_pass", "small_sample"]
    train_cols = ["setup", "training_target_pct", "training_train_fills", "training_train_win_rate_pct",
        "training_holdout_fills", "training_holdout_win_rate_pct", "training_holdout_net_pnl_rs", "chronological_check_pass"]
    later = portfolio.loc[portfolio.period.eq("holdout"), ["scenario", "scope", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs"]]
    actual = scenario_setups.loc[scenario_setups.scenario.eq("historical_profitable70_only") & scenario_setups.period.eq("all"),
        ["setup_id", "target_pct", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "observed_70_positive"]]
    ranking = pd.read_csv(output / "scenario_comparison.csv")
    tables = [("Actual portfolio ranking: maximum net P&L among tested scenarios", ranking),
        ("Portfolio results: all 49 sessions", all_stats), ("Best historical target for each setup", decisions[hist_cols]),
        ("Actual executions at historical per-setup targets", actual),
        ("Training-selected targets and later check", decisions[train_cols]), ("Later 11 sessions: scenario comparison", later)]
    for title, frame in tables:
        page += f'<h2>{html.escape(title)}</h2><div class=scroll>{frame.to_html(index=False,border=0,na_rep="—",float_format=lambda v:f"{v:,.2f}")}</div>'
    page += f"<p>Baseline: {html.escape(summary['baseline_description'])}. The entry study excluded {summary['reference_source_rejections']} malformed liquidity inputs before selection; those exclusions remain fixed. Best tested scenario with 70% in both the new trades and combined book: {html.escape(str(summary['best_tested_portfolio70_scenario']))}. Best tested scenario with every included new setup also passing 70% after allocation: {html.escape(str(summary['best_tested_every_setup70_scenario']))}.</p>"
    page += '<p>Configurations without a qualifying target remain at 1% only in the explicitly named all-setup diagnostic scenarios; the qualifying-only scenarios exclude them. The chronological-check subset uses later outcomes for membership. A common target may meet portfolio-wide 70% while individual setups fail it.</p>'
    if summary.get("portfolio_requalification"):
        page += '<p>The historical_portfolio_qualified70_only scenario repeatedly removes every setup failing the minimum fills, 70% wins or positive net P&amp;L requirement after capital allocation, then reallocates until all remaining setups pass. Membership uses the full historical period. It is a deterministic descriptive subset, not a global optimum. Empty setups remain visible in the all-setup diagnostics.</p>'
    page += '<p>Entry filters were selected sequentially at the original 1% target and then frozen. This experiment optimizes targets within those fixed entries; it does not jointly search every entry-filter and target combination.</p>'
    page += f'<p>Historical qualifying setups: {summary["historical_70_setup_count"]}/{summary["setups"]}. Training-qualified setups: {summary["training_70_setup_count"]}/{summary["setups"]}. Standalone chronological-screen passes: {summary["chronological_check_count"]}/{summary["setups"]}.</p>'
    page += '<p>' + ' · '.join(f'<a href="{p}">{p}</a>' for p in ("best_targets.csv", "scenario_comparison.csv", "target_sweep.csv", "common_target_sweep.csv", "daily_results.csv", "monthly_results.csv", "scenario_setup_results.csv", "scenario_configs.json", "summary.json", "manifest.json")) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(config_path=DEFAULT_CONFIG, output_dir=None, *, workers=3):
    config = configuration(config_path)
    version = config["variant_id"]
    profile = PROFILES[version]
    count_setups = profile["setups"]
    now = datetime.now(contract.IST)
    output = Path(output_dir or BACKTEST_ROOT / "target_sweeps" / f"{now:%Y-%m-%d}" / f"{now:%Y%m%dT%H%M%S%f}_all{count_setups}_targets").resolve()
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Output must be new and within the G4 backtest root")
    reference_dir = (BACKTEST_ROOT / config["reference_run_relative"]).resolve()
    if (not reference_dir.is_relative_to(BACKTEST_ROOT.resolve())
            or sha256(reference_dir / "manifest.json") != config["reference_manifest_sha256"]):
        raise ValueError("Frozen G4 reference manifest changed")
    proof = verified_run(reference_dir, profile["reference_version"])
    required_artifacts = {"summary.json", "selected_orders.csv", profile["ledger"],
        "baseline_g4_trades.csv", "daily_results.csv", "selected_config.json"}
    reference_manifest = json.loads((reference_dir / "manifest.json").read_text(encoding="utf-8"))
    if not required_artifacts.issubset(reference_manifest["files"]):
        raise ValueError("Frozen reference manifest does not attest required input artifacts")
    summary_source = json.loads((reference_dir / "summary.json").read_text(encoding="utf-8"))
    previous_config = json.loads((reference_dir / "selected_config.json").read_text(encoding="utf-8"))
    orders = read_frozen_csv(reference_dir / "selected_orders.csv")
    saved = read_frozen_csv(reference_dir / profile["ledger"])
    baseline_saved = read_frozen_csv(reference_dir / "baseline_g4_trades.csv")
    days = sorted(pd.read_csv(reference_dir / "daily_results.csv").day.unique())
    if version == AFTERNOON_VERSION:
        setup_labels, expected_fills = validate_afternoon_cohort(
            summary_source, previous_config, orders, saved, baseline_saved, days)
    else:
        if len(days) != 49 or len(orders) != profile["orders"] or orders.setup_id.nunique() != count_setups:
            raise ValueError("Frozen setup cohort changed")
        setup_labels = {setup_id: f"{frame.signal_end.iloc[0]}_{frame.side.iloc[0]}"
            for setup_id, frame in orders.groupby("setup_id", sort=True)}
        expected_fills = profile["fills"]
    for name in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        orders[name] = pd.to_datetime(orders[name], utc=True).dt.tz_convert(contract.IST)
    paths, path_proof = load_paths(orders, summary_source)
    reference = replay_target(orders, paths, 1.)
    reference["day"] = reference.day.astype(str)
    same_raw(reference, saved.loc[saved.sid.isin(orders.sid)])
    baseline = checked_allocate(baseline_saved, baseline_saved)
    combined_reference = allocate(pd.concat((baseline, reference), ignore_index=True))
    checked_allocate(combined_reference, saved)
    if int(reference.filled.sum()) != expected_fills:
        raise ValueError("Reference fill count changed")
    modules = [Path(__file__), Path(simulator.__file__), Path(simulator.g.v9.v5.__file__), Path(simulator.g.v9.v6.__file__),
        Path(contract.__file__), ROOT / "research/g4_extended_slot_backtest.py", ROOT / "research/g4_late_slot_source.py",
        ROOT / "research/g3_g4_late_target_sweep.py", ROOT / "research/g3_g4_late_combined_backtest.py",
        ROOT / "research/g3_0920_target_sweep.py", ROOT / "research/g3_0920_backtest.py"]
    initial_code_hashes = {str(p.relative_to(ROOT)): sha256(p) for p in modules}
    initial_config_hash = sha256(config_path)
    output.mkdir(parents=True)
    print(f"Verified 1% reference: {len(orders)} orders / {int(reference.filled.sum())} fills, {count_setups} setups. Output: {output}", flush=True)
    rows, common_rows, raw_by_target = [], [], {}
    targets = config["targets_pct"]
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
            initargs=(orders, paths, baseline, reference, days, setup_labels)) as executor:
        pending = [executor.submit(_trial, target) for target in targets]
        for count, future in enumerate(as_completed(pending), 1):
            target, raw, stats, common = future.result()
            raw_by_target[target] = raw
            rows.extend(stats)
            common_rows.append(common)
            if count % 25 == 0 or count == len(targets):
                print(f"Targets completed: {count}/{len(targets)}; native entries unchanged", flush=True)
    table = pd.DataFrame(rows).sort_values(["setup_id", "target_pct"])
    common_table = pd.DataFrame(common_rows).sort_values("target_pct")
    table.to_csv(output / "target_sweep.csv", index=False)
    common_table.to_csv(output / "common_target_sweep.csv", index=False)
    pd.concat([raw_by_target[t] for t in sorted(raw_by_target)], ignore_index=True).to_parquet(
        output / "minute_replay_all_targets.parquet", index=False)
    decisions, historical, training, chronological = decisions_for(rows, setup_labels, config)
    decisions.to_csv(output / "best_targets.csv", index=False)
    original_map = {s: 1. for s in setup_labels}
    common_historical = [r for r in common_rows if common_eligible(r, "all", require_additions=profile["require_additions"])]
    common_best = max(common_historical, key=lambda r: (round(r["all_net_pnl_rs"], 6),
        -round(r["all_max_drawdown_rs"], 6), -r["target_pct"])) if common_historical else None
    common_training = [r for r in common_rows if common_eligible(r, "train", require_additions=profile["require_additions"])]
    common_train_best = max(common_training, key=lambda r: (round(r["train_net_pnl_rs"], 6),
        -round(r["train_max_drawdown_rs"], 6), -r["target_pct"])) if common_training else None
    plans = {f"original_1pct_all{count_setups}": original_map,
        f"historical_best_all{count_setups}_diagnostic": {**original_map, **historical},
        "historical_profitable70_only": historical,
        f"training_selected_all{count_setups}_diagnostic": {**original_map, **training},
        "training_eligible_only": training, "chronological_check_subset": chronological}
    if common_best:
        plans["historical_common_target_best70"] = {s: common_best["target_pct"] for s in original_map}
    if common_train_best:
        plans["training_common_target_best70"] = {s: common_train_best["target_pct"] for s in original_map}
    portfolio_requalification = {}
    if version == AFTERNOON_VERSION:
        qualified, removals = portfolio_qualified_subset(raw_by_target, historical,
            baseline, reference, days, minimum=config["minimum_historical_fills"])
        plans["historical_portfolio_qualified70_only"] = qualified
        portfolio_requalification = dict(method="ITERATIVELY_REMOVE_EVERY_FAILING_SETUP_THEN_REALLOCATE",
            starting_targets=historical, retained_targets=qualified, removals=removals,
            historical_selection_is_in_sample=True, global_optimum=False)
    scenario_rows, setup_rows, daily, monthly, impacts = [], [], [], [], []
    periods = dict(train=days[:38], holdout=days[38:], all=days)
    ledgers = {}
    for name, target_map in plans.items():
        wanted = orders.loc[orders.setup_id.isin(target_map)].copy()
        raw = replay_target(wanted, {int(s): paths[int(s)] for s in wanted.sid}, target_map) if len(wanted) else reference.iloc[:0].copy()
        cached = mix_cached(raw_by_target, target_map, reference)
        if len(raw):
            assert_same_entries(raw, reference.loc[reference.sid.isin(raw.sid)])
            same_raw(raw, cached)
        raw["research_exit_variant"] = version
        fingerprint = hashlib.sha256(json.dumps(dict(config=config, targets=target_map), sort_keys=True).encode()).hexdigest()
        raw["research_exit_fingerprint"] = fingerprint
        combined = allocate(pd.concat((baseline, raw), ignore_index=True, sort=False))
        added = combined.loc[combined.sid.isin(raw.sid)].copy()
        old = combined.loc[combined.sid.isin(baseline.sid)]
        same_raw(old, baseline)
        changed = portfolio_changed(baseline, combined)
        if changed:
            a, b = (x.set_index("sid").sort_index() for x in (baseline, old))
            for sid in a.index[a.portfolio_executed.ne(b.portfolio_executed) | ~np.isclose(a.portfolio_net_profit_rupees, b.portfolio_net_profit_rupees, atol=1e-6, rtol=0)]:
                impacts.append(dict(scenario=name, sid=int(sid), day=a.loc[sid,"day"], symbol=a.loc[sid,"tradingsymbol"],
                    setup_id=a.loc[sid,"setup_id"], before_pnl_rs=a.loc[sid,"portfolio_net_profit_rupees"],
                    after_pnl_rs=b.loc[sid,"portfolio_net_profit_rupees"]))
        ledgers[name] = combined
        for period, cohort in periods.items():
            for scope, frame in (("combined", combined), ("additions", added)):
                part = frame.loc[frame.day.isin(cohort)]
                scenario_rows.append(dict(scenario=name, scope=scope, period=period,
                    **_stats(part, cohort), baseline_trades_changed=changed))
            for setup_id, target in sorted(target_map.items()):
                part = added.loc[added.setup_id.eq(setup_id) & added.day.isin(cohort)]
                stats = _stats(part, cohort)
                setup_rows.append(dict(scenario=name, setup_id=setup_id, target_pct=target, period=period,
                    **stats, observed_70_positive=stats["executed_trades"] >= (3 if period == "holdout" else 5)
                    and stats["win_rate_pct"] is not None and stats["win_rate_pct"] >= 70 and stats["net_pnl_rs"] > 0))
        for day in days:
            daily.append(dict(scenario=name, day=day, **_stats(combined.loc[combined.day.eq(day)], [day]),
                added_net_pnl_rs=float(added.loc[added.day.eq(day)].portfolio_net_profit_rupees.sum())))
        for month in sorted({d[:7] for d in days}):
            cohort = [d for d in days if d.startswith(month)]
            monthly.append(dict(scenario=name, month=month, **_stats(combined.loc[combined.day.isin(cohort)], cohort)))
        combined.to_csv(output / f"{name}_trades.csv", index=False)
        raw.to_csv(output / f"{name}_added_raw_trades.csv", index=False)
        print(f"Final replay {name}: {_stats(combined, days)['net_pnl_rs']:,.2f} net; "
              f"{_stats(combined, days)['win_rate_pct']:.2f}% wins; old trades affected={changed}", flush=True)
    portfolio = pd.DataFrame(scenario_rows)
    setup_results = pd.DataFrame(setup_rows)
    ranking = scenario_ranking(portfolio, setup_results)
    ranking.to_csv(output / "scenario_comparison.csv", index=False)
    for file, frame in (("portfolio_results", portfolio), ("scenario_setup_results", setup_results),
                         ("daily_results", pd.DataFrame(daily)), ("monthly_results", pd.DataFrame(monthly)),
                         ("portfolio_impacts", pd.DataFrame(impacts, columns=["scenario", "sid", "day", "symbol", "setup_id", "before_pnl_rs", "after_pnl_rs"]))):
        frame.to_csv(output / f"{file}.csv", index=False)
    portable = {name: [{**setup, "target_pct": target_map[setup["setup_id"]],
        "prior_entry_training_pass": setup["training_pass"], "prior_entry_chronological_check_pass": setup["chronological_check_pass"]}
        for setup in previous_config["setups"] if setup["setup_id"] in target_map] for name, target_map in plans.items()}
    # Previous 1% qualification is evidence about the entry study, not approval
    # for the changed targets. Target qualification is in best_targets.csv.
    for setups in portable.values():
        for setup in setups:
            setup.pop("training_pass", None)
            setup.pop("chronological_check_pass", None)
    (output / "scenario_configs.json").write_text(json.dumps(dict(variant_id=version, scope="BACKTEST_ONLY",
        execution_authority=False, reference=proof, fixed_policy=config, scenarios=portable), indent=2) + "\n", encoding="utf-8")
    summary = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=now.isoformat(), sessions=49, first_date=days[0], last_date=days[-1],
        training_sessions=38, later_sessions=11, selected_orders=len(orders), potential_fills=int(reference.filled.sum()),
        setups=count_setups, target_levels=len(targets), target_trials=len(table), target_min_pct=min(targets), target_max_pct=max(targets),
        best_tested_portfolio70_scenario=ranking.loc[ranking.portfolio_70_positive, "scenario"].iloc[0] if ranking.portfolio_70_positive.any() else None,
        best_tested_every_setup70_scenario=ranking.loc[ranking.every_setup_70_positive, "scenario"].iloc[0] if ranking.every_setup_70_positive.any() else None,
        common_target_requires_additions70=profile["require_additions"],
        baseline_description=summary_source.get("baseline_description", "Original 27-setup G4 baseline"),
        reference_source_rejections=summary_source.get("rejected_source_candidates", 0),
        historical_70_setup_count=len(historical), training_70_setup_count=len(training), chronological_check_count=len(chronological),
        portfolio_requalification=portfolio_requalification,
        historical_targets=historical, training_targets=training, chronological_check_targets=chronological,
        common_best=common_best, common_training_best=common_train_best,
        selected_upper_boundary_setups=[s for s,t in historical.items() if t == max(targets)],
        source=proof, minute_path_evidence=path_proof, config_path=str(Path(config_path).resolve()), config_sha256=initial_config_hash,
        fixed_exit=dict(initial_stop_pct=1.25, tightened_stop_pct=1., tighten_after_minutes=120,
                        cost_bps=5., square_off="15:15"),
        reference_replay_parity=True, entry_parity_all_targets=True, final_scenarios_replayed_independently=True,
        historical_selection_is_in_sample=True, later_period_previously_used=True, untouched_out_of_sample=False,
        entry_optimization_scope="FROZEN_SEQUENTIAL_ENTRY_SELECTION_AT_1_PERCENT_THEN_TARGET_ONLY_GRID",
        optimum_scope="EXHAUSTIVE_PER_SETUP_AND_COMMON_TARGET_GRID; INDEPENDENT_TARGET_MAP_RECHECKED_IN_PORTFOLIO; NOT_GLOBAL_COMBINATORIAL_OPTIMUM",
        all_scenarios=json.loads(portfolio.to_json(orient="records")),
        baseline=_stats(baseline, days), excluded_session=summary_source["excluded_session"])
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    write_report(output, summary, decisions, portfolio, setup_results)
    final_code_hashes = {str(p.relative_to(ROOT)): sha256(p) for p in modules}
    if initial_code_hashes != final_code_hashes or initial_config_hash != sha256(config_path):
        raise ValueError("Target-sweep code or configuration changed during execution")
    manifest = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        code_sha256=initial_code_hashes,
        files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(decisions[["setup", "historical_target_pct", "historical_all_fills", "historical_all_win_rate_pct", "historical_all_net_pnl_rs", "chronological_check_pass"]].to_string(index=False), flush=True)
    print(output, flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--workers", type=int, default=3, choices=range(1, 5))
    args = parser.parse_args()
    run(args.config, args.output_dir, workers=args.workers)


if __name__ == "__main__":
    main()
