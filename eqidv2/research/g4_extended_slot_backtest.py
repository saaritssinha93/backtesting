"""G4 morning, midday and afternoon additions: strict, relaxed 1m, then relaxed 5m.

Select on training data only, before examining the chronological check.
All source and execution code is shared with the original G4 study. The
archived V2 selection contract isolates this experiment from current G3.
"""
from __future__ import annotations

import html
import json
import math
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from eqidv2_runtime_paths import runtime_dir
from research import g3_v2_contract as contract
from research.g3_0920_backtest import allocate, _stats
from research.g3_g4_late_combined_backtest import checked_allocate, same_raw, verified_run
from research.g4_late_slot_backtest import configuration as original_configuration
from research.g4_late_slot_source import load_late_source, sha256

ROOT = Path(__file__).resolve().parents[1]
VERSION = "V13-V10-G-4-BACKTEST-1035-1130-V2"
CLOCKS = ("10:35", "10:40", "10:45", "10:50", "10:55", "11:00", "11:05",
          "11:10", "11:15", "11:25", "11:30")
STAGES = ("strict", "relax_1m", "relax_5m")
DEFAULT_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_1035_1130_v2.json"
BACKTEST_ROOT = runtime_dir("backtesting_result_v13_v10_g_4")
DEFAULT_CACHE = BACKTEST_ROOT / "source_cache/1035_1130_v2"
MIDDAY_VERSION = "V13-V10-G-4-BACKTEST-1135-1230-V4"
MIDDAY_CLOCKS = ("11:35", "11:40", "11:45", "11:50", "11:55", "12:00",
                 "12:05", "12:10", "12:15", "12:20", "12:25", "12:30")
MIDDAY_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_1135_1230_v4.json"
MIDDAY_CACHE = BACKTEST_ROOT / "source_cache/1135_1230_v4"
AFTERNOON_VERSION = "V13-V10-G-4-BACKTEST-1235-1330-V6"
AFTERNOON_CLOCKS = ("12:35", "12:40", "12:45", "12:50", "12:55", "13:00",
                    "13:05", "13:10", "13:15", "13:20", "13:25", "13:30")
AFTERNOON_CONFIG = ROOT / "configs/strategies/v13_v10_g_4_backtest_1235_1330_v6.json"
AFTERNOON_CACHE = BACKTEST_ROOT / "source_cache/1235_1330_v6"
PROFILES = {
    VERSION: dict(clocks=CLOCKS, sid_base=5_000_000, cache=DEFAULT_CACHE,
        baseline_version="V13-V10-G-4-BACKTEST-LATE-SLOTS-V1",
        baseline_ledger="g4_all_12_trades.csv", baseline_setups=27,
        baseline_description="Original G4: G3 V2 and twelve 10:05-10:30 additions"),
    MIDDAY_VERSION: dict(clocks=MIDDAY_CLOCKS, sid_base=6_000_000, cache=MIDDAY_CACHE,
        baseline_version="V13-V10-G-4-SELECTED-TEN-V3",
        baseline_ledger="combined_trades.csv", baseline_setups=37,
        baseline_description="Selected G4 V3: original 27 setups and ten selected 10:35-11:25 additions"),
    AFTERNOON_VERSION: dict(clocks=AFTERNOON_CLOCKS, sid_base=7_000_000, cache=AFTERNOON_CACHE,
        baseline_version="V13-V10-G-4-SELECTED-MIDDAY-TWELVE-V5",
        baseline_ledger="combined_trades.csv", baseline_setups=49,
        baseline_description="Selected G4 V5: existing 37 setups and twelve approved 11:35-12:30 additions"),
}


def confirmation(clock):
    return (datetime.strptime(clock, "%H:%M") + timedelta(minutes=1)).strftime("%H:%M")


def configuration(path=DEFAULT_CONFIG):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    profile = PROFILES.get(config.get("variant_id"))
    if profile is None:
        raise ValueError("Unknown G4 slot-study profile")
    expected = dict(scope="BACKTEST_ONLY", execution_authority=False,
        preserve_existing_g4_orders=True, signal_ends=list(profile["clocks"]), sides=["LONG", "SHORT"],
        confirmation_offset_minutes=1, entry_expiry_minutes=10,
        max_entries_per_setup_day=1, picker="max_liquidity", initial_stop_pct=1.25,
        tightened_stop_pct=1., tighten_after_minutes=120, target_pct=1., square_off="15:15",
        round_trip_cost_bps=5., capital_per_entry_rs=100_000., leverage=5.,
        portfolio_capital_rs=1_000_000., training_sessions=38, holdout_sessions=11,
        minimum_training_fills=5, minimum_holdout_fills=3, minimum_historical_fills=8,
        required_win_rate_pct=70., stage_order=list(STAGES),
        selection_policy="FIRST_TRAINING_PASSING_STAGE_THEN_MAX_POSITIVE_NET_PNL")
    if any(config.get(k) != v for k, v in expected.items()):
        raise ValueError("G4 configuration changed its declared clocks, exits or study protocol")
    source_path = (ROOT / config["strict_profile_config"]).resolve()
    if not source_path.is_relative_to(ROOT):
        raise ValueError("Strict profile must be a repository config")
    original = original_configuration(source_path)
    variants = {}
    for side in config["sides"]:
        entries = []
        seen = set()

        def add(stage, name, level):
            values = list(level.values())
            if not all(math.isfinite(float(v)) for v in values):
                raise ValueError("Nonfinite threshold")
            if (level["price_change_pct"] < .1 or not .05 <= level["oi_change_pct"] <= 1.
                    or level["five_minute_volume_ratio_min"] < .8
                    or not 0 <= level["one_minute_body_ratio_min"] <= 1
                    or not 0 <= level["one_minute_wick_ratio_max"] <= 1
                    or level["one_minute_volume_ratio_min"] < 0):
                raise ValueError("Relaxation exceeds the verified source envelope")
            key = json.dumps(level, sort_keys=True)
            if key not in seen:
                seen.add(key)
                entries.append(dict(index=len(entries), stage=stage, name=name, thresholds=level))

        for trial in original["candidate_variants"][side]:
            add("strict", trial["name"], dict(trial["thresholds"]))
        donor = original["levels"][side][0]
        one = config["relax_1m"][side]
        five = config["relax_5m"][side]
        if (max(one["body"]) > donor["one_minute_body_ratio_min"]
                or min(one["wick"]) < donor["one_minute_wick_ratio_max"]
                or max(one["volume"]) > donor["one_minute_volume_ratio_min"]
                or max(five["price"]) > donor["price_change_pct"]
                or max(five["oi"]) > donor["oi_change_pct"]
                or max(five["volume"]) > donor["five_minute_volume_ratio_min"]):
            raise ValueError("Relaxation grids must only loosen the donor filters")
        one_levels = []
        for body, wick, volume in product(one["body"], one["wick"], one["volume"]):
            level = dict(donor, one_minute_body_ratio_min=body,
                         one_minute_wick_ratio_max=wick, one_minute_volume_ratio_min=volume)
            one_levels.append(level)
            add("relax_1m", f"body{body}_wick{wick}_v1m{volume}", level)
        for price, oi, volume, level in product(five["price"], five["oi"], five["volume"], one_levels):
            changed = dict(level, price_change_pct=price, oi_change_pct=oi,
                           five_minute_volume_ratio_min=volume)
            add("relax_5m", f"p{price}_oi{oi}_v5m{volume}_b{level['one_minute_body_ratio_min']}"
                f"_w{level['one_minute_wick_ratio_max']}_v{level['one_minute_volume_ratio_min']}", changed)
        variants[side] = entries
    config["candidate_variants"] = variants
    config["strict_profile_sha256"] = sha256(source_path)
    return config


@contextmanager
def setup_context(config, choices=None):
    """Only mutate the archived offline contract, restoring it even on failure."""
    names = ("CORE_SETUPS", "ACTIVE_SETUPS", "_CORE_BY_ID", "_ACTIVE_BY_ID",
             "MIN_CONFIRMATION_VOLUME_BY_SETUP", "SIGNAL_TO_CONFIRMATION",
             "STRATEGY_PROFILE", "STRATEGY_VERSION", "ACCEPTED_VARIANT")
    old = {k: getattr(contract, k) for k in names}
    specs, volumes = [], {}
    try:
        for clock, side in product(config["signal_ends"], config["sides"]):
            key = f"{clock}_{side}"
            level = (dict(price_change_pct=.1, oi_change_pct=.05, five_minute_volume_ratio_min=.8,
                          one_minute_body_ratio_min=0., one_minute_wick_ratio_max=1.,
                          one_minute_volume_ratio_min=0.) if choices is None else
                     config["candidate_variants"][side][int(choices[key])]["thresholds"])
            donor = contract.setup_for("09:50", side)
            spec = replace(donor, signal_end=clock, confirmation_end=confirmation(clock),
                max_entries=10000 if choices is None else 1, picker="max_liquidity",
                price_change_pct=level["price_change_pct"], oi_change_pct=level["oi_change_pct"],
                volume_ratio=level["five_minute_volume_ratio_min"],
                body_ratio=level["one_minute_body_ratio_min"], max_wick_ratio=level["one_minute_wick_ratio_max"],
                stop_pct=config["initial_stop_pct"], target_pct=config["target_pct"], source_version=config["variant_id"])
            specs.append(spec)
            volumes[spec.setup_id] = level["one_minute_volume_ratio_min"]
        contract.CORE_SETUPS = contract.ACTIVE_SETUPS = tuple(specs)
        contract._CORE_BY_ID = {s.setup_id: s for s in specs}
        contract._ACTIVE_BY_ID = dict(contract._CORE_BY_ID)
        contract.MIN_CONFIRMATION_VOLUME_BY_SETUP = volumes
        contract.SIGNAL_TO_CONFIRMATION = {s.signal_end: s.confirmation_end for s in specs}
        contract.STRATEGY_PROFILE = contract.STRATEGY_VERSION = contract.ACCEPTED_VARIANT = config["variant_id"]
        yield
    finally:
        for k, v in old.items():
            setattr(contract, k, v)


def screen_source_liquidity(candidates):
    """Reject invalid liquidity inputs before observing fills or returns."""
    liquidity = pd.to_numeric(candidates.traded_value, errors="coerce")
    valid = np.isfinite(liquidity) & liquidity.ge(0)
    rejected = candidates.loc[~valid].copy()
    rejected["source_rejection_reason"] = "NONFINITE_OR_NEGATIVE_TRADED_VALUE"
    return candidates.loc[valid].copy(), rejected


def label_candidates(candidates, paths, config):
    with setup_context(config):
        audit = contract.selection_audit(candidates)
        orders = audit.loc[audit.v9_selected.eq(True)].copy()
    if set(orders.sid.astype(int)) != set(candidates.sid.astype(int)):
        raise ValueError("Permissive contract dropped source candidates")
    simulator.g.v9.validate_paths(orders, paths)
    raw = simulator.simulate_staged(orders, paths, cost_bps=config["round_trip_cost_bps"],
                                   max_entry_delay_minutes=config["entry_expiry_minutes"])
    raw["day"] = pd.to_datetime(raw.day).dt.date.astype(str)
    return raw


def select(frame, side, level):
    price = frame.price_change_pct * (1 if side == "LONG" else -1)
    mask = (price.ge(level["price_change_pct"]) & frame.oi_change_pct.ge(level["oi_change_pct"])
        & frame.volume_ratio.ge(level["five_minute_volume_ratio_min"])
        & frame.body_ratio.ge(level["one_minute_body_ratio_min"])
        & frame.wick_ratio.le(level["one_minute_wick_ratio_max"])
        & frame.v9_1m_volume_ratio.ge(level["one_minute_volume_ratio_min"]))
    return (frame.loc[mask].sort_values(["day", "traded_value", "tradingsymbol"],
            ascending=[True, False, True], kind="stable").drop_duplicates("day"))


def score(raw, days, config):
    selected = raw.loc[raw.day.isin(days)]
    filled = selected.loc[selected.filled.eq(True)]
    pnl = filled.net_return_pct.astype(float) * config["capital_per_entry_rs"] * config["leverage"] / 100
    daily = pd.Series(pnl.to_numpy(), index=filled.day).groupby(level=0).sum().reindex(days, fill_value=0.)
    curve = np.r_[0., daily.cumsum().to_numpy()]
    return dict(selections=len(selected), fills=len(filled), wins=int(pnl.gt(0).sum()),
        losses=int(pnl.lt(0).sum()), win_rate_pct=100 * float(pnl.gt(0).mean()) if len(pnl) else None,
        net_pnl_rs=float(pnl.sum()), max_drawdown_rs=float((np.maximum.accumulate(curve) - curve).max()))


def passes(row, period, minimum, config):
    return (row[f"{period}_fills"] >= minimum and row[f"{period}_win_rate_pct"] is not None
        and row[f"{period}_win_rate_pct"] >= config["required_win_rate_pct"]
        and row[f"{period}_net_pnl_rs"] > 0)


def choose(rows, config, *, period="train", stages=STAGES):
    minimum = config["minimum_training_fills"] if period == "train" else config["minimum_historical_fills"]
    for stage in stages:
        eligible = [r for r in rows if r["stage"] == stage and passes(r, period, minimum, config)]
        if eligible:
            return max(eligible, key=lambda r: (round(r[f"{period}_net_pnl_rs"], 6),
                -round(r[f"{period}_max_drawdown_rs"], 6), r[f"{period}_fills"], -r["index"])), True
    # Retain a reviewable diagnostic row when the requirement cannot be met.
    eligible = [r for r in rows if r["stage"] in stages and r[f"{period}_fills"] >= minimum]
    if eligible:
        return max(eligible, key=lambda r: (r[f"{period}_win_rate_pct"],
                    r[f"{period}_net_pnl_rs"], -r["index"])), False
    return next(r for r in rows if r["stage"] in stages), False


def sweep(raw, days, config):
    dates = [str(d) for d in days]
    if len(dates) != config["training_sessions"] + config["holdout_sessions"] or dates != sorted(set(dates)):
        raise ValueError("Incorrect chronological cohort")
    periods = dict(train=dates[:config["training_sessions"]], holdout=dates[config["training_sessions"]:], all=dates)
    trials, decisions, stage_decisions, historical = [], [], [], []
    for clock, side in product(config["signal_ends"], config["sides"]):
        pool = raw.loc[raw.hhmm_int.eq(int(clock.replace(":", ""))) & raw.side.eq(side)]
        rows = []
        for option in config["candidate_variants"][side]:
            selected = select(pool, side, option["thresholds"])
            row = dict(setup=f"{clock}_{side}", setup_id=f"{confirmation(clock).replace(':', '')}_{side}",
                signal_end=clock, confirmation_end=confirmation(clock), side=side,
                stage=option["stage"], index=option["index"], variant=option["name"],
                thresholds=json.dumps(option["thresholds"], sort_keys=True))
            for period, cohort in periods.items():
                row.update({f"{period}_{k}": v for k, v in score(selected, cohort, config).items()})
            rows.append(row)
        trials.extend(rows)
        choice, passed = choose(rows, config)
        supported = passed and passes(choice, "holdout", config["minimum_holdout_fills"], config) and passes(choice, "all", 8, config)
        decisions.append(dict(choice, training_pass=passed, chronological_check_pass=supported,
            selection_reason="FIRST_PASSING_STAGE_MAX_TRAIN_NET_PNL" if passed else "NO_TRAINING_VARIANT_PASSED"))
        for stage in STAGES:
            stage_choice, stage_pass = choose(rows, config, stages=(stage,))
            stage_decisions.append(dict(stage_choice, tested_stage=stage, training_pass=stage_pass))
        fit, fit_pass = choose(rows, config, period="all")
        historical.append(dict(fit, historical_pass=fit_pass,
                               evidence="RETROSPECTIVE_SELECTION_USING_ALL_OUTCOMES"))
        print(f"G4 sweep {clock} {side}: {len(rows)} trials; {choice['stage']}; "
              f"train {choice['train_wins']}/{choice['train_fills']}; "
              f"later {choice['holdout_wins']}/{choice['holdout_fills']}; pass={supported}", flush=True)
    return tuple(pd.DataFrame(x) for x in (trials, decisions, stage_decisions, historical))


def selected_raw(raw, choices, config):
    parts = []
    for row in choices.itertuples(index=False):
        pool = raw.loc[raw.hhmm_int.eq(int(row.signal_end.replace(":", ""))) & raw.side.eq(row.side)]
        option = config["candidate_variants"][row.side][int(row.index)]
        part = select(pool, row.side, option["thresholds"]).copy()
        part["max_entries"] = 1
        part["v9_rank_in_setup_day"] = 1
        part["research_stage"] = option["stage"]
        part["research_variant_index"] = int(row.index)
        part["research_thresholds"] = json.dumps(option["thresholds"], sort_keys=True)
        parts.append(part)
    return pd.concat(parts, ignore_index=True) if parts else raw.iloc[:0].copy()


def portfolio_changed(baseline, combined):
    a = baseline.set_index("sid").sort_index()
    b = combined.loc[combined.sid.isin(a.index)].set_index("sid").sort_index()
    same_raw(a.reset_index(), b.reset_index())
    return int((a.portfolio_executed.ne(b.portfolio_executed)
                | ~np.isclose(a.portfolio_net_profit_rupees, b.portfolio_net_profit_rupees, atol=1e-6, rtol=0)).sum())


def write_report(output, summary, decisions, historical, scenarios):
    cols = ["setup", "stage", "train_fills", "train_win_rate_pct", "holdout_fills",
            "holdout_win_rate_pct", "all_fills", "all_win_rate_pct", "all_net_pnl_rs",
            "training_pass", "chronological_check_pass", "portfolio_chronological_check_pass"]
    clocks = summary["signal_ends"]
    label = f"{clocks[0]}-{clocks[-1]}"
    page = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>G4 SLOT_LABEL backtest</title><style>body{font:15px/1.55 system-ui,sans-serif;margin:32px;background:#f4f6fa;color:#182536}h1,h2{line-height:1.2}.note{background:#fff0ca;padding:18px;border-left:4px solid #bd8500}.scroll{overflow:auto;margin-bottom:30px}table{border-collapse:collapse;background:white;width:100%}td,th{padding:9px;border-bottom:1px solid #ddd;text-align:right;white-space:nowrap}th{background:#23374d;color:white}td:first-child,th:first-child{text-align:left}a{color:#075ead}code{font-size:13px}</style><body>
<h1>G4 SLOT_LABEL: SETUP_COUNT new long / short setups</h1>'''
    page = page.replace("SLOT_LABEL", label).replace("SETUP_COUNT", str(summary["new_setups"]))
    page += f"<p>{summary['first_date']} to {summary['last_date']}; {summary['sessions']} sessions. Target 1.00%; stop 1.25%, tightening to 1.00% after 120 minutes. Exact next-minute confirmation; 10-minute entry expiry; 15:15 square-off.</p>"
    page += f"<p>Source: {summary['source_candidates']} candidates; {summary['eligible_source_candidates']} eligible after the liquidity-input quality check. {summary['rejected_source_candidates']} rejected inputs are recorded in source_rejections.csv. Selection never uses rejected inputs.</p>"
    page += '<div class="note">Thresholds are chosen on the first 38 sessions, in strict → relaxed 1m → relaxed 5m order. Later 11 sessions are a chronological check. These dates have already been used in earlier research, so this is not an untouched out-of-sample test. A 70% observed win rate is not a future guarantee. October 1 is excluded for incomplete immutable input.</div>'
    page += '<p>Highest liquidity is selected before knowing whether it fills. No replacement after a missed trigger. Relaxation retains directional EMA order, positive increasing OI of 0.05–1.00%, absolute price move ≥0.10%, 5m volume ratio ≥0.80, and a directional confirmation closing beyond signal close. Capital: Rs 100,000 per entry × 5 exposure; Rs 1,000,000 portfolio capital; 5 bps modeled round-trip cost. A win means positive net P&amp;L, including profitable time exits; it does not necessarily mean the 1% target was hit. Drawdown is calculated from daily realized P&amp;L.</p>'
    results = pd.read_csv(output / "portfolio_results.csv")
    stage_names = ["baseline_g4", "strict_training_eligible", "relax_1m_training_eligible", "relax_5m_training_eligible"]
    comparison = results.loc[results.scenario.isin(stage_names), ["scenario", "period", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]]
    supported = decisions.loc[decisions.portfolio_chronological_check_pass, cols]
    rules = decisions.loc[decisions.portfolio_chronological_check_pass, ["setup", "thresholds"]]
    tables = [("Strict → relaxed 1m → relaxed 5m: training-selected stage portfolios", comparison),
        ("Setups passing the training and chronological screens", supported),
        ("Filters for the passing setups", rules),
        ("Portfolio results after modeled cost: all 49 sessions", scenarios),
        ("Each stage: new additions only, without the baseline", results.loc[results.scenario.isin([stage + "_additions_only" for stage in STAGES]), ["scenario", "period", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs"]]),
        ("Training-selected configurations: every requested setup", decisions[cols]),
        ("Retrospective alternatives (selected using all 49 sessions)", historical[["setup", "stage", "all_fills", "all_wins", "all_win_rate_pct", "all_net_pnl_rs", "historical_pass"]])]
    for title, table in tables:
        page += f"<h2>{html.escape(title)}</h2><div class=scroll>{table.to_html(index=False, border=0, float_format=lambda v: f'{v:,.2f}', na_rep='—')}</div>"
    page += '<p>The chronological-check subset and retrospective subset use later outcomes to determine membership and are descriptive portfolios. The training-eligible portfolio uses thresholds and membership chosen without later outcomes.</p>'
    page += f"<p>Preserved baseline: {html.escape(summary['baseline_description'])} ({summary['baseline_setups']} setups). Its own setup and target selection used this research history; combined portfolio checks are descriptive. Baseline execution changes from capital competition are recorded in portfolio_impacts.csv.</p>"
    page += '<h2>Files</h2><p>' + ' · '.join(f'<a href="{name}">{name}</a>' for name in (
        "daily_results.csv", "monthly_results.csv", "setup_results.csv", "stage_decisions.csv", "strictness_sweep.csv",
        "selected_config.json", "scenario_configs.json", "selected_orders.csv", "portfolio_impacts.csv",
        "candidate_outcomes.csv", "source_rejections.csv", "summary.json", "manifest.json")) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(config_path, output_dir, *, cache=None):
    config = configuration(config_path)
    profile = PROFILES[config["variant_id"]]
    version = config["variant_id"]
    setup_count = len(config["signal_ends"]) * len(config["sides"])
    diagnostic_name = f"all_{setup_count}_diagnostic"
    output = Path(output_dir).resolve()
    if not output.is_relative_to(BACKTEST_ROOT.resolve()) or (output.exists() and any(output.iterdir())):
        raise ValueError("Output must be a new directory within the G4 backtest root")
    candidates, paths, days, evidence, _ = load_late_source(cache or profile["cache"],
        clocks=profile["clocks"], sid_base=profile["sid_base"], verify_snapshots=True)
    prior = (BACKTEST_ROOT / config["base_g4_run_relative"]).resolve()
    if not prior.is_relative_to(BACKTEST_ROOT.resolve()) or sha256(prior / "manifest.json") != config["base_g4_manifest_sha256"]:
        raise ValueError("Pinned G4 baseline changed")
    baseline_proof = verified_run(prior, profile["baseline_version"])
    previous_summary = json.loads((prior / "summary.json").read_text(encoding="utf-8"))
    if (previous_summary["sessions"] != len(days) or previous_summary["first_date"] != str(days[0])
            or previous_summary["last_date"] != str(days[-1])):
        raise ValueError("Pinned G4 baseline and new source cohorts differ")
    saved = pd.read_csv(prior / profile["baseline_ledger"], float_precision="round_trip")
    baseline = checked_allocate(saved, saved)
    if set(candidates.sid) & set(baseline.sid):
        raise ValueError("New candidate IDs overlap preserved G4 orders")
    print(f"G4 baseline verified: {len(baseline)} orders; {len(candidates)} new candidates", flush=True)
    source_count = len(candidates)
    candidates, source_rejections = screen_source_liquidity(candidates)
    print(f"G4 input quality: {len(candidates)} eligible; {len(source_rejections)} invalid liquidity inputs excluded", flush=True)
    raw = label_candidates(candidates, paths, config)
    trials, decisions, stage_choices, historical = sweep(raw, days, config)
    selection = dict(zip(decisions.setup, decisions["index"].astype(int)))
    with setup_context(config, selection):
        audit = contract.selection_audit(candidates)
        orders = audit.loc[audit.v9_selected.eq(True)].copy()
    selected = selected_raw(raw, decisions, config)
    if set(orders.sid) != set(selected.sid):
        raise ValueError("Optimized selection differs from full contract audit")
    actual = simulator.simulate_staged(orders, {int(s): paths[int(s)] for s in orders.sid},
        cost_bps=config["round_trip_cost_bps"], max_entry_delay_minutes=config["entry_expiry_minutes"])
    same_raw(actual, selected)
    if (not orders.initial_stop_pct.eq(1.25).all()
            or not actual.native_target_pct.eq(1.).all()
            or not actual.loc[actual.filled.eq(True), "initial_stop_pct"].eq(1.25).all()):
        raise ValueError("New exit policy drift")
    output.mkdir(parents=True)
    scenario_rows, daily, monthly, impacts, ledgers = [], [], [], [], {}
    period_days = dict(train=days[:38], holdout=days[38:], all=days)

    def add_scenario(name, addition, *, with_baseline=True):
        combined_raw = pd.concat((baseline, addition), ignore_index=True) if with_baseline else addition.reset_index(drop=True)
        ledger = allocate(combined_raw)
        ledger["research_scenario"] = name
        ledgers[name] = ledger
        changed = portfolio_changed(baseline, ledger) if with_baseline else 0
        if with_baseline and changed:
            a = baseline.set_index("sid").sort_index()
            b = ledger.loc[ledger.sid.isin(a.index)].set_index("sid").sort_index()
            changed_ids = a.index[a.portfolio_executed.ne(b.portfolio_executed)
                | ~np.isclose(a.portfolio_net_profit_rupees, b.portfolio_net_profit_rupees, atol=1e-6, rtol=0)]
            for sid in changed_ids:
                impacts.append(dict(scenario=name, sid=int(sid), day=str(a.loc[sid, "day"]),
                    setup_id=a.loc[sid, "setup_id"], symbol=a.loc[sid, "tradingsymbol"],
                    before_executed=bool(a.loc[sid, "portfolio_executed"]), after_executed=bool(b.loc[sid, "portfolio_executed"]),
                    before_net_pnl_rs=float(a.loc[sid, "portfolio_net_profit_rupees"]),
                    after_net_pnl_rs=float(b.loc[sid, "portfolio_net_profit_rupees"])))
        for period, cohort in period_days.items():
            part = ledger.loc[ledger.day.isin([str(d) for d in cohort])]
            scenario_rows.append(dict(scenario=name, period=period, **_stats(part, cohort), baseline_trades_changed=changed))
        for day in days:
            daily.append(dict(scenario=name, day=str(day), **_stats(ledger.loc[ledger.day.eq(str(day))], [day])))
        for month in sorted({str(day)[:7] for day in days}):
            cohort = [day for day in days if str(day).startswith(month)]
            monthly.append(dict(scenario=name, month=month,
                **_stats(ledger.loc[ledger.day.str.startswith(month)], cohort)))
        ledger.to_csv(output / f"{name}_trades.csv", index=False)
        return ledger

    add_scenario("baseline_g4", raw.iloc[:0])
    combined = add_scenario(diagnostic_name, actual)
    added = combined.loc[combined.sid.isin(actual.sid)].copy()
    for i, row in decisions.iterrows():
        part = added.loc[added.setup_id.eq(row.setup_id)]
        for period, cohort in period_days.items():
            stats = _stats(part.loc[part.day.isin([str(d) for d in cohort])], cohort)
            for field in ("executed_trades", "wins", "win_rate_pct", "net_pnl_rs"):
                decisions.loc[i, f"portfolio_{period}_{field}"] = stats[field]
    decisions["portfolio_chronological_check_pass"] = (
        decisions.chronological_check_pass & decisions.portfolio_train_executed_trades.ge(5)
        & decisions.portfolio_holdout_executed_trades.ge(3)
        & decisions.portfolio_train_win_rate_pct.ge(70) & decisions.portfolio_holdout_win_rate_pct.ge(70)
        & decisions.portfolio_all_win_rate_pct.ge(70) & decisions.portfolio_train_net_pnl_rs.gt(0)
        & decisions.portfolio_holdout_net_pnl_rs.gt(0) & decisions.portfolio_all_net_pnl_rs.gt(0))
    train_raw = selected_raw(raw, decisions.loc[decisions.training_pass], config)
    supported_raw = selected_raw(raw, decisions.loc[decisions.portfolio_chronological_check_pass], config)
    fit_raw = selected_raw(raw, historical.loc[historical.historical_pass], config)
    for label, part in (("training_eligible", train_raw), ("chronological_check_subset", supported_raw), ("historical_fit_subset", fit_raw)):
        add_scenario(label, part)
        add_scenario(label + "_additions_only", part, with_baseline=False)
    for stage in STAGES:
        part = selected_raw(raw, stage_choices.loc[stage_choices.tested_stage.eq(stage) & stage_choices.training_pass], config)
        add_scenario(stage + "_training_eligible", part)
        add_scenario(stage + "_additions_only", part, with_baseline=False)
    scenarios = pd.DataFrame(scenario_rows)
    artifacts = dict(candidate_outcomes=raw, source_rejections=source_rejections,
        strictness_sweep=trials, setup_results=decisions,
        stage_decisions=stage_choices, historical_fit_results=historical,
        selected_orders=orders, selection_audit=audit, portfolio_results=scenarios,
        daily_results=pd.DataFrame(daily), monthly_results=pd.DataFrame(monthly),
        portfolio_impacts=pd.DataFrame(impacts, columns=["scenario", "sid", "day", "setup_id", "symbol",
            "before_executed", "after_executed", "before_net_pnl_rs", "after_net_pnl_rs"]))
    for name, frame in artifacts.items():
        frame.to_csv(output / f"{name}.csv", index=False)
    config_rows = []
    for row in decisions.itertuples(index=False):
        config_rows.append(dict(setup_id=row.setup_id, signal_end=row.signal_end,
            confirmation_end=row.confirmation_end, side=row.side, stage=row.stage,
            thresholds=json.loads(row.thresholds), target_pct=1., initial_stop_pct=1.25,
            tightened_stop_pct=1., tighten_after_minutes=120, max_entries=1, picker="max_liquidity",
            training_pass=bool(row.training_pass), chronological_check_pass=bool(row.portfolio_chronological_check_pass)))
    selected_config = dict(config, setups=config_rows, config_sha256=sha256(config_path))
    (output / "selected_config.json").write_text(json.dumps(selected_config, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    scenario_configs = {diagnostic_name: decisions,
        "training_eligible": decisions.loc[decisions.training_pass],
        "chronological_check_subset": decisions.loc[decisions.portfolio_chronological_check_pass],
        "historical_fit_subset": historical.loc[historical.historical_pass]}
    scenario_configs.update({stage + "_training_eligible": stage_choices.loc[
        stage_choices.tested_stage.eq(stage) & stage_choices.training_pass] for stage in STAGES})
    portable = {}
    for name, frame in scenario_configs.items():
        portable[name] = [dict(setup_id=row.setup_id, signal_end=row.signal_end,
            confirmation_end=row.confirmation_end, side=row.side, stage=row.stage,
            thresholds=json.loads(row.thresholds), target_pct=1., initial_stop_pct=1.25,
            tightened_stop_pct=1., tighten_after_minutes=120, max_entries=1, picker="max_liquidity")
            for row in frame.itertuples(index=False)]
    (output / "scenario_configs.json").write_text(json.dumps(dict(variant_id=version,
        scope="BACKTEST_ONLY", execution_authority=False, scenarios=portable), indent=2) + "\n", encoding="utf-8")
    summary = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), first_date=str(days[0]), last_date=str(days[-1]),
        sessions=len(days), training_sessions=38, holdout_sessions=11, new_setups=setup_count,
        source_candidates=source_count, eligible_source_candidates=len(candidates),
        rejected_source_candidates=len(source_rejections),
        selected_orders=len(orders), potential_fills=int(actual.filled.sum()),
        signal_ends=config["signal_ends"], baseline_setups=profile["baseline_setups"],
        baseline_description=profile["baseline_description"],
        excluded_session="2026-10-01: incomplete immutable input", source_evidence=evidence,
        baseline=baseline_proof, config_path=str(Path(config_path).resolve()), config_sha256=sha256(config_path),
        tested_configurations=len(trials), candidate_variants_per_side={s: len(config["candidate_variants"][s]) for s in config["sides"]},
        training_eligible_setups=decisions.loc[decisions.training_pass, "setup"].tolist(),
        chronological_check_pass_setups=decisions.loc[decisions.portfolio_chronological_check_pass, "setup"].tolist(),
        historical_fit_pass_setups=historical.loc[historical.historical_pass, "setup"].tolist(),
        evidence_status=config["evidence_status"], normal_minute_replay_verified=True,
        baseline_orders_preserved=True, baseline_raw_outcomes_unchanged=True,
        qualification_rule=f"Training >=5 fills, later >=3, both >=70% net wins and positive net P&L; all >=70%; standalone and all-{setup_count} portfolio",
        limitations=["Previously researched dates; chronological check is not untouched out of sample",
                    "Fixed 5 bps cost model; no broker-specific fees, market impact or extra slippage",
                    "Small per-setup samples and multiple configuration testing",
                    "Relaxations cannot go below native source gates; prices, OI and EMA direction retained"],
        scenarios=json.loads(scenarios.to_json(orient="records")))
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    view = scenarios.loc[scenarios.period.eq("all"), ["scenario", "selected_orders", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "max_drawdown_rs", "baseline_trades_changed"]]
    write_report(output, summary, decisions, historical, view)
    code_paths = [Path(__file__), ROOT / "fno_v13_v10_g_4_backtest.py", ROOT / "research/g4_late_slot_source.py",
                  Path(contract.__file__), Path(simulator.__file__), ROOT / "research/g3_0920_backtest.py",
                  ROOT / "research/g3_g4_late_combined_backtest.py", ROOT / "fno_v13_replay_source.py"]
    manifest = dict(variant_id=version, scope="BACKTEST_ONLY", execution_authority=False,
        code_hashes={str(p.relative_to(ROOT)): sha256(p) for p in code_paths},
        files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(view.to_string(index=False), flush=True)
    return summary
