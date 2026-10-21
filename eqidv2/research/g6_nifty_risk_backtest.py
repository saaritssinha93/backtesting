"""G6 research: reproduce Nifty filters, then test selective entry risk rules.

Uses the pinned, verified 54-session G4 benchmark to avoid rewriting six
historical selections changed in newer source reconstruction. Native entry and
exit paths are unchanged; every variant reruns chronological portfolio sizing.
Run: python -m research.g6_nifty_risk_backtest
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g4_nifty_direction_backtest as source
from research import g4_nifty_direction_verify as source_verify
from research import g6_g4_replica_backtest as replica

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs/research/g6_nifty_risk_filters_v1.json"
ENGINE = source.sealed.bounded.history.simulator.g.v9
MONEY_COLUMNS = (
    "capital_per_entry_rupees", "exposure_per_entry_rupees",
    "unleveraged_pre_cost_profit_rupees", "unleveraged_cost_rupees",
    "unleveraged_net_profit_rupees", "pre_cost_profit_rupees", "cost_rupees", "net_profit_rupees",
)
DYNAMIC = {"daily_1r_half", "daily_2r_halt", "combined_persistent_daily_half"}


def features_at(frame, side, stamp, config):
    if side not in ("LONG", "SHORT"):
        raise ValueError("Invalid side")
    values = source.direction_at(frame, stamp)
    if not np.isfinite([values["slot_candle"], values["session_open"]]).all():
        raise ValueError("Incomplete primary Nifty context")
    expected = pd.Timestamp(values["nifty_bar_ts"])
    previous = frame.loc[frame.ts.eq(expected - pd.Timedelta(minutes=5))]
    prev_change = np.nan
    status = "BEFORE_SECOND_CANDLE" if expected.hour == 9 and expected.minute == 20 else "MISSING_PREVIOUS_BAR"
    if len(previous) == 1:
        row = previous.iloc[0]
        valid = str(row.get("quality_state", "VALID")).upper() == "VALID"
        if valid and np.isfinite([row.open, row.close]).all() and min(row.open, row.close) > 0:
            prev_change = (float(row.close) / float(row.open) - 1) * 100
            status = "OK"
        else:
            status = "INVALID_PREVIOUS_BAR"
    sign = 1 if side == "LONG" else -1
    slot, session = sign * values["slot_candle"], sign * values["session_open"]
    persistent = (status == "OK" and session <= -config["persistent_session_move_pct"]
        and slot <= -config["persistent_each_candle_move_pct"]
        and sign * prev_change <= -config["persistent_each_candle_move_pct"])
    return dict(nifty_bar_ts=str(expected), previous_bar_status=status,
        slot_alignment_pct=slot, session_alignment_pct=session,
        previous_candle_alignment_pct=sign * prev_change,
        slot_counter=slot < 0, session_counter=session < 0,
        both_counter=slot < 0 and session < 0, persistent_counter=bool(persistent))


def build_features(original, directory, config):
    records = []
    for day, group in original.groupby("day", sort=True):
        frame = pd.read_parquet(directory / "nifty_inputs" / (day + ".parquet"))
        for row in group.itertuples():
            records.append(dict(sid=int(row.sid), day=day, setup_id=row.setup_id,
                tradingsymbol=row.tradingsymbol, side=row.side, confirmation_ts=str(row.confirmation_ts),
                **features_at(frame, row.side, row.confirmation_ts, config)))
    return pd.DataFrame(records).set_index("sid", drop=False)


def static_factors(features, variant):
    factor = pd.Series(1., index=features.index)
    if variant == "uniform_half":
        factor[:] = .5
    flags = {
        "slot_candle": ("slot_counter", 0.), "session_open": ("session_counter", 0.),
        "both_counter_skip": ("both_counter", 0.), "session_counter_half": ("session_counter", .5),
        "persistent_counter_skip": ("persistent_counter", 0.),
        "persistent_counter_half": ("persistent_counter", .5),
        "combined_persistent_daily_half": ("persistent_counter", .5),
    }
    if variant in flags:
        column, size = flags[variant]
        factor.loc[features[column]] = size
    elif variant not in {"control", "uniform_half", "daily_1r_half", "daily_2r_halt"}:
        raise ValueError("Unknown variant")
    return factor


def allocate(native, factors, config, cost_bps, variant):
    work = native.copy()
    factor = work.sid.map(factors).to_numpy(float)
    if not np.isfinite(factor).all() or not np.isin(factor, [0., .5, 1.]).all():
        raise ValueError("Invalid sizing factor")
    work["g6_native_filled"] = work.filled.eq(True)
    work["g6_multiplier"] = factor
    work["filled"] = work.filled.eq(True) & (factor > 0)
    # Preserve native return evidence on skipped fills; the capital model zeros
    # their monetary outcomes. This also makes the original filter parity exact.
    work["cost_pct"] = np.where(work.g6_native_filled, cost_bps / 100, np.nan)
    work["net_return_pct"] = work.gross_return_pct - work.cost_pct
    sized = ENGINE.v5.apply_fixed_capital_model(work, config["capital_per_entry_rs"], config["leverage"])
    for field in MONEY_COLUMNS:
        sized[field] = sized[field].to_numpy(float) * factor
    settings = ENGINE.V9Config(portfolio_capital_rupees=config["portfolio_capital_rs"],
        capital_per_entry_rupees=config["capital_per_entry_rs"], leverage_factor=config["leverage"],
        max_positions=10, cost_bps=cost_bps)
    book, allocation = ENGINE.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    skipped = book.g6_multiplier.eq(0)
    book.loc[skipped, "portfolio_status"] = "G6_RISK_FILTER_SKIPPED"
    book.loc[skipped, "portfolio_reject_reason"] = variant
    book["strategy"] = "V13-V10-G-6"
    book["strategy_version"] = "G6-NIFTY-RISK-FILTERS-V1-" + variant
    book["execution_authority"] = False
    book["scope"] = "BACKTEST_ONLY"
    return book, allocation


def realised_state(book, stamp):
    """Do not use eventual losses or exits on the decision's own bar."""
    if book is None or book.empty:
        return 0., 0., 0
    stamp = pd.Timestamp(stamp)
    settled = book.loc[book.portfolio_executed.eq(True) & book.exit_ts.lt(stamp)
        & book.day.eq(stamp.strftime("%Y-%m-%d"))]
    events = settled.groupby("exit_ts").portfolio_net_profit_rupees.sum().sort_index()
    curve = events.cumsum()
    return float(events.sum()), min(0., float(curve.min())) if len(curve) else 0., len(settled)


def replay_variant(original, features, variant, base_config, config, cost_bps):
    factors = static_factors(features, variant)
    audits = []
    for day, day_orders in original.groupby("day", sort=True):
        past_book = None
        for stamp, group in day_orders.groupby("confirmation_ts", sort=True):
            net, floor, closed = realised_state(past_book, stamp)
            threshold = config["risk_unit_rs"] * (2 if variant == "daily_2r_halt" else 1)
            triggered = variant in DYNAMIC and floor <= -threshold
            governor = (0. if variant == "daily_2r_halt" else .5) if triggered else 1.
            for sid in group.sid:
                initial = float(factors.loc[sid])
                factors.loc[sid] = min(initial, governor)
                audits.append(dict(sid=int(sid), day=day, confirmation_ts=str(stamp), variant=variant,
                    cost_bps=cost_bps, static_multiplier=initial, multiplier=float(factors.loc[sid]),
                    realised_before_rs=net, realised_floor_before_rs=floor, closed_trades_before=closed,
                    governor_triggered=triggered))
            if variant in DYNAMIC:
                prefix = day_orders.loc[day_orders.confirmation_ts.le(stamp)]
                past_book, _ = allocate(prefix, factors, base_config, cost_bps, variant)
    book, allocation = allocate(original, factors, base_config, cost_bps, variant)
    audit = pd.DataFrame(audits)
    # A later order must not retroactively alter the state used by the governor.
    if variant in DYNAMIC:
        for row in audit.itertuples():
            net, floor, closed = realised_state(book, row.confirmation_ts)
            np.testing.assert_allclose([net, floor], [row.realised_before_rs, row.realised_floor_before_rs], atol=1e-6, rtol=0)
            if closed != row.closed_trades_before:
                raise ValueError("Later orders changed earlier portfolio state")
    return book, allocation, audit


def daily_values(book, days):
    return book.groupby("day").portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.)


def metrics(book, control, days):
    result = source.bounded.stats(book, days)
    day, base_day = daily_values(book, days), daily_values(control, days)
    candidate = book.loc[book.day.isin(days)].set_index("sid")
    baseline = control.loc[control.day.isin(days)].set_index("sid").reindex(candidate.index)
    pnl, old = candidate.portfolio_net_profit_rupees, baseline.portfolio_net_profit_rupees
    winners, losers = old.gt(1e-8), old.lt(-1e-8)
    exposure = candidate.loc[candidate.portfolio_executed, "exposure_per_entry_rupees"].sum()
    result.update(flat_days=int(day.abs().le(1e-8).sum()), worst_day_rs=float(day.min()),
        loss_day_sum_abs_rs=float(-day.clip(upper=0).sum()), net_change_rs=float((pnl-old).sum()),
        net_retention_pct=float(pnl.sum()/old.sum()*100) if old.sum() > 0 else None,
        original_winner_profit_retention_pct=float(pnl[winners].sum()/old[winners].sum()*100) if winners.any() else None,
        original_loser_loss_reduced_rs=float((pnl[losers]-old[losers]).sum()),
        original_winner_profit_sacrificed_rs=float((old[winners]-pnl[winners]).sum()),
        losing_days_rescued=int((base_day.lt(-1e-8) & day.ge(-1e-8)).sum()),
        profitable_days_turned_loss=int((base_day.gt(1e-8) & day.lt(-1e-8)).sum()),
        losing_days_improved=int((base_day.lt(-1e-8) & day.gt(base_day+1e-8)).sum()),
        baseline_winners_skipped=int((winners & ~candidate.portfolio_executed).sum()),
        baseline_losers_skipped=int((losers & ~candidate.portfolio_executed).sum()),
        winners_reduced=int((winners & candidate.portfolio_executed & pnl.lt(old-1e-8)).sum()),
        losers_reduced=int((losers & candidate.portfolio_executed & pnl.gt(old+1e-8)).sum()),
        trade_exposure_sum_rs=float(exposure),
        net_per_million_trade_exposure_rs=float(pnl.sum()/exposure*1e6) if exposure else 0.)
    return result


def historical_screen(candidate, control, rules):
    return bool(candidate["net_pnl_rs"] >= control["net_pnl_rs"]*rules["minimum_net_retention"]
        and candidate["original_winner_profit_retention_pct"] >= rules["minimum_original_winner_profit_retention"]*100
        and candidate["loss_day_sum_abs_rs"] <= control["loss_day_sum_abs_rs"]*(1-rules["minimum_loss_day_amount_reduction"])
        and candidate["losing_days"] <= control["losing_days"]-rules["minimum_losing_day_count_reduction"]
        and candidate["daily_close_drawdown_rs"] <= control["daily_close_drawdown_rs"])


def bootstrap_delta(delta, settings):
    values = np.asarray(delta, float)
    rng = np.random.default_rng(settings["seed"])
    block = settings["block_sessions"]
    starts = rng.integers(0, len(values), size=(settings["repetitions"], int(np.ceil(len(values)/block))))
    indices = ((starts[:, :, None] + np.arange(block)) % len(values)).reshape(len(starts), -1)[:, :len(values)]
    totals = values[indices].sum(axis=1)
    lower, upper = np.quantile(totals, [.025, .975])
    return dict(delta_lower_95_rs=float(lower), delta_upper_95_rs=float(upper),
        bootstrap_fraction_positive=float(np.mean(totals > 0)))


def report(summary, comparison, daily, transitions):
    def table(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0, float_format=lambda x:f'{x:,.2f}') + '</div>'
    columns = ["variant", "trades", "losses", "losing_days", "net_pnl_rs", "net_change_rs", "loss_day_sum_abs_rs",
        "daily_close_drawdown_rs", "original_winner_profit_retention_pct", "historical_screen_pass"]
    primary = comparison.loc[comparison.cost_bps.eq(5)]
    return ('<!doctype html><html><head><meta charset="utf-8"><title>G6 Nifty risk filter research</title>'
        '<style>body{font:15px/1.6 system-ui;background:#101827;color:#e6edf6;max-width:1600px;margin:auto;padding:25px}'
        'a{color:#8dd2ff}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #405069;white-space:nowrap;text-align:right}.scroll{overflow:auto}</style></head><body>'
        '<h1>G6: reduce losses while preserving winning trades</h1>'
        f'<p>{summary["days"][0]} to {summary["days"][-1]}: 54 sessions, all 51 setups, 550 selected orders. Backtest only.</p>'
        '<p>Exact frozen G4 control, slot and session parity verified. Same native entries, stops and targets; full portfolio allocation recomputed for every rule. Nifty means dated futures, not spot.</p>'
        '<h2>Full history</h2>' + table(primary.loc[primary.period.eq("all"), columns]) +
        '<h2>First 30 and later 24 sessions</h2>' + table(primary.loc[primary.period.isin(["first_30", "later_24"]), ["period"]+columns]) +
        '<h2>15 / 16 October</h2>' + table(primary.loc[primary.period.isin(["october_15", "october_16"]), ["period"]+columns]) +
        '<h2>Which losing days changed?</h2>' + table(transitions.loc[transitions.cost_bps.eq(5) &
            (transitions.baseline_pnl_rs.lt(0) | transitions.variant_pnl_rs.lt(0))]) +
        '<h2>Daily results</h2>' + table(daily.loc[daily.cost_bps.eq(5)].pivot(index="day", columns="variant", values="net_pnl_rs").reset_index()) +
        '<h2>Interpretation and next candidates</h2><ul>' + ''.join('<li>'+html.escape(x)+'</li>' for x in summary["limitations"]) +
        '</ul><p>Next hypotheses: retain a counter-Nifty stock when its completed-bar relative strength and sector direction support it; exit a still-losing trade only after a confirmed failed breakout and adverse market alignment; cap clusters of correlated entries. These require additional causal feature replay and have NOT been validated by this report.</p>'
        '<p><a href="comparison.csv">All comparisons and cost stress</a> | <a href="daily_comparison.csv">Daily CSV</a> | '
        '<a href="setup_results.csv">All setup results</a> | <a href="decision_audit.csv">Every decision</a> | '
        '<a href="day_transitions.csv">Day transitions</a> | <a href="uncertainty.csv">Block resampling</a> | '
        '<a href="config.json">Fixed rules</a> | <a href="summary.json">Evidence</a></p></body></html>')


def run(output, config_path=DEFAULT_CONFIG):
    config = source.base.read(config_path)
    directory = Path(config["source_run"])
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((replica.G6_ROOT / "research").resolve()):
        raise ValueError("Require a fresh isolated G6 research directory")
    if source.base.sha256(directory/"manifest.json") != config["source_manifest_sha256"]:
        raise ValueError("Pinned source manifest changed")
    verification = source_verify.verify(directory)
    summary_source = source.base.read(directory/"summary.json")
    days = summary_source["days"]
    original = source.base.read_frozen_csv(directory/"baseline_native.csv")
    base_config = source.base.read(directory/"config.json")["base_g4_config"]
    output.mkdir(parents=True)
    source.dump(output/"config.json", config)
    source.dump(output/"source_verification.json", verification)
    features = build_features(original, directory, config)
    features.to_csv(output/"nifty_features.csv", index=False)
    periods = {"all":days, "first_30":days[:30], "later_24":days[30:],
        "block_1":days[:18], "block_2":days[18:36], "block_3":days[36:],
        "october_15":["2026-10-15"], "october_16":["2026-10-16"], "last_day":[days[-1]]}
    comparisons, daily, setups, decisions, transitions, intervals = [], [], [], [], [], []
    for cost in config["cost_bps_round_trip"]:
        control, _ = allocate(original, pd.Series(1., index=features.index), base_config, cost, "control")
        if cost == 5:
            source.sealed.parity(control, original)
        for variant in config["variants"]:
            book, allocation, audit = replay_variant(original, features, variant, base_config, config, cost)
            decisions.append(audit)
            if cost == 5 and variant in ("control", "slot_candle", "session_open"):
                source.sealed.parity(book, source.base.read_frozen_csv(directory/variant/"portfolio_trades.csv"))
            for period, cohort in periods.items():
                stats = metrics(book, control, cohort)
                stats["historical_screen_pass"] = historical_screen(stats, metrics(control, control, cohort), config["historical_screen"])
                comparisons.append(dict(variant=variant, cost_bps=cost, period=period, **stats))
            for day in days:
                stats = metrics(book, control, [day])
                daily.append(dict(variant=variant, cost_bps=cost, day=day, **stats))
            old_day, new_day = daily_values(control, days), daily_values(book, days)
            for day in days:
                transitions.append(dict(variant=variant, cost_bps=cost, day=day,
                    baseline_pnl_rs=float(old_day.loc[day]), variant_pnl_rs=float(new_day.loc[day]),
                    change_rs=float(new_day.loc[day]-old_day.loc[day])))
            for period in ("all", "later_24"):
                delta = (new_day-old_day).reindex(periods[period])
                intervals.append(dict(variant=variant, cost_bps=cost, period=period,
                    **bootstrap_delta(delta, config["bootstrap"])))
            for item in base_config["active_setups"]:
                sid = item["setup_id"]
                setups.append(dict(variant=variant, cost_bps=cost, setup_id=sid,
                    **source.bounded.stats(book.loc[book.setup_id.eq(sid)], days)))
            folder = output/f"cost_{cost:g}bps"/variant
            folder.mkdir(parents=True)
            book.to_csv(folder/"portfolio_trades.csv", index=False)
            source.dump(folder/"allocation.json", allocation)
            print(cost, variant, source.bounded.stats(book, days), flush=True)
    comparison, daily, transitions = pd.DataFrame(comparisons), pd.DataFrame(daily), pd.DataFrame(transitions)
    for name, frame in (("comparison", comparison), ("daily_comparison", daily), ("day_transitions", transitions),
        ("setup_results", pd.DataFrame(setups)), ("decision_audit", pd.concat(decisions)), ("uncertainty", pd.DataFrame(intervals))):
        frame.to_csv(output/(name+".csv"), index=False)
    summary = dict(version=config["version"], scope="BACKTEST_ONLY", execution_authority=False,
        source_run=str(directory), source_manifest_sha256=config["source_manifest_sha256"],
        days=days, sessions=len(days), setups=51, selected_orders=len(original),
        baseline_and_existing_filters_parity=True, primary_candidate=config["primary_candidate"],
        previous_bar_status=features.previous_bar_status.value_counts().to_dict(),
        historical_selection_drift_preserved=summary_source["historical_g3_reconstruction_drift"],
        limitations=[
            "Previously reviewed history ends 19 October 2026. It is not a fresh holdout. No future-data or outcome-based loser veto is permitted.",
            "All rules apply across 51 setups; all variants, including failures, are reported. Fixed thresholds are research hypotheses, not optimised estimates.",
            "A pass means only the predeclared historical screen passed, not proven robustness or permission for live use. Block intervals are exploratory and not multiple-testing corrected.",
            "Daily drawdown uses day-end realised equity, not intraday mark-to-market. Risk governors use only earlier closed trades; existing open losses can exceed the trigger.",
            "Half sizing can reduce losses mechanically. The uniform-half control and exposure efficiency distinguish risk reduction from better selection.",
            "The 10bps stress doubles model costs but does not simulate liquidity impact, exact tick sequencing or order submission delay.",
            "Two-candle rules keep baseline behaviour before two candles exist or when the previous bar is missing/invalid; every such fallback is audited.",
            "The benchmark preserves accepted frozen G4 history plus 19 October, rather than the current reconstruction that drops six older selected orders.",
            "No strategy defaults, latest pointers or live execution settings changed. Rollback consists of using the G6 control or existing replica runner.",
            "Testing many alternatives on reused data increases overfitting risk: https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf",
        ])
    source.dump(output/"summary.json", summary)
    (output/"report.html").write_text(report(summary, comparison, daily, transitions), encoding="utf-8")
    (output/"ROLLBACK.txt").write_text("Research only. No default or latest pointer changed. Use cost_5bps/control/portfolio_trades.csv for exact pinned G4 baseline; python fno_v13_v10_g_6_backtest.py remains the original unfiltered replica runner.\n", encoding="utf-8")
    paths = [Path(__file__), Path(config_path), Path(source.__file__), Path(source_verify.__file__),
        Path(ENGINE.v5.__file__), Path(ENGINE.v6.__file__), Path(ENGINE.__file__), Path(source.bounded.__file__)]
    (output/"code").mkdir()
    for path in paths:
        shutil.copy2(path, output/"code"/path.name)
    source.dump(output/"manifest.json", dict(files={p.relative_to(output).as_posix():source.base.sha256(p)
        for p in output.rglob("*") if p.is_file()}, code_sha256={str(p.resolve()):source.base.sha256(p) for p in paths}))
    print(output, flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or replica.G6_ROOT/"research"/datetime.now(source.base.production.IST).strftime("%Y%m%dT%H%M%S_nifty_risk_v1")
    run(output, args.config)
