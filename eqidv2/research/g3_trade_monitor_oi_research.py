"""Frozen OI exit experiment. Research artifacts only; no strategy promotion."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from html import escape
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd

from research import g3_trade_monitor_parameter_search as search
from research import g3_trade_monitor_backtest as fixed
from research import g3_trade_monitor_robustness as robust
from research import g3_trade_monitor_oi_data as oi_data
from research import g3_trade_monitor_oi_rules as rules
from research.g3_trade_monitor_risk import risk_diagnostics

VERSION = "G3-OI-EXIT-RESEARCH-V1"
REPO = Path(__file__).resolve().parents[1]
PROTOCOL = REPO / "configs/research/g3_oi_exit_protocol_v1.json"
OUTPUT = Path("C:/TradingData/eqidv2/v13_v10_g_3_trade_monitor_oi_research")


def validate_protocol(protocol):
    expected = dict(minimum_completed_post_entry_bars=rules.GUARD_CONSTANTS["minimum_bars"],
        minimum_adverse_close_changes=rules.GUARD_CONSTANTS["adverse_closes"],
        maximum_signed_five_minute_return_pct=rules.GUARD_CONSTANTS["trend_momentum_5m_pct"],
        maximum_signed_entry_return_pct=rules.GUARD_CONSTANTS["trend_entry_move_pct"])
    valid = (protocol["cash_guard"] == expected
        and protocol["oi_rules"]["expansion_thresholds_pct"] == [.5, 1.]
        and protocol["oi_rules"]["contraction_thresholds_pct"] == [-.5, -1.]
        and protocol["oi_rules"]["absolute_activity_thresholds_pct"] == [.5, 1.]
        and protocol["oi_rules"]["exact_same_contract_lookback_minutes"] == 10
        and protocol["oi_rules"]["require_exact_midpoint_minutes"] == 5
        and protocol["base_round_trip_cost_bps"] == search.history.contract.ROUND_TRIP_COST_BPS == 5
        and protocol["additional_advisory_exit_cost_bps"] == 10
        and protocol["lag_sensitivity_minutes"] == [0, 1, 5]
        and protocol["development_lock"] == dict(calibration_sessions=25, validation_sessions=10, evaluation_sessions=15)
        and protocol["execution_authority"] is False and protocol["configuration_promoted"] is False)
    if not valid:
        raise ValueError("Frozen OI protocol differs from implemented experiment")


def apply_events(book, events, allocation="paired"):
    if allocation not in {"paired", "reallocated"}:
        raise ValueError("Unknown allocation mode")
    outcome = book.outcome(events)
    if allocation == "paired":
        outcome["accepted"] = book.baseline["accepted"].copy()
    return outcome


def period_metrics(book, outcome, days):
    result = book.metrics(outcome, days, book.baseline)
    original = book.metrics(book.baseline, days)
    inside = np.isin(book.day, [str(day) for day in days])
    stopped = book.frame.exit_reason.isin({"STOP", "TIGHTENED_STOP"}).to_numpy(bool)
    executed = outcome["accepted"] & inside
    result.update(baseline_max_drawdown_rs=original["max_drawdown_rs"],
        baseline_losing_days=original["losing_days"],
        baseline_net_pnl_rs=original["net_pnl_rs"],
        baseline_realized_loss_rs=original["realized_loss_rs"],
        baseline_native_stop_count=int((inside & book.baseline["accepted"] & stopped).sum()),
        remaining_native_stop_count=int((executed & stopped & ~outcome["modified"]).sum()),
        early_exit_losing_trades=int((executed & outcome["modified"] & (outcome["net"] < 0)).sum()),
        total_losing_trades=result["losses"])
    return result


def canonical_ledger(baseline, outcome, events, policy, allocation):
    """Reconstruct cached-open exits; never retain stale excursion measurements."""
    if allocation not in {"paired", "reallocated"}:
        raise ValueError("Unknown allocation mode")
    raw = baseline.copy().reset_index(drop=True)
    raw["monitor_exit"] = outcome["modified"]
    raw["monitor_flags"] = "[]"
    raw["monitor_decision_at_ist"] = ""
    raw["monitor_open_observed_at_ist"] = ""
    raw["original_exit_reason"] = raw.exit_reason
    raw["original_exit_ts"] = raw.exit_ts
    lookup = {int(sid): i for i, sid in enumerate(raw.sid)}
    for event in events:
        i = lookup[int(event["sid"])]
        raw.at[i, "exit_ts"] = event["modeled_execution_at_ist"]
        raw.at[i, "exit_bar_end_ts"] = event["modeled_execution_at_ist"]
        raw.at[i, "exit_execution_ts"] = event["next_bar_open_at_ist"]
        raw.at[i, "exit_price"] = event["next_bar_open_price"]
        raw.at[i, "exit_reason"] = "MONITOR_OI_RESEARCH"
        raw.at[i, "exit_event"] = "NEXT_OPEN_WITH_END_RESERVATION"
        raw.at[i, "exit_path_index"] = event["execution_path_index"]
        raw.at[i, "holding_minutes"] = (pd.Timestamp(event["modeled_execution_at_ist"]) - pd.Timestamp(raw.at[i, "entry_ts"])).total_seconds() / 60
        for flag in ("stop_hit", "target_hit", "first_target_hit", "runner_target_hit", "same_bar_ambiguous", "exit_gap_through"):
            raw.at[i, flag] = False
        raw.at[i, "exit_gap_bps"] = 0.
        raw.at[i, "active_stop_pct_at_exit"] = (search.history.simulator.TIGHTENED_STOP_PCT
            if pd.Timestamp(event["next_bar_open_at_ist"]) >= pd.Timestamp(raw.at[i, "entry_ts"]) + pd.Timedelta(minutes=search.history.simulator.TIGHTEN_AFTER_MINUTES)
            else search.history.simulator.INITIAL_STOP_PCT)
        raw.at[i, "monitor_decision_at_ist"] = event["decision_at_ist"]
        raw.at[i, "monitor_open_observed_at_ist"] = event["next_bar_open_at_ist"]
        raw.at[i, "monitor_flags"] = event["flags"]
        for column in raw.columns:
            if column.startswith(("mfe_", "mae_")):
                raw.at[i, column] = np.nan
    raw["gross_return_pct"] = outcome["gross"] / 5000
    raw["net_return_pct"] = outcome["net"] / 5000
    raw["cost_pct"] = outcome["cost"] / 5000
    # Source CSVs use space separators; cached events use ISO T separators.
    # Normalize explicitly before the older canonical allocator infers format.
    for column in ("entry_ts", "exit_ts", "confirmation_ts"):
        if column in raw:
            raw[column] = pd.to_datetime(raw[column], utc=True, format="mixed", errors="coerce").dt.tz_convert("Asia/Kolkata")
    if allocation == "reallocated":
        ledger = fixed._allocate(raw, policy)
    else:
        ledger = search.history.simulator.g.v9.v5.apply_fixed_capital_model(raw,
            search.history.contract.CAPITAL_PER_ENTRY_RS, search.history.contract.LEVERAGE)
        ledger["portfolio_executed"] = outcome["accepted"]
        ledger["admission_metadata_scope"] = "ORIGINAL_BASELINE_ADMISSION_STATE_NOT_RECOMPUTED_CANDIDATE_OCCUPANCY"
        for field, key in [("net", "net"), ("gross", "gross"), ("cost", "cost")]:
            column = f"portfolio_{field}_profit_rupees" if field != "cost" else "portfolio_cost_rupees"
            ledger[column] = np.where(outcome["accepted"], outcome[key], 0.)
    ledger["monitor_policy"] = policy
    ledger["allocation_model"] = allocation
    ledger["strategy_version"] = VERSION
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    ledger["configuration_promoted"] = False
    return ledger


def _json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(search.history.replay.json_ready(data), indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def _rows(book, features, candidates, days, period, allocation="paired"):
    rows = []
    for candidate in candidates:
        events = rules.candidate_events(features, candidate, days=days)
        outcome = apply_events(book, events, allocation)
        rows.append(dict(candidate_id=candidate["candidate_id"], family=candidate["family"],
            candidate_sha256=candidate["sha256"], period=period, allocation=allocation,
            **period_metrics(book, outcome, days)))
    control = next(row for row in rows if row["candidate_id"] == "futures_price_guard_only")
    for row in rows:
        row["stress_delta_vs_matched_futures_control_rs"] = (
            row["stress_10bps_net_pnl_rs"] - control["stress_10bps_net_pnl_rs"])
    return rows


def _subset(baseline, features, days):
    allowed = [str(day) for day in days]
    return (search.FastBook(baseline.loc[baseline.day.astype(str).isin(allowed)].copy()),
            features.loc[features.day.astype(str).isin(allowed)].copy())


def _lock(rows, candidates):
    oi_candidates = [candidate for candidate in candidates if candidate["family"].startswith("oi_")]
    oi_rows = pd.DataFrame([row for row in rows if row["family"].startswith("oi_") and
        row["period"] in {"calibration25", "validation10", "development35"}])
    lock = search._choose_finalist(oi_rows, oi_candidates)
    # Incremental OI value, not just an added futures-price signal, is required.
    for row in oi_rows.to_dict("records"):
        if row["stress_delta_vs_matched_futures_control_rs"] <= 0:
            lock["qualification_failures"][row["candidate_id"]].append(
                row["period"] + ":no_incremental_value_over_matched_futures_control")
    eligible = [key for key in lock["qualified_candidate_ids"] if not lock["qualification_failures"][key]]
    lock["qualified_candidate_ids"] = eligible
    lock["development_qualified"] = bool(eligible)
    lock["selected_id"] = eligible[0] if eligible else "baseline"
    if eligible:
        lock["locked_candidate_id"] = eligible[0]
        lock["locked_candidate_sha256"] = next(c["sha256"] for c in candidates if c["candidate_id"] == eligible[0])
    lock.update(execution_authority=False, configuration_promoted=False,
        history_status="REUSED_HISTORY_NOT_FRESH_HOLDOUT", locked_at_utc=datetime.now(timezone.utc).isoformat(),
        final_period_excluded_from_selection=True,
        lock_reason="PASSED_ALL_GATES" if eligible else "BASELINE_RETAINED_DIAGNOSTIC_CANDIDATE_ONLY")
    return lock


def _rolling(baseline, features, candidates, days, output):
    locks, daily_rows, score_rows = [], [], []
    for fold in robust.walk_forward_folds(days):
        scored = {}
        for label in ("train", "validation"):
            current_days = fold[label + "_days"]
            book, current_features = _subset(baseline, features, current_days)
            scored[label] = _rows(book, current_features, candidates, current_days, label)
            score_rows.extend(dict(fold_id=fold["fold_id"], **row) for row in scored[label])
        selected = robust.select_candidate(
            [row for row in scored["train"] if row["family"].startswith("oi_")],
            [row for row in scored["validation"] if row["family"].startswith("oi_")])
        for label in ("train", "validation"):
            for row in scored[label]:
                if row["family"].startswith("oi_") and row["stress_delta_vs_matched_futures_control_rs"] <= 0:
                    selected["failures"][row["candidate_id"]].append(label + ":no_incremental_OI_value")
        selected["eligible_ids"] = [key for key in selected["eligible_ids"] if not selected["failures"][key]]
        selected["selected_id"] = selected["eligible_ids"][0] if selected["eligible_ids"] else "baseline"
        fold_lock = dict(**fold, **selected, locked_at_utc=datetime.now(timezone.utc).isoformat(),
            execution_authority=False, fresh_holdout=False)
        _json(output / f"fold_{fold['fold_id']}_lock.json", fold_lock)
        locks.append(fold_lock)
        test_days = fold["test_days"]
        book, test_features = _subset(baseline, features, test_days)
        candidate = next((c for c in candidates if c["candidate_id"] == selected["selected_id"]), None)
        events = [] if candidate is None else rules.candidate_events(test_features, candidate)
        outcome = apply_events(book, events)
        daily_rows.extend(dict(fold_id=fold["fold_id"], **row) for row in
            robust._daily(book, outcome, test_days, selected["selected_id"]))
    pd.DataFrame(score_rows).to_csv(output / "rolling_development_metrics.csv", index=False)
    pd.DataFrame(daily_rows).to_csv(output / "rolling_test_daily.csv", index=False)
    _json(output / "rolling_locks.json", locks)
    return locks, pd.DataFrame(daily_rows)


def _plot(output, daily, rolling, diagnostic_id):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), constrained_layout=True)
    for key in ["price_guard_only", "futures_price_guard_only", diagnostic_id]:
        part = daily.loc[daily.candidate_id.eq(key)]
        axes[0].plot(pd.to_datetime(part.day), part.stress_delta_rs.cumsum(), label=key)
    axes[0].axhline(0, color="#777", linewidth=.8)
    axes[0].set(title="Paired exits: cumulative difference from baseline after exit stress", ylabel="INR")
    axes[0].legend(fontsize=8)
    axes[1].plot(pd.to_datetime(rolling.day), rolling.stress_delta_rs.cumsum(), label="Rolling selected rule")
    axes[1].axhline(0, color="#777", linewidth=.8)
    axes[1].set(title="Chronological rolling selection (baseline retained when gates fail)", ylabel="INR")
    axes[1].legend(fontsize=8)
    for axis in axes:
        axis.grid(alpha=.2)
    fig.savefig(output / "comparison.png", dpi=150)
    plt.close(fig)


def _report(output, summary, results, sensitivity, diagnostics):
    columns = ["candidate_id", "net_pnl_rs", "net_pnl_delta_vs_baseline_rs",
        "stress_10bps_net_pnl_delta_vs_baseline_rs", "monitor_exit_count", "remaining_native_stop_count",
        "early_exit_losing_trades", "total_losing_trades", "win_rate_pct", "realized_loss_reduction_rs",
        "baseline_winners_cut", "winners_turned_nonpositive", "max_drawdown_rs",
        "stress_delta_vs_matched_futures_control_rs"]
    table = lambda frame: frame.to_html(index=False, float_format=lambda value: f"{value:,.2f}", border=0)
    content = f"""<!doctype html><html><head><meta charset="utf-8"><title>G3 OI exit research</title>
<style>body{{font:15px system-ui;max-width:1600px;margin:35px auto;padding:0 24px;color:#192c37}}h1,h2{{color:#164d63}}
table{{border-collapse:collapse;font-size:12px;white-space:nowrap}}td,th{{padding:8px;border-bottom:1px solid #ddd;text-align:right}}
th{{background:#edf5f8}}.scroll{{overflow:auto}}.note{{background:#fff4da;padding:18px}}img{{max-width:100%}}</style></head><body>
<h1>G3 OI exit research</h1><p>{escape(summary['conclusion'])}</p>
<p class="note">BACKTEST ONLY. No live or paper configuration changed. Reused strategy-selected history,
not a fresh holdout. Primary OI availability is assumed at completed futures bar end plus one minute;
retained historical revisions are not proof that these values were available live.</p>
<p>Original cohort: {summary['baseline']['executed_trades']} trades over {summary['sessions']} sessions.
Baseline net INR {summary['baseline']['net_pnl_rs']:,.2f}; native stop exits {summary['baseline']['baseline_native_stop_count']}.
Same sizing: INR 500,000 exposure per trade, 5 bps round-trip cost; stress adds INR 500 per early exit.
Primary comparison retains the original execution mask. Missing OI leaves native stops/targets intact.</p>
<h2>All-session paired comparison</h2><div class="scroll">{table(results.loc[(results.period=='all') & (results.allocation=='paired'), columns])}</div>
<p>A reduction in exits labelled STOP is not a reduction in losing trades. Early losses remain losses.
Drawdown here is daily-close realized equity, not intraday mark-to-market.</p>
<img src="comparison.png" alt="Cumulative differences from the baseline after extra exit costs">
<h2>Locked final 15-session diagnostic</h2><p>Selected for diagnostic evaluation: {escape(summary['lock']['locked_candidate_id'])}.
Actual eligible selection: {escape(summary['lock']['selected_id'])}. Final period excluded from selection, but underlying strategy used all this history.</p>
<div class="scroll">{table(results.loc[(results.period=='evaluation15') & (results.allocation=='paired') & results.candidate_id.isin([summary['lock']['locked_candidate_id'], 'price_guard_only', 'futures_price_guard_only']), columns])}</div>
<h2>Availability sensitivity</h2><div class="scroll">{table(sensitivity)}</div>
<p>Receipt-constrained mode uses timestamps of retained revisions, not immutable first-seen snapshots.
Sparse receipt-constrained triggers cannot establish historical live equivalence. Lag settings are diagnostics, never used to select a winner.</p>
<h2>Uncertainty and concentration</h2><div class="scroll">{table(diagnostics)}</div>
<p>Bootstrap intervals are descriptive daily-mean differences after exit stress. Simultaneous bounds cover the six frozen OI arms only;
they cannot adjust earlier strategy selection or unknown prior trials. Bootstrap positive fractions are not probabilities of future profit.</p>
<h2>Evidence</h2><p>Full ledgers, event traces, frozen decision inputs, matched futures-price control, rolling selection locks,
reallocation sensitivity, cost sensitivities, source and code hashes are retained beside this report.
No stops were widened, no missing-data trades were dropped, and no thresholds were refined after scoring.</p>
<p><a href="policy_results.csv">Policy results</a> · <a href="summary.json">Summary</a> · <a href="selection_lock.json">Selection lock</a>
 · <a href="coverage.csv">Coverage</a> · <a href="manifest.json">Artifact hashes</a></p></body></html>"""
    (output / "report.html").write_text(content, encoding="utf-8")


def run(source=None, output_root=OUTPUT, data_root=oi_data.DEFAULT_DATA_ROOT):
    # Current operational profiles may advance independently. This experiment
    # is pinned to the report's V7 donor / V9 backtest, in this process only.
    from research.archived_g3_v7 import contract as production
    from research.archived_g3_v9 import contract
    if (production.strategy_fingerprint() != "68e842bb99002c950304cc40a41aa40e93a6394b131199ade872ec5e852a281f"
            or contract.strategy_fingerprint() != "d4948aee6d2c662ff362e67ea3c949268b9b4b1b4d69141881edb24f2be5127f"):
        raise ValueError("Archived study contracts changed")
    original = search.history.production, search.history.contract
    try:
        search.history.production, search.history.contract = production, contract
        return _run_pinned(source, output_root, data_root)
    finally:
        search.history.production, search.history.contract = original


def _run_pinned(source, output_root, data_root):
    protocol = json.loads(PROTOCOL.read_text(encoding="utf-8"))
    validate_protocol(protocol)
    source = Path(source or protocol["source_search_run"])
    print("Verifying sealed corrected source", flush=True)
    source_summary, _ = search.verified_package(source)
    days = [date.fromisoformat(value) for value in source_summary["days"]]
    periods = search._periods(days)
    output = Path(output_root) / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + "_" + secrets.token_hex(4) + "_oi_exits")
    output.mkdir(parents=True, exist_ok=False)
    (output / "ledgers").mkdir()
    (output / "events").mkdir()
    (output / "source_code").mkdir()
    _json(output / "protocol.json", protocol)
    candidates = rules.candidate_registry()
    _json(output / "candidate_registry.json", candidates)
    code_paths = [Path(__file__), Path(oi_data.__file__), Path(rules.__file__), Path(search.__file__),
        Path(fixed.__file__), Path(robust.__file__), REPO / "research/g3_trade_monitor_risk.py",
        Path(search.history.__file__), Path(search.history.production.__file__), Path(search.history.contract.__file__),
        Path(search.history.simulator.__file__), Path(search.history.simulator.g.v9.v6.__file__),
        Path(search.history.simulator.g.v9.v5.__file__)]
    code_hashes = {str(path): search.history.sha256(path) for path in code_paths}
    for path in code_paths:
        destination = output / "source_code" / path.relative_to(REPO)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    _json(output / "source_provenance.json", dict(source=str(source),
        source_manifest_sha256=search.history.sha256(source / "manifest.json"), code_sha256=code_hashes,
        protocol_sha256=search.history.sha256(PROTOCOL)))
    baseline = search.read_frozen_csv(source / "ledgers/baseline.csv")
    features = search.read_frozen_csv(source / "decision_features.csv")
    identity = [key for key in ["futures_tradingsymbol", "futures_instrument_token", "contract", "opening_oi_contract"] if key in baseline]
    features = features.merge(baseline[["sid"] + identity], on="sid", how="left", validate="many_to_one")
    baseline.to_csv(output / "ledgers/baseline.csv", index=False)
    print("Joining exact-contract OI with assumed one-minute availability", flush=True)
    enriched, coverage, provenance = oi_data.join_decision_features(features, data_root, lag_minutes=1)
    enriched.to_parquet(output / "decision_features_oi.parquet", index=False)
    coverage.to_csv(output / "coverage.csv", index=False)
    _json(output / "oi_provenance.json", provenance)
    development = []
    for period in ("calibration25", "validation10", "development35"):
        book, part = _subset(baseline, enriched, periods[period])
        development.extend(_rows(book, part, candidates, periods[period], period))
    lock = _lock(development, candidates)
    _json(output / "selection_lock.json", lock)
    print("Development lock: " + lock["selected_id"] + "; diagnostic: " + lock["locked_candidate_id"], flush=True)
    book = search.FastBook(baseline)
    results = development.copy()
    for allocation in ("paired", "reallocated"):
        for period in ("evaluation15", "all"):
            results.extend(_rows(book, enriched, candidates, periods[period], period, allocation))
    results = pd.DataFrame(results)
    results.to_csv(output / "policy_results.csv", index=False)
    daily_rows, risk_rows = [], []
    for candidate in candidates:
        identifier = candidate["candidate_id"]
        events = rules.candidate_events(enriched, candidate)
        _json(output / "events" / (identifier + ".json"), events)
        for allocation in ("paired", "reallocated"):
            outcome = apply_events(book, events, allocation)
            ledger = canonical_ledger(baseline, outcome, events, identifier, allocation)
            search._assert_fast_parity(book, outcome, ledger, identifier + ":" + allocation)
            ledger.to_csv(output / "ledgers" / f"{identifier}_{allocation}.csv", index=False)
            risk_rows.append(dict(candidate_id=identifier, allocation=allocation, **risk_diagnostics(ledger)))
            if allocation == "paired":
                daily_rows.extend(robust._daily(book, outcome, days, identifier))
    _json(output / "risk_diagnostics.json", dict(baseline=risk_diagnostics(baseline), candidates=risk_rows))
    daily = pd.DataFrame(daily_rows)
    daily.to_csv(output / "daily_comparison.csv", index=False)
    print("Evaluating six chronological rolling folds", flush=True)
    locks, rolling = _rolling(baseline, enriched, candidates, days, output)
    print("Testing availability assumptions without changing selection", flush=True)
    sensitivity_rows = []
    for mode, lag in [("bar_end_plus_lag", 0), ("bar_end_plus_lag", 1), ("bar_end_plus_lag", 5), ("received_time", 1)]:
        if mode == "bar_end_plus_lag" and lag == 1:
            scenario, scenario_provenance = enriched, provenance
        else:
            scenario, scenario_coverage, scenario_provenance = oi_data.join_decision_features(features, data_root, availability_mode=mode, lag_minutes=lag)
            scenario.to_parquet(output / f"decision_features_{mode}_{lag}.parquet", index=False)
            scenario_coverage.to_csv(output / f"coverage_{mode}_{lag}.csv", index=False)
            _json(output / f"oi_provenance_{mode}_{lag}.json", scenario_provenance)
        for row in _rows(book, scenario, candidates, days, "all"):
            sensitivity_rows.append({key: row[key] for key in ["candidate_id", "monitor_exit_count", "net_pnl_delta_vs_baseline_rs", "stress_10bps_net_pnl_delta_vs_baseline_rs"]} |
                dict(availability_mode=mode, lag_minutes=lag, valid_decision_rows=scenario_provenance["valid_decision_rows"]))
    sensitivity = pd.DataFrame(sensitivity_rows)
    sensitivity.to_csv(output / "availability_sensitivity.csv", index=False)
    oi_ids = [candidate["candidate_id"] for candidate in candidates if candidate["family"].startswith("oi_")]
    familywise = robust._familywise(daily.loc[daily.candidate_id.isin(oi_ids)], oi_ids, days)
    diagnostics = []
    for key in oi_ids:
        part = daily.loc[daily.candidate_id.eq(key)]
        bootstrap = robust.moving_block_bootstrap(part.stress_delta_rs)
        diagnostics.append(dict(candidate_id=key, **bootstrap, **familywise[key],
            stress_delta_excluding_best_day_rs=float(part.stress_delta_rs.sum()-part.stress_delta_rs.max()),
            positive_delta_days=int((part.stress_delta_rs > 0).sum()), negative_delta_days=int((part.stress_delta_rs < 0).sum())))
    diagnostics = pd.DataFrame(diagnostics)
    diagnostics.to_csv(output / "uncertainty_diagnostics.csv", index=False)
    costs = []
    base = period_metrics(book, book.baseline, days)
    for row in results.loc[results.period.eq("all")].to_dict("records"):
        for extra in (0, 5, 10, 20):
            costs.append(dict(candidate_id=row["candidate_id"], allocation=row["allocation"], additional_round_trip_bps=extra,
                stress_delta_rs=robust.stress_delta(row["net_pnl_rs"], base["net_pnl_rs"], row["executed_trades"], base["executed_trades"], row["monitor_exit_count"], extra)))
    pd.DataFrame(costs).to_csv(output / "cost_sensitivity.csv", index=False)
    locked_eval = results.loc[(results.period.eq("evaluation15")) & (results.allocation.eq("paired")) & results.candidate_id.eq(lock["locked_candidate_id"])].iloc[0].to_dict()
    evaluation_failures = search._qualification_failures(locked_eval, "evaluation15")
    if locked_eval["stress_delta_vs_matched_futures_control_rs"] <= 0:
        evaluation_failures.append("no_incremental_OI_value")
    passes = lock["development_qualified"] and not evaluation_failures
    conclusion = ("Historical gates passed; fresh forward evidence still required before promotion." if passes else
        "No OI exit rule qualified for promotion. Retain the original stops and targets; frozen rules remain research only.")
    summary = dict(version=VERSION, source=str(source), output=str(output), sessions=len(days), days=[str(day) for day in days],
        execution_authority=False, configuration_promoted=False, modify_live_or_paper=False, fresh_strategy_holdout=False,
        baseline=base, lock=lock, selection_lock_sha256=search.history.sha256(output / "selection_lock.json"),
        locked_evaluation=locked_eval, evaluation_failures=evaluation_failures, historical_gates_passed=passes,
        rolling_baseline_folds=sum(item["selected_id"] == "baseline" for item in locks), rolling_folds=len(locks),
        rolling_net_delta_rs=float(rolling.delta_rs.sum()), rolling_stress_delta_rs=float(rolling.stress_delta_rs.sum()),
        oi_valid_decision_rows=provenance["valid_decision_rows"], decision_rows=len(enriched),
        retained_receipt_proven_rows_primary=provenance["point_in_time_proven_rows"], conclusion=conclusion,
        limitations=["Underlying setups selected on all 50 sessions; this is not fresh out-of-sample evidence.",
            "Default OI availability is hypothetical; archives contain later fetches and overwritten revisions.",
            "5-minute OI cannot predict every intraminute stop; native stops/targets remain active.",
            "Next-open pricebar fills omit live spread, queue and latency; additional cost stress is modeled.",
            "Daily-close and realized-exit drawdowns are not intraday mark-to-market drawdown.",
            "Six-arm bootstrap excludes unknown prior research multiplicity."])
    _json(output / "summary.json", summary)
    _plot(output, daily, rolling, lock["locked_candidate_id"])
    _report(output, summary, results, sensitivity, diagnostics)
    for path in code_paths:
        if search.history.sha256(path) != code_hashes[str(path)]:
            raise RuntimeError("Research code changed during run: " + str(path))
    files = {path.relative_to(output).as_posix(): search.history.sha256(path) for path in output.rglob("*") if path.is_file()}
    _json(output / "manifest.json", dict(version=VERSION, execution_authority=False, configuration_promoted=False, files=files))
    for relative, digest in files.items():
        if search.history.sha256(output / relative) != digest:
            raise RuntimeError("Artifact verification failed: " + relative)
    print(json.dumps(dict(output=str(output), conclusion=conclusion, verified_artifacts=len(files))), flush=True)
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--output-root", type=Path, default=OUTPUT)
    parser.add_argument("--data-root", type=Path, default=oi_data.DEFAULT_DATA_ROOT)
    args = parser.parse_args(argv)
    run(args.source, args.output_root, args.data_root)


if __name__ == "__main__":
    main()
