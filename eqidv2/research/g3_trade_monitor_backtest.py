"""Fixed advisory-exit research on sealed G3 trades and genuine minute paths.

The dashboard may refresh every 15 seconds. These historical inputs contain
one-minute OHLC bars only; no synthetic subminute observations are created.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import date, datetime
from html import escape
import json
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
import g3_trade_monitor_rules as rules
from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv


VERSION = "G3-TRADE-MONITOR-EXIT-RESEARCH-V2"
SCHEMA = "g3_trade_monitor_research_run_v2"
POINTER_SCHEMA = "g3_trade_monitor_research_pointer_v2"
DEFAULT_SOURCE = runtime_dir("backtesting_result_v13_v10_g_3")
DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_3_trade_monitor_backtest")
POLICIES = {
    "baseline": frozenset(),
    "trend_failure": frozenset({"TREND_FAILURE"}),
    "profit_giveback": frozenset({"PROFIT_GIVEBACK"}),
    "combined": frozenset({"TREND_FAILURE", "PROFIT_GIVEBACK"}),
}
MINUTE_NS = 60_000_000_000
FILES = {"summary.json", "config.json", "report.html", "policy_results.csv", "daily_comparison.csv",
    "trade_comparison.csv", "signals.csv", "monthly_results.csv", "selected_orders.csv",
    *(f"ledgers/{name}.csv" for name in POLICIES)}


def configuration():
    expected = dict(refresh_seconds=15, max_price_age_seconds=120, minimum_bars=6,
        adverse_closes=3, trend_momentum_5m_pct=-.15, trend_entry_move_pct=-.30,
        profit_activation_pct=.50, profit_giveback_pct=.30)
    if rules.MONITOR_CONFIG != expected:
        raise ValueError("Shared monitor rules changed from the preregistered fixed policies")
    return dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        shared_rules_version=rules.RULES_VERSION, shared_rule_parameters=expected,
        decision_role="ADVISORY_EXIT_RESEARCH_ONLY", policies=list(POLICIES),
        optimize_parameters=False, modify_live_or_paper=False,
        rules=dict(minimum_contiguous_full_post_entry_bars=6, adverse_close_changes=3,
            trend_failure_signed_five_minute_move_max_pct=-.15,
            trend_failure_signed_entry_move_max_pct=-.30,
            profit_giveback_minimum_mfe_pct=.50, profit_giveback_minimum_percentage_points=.30),
        historical_bar_resolution_seconds=60, dashboard_refresh_seconds=15,
        subminute_ohlc_fabricated=False, volume_required_for_price_rules=False,
        unavailable_volume="OMITTED_NOT_SYNTHESIZED",
        execution_model="NEXT_AVAILABLE_ONE_MINUTE_BAR_OPEN_PRICE_WITH_BAR_END_EXECUTION_TIMESTAMP_PROXY",
        original_exit_precedence="KEEP_ORIGINAL_EXIT_ONLY_WHEN_EXECUTION_TIMESTAMP_LE_MONITOR_NEXT_OPEN",
        pending_open_order_convention="ADVISORY_MARKET_OPEN_PRECEDES_LATER_INTRABAR_EVENTS_INCLUDING_NATIVE_TARGETS",
        target_gap_convention="PENDING_ADVISORY_EXECUTES_AT_OBSERVED_OPEN_WITHOUT_LOOKING_AT_LATER_INTRABAR_TARGET_OR_STOP",
        timer_and_capital_convention="CONSERVATIVE_FULL_NEXT_BAR_RESERVATION",
        original_stops_targets_squareoff_unchanged=True,
        development_sessions=35, evaluation_sessions=15,
        evaluation_label="MONITOR_CHRONOLOGICAL_CHECK_NOT_UNTOUCHED_STRATEGY_HOLDOUT",
        source_strategy_fitted_on_historical_cohort=True,
        capital_per_entry_rs=history.contract.CAPITAL_PER_ENTRY_RS,
        leverage=history.contract.LEVERAGE,
        portfolio_capital_rs=history.contract.PORTFOLIO_CAPITAL_RS,
        round_trip_cost_bps=history.contract.ROUND_TRIP_COST_BPS)


def fingerprint():
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _resolve_source(source):
    source = Path(source).resolve()
    canonical_root = DEFAULT_SOURCE.resolve()
    if (source / "latest/current_backtest.json").is_file():
        root = source
        pointer = _read(root / "latest/current_backtest.json")
        directory = history._child(root, pointer["run_dir"])
        if history.sha256(directory / "manifest.json") != pointer["manifest_sha256"]:
            raise ValueError("Current G3 source pointer hash changed")
    else:
        root, directory = canonical_root, source
    if root != canonical_root or not directory.is_relative_to(root):
        raise ValueError("Monitor research source must be a sealed run inside the canonical G3 result root")
    summary, manifest = history.verified_package(directory, root)
    if (summary["strategy_fingerprint"] != history.contract.strategy_fingerprint()
            or summary["active_setup_count"] != len(history.contract.ACTIVE_SETUPS)
            or summary["sessions"] < 50):
        raise ValueError("Fixed monitor research requires current G3 history with the original 50 sessions")
    return root, directory, summary, manifest


def _paths_for_source(root, orders, days, source_summary=None):
    original_orders, _, original_days, members, proof = history._baseline(root)
    if days[:len(original_days)] != original_days or days != sorted(set(days)):
        raise ValueError("Monitor source differs from the sealed original 50-session minute inventory")
    revised_orders, revised_paths, revision_proof = history._revised_0941_orders(original_orders, original_days, members)
    paths, historical_proof = history._historical_paths(original_days[:49])
    historical_orders = original_orders.loc[pd.to_datetime(original_orders.day).dt.date.isin(original_days[:49])]
    paths = history._selected_path_subset(paths, historical_orders)
    daily_proofs = []
    for member, payload, selected in members:
        group, source_proof = history._daily_paths(member, payload, selected)
        if set(group) & set(paths):
            raise ValueError("Selected historical and daily execution path identities overlap")
        paths.update(group)
        daily_proofs.append(source_proof)
    if set(revised_paths) & set(paths):
        raise ValueError("Revised 09:41 paths overlap original monitor paths")
    paths.update(revised_paths)
    verified_ids = set(original_orders.sid) | set(revised_orders.sid)
    revision_sources = {}
    if orders.setup_id.eq('0926_LONG').any():
        from research.g3_0926_volume_backtest import reselect as reselect_0926
        revised_0926, volume_paths, volume_proof, _ = reselect_0926(original_orders, original_days)
        for sid, path in volume_paths.items():
            if sid in paths:
                original_path = paths[sid]
                if (set(path) != set(original_path)
                        or any(not np.array_equal(path[key], original_path[key]) for key in path)):
                    raise ValueError("Pinned 09:26 execution path differs from candidate evidence")
            else:
                paths[sid] = path
        verified_ids.update(revised_0926.sid)
        revision_sources['revised_0926_long_volume'] = volume_proof
    if orders.setup_id.isin({'1036_LONG', '1036_SHORT'}).any():
        from research.g3_1035_extension_backtest import reselect
        guarded, guarded_paths, guard_proof, _ = reselect(original_orders, original_days, members)
        if set(guarded_paths) & set(paths):
            raise ValueError("Revised 10:35 paths overlap monitor paths")
        paths.update(guarded_paths)
        verified_ids.update(guarded.sid)
        revision_sources['revised_1035_extension'] = guard_proof
    if orders.setup_id.isin({'0921_SHORT', '1011_LONG'}).any():
        from research.g3_two_slot_backtest import reselect
        added, added_paths, added_proof, _ = reselect(original_orders, original_days, members)
        if set(added_paths) & set(paths):
            raise ValueError("New-slot paths overlap monitor paths")
        paths.update(added_paths)
        verified_ids.update(added.sid)
        revision_sources['enabled_two_slots'] = added_proof
    base_orders = orders.loc[pd.to_datetime(orders.day).dt.date.isin(original_days)]
    if not set(base_orders.sid).issubset(verified_ids):
        raise ValueError("Monitor source contains orders outside verified historical candidates")
    paths = history._selected_path_subset(paths, base_orders)
    extension_days = days[len(original_days):]
    records = (source_summary or {}).get('daily_sources', [])
    observed_days = []
    for record in records:
        member, payload = history._checked_reference(root, record)
        history._check_files(member, payload, daily=True)
        day = date.fromisoformat(payload['session_date'])
        if (day not in extension_days or day in observed_days or payload.get('complete') is not True
                or payload.get('strategy_fingerprint') != history.contract.strategy_fingerprint()):
            raise ValueError("Monitor daily extension identity or calendar changed")
        raw = read_frozen_csv(member / payload['artifacts']['selected_orders']['path'])
        group, source_proof = history._daily_paths(member, payload, raw)
        selected = orders.loc[pd.to_datetime(orders.day).dt.date.eq(day)]
        if set(raw.signal_id) != set(selected.signal_id) or raw.signal_id.duplicated().any():
            raise ValueError("Monitor daily extension orders differ from their sealed source")
        original_ids = raw.set_index('signal_id').sid
        for row in selected.itertuples(index=False):
            source_sid = int(original_ids.loc[row.signal_id])
            if int(row.source_sid) != source_sid or int(row.sid) in paths:
                raise ValueError("Monitor daily extension path identity changed or overlaps")
            paths[int(row.sid)] = group[source_sid]
        observed_days.append(day)
        daily_proofs.append(source_proof)
    if sorted(observed_days) != extension_days:
        raise ValueError("Monitor source is missing a sealed daily extension")
    return history._selected_path_subset(paths, orders), dict(original_source=proof,
        historical_paths=historical_proof, daily_paths=daily_proofs,
        revised_0941_long=revision_proof, **revision_sources)


def _periods(days):
    """Keep the original 35/15 check fixed as subsequent sessions arrive."""
    if len(days) < 50 or days != sorted(set(days)):
        raise ValueError("Monitor research requires the original 50 chronological sessions")
    periods = {'development35': days[:35], 'evaluation15': days[35:50], 'all': days}
    if len(days) > 50:
        periods['daily_extensions'] = days[50:]
    return periods


def _timestamp(value):
    return pd.Timestamp(value).tz_convert(history.contract.IST)


def _bar(path, index):
    item = dict(timestamp=pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC").tz_convert(history.contract.IST),
        **{key: float(path[key][index]) for key in ("open", "high", "low", "close")})
    if "volume" in path and np.isfinite(path["volume"][index]):
        item["volume"] = float(path["volume"][index])
    return item


def trade_advisories(row, path, *, assessor=None):
    """Return first eligible exit per flag using completed post-entry bars.

    Price is the following bar's open. The position remains reserved through
    that bar's end. Only native exits at or before the actual open take
    precedence; later extremes cannot cancel a pending market-open exit.
    The capital reservation is conservative, not an execution latency model.
    """
    assessor = assessor or rules.assess_trade
    if not bool(row.filled):
        return {}, []
    entry_index = int(row.entry_path_index)
    entry_at = _timestamp(row.entry_ts)
    original_exit_at = _timestamp(row.exit_ts)
    direction = 1 if row.side == "LONG" else -1
    trade = dict(side=row.side, entry_price=float(row.entry_price), entry_at_ist=entry_at,
        stop_price=float(row.entry_price) * (1 - direction * float(row.initial_stop_pct) / 100),
        target_price=float(row.entry_price) * (1 + direction * float(row.native_target_pct) / 100),
        quantity=history.contract.CAPITAL_PER_ENTRY_RS * history.contract.LEVERAGE / float(row.entry_price))
    completed, first, signals = [], {}, []
    for index in range(entry_index + 1, len(path["timestamp_ns"]) - 1):
        bar = _bar(path, index)
        decision_at = bar["timestamp"]
        if original_exit_at <= decision_at:
            break
        if decision_at - pd.Timedelta(minutes=1) < entry_at:
            continue
        completed.append(bar)
        if len(completed) < 6:
            continue
        assessment = assessor(trade, completed, now=decision_at,
            mark_price=bar["close"], mark_timestamp=decision_at)
        flags = sorted(set(assessment.get("advisory_flags", [])) & POLICIES["combined"])
        if not flags:
            continue
        execution_index = index + 1
        execution_at = pd.Timestamp(int(path["timestamp_ns"][execution_index]), tz="UTC").tz_convert(history.contract.IST)
        open_at = execution_at - pd.Timedelta(minutes=1)
        eligible = original_exit_at > open_at
        signal = dict(sid=int(row.sid), source_signal_id=row.signal_id, day=str(row.day), setup_id=row.setup_id,
            tradingsymbol=row.tradingsymbol, side=row.side, decision_at_ist=decision_at.isoformat(),
            completed_bar_count=len(completed), flags=json.dumps(flags),
            next_bar_open_at_ist=open_at.isoformat(),
            modeled_execution_at_ist=execution_at.isoformat(), execution_path_index=execution_index,
            next_bar_open_price=float(path["open"][execution_index]),
            original_exit_at_ist=original_exit_at.isoformat(), eligible_for_monitor_exit=eligible,
            decision="MONITOR_CANDIDATE" if eligible else "ORIGINAL_EXIT_AT_OR_BEFORE_OPEN_HAS_PRECEDENCE")
        signals.append(signal)
        if eligible:
            for flag in flags:
                first.setdefault(flag, signal)
        if set(first) == POLICIES["combined"]:
            break
    return first, signals


def _apply_monitor_exit(frame, index, event, path, policy):
    row = frame.loc[index]
    execution_index = int(event["execution_path_index"])
    execution_at = _timestamp(event["modeled_execution_at_ist"])
    open_at = _timestamp(event["next_bar_open_at_ist"])
    entry_at = _timestamp(row.entry_ts)
    if (open_at != execution_at - pd.Timedelta(minutes=1)
            or open_at < _timestamp(event["decision_at_ist"])
            or not open_at < _timestamp(row.exit_ts)):
        raise ValueError("Monitor open fill cannot precede its decision or overtake an original same-or-earlier exit")
    direction = 1 if row.side == "LONG" else -1
    entry_price, exit_price = float(row.entry_price), float(event["next_bar_open_price"])
    gross = direction * (exit_price / entry_price - 1) * 100
    mfe, mae = history.simulator.g.v9.v5._excursions(path, entry_price, int(row.entry_path_index),
        max(int(row.entry_path_index), execution_index - 1), row.side == "LONG")
    active_stop = (history.simulator.TIGHTENED_STOP_PCT
        if open_at >= entry_at + pd.Timedelta(minutes=history.simulator.TIGHTEN_AFTER_MINUTES)
        else history.simulator.INITIAL_STOP_PCT)
    changes = dict(exit_path_index=execution_index, exit_price=exit_price, exit_ts=execution_at,
        exit_bar_end_ts=execution_at.isoformat(), exit_execution_ts=open_at.isoformat(),
        exit_reason="MONITOR_" + policy.upper(), exit_event="NEXT_OPEN_WITH_END_RESERVATION",
        gross_return_pct=gross, net_return_pct=gross-history.contract.ROUND_TRIP_COST_BPS/100,
        cost_pct=history.contract.ROUND_TRIP_COST_BPS/100,
        mfe_pct=max(mfe, gross), mae_pct=min(mae, gross),
        holding_minutes=(execution_at-entry_at).total_seconds()/60,
        active_stop_pct_at_exit=active_stop, stop_hit=False, target_hit=False, first_target_hit=False,
        runner_target_hit=False, same_bar_ambiguous=False, exit_gap_through=False, exit_gap_bps=0.,
        monitor_exit=True, monitor_flags=event["flags"], monitor_decision_at_ist=event["decision_at_ist"],
        monitor_open_observed_at_ist=event["next_bar_open_at_ist"])
    for name, value in changes.items():
        frame.loc[index, name] = value


def _allocate(raw, policy):
    settings = history.simulator.g.v9.V9Config(
        portfolio_capital_rupees=history.contract.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=history.contract.CAPITAL_PER_ENTRY_RS,
        leverage_factor=history.contract.LEVERAGE, max_positions=history.contract.MAX_POSITIONS,
        cost_bps=history.contract.ROUND_TRIP_COST_BPS)
    raw = history.simulator.g.v9.v5.apply_fixed_capital_model(raw,
        settings.capital_per_entry_rupees, settings.leverage_factor)
    ledger, _ = history.simulator.g.v9.v6.apply_portfolio_constraints(raw, settings.portfolio_config())
    ledger["day"] = pd.to_datetime(ledger.day).dt.strftime("%Y-%m-%d")
    ledger["source_signal_id"] = ledger.signal_id
    ledger["source_strategy_version"] = history.contract.STRATEGY_VERSION
    ledger["source_strategy_fingerprint"] = history.contract.strategy_fingerprint()
    ledger["strategy_version"] = VERSION
    ledger["strategy_fingerprint"] = fingerprint()
    ledger["strategy"] = "G3_TRADE_MONITOR_RESEARCH"
    ledger["monitor_policy"] = policy
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    return ledger


def evaluate_policies(fresh_baseline, paths, *, assessor=None):
    """Replay all native fills, including capital-rejected baseline candidates."""
    first_by_sid, signals = {}, []
    for row in fresh_baseline.itertuples(index=False):
        first, observed = trade_advisories(row, paths[int(row.sid)], assessor=assessor)
        first_by_sid[int(row.sid)] = first
        signals.extend(observed)
    ledgers = {}
    for policy, wanted in POLICIES.items():
        raw = fresh_baseline.copy()
        raw["monitor_exit"] = False
        raw["monitor_flags"] = "[]"
        raw["monitor_decision_at_ist"] = ""
        raw["monitor_open_observed_at_ist"] = ""
        for index, row in raw.iterrows():
            candidates = [event for flag, event in first_by_sid[int(row.sid)].items() if flag in wanted]
            if candidates:
                event = min(candidates, key=lambda item: (item["execution_path_index"], item["flags"]))
                _apply_monitor_exit(raw, index, event, paths[int(row.sid)], policy)
        ledgers[policy] = _allocate(raw, policy)
    history._same_native(ledgers["baseline"], fresh_baseline)
    history._same_allocation(ledgers["baseline"], fresh_baseline)
    signal_columns = ["sid", "source_signal_id", "day", "setup_id", "tradingsymbol", "side",
        "decision_at_ist", "completed_bar_count", "flags", "next_bar_open_at_ist",
        "modeled_execution_at_ist", "execution_path_index", "next_bar_open_price", "original_exit_at_ist",
        "eligible_for_monitor_exit", "decision"]
    return ledgers, pd.DataFrame(signals, columns=signal_columns)


def comparisons(ledgers, days):
    baseline = ledgers["baseline"].set_index("sid")
    rows = []
    for policy, ledger in ledgers.items():
        for row in ledger.itertuples(index=False):
            original = baseline.loc[int(row.sid)]
            common = bool(original.portfolio_executed and row.portfolio_executed)
            changed = bool(row.monitor_exit)
            delta = float(row.portfolio_net_profit_rupees-original.portfolio_net_profit_rupees)
            stopped = original.exit_reason in {"STOP", "TIGHTENED_STOP"}
            rows.append(dict(policy=policy, sid=int(row.sid), source_signal_id=row.source_signal_id,
                day=str(row.day), setup_id=row.setup_id, tradingsymbol=row.tradingsymbol, side=row.side,
                native_filled=bool(row.filled), baseline_executed=bool(original.portfolio_executed),
                policy_executed=bool(row.portfolio_executed), monitor_exit=changed,
                monitor_decision_at_ist=row.monitor_decision_at_ist, monitor_flags=row.monitor_flags,
                entry_at_ist=row.entry_ts, baseline_exit_at_ist=original.exit_ts, policy_exit_at_ist=row.exit_ts,
                baseline_exit_reason=original.exit_reason, policy_exit_reason=row.exit_reason,
                baseline_exit_price=original.exit_price, policy_exit_price=row.exit_price,
                baseline_net_pnl_rs=float(original.portfolio_net_profit_rupees),
                policy_net_pnl_rs=float(row.portfolio_net_profit_rupees), net_pnl_delta_rs=delta,
                newly_admitted=bool(row.portfolio_executed and not original.portfolio_executed),
                newly_rejected=bool(original.portfolio_executed and not row.portfolio_executed),
                stopped_trade_improved=bool(common and changed and stopped and delta > 0),
                stop_loss_saved_rs=max(delta, 0.) if common and changed and stopped else 0.,
                stopped_trade_worsened=bool(common and changed and stopped and delta < 0),
                baseline_winner_cut=bool(common and changed and original.portfolio_net_profit_rupees > 0 and delta < 0),
                winner_profit_reduced_rs=-delta if common and changed and original.portfolio_net_profit_rupees > 0 and delta < 0 else 0.,
                winner_turned_nonpositive=bool(common and changed and original.portfolio_net_profit_rupees > 0
                    and row.portfolio_net_profit_rupees <= 0)))
    trades = pd.DataFrame(rows)
    periods = _periods(days)
    metrics, daily, monthly = [], [], []
    for period, cohort in periods.items():
        original_stats = history._stats(ledgers["baseline"].loc[ledgers["baseline"].day.isin([str(d) for d in cohort])], cohort)
        for policy, ledger in ledgers.items():
            selected = ledger.loc[ledger.day.isin([str(d) for d in cohort])]
            comp = trades.loc[trades.policy.eq(policy) & trades.day.isin([str(d) for d in cohort])]
            stats = history._stats(selected, cohort)
            metrics.append(dict(period=period, policy=policy, first_date=str(cohort[0]), last_date=str(cohort[-1]),
                sessions=len(cohort), **stats,
                net_pnl_delta_vs_baseline_rs=stats["net_pnl_rs"]-original_stats["net_pnl_rs"],
                executed_trade_delta=stats["executed_trades"]-original_stats["executed_trades"],
                monitor_exit_count=int((comp.monitor_exit & comp.policy_executed).sum()),
                newly_admitted=int(comp.newly_admitted.sum()), newly_rejected=int(comp.newly_rejected.sum()),
                stopped_trades_improved=int(comp.stopped_trade_improved.sum()),
                stop_loss_saved_rs=float(comp.stop_loss_saved_rs.sum()),
                stopped_trades_worsened=int(comp.stopped_trade_worsened.sum()),
                baseline_winners_cut=int(comp.baseline_winner_cut.sum()),
                winner_profit_reduced_rs=float(comp.winner_profit_reduced_rs.sum()),
                winners_turned_nonpositive=int(comp.winner_turned_nonpositive.sum())))
    for policy, ledger in ledgers.items():
        for day in days:
            actual = history._stats(ledger.loc[ledger.day.eq(str(day))], [day])
            original = history._stats(ledgers["baseline"].loc[ledgers["baseline"].day.eq(str(day))], [day])
            daily.append(dict(day=str(day), policy=policy, **actual,
                baseline_net_pnl_rs=original["net_pnl_rs"], net_pnl_delta_rs=actual["net_pnl_rs"]-original["net_pnl_rs"]))
        for month in sorted({str(d)[:7] for d in days}):
            cohort = [d for d in days if str(d).startswith(month)]
            monthly.append(dict(month=month, policy=policy,
                **history._stats(ledger.loc[ledger.day.str.startswith(month)], cohort)))
    return pd.DataFrame(metrics), pd.DataFrame(daily), trades, pd.DataFrame(monthly)


def _code_provenance():
    paths = {Path(__file__).resolve(), Path(rules.__file__).resolve(),
        Path(__file__).resolve().parents[1] / "tools/g3_trade_monitor_backtest.py"}
    return {**history._provenance(), **{str(path): history.sha256(path) for path in paths}}


def _write_report(output, summary, metrics, daily, trades, monthly):
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>G3 fixed advisory exit research</title><style>body{font:15px/1.5 system-ui,sans-serif;max-width:1450px;margin:28px auto;padding:0 20px;color:#243348;background:#f3f6fa}h1,h2{line-height:1.25}.note{background:#fff0cd;border-left:4px solid #ba8200;padding:16px}.scroll{overflow:auto;max-height:600px;margin-bottom:25px}table{border-collapse:collapse;background:white;width:100%}th,td{padding:9px;border-bottom:1px solid #ccd5df;text-align:right;white-space:nowrap}th{background:#223951;color:white;position:sticky;top:0}td:first-child,th:first-child{text-align:left}a{color:#075dae}</style></head><body><h1>G3: fixed advisory exit research</h1>'''
    page += f'<p>{escape(summary["first_date"])} through {escape(summary["last_date"])}; {summary["sessions"]} sessions; {summary["source_active_setup_count"]} source setups. Execution authority: none.</p>'
    page += '<p class="note"><strong>Historical resolution is one minute.</strong> The monitor display may refresh every 15 seconds using available observations. This research contains no invented 15-second bars or fills. No policy has been optimized or promoted into PAPER/LIVE.</p>'
    page += '<p>All policies retain original targets, 1.25% stops tightening to 1% after 120 minutes, and 15:15 square-off. An advisory uses only completed full post-entry bars and fills at the following available minute open. Only a native exit at or before that open has priority. Later intrabar stop/target touches cannot cancel a pending open exit. Capital remains reserved through the fill bar end; this is a capital convention, not a measured execution delay. Pending advisory market orders use the observed open even across a native target; simultaneous target order priority, spread, and latency require sensitivity checks. The unchanged native baseline retains its stop-first intrabar assumptions.</p>'
    page += '<p>Trend failure: six contiguous completed bars, three consecutive adverse close changes, signed five-minute move ≤−0.15%, and signed move from entry ≤−0.30%. Profit giveback: post-entry maximum favorable excursion ≥0.50%, giveback ≥0.30 percentage points, and three adverse close changes. Combined exits on either fixed rule.</p>'
    page += '<p>Every selected native fill is replayed and capital is allocated afresh, including candidates rejected by the baseline book. Sizing is Rs100,000 per entry at five-times exposure, Rs1,000,000 portfolio capital, and five basis points round-trip cost. Extra slippage and broker-specific charges beyond that estimate are not modeled. Historical volume unavailable in the execution paths is omitted and is not needed by these price rules.</p>'
    page += f'<p class="note">The first 35 sessions ({summary["development_first_date"]}–{summary["development_last_date"]}) and original sessions 36-50 ({summary["evaluation_first_date"]}–{summary["evaluation_last_date"]}) form a monitor chronological check. The source strategy was selected using historical results; these are <strong>not untouched strategy holdout results</strong>. Later daily extensions are reported separately without changing that 35/15 split. October 1 remains excluded for incomplete immutable input.</p>'
    columns = ["policy", "executed_trades", "wins", "win_rate_pct", "net_pnl_rs", "net_pnl_delta_vs_baseline_rs",
        "max_drawdown_rs", "monitor_exit_count", "stopped_trades_improved", "stop_loss_saved_rs",
        "baseline_winners_cut", "winner_profit_reduced_rs", "newly_admitted", "newly_rejected"]
    labels = [("all", f"All {summary['sessions']} sessions"), ("development35", "First 35 sessions"),
              ("evaluation15", "Original sessions 36-50: monitor chronological check")]
    if summary.get('daily_extension_sessions', 0):
        labels.append(("daily_extensions", "Later daily extensions: fixed policies"))
    for period, label in labels:
        page += f'<h2>{label}</h2><div class="scroll">' + metrics.loc[metrics.period.eq(period), columns].to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    for label, frame in (("Monthly comparison", monthly), ("Daywise comparison", daily)):
        page += f'<h2>{label}</h2><div class="scroll">' + frame.to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    changed = trades.loc[trades.monitor_exit & trades.policy_executed,
        ["policy", "day", "setup_id", "tradingsymbol", "side", "monitor_decision_at_ist", "baseline_exit_reason",
         "policy_exit_at_ist", "baseline_net_pnl_rs", "policy_net_pnl_rs", "net_pnl_delta_rs", "baseline_winner_cut"]]
    page += '<h2>Executed advisory exits: benefits and costs</h2><p>Stopped trades improved and winners cut compare trades executed in both books. Admission/rejection effects are reported separately. Complete selected-order details remain in the CSV files.</p><div class="scroll">' + changed.to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    links = sorted(FILES - {"report.html"}) + ["manifest.json"]
    page += '<p>' + ' | '.join(f'<a href="{escape(name)}">{escape(name)}</a>' for name in links) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(source=DEFAULT_SOURCE, output_root=DEFAULT_OUTPUT_ROOT):
    root, source_dir, source_summary, _ = _resolve_source(source)
    output_root = Path(output_root).resolve()
    if output_root == root or output_root.is_relative_to(root):
        raise ValueError("Monitor research output must be separate from canonical G3 backtest artifacts")
    code_proof = _code_provenance()
    source_manifest_sha256 = history.sha256(source_dir / "manifest.json")
    config = configuration()
    days = [date.fromisoformat(value) for value in source_summary["days"]]
    orders = read_frozen_csv(source_dir / "selected_orders.csv")
    saved = read_frozen_csv(source_dir / "portfolio_trades.csv")
    paths, input_proof = _paths_for_source(root, orders, days, source_summary)
    fresh = history._simulate(orders, paths, days, history.contract)
    history._same_native(fresh, saved)
    history._same_allocation(fresh, saved)
    print(f"Monitor source baseline verified: {len(fresh)} selected orders, {int(fresh.portfolio_executed.sum())} executions", flush=True)
    ledgers, signals = evaluate_policies(fresh, paths)
    metrics, daily, trades, monthly = comparisons(ledgers, days)
    now = datetime.now(history.contract.IST)
    output = output_root / "runs" / str(days[-1]) / f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_fixed_advisory_exits"
    if output.exists():
        raise FileExistsError("An immutable monitor research run cannot be overwritten")
    summary = dict(schema_version=SCHEMA, version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        advisory_only=True, created_at_ist=now.isoformat(), first_date=str(days[0]), last_date=str(days[-1]),
        sessions=len(days), days=[str(day) for day in days], source_active_setup_count=source_summary["active_setup_count"],
        source_strategy_version=source_summary["strategy_version"], source_strategy_fingerprint=source_summary["strategy_fingerprint"],
        source=dict(run_dir=str(source_dir), root=str(root), manifest_sha256=source_manifest_sha256),
        baseline_native_parity=True, baseline_allocation_parity=True,
        development_first_date=str(days[0]), development_last_date=str(days[34]),
        evaluation_first_date=str(days[35]), evaluation_last_date=str(days[49]),
        daily_extension_sessions=max(0, len(days)-50),
        daily_extension_days=[str(day) for day in days[50:]],
        evaluation_label=config["evaluation_label"], historical_bar_resolution_seconds=60,
        dashboard_refresh_seconds=15, subminute_ohlc_fabricated=False,
        monitor_policies_optimized=False, configuration_promoted=False,
        policy_totals=metrics.loc[metrics.period.eq("all")].to_dict("records"),
        chronological_check=metrics.loc[metrics.period.eq("evaluation15")].to_dict("records"),
        input_proof=input_proof)
    if _code_provenance() != code_proof or history.sha256(source_dir / "manifest.json") != source_manifest_sha256:
        raise ValueError("Monitor source or execution code changed during the replay")
    (output / "ledgers").mkdir(parents=True)
    for policy, ledger in ledgers.items():
        ledger.to_csv(output / "ledgers" / (policy + ".csv"), index=False)
    for name, frame in dict(policy_results=metrics, daily_comparison=daily, trade_comparison=trades,
            signals=signals, monthly_results=monthly, selected_orders=orders).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    history.replay.atomic_json(output / "config.json", config)
    history.replay.atomic_json(output / "summary.json", summary)
    _write_report(output, summary, metrics, daily, trades, monthly)
    if _code_provenance() != code_proof:
        raise ValueError("Monitor execution code changed during artifact creation")
    history.replay.atomic_json(output / "manifest.json", dict(schema_version=SCHEMA, version=VERSION,
        scope="BACKTEST_ONLY", execution_authority=False, source=summary["source"], code_sha256=code_proof,
        files={name: history.sha256(output / name) for name in sorted(FILES)}))
    verified_package(output, output_root)
    pointer = dict(schema_version=POINTER_SCHEMA, role="advisory_exit_research", execution_authority=False,
        run_dir=output.relative_to(output_root).as_posix(), manifest_sha256=history.sha256(output / "manifest.json"),
        source_strategy_fingerprint=source_summary["strategy_fingerprint"], session_date=str(days[-1]),
        created_at_ist=summary["created_at_ist"])
    history.replay.atomic_json(output_root / "latest/current_run.json", pointer)
    print(metrics.loc[metrics.period.eq("all"), ["policy", "executed_trades", "win_rate_pct", "net_pnl_rs", "net_pnl_delta_vs_baseline_rs"]].to_string(index=False), flush=True)
    return output, summary


def verified_package(directory, root=None):
    directory = Path(directory).resolve()
    if root is not None and not directory.is_relative_to(Path(root).resolve()):
        raise ValueError("Monitor research run escaped its output root")
    manifest = _read(directory / "manifest.json")
    if (manifest.get("schema_version") != SCHEMA or manifest.get("version") != VERSION
            or manifest.get("execution_authority") is not False or set(manifest.get("files", {})) != FILES):
        raise ValueError("Invalid monitor research manifest")
    history._check_files(directory, manifest)
    summary = _read(directory / "summary.json")
    if (_read(directory / "config.json") != configuration() or summary.get("execution_authority") is not False
            or summary.get("advisory_only") is not True or summary.get("baseline_native_parity") is not True
            or summary.get("baseline_allocation_parity") is not True or summary.get("sessions", 0) < 50
            or summary.get("configuration_promoted") is not False
            or summary.get("historical_bar_resolution_seconds") != 60
            or summary.get("subminute_ohlc_fabricated") is not False):
        raise ValueError("Monitor research changed its fixed rules, evidence, or authority")
    proof = summary["source"]
    if manifest.get("source") != proof or history.sha256(Path(proof["run_dir"]) / "manifest.json") != proof["manifest_sha256"]:
        raise ValueError("Monitor historical source identity changed")
    source_summary, _ = history.verified_package(proof["run_dir"], proof["root"])
    if (source_summary["strategy_fingerprint"] != summary["source_strategy_fingerprint"]
            or source_summary["days"] != summary["days"]):
        raise ValueError("Monitor source calendar or strategy differs")
    days = [date.fromisoformat(value) for value in summary["days"]]
    ledgers = {policy: read_frozen_csv(directory / "ledgers" / (policy + ".csv")) for policy in POLICIES}
    source_ledger = read_frozen_csv(Path(proof["run_dir"]) / "portfolio_trades.csv")
    history._same_native(ledgers["baseline"], source_ledger)
    history._same_allocation(ledgers["baseline"], source_ledger)
    baseline_ids = set(ledgers["baseline"].sid)
    for policy, ledger in ledgers.items():
        if (set(ledger.sid) != baseline_ids or ledger.sid.duplicated().any()
                or not ledger.execution_authority.eq(False).all() or not ledger.monitor_policy.eq(policy).all()):
            raise ValueError("Monitor policy mixes identities or drops selected candidates")
        if not np.allclose(ledger.portfolio_gross_profit_rupees-ledger.portfolio_cost_rupees,
                           ledger.portfolio_net_profit_rupees, atol=1e-6, rtol=0):
            raise ValueError("Monitor gross/cost/net accounting mismatch")
        changed = ledger.loc[ledger.monitor_exit.eq(True)]
        if len(changed):
            decision = pd.to_datetime(changed.monitor_decision_at_ist, utc=True)
            modeled = pd.to_datetime(changed.exit_ts, utc=True)
            opened = pd.to_datetime(changed.monitor_open_observed_at_ist, utc=True)
            if (not modeled.sub(opened).eq(pd.Timedelta(minutes=1)).all()
                    or not opened.ge(decision).all()
                    or not pd.to_datetime(changed.exit_execution_ts, utc=True).equals(opened)):
                raise ValueError("Monitor used the same bar or fabricated subminute fills")
            original_times = changed.sid.map(source_ledger.set_index("sid").exit_ts)
            if not opened.lt(pd.to_datetime(original_times, utc=True)).all():
                raise ValueError("Monitor overrode an original same-or-earlier exit")
    expected = comparisons(ledgers, days)
    for name, frame in zip(("policy_results", "daily_comparison", "trade_comparison", "monthly_results"), expected):
        actual = read_frozen_csv(directory / (name + ".csv"))
        pd.testing.assert_frame_equal(actual, frame, check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    totals = history.replay.json_ready(expected[0].loc[expected[0].period.eq("all")].to_dict("records"))
    if totals != summary["policy_totals"]:
        # CSV decimal roundtrips can differ at the final binary digit.
        pd.testing.assert_frame_equal(pd.DataFrame(summary["policy_totals"]), pd.DataFrame(totals),
            check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    chronological = expected[0].loc[expected[0].period.eq("evaluation15")].to_dict("records")
    pd.testing.assert_frame_equal(pd.DataFrame(summary["chronological_check"]), pd.DataFrame(chronological),
        check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    return summary, manifest


def load_latest(output_root=DEFAULT_OUTPUT_ROOT):
    output_root = Path(output_root).resolve()
    pointer = _read(output_root / "latest/current_run.json")
    if pointer.get("schema_version") != POINTER_SCHEMA or pointer.get("execution_authority") is not False:
        raise ValueError("Invalid monitor research latest pointer")
    directory = history._child(output_root, pointer["run_dir"])
    if history.sha256(directory / "manifest.json") != pointer["manifest_sha256"]:
        raise ValueError("Monitor research latest manifest changed")
    summary, manifest = verified_package(directory, output_root)
    return dict(summary=summary, manifest=manifest, run_dir=str(directory), report_path=str(directory / "report.html"),
        policy_results=history.replay.json_ready(read_frozen_csv(directory / "policy_results.csv").to_dict("records")),
        daily_comparison=history.replay.json_ready(read_frozen_csv(directory / "daily_comparison.csv").to_dict("records")))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
        help="Canonical G3 result root/current pointer, or a sealed current G3 backtest run")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    directory, _ = run(args.source, args.output_root)
    print(directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
