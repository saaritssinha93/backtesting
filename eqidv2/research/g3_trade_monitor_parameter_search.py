"""Bounded, chronological parameter search for the G3 advisory exit monitor.

This module is deliberately isolated from PAPER and LIVE configuration.  It
uses the sealed G3 one-minute execution paths, locks a candidate using only the
first 35 sessions, and reads the final 15 sessions only after that lock exists.
The source strategy itself was fitted on this historical cohort, so the last
15 sessions are a monitor-parameter check rather than a fresh strategy holdout.
"""
from __future__ import annotations

import argparse
import hashlib
import heapq
from datetime import date, datetime
from html import escape
import json
import math
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
import g3_trade_monitor_rules as shared_rules
from research import g3_trade_monitor_backtest as fixed
from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv


VERSION = "G3-TRADE-MONITOR-PARAMETER-SEARCH-V2"
SCHEMA = "g3_trade_monitor_parameter_search_v2"
POINTER_SCHEMA = "g3_trade_monitor_parameter_search_pointer_v2"
DEFAULT_SOURCE = fixed.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_3_trade_monitor_parameter_search")
EXPECTED_SHARED_CONFIG = dict(refresh_seconds=15, max_price_age_seconds=120,
    minimum_bars=6, adverse_closes=3, trend_momentum_5m_pct=-.15,
    trend_entry_move_pct=-.30, profit_activation_pct=.50, profit_giveback_pct=.30)
EXTRA_SLIPPAGE_STRESS_BPS = 10.0
EXTRA_SLIPPAGE_PER_EXIT_RS = (history.contract.CAPITAL_PER_ENTRY_RS
    * history.contract.LEVERAGE * EXTRA_SLIPPAGE_STRESS_BPS / 10_000)
FILES = {
    "config.json", "summary.json", "selection_lock.json", "parameter_grid.csv",
    "coarse_results.csv", "finalist_grid.csv", "finalist_results.csv",
    "decision_features.csv", "evaluation_results.csv", "daily_comparison.csv",
    "trade_comparison.csv", "monthly_results.csv", "final_events.csv",
    "parity_checks.csv", "report.html", "ledgers/baseline.csv",
    "ledgers/locked_candidate.csv",
}


def _candidate_id(prefix, **values):
    def token(value):
        return f"{abs(int(round(float(value) * 100))):03d}"
    return prefix + "_" + "_".join(f"{key}{token(value)}" for key, value in values.items())


def _candidate_hash(candidate):
    payload = {key: value for key, value in candidate.items() if key != "candidate_sha256"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _finish_candidate(candidate):
    candidate = dict(candidate)
    candidate["candidate_sha256"] = _candidate_hash(candidate)
    return candidate


def candidate_registry():
    """Return the preregistered, bounded single-rule coarse grid."""
    candidates = []
    order = 0
    for adverse in (3, 4, 5):
        for momentum in (-.15, -.25, -.40):
            for entry in (-.30, -.50, -.70):
                order += 1
                candidates.append(_finish_candidate(dict(
                    candidate_id=_candidate_id("trend", a=adverse, m=momentum, e=entry),
                    grid_order=order, family="trend_failure", active_flags="TREND_FAILURE",
                    minimum_bars=6, trend_adverse_closes=adverse,
                    trend_momentum_5m_pct=momentum, trend_entry_move_pct=entry,
                    profit_adverse_closes=None, profit_activation_pct=None,
                    profit_activation_target_fraction=None,
                    profit_activation_floor_pct=None, profit_giveback_pct=None,
                )))
    # Iteration two extends only the conservative edge of the coarse trend
    # grid.  It tests whether waiting nearer the unchanged 1.25% initial stop
    # avoids the winner damage seen at -0.70%, without adding setup/side/time
    # degrees of freedom.
    for adverse in (3, 4, 5):
        for momentum in (-.50, -.60):
            for entry in (-.70, -.80, -.90, -1.00):
                order += 1
                candidates.append(_finish_candidate(dict(
                    candidate_id=_candidate_id("trend_refine", a=adverse, m=momentum, e=entry),
                    grid_order=order, family="trend_failure", active_flags="TREND_FAILURE",
                    minimum_bars=6, trend_adverse_closes=adverse,
                    trend_momentum_5m_pct=momentum, trend_entry_move_pct=entry,
                    profit_adverse_closes=None, profit_activation_pct=None,
                    profit_activation_target_fraction=None,
                    profit_activation_floor_pct=None, profit_giveback_pct=None,
                )))
    # Keep the current absolute rule as an explicit control.  The bounded
    # alternatives scale activation to each setup's heterogeneous target while
    # retaining a 0.50 percentage-point floor.
    order += 1
    candidates.append(_finish_candidate(dict(
        candidate_id="giveback_current_absolute", grid_order=order,
        family="profit_giveback", active_flags="PROFIT_GIVEBACK", minimum_bars=6,
        trend_adverse_closes=None, trend_momentum_5m_pct=None,
        trend_entry_move_pct=None, profit_adverse_closes=3,
        profit_activation_pct=.50, profit_activation_target_fraction=None,
        profit_activation_floor_pct=.50, profit_giveback_pct=.30,
    )))
    for adverse in (3, 4):
        for fraction in (.60, .80):
            for giveback in (.40, .60, .80):
                order += 1
                candidates.append(_finish_candidate(dict(
                    candidate_id=_candidate_id("giveback_target", a=adverse, f=fraction, g=giveback),
                    grid_order=order, family="profit_giveback", active_flags="PROFIT_GIVEBACK",
                    minimum_bars=6, trend_adverse_closes=None,
                    trend_momentum_5m_pct=None, trend_entry_move_pct=None,
                    profit_adverse_closes=adverse, profit_activation_pct=None,
                    profit_activation_target_fraction=fraction,
                    profit_activation_floor_pct=.50, profit_giveback_pct=giveback,
                )))
    ids = [item["candidate_id"] for item in candidates]
    hashes = [item["candidate_sha256"] for item in candidates]
    if len(ids) != len(set(ids)) or len(hashes) != len(set(hashes)):
        raise ValueError("Parameter grid contains duplicate candidates")
    return candidates


def combined_candidate(trend, giveback, order):
    if trend["family"] != "trend_failure" or giveback["family"] != "profit_giveback":
        raise ValueError("Combined candidate requires one candidate from each rule family")
    return _finish_candidate(dict(
        candidate_id=f"combined__{trend['candidate_id']}__{giveback['candidate_id']}",
        grid_order=int(order), family="combined",
        active_flags="PROFIT_GIVEBACK|TREND_FAILURE", minimum_bars=6,
        trend_adverse_closes=int(trend["trend_adverse_closes"]),
        trend_momentum_5m_pct=float(trend["trend_momentum_5m_pct"]),
        trend_entry_move_pct=float(trend["trend_entry_move_pct"]),
        profit_adverse_closes=int(giveback["profit_adverse_closes"]),
        profit_activation_pct=(None if giveback["profit_activation_pct"] is None
            else float(giveback["profit_activation_pct"])),
        profit_activation_target_fraction=(None
            if giveback["profit_activation_target_fraction"] is None
            else float(giveback["profit_activation_target_fraction"])),
        profit_activation_floor_pct=float(giveback["profit_activation_floor_pct"]),
        profit_giveback_pct=float(giveback["profit_giveback_pct"]),
    ))


def configuration():
    if shared_rules.MONITOR_CONFIG != EXPECTED_SHARED_CONFIG:
        raise ValueError("Shared G3 monitor configuration changed during isolated parameter research")
    return dict(
        version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        decision_role="ADVISORY_EXIT_PARAMETER_RESEARCH_ONLY",
        modify_live_or_paper=False, configuration_promoted=False,
        source_strategy_fitted_on_historical_cohort=True, fresh_strategy_holdout=False,
        shared_rules_version=shared_rules.RULES_VERSION,
        shared_rule_parameters=EXPECTED_SHARED_CONFIG,
        search=dict(
            method="BOUNDED_TWO_STAGE_CHRONOLOGICAL_SEARCH",
            coarse_candidates=len(candidate_registry()),
            coarse_families=["trend_failure", "profit_giveback"],
            iteration_two="CONSERVATIVE_TREND_BOUNDARY_REFINEMENT_ONLY",
            calibration_sessions=25, validation_sessions=10,
            locked_evaluation_sessions=15, finalists_per_family=3,
            combined_finalists="PAIRWISE_3_BY_3_FROM_CALIBRATION_SHORTLISTS",
            objective="MAXIMIZE_10BPS_STRESSED_REALLOCATED_NET_PNL_DELTA",
            tie_break=["development35_stress_delta", "validation10_stress_delta",
                       "development35_loss_reduction", "candidate_id"],
            minimum_monitor_exits=dict(calibration25=8, validation10=3,
                                       development35=12, evaluation15=5),
            constraints=["win_rate_at_least_70_pct", "raw_net_pnl_delta_positive",
                "10bps_stressed_net_pnl_delta_positive", "realized_loss_magnitude_reduced",
                "daily_realized_drawdown_not_worse", "losing_day_count_not_higher",
                "no_baseline_execution_displaced"],
        ),
        parameter_grid=dict(
            minimum_bars=[6], trend_adverse_closes=[3, 4, 5],
            trend_momentum_5m_pct=[-.15, -.25, -.40],
            trend_entry_move_pct=[-.30, -.50, -.70],
            conservative_trend_refinement=dict(adverse_closes=[3, 4, 5],
                momentum_5m_pct=[-.50, -.60], entry_move_pct=[-.70, -.80, -.90, -1.00]),
            current_profit_control=dict(adverse_closes=3, activation_pct=.50,
                                        giveback_pct=.30),
            target_scaled_profit_grid=dict(adverse_closes=[3, 4],
                activation_target_fraction=[.60, .80], activation_floor_pct=.50,
                giveback_pct=[.40, .60, .80]),
        ),
        historical_bar_resolution_seconds=60, subminute_ohlc_fabricated=False,
        execution_model="DECIDE_ON_COMPLETED_BAR_FILL_AT_NEXT_AVAILABLE_MINUTE_OPEN_WITH_BAR_END_TIMESTAMP_PROXY",
        original_exit_precedence=fixed.configuration()["original_exit_precedence"],
        pending_open_order_convention=fixed.configuration()["pending_open_order_convention"],
        target_gap_convention=fixed.configuration()["target_gap_convention"],
        timer_and_capital_convention="CONSERVATIVE_FULL_NEXT_BAR_RESERVATION",
        extra_exit_slippage_stress_bps=EXTRA_SLIPPAGE_STRESS_BPS,
        capital_per_entry_rs=history.contract.CAPITAL_PER_ENTRY_RS,
        leverage=history.contract.LEVERAGE,
        portfolio_capital_rs=history.contract.PORTFOLIO_CAPITAL_RS,
        round_trip_cost_bps=history.contract.ROUND_TRIP_COST_BPS,
    )


def fingerprint():
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
        separators=(",", ":")).encode()).hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _periods(days):
    return {"calibration25": days[:25], "validation10": days[25:35], **fixed._periods(days)}


FEATURE_COLUMNS = [
    "sid", "source_signal_id", "day", "setup_id", "tradingsymbol", "side",
    "decision_at_ist", "completed_bar_count", "consecutive_adverse_closes",
    "momentum_5m_pct", "signed_move_pct", "mfe_pct", "giveback_pct",
    "next_bar_open_at_ist", "modeled_execution_at_ist", "execution_path_index",
    "next_bar_open_price", "original_exit_at_ist", "native_target_pct",
]


def decision_features(row, path):
    """Build causal threshold-independent decision rows for one native fill."""
    if not bool(row.filled):
        return []
    entry_index = int(row.entry_path_index)
    entry_at = fixed._timestamp(row.entry_ts)
    original_exit_at = fixed._timestamp(row.exit_ts)
    entry = float(row.entry_price)
    sign = 1 if row.side == "LONG" else -1
    completed = []
    favorable = [0.0]
    rows = []
    invalid_seen = False
    for index in range(entry_index + 1, len(path["timestamp_ns"]) - 1):
        bar = fixed._bar(path, index)
        decision_at = bar["timestamp"]
        if original_exit_at <= decision_at:
            break
        if decision_at - pd.Timedelta(minutes=1) < entry_at:
            continue
        values = [bar[key] for key in ("open", "high", "low", "close")]
        valid = (all(np.isfinite(value) and value > 0 for value in values)
            and bar["low"] <= min(bar["open"], bar["close"])
            <= max(bar["open"], bar["close"]) <= bar["high"])
        if not valid:
            invalid_seen = True
            continue
        completed.append(bar)
        favorable.append(sign * (bar["high" if sign == 1 else "low"] / entry - 1) * 100)
        if invalid_seen or len(completed) < 6:
            continue
        recent = completed[-6:]
        if any((recent[i]["timestamp"] - recent[i - 1]["timestamp"]).total_seconds() != 60
               for i in range(1, 6)):
            continue
        closes = [item["close"] for item in recent]
        changes = [sign * (closes[i] / closes[i - 1] - 1) * 100 for i in range(1, 6)]
        streak = 0
        for change in reversed(changes):
            if change >= 0:
                break
            streak += 1
        move = sign * (bar["close"] / entry - 1) * 100
        momentum = sign * (closes[-1] / closes[-6] - 1) * 100
        mfe = max(max(favorable), move)
        execution_index = index + 1
        execution_at = pd.Timestamp(int(path["timestamp_ns"][execution_index]),
            tz="UTC").tz_convert(history.contract.IST)
        open_at = execution_at - pd.Timedelta(minutes=1)
        if original_exit_at <= open_at:
            continue
        rows.append(dict(
            sid=int(row.sid), source_signal_id=row.signal_id, day=str(row.day),
            setup_id=row.setup_id, tradingsymbol=row.tradingsymbol, side=row.side,
            decision_at_ist=decision_at.isoformat(), completed_bar_count=len(completed),
            consecutive_adverse_closes=streak, momentum_5m_pct=momentum,
            signed_move_pct=move, mfe_pct=mfe, giveback_pct=max(0.0, mfe - move),
            next_bar_open_at_ist=open_at.isoformat(),
            modeled_execution_at_ist=execution_at.isoformat(), execution_path_index=execution_index,
            next_bar_open_price=float(path["open"][execution_index]),
            original_exit_at_ist=original_exit_at.isoformat(),
            native_target_pct=float(row.native_target_pct),
        ))
    return rows


def build_feature_cache(baseline, paths):
    rows = []
    for number, row in enumerate(baseline.itertuples(index=False), 1):
        rows.extend(decision_features(row, paths[int(row.sid)]))
        if number % 100 == 0:
            print(f"Decision features: {number}/{len(baseline)} selected orders", flush=True)
    frame = pd.DataFrame(rows, columns=FEATURE_COLUMNS)
    if len(frame):
        frame = frame.sort_values(["sid", "execution_path_index"], kind="stable").reset_index(drop=True)
    return frame


class DecisionCache:
    def __init__(self, frame):
        self.frame = frame.reset_index(drop=True)
        self.sid = self.frame.sid.to_numpy(np.int64)
        self.day = self.frame.day.astype(str).to_numpy()
        self.streak = self.frame.consecutive_adverse_closes.to_numpy(np.int64)
        self.momentum = self.frame.momentum_5m_pct.to_numpy(float)
        self.move = self.frame.signed_move_pct.to_numpy(float)
        self.mfe = self.frame.mfe_pct.to_numpy(float)
        self.giveback = self.frame.giveback_pct.to_numpy(float)
        self.native_target = self.frame.native_target_pct.to_numpy(float)

    def _masks(self, candidate):
        trend = np.zeros(len(self.frame), dtype=bool)
        profit = np.zeros(len(self.frame), dtype=bool)
        if candidate["family"] in {"trend_failure", "combined"}:
            trend = ((self.streak >= int(candidate["trend_adverse_closes"]))
                & (self.momentum <= float(candidate["trend_momentum_5m_pct"]))
                & (self.move <= float(candidate["trend_entry_move_pct"])))
        if candidate["family"] in {"profit_giveback", "combined"}:
            if candidate["profit_activation_target_fraction"] is None:
                activation = np.full(len(self.frame), float(candidate["profit_activation_pct"]))
                activation_pass = self.mfe >= activation
                giveback_pass = self.giveback >= float(candidate["profit_giveback_pct"])
            else:
                activation = np.maximum(float(candidate["profit_activation_floor_pct"]),
                    float(candidate["profit_activation_target_fraction"]) * self.native_target)
                # Preserve mathematically inclusive percentage boundaries when
                # decimal products such as 0.8 * 0.75 land one ULP above 0.60.
                activation_pass = self.mfe >= activation - 1e-12
                giveback_pass = self.giveback >= float(candidate["profit_giveback_pct"]) - 1e-12
            profit = ((self.streak >= int(candidate["profit_adverse_closes"]))
                & activation_pass & giveback_pass)
        return trend, profit

    def events(self, candidate, *, days=None):
        trend, profit = self._masks(candidate)
        mask = trend | profit
        if days is not None:
            mask &= np.isin(self.day, [str(day) for day in days])
        indices = np.flatnonzero(mask)
        if not len(indices):
            return []
        indices = indices[np.r_[True, self.sid[indices][1:] != self.sid[indices][:-1]]]
        events = []
        for index in indices:
            row = self.frame.iloc[int(index)]
            flags = []
            if trend[index]:
                flags.append("TREND_FAILURE")
            if profit[index]:
                flags.append("PROFIT_GIVEBACK")
            events.append(dict(
                sid=int(row.sid), source_signal_id=row.source_signal_id, day=str(row.day),
                setup_id=row.setup_id, tradingsymbol=row.tradingsymbol, side=row.side,
                decision_at_ist=row.decision_at_ist, completed_bar_count=int(row.completed_bar_count),
                flags=json.dumps(sorted(flags)), next_bar_open_at_ist=row.next_bar_open_at_ist,
                modeled_execution_at_ist=row.modeled_execution_at_ist,
                execution_path_index=int(row.execution_path_index),
                next_bar_open_price=float(row.next_bar_open_price),
                original_exit_at_ist=row.original_exit_at_ist, eligible_for_monitor_exit=True,
                decision="MONITOR_CANDIDATE",
            ))
        return events

    def signature(self, candidate, days):
        return tuple((item["sid"], item["execution_path_index"], item["flags"])
            for item in self.events(candidate, days=days))


def _heap_accept(entries, exits, ordered_indices, capacity=10):
    accepted, active = [], []
    for index in ordered_indices:
        entry = entries[index]
        while active and active[0] <= entry:
            heapq.heappop(active)
        if len(active) < capacity:
            accepted.append(index)
            heapq.heappush(active, exits[index])
    return np.asarray(accepted, dtype=np.int64)


class FastBook:
    """Exact fast allocator for the pinned fixed-capital G3 policy."""
    def __init__(self, baseline):
        settings = history.simulator.g.v9.V9Config(
            portfolio_capital_rupees=history.contract.PORTFOLIO_CAPITAL_RS,
            capital_per_entry_rupees=history.contract.CAPITAL_PER_ENTRY_RS,
            leverage_factor=history.contract.LEVERAGE, max_positions=history.contract.MAX_POSITIONS,
            cost_bps=history.contract.ROUND_TRIP_COST_BPS)
        limits = settings.portfolio_config()
        capacity = int(limits.portfolio_capital_rupees / settings.capital_per_entry_rupees)
        if (capacity != 10 or limits.max_positions not in (None, 10)
                or any(getattr(limits, name) is not None for name in
                       ("max_positions_per_symbol", "max_gross_exposure_rupees", "max_open_risk_rupees"))):
            raise ValueError("Fast search supports only the pinned ten-slot G3 capital policy")
        self.capacity = capacity
        self.frame = baseline.reset_index(drop=True)
        if self.frame.sid.duplicated().any():
            raise ValueError("Baseline candidate IDs are duplicated")
        self.sid_to_index = {int(sid): index for index, sid in enumerate(self.frame.sid)}
        self.filled = self.frame.filled.eq(True).to_numpy(bool)
        self.day = self.frame.day.astype(str).to_numpy()
        self.entry = pd.to_datetime(self.frame.entry_ts, utc=True).astype("int64").to_numpy()
        self.base_exit = pd.to_datetime(self.frame.exit_ts, utc=True).astype("int64").to_numpy()
        self.base_gross = pd.to_numeric(self.frame.gross_return_pct).to_numpy(float) * 5000.0
        self.base_net = pd.to_numeric(self.frame.net_return_pct).to_numpy(float) * 5000.0
        self.base_cost = pd.to_numeric(self.frame.cost_pct).to_numpy(float) * 5000.0
        priority = history.simulator.g.v9.v6.prepare_source_ledger(self.frame)
        priority["_confirmation"] = priority.confirmation_ts.fillna(priority.entry_ts)
        priority["_setup"] = priority.setup_id.fillna("").astype(str)
        self.order = priority.loc[self.filled].sort_values(
            ["entry_ts", "_confirmation", "_setup", "portfolio_priority_value", "tradingsymbol", "sid"],
            ascending=[True, True, True, False, True, True], kind="stable").index.to_numpy(np.int64)
        self.baseline = self.outcome([])
        expected = self.frame.portfolio_executed.eq(True).to_numpy(bool)
        if not np.array_equal(self.baseline["accepted"], expected):
            raise ValueError("Fast baseline allocator differs from canonical G3 allocation")
        expected_pnl = pd.to_numeric(self.frame.portfolio_net_profit_rupees).to_numpy(float)
        actual_pnl = np.where(expected, self.base_net, 0.0)
        if not np.allclose(actual_pnl, expected_pnl, atol=1e-6, rtol=0):
            raise ValueError("Fast baseline P&L differs from canonical G3 allocation")

    def outcome(self, events):
        exits, gross, net, cost = (self.base_exit.copy(), self.base_gross.copy(),
            self.base_net.copy(), self.base_cost.copy())
        modified = np.zeros(len(self.frame), dtype=bool)
        for event in events:
            index = self.sid_to_index[int(event["sid"])]
            if not self.filled[index]:
                raise ValueError("Monitor event references an unfilled order")
            modeled = pd.Timestamp(event["modeled_execution_at_ist"]).tz_convert("UTC").value
            opened = pd.Timestamp(event["next_bar_open_at_ist"]).tz_convert("UTC").value
            decision = pd.Timestamp(event["decision_at_ist"]).tz_convert("UTC").value
            if (modeled - opened != fixed.MINUTE_NS or opened < decision
                    or opened >= self.base_exit[index]):
                raise ValueError("Monitor open event did not follow its decision and precede the original exit")
            direction = 1 if self.frame.iloc[index].side == "LONG" else -1
            entry = float(self.frame.iloc[index].entry_price)
            raw = direction * (float(event["next_bar_open_price"]) / entry - 1) * 100
            exits[index], gross[index] = modeled, raw * 5000.0
            cost[index], net[index] = history.contract.ROUND_TRIP_COST_BPS / 100 * 5000.0, (raw - history.contract.ROUND_TRIP_COST_BPS / 100) * 5000.0
            modified[index] = True
        accepted = np.zeros(len(self.frame), dtype=bool)
        accepted[_heap_accept(self.entry, exits, self.order, self.capacity)] = True
        return dict(exit=exits, gross=gross, net=net, cost=cost,
                    modified=modified, accepted=accepted)

    def metrics(self, outcome, days, baseline=None):
        day_values = [str(day) for day in days]
        in_period = np.isin(self.day, day_values)
        executed = outcome["accepted"] & in_period
        pnl = outcome["net"][executed]
        daily = np.asarray([outcome["net"][outcome["accepted"] & self.day.__eq__(day)].sum()
                            for day in day_values], dtype=float)
        curve = np.r_[0.0, daily.cumsum()]
        high = np.maximum.accumulate(curve)
        monitor = outcome["modified"] & outcome["accepted"] & in_period
        result = dict(
            sessions=len(days), selected_orders=int(in_period.sum()),
            native_fills=int((self.filled & in_period).sum()),
            executed_trades=int(executed.sum()), wins=int((pnl > 0).sum()),
            losses=int((pnl < 0).sum()),
            win_rate_pct=100 * float((pnl > 0).mean()) if len(pnl) else None,
            gross_pnl_rs=float(outcome["gross"][executed].sum()),
            costs_rs=float(outcome["cost"][executed].sum()), net_pnl_rs=float(pnl.sum()),
            realized_loss_rs=float(-pnl[pnl < 0].sum()),
            average_loss_rs=float(-pnl[pnl < 0].mean()) if (pnl < 0).any() else 0.0,
            max_drawdown_rs=float((high-curve).max()), worst_day_pnl_rs=float(daily.min()),
            profitable_days=int((daily > 0).sum()), losing_days=int((daily < 0).sum()),
            monitor_exit_count=int(monitor.sum()),
            stress_10bps_net_pnl_rs=float(pnl.sum() - monitor.sum() * EXTRA_SLIPPAGE_PER_EXIT_RS),
        )
        if baseline is None:
            result.update(net_pnl_delta_vs_baseline_rs=0.0, realized_loss_reduction_rs=0.0,
                stress_10bps_net_pnl_delta_vs_baseline_rs=0.0, newly_admitted=0, newly_rejected=0,
                stopped_trades_improved=0, stop_loss_saved_rs=0.0, stopped_trades_worsened=0,
                baseline_winners_cut=0, winner_profit_reduced_rs=0.0,
                winners_turned_nonpositive=0)
            return result
        original = self.metrics(baseline, days, None)
        base_exec = baseline["accepted"] & in_period
        common = base_exec & outcome["accepted"] & outcome["modified"]
        delta = outcome["net"] - baseline["net"]
        stopped = self.frame.exit_reason.isin({"STOP", "TIGHTENED_STOP"}).to_numpy(bool)
        improved = common & stopped & (delta > 0)
        worsened = common & stopped & (delta < 0)
        cut = common & (baseline["net"] > 0) & (delta < 0)
        turned = cut & (outcome["net"] <= 0)
        result.update(
            net_pnl_delta_vs_baseline_rs=result["net_pnl_rs"]-original["net_pnl_rs"],
            realized_loss_reduction_rs=original["realized_loss_rs"]-result["realized_loss_rs"],
            stress_10bps_net_pnl_delta_vs_baseline_rs=(result["stress_10bps_net_pnl_rs"]-original["net_pnl_rs"]),
            newly_admitted=int((outcome["accepted"] & ~baseline["accepted"] & in_period).sum()),
            newly_rejected=int((~outcome["accepted"] & baseline["accepted"] & in_period).sum()),
            stopped_trades_improved=int(improved.sum()),
            stop_loss_saved_rs=float(np.maximum(delta[improved], 0).sum()),
            stopped_trades_worsened=int(worsened.sum()),
            baseline_winners_cut=int(cut.sum()),
            winner_profit_reduced_rs=float((-delta[cut]).sum()),
            winners_turned_nonpositive=int(turned.sum()),
        )
        return result


def _with_identity(candidate, period, metrics):
    return {"candidate_id": candidate["candidate_id"], "candidate_sha256": candidate["candidate_sha256"],
        "family": candidate["family"], "period": period, **metrics}


def _qualification_failures(row, period):
    minimum = dict(calibration25=8, validation10=3, development35=12, evaluation15=5)[period]
    failures = []
    if int(row["monitor_exit_count"]) < minimum:
        failures.append(f"monitor_exits<{minimum}")
    if 10 * int(row["wins"]) < 7 * int(row["executed_trades"]):
        failures.append("win_rate<70")
    if not float(row["net_pnl_delta_vs_baseline_rs"]) > 0:
        failures.append("raw_net_delta<=0")
    if not float(row["stress_10bps_net_pnl_delta_vs_baseline_rs"]) > 0:
        failures.append("stress10_net_delta<=0")
    if not float(row["realized_loss_reduction_rs"]) > 0:
        failures.append("realized_loss_not_reduced")
    if float(row["max_drawdown_rs"]) > float(row["baseline_max_drawdown_rs"]) + 1e-6:
        failures.append("drawdown_worse")
    if int(row["losing_days"]) > int(row["baseline_losing_days"]):
        failures.append("more_losing_days")
    if int(row["newly_rejected"]) > 0:
        failures.append("baseline_execution_displaced")
    return failures


def _attach_baseline(metrics, baseline_metrics):
    return {**metrics,
        "baseline_net_pnl_rs": baseline_metrics["net_pnl_rs"],
        "baseline_realized_loss_rs": baseline_metrics["realized_loss_rs"],
        "baseline_max_drawdown_rs": baseline_metrics["max_drawdown_rs"],
        "baseline_losing_days": baseline_metrics["losing_days"],
        "qualified": len(_qualification_failures({**metrics,
            "baseline_max_drawdown_rs": baseline_metrics["max_drawdown_rs"]},
            "calibration25")) == 0,
    }


def _shortlist(coarse, candidates, cache, calibration_days, count=3):
    lookup = {item["candidate_id"]: item for item in candidates}
    chosen = []
    for family in ("trend_failure", "profit_giveback"):
        rows = coarse.loc[(coarse.family == family) & (coarse.monitor_exit_count >= 3)].copy()
        rows = rows.sort_values(["stress_10bps_net_pnl_delta_vs_baseline_rs",
            "realized_loss_reduction_rs", "net_pnl_delta_vs_baseline_rs", "candidate_id"],
            ascending=[False, False, False, True], kind="stable")
        signatures = set()
        for row in rows.itertuples(index=False):
            candidate = lookup[row.candidate_id]
            signature = cache.signature(candidate, calibration_days)
            if signature in signatures:
                continue
            signatures.add(signature)
            chosen.append(candidate)
            if sum(item["family"] == family for item in chosen) == count:
                break
    return chosen


def _choose_finalist(results, candidates):
    lookup = {item["candidate_id"]: item for item in candidates}
    qualified, failures = [], {}
    for candidate_id, part in results.groupby("candidate_id", sort=False):
        by_period = {row.period: row._asdict() for row in part.itertuples(index=False)}
        current = []
        for period in ("calibration25", "validation10", "development35"):
            current.extend(f"{period}:{value}" for value in _qualification_failures(by_period[period], period))
        failures[candidate_id] = current
        if not current:
            qualified.append(candidate_id)
    development = results.loc[results.period.eq("development35")].set_index("candidate_id")
    validation = results.loc[results.period.eq("validation10")].set_index("candidate_id")
    observed = sorted(development.index, key=lambda key: (
        -float(development.loc[key, "stress_10bps_net_pnl_delta_vs_baseline_rs"]),
        -float(validation.loc[key, "stress_10bps_net_pnl_delta_vs_baseline_rs"]),
        -float(development.loc[key, "realized_loss_reduction_rs"]), key))
    best_observed = observed[0] if observed else None
    eligible = [key for key in observed if key in qualified]
    # Even when no candidate clears the deployment-quality gates, lock the
    # best observed development candidate before reading the final period.  It
    # remains diagnostic only; recommendation status separately retains the
    # baseline unless both development and evaluation pass.
    locked = eligible[0] if eligible else best_observed
    return dict(locked_candidate_id=locked,
        locked_candidate_sha256=(lookup[locked]["candidate_sha256"] if locked is not None else None),
        parameter_change_locked=locked is not None,
        development_qualified=bool(eligible), best_observed_candidate_id=best_observed,
        qualified_candidate_ids=eligible,
        lock_reason=("BEST_DEVELOPMENT_CANDIDATE_PASSING_ALL_CALIBRATION_VALIDATION_AND_DEVELOPMENT_CONSTRAINTS"
            if eligible else "NO_CANDIDATE_PASSED_PREREGISTERED_DEVELOPMENT_CONSTRAINTS_BEST_OBSERVED_LOCKED_FOR_DIAGNOSTIC_EVALUATION_ONLY"),
        qualification_failures=failures)


def _canonical_ledger(baseline, paths, candidate, events, policy_name):
    raw = baseline.copy()
    raw["monitor_exit"] = False
    raw["monitor_flags"] = "[]"
    raw["monitor_decision_at_ist"] = ""
    raw["monitor_open_observed_at_ist"] = ""
    by_sid = {int(sid): index for index, sid in raw.sid.items()}
    for event in events:
        fixed._apply_monitor_exit(raw, by_sid[int(event["sid"])], event,
                                  paths[int(event["sid"])], policy_name)
    ledger = fixed._allocate(raw, policy_name)
    candidate_hash = candidate["candidate_sha256"] if candidate is not None else "BASELINE"
    ledger["strategy_version"] = VERSION
    ledger["strategy_fingerprint"] = fingerprint()
    ledger["strategy"] = "G3_TRADE_MONITOR_PARAMETER_SEARCH"
    ledger["monitor_policy"] = policy_name
    ledger["search_candidate_id"] = candidate["candidate_id"] if candidate is not None else "baseline"
    ledger["search_candidate_sha256"] = candidate_hash
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    return ledger


def _assert_fast_parity(book, outcome, ledger, label):
    accepted = ledger.portfolio_executed.eq(True).to_numpy(bool)
    if not np.array_equal(outcome["accepted"], accepted):
        raise ValueError(f"Fast allocator differs from canonical ledger: {label}")
    expected = pd.to_numeric(ledger.portfolio_net_profit_rupees).to_numpy(float)
    actual = np.where(outcome["accepted"], outcome["net"], 0.0)
    if not np.allclose(actual, expected, atol=1e-6, rtol=0):
        raise ValueError(f"Fast P&L differs from canonical ledger: {label}")


def _current_candidate(family):
    for item in candidate_registry():
        if (family == "trend_failure" and item["family"] == family
                and item["trend_adverse_closes"] == 3
                and item["trend_momentum_5m_pct"] == -.15
                and item["trend_entry_move_pct"] == -.30):
            return item
        if (family == "profit_giveback" and item["family"] == family
                and item["profit_adverse_closes"] == 3
                and item["profit_activation_pct"] == .50
                and item["profit_activation_target_fraction"] is None
                and item["profit_giveback_pct"] == .30):
            return item
    raise AssertionError("Current shared candidate is absent from the grid")


def _parity_checks(baseline, paths, cache, book):
    trend, profit = _current_candidate("trend_failure"), _current_candidate("profit_giveback")
    combined = combined_candidate(trend, profit, 10_000)
    event_maps = {item["family"]: {event["sid"]: event for event in cache.events(item)}
                  for item in (trend, profit)}
    checked = 0
    for row in baseline.itertuples(index=False):
        slow, _ = fixed.trade_advisories(row, paths[int(row.sid)])
        for flag, family in (("TREND_FAILURE", "trend_failure"),
                             ("PROFIT_GIVEBACK", "profit_giveback")):
            fast_event = event_maps[family].get(int(row.sid))
            slow_event = slow.get(flag)
            if (fast_event is None) != (slow_event is None):
                raise ValueError(f"Feature cache differs from shared rule for sid={row.sid} {flag}")
            if fast_event is not None:
                if (fast_event["execution_path_index"] != slow_event["execution_path_index"]
                        or not math.isclose(fast_event["next_bar_open_price"],
                                            slow_event["next_bar_open_price"], rel_tol=0, abs_tol=1e-10)):
                    raise ValueError(f"Feature cache timing differs for sid={row.sid} {flag}")
                checked += 1
    rows = [dict(check="feature_cache_vs_shared_rule_first_events", passed=True,
                 observations=checked, detail="All current fixed first events match")]
    for candidate in (trend, profit, combined):
        events = cache.events(candidate)
        outcome = book.outcome(events)
        ledger = _canonical_ledger(baseline, paths, candidate, events, "parity_"+candidate["family"])
        _assert_fast_parity(book, outcome, ledger, candidate["family"])
        rows.append(dict(check=f"fast_allocator_vs_canonical_{candidate['family']}", passed=True,
            observations=int(outcome["accepted"].sum()), detail="Accepted IDs and P&L match to Rs0.000001"))
    return pd.DataFrame(rows)


def _ledger_extended_metrics(ledger, baseline, days):
    day_values = [str(day) for day in days]
    ledger_period = ledger.loc[ledger.day.isin(day_values)]
    baseline_period = baseline.loc[baseline.day.isin(day_values)]
    actual = history._stats(ledger_period, days)
    original = history._stats(baseline_period, days)
    selected = ledger.loc[ledger.day.isin(day_values) & ledger.portfolio_executed.eq(True)]
    base = baseline.loc[baseline.day.isin(day_values) & baseline.portfolio_executed.eq(True)]
    pnl = pd.to_numeric(selected.portfolio_net_profit_rupees)
    base_pnl = pd.to_numeric(base.portfolio_net_profit_rupees)
    monitor_count = int(selected.monitor_exit.eq(True).sum())
    daily = ledger.groupby(ledger.day.astype(str)).portfolio_net_profit_rupees.sum().reindex(day_values, fill_value=0)
    actual.update(
        realized_loss_rs=float(-pnl[pnl < 0].sum()),
        average_loss_rs=float(-pnl[pnl < 0].mean()) if (pnl < 0).any() else 0.0,
        worst_day_pnl_rs=float(daily.min()), profitable_days=int((daily > 0).sum()),
        losing_days=int((daily < 0).sum()), monitor_exit_count=monitor_count,
        stress_10bps_net_pnl_rs=float(actual["net_pnl_rs"]-monitor_count*EXTRA_SLIPPAGE_PER_EXIT_RS),
        net_pnl_delta_vs_baseline_rs=float(actual["net_pnl_rs"]-original["net_pnl_rs"]),
        realized_loss_reduction_rs=float(-base_pnl[base_pnl < 0].sum() + pnl[pnl < 0].sum()),
        stress_10bps_net_pnl_delta_vs_baseline_rs=float(actual["net_pnl_rs"]
            - monitor_count*EXTRA_SLIPPAGE_PER_EXIT_RS-original["net_pnl_rs"]),
        baseline_net_pnl_rs=original["net_pnl_rs"],
        baseline_realized_loss_rs=float(-base_pnl[base_pnl < 0].sum()),
        baseline_max_drawdown_rs=original["max_drawdown_rs"],
        baseline_losing_days=int((baseline.groupby(baseline.day.astype(str))
            .portfolio_net_profit_rupees.sum().reindex(day_values, fill_value=0) < 0).sum()),
    )
    actual_by_sid = ledger.set_index("sid")
    base_by_sid = baseline.set_index("sid")
    period_ids = actual_by_sid.index[actual_by_sid.day.astype(str).isin(day_values)]
    actual_rows = actual_by_sid.loc[period_ids]
    base_rows = base_by_sid.loc[period_ids]
    actual_executed = actual_rows.portfolio_executed.eq(True)
    base_executed = base_rows.portfolio_executed.eq(True)
    changed = actual_rows.monitor_exit.eq(True)
    common = actual_executed & base_executed & changed
    delta = (pd.to_numeric(actual_rows.portfolio_net_profit_rupees)
        - pd.to_numeric(base_rows.portfolio_net_profit_rupees))
    stopped = base_rows.exit_reason.isin({"STOP", "TIGHTENED_STOP"})
    improved = common & stopped & delta.gt(0)
    worsened = common & stopped & delta.lt(0)
    cut = common & pd.to_numeric(base_rows.portfolio_net_profit_rupees).gt(0) & delta.lt(0)
    turned = cut & pd.to_numeric(actual_rows.portfolio_net_profit_rupees).le(0)
    actual.update(
        newly_admitted=int((actual_executed & ~base_executed).sum()),
        newly_rejected=int((~actual_executed & base_executed).sum()),
        stopped_trades_improved=int(improved.sum()),
        stop_loss_saved_rs=float(delta.loc[improved].clip(lower=0).sum()),
        stopped_trades_worsened=int(worsened.sum()),
        baseline_winners_cut=int(cut.sum()),
        winner_profit_reduced_rs=float((-delta.loc[cut]).sum()),
        winners_turned_nonpositive=int(turned.sum()),
    )
    return actual


def _code_provenance():
    paths = {Path(__file__).resolve(), Path(fixed.__file__).resolve(),
        Path(shared_rules.__file__).resolve(),
        Path(__file__).resolve().parents[1] / "tools/g3_trade_monitor_parameter_search.py"}
    return {**history._provenance(), **{str(path): history.sha256(path) for path in paths}}


def _write_report(output, summary, lock, coarse, finalists, evaluation, daily):
    page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 monitor parameter search</title><style>body{font:15px/1.5 system-ui,sans-serif;max-width:1500px;margin:28px auto;padding:0 20px;color:#243348;background:#f3f6fa}h1,h2{line-height:1.25}.note{background:#fff0cd;border-left:4px solid #ba8200;padding:16px}.good{background:#e8f6ed;border-left:4px solid #21834b;padding:16px}.scroll{overflow:auto;max-height:650px;margin-bottom:25px}table{border-collapse:collapse;background:white;width:100%}th,td{padding:8px;border-bottom:1px solid #ccd5df;text-align:right;white-space:nowrap}th{background:#223951;color:white;position:sticky;top:0}td:first-child,th:first-child{text-align:left}a{color:#075dae}</style></head><body>'''
    page += '<h1>G3 trade monitor: bounded chronological parameter search</h1>'
    page += f'<p>{summary["first_date"]} through {summary["last_date"]}; {summary["sessions"]} sessions. Execution authority: none.</p>'
    page += '<p class="note"><strong>Research only.</strong> No PAPER, LIVE, strategy target, stop, session, Python execution, or batch configuration was changed. The source strategy was fitted on this historical cohort, so original sessions 36-50 are a chronological monitor-parameter check, not a fresh strategy holdout. Later daily extensions are reported separately and do not enter parameter selection.</p>'
    page += '<p>The search used genuine completed one-minute bars and pending market exits at the following available minute open. Only native exits at or before the actual open have priority; later intrabar extremes cannot cancel a pending exit. The full ten-slot portfolio is reallocated while reserving capital through the fill bar end. A pending advisory uses the observed open even across a native target. The unchanged native baseline retains its stop-first intrabar assumptions; simultaneous target order priority and execution latency remain modeling limits. Costs remain 5 bps round trip, plus a 10 bps adverse advisory-exit slippage stress used in selection.</p>'
    status = summary["recommendation_status"]
    klass = "good" if status == "RESEARCH_CANDIDATE_PASSED_CHRONOLOGICAL_CHECK" else "note"
    page += f'<p class="{klass}"><strong>{escape(status)}</strong><br>Locked candidate: {escape(lock["locked_candidate_id"])}<br>{escape(lock["lock_reason"])}</p>'
    page += '<h2>Locked candidate parameters</h2><pre>' + escape(json.dumps(lock.get("locked_candidate"), indent=2)) + '</pre>'
    cols = ["candidate_id", "family", "period", "monitor_exit_count", "executed_trades", "wins", "losses", "win_rate_pct", "net_pnl_rs", "net_pnl_delta_vs_baseline_rs", "realized_loss_rs", "realized_loss_reduction_rs", "stress_10bps_net_pnl_delta_vs_baseline_rs", "max_drawdown_rs", "baseline_max_drawdown_rs"]
    page += '<h2>Locked evaluation</h2><div class="scroll">' + evaluation[[c for c in cols if c in evaluation]].to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    top = coarse.sort_values("stress_10bps_net_pnl_delta_vs_baseline_rs", ascending=False).head(25)
    page += f'<h2>Coarse calibration: top 25 of {len(coarse)}</h2><div class="scroll">' + top[[c for c in cols if c in top]].to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    page += '<h2>Development finalists</h2><div class="scroll">' + finalists[[c for c in cols if c in finalists]].to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    page += '<h2>Daywise locked comparison</h2><div class="scroll">' + daily.to_html(index=False, border=0, float_format=lambda x:f"{x:,.2f}") + '</div>'
    links = (FILES - {"report.html"}) | {"manifest.json"}
    page += '<p>' + ' | '.join(f'<a href="{escape(name)}">{escape(name)}</a>' for name in sorted(links)) + '</p></body></html>'
    (output / "report.html").write_text(page, encoding="utf-8")


def run(source=DEFAULT_SOURCE, output_root=DEFAULT_OUTPUT_ROOT):
    config = configuration()
    code_proof = _code_provenance()
    root, source_dir, source_summary, _ = fixed._resolve_source(source)
    output_root = Path(output_root).resolve()
    if output_root == root or output_root.is_relative_to(root):
        raise ValueError("Parameter search output must be separate from canonical G3 results")
    source_manifest_sha256 = history.sha256(source_dir / "manifest.json")
    days = [date.fromisoformat(value) for value in source_summary["days"]]
    periods = _periods(days)
    orders = read_frozen_csv(source_dir / "selected_orders.csv")
    saved = read_frozen_csv(source_dir / "portfolio_trades.csv")
    paths, input_proof = fixed._paths_for_source(root, orders, days, source_summary)
    baseline = history._simulate(orders, paths, days, history.contract)
    history._same_native(baseline, saved)
    history._same_allocation(baseline, saved)
    print(f"Search source verified: {len(baseline)} orders, {int(baseline.portfolio_executed.sum())} executions", flush=True)
    features = build_feature_cache(baseline, paths)
    cache, book = DecisionCache(features), FastBook(baseline)
    parity = _parity_checks(baseline, paths, cache, book)
    print(f"Causal feature and allocator parity passed: {len(parity)} checks", flush=True)

    candidates = candidate_registry()
    base_metrics = book.metrics(book.baseline, periods["calibration25"])
    coarse_rows, outcomes = [], {}
    for number, candidate in enumerate(candidates, 1):
        events = cache.events(candidate)
        outcome = book.outcome(events)
        outcomes[candidate["candidate_id"]] = outcome
        metrics = book.metrics(outcome, periods["calibration25"], book.baseline)
        metrics.update(baseline_net_pnl_rs=base_metrics["net_pnl_rs"],
            baseline_realized_loss_rs=base_metrics["realized_loss_rs"],
            baseline_max_drawdown_rs=base_metrics["max_drawdown_rs"],
            baseline_losing_days=base_metrics["losing_days"])
        metrics["qualified"] = not _qualification_failures(metrics, "calibration25")
        coarse_rows.append(_with_identity(candidate, "calibration25", metrics))
        if number % 20 == 0 or number == len(candidates):
            print(f"Coarse calibration: {number}/{len(candidates)} candidates", flush=True)
    coarse = pd.DataFrame(coarse_rows)
    shortlisted = _shortlist(coarse, candidates, cache, periods["calibration25"])
    trends = [item for item in shortlisted if item["family"] == "trend_failure"]
    givebacks = [item for item in shortlisted if item["family"] == "profit_giveback"]
    finalists = list(shortlisted)
    order = len(candidates)
    for trend in trends:
        for giveback in givebacks:
            order += 1
            finalists.append(combined_candidate(trend, giveback, order))
    # Remove behaviorally identical calibration candidates without using later sessions.
    unique, seen = [], set()
    for candidate in finalists:
        signature = cache.signature(candidate, periods["calibration25"])
        if signature not in seen:
            seen.add(signature)
            unique.append(candidate)
    finalists = unique
    finalist_rows = []
    for candidate in finalists:
        events = cache.events(candidate)
        outcome = outcomes.get(candidate["candidate_id"]) or book.outcome(events)
        outcomes[candidate["candidate_id"]] = outcome
        for period in ("calibration25", "validation10", "development35"):
            original = book.metrics(book.baseline, periods[period])
            metrics = book.metrics(outcome, periods[period], book.baseline)
            metrics.update(baseline_net_pnl_rs=original["net_pnl_rs"],
                baseline_realized_loss_rs=original["realized_loss_rs"],
                baseline_max_drawdown_rs=original["max_drawdown_rs"],
                baseline_losing_days=original["losing_days"])
            metrics["qualified"] = not _qualification_failures(metrics, period)
            finalist_rows.append(_with_identity(candidate, period, metrics))
    finalist_results = pd.DataFrame(finalist_rows)
    lock = _choose_finalist(finalist_results, finalists)
    candidate_lookup = {item["candidate_id"]: item for item in finalists}
    locked_candidate = candidate_lookup.get(lock["locked_candidate_id"])
    lock["locked_candidate"] = locked_candidate
    lock["selection_uses_sessions"] = [str(day) for day in periods["development35"]]
    lock["evaluation_sessions_not_read_for_selection"] = [str(day) for day in periods["evaluation15"]]
    lock["daily_extension_sessions_not_read_for_selection"] = [str(day) for day in periods.get("daily_extensions", [])]
    lock["locked_before_evaluation"] = True
    lock["scope"] = "BACKTEST_ONLY"
    lock["execution_authority"] = False
    lock["configuration_promoted"] = False

    now = datetime.now(history.contract.IST)
    output = output_root / "runs" / str(days[-1]) / f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_bounded_walkforward"
    if output.exists():
        raise FileExistsError("An immutable parameter-search run cannot be overwritten")
    (output / "ledgers").mkdir(parents=True)
    pd.DataFrame(candidates).to_csv(output / "parameter_grid.csv", index=False)
    coarse.to_csv(output / "coarse_results.csv", index=False)
    pd.DataFrame(finalists).to_csv(output / "finalist_grid.csv", index=False)
    finalist_results.to_csv(output / "finalist_results.csv", index=False)
    history.replay.atomic_json(output / "config.json", config)
    history.replay.atomic_json(output / "selection_lock.json", lock)
    lock_sha256 = history.sha256(output / "selection_lock.json")
    print(f"Candidate locked before evaluation: {lock['locked_candidate_id']}", flush=True)

    # The final 15-session result is first materialized after selection_lock.json exists.
    final_events = cache.events(locked_candidate) if locked_candidate is not None else []
    final_outcome = book.outcome(final_events)
    baseline_ledger = _canonical_ledger(baseline, paths, None, [], "baseline")
    final_ledger = _canonical_ledger(baseline, paths, locked_candidate, final_events,
        lock["locked_candidate_id"])
    history._same_native(baseline_ledger, baseline)
    history._same_allocation(baseline_ledger, baseline)
    _assert_fast_parity(book, final_outcome, final_ledger, "locked_candidate")
    ledgers = {"baseline": baseline_ledger, "locked_candidate": final_ledger}
    _, daily, trades, monthly = fixed.comparisons(ledgers, days)
    evaluation_rows = []
    for period in fixed._periods(days):
        pairs = [("baseline", baseline_ledger)]
        if locked_candidate is not None:
            pairs.append((lock["locked_candidate_id"], final_ledger))
        for candidate_id, ledger in pairs:
            metrics = _ledger_extended_metrics(ledger, baseline_ledger, periods[period])
            evaluation_rows.append(dict(candidate_id=candidate_id,
                candidate_sha256=(locked_candidate["candidate_sha256"] if candidate_id != "baseline" else None),
                family=(locked_candidate["family"] if candidate_id != "baseline" else "baseline"),
                period=period, **metrics))
    evaluation = pd.DataFrame(evaluation_rows)
    evaluation_row = evaluation.loc[(evaluation.candidate_id.eq(lock["locked_candidate_id"]))
        & evaluation.period.eq("evaluation15")].iloc[0].to_dict()
    evaluation_failures = ([] if locked_candidate is None
        else _qualification_failures(evaluation_row, "evaluation15"))
    recommendation = ("RESEARCH_CANDIDATE_PASSED_CHRONOLOGICAL_CHECK"
        if locked_candidate is not None and lock["development_qualified"] and not evaluation_failures
        else "NO_PARAMETER_CHANGE_PASSED_ALL_REALISM_CHECKS")
    summary = dict(
        schema_version=SCHEMA, version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        configuration_promoted=False, modify_live_or_paper=False, created_at_ist=now.isoformat(),
        first_date=str(days[0]), last_date=str(days[-1]), sessions=len(days),
        daily_extension_sessions=max(0, len(days)-50),
        daily_extension_days=[str(day) for day in days[50:]],
        days=[str(day) for day in days], source_strategy_version=source_summary["strategy_version"],
        source_strategy_fingerprint=source_summary["strategy_fingerprint"],
        source_active_setup_count=source_summary["active_setup_count"],
        source=dict(run_dir=str(source_dir), root=str(root), manifest_sha256=source_manifest_sha256),
        baseline_native_parity=True, baseline_allocation_parity=True,
        causal_feature_parity=True, fast_allocator_parity=True,
        input_proof=input_proof, coarse_candidate_count=len(candidates),
        finalist_count=len(finalists), selection_lock_sha256=lock_sha256,
        locked_candidate_id=lock["locked_candidate_id"],
        locked_candidate_sha256=lock["locked_candidate_sha256"],
        best_observed_development_candidate_id=lock["best_observed_candidate_id"],
        evaluation_failures=evaluation_failures, recommendation_status=recommendation,
        recommended_candidate_id=(lock["locked_candidate_id"]
            if recommendation == "RESEARCH_CANDIDATE_PASSED_CHRONOLOGICAL_CHECK" else "baseline"),
        historical_bar_resolution_seconds=60, subminute_ohlc_fabricated=False,
        fresh_strategy_holdout=False,
        evaluation_label="LOCKED_MONITOR_PARAMETER_CHRONOLOGICAL_CHECK_NOT_FRESH_STRATEGY_HOLDOUT",
    )
    features.to_csv(output / "decision_features.csv", index=False)
    evaluation.to_csv(output / "evaluation_results.csv", index=False)
    daily.to_csv(output / "daily_comparison.csv", index=False)
    trades.to_csv(output / "trade_comparison.csv", index=False)
    monthly.to_csv(output / "monthly_results.csv", index=False)
    pd.DataFrame(final_events, columns=["sid", "source_signal_id", "day", "setup_id",
        "tradingsymbol", "side", "decision_at_ist", "completed_bar_count", "flags",
        "next_bar_open_at_ist", "modeled_execution_at_ist", "execution_path_index",
        "next_bar_open_price", "original_exit_at_ist", "eligible_for_monitor_exit",
        "decision"]).to_csv(output / "final_events.csv", index=False)
    parity.to_csv(output / "parity_checks.csv", index=False)
    baseline_ledger.to_csv(output / "ledgers/baseline.csv", index=False)
    final_ledger.to_csv(output / "ledgers/locked_candidate.csv", index=False)
    history.replay.atomic_json(output / "summary.json", summary)
    _write_report(output, summary, lock, coarse, finalist_results, evaluation, daily)
    if (_code_provenance() != code_proof or history.sha256(source_dir / "manifest.json") != source_manifest_sha256
            or shared_rules.MONITOR_CONFIG != EXPECTED_SHARED_CONFIG):
        raise ValueError("Source, research code, or shared monitor configuration changed during search")
    history.replay.atomic_json(output / "manifest.json", dict(
        schema_version=SCHEMA, version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        source=summary["source"], code_sha256=code_proof,
        files={name: history.sha256(output / name) for name in sorted(FILES)}))
    verified_package(output, output_root)
    pointer = dict(schema_version=POINTER_SCHEMA, role="advisory_exit_parameter_research",
        execution_authority=False, configuration_promoted=False,
        run_dir=output.relative_to(output_root).as_posix(),
        manifest_sha256=history.sha256(output / "manifest.json"),
        source_strategy_fingerprint=source_summary["strategy_fingerprint"],
        session_date=str(days[-1]), created_at_ist=summary["created_at_ist"])
    history.replay.atomic_json(output_root / "latest/current_run.json", pointer)
    print(evaluation[["candidate_id", "period", "executed_trades", "win_rate_pct",
        "net_pnl_rs", "net_pnl_delta_vs_baseline_rs", "realized_loss_reduction_rs",
        "stress_10bps_net_pnl_delta_vs_baseline_rs", "max_drawdown_rs"]].to_string(index=False), flush=True)
    print(f"Recommendation: {recommendation}", flush=True)
    return output, summary


def verified_package(directory, root=None):
    directory = Path(directory).resolve()
    if root is not None and not directory.is_relative_to(Path(root).resolve()):
        raise ValueError("Parameter search run escaped its output root")
    manifest = _read(directory / "manifest.json")
    if (manifest.get("schema_version") != SCHEMA or manifest.get("version") != VERSION
            or manifest.get("execution_authority") is not False
            or set(manifest.get("files", {})) != FILES):
        raise ValueError("Invalid parameter search manifest")
    history._check_files(directory, manifest)
    summary, lock = _read(directory / "summary.json"), _read(directory / "selection_lock.json")
    if (_read(directory / "config.json") != configuration()
            or summary.get("execution_authority") is not False
            or summary.get("configuration_promoted") is not False
            or summary.get("modify_live_or_paper") is not False
            or summary.get("fresh_strategy_holdout") is not False
            or summary.get("sessions", 0) < 50
            or history.sha256(directory / "selection_lock.json") != summary.get("selection_lock_sha256")
            or lock.get("execution_authority") is not False
            or lock.get("configuration_promoted") is not False):
        raise ValueError("Parameter search changed its evidence, authority, or chronological lock")
    proof = summary["source"]
    if (manifest.get("source") != proof
            or history.sha256(Path(proof["run_dir"]) / "manifest.json") != proof["manifest_sha256"]):
        raise ValueError("Parameter search source identity changed")
    source_summary, _ = history.verified_package(proof["run_dir"], proof["root"])
    if (source_summary["strategy_fingerprint"] != summary["source_strategy_fingerprint"]
            or source_summary["days"] != summary["days"]):
        raise ValueError("Parameter search source strategy or calendar changed")
    expected_grid = pd.DataFrame(candidate_registry())
    actual_grid = read_frozen_csv(directory / "parameter_grid.csv")
    pd.testing.assert_frame_equal(actual_grid, expected_grid, check_dtype=False,
                                  check_exact=False, atol=1e-12, rtol=0)
    finalist_grid = read_frozen_csv(directory / "finalist_grid.csv")
    finalist_results = read_frozen_csv(directory / "finalist_results.csv")
    candidates = finalist_grid.where(pd.notna(finalist_grid), None).to_dict("records")
    recomputed = _choose_finalist(finalist_results, candidates)
    for key in ("locked_candidate_id", "locked_candidate_sha256", "parameter_change_locked",
                "development_qualified", "best_observed_candidate_id",
                "qualified_candidate_ids", "lock_reason"):
        if recomputed[key] != lock[key]:
            raise ValueError("Stored parameter lock differs from the development-only selection rule")
    baseline = read_frozen_csv(directory / "ledgers/baseline.csv")
    final = read_frozen_csv(directory / "ledgers/locked_candidate.csv")
    source_ledger = read_frozen_csv(Path(proof["run_dir"]) / "portfolio_trades.csv")
    history._same_native(baseline, source_ledger)
    history._same_allocation(baseline, source_ledger)
    if (set(final.sid) != set(baseline.sid) or final.sid.duplicated().any()
            or not final.execution_authority.eq(False).all()
            or not final.search_candidate_id.eq(lock["locked_candidate_id"]).all()):
        raise ValueError("Locked ledger mixes identities or authority")
    changed = final.loc[final.monitor_exit.eq(True)]
    if len(changed):
        decision = pd.to_datetime(changed.monitor_decision_at_ist, utc=True)
        modeled = pd.to_datetime(changed.exit_ts, utc=True)
        opened = pd.to_datetime(changed.monitor_open_observed_at_ist, utc=True)
        original = changed.sid.map(source_ledger.set_index("sid").exit_ts)
        if (not modeled.sub(opened).eq(pd.Timedelta(minutes=1)).all()
                or not opened.ge(decision).all()
                or not pd.to_datetime(changed.exit_execution_ts, utc=True).equals(opened)
                or not opened.lt(pd.to_datetime(original, utc=True)).all()):
            raise ValueError("Locked ledger used same-bar data or overrode original exit priority")
    days = [date.fromisoformat(value) for value in summary["days"]]
    expected = fixed.comparisons({"baseline": baseline, "locked_candidate": final}, days)
    for name, frame in zip(("daily_comparison", "trade_comparison", "monthly_results"), expected[1:]):
        actual = read_frozen_csv(directory / (name + ".csv"))
        pd.testing.assert_frame_equal(actual, frame, check_dtype=False, check_exact=False,
                                      atol=1e-6, rtol=0)
    periods = _periods(days)
    candidate_by_id = {item["candidate_id"]: item for item in candidates}
    locked_candidate = candidate_by_id[lock["locked_candidate_id"]]
    evaluation_rows = []
    for period in fixed._periods(days):
        for candidate_id, ledger in (("baseline", baseline),
                                     (lock["locked_candidate_id"], final)):
            metrics = _ledger_extended_metrics(ledger, baseline, periods[period])
            evaluation_rows.append(dict(candidate_id=candidate_id,
                candidate_sha256=(locked_candidate["candidate_sha256"]
                    if candidate_id != "baseline" else None),
                family=(locked_candidate["family"] if candidate_id != "baseline" else "baseline"),
                period=period, **metrics))
    actual_evaluation = read_frozen_csv(directory / "evaluation_results.csv")
    pd.testing.assert_frame_equal(actual_evaluation, pd.DataFrame(evaluation_rows),
        check_dtype=False, check_exact=False, atol=1e-6, rtol=0)
    return summary, manifest


def load_latest(output_root=DEFAULT_OUTPUT_ROOT):
    output_root = Path(output_root).resolve()
    pointer = _read(output_root / "latest/current_run.json")
    if (pointer.get("schema_version") != POINTER_SCHEMA
            or pointer.get("execution_authority") is not False
            or pointer.get("configuration_promoted") is not False):
        raise ValueError("Invalid parameter-search latest pointer")
    directory = history._child(output_root, pointer["run_dir"])
    if history.sha256(directory / "manifest.json") != pointer["manifest_sha256"]:
        raise ValueError("Parameter-search latest manifest changed")
    summary, manifest = verified_package(directory, output_root)
    return dict(summary=summary, manifest=manifest, run_dir=str(directory),
                report_path=str(directory / "report.html"),
                evaluation_results=history.replay.json_ready(
                    read_frozen_csv(directory / "evaluation_results.csv").to_dict("records")))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    directory, _ = run(args.source, args.output_root)
    print(directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
