"""Six-month causal V12 replay of the frozen V5 long setup at ranks 200-240.

The hourly prefilter is not changed.  This research runner applies the tighter
rank band inside the long setup, rebuilds both V5 models on expanding prior
history, and evaluates non-overlapping future blocks.  The first 48 sessions
are an explicit model/score warm-up because no pre-window training source is
available.  Rank 200-240 was chosen after inspecting this historical period,
so even the causal model replay is a post-selection diagnostic rather than a
fresh holdout.

Production configuration is never imported or modified by this module.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

import research_v12_ml_long_entry_backtest as replay_helpers
import research_v12_path_aware_long_rebuild as v2
import research_v12_two_stage_long_rebuild_v5 as v5


PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
RANK_MIN = 200
RANK_MAX = 240
INITIAL_TRAIN_SESSIONS = 48
REFIT_BLOCK_SESSIONS = 10
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_rank200_240_six_month_walkforward_v6_20260205_20260804"
)
SELECTION_BIAS_NOTE = (
    "rank 200-240 was selected after inspecting dates inside this window"
)

FROZEN_CONFIG = v5.Config(
    config_id="LEVEL12_SEQ8_SL1p0_T2p0_F0p25",
    feature_family="LEVEL12_SEQ8",
    sl_pct=1.0,
    tgt_pct=2.0,
    rolling_fraction=0.25,
)


@dataclass(frozen=True)
class WalkForwardBlock:
    block_id: str
    train_days: tuple[str, ...]
    evaluation_days: tuple[str, ...]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_safe(value: Any) -> Any:
    return v5.json_safe(value)


def session_calendar() -> list[str]:
    source = pd.read_csv(v2.SESSION_SOURCE)
    calendar = sorted(source["trade_date"].astype(str).unique())
    calendar = [day for day in calendar if START_DATE <= day <= END_DATE]
    if len(calendar) != 120:
        raise RuntimeError(f"unexpected six-month session count: {len(calendar)}")
    if calendar[0] != START_DATE or calendar[-1] != END_DATE:
        raise RuntimeError(
            f"unexpected six-month bounds: {calendar[0]} through {calendar[-1]}"
        )
    return calendar


def walkforward_blocks(
    calendar: Sequence[str],
    *,
    initial_train_sessions: int = INITIAL_TRAIN_SESSIONS,
    block_sessions: int = REFIT_BLOCK_SESSIONS,
) -> list[WalkForwardBlock]:
    days = list(calendar)
    if initial_train_sessions < v5.ROLLING_SCORE_SESSIONS:
        raise ValueError("initial training window is shorter than rolling-score history")
    if initial_train_sessions >= len(days):
        raise ValueError("initial training window consumes the full calendar")
    if block_sessions <= 0:
        raise ValueError("block_sessions must be positive")
    blocks: list[WalkForwardBlock] = []
    for number, start in enumerate(
        range(initial_train_sessions, len(days), block_sessions), 1
    ):
        end = min(start + block_sessions, len(days))
        train = tuple(days[:start])
        evaluation = tuple(days[start:end])
        if not train or not evaluation or train[-1] >= evaluation[0]:
            raise RuntimeError("noncausal walk-forward block")
        blocks.append(WalkForwardBlock(f"WF{number:02d}", train, evaluation))
    flattened = [day for block in blocks for day in block.evaluation_days]
    if flattened != days[initial_train_sessions:]:
        raise RuntimeError("walk-forward evaluation coverage is not exact")
    if len(flattened) != len(set(flattened)):
        raise RuntimeError("overlapping walk-forward evaluation sessions")
    return blocks


def _with_block(frame: pd.DataFrame, block: WalkForwardBlock) -> pd.DataFrame:
    work = frame.copy()
    work["walkforward_block"] = block.block_id
    work["model_train_through"] = block.train_days[-1]
    return work


def _effective_support(frame: pd.DataFrame, label: str) -> dict[str, float]:
    weights = v5.unit_weights(frame)
    return {
        str(value): float(weights.loc[frame[label].eq(value)].sum())
        for value in sorted(frame[label].dropna().unique(), key=str)
    }


def run_walkforward(
    ticket_frame: pd.DataFrame,
    state_frame: pd.DataFrame,
    calendar: Sequence[str],
    config: v5.Config,
) -> dict[str, Any]:
    blocks = walkforward_blocks(calendar)
    trade_parts: list[pd.DataFrame] = []
    threshold_parts: list[pd.DataFrame] = []
    decision_parts: list[pd.DataFrame] = []
    stage_a_parts: list[pd.DataFrame] = []
    stage_b_parts: list[pd.DataFrame] = []
    stage_b_all_parts: list[pd.DataFrame] = []
    coefficient_parts: list[pd.DataFrame] = []
    block_rows: list[dict[str, Any]] = []
    models: dict[str, dict[str, Any]] = {}

    for block in blocks:
        train_tickets = ticket_frame.loc[
            ticket_frame["trade_date"].isin(block.train_days)
        ].copy()
        train_states = state_frame.loc[
            state_frame["trade_date"].isin(block.train_days)
        ].copy()
        if train_tickets.empty or train_states.empty:
            raise RuntimeError(f"empty training data for {block.block_id}")
        if train_tickets["trade_date"].max() >= block.evaluation_days[0]:
            raise RuntimeError(f"future ticket leakage in {block.block_id}")
        if train_states["trade_date"].max() >= block.evaluation_days[0]:
            raise RuntimeError(f"future state leakage in {block.block_id}")

        result = v5.evaluate_config(
            ticket_frame,
            state_frame,
            block.train_days,
            block.evaluation_days,
            calendar,
            config,
            return_models=True,
            insufficient_reference_policy="abstain",
        )
        trades = _with_block(result.pop("trades_frame"), block)
        thresholds = _with_block(result.pop("thresholds_frame"), block)
        decisions = _with_block(result.pop("decisions_frame"), block)
        stage_a_diagnostics = _with_block(
            result.pop("stage_a_diagnostics_frame"), block
        )
        stage_b_diagnostics = _with_block(
            result.pop("stage_b_diagnostics_frame"), block
        )
        stage_b_all_diagnostics = _with_block(
            result.pop("stage_b_all_states_diagnostics_frame"), block
        )
        stage_a_model = result.pop("stage_a_model")
        stage_b_model = result.pop("stage_b_model")

        if not trades.empty:
            ranks = pd.to_numeric(trades["selection_rank"], errors="raise")
            if not ranks.between(RANK_MIN, RANK_MAX).all():
                raise RuntimeError(f"rank contract breach in {block.block_id}")
        if set(thresholds["trade_date"].astype(str)) != set(block.evaluation_days):
            raise RuntimeError(f"threshold log coverage failure in {block.block_id}")

        support_a = _effective_support(train_tickets, "opportunity_positive")
        support_b = _effective_support(train_states, "stage_b_label")
        block_rows.append({
            **v5._metric_record(result),
            "walkforward_block": block.block_id,
            "train_start": block.train_days[0],
            "train_end": block.train_days[-1],
            "train_sessions": len(block.train_days),
            "evaluation_start": block.evaluation_days[0],
            "evaluation_end": block.evaluation_days[-1],
            "evaluation_sessions": len(block.evaluation_days),
            "stage_a_effective_negative_units": support_a.get("0", 0.0),
            "stage_a_effective_positive_units": support_a.get("1", 0.0),
            "stage_b_effective_defer_units": support_b.get("DEFER", 0.0),
            "stage_b_effective_enter_units": support_b.get("ENTER", 0.0),
        })
        a_coefficients = v5.model_coefficients(
            stage_a_model, config.stage_a_features, "STAGE_A"
        )
        b_coefficients = v5.model_coefficients(
            stage_b_model, config.stage_b_features, "STAGE_B"
        )
        coefficients = pd.concat([a_coefficients, b_coefficients], ignore_index=True)
        coefficients["walkforward_block"] = block.block_id
        coefficients["model_train_through"] = block.train_days[-1]

        trade_parts.append(trades)
        threshold_parts.append(thresholds)
        decision_parts.append(decisions)
        stage_a_parts.append(stage_a_diagnostics)
        stage_b_parts.append(stage_b_diagnostics)
        stage_b_all_parts.append(stage_b_all_diagnostics)
        coefficient_parts.append(coefficients)
        models[block.block_id] = {
            "stage_a": stage_a_model,
            "stage_b": stage_b_model,
            "config": json_safe(config.__dict__),
            "train_start": block.train_days[0],
            "train_end": block.train_days[-1],
            "evaluation_start": block.evaluation_days[0],
            "evaluation_end": block.evaluation_days[-1],
        }

    def combine(parts: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    trades = combine(trade_parts)
    if not trades.empty and trades.duplicated(["trade_date", "ticker"]).any():
        raise RuntimeError("one-entry-per-ticker-day contract breached")
    evaluation_days = [day for block in blocks for day in block.evaluation_days]
    return {
        "blocks": blocks,
        "evaluation_days": evaluation_days,
        "block_results": pd.DataFrame(block_rows),
        "trades": trades,
        "thresholds": combine(threshold_parts),
        "decisions": combine(decision_parts),
        "stage_a_diagnostics": combine(stage_a_parts),
        "stage_b_diagnostics": combine(stage_b_parts),
        "stage_b_all_diagnostics": combine(stage_b_all_parts),
        "coefficients": combine(coefficient_parts),
        "models": models,
    }


def pooled_auc(frame: pd.DataFrame) -> float:
    if frame.empty:
        return float("nan")
    work = pd.DataFrame({
        "label": pd.to_numeric(frame["label"], errors="coerce"),
        "score": pd.to_numeric(frame["score"], errors="coerce"),
        "weight": pd.to_numeric(frame["weight"], errors="coerce"),
    }).dropna()
    if work.empty or work["label"].nunique() != 2:
        return float("nan")
    return float(roc_auc_score(
        work["label"].astype(int),
        work["score"].astype(float),
        sample_weight=work["weight"].astype(float),
    ))


def candidate_daily(tickets: pd.DataFrame, calendar: Sequence[str]) -> pd.DataFrame:
    grouped = (
        tickets.groupby("trade_date", as_index=False)
        .agg(
            base_ticket_rows=("ticket_id", "size"),
            unique_tickers=("ticker", "nunique"),
            earliest_ticket=("ticket_time_ist", "min"),
            latest_ticket=("ticket_time_ist", "max"),
        )
    )
    base = pd.DataFrame({"trade_date": list(calendar)})
    result = base.merge(grouped, on="trade_date", how="left", validate="one_to_one")
    result[["base_ticket_rows", "unique_tickers"]] = result[
        ["base_ticket_rows", "unique_tickers"]
    ].fillna(0).astype(int)
    return result


def candidate_hourly(tickets: pd.DataFrame) -> pd.DataFrame:
    work = tickets.copy()
    work["hour_ist"] = pd.to_datetime(work["ticket_time_ist"]).dt.hour
    work["ticker_day"] = (
        work["trade_date"].astype(str) + "|" + work["ticker"].astype(str)
    )
    return (
        work.groupby("hour_ist", as_index=False)
        .agg(
            base_ticket_rows=("ticket_id", "size"),
            unique_ticker_days=("ticker_day", "nunique"),
            active_sessions=("trade_date", "nunique"),
        )
        .sort_values("hour_ist")
    )


def six_month_daily_overview(
    candidates: pd.DataFrame,
    trade_daily: pd.DataFrame,
    thresholds: pd.DataFrame,
    blocks: Sequence[WalkForwardBlock],
) -> pd.DataFrame:
    overview = candidates.copy()
    block_by_day = {
        day: block.block_id
        for block in blocks
        for day in block.evaluation_days
    }
    overview["phase"] = np.where(
        overview["trade_date"].isin(block_by_day), "WALKFORWARD_OOS", "WARMUP"
    )
    overview["walkforward_block"] = overview["trade_date"].map(block_by_day).fillna("")
    daily = trade_daily.rename(columns={
        "trades": "selected_trades",
        "gross_pnl_rs": "selected_gross_pnl_rs",
        "cost_rs": "selected_cost_rs",
        "net_pnl_rs": "selected_net_pnl_rs",
        "cum_pnl_rs": "oos_cumulative_pnl_rs",
        "drawdown_rs": "oos_drawdown_rs",
    })
    overview = overview.merge(daily, on="trade_date", how="left", validate="one_to_one")
    threshold_fields = thresholds[[
        "trade_date", "reference_status", "abstention_reason",
        "reference_unique_ticker_days", "tail_k", "threshold",
    ]].copy()
    overview = overview.merge(
        threshold_fields, on="trade_date", how="left", validate="one_to_one"
    )
    numeric_zero = [
        "selected_trades", "selected_gross_pnl_rs", "selected_cost_rs",
        "selected_net_pnl_rs",
    ]
    overview[numeric_zero] = overview[numeric_zero].fillna(0)
    overview["selected_trades"] = overview["selected_trades"].astype(int)
    overview["reference_status"] = overview["reference_status"].fillna("NOT_APPLICABLE")
    overview["abstention_reason"] = overview["abstention_reason"].fillna("")
    return overview


def period_metrics(
    trades: pd.DataFrame, evaluation_days: Sequence[str]
) -> dict[str, Any]:
    days = list(evaluation_days)
    midpoint = len(days) // 2
    first_days = days[:midpoint]
    second_days = days[midpoint:]
    overall = v5.performance(trades, days)
    first = v5.performance(
        trades.loc[trades["trade_date"].isin(first_days)].copy(), first_days
    )
    second = v5.performance(
        trades.loc[trades["trade_date"].isin(second_days)].copy(), second_days
    )
    return {
        "overall": overall,
        "first_half": first,
        "second_half": second,
        "active_trade_days": overall["sessions"] - overall["zero_trade_sessions"],
        "first_half_window": [first_days[0], first_days[-1], len(first_days)],
        "second_half_window": [second_days[0], second_days[-1], len(second_days)],
    }


def validation_gate(
    trades: pd.DataFrame,
    evaluation_days: Sequence[str],
    block_results: pd.DataFrame,
    stage_a_diagnostics: pd.DataFrame,
    stage_b_diagnostics: pd.DataFrame,
) -> dict[str, Any]:
    periods = period_metrics(trades, evaluation_days)
    metrics = periods["overall"]
    concentration = v5.concentration_metrics(trades)
    positive_blocks = int(block_results["net_pnl_rs"].gt(0).sum())
    required_positive_blocks = int(math.ceil(len(block_results) * 0.60))
    a_auc = pooled_auc(stage_a_diagnostics)
    b_auc = pooled_auc(stage_b_diagnostics)
    checks = {
        "minimum_30_trades": metrics["trades"] >= 30,
        "minimum_20_active_days": periods["active_trade_days"] >= 20,
        "net_positive": metrics["net_pnl_rs"] > 0,
        "profit_factor_at_least_1_20": metrics["profit_factor"] >= 1.20,
        "first_half_net_positive": periods["first_half"]["net_pnl_rs"] > 0,
        "second_half_net_positive": periods["second_half"]["net_pnl_rs"] > 0,
        "positive_blocks_at_least_60pct": positive_blocks >= required_positive_blocks,
        "pooled_stage_a_auc_at_least_0_55": a_auc >= 0.55,
        "pooled_stage_b_auc_at_least_0_55": b_auc >= 0.55,
        "largest_ticker_trade_share_at_most_0_20": (
            concentration["largest_ticker_trade_share"] <= 0.20
        ),
        "largest_positive_day_pnl_share_at_most_0_40": (
            concentration["largest_positive_day_pnl_share"] <= 0.40
        ),
    }
    return {
        **periods,
        "positive_blocks": positive_blocks,
        "total_blocks": len(block_results),
        "required_positive_blocks": required_positive_blocks,
        "pooled_stage_a_auc": a_auc,
        "pooled_stage_b_enter_auc": b_auc,
        **concentration,
        "checks": checks,
        "passed": bool(all(checks.values())),
    }


def write_config(path: Path) -> None:
    content = f'''"""Research-only six-month V12 long configuration."""

PRODUCTION_APPROVED = False
SETUP = {v5.SETUP!r}
PREFILTER_JOB_CHANGED = False
PREFILTER_REQUIRE_PRIMARY_SIDE = "LONG"
SETUP_SELECTION_RANK_MIN = {RANK_MIN}
SETUP_SELECTION_RANK_MAX = {RANK_MAX}
BASE_SIGNAL_MINUTE_MIN = 570
BASE_SIGNAL_MINUTE_MAX = 855
ATR_PCT_MIN = 1.05
RANGE_PCT_MIN = 1.25
VWAP_DISTANCE_ATR_MIN = 0.05
FEATURE_FAMILY = {FROZEN_CONFIG.feature_family!r}
STAGE_A_FEATURES = {FROZEN_CONFIG.stage_a_features!r}
STAGE_B_FEATURES = {FROZEN_CONFIG.stage_b_features!r}
STAGE_B_ENTER_PROBABILITY_MIN = {v5.STAGE_B_ENTER_PROBABILITY!r}
WAIT_OFFSETS_MINUTES = (0, 5, 10)
WAITED_STATE_REQUIRES_LONG_AND_RANK_BAND = True
ROLLING_SCORE_SESSIONS = {v5.ROLLING_SCORE_SESSIONS}
ROLLING_TOP_FRACTION = {FROZEN_CONFIG.rolling_fraction!r}
ROLLING_REFERENCE_MIN_TICKER_DAYS = 30
ROLLING_REFERENCE_MIN_ACTIVE_SESSIONS = 15
ROLLING_TAIL_MIN_UNITS = 10
INSUFFICIENT_ROLLING_REFERENCE_ACTION = "ABSTAIN"
STOP_LOSS_PCT = {FROZEN_CONFIG.sl_pct!r}
TARGET_PCT = {FROZEN_CONFIG.tgt_pct!r}
ONE_ENTRY_PER_TICKER_DAY = True
DAILY_CAP = {v5.DAILY_CAP}
ENTRY_EXECUTION = "next available 1-minute open after completed 5-minute signal"
EXIT_EXECUTION = "exact V12 1-minute SL/target/EOD resolver"
COST_MODEL = "NSE statutory intraday-equity costs"
V12_RISK_CONTRACT = {v5.EXPECTED_V12_RISK_CONTRACT!r}
WALKFORWARD = {{
    "window": ({START_DATE!r}, {END_DATE!r}),
    "initial_train_sessions": {INITIAL_TRAIN_SESSIONS},
    "refit_block_sessions": {REFIT_BLOCK_SESSIONS},
    "rank_band_was_selected_after_inspecting_this_period": True,
}}
SELECTION_BIAS_NOTE = {SELECTION_BIAS_NOTE!r}
'''
    path.write_text(content, encoding="utf-8")


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    primary = summary["walkforward_v12_parity"]
    strict = summary["walkforward_strict_all_in_risk500"]
    metrics = primary["overall"]
    evaluation = summary["evaluation_contract"]
    failed_checks = [
        name for name, passed in primary["checks"].items() if not passed
    ]
    second_half_trades = int(primary["second_half"]["trades"])
    second_half_trade_word = "trade" if second_half_trades == 1 else "trades"
    text = f"""# Six-month V12 rank-200-240 long replay

## Result

Verdict: **{summary['verdict']}**.

The source covers all 120 sessions from {START_DATE} through {END_DATE}. The
first {INITIAL_TRAIN_SESSIONS} sessions are model and rolling-score warm-up;
the remaining {metrics['sessions']} sessions are evaluated in expanding,
chronological blocks. Those evaluated sessions produced **{metrics['trades']}
trades**, net P&L **INR {metrics['net_pnl_rs']:,.2f}**, PF
**{metrics['profit_factor']:.3f}**, win rate **{metrics['win_rate_pct']:.2f}%**,
and max drawdown **INR {metrics['max_drawdown_rs']:,.2f}** under V12 parity
sizing and statutory costs.

This was sparse: only **{primary['active_trade_days']} of {metrics['sessions']}
sessions** traded, only **{second_half_trades} {second_half_trade_word}** occurred in
the second half, and the final trade was on **{evaluation['last_trade_date']}**.
The strategy abstained on **{evaluation['rolling_reference_abstention_days']}**
sessions when the frozen rolling-reference minimum was unavailable. Failed
robustness checks: `{', '.join(failed_checks)}`.

The strict all-in INR 500 risk diagnostic produced
**{strict['overall']['trades']} trades**, net P&L
**INR {strict['overall']['net_pnl_rs']:,.2f}**, and PF
**{strict['overall']['profit_factor']:.3f}**.

## Interpretation

The hourly prefilter was not changed. The long setup alone required active
LONG membership and rank {RANK_MIN}-{RANK_MAX} at the base and every deferred
five-minute state. Entries and exits use exact V12 one-minute execution.

This is not a fresh promotion holdout: {summary['fresh_holdout_reason']}. A
positive result can support more research, but cannot establish expected live
profitability.
`PRODUCTION_APPROVED=False`; no live configuration was enabled or restarted.
"""
    path.write_text(text, encoding="utf-8")


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    runtime_risk_contract = v5.v12_risk_contract()
    calendar = session_calendar()
    blocks = walkforward_blocks(calendar)

    tickets, states, nodes, funnel = v5.load_ticket_states(
        start_date=START_DATE,
        end_date=END_DATE,
        rank_min=RANK_MIN,
        rank_max=RANK_MAX,
    )
    if not pd.to_numeric(tickets["selection_rank"], errors="raise").between(
        RANK_MIN, RANK_MAX
    ).all():
        raise RuntimeError("base ticket rank contract breach")
    states, raw, outcomes, rejects, prewarm = v5.build_exact_paths(
        states,
        nodes,
        exit_pairs=((FROZEN_CONFIG.sl_pct, FROZEN_CONFIG.tgt_pct),),
    )
    ticket_frame, state_frame = v5.make_exit_dataset(
        tickets,
        states,
        raw,
        outcomes,
        sl_pct=FROZEN_CONFIG.sl_pct,
        tgt_pct=FROZEN_CONFIG.tgt_pct,
    )
    walkforward = run_walkforward(
        ticket_frame, state_frame, calendar, FROZEN_CONFIG
    )
    evaluation_days = walkforward["evaluation_days"]
    trades = walkforward["trades"]
    abstention_rows = walkforward["thresholds"].loc[
        walkforward["thresholds"]["reference_status"].eq("ABSTAIN")
    ].copy()
    gate = validation_gate(
        trades,
        evaluation_days,
        walkforward["block_results"],
        walkforward["stage_a_diagnostics"],
        walkforward["stage_b_diagnostics"],
    )

    strict_ledger = v5.strict_risk_ledger(trades)
    if "strict_sizing_rejected" not in strict_ledger:
        strict_ledger["strict_sizing_rejected"] = pd.Series(dtype=bool)
    strict_trades = strict_ledger.loc[
        ~strict_ledger["strict_sizing_rejected"].astype(bool)
    ].copy()
    strict_metrics = period_metrics(strict_trades, evaluation_days)
    strict_metrics["selected_entries"] = len(strict_ledger)
    strict_metrics["executed_entries"] = len(strict_trades)
    strict_metrics["sizing_rejections"] = int(
        strict_ledger["strict_sizing_rejected"].sum()
    )
    risk_audit = v5.parity_risk_audit(trades)
    if risk_audit["expected_v12_quantity_mismatches"] != 0:
        raise RuntimeError("V12 parity quantity invariant failed")

    verdict = (
        "RESEARCH_SIGNAL_ONLY_REQUIRES_GENUINELY_FRESH_HOLDOUT"
        if gate["passed"]
        else "REJECTED_NO_ROBUST_PROFITABLE_EDGE"
    )
    candidate_daily_frame = candidate_daily(tickets, calendar)
    candidate_hourly_frame = candidate_hourly(tickets)
    trade_daily = replay_helpers.daily_summary(trades, evaluation_days)
    trade_hourly = replay_helpers.hourly_summary(trades)
    strict_daily = replay_helpers.daily_summary(strict_trades, evaluation_days)
    daily_overview = six_month_daily_overview(
        candidate_daily_frame,
        trade_daily,
        walkforward["thresholds"],
        blocks,
    )

    funnel = pd.concat([
        funnel,
        pd.DataFrame([
            {"stage": "v12_executable_state_nodes", "rows": len(raw)},
            {"stage": "exact_exit_rows_frozen_pair", "rows": len(outcomes)},
            {"stage": "labelled_base_tickets", "rows": len(ticket_frame)},
            {"stage": "labelled_reachable_states", "rows": len(state_frame)},
            {"stage": "walkforward_selected_entries", "rows": len(trades)},
            {"stage": "entry_engine_rejects", "rows": len(rejects)},
        ]),
    ], ignore_index=True)

    summary = {
        "production_approved": False,
        "research_only": True,
        "verdict": verdict,
        "fresh_holdout": False,
        "fresh_holdout_reason": SELECTION_BIAS_NOTE,
        "setup_contract": {
            "prefilter_job_changed": False,
            "required_primary_side": "LONG",
            "setup_rank_band_inclusive": [RANK_MIN, RANK_MAX],
            "rank_checked_again_at_deferred_states": True,
            "base_signal_minute_range": [570, 855],
            "atr_pct_min": 1.05,
            "range_pct_min": 1.25,
            "vwap_distance_atr_min": 0.05,
            "config": json_safe(FROZEN_CONFIG.__dict__),
            "stage_b_enter_probability_min": v5.STAGE_B_ENTER_PROBABILITY,
            "rolling_score_sessions": v5.ROLLING_SCORE_SESSIONS,
            "wait_offsets_minutes": [0, 5, 10],
            "daily_cap": v5.DAILY_CAP,
            "one_entry_per_ticker_day": True,
            "exact_next_1m_entry": True,
            "exact_1m_exit": True,
            "statutory_costs": True,
            "runtime_v12_risk_contract": runtime_risk_contract,
        },
        "evaluation_contract": {
            "source_window": [START_DATE, END_DATE, len(calendar)],
            "warmup_window": [
                calendar[0], calendar[INITIAL_TRAIN_SESSIONS - 1],
                INITIAL_TRAIN_SESSIONS,
            ],
            "walkforward_window": [
                evaluation_days[0], evaluation_days[-1], len(evaluation_days),
            ],
            "refit_block_sessions": REFIT_BLOCK_SESSIONS,
            "blocks": [
                {
                    "block_id": block.block_id,
                    "train_start": block.train_days[0],
                    "train_end": block.train_days[-1],
                    "train_sessions": len(block.train_days),
                    "evaluation_start": block.evaluation_days[0],
                    "evaluation_end": block.evaluation_days[-1],
                    "evaluation_sessions": len(block.evaluation_days),
                }
                for block in blocks
            ],
            "future_labels_used_for_model_fit": False,
            "full_period_fitted_replay_reported": False,
            "rolling_reference_min_ticker_days": 30,
            "rolling_reference_min_active_sessions": 15,
            "rolling_tail_min_units": 10,
            "insufficient_rolling_reference_action": "ABSTAIN",
            "rolling_reference_abstention_days": len(abstention_rows),
            "rolling_reference_abstention_dates": (
                abstention_rows["trade_date"].astype(str).tolist()
            ),
            "last_trade_date": (
                str(trades["trade_date"].max()) if not trades.empty else None
            ),
        },
        "candidate_counts": {
            "source_rows": int(
                funnel.loc[funnel["stage"].eq("source_date_window"), "rows"].iloc[0]
            ),
            "base_ticket_rows": len(tickets),
            "base_unique_ticker_days": int(
                tickets[["trade_date", "ticker"]].drop_duplicates().shape[0]
            ),
            "unique_execution_nodes": len(nodes),
            "v12_executable_state_nodes": len(raw),
            "entry_engine_rejects": len(rejects),
        },
        "prewarm_1m": prewarm,
        "walkforward_v12_parity": gate,
        "walkforward_strict_all_in_risk500": strict_metrics,
        "v12_parity_risk_audit": risk_audit,
        "promotion_candidate": False,
        "no_production_mutation": True,
    }

    frames: dict[str, pd.DataFrame] = {
        "candidate_funnel.csv": funnel,
        "candidate_daily.csv": candidate_daily_frame,
        "candidate_hourly.csv": candidate_hourly_frame,
        "six_month_daily_overview.csv": daily_overview,
        "entry_engine_raw.csv": raw,
        "entry_engine_rejects.csv": rejects,
        "exact_state_outcomes.csv": outcomes,
        "walkforward_block_results.csv": walkforward["block_results"],
        "walkforward_trades_v12_parity.csv": trades,
        "walkforward_trades_strict_risk500.csv": strict_ledger,
        "walkforward_daily_v12_parity.csv": trade_daily,
        "walkforward_daily_strict_risk500.csv": strict_daily,
        "walkforward_hourly_v12_parity.csv": trade_hourly,
        "walkforward_rolling_thresholds.csv": walkforward["thresholds"],
        "walkforward_ticket_decisions.csv": walkforward["decisions"],
        "walkforward_stage_a_diagnostics.csv": walkforward["stage_a_diagnostics"],
        "walkforward_stage_b_diagnostics.csv": walkforward["stage_b_diagnostics"],
        "walkforward_model_coefficients.csv": walkforward["coefficients"],
    }
    output_paths: dict[str, Path] = {}
    for name, frame in frames.items():
        output = OUTPUT_DIR / name
        frame.to_csv(output, index=False)
        output_paths[name] = output

    config_path = OUTPUT_DIR / "six_month_long_setup_conf.py"
    models_path = OUTPUT_DIR / "walkforward_models.joblib"
    summary_path = OUTPUT_DIR / "summary.json"
    report_path = OUTPUT_DIR / "RESEARCH_REPORT.md"
    write_config(config_path)
    joblib.dump(walkforward["models"], models_path)
    summary_path.write_text(
        json.dumps(json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    write_report(report_path, summary)

    artifact_paths = list(output_paths.values()) + [
        config_path, models_path, summary_path, report_path,
    ]
    manifest = {
        "production_approved": False,
        "artifacts": [
            {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
            for path in sorted(artifact_paths, key=lambda item: item.name)
        ],
        "source_inputs": {
            str(v2.SOURCE.resolve()): sha256(v2.SOURCE),
            str(v2.SESSION_SOURCE.resolve()): sha256(v2.SESSION_SOURCE),
        },
    }
    (OUTPUT_DIR / "integrity_manifest.json").write_text(
        json.dumps(json_safe(manifest), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(json_safe({
        "output_dir": str(OUTPUT_DIR),
        "verdict": verdict,
        "source_sessions": len(calendar),
        "warmup_sessions": INITIAL_TRAIN_SESSIONS,
        "walkforward_sessions": len(evaluation_days),
        "v12_parity": gate,
        "strict_all_in_risk500": strict_metrics,
    }), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
