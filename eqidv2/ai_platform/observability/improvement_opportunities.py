"""Evidence-backed research proposals; no strategy selection or execution authority.

Inputs are summaries of the same artifacts used by the other dashboard reports.
Observed associations never become recommended parameter values or profit forecasts.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "eqidv2.v13_v10_g.improvement_opportunities.v1"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _fmt(value: Any, digits: int = 2) -> str:
    result = _number(value)
    return f"{result:,.{digits}f}" if result is not None else "UNAVAILABLE"


def _evidence(card: str, label: str) -> dict[str, str]:
    return {"card_id": f"fno_v13_v10_g_{card}", "label": label}


def build_improvement_opportunities(
    *,
    baseline: Mapping[str, Any],
    by_setup: Sequence[tuple[str, Mapping[str, Any]]],
    regimes: Mapping[str, Sequence[tuple[str, Mapping[str, Any]]]],
    operational: Mapping[str, Any],
    execution_research: Mapping[str, Any],
    prediction_metrics: Mapping[str, Any],
    coverage: Mapping[str, Any],
    audit: Mapping[str, Any],
    point_in_time: Mapping[str, Any],
    shadow: Mapping[str, Any],
    promotion_gates: Mapping[str, bool],
    historical_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build reproducible proposals, keeping absent evidence explicitly unavailable."""
    items: list[dict[str, Any]] = []

    def add(key: str, title: str, category: str, priority: str, state: str,
            observations: list[str], benefit: str, experiment: str, acceptance: str,
            limitations: str, sources: list[dict[str, str]]) -> None:
        items.append({
            "id": key, "title": title, "category": category, "priority": priority,
            "evidence_state": state, "observations": observations,
            "potential_benefit": benefit, "proposed_experiment": experiment,
            "acceptance_criteria": acceptance, "limitations": limitations,
            "sources": sources, "estimated_profit_improvement_rupees": None,
            "safe_for_live_selection": False, "execution_authority": False,
            "implementation_state": "PROPOSED_NOT_APPLIED",
        })

    execution = operational.get("execution", {})
    live = execution.get("LIVE", {})
    paper = execution.get("PAPER", {})
    live_orders = int(live.get("orders", 0))
    cancelled = int(live.get("cancelled", 0))
    journal = int(live.get("first_reason_signal_coverage", 0))
    technical_markers = ("LATE_START", "TOKENEXCEPTION", "AUTH", "KITE CLIENT UNAVAILABLE",
                         "QUOTE_UNAVAILABLE", "ALL_APPS", "CONNECTION", "TIMEOUT")
    reasons = {**live.get("status_reason_counts", {}), **live.get("first_observed_reason_counts", {})}
    technical_failure = any(any(marker in reason.upper() for marker in technical_markers)
                            for reason in reasons)
    execution_observations = [
        f"Persisted LIVE: {live.get('filled', 'UNAVAILABLE')}/{live_orders} fills; "
        f"{cancelled} cancellations; first-reason journal coverage {journal}/{live_orders}.",
    ] if live_orders else ["No persisted LIVE order sample is available."]
    execution_observations.append(
        f"Persisted PAPER: {paper.get('filled', 'UNAVAILABLE')}/{paper['orders']} fills."
        if paper.get("orders") else "No persisted PAPER order sample is available.")
    for reason, count in live.get("status_reason_counts", {}).items():
        execution_observations.append(f"Final LIVE reason: {reason} ({count} orders).")
    for reason, count in live.get("first_observed_reason_counts", {}).items():
        execution_observations.append(f"Journaled first reason: {reason} ({count} signals).")
    add("execution_availability", "Recover entries lost to execution failures", "Execution", "P0",
        "UNAVAILABLE" if not live_orders else (
            "OBSERVED_ISSUE" if technical_failure or journal < cancelled else "MONITOR"),
        execution_observations,
        "Reduce avoidable missed opportunities and identify the original failure.",
        "Record authentication/failover readiness, data freshness, activation time and every order transition. "
        "Separate late-start, quote/auth failure and genuine untouched-trigger cancellations.",
        "Every eligible prospective signal has a first-cause record; report in-window technical-failure rate "
        "separately from market non-fills and demonstrate the predeclared service target in shadow.",
        "The pooled LIVE and PAPER samples may cover different operating conditions. A missing fill does not "
        "prove a profitable trade was missed; recovered profit is unquantified.",
        [_evidence("observability_entry_execution", "Entry and Execution")])

    drift = operational.get("drift", [])
    changed = [row for row in drift if any(
        (_number(row.get(key)) or 0) > 0 for key in (
            "live_only", "finalized_only", "price_or_bar_changes", "oi_changes", "indicator_changes"))]
    latest_drift = max(drift, key=lambda row: str(row.get("day", ""))) if drift else None
    add("signal_parity", "Explain live versus finalized signal differences", "Data and parity", "P1",
        "OBSERVED_ISSUE" if changed else ("MONITOR" if drift else "UNAVAILABLE"),
        ([f"{len(changed)}/{len(drift)} compared days have selected-set or input differences.",
          f"Latest comparison {latest_drift.get('day')}: live {latest_drift.get('live_selected')}, "
          f"finalized {latest_drift.get('finalized_selected')}, common {latest_drift.get('common')}; "
          f"first divergence {latest_drift.get('first_divergence', 'UNAVAILABLE')}."]
         if latest_drift else ["No comparable persisted live/finalized selection bundle is available."]),
        "Make backtest decisions reproducible in the operating pipeline.",
        "Join existing dated snapshots and feature hashes across live, observed and finalized stages using "
        "one deterministic signal ID; compare bars, features, gates, ranking and selection in order.",
        "Every selected-set difference has an identified first differing stage and an explained cause; "
        "replaying identical inputs yields identical ordered selections.",
        "Natural-key differences alone cannot identify the cause or quantify P&L improvement.",
        [_evidence("observability_live_finalized_drift", "Live vs Finalized Drift")])

    scenarios = execution_research.get("scenarios", []) if execution_research.get("state") == "READY_VERIFIED" else []
    no_delay = [row for row in scenarios if _number(row.get("delay_bars")) == 0]
    cost_observations = [
        f"{row.get('scenario')}: net INR {_fmt(row.get('net_profit_rupees'))}; "
        f"delta against its frozen control INR {_fmt(row.get('net_delta_vs_baseline_rupees'))}."
        for row in no_delay
    ] or [f"Verified execution sensitivity unavailable ({execution_research.get('state', 'UNAVAILABLE')})."]
    add("execution_costs", "Measure execution costs before tuning entries", "Execution", "P1",
        "SENSITIVITY_ONLY" if no_delay else "UNAVAILABLE", cost_observations,
        "Measure how much the strategy depends on ideal fills and identify avoidable execution drag.",
        "Capture quote-at-submit, acknowledgement and fills; replay whole-share quantities, tick rounding, "
        "fees and entry/exit slippage. Keep an exact G control and compare both strategies under identical costs.",
        "Reconcile fills/fees with the broker ledger; pass quantity/tick accounting and absolute-expiry tests. "
        "Latency must shorten the remaining confirmation-plus-10-minute window, never extend it.",
        "Trigger-to-fill distance is a proxy, not measured broker slippage. Delayed scenarios are excluded "
        "from this evidence table pending verification of their absolute-expiry semantics. Scenario losses "
        "are not recoverable-profit forecasts.",
        [_evidence("observability_entry_execution", "Entry and Execution"),
         _evidence("observability_pnl_attribution", "P&L Attribution")])

    dates = coverage.get("dates", {})
    partial = {day: len(detail.get("futures_oi_5m", {}).get("partial_contracts", {}))
               for day, detail in sorted(dates.items())}
    missing_ts = (sum(point_in_time["missing_timestamps"].values())
                  if "missing_timestamps" in point_in_time else None)
    has_quality_evidence = bool(dates) or "rows" in audit or "violation_count" in point_in_time
    add("data_coverage", "Measure missing data across the complete research period", "Data and parity", "P1",
        "OBSERVED_ISSUE" if any(partial.values()) or missing_ts or point_in_time.get("violation_count", 0)
        else ("MEASUREMENT_GAP" if has_quality_evidence else "UNAVAILABLE"),
        [f"Decision-audit rows: {audit.get('rows', 'UNAVAILABLE')}; coverage audit contains {len(dates)} dates.",
         "Partial OI contracts by audited date: " + (
             "; ".join(f"{day}: {count}" for day, count in partial.items()) or "UNAVAILABLE"),
         f"Missing selected-order timestamps: {missing_ts if missing_ts is not None else 'UNAVAILABLE'}; ordering violations: "
         f"{point_in_time.get('violation_count', 'UNAVAILABLE')}."] ,
        "Avoid drawing parameter conclusions from incomplete inputs or misclassified zero-trade sessions.",
        "Publish per-day/per-slot missing OI, price, confirmation and dated-universe coverage for the full "
        "corpus. Replay G and each challenger on the same declared usable sessions.",
        "Every expected input has a verified observation or explicit missing-data state; complete zero-trade "
        "days remain in performance denominators, and excluded sessions have visible reasons.",
        "Artifact presence and timestamp ordering do not establish full row-level coverage; partial-contract "
        "counts apply only to the listed audited dates.",
        [_evidence("research_dataset", "Historical Dataset"),
         _evidence("research_data_quality", "Research Data Quality")])

    gate_counts = operational.get("gate_failures", {})
    gate_evidence = [f"First failed gate {name}: {count} symbol-slot rows."
                     for name, count in sorted(gate_counts.items(), key=lambda item: (-item[1], item[0]))[:5]]
    add("indicator_thresholds", "Research indicator thresholds and ranking", "Strategy experiment", "P2",
        "MEASUREMENT_GAP", gate_evidence or ["No live first-failed-gate sample is available."],
        "Identify whether borderline candidates or current rankings systematically miss worthwhile opportunities.",
        "Capture numeric margins for price/OI/volume, EMA separation and confirmation body/wick, plus ranked-out "
        "candidates. Register one parameter family and a fixed trial budget; change either a threshold or a "
        "ranking rule at a time. Any EMA-period change requires recomputing causal features with adequate warm-up.",
        "Independent candidate paths and decision-time availability are verified; one frozen challenger improves "
        "paired net results on future evidence within a predeclared drawdown limit, including realistic costs.",
        "Rejection frequency does not measure filter harm. Numeric threshold margins/rank stability are not "
        "published by the current collector. Observability supplies no validated replacement EMA period or "
        "price/OI/volume/body/wick value; no numeric adjustment is recommended yet.",
        [_evidence("observability_selection_funnel", "Selection Funnel"),
         _evidence("research_attribution", "Decision Attribution")])

    weak = sorted(
        [(name, metrics) for name, metrics in by_setup
         if metrics.get("executed_trades", 0) > 0 and (_number(metrics.get("net_profit_rupees")) or 0) < 0],
        key=lambda item: (float(item[1]["net_profit_rupees"]), item[0]),
    )
    add("setup_selection", "Test one weak setup independently", "Strategy experiment", "P2",
        "RESEARCH_HYPOTHESIS" if weak else ("NO_CURRENT_CANDIDATE" if by_setup else "UNAVAILABLE"),
        [f"{name}: {metrics['executed_trades']} trades, net INR {_fmt(metrics.get('net_profit_rupees'))}, "
         f"PF {_fmt(metrics.get('profit_factor'), 3)}; "
         f"{'small sample (<20)' if metrics['executed_trades'] < 20 else 'historical association only'}."
         for name, metrics in weak] or ["No negative-net setup bucket in the supplied executed-trade sample."
                                        if by_setup else "No setup performance sample is available."],
        "Test whether removing one preregistered setup improves net returns or drawdown.",
        "Select at most one setup exclusion before collecting new evidence; rerun the complete portfolio. "
        "Do not combine the exclusion with risk sizing, regime filters or indicator changes.",
        "Compare daily paired G/challenger P&L, costs, trades and drawdown on reserved future sessions; "
        "report uncertainty and retain the exclusion only if its preregistered acceptance rule passes.",
        "These setups were identified after observing outcomes. Removing historical losses is not proof of "
        "future improvement; freed capital can change other executions. Small buckets cannot justify live removal.",
        [_evidence("observability_regime_profitability", "Regime Profitability")])

    regime_evidence: list[str] = []
    for dimension, groups in regimes.items():
        for name, metrics in groups:
            if not metrics.get("executed_trades", 0):
                continue
            regime_evidence.append(
                f"{dimension} / {name}: {metrics.get('executed_trades', 0)} trades; "
                f"net INR {_fmt(metrics.get('net_profit_rupees'))}; PF {_fmt(metrics.get('profit_factor'), 3)}.")
    add("regime_filter", "Test a decision-time market or liquidity condition", "Strategy experiment", "P2",
        "RESEARCH_HYPOTHESIS" if regime_evidence else "UNAVAILABLE",
        regime_evidence or ["No executed-trade regime evidence is available."],
        "Test whether a predeclared market condition can improve risk-adjusted results.",
        "Choose one existing decision-time regime condition and lock it before evaluation. Add verified VIX "
        "or realized-volatility history before researching conditions that require those missing inputs.",
        "No outcome-derived regime labels; compare paired future results and execution stress against G, "
        "with trade counts and confidence intervals for every bucket.",
        "The same trades appear in different regime dimensions, so their profits cannot be added. Strong "
        "historical buckets are associations, not validated filters; no preferred direction is automatically selected.",
        [_evidence("research_regimes", "Market Regimes and Drift"),
         _evidence("observability_market_regime", "Market Regime")])

    stops = [value for row in historical_rows
             if (value := _number(row.get("native_stop_pct"))) is not None and value > 0]
    add("risk_sizing", "Compare fixed exposure with predefined rupee risk", "Strategy experiment", "P2",
        "RESEARCH_HYPOTHESIS" if stops else "MEASUREMENT_GAP",
        ([f"Persisted selected-order stop distances span {_fmt(min(stops))}% to {_fmt(max(stops))}% "
          f"across {len(stops)} rows."] if stops else ["Per-order native stop distances unavailable in this bundle."]),
        "Make planned risk more consistent across setups; profit may rise or fall.",
        "Keep signals and exit percentages fixed. Size integer shares from a preregistered rupee risk budget, "
        "then apply the same portfolio capital, exposure and liquidity limits as the control.",
        "Exact-G control parity; valid integer/tick accounting; compare net return, intraday mark-to-market "
        "drawdown, turnover and capital usage under the same stress scenarios and future sessions.",
        "This changes sizing, not the indicator thresholds. Stop gaps and fees can exceed planned risk. "
        "Do not select the risk budget by maximizing this already-reviewed history.",
        [_evidence("research_baseline", "Frozen Baseline"),
         _evidence("observability_regime_profitability", "Regime Profitability")])

    add("prediction_validation", "Establish whether predictions beat a simple prior", "Evidence required", "P2",
        "MEASUREMENT_GAP" if prediction_metrics else "UNAVAILABLE",
        [f"Eligible predictions {prediction_metrics.get('eligible_predictions', 'UNAVAILABLE')}/"
         f"{prediction_metrics.get('total_rows', 'UNAVAILABLE')}; fill Brier {_fmt(prediction_metrics.get('fill_brier_score'), 4)}; "
         f"return MAE {_fmt(prediction_metrics.get('net_return_mae_pct_points'), 4)} percentage points."],
        "Determine whether probabilities add usable information beyond historical base rates.",
        "Compare to a chronological expanding prior using identical rows; publish calibration bins, "
        "skill scores and day-block confidence intervals before studying a prediction-based selection rule.",
        "Predictions outperform the registered baseline on untouched future data with adequate sample size; "
        "any resulting selection rule passes a separate cost-aware portfolio and shadow evaluation.",
        "Brier/MAE values without comparators do not establish predictive skill. Selected-order outcomes "
        "cannot train a new ranking policy for unevaluated ranked-out candidates.",
        [_evidence("research_predictions", "Prediction Quality and Calibration")])

    blocked = [name for name, passed in promotion_gates.items() if not passed]
    add("prospective_validation", "Reserve future evidence before accepting an improvement", "Evidence required", "P1",
        "UNAVAILABLE" if not promotion_gates else ("BLOCKED" if blocked else "MANUAL_REVIEW_REQUIRED"),
        [f"Prospective shadow: {shadow.get('state', 'UNAVAILABLE')}; "
         f"{shadow.get('sessions', 'UNAVAILABLE')}/20 completed sessions.",
         "Unpassed gates: " + (", ".join(blocked) if blocked else (
             "none in supplied gate summary" if promotion_gates else "UNAVAILABLE; no gate evidence supplied"))],
        "Distinguish repeatable improvements from selection on previously examined outcomes.",
        "Register one falsifiable change, development/validation windows, an untouched future window and "
        "a trial budget. Freeze the control, sizing/cost assumptions, primary metric and acceptance rule.",
        "Complete the required 20 prospective sessions, untouched holdout and independent review; evaluate "
        "sample adequacy, paired net differences, drawdown and uncertainty before a separate promotion decision.",
        "Twenty sessions is a minimum lifecycle requirement, not statistical proof. These proposals do not "
        "register experiments, alter parameters or authorize live promotion.",
        [_evidence("research_walkforward", "Walk-Forward and Holdout Evaluation"),
         _evidence("research_shadow", "Prospective Shadow and Promotion Gate")])

    items.sort(key=lambda item: (item["priority"], item["id"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "state": "RESEARCH_PROPOSALS_ONLY",
        "baseline": dict(baseline),
        "items": items,
        "counts_by_evidence_state": dict(Counter(item["evidence_state"] for item in items)),
        "operational_read_errors": list(operational.get("errors", [])),
        "estimated_profit_improvement_rupees": None,
        "safe_for_live_selection": False,
        "execution_authority": False,
        "live_configuration_changed": False,
    }


def _text(value: Any) -> str:
    """Keep table structure and raw Markdown/HTML separate from observed labels."""
    return str(value).replace("\r", " ").replace("\n", " ").replace("|", "/").replace("<", "&lt;").replace(">", "&gt;")


def render_improvement_opportunities(payload: Mapping[str, Any]) -> str:
    lines = [
        "## What could improve results",
        "",
        "This section refreshes from the same evidence as the research and observability reports. "
        "It proposes operational work and research experiments; no indicator, threshold, sizing or trading rule is changed.",
        "",
        "Estimated profit uplift: **UNQUANTIFIED**. No candidate has been validated by this report. "
        "All proposals are **PROPOSED_NOT_APPLIED** and require the acceptance evidence below.",
        "",
        "Priorities: P0 execution reliability; P1 measurement, replay validity and prospective evidence; "
        "P2 isolated strategy experiments. An observed problem is evidence of a problem, not proof of a profitable fix.",
        "",
        "| Priority | Opportunity | Type | Evidence state |",
        "| --- | --- | --- | --- |",
    ]
    for item in payload["items"]:
        lines.append("| " + " | ".join(_text(item[key]) for key in (
            "priority", "title", "category", "evidence_state")) + " |")
    errors = payload.get("operational_read_errors", [])
    if errors:
        lines += ["", f"Operational evidence is partial: **{len(errors)} read/parse errors**.", ""]
        lines.extend(f"- {_text(error)}" for error in errors)
    for item in payload["items"]:
        lines += ["", f"## {_text(item['title'])}", "",
                  f"- Priority / evidence: **{item['priority']} / {item['evidence_state']}**."]
        lines.extend(f"- Observed: {_text(observation)}" for observation in item["observations"])
        lines += [
            "- Proof: " + ", ".join(
                f"[{source['label']}](#card-{source['card_id']})" for source in item["sources"]),
            f"- Potential benefit: {_text(item['potential_benefit'])}",
            f"- Proposed test: {_text(item['proposed_experiment'])}",
            f"- Acceptance criteria: {_text(item['acceptance_criteria'])}",
            f"- Limits: {_text(item['limitations'])}",
        ]
    lines += ["", "## Suggested next experiment", "",
              "After validating the common execution model, compare predefined rupee-risk sizing with fixed "
              "exposure while retaining G's signals and exit percentages. A weak-setup exclusion, a regime "
              "filter or an indicator-threshold change must be a separate registered challenger. "
              "No experiment is started by generating this section.", ""]
    return "\n".join(lines)
