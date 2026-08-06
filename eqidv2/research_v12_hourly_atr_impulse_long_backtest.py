"""Locked, research-only V12 replay for the hourly ATR-impulse LONG rule.

This module deliberately imports the proven hourly-membership, canonical-bar,
V12 entry, execution-guard, and exit primitives from
``research_v12_hourly_two_bar_long_backtest``.  It does not edit the V12 setup
book or any live/approved configuration.

The signal contract is frozen from the 2026-06-05..2026-08-04 discovery
window.  A candidate must satisfy all of the following using completed data:

* continuous active hourly LONG membership for both five-minute bars;
* each close-to-close return is in [0.50%, 1.50%];
* compounded two-bar displacement / current causal ATR percent >= 2.50;
* price is at or above causal session VWAP;
* current completed five-minute traded value >= Rs 5,000,000.
* completed price >= Rs 80, positive causal ATR, and range <= 3.5 ATR.

ADX, stochastic, relative-volume, and the legacy context score are recorded
but are never signal gates.  Entry remains the V12 next-available one-minute
open, followed by the existing structural stop, execution guards, statutory
cost model, 1.5R target, follow-through exit, and two-bar-low trail.

The discovery interval is always reported separately.  Only trades strictly
before it can populate the backward-validation qualification gates.  Passing
those gates never promotes this setup; production approval is hard-coded
false.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import pandas as pd

import research_v12_hourly_two_bar_long_backtest as base


SETUP = "L_HOURLY_ATR_IMPULSE_LONG_RESEARCH"
PRODUCTION_APPROVED = False

# Frozen discovery metadata.  These are intentionally not CLI-tunable.
DISCOVERY_START = "2026-06-05"
DISCOVERY_END = "2026-08-04"

RETURN_MIN_PCT = 0.50
RETURN_MAX_PCT = 1.50
MIN_IMPULSE_ATR_RATIO = 2.50
MIN_VWAP_DISTANCE_ATR = 0.0
MIN_TRADED_VALUE_RS = 5_000_000.0
MIN_SIGNAL_PRICE_RS = 80.0
MAX_SIGNAL_RANGE_ATR = 3.50
UPSTREAM_MIN_TRADED_VALUE_RS = 1_000_000.0
ENTRY_START_MINUTE = 9 * 60 + 25
ENTRY_END_MINUTE = 14 * 60 + 30

MIN_PRIOR_TRADES = 100
MIN_VALIDATION_NET_PF = 1.60
MIN_VALIDATION_GROSS_PF = 1.20
MIN_HALF_NET_PF = 1.00

DEFAULT_START = DISCOVERY_START
DEFAULT_END = DISCOVERY_END
DEFAULT_PREFILTER = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_canonical_strict_v2_20260605_20260804_k300"
    r"\hourly_candidates_20260605_20260804_k300.csv"
)
DEFAULT_5M_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\canonical_5m_from_1m_20260526_20260804"
)
DEFAULT_NIFTY_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_hourly_atr_impulse_long_research"
)


def _date(value: object) -> pd.Timestamp:
    """Return a normalized, timezone-naive calendar date."""

    return pd.Timestamp(value).tz_localize(None).normalize()


def compounded_two_bar_pct(
    previous_return_pct: pd.Series,
    current_return_pct: pd.Series,
) -> pd.Series:
    """Causal close displacement across the two completed five-minute bars."""

    previous = pd.to_numeric(previous_return_pct, errors="coerce")
    current = pd.to_numeric(current_return_pct, errors="coerce")
    return ((1.0 + previous / 100.0) * (1.0 + current / 100.0) - 1.0) * 100.0


def add_locked_signal_context(
    states: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Build frozen causal/diagnostic fields without inheriting gate drift.

    ADX, stochastic, RVOL, market context, and candle diagnostics are recorded
    for audit only.  None participates in ``common_gate_pass``.
    """

    work = states.copy()
    work["raw_two_bar_trigger"] = base.mark_first_two_bar_trigger(
        work,
        minimum=RETURN_MIN_PCT,
        maximum=RETURN_MAX_PCT,
    )
    adx = pd.to_numeric(work["signal_adx"], errors="coerce")
    stoch_k = pd.to_numeric(work["stoch_k"], errors="coerce")
    stoch_d = pd.to_numeric(work["stoch_d"], errors="coerce")
    work["score_avwap_price"] = pd.to_numeric(
        work["vwap_dist_atr"], errors="coerce"
    ).ge(0.0)
    work["score_avwap_slope"] = pd.to_numeric(
        work["vwap_slope_3"], errors="coerce"
    ).ge(0.0)
    work["score_dmi"] = pd.to_numeric(
        work["plus_di"], errors="coerce"
    ) > pd.to_numeric(work["minus_di"], errors="coerce")
    work["score_adx"] = adx.ge(25.0) | (
        adx.ge(20.0) & work["adx_rising_3"].astype(bool)
    )
    work["score_stochastic"] = (stoch_k > stoch_d) | work[
        "stoch_rising"
    ].astype(bool)
    work["score_relative_volume"] = pd.to_numeric(
        work["volume_ratio20"], errors="coerce"
    ).ge(1.20)
    work["score_candle_quality"] = pd.to_numeric(
        work["close_location"], errors="coerce"
    ).ge(0.60)
    work["score_market"] = work["nifty_aligned"].astype(bool)
    diagnostic_columns = [
        column for column in work.columns if column.startswith("score_")
    ]
    work["context_score"] = (
        work[diagnostic_columns].fillna(False).astype(int).sum(axis=1)
    )

    close = pd.to_numeric(work["signal_close"], errors="coerce")
    atr = pd.to_numeric(work["signal_atr"], errors="coerce")
    traded_value = pd.to_numeric(work["traded_value_rs"], errors="coerce")
    range_atr = pd.to_numeric(work["range_atr"], errors="coerce")
    work["common_gate_pass"] = (
        work["return_pair_exact"].astype("boolean").fillna(False)
        & close.ge(MIN_SIGNAL_PRICE_RS)
        & atr.gt(0.0)
        & range_atr.le(MAX_SIGNAL_RANGE_ATR)
        & traded_value.ge(UPSTREAM_MIN_TRADED_VALUE_RS)
    )
    raw = work["raw_two_bar_trigger"].astype(bool)
    common = raw & work["common_gate_pass"].astype(bool)
    return work, [
        {
            "stage": "eligible_hourly_long_5m_states",
            "before": int(len(work)),
            "after": int(len(work)),
            "removed": 0,
        },
        {
            "stage": "first_return_pair_per_continuous_streak",
            "before": int(len(work)),
            "after": int(raw.sum()),
            "removed": int(len(work) - raw.sum()),
        },
        {
            "stage": "frozen_price_atr_range_liquidity_data_gate",
            "before": int(raw.sum()),
            "after": int(common.sum()),
            "removed": int(raw.sum() - common.sum()),
        },
    ]


def apply_preregistered_signal_gate(
    states: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Apply the immutable ATR-impulse rule and return its sequential funnel.

    The explicit return bounds are repeated here even though the upstream
    latch produces them.  That makes contract drift visible in unit tests and
    prevents a later upstream change from silently widening this setup.
    """

    work = states.copy()
    previous = pd.to_numeric(work["previous_return_5m_close_pct"], errors="coerce")
    current = pd.to_numeric(work["return_5m_close_pct"], errors="coerce")
    close = pd.to_numeric(work["signal_close"], errors="coerce")
    atr = pd.to_numeric(work["signal_atr"], errors="coerce")
    vwap_distance = pd.to_numeric(work["vwap_dist_atr"], errors="coerce")
    traded_value = pd.to_numeric(work["traded_value_rs"], errors="coerce")

    work["compounded_two_bar_pct"] = compounded_two_bar_pct(previous, current)
    work["current_atr_pct"] = atr / close.replace(0.0, np.nan) * 100.0
    work["impulse_atr_ratio"] = (
        work["compounded_two_bar_pct"]
        / pd.to_numeric(work["current_atr_pct"], errors="coerce").replace(0.0, np.nan)
    )

    raw = work["raw_two_bar_trigger"].astype("boolean").fillna(False)
    common = work["common_gate_pass"].astype("boolean").fillna(False)
    exact_returns = (
        previous.between(RETURN_MIN_PCT, RETURN_MAX_PCT, inclusive="both")
        & current.between(RETURN_MIN_PCT, RETURN_MAX_PCT, inclusive="both")
    )
    stage_return = raw & common & exact_returns
    stage_impulse = stage_return & work["impulse_atr_ratio"].ge(MIN_IMPULSE_ATR_RATIO)
    stage_vwap = stage_impulse & vwap_distance.ge(MIN_VWAP_DISTANCE_ATR)
    stage_liquidity = stage_vwap & traded_value.ge(MIN_TRADED_VALUE_RS)

    work["preregistered_return_pair_pass"] = exact_returns
    work["preregistered_impulse_pass"] = work["impulse_atr_ratio"].ge(
        MIN_IMPULSE_ATR_RATIO
    )
    work["preregistered_vwap_pass"] = vwap_distance.ge(MIN_VWAP_DISTANCE_ATR)
    work["preregistered_liquidity_pass"] = traded_value.ge(MIN_TRADED_VALUE_RS)
    work["preregistered_signal"] = stage_liquidity

    def record(name: str, before: pd.Series, after: pd.Series) -> dict[str, Any]:
        before_count = int(before.sum())
        after_count = int(after.sum())
        return {
            "stage": name,
            "before": before_count,
            "after": after_count,
            "removed": before_count - after_count,
        }

    funnel = [
        {
            "stage": "eligible_hourly_long_5m_states",
            "before": int(len(work)),
            "after": int(len(work)),
            "removed": 0,
        },
        record("first_two_bar_trigger_per_streak", pd.Series(True, index=work.index), raw),
        record("upstream_common_data_execution_gate", raw, raw & common),
        record("locked_each_return_0p50_to_1p50_pct", raw & common, stage_return),
        record("locked_compounded_displacement_ge_2p50_current_atr", stage_return, stage_impulse),
        record("locked_price_at_or_above_causal_session_vwap", stage_impulse, stage_vwap),
        record("locked_completed_5m_traded_value_ge_rs_5m", stage_vwap, stage_liquidity),
    ]
    return work, funnel


def build_candidates(signals: pd.DataFrame) -> pd.DataFrame:
    """Build V12 candidate rows without using soft indicators for priority."""

    out = base.candidate_frame(signals).copy()
    # V12 only uses this score to deduplicate the same ticker/time.  Keeping it
    # independent of ADX/stochastic/RVOL makes their nonbinding status explicit.
    out["quality_score"] = (
        pd.to_numeric(out["impulse_atr_ratio"], errors="coerce").fillna(0.0) * 100.0
        + (301.0 - pd.to_numeric(out["selection_rank"], errors="coerce").fillna(300.0))
        / 100.0
    )
    out["score"] = out["quality_score"]
    out["decision_ready_source"] = "locked_completed_5m_atr_impulse_rule"
    return out


def label_trade_windows(values: pd.Series) -> pd.Series:
    """Label dates as pre-discovery validation, discovery, or post-discovery."""

    dates = pd.to_datetime(values, errors="coerce").dt.tz_localize(None).dt.normalize()
    discovery_start = _date(DISCOVERY_START)
    discovery_end = _date(DISCOVERY_END)
    labels = np.select(
        [dates.lt(discovery_start), dates.le(discovery_end)],
        ["backward_pre_discovery", "discovery"],
        default="post_discovery",
    )
    result = pd.Series(labels, index=values.index, dtype="string")
    return result.mask(dates.isna(), "invalid")


def split_chronological_sessions(sessions: Iterable[str]) -> tuple[list[str], list[str]]:
    ordered = sorted({str(value)[:10] for value in sessions})
    midpoint = len(ordered) // 2
    return ordered[:midpoint], ordered[midpoint:]


def metrics_with_gross_pf(
    trades: pd.DataFrame,
    sessions: Iterable[str],
) -> tuple[dict[str, Any], pd.DataFrame]:
    metrics, daily = base.session_metrics(trades, sessions)
    gross = pd.to_numeric(
        trades.get("gross_pnl_rs", pd.Series(dtype=float)), errors="coerce"
    )
    metrics["gross_profit_factor"] = base._profit_factor(gross)
    metrics["net_profit_factor"] = metrics["profit_factor"]
    return metrics, daily


def evaluate_backward_validation_gates(
    trades: pd.DataFrame,
    sessions: Iterable[str],
) -> dict[str, Any]:
    """Evaluate frozen research qualification gates; never approve production."""

    ordered = sorted({str(value)[:10] for value in sessions})
    metrics, _ = metrics_with_gross_pf(trades, ordered)
    first_sessions, second_sessions = split_chronological_sessions(ordered)
    first = trades.loc[trades.get("trade_date", pd.Series(dtype=str)).isin(first_sessions)].copy()
    second = trades.loc[trades.get("trade_date", pd.Series(dtype=str)).isin(second_sessions)].copy()
    first_metrics, _ = metrics_with_gross_pf(first, first_sessions)
    second_metrics, _ = metrics_with_gross_pf(second, second_sessions)

    checks = {
        "at_least_100_prior_trades": int(metrics["trades"]) >= MIN_PRIOR_TRADES,
        "net_profit_factor_at_least_1p60": float(metrics["net_profit_factor"])
        >= MIN_VALIDATION_NET_PF,
        "gross_profit_factor_at_least_1p20": float(metrics["gross_profit_factor"])
        >= MIN_VALIDATION_GROSS_PF,
        "first_chronological_half_net_pf_above_1": bool(first_sessions)
        and float(first_metrics["net_profit_factor"]) > MIN_HALF_NET_PF,
        "second_chronological_half_net_pf_above_1": bool(second_sessions)
        and float(second_metrics["net_profit_factor"]) > MIN_HALF_NET_PF,
    }
    return {
        "qualification_pass": bool(all(checks.values())),
        "checks": checks,
        "thresholds": {
            "minimum_prior_trades": MIN_PRIOR_TRADES,
            "minimum_net_profit_factor": MIN_VALIDATION_NET_PF,
            "minimum_gross_profit_factor": MIN_VALIDATION_GROSS_PF,
            "minimum_each_half_net_profit_factor_exclusive": MIN_HALF_NET_PF,
        },
        "overall": metrics,
        "first_chronological_half": {
            "sessions": first_sessions,
            "results": first_metrics,
        },
        "second_chronological_half": {
            "sessions": second_sessions,
            "results": second_metrics,
        },
        "production_approved": False,
        "promotion_action": "NONE_RESEARCH_ONLY",
    }


def _window_sessions(sessions: Iterable[str], label: str) -> list[str]:
    frame = pd.DataFrame({"trade_date": sorted({str(value)[:10] for value in sessions})})
    frame["window"] = label_trade_windows(frame["trade_date"])
    return frame.loc[frame["window"].eq(label), "trade_date"].tolist()


def summarize_windows(
    trades: pd.DataFrame,
    sessions: Iterable[str],
) -> tuple[dict[str, Any], pd.DataFrame]:
    work = trades.copy()
    if not work.empty:
        work["research_window"] = label_trade_windows(work["trade_date"])
    results: dict[str, Any] = {}
    daily_frames: list[pd.DataFrame] = []
    for label in ("backward_pre_discovery", "discovery", "post_discovery"):
        subset_sessions = _window_sessions(sessions, label)
        subset = (
            work.loc[work["research_window"].eq(label)].copy()
            if not work.empty
            else work.copy()
        )
        metrics, daily = metrics_with_gross_pf(subset, subset_sessions)
        results[label] = metrics
        daily.insert(0, "research_window", label)
        daily_frames.append(daily)
    combined_daily = (
        pd.concat(daily_frames, ignore_index=True)
        if daily_frames
        else pd.DataFrame()
    )
    return results, combined_daily


@contextmanager
def _isolated_base_runtime(one_minute_dir: Path) -> Iterator[None]:
    """Temporarily point inherited V12 helpers at this isolated setup/root."""

    previous_setup = base.SETUP
    previous_1m_dir = base.v12.v6.DATA_1M_DIR
    base.SETUP = SETUP
    base.v12.v6.DATA_1M_DIR = one_minute_dir
    try:
        yield
    finally:
        base.SETUP = previous_setup
        base.v12.v6.DATA_1M_DIR = previous_1m_dir


def _empty_rejects() -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "setup", "signal_time_ist", "reject_reason"])


def _validate_args(args: argparse.Namespace) -> None:
    if _date(args.start_date) > _date(args.end_date):
        raise ValueError("--start-date must not be after --end-date")
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if not args.prefilter.is_file():
        raise FileNotFoundError(f"prefilter not found: {args.prefilter}")
    if not args.five_minute_dir.is_dir():
        raise FileNotFoundError(f"canonical five-minute directory not found: {args.five_minute_dir}")
    canonical_marker = args.five_minute_dir / "canonical_build_summary.json"
    if not canonical_marker.is_file():
        raise RuntimeError(
            "five-minute root is not marked canonical; expected " + str(canonical_marker)
        )
    if not args.one_minute_dir.is_dir():
        raise FileNotFoundError(f"one-minute directory not found: {args.one_minute_dir}")
    if not args.nifty_five_minute_dir.is_dir():
        raise FileNotFoundError(
            f"NIFTY five-minute directory not found: {args.nifty_five_minute_dir}"
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Locked research-only V12 hourly ATR-impulse LONG replay"
    )
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--prefilter", type=Path, default=DEFAULT_PREFILTER)
    parser.add_argument(
        "--five-minute-dir",
        type=Path,
        default=DEFAULT_5M_DIR,
        help="canonical five-minute root containing canonical_build_summary.json",
    )
    parser.add_argument(
        "--nifty-five-minute-dir", type=Path, default=DEFAULT_NIFTY_5M_DIR
    )
    parser.add_argument("--one-minute-dir", type=Path, default=DEFAULT_1M_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args(argv)


def _report_markdown(summary: dict[str, Any]) -> str:
    windows = summary["window_results"]

    def row(label: str, key: str) -> str:
        metrics = windows[key]
        return (
            f"| {label} | {metrics['sessions']} | {metrics['trades']} | "
            f"{metrics['gross_profit_factor']:.3f} | "
            f"{metrics['net_profit_factor']:.3f} | "
            f"Rs {metrics['net_pnl_rs']:,.2f} |"
        )

    gate = summary["backward_validation_gates"]
    checks = "\n".join(
        f"- {'PASS' if passed else 'FAIL'}: `{name}`"
        for name, passed in gate["checks"].items()
    )
    return f"""# Locked hourly ATR-impulse LONG research replay

**Production approved: NO. Promotion action: NONE.**

Discovery was fixed at {DISCOVERY_START} through {DISCOVERY_END}. Results from
that interval are descriptive and are never allowed to satisfy the backward
validation gate.

## Immutable signal

- two continuous active-membership 5-minute returns, each {RETURN_MIN_PCT:.2f}% to {RETURN_MAX_PCT:.2f}% inclusive;
- only the first qualifying return pair in each uninterrupted return streak is considered;
- compounded displacement / current causal ATR% at least {MIN_IMPULSE_ATR_RATIO:.2f};
- completed price at or above causal session VWAP;
- completed 5-minute traded value at least Rs {MIN_TRADED_VALUE_RS:,.0f};
- completed price at least Rs {MIN_SIGNAL_PRICE_RS:,.0f}, positive ATR, and range no more than {MAX_SIGNAL_RANGE_ATR:.2f} ATR;
- ADX, stochastic, RVOL, and context score recorded but nonbinding;
- original V12 next-available-1m entry (up to its configured delay), structural guards, statutory costs, and full exit;
- at most one selected entry per ticker/day; an already-open trade is never cancelled by an hourly-list refresh.

## Results kept in separate windows

| Window | Sessions | Trades | Gross PF | Net PF | Net P&L |
|---|---:|---:|---:|---:|---:|
{row('Backward pre-discovery validation', 'backward_pre_discovery')}
{row('Discovery (in-sample)', 'discovery')}
{row('Post-discovery observation', 'post_discovery')}

## Backward-validation research gates

Qualification pass: **{'YES' if gate['qualification_pass'] else 'NO'}**. This is
only a research qualification flag and cannot approve production.

{checks}
"""


def run(args: argparse.Namespace) -> dict[str, Any]:
    _validate_args(args)
    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()

    memberships, membership_audit = base.load_long_memberships(
        args.prefilter, args.start_date, args.end_date
    )
    saved_sessions = sorted(
        str(row["trade_date"]) for row in membership_audit["session_rows"]
    )
    eligibility = base.expand_membership_schedule(memberships)
    nifty, expected_sessions = base.load_nifty_context(
        args.nifty_five_minute_dir, args.start_date, args.end_date
    )
    states, feature_audit = base.load_eligible_bar_states(
        eligibility,
        args.five_minute_dir,
        args.one_minute_dir,
        args.start_date,
        args.end_date,
        nifty,
        args.workers,
        False,
    )
    alignment_records = feature_audit.pop("_alignment_records", [])
    required_prices = [
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
        "signal_volume",
    ]
    missing_price_rows = states[required_prices].isna().any(axis=1)
    feature_audit["missing_required_price_rows"] = int(missing_price_rows.sum())
    feature_audit["missing_required_price_pct"] = float(
        missing_price_rows.mean() * 100.0 if len(states) else 0.0
    )
    states, upstream_funnel = add_locked_signal_context(states)
    states, locked_funnel = apply_preregistered_signal_gate(states)
    signals = states.loc[states["preregistered_signal"]].copy()

    candidates = pd.DataFrame()
    raw_entries = pd.DataFrame()
    entry_rejects = _empty_rejects()
    selected = pd.DataFrame()
    trades = pd.DataFrame()
    prewarm: dict[str, Any] = {"loaded": 0, "missing": 0, "failed": 0}

    with _isolated_base_runtime(args.one_minute_dir):
        if not signals.empty:
            candidates = build_candidates(signals)
            raw_entries, entry_rejects, prewarm = base.install_v12_entry(
                candidates, args.start_date, args.end_date, args.workers
            )
            if not raw_entries.empty:
                raw_entries = base.add_execution_guards(raw_entries)
                executable = raw_entries.loc[
                    raw_entries["execution_guard_pass"].astype(bool)
                ].copy()
                if not executable.empty:
                    selected = base.v12._select_v7_entry_engine_signals(executable)
                    trades = base.resolve_policy(selected, base.PRIMARY_POLICY, SETUP)

    window_results, window_daily = summarize_windows(trades, saved_sessions)
    pre_sessions = _window_sessions(saved_sessions, "backward_pre_discovery")
    if trades.empty:
        pre_trades = trades.copy()
    else:
        trade_windows = label_trade_windows(trades["trade_date"])
        pre_trades = trades.loc[trade_windows.eq("backward_pre_discovery")].copy()
    validation_gates = evaluate_backward_validation_gates(pre_trades, pre_sessions)

    if selected.empty:
        boundary = selected.copy()
        boundary_owner_ok: bool | None = None
    else:
        signal_times = selected["signal_time_ist"].map(base._timestamp_ist)
        boundary = selected.loc[signal_times.map(lambda value: value.minute == 20)].copy()
        boundary_owner_ok = (
            bool(
                (
                    pd.to_datetime(boundary["signal_time_ist"], utc=True)
                    - pd.to_datetime(boundary["slot_ist"], utc=True)
                )
                .dt.total_seconds()
                .eq(3600.0)
                .all()
            )
            if not boundary.empty
            else None
        )

    expected_set = set(expected_sessions)
    saved_set = set(saved_sessions)
    summary: dict[str, Any] = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "promotion_action": "NONE_RESEARCH_ONLY",
        "discovery_window": {
            "start": DISCOVERY_START,
            "end": DISCOVERY_END,
            "role": "in_sample_discovery_only",
            "eligible_for_validation_gates": False,
        },
        "requested_data_window": {
            "start": args.start_date,
            "end": args.end_date,
            "saved_prefilter_sessions": len(saved_sessions),
            "expected_market_sessions": len(expected_sessions),
            "missing_prefilter_sessions": sorted(expected_set - saved_set),
        },
        "window_results": window_results,
        "backward_validation_gates": validation_gates,
        "membership_audit": membership_audit,
        "feature_audit": feature_audit,
        "upstream_funnel": upstream_funnel,
        "locked_signal_funnel": locked_funnel,
        "entry_engine": {
            "locked_candidates": int(len(candidates)),
            "raw_entries": int(len(raw_entries)),
            "execution_guard_pass": int(
                raw_entries.get("execution_guard_pass", pd.Series(dtype=bool)).sum()
            ),
            "selected_first_ticker_day": int(len(selected)),
            "resolved_trades": int(len(trades)),
            "rejects": int(len(entry_rejects)),
            "prewarm": prewarm,
        },
        "boundary_audit": {
            "selected_signals_at_xx20": int(len(boundary)),
            "all_owned_by_previous_hourly_list": boundary_owner_ok,
            "ownership_check_status": (
                "passed" if boundary_owner_ok is True else "not_observed"
            ),
            "open_positions_recalculated_at_refresh": False,
            "open_position_refresh_policy": "position persists; refresh affects future eligibility only",
        },
        "runtime_seconds": time.time() - started,
        "limitations": [
            "discovery thresholds were found on 2026-06-05..2026-08-04 and cannot validate themselves",
            "backward validation is weaker than a later forward holdout and remains research-only",
            "static current universe may contain survivorship bias unless the supplied prefilter has point-in-time constituents",
            "historical quoted spreads are unavailable; V12 statutory costs and entry slippage are used",
            "one current statutory-rate table is applied to the older history",
            "the V12 selector permits at most one selected entry per ticker/day",
            "no portfolio-overlap capital constraint is applied",
        ],
    }

    contract = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "discovery_window_locked": [DISCOVERY_START, DISCOVERY_END],
        "signal": {
            "side": "LONG",
            "continuous_active_hourly_membership_bars": 2,
            "latch": "first qualifying return pair per uninterrupted return streak",
            "entry_signal_time_window_ist_inclusive": ["09:25", "14:30"],
            "each_completed_5m_return_pct_inclusive": [
                RETURN_MIN_PCT,
                RETURN_MAX_PCT,
            ],
            "compounded_displacement_over_current_causal_atr_pct_min": MIN_IMPULSE_ATR_RATIO,
            "vwap_dist_atr_min": MIN_VWAP_DISTANCE_ATR,
            "completed_5m_traded_value_rs_min": MIN_TRADED_VALUE_RS,
            "completed_price_rs_min": MIN_SIGNAL_PRICE_RS,
            "causal_atr_must_be_positive": True,
            "completed_range_atr_max": MAX_SIGNAL_RANGE_ATR,
            "adx_hard_gate": False,
            "stochastic_hard_gate": False,
            "relative_volume_hard_gate": False,
            "context_score_hard_gate": False,
        },
        "execution": {
            "entry": "V12 next available 1m open after completed signal",
            "entry_search_max_delay_minutes": base.v12.V7_ENTRY_SEARCH_MAX_DELAY_MIN,
            "entry_slippage_pct": base.v12.V7_PAPER_SLIPPAGE_PCT,
            "structure_stop": "signal low - 0.10 current ATR",
            "maximum_stop_distance_atr": base.MAX_STOP_DISTANCE_ATR,
            "maximum_entry_gap_atr": base.MAX_ENTRY_GAP_ATR,
            "maximum_order_participation": base.MAX_ORDER_PARTICIPATION,
            "selected_entry_limit": "at most one entry per ticker/day",
            "cost_model": "NSE statutory intraday equity",
            "exit_policy": base.PRIMARY_POLICY.name,
            "target_r": base.PRIMARY_POLICY.target_r,
            "conditional_time_stop": base.PRIMARY_POLICY.conditional_time_stop,
            "two_bar_low_trail": base.PRIMARY_POLICY.two_bar_low_trail,
            "risk_sizing": {
                "enabled": base.v12.RISK_SIZING_ENABLED,
                "equity_rs": base.v12.RISK_EQUITY_RS,
                "risk_pct_per_trade": base.v12.RISK_PCT_PER_TRADE,
                "minimum_notional_rs": base.v12.RISK_MIN_NOTIONAL_RS,
                "maximum_notional_rs": base.v12.RISK_MAX_NOTIONAL_RS,
            },
        },
        "validation_gates": validation_gates["thresholds"],
        "promotion": "never automatic; production_approved remains false",
    }

    pd.DataFrame(locked_funnel).to_csv(args.out / "locked_signal_funnel.csv", index=False)
    pd.DataFrame(alignment_records).to_csv(
        args.out / "timestamp_alignment_audit.csv", index=False
    )
    signals.to_csv(args.out / "locked_signal_states.csv", index=False)
    candidates.to_csv(args.out / "v12_input_candidates.csv", index=False)
    entry_rejects.to_csv(args.out / "entry_engine_rejects.csv", index=False)
    raw_entries.to_csv(args.out / "entry_engine_raw_entries.csv", index=False)
    selected.to_csv(args.out / "selected_entries.csv", index=False)
    boundary.to_csv(args.out / "boundary_audit.csv", index=False)
    trades.to_csv(args.out / "trades.csv", index=False)
    window_daily.to_csv(args.out / "window_daily_summary.csv", index=False)
    (args.out / "filter_contract.json").write_text(
        json.dumps(base._json_value(contract), indent=2), encoding="utf-8"
    )
    (args.out / "summary.json").write_text(
        json.dumps(base._json_value(summary), indent=2), encoding="utf-8"
    )
    (args.out / "RESEARCH_REPORT.md").write_text(
        _report_markdown(summary), encoding="utf-8"
    )

    artifacts = []
    for path in sorted(args.out.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append(
                {
                    "file": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": base._sha256(path),
                }
            )
    canonical_marker = args.five_minute_dir / "canonical_build_summary.json"
    strict_prefilter_contract = next(
        (
            path
            for path in (
                args.prefilter.parent / "strict_entry_contract.json",
                args.prefilter.parent / "strict_contract.json",
            )
            if path.is_file()
        ),
        None,
    )
    nifty_source = (
        args.nifty_five_minute_dir / "NIFTY_stocks_indicators_5min.parquet"
    )
    manifest_inputs = {
        "prefilter": str(args.prefilter.resolve()),
        "prefilter_sha256": base._sha256(args.prefilter),
        "five_minute_dir": str(args.five_minute_dir.resolve()),
        "canonical_build_summary": str(canonical_marker.resolve()),
        "canonical_build_summary_sha256": base._sha256(canonical_marker),
        "nifty_five_minute_dir": str(args.nifty_five_minute_dir.resolve()),
        "one_minute_dir": str(args.one_minute_dir.resolve()),
        "adapter_source": str(Path(__file__).resolve()),
        "adapter_source_sha256": base._sha256(Path(__file__)),
        "inherited_research_source": str(Path(base.__file__).resolve()),
        "inherited_research_source_sha256": base._sha256(Path(base.__file__)),
    }
    if strict_prefilter_contract is not None:
        manifest_inputs.update(
            {
                "strict_prefilter_contract": str(strict_prefilter_contract.resolve()),
                "strict_prefilter_contract_sha256": base._sha256(
                    strict_prefilter_contract
                ),
            }
        )
    if nifty_source.is_file():
        manifest_inputs.update(
            {
                "nifty_five_minute_source": str(nifty_source.resolve()),
                "nifty_five_minute_source_sha256": base._sha256(nifty_source),
            }
        )
    manifest = {
        "production_approved": False,
        "promotion_action": "NONE_RESEARCH_ONLY",
        "inputs": manifest_inputs,
        "artifacts": artifacts,
    }
    (args.out / "integrity_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run(args)
    print(json.dumps(base._json_value(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
