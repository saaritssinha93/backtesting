"""Research-only V12 replay for a two-bar impulse followed by a pullback reclaim.

This is a causal redesign of ``L_HOURLY_TWO_BAR_5M_MOMENTUM``.  It does not
enter immediately after the two qualifying impulse bars.  It waits for one to
three completed pullback bars, requires the impulse base and session VWAP area
to hold, and then requires a bullish break of the pullback high.  Entry remains
the next available one-minute open through V12.

The four variants below are deliberately specified in code before evaluation.
They are comparisons, not a parameter search.  The acceptance gate requires a
time-separated validation PF >= 1.60 with adequate trade count and supporting
gross expectancy; a high in-sample PF alone is never promoted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import avwap_5min_ID_v12_backtesting as v12
import research_v12_hourly_two_bar_long_backtest as impulse
import research_v12_prefilter_train_test_optimizer as optimizer


IST = "Asia/Kolkata"
SETUP = "L_HOURLY_TWO_BAR_PULLBACK_RECLAIM"
PRODUCTION_APPROVED = False
PRIMARY_VARIANT = "market_confirmed"

DEFAULT_START = "2026-06-05"
DEFAULT_END = "2026-08-04"
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
    r"\v12_hourly_pullback_long_20260605_20260804"
)

PULLBACK_MIN_BARS = 1
PULLBACK_MAX_BARS = 3
BROAD_RETRACE_MIN = 0.05
BROAD_RETRACE_MAX = 0.80
BROAD_MIN_HOLD_FRACTION = 0.00
BROAD_VWAP_FLOOR_ATR = -0.25
BROAD_MAX_CONFIRM_VWAP_ATR = 2.50
BROAD_MAX_CONFIRM_RETURN_PCT = 1.00
BROAD_MIN_CONFIRM_RETURN_PCT = 0.05
BROAD_MIN_CONFIRM_CLOSE_LOCATION = 0.55
BROAD_MAX_CONFIRM_RANGE_ATR = 2.50

STOP_BUFFER_ATR = 0.10
MAX_STOP_DISTANCE_ATR = 1.00
MAX_ENTRY_GAP_ATR = 0.15
MAX_ORDER_PARTICIPATION = 0.02
TARGET_R = 2.00

MIN_VALIDATION_TRADES = 40
MIN_FULL_TRADES = 100
MIN_VALIDATION_NET_PF = 1.60
MIN_VALIDATION_GROSS_PF = 1.20
MIN_DEVELOPMENT_NET_PF = 1.05


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    return impulse._json_value(value)


def _numeric(value: Any, default: float = math.nan) -> float:
    number = pd.to_numeric(value, errors="coerce")
    return float(number) if pd.notna(number) else default


def _profit_factor(values: pd.Series) -> float:
    return impulse._profit_factor(values)


def _contiguous(rows: pd.DataFrame) -> bool:
    if len(rows) <= 1:
        return True
    times = pd.to_datetime(rows["signal_time_ist"], utc=True)
    return times.diff().dropna().dt.total_seconds().eq(300.0).all()


def find_pullback_reclaims(states: pd.DataFrame) -> pd.DataFrame:
    """Find the first causal pullback reclaim after each latched impulse."""
    if states.empty:
        return pd.DataFrame()
    work = states.sort_values(
        ["ticker", "trade_date", "signal_time_ist"], kind="mergesort"
    ).reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for _, group in work.groupby(["ticker", "trade_date"], sort=False):
        group = group.reset_index(drop=True)
        trigger_positions = np.flatnonzero(
            group["raw_two_bar_trigger"].astype(bool).to_numpy()
            & group["common_gate_pass"].astype(bool).to_numpy()
        )
        for position in trigger_positions:
            if position < 1:
                continue
            previous = group.iloc[position - 1]
            impulse_row = group.iloc[position]
            pair = group.iloc[position - 1 : position + 1]
            if not _contiguous(pair):
                continue
            previous_return = _numeric(previous.get("return_5m_close_pct"))
            previous_close = _numeric(previous.get("signal_close"))
            atr = _numeric(impulse_row.get("signal_atr"))
            if not (
                math.isfinite(previous_return)
                and previous_return > -100.0
                and previous_close > 0.0
                and atr > 0.0
            ):
                continue
            impulse_start_close = previous_close / (1.0 + previous_return / 100.0)
            impulse_high = max(
                _numeric(previous.get("signal_high")),
                _numeric(impulse_row.get("signal_high")),
            )
            impulse_range = impulse_high - impulse_start_close
            if not (math.isfinite(impulse_range) and impulse_range > 0.0):
                continue
            impulse_volume = np.nanmean(
                [
                    _numeric(previous.get("signal_volume")),
                    _numeric(impulse_row.get("signal_volume")),
                ]
            )
            impulse_bar_range = np.nanmean(
                [
                    _numeric(previous.get("signal_high"))
                    - _numeric(previous.get("signal_low")),
                    _numeric(impulse_row.get("signal_high"))
                    - _numeric(impulse_row.get("signal_low")),
                ]
            )

            for pullback_bars in range(PULLBACK_MIN_BARS, PULLBACK_MAX_BARS + 1):
                confirm_position = position + pullback_bars + 1
                if confirm_position >= len(group):
                    break
                spell = group.iloc[position : confirm_position + 1]
                if not _contiguous(spell):
                    break
                pullback = group.iloc[position + 1 : confirm_position]
                confirm = group.iloc[confirm_position]
                pullback_closes = pd.to_numeric(
                    pullback["signal_close"], errors="coerce"
                )
                pullback_lows = pd.to_numeric(pullback["signal_low"], errors="coerce")
                pullback_highs = pd.to_numeric(pullback["signal_high"], errors="coerce")
                pullback_volumes = pd.to_numeric(
                    pullback["signal_volume"], errors="coerce"
                )
                pullback_ranges = pullback_highs - pullback_lows
                if (
                    pullback_closes.isna().any()
                    or pullback_lows.isna().any()
                    or pullback_highs.isna().any()
                ):
                    continue
                prior_closes = pd.concat(
                    [
                        pd.Series([_numeric(impulse_row.get("signal_close"))]),
                        pullback_closes.iloc[:-1].reset_index(drop=True),
                    ],
                    ignore_index=True,
                )
                has_down_close = bool(
                    (pullback_closes.reset_index(drop=True) < prior_closes).any()
                )
                pullback_low = float(pullback_lows.min())
                pullback_high = float(pullback_highs.max())
                retrace = (impulse_high - pullback_low) / impulse_range
                hold_level = impulse_start_close + BROAD_MIN_HOLD_FRACTION * impulse_range
                pullback_vwap_floor = pd.to_numeric(
                    pullback["session_vwap_causal"], errors="coerce"
                ) + BROAD_VWAP_FLOOR_ATR * pd.to_numeric(
                    pullback["signal_atr"], errors="coerce"
                )
                pullback_holds_vwap = bool(
                    (pullback_closes.to_numpy() >= pullback_vwap_floor.to_numpy()).all()
                )
                confirm_open = _numeric(confirm.get("signal_open"))
                confirm_close = _numeric(confirm.get("signal_close"))
                confirm_return = _numeric(confirm.get("return_5m_close_pct"))
                confirm_location = _numeric(confirm.get("close_location"))
                confirm_vwap_dist = _numeric(confirm.get("vwap_dist_atr"))
                confirm_range_atr = _numeric(confirm.get("range_atr"))
                confirm_gap = _numeric(confirm.get("gap_filled"), 1.0)
                broad_pass = (
                    has_down_close
                    and BROAD_RETRACE_MIN <= retrace <= BROAD_RETRACE_MAX
                    and float(pullback_closes.min()) >= hold_level
                    and pullback_holds_vwap
                    and confirm_gap < 0.5
                    and confirm_close > confirm_open
                    and confirm_close > pullback_high
                    and confirm_close > _numeric(confirm.get("session_vwap_causal"))
                    and BROAD_MIN_CONFIRM_RETURN_PCT
                    <= confirm_return
                    <= BROAD_MAX_CONFIRM_RETURN_PCT
                    and confirm_location >= BROAD_MIN_CONFIRM_CLOSE_LOCATION
                    and -0.10 <= confirm_vwap_dist <= BROAD_MAX_CONFIRM_VWAP_ATR
                    and confirm_range_atr <= BROAD_MAX_CONFIRM_RANGE_ATR
                    and _numeric(confirm.get("traded_value_rs"))
                    >= impulse.MIN_5M_TRADED_VALUE_RS
                )
                if not broad_pass:
                    continue
                record = confirm.to_dict()
                record.update(
                    {
                        "impulse_time_ist": impulse_row["signal_time_ist"],
                        "impulse_membership_slot_ist": impulse_row["slot_ist"],
                        "impulse_start_close": impulse_start_close,
                        "impulse_high": impulse_high,
                        "impulse_move_pct": (
                            _numeric(impulse_row.get("signal_close"))
                            / impulse_start_close
                            - 1.0
                        )
                        * 100.0,
                        "impulse_move_atr": impulse_range / atr,
                        "pullback_bars": pullback_bars,
                        "pullback_low": pullback_low,
                        "pullback_high": pullback_high,
                        "pullback_retrace": retrace,
                        "pullback_volume_ratio": (
                            float(pullback_volumes.mean()) / impulse_volume
                            if impulse_volume > 0.0
                            else math.nan
                        ),
                        "pullback_range_ratio": (
                            float(pullback_ranges.mean()) / impulse_bar_range
                            if impulse_bar_range > 0.0
                            else math.nan
                        ),
                        "confirmation_return_pct": confirm_return,
                        "confirmation_close_location": confirm_location,
                        "confirmation_vwap_dist_atr": confirm_vwap_dist,
                        "confirmation_volume_ratio20": _numeric(
                            confirm.get("volume_ratio20")
                        ),
                        "confirmation_adx": _numeric(confirm.get("signal_adx")),
                        "confirmation_plus_di": _numeric(confirm.get("plus_di")),
                        "confirmation_minus_di": _numeric(confirm.get("minus_di")),
                        "confirmation_nifty_aligned": bool(
                            confirm.get("nifty_aligned", False)
                        ),
                    }
                )
                records.append(record)
                break
    return pd.DataFrame(records)


def make_candidates(patterns: pd.DataFrame) -> pd.DataFrame:
    candidates = impulse.candidate_frame(patterns)
    candidates["setup"] = SETUP
    candidates["side"] = "LONG"
    candidates["decision_ready_source"] = "completed_pullback_reclaim_5m"
    retrace_quality = 1.0 - (
        pd.to_numeric(candidates["pullback_retrace"], errors="coerce") - 0.35
    ).abs()
    candidates["quality_score"] = (
        500.0
        + 40.0 * retrace_quality.fillna(0.0)
        + 15.0
        * pd.to_numeric(candidates["confirmation_close_location"], errors="coerce").fillna(0.0)
        + 10.0
        * pd.to_numeric(candidates["confirmation_volume_ratio20"], errors="coerce").fillna(0.0).clip(0, 3)
        - pd.to_numeric(candidates["selection_rank"], errors="coerce").fillna(300.0) / 100.0
    )
    candidates["score"] = candidates["quality_score"]
    return candidates


def install_v12_entry(
    candidates: pd.DataFrame,
    start_date: str,
    end_date: str,
    workers: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    loader = optimizer.install_windowed_1m_loader(
        v12, start_date=start_date, end_date=end_date
    )
    prewarm = optimizer.prewarm_windowed_1m_loader(
        loader, candidates["ticker"], workers=workers
    )
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0
    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (1.0, TARGET_R)
    try:
        raw, rejects = v12._v7_entry_engine_raw_rows(candidates)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit
    if rejects is None or (rejects.empty and len(rejects.columns) == 0):
        rejects = pd.DataFrame(
            columns=["ticker", "setup", "signal_time_ist", "reject_reason"]
        )
    return raw, rejects, prewarm


def add_execution_guards(raw: pd.DataFrame) -> pd.DataFrame:
    work = raw.copy()
    raw_entry = pd.to_numeric(work["v7_signal_entry_price"], errors="coerce")
    atr = pd.to_numeric(work["signal_atr"], errors="coerce")
    signal_close = pd.to_numeric(work["signal_close"], errors="coerce")
    work["entry_gap_atr"] = (raw_entry - signal_close) / atr.replace(0.0, np.nan)
    work["entry_price_with_slippage"] = (
        raw_entry * (1.0 + v12.V7_PAPER_SLIPPAGE_PCT)
    ).round(2)
    work["structure_stop_price"] = (
        pd.to_numeric(work["pullback_low"], errors="coerce") - STOP_BUFFER_ATR * atr
    ).round(2)
    work["structure_risk_per_share"] = (
        work["entry_price_with_slippage"] - work["structure_stop_price"]
    )
    work["structure_stop_distance_atr"] = (
        work["structure_risk_per_share"] / atr.replace(0.0, np.nan)
    )
    quantities = []
    for row in work.itertuples():
        entry = float(row.entry_price_with_slippage)
        stop = float(row.structure_stop_price)
        quantities.append(v12._risk_based_qty(entry, stop) if entry > stop > 0 else 0)
    work["quantity"] = quantities
    work["order_participation"] = (
        pd.to_numeric(work["quantity"], errors="coerce")
        / pd.to_numeric(work["signal_volume"], errors="coerce").replace(0.0, np.nan)
    )
    work["execution_guard_pass"] = (
        work["entry_gap_atr"].le(MAX_ENTRY_GAP_ATR)
        & work["structure_risk_per_share"].gt(0.0)
        & work["structure_stop_distance_atr"].le(MAX_STOP_DISTANCE_ATR)
        & pd.to_numeric(work["quantity"], errors="coerce").gt(0)
        & work["order_participation"].le(MAX_ORDER_PARTICIPATION)
    )
    return work


def variant_masks(raw: pd.DataFrame) -> dict[str, pd.Series]:
    retrace = pd.to_numeric(raw["pullback_retrace"], errors="coerce")
    volume = pd.to_numeric(raw["pullback_volume_ratio"], errors="coerce")
    range_ratio = pd.to_numeric(raw["pullback_range_ratio"], errors="coerce")
    location = pd.to_numeric(raw["confirmation_close_location"], errors="coerce")
    vwap = pd.to_numeric(raw["confirmation_vwap_dist_atr"], errors="coerce")
    confirm_volume = pd.to_numeric(
        raw["confirmation_volume_ratio20"], errors="coerce"
    )
    adx = pd.to_numeric(raw["confirmation_adx"], errors="coerce")
    plus = pd.to_numeric(raw["confirmation_plus_di"], errors="coerce")
    minus = pd.to_numeric(raw["confirmation_minus_di"], errors="coerce")
    nifty = raw["confirmation_nifty_aligned"].astype("boolean").fillna(False)
    balanced = (
        retrace.between(0.15, 0.65)
        & volume.le(1.10)
        & range_ratio.le(1.20)
        & location.ge(0.62)
        & vwap.between(0.0, 1.75)
        & confirm_volume.ge(0.80)
    )
    market = balanced & nifty & (plus > minus) & adx.ge(20.0)
    high_quality = (
        retrace.between(0.20, 0.55)
        & volume.le(0.90)
        & range_ratio.le(1.00)
        & location.ge(0.70)
        & vwap.between(0.0, 1.25)
        & confirm_volume.ge(1.10)
        & nifty
        & (plus > minus)
        & adx.ge(22.0)
    )
    return {
        "broad_pattern": pd.Series(True, index=raw.index),
        "balanced": balanced,
        "market_confirmed": market,
        "high_quality": high_quality,
    }


def _window_metrics(trades: pd.DataFrame, sessions: Iterable[str]) -> dict[str, Any]:
    metrics, _ = impulse.session_metrics(trades, sessions)
    metrics["gross_profit_factor"] = _profit_factor(
        pd.to_numeric(trades.get("gross_pnl_rs", pd.Series(dtype=float)), errors="coerce")
    )
    return metrics


def split_metrics(trades: pd.DataFrame, sessions: list[str]) -> dict[str, Any]:
    split = len(sessions) // 2
    development_sessions = sessions[:split]
    validation_sessions = sessions[split:]
    development = trades.loc[
        trades.get("trade_date", pd.Series(dtype=str)).isin(development_sessions)
    ].copy()
    validation = trades.loc[
        trades.get("trade_date", pd.Series(dtype=str)).isin(validation_sessions)
    ].copy()
    full_metrics = _window_metrics(trades, sessions)
    development_metrics = _window_metrics(development, development_sessions)
    validation_metrics = _window_metrics(validation, validation_sessions)
    accepted = (
        full_metrics["trades"] >= MIN_FULL_TRADES
        and validation_metrics["trades"] >= MIN_VALIDATION_TRADES
        and (validation_metrics["profit_factor"] or 0.0) >= MIN_VALIDATION_NET_PF
        and (validation_metrics["gross_profit_factor"] or 0.0)
        >= MIN_VALIDATION_GROSS_PF
        and (development_metrics["profit_factor"] or 0.0)
        >= MIN_DEVELOPMENT_NET_PF
    )
    return {
        "full": full_metrics,
        "development": development_metrics,
        "validation": validation_metrics,
        "accepted": bool(accepted),
        "acceptance_failures": [
            label
            for label, passed in (
                ("full_trades", full_metrics["trades"] >= MIN_FULL_TRADES),
                (
                    "validation_trades",
                    validation_metrics["trades"] >= MIN_VALIDATION_TRADES,
                ),
                (
                    "validation_net_pf",
                    (validation_metrics["profit_factor"] or 0.0)
                    >= MIN_VALIDATION_NET_PF,
                ),
                (
                    "validation_gross_pf",
                    (validation_metrics["gross_profit_factor"] or 0.0)
                    >= MIN_VALIDATION_GROSS_PF,
                ),
                (
                    "development_net_pf",
                    (development_metrics["profit_factor"] or 0.0)
                    >= MIN_DEVELOPMENT_NET_PF,
                ),
            )
            if not passed
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="V12 hourly LONG two-bar pullback-reclaim research replay"
    )
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--prefilter", type=Path, default=DEFAULT_PREFILTER)
    parser.add_argument("--five-minute-dir", type=Path, default=DEFAULT_5M_DIR)
    parser.add_argument(
        "--nifty-five-minute-dir", type=Path, default=DEFAULT_NIFTY_5M_DIR
    )
    parser.add_argument("--one-minute-dir", type=Path, default=DEFAULT_1M_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    memberships, membership_audit = impulse.load_long_memberships(
        args.prefilter, args.start_date, args.end_date
    )
    eligibility = impulse.expand_membership_schedule(memberships)
    nifty, expected_sessions = impulse.load_nifty_context(
        args.nifty_five_minute_dir, args.start_date, args.end_date
    )
    states, feature_audit = impulse.load_eligible_bar_states(
        eligibility,
        args.five_minute_dir,
        args.one_minute_dir,
        args.start_date,
        args.end_date,
        nifty,
        args.workers,
        False,
    )
    feature_audit.pop("_alignment_records", None)
    states, impulse_funnel = impulse.add_signal_rules(states)
    patterns = find_pullback_reclaims(states)
    if patterns.empty:
        raise RuntimeError("no pullback-reclaim patterns")
    candidates = make_candidates(patterns)
    raw_entries, rejects, prewarm = install_v12_entry(
        candidates, args.start_date, args.end_date, args.workers
    )
    raw_entries = add_execution_guards(raw_entries)
    masks = variant_masks(raw_entries)
    sessions = sorted(memberships["trade_date"].unique())
    policy = impulse.ExitPolicy(
        "pullback_reclaim_2r_time_trail",
        target_r=TARGET_R,
        conditional_time_stop=True,
        two_bar_low_trail=True,
    )
    original_setup = impulse.SETUP
    impulse.SETUP = SETUP
    variants: dict[str, Any] = {}
    trade_frames: dict[str, pd.DataFrame] = {}
    selected_frames: dict[str, pd.DataFrame] = {}
    try:
        for name, mask in masks.items():
            executable = raw_entries.loc[
                mask & raw_entries["execution_guard_pass"].astype(bool)
            ].copy()
            selected = v12._select_v7_entry_engine_signals(executable)
            trades = impulse.resolve_policy(selected, policy, name)
            evaluation = split_metrics(trades, sessions)
            evaluation.update(
                {
                    "pattern_candidates": int(mask.sum()),
                    "execution_guard_pass": int(len(executable)),
                    "selected": int(len(selected)),
                }
            )
            variants[name] = evaluation
            trade_frames[name] = trades
            selected_frames[name] = selected
    finally:
        impulse.SETUP = original_setup

    primary_trades = trade_frames[PRIMARY_VARIANT]
    primary_selected = selected_frames[PRIMARY_VARIANT]
    accepted_variants = [name for name, result in variants.items() if result["accepted"]]
    summary = {
        "production_approved": False,
        "setup": SETUP,
        "primary_variant": PRIMARY_VARIANT,
        "window": {
            "start": args.start_date,
            "end": args.end_date,
            "sessions": len(sessions),
            "development_sessions": len(sessions) // 2,
            "validation_sessions": len(sessions) - len(sessions) // 2,
            "development_end": sessions[len(sessions) // 2 - 1],
            "validation_start": sessions[len(sessions) // 2],
        },
        "acceptance_gate": {
            "minimum_full_trades": MIN_FULL_TRADES,
            "minimum_validation_trades": MIN_VALIDATION_TRADES,
            "minimum_validation_net_pf": MIN_VALIDATION_NET_PF,
            "minimum_validation_gross_pf": MIN_VALIDATION_GROSS_PF,
            "minimum_development_net_pf": MIN_DEVELOPMENT_NET_PF,
        },
        "accepted_variants": accepted_variants,
        "honest_pf_1p6_supported": bool(accepted_variants),
        "membership_audit": membership_audit,
        "feature_audit": feature_audit,
        "impulse_funnel": impulse_funnel,
        "pullback_patterns": int(len(patterns)),
        "entry_engine": {
            "raw_entries": int(len(raw_entries)),
            "rejects": int(len(rejects)),
            "prewarm": prewarm,
        },
        "variants": variants,
        "runtime_seconds": time.time() - started,
        "limitations": [
            "the 42-session window has already been inspected and is not a fresh holdout",
            "hourly K300 lists are reconstructed shadow research lists using a static current universe",
            "sector history and historical quoted spread are unavailable",
            "no portfolio-overlap capital constraint is applied",
        ],
    }

    patterns.to_csv(args.out / "pullback_patterns.csv", index=False)
    candidates.to_csv(args.out / "v12_input_candidates.csv", index=False)
    raw_entries.to_csv(args.out / "entry_engine_raw_entries.csv", index=False)
    rejects.to_csv(args.out / "entry_engine_rejects.csv", index=False)
    primary_selected.to_csv(args.out / "primary_selected_entries.csv", index=False)
    primary_trades.to_csv(args.out / "primary_trades.csv", index=False)
    for name, trades in trade_frames.items():
        trades.to_csv(args.out / f"trades_{name}.csv", index=False)
    pd.DataFrame(
        [
            {
                "variant": name,
                "accepted": result["accepted"],
                "acceptance_failures": "|".join(result["acceptance_failures"]),
                **{f"full_{key}": value for key, value in result["full"].items()},
                **{
                    f"development_{key}": value
                    for key, value in result["development"].items()
                },
                **{
                    f"validation_{key}": value
                    for key, value in result["validation"].items()
                },
            }
            for name, result in variants.items()
        ]
    ).to_csv(args.out / "variant_summary.csv", index=False)
    (args.out / "summary.json").write_text(
        json.dumps(_json_value(summary), indent=2), encoding="utf-8"
    )
    report = (
        "# V12 hourly pullback-reclaim LONG research\n\n"
        f"Primary pre-registered variant: `{PRIMARY_VARIANT}`. "
        f"PF>=1.6 honestly supported: **{bool(accepted_variants)}**.\n\n"
        "A variant is accepted only when it clears every stored development/validation "
        "gate, including minimum trades and gross PF. This run is research-only and "
        "cannot be promoted from an already-inspected 42-session window.\n"
    )
    (args.out / "RESEARCH_REPORT.md").write_text(report, encoding="utf-8")

    artifacts = []
    for path in sorted(args.out.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append(
                {
                    "file": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
    manifest = {
        "production_approved": False,
        "inputs": {
            "prefilter": str(args.prefilter.resolve()),
            "prefilter_sha256": _sha256(args.prefilter),
            "five_minute_dir": str(args.five_minute_dir.resolve()),
            "one_minute_dir": str(args.one_minute_dir.resolve()),
            "nifty_five_minute_dir": str(args.nifty_five_minute_dir.resolve()),
            "v12_source": str(Path(v12.__file__).resolve()),
            "v12_source_sha256": _sha256(Path(v12.__file__)),
            "impulse_adapter_source": str(Path(impulse.__file__).resolve()),
            "impulse_adapter_source_sha256": _sha256(Path(impulse.__file__)),
            "pullback_source": str(Path(__file__).resolve()),
            "pullback_source_sha256": _sha256(Path(__file__)),
        },
        "artifacts": artifacts,
    }
    (args.out / "integrity_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps(_json_value(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
