"""Test an exact max-two SHORT selector for the 09:31 V3 add-on.

This is an isolated experiment. The locked V2 baseline, the current V3 LONG
leg, and all current selected V3 outputs are read-only inputs. Only the 09:31
SHORT family is re-optimised, with a maximum of two selected contracts.
"""

from __future__ import annotations

import argparse
import itertools
import math
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_0925_0930_best_combo_v3 as combo
import fno_oi_ema_confirm_0925_0930_pf_v3 as v3
import fno_oi_ema_confirm_optimize as opt
import fno_oi_ema_confirm_sweep as sw


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = (
    common.LATEST_DIR
    / "latest_fno_oi_ema_confirm_0925_0930_short2_expansion_v3.md"
)
DAILY_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_short2_expansion_v3_daily.csv"
)
SELECTED_DAILY_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_short2_expansion_v3_selected_daily.csv"
)
AUDIT_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_short2_expansion_v3_selected_trades.csv"
)
RANKED_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_short2_expansion_v3_ranked.csv"
)

FIXED_LONG_MODE = "FILTERED"
FIXED_LONG_SOURCE = "FULL_HISTORY_MAX_PF"
LONG_MAX = 1
SHORT_MAX = 2
SELECTED_OBJECTIVE = "BEST_TRADE_PF"
OBJECTIVES = {
    "BEST_TRADE_PF": "trade_pf",
    "BEST_DAY_PF": "day_pf",
    "BEST_NET": "net_pct",
}


@dataclass
class PortfolioCandidate:
    mode: str
    candidate: v3.Candidate


@dataclass(frozen=True)
class FixedContext:
    day_net: np.ndarray
    trade_profit: float
    trade_loss: float
    net_pct: float
    orders: int
    fills: int


def pf(profit: float, loss: float) -> float:
    if loss > 0:
        return float(profit / loss)
    return float("inf") if profit > 0 else float("nan")


def fmt(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isinf(number):
        return "INF"
    if math.isnan(number):
        return ""
    return f"{number:.{digits}f}"


def fmt_signed(value: Any, digits: int = 3) -> str:
    value_fmt = fmt(value, digits)
    if not value_fmt:
        return ""
    return value_fmt if value_fmt == "INF" else f"{float(value):+.{digits}f}"


def finite_metric(value: Any) -> float:
    number = float(value)
    return number if math.isfinite(number) else float("-inf")


def setup_v3_globals(signals: pd.DataFrame, all_days: list[date]) -> None:
    v3.SLOT_DAYS = signals["day"].to_numpy()
    day_code = {day: idx for idx, day in enumerate(all_days)}
    v3.SLOT_DAY_IDX = np.array(
        [day_code[day] for day in v3.SLOT_DAYS], dtype=int
    )
    scan_keys = list(zip(signals["day"], signals["hhmm_int"].astype(int)))
    unique_scans = {key: idx for idx, key in enumerate(dict.fromkeys(scan_keys))}
    v3.SLOT_SCAN_IDX = np.array(
        [unique_scans[key] for key in scan_keys], dtype=int
    )
    v3.ALL_DAY_VALUES = list(all_days)
    v3.SIGNAL_END_LABEL = "09:30"
    v3.CONFIRMATION_END_LABEL = "09:31"


def fixed_long_choice() -> combo.SetupChoice:
    matches = [
        choice
        for choice in combo.setup_options("LONG")
        if choice.mode == FIXED_LONG_MODE
        and choice.source_model == FIXED_LONG_SOURCE
        and int(choice.row["max_per_side"]) == LONG_MAX
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "Could not identify the current V3 09:31 LONG setup uniquely."
        )
    return matches[0]


def exact_two_selections(
    signals: pd.DataFrame,
    force_daily: bool,
) -> list[v3.Candidate]:
    return [
        candidate
        for candidate in v3.build_selections(
            signals,
            "SHORT",
            max_per_side=SHORT_MAX,
            force_daily=force_daily,
        )
        if int(candidate.values["max_per_side"]) == SHORT_MAX
    ]


def short_day_net(
    net_all: np.ndarray,
    selected_idx: np.ndarray,
    day_count: int,
) -> np.ndarray:
    result = np.zeros(day_count, dtype=float)
    values = net_all[selected_idx]
    filled = np.isfinite(values)
    if filled.any():
        np.add.at(
            result,
            v3.SLOT_DAY_IDX[selected_idx[filled]],
            values[filled],
        )
    return result


def add_portfolio_values(
    values: dict[str, Any],
    selected_idx: np.ndarray,
    net_all: np.ndarray,
    fixed: FixedContext,
) -> None:
    combined_trade_profit = (
        fixed.trade_profit + float(values["all_gross_profit_pct"])
    )
    combined_trade_loss = (
        fixed.trade_loss + float(values["all_gross_loss_pct"])
    )
    combined_day_net = fixed.day_net + short_day_net(
        net_all, selected_idx, fixed.day_net.size
    )
    day_profit = float(combined_day_net[combined_day_net > 0].sum())
    day_loss = float(-combined_day_net[combined_day_net < 0].sum())
    values.update(
        {
            "portfolio_all_orders": fixed.orders + int(values["all_orders"]),
            "portfolio_all_fills": fixed.fills + int(values["all_trades"]),
            "portfolio_all_trade_pf": pf(
                combined_trade_profit, combined_trade_loss
            ),
            "portfolio_all_day_pf": pf(day_profit, day_loss),
            "portfolio_all_net_pct": (
                fixed.net_pct + float(values["all_net_pct"])
            ),
            "portfolio_all_positive_days": int((combined_day_net > 0).sum()),
            "portfolio_all_negative_days": int((combined_day_net < 0).sum()),
            "portfolio_all_flat_days": int((combined_day_net == 0).sum()),
            "portfolio_all_max_day_pct": float(combined_day_net.max()),
        }
    )


def prune_for_fixed_system(
    candidates: list[v3.Candidate],
    guards: v3.Guards,
    retain_n: int,
) -> list[v3.Candidate]:
    base = v3.prune_candidates(candidates, guards, retain_n)
    eligible = [
        candidate
        for candidate in candidates
        if int(candidate.values["all_trades"]) >= guards.min_trades
    ]

    by_portfolio_trade = sorted(
        eligible,
        key=lambda candidate: (
            finite_metric(candidate.values["portfolio_all_trade_pf"]),
            finite_metric(candidate.values["portfolio_all_day_pf"]),
            float(candidate.values["portfolio_all_net_pct"]),
        ),
        reverse=True,
    )[:retain_n]
    by_portfolio_day = sorted(
        eligible,
        key=lambda candidate: (
            finite_metric(candidate.values["portfolio_all_day_pf"]),
            finite_metric(candidate.values["portfolio_all_trade_pf"]),
            float(candidate.values["portfolio_all_net_pct"]),
        ),
        reverse=True,
    )[:retain_n]
    by_portfolio_net = sorted(
        eligible,
        key=lambda candidate: (
            float(candidate.values["portfolio_all_net_pct"]),
            finite_metric(candidate.values["portfolio_all_trade_pf"]),
            finite_metric(candidate.values["portfolio_all_day_pf"]),
        ),
        reverse=True,
    )[:retain_n]

    kept: dict[int, v3.Candidate] = {}
    for candidate in (
        base + by_portfolio_trade + by_portfolio_day + by_portfolio_net
    ):
        kept[id(candidate)] = candidate
    return list(kept.values())


def optimise_short_two(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    train_days: set[date],
    test_days: set[date],
    guards: v3.Guards,
    cost_bps: float,
    retain_n: int,
    fixed: FixedContext,
) -> tuple[
    dict[str, list[v3.Candidate]],
    dict[tuple[float, float], np.ndarray],
]:
    train_mask = np.fromiter(
        (day in train_days for day in v3.SLOT_DAYS),
        dtype=bool,
        count=len(v3.SLOT_DAYS),
    )
    test_mask = np.fromiter(
        (day in test_days for day in v3.SLOT_DAYS),
        dtype=bool,
        count=len(v3.SLOT_DAYS),
    )
    all_mask = np.ones(len(v3.SLOT_DAYS), dtype=bool)
    selections = {
        "FILTERED": exact_two_selections(signals, force_daily=False),
        "FORCE_DAILY": exact_two_selections(signals, force_daily=True),
    }
    for mode in selections:
        selections[mode] = [
            candidate
            for candidate in selections[mode]
            if int(train_mask[candidate.selected_idx].sum()) >= guards.min_trades
        ]
        print(
            f"[SHORT2 {mode}] {len(selections[mode]):,} exact max-two selections",
            flush=True,
        )

    survivors: dict[str, list[v3.Candidate]] = {
        mode: [] for mode in selections
    }
    survivor_counts = {mode: 0 for mode in selections}
    net_cache: dict[tuple[float, float], np.ndarray] = {}
    brackets = list(itertools.product(v3.STOP_PCTS, v3.TARGET_PCTS))
    for bracket_no, (stop_pct, target_pct) in enumerate(brackets, start=1):
        net_all = sw.simulate_bracket(
            signals,
            paths,
            stop_pct=stop_pct,
            target_pct=target_pct,
            cost_bps=cost_bps,
        )
        net_cache[(stop_pct, target_pct)] = net_all
        for mode, mode_selections in selections.items():
            for candidate in mode_selections:
                train = v3.score(net_all, candidate.selected_idx, train_mask)
                if not v3.passes_guards(train, guards):
                    continue
                values = {
                    **candidate.values,
                    "stop_pct": stop_pct,
                    "target_pct": target_pct,
                    **v3.prefixed("train", train),
                    **v3.prefixed(
                        "test",
                        v3.score(net_all, candidate.selected_idx, test_mask),
                    ),
                    **v3.prefixed(
                        "all",
                        v3.score(net_all, candidate.selected_idx, all_mask),
                    ),
                }
                add_portfolio_values(
                    values,
                    candidate.selected_idx,
                    net_all,
                    fixed,
                )
                survivors[mode].append(
                    v3.Candidate(values, candidate.selected_idx)
                )
                survivor_counts[mode] += 1
            survivors[mode] = prune_for_fixed_system(
                survivors[mode], guards, retain_n
            )
        retained = ", ".join(
            f"{mode}={len(items)}" for mode, items in survivors.items()
        )
        print(
            f"[SHORT2] bracket {bracket_no:02d}/{len(brackets)} "
            f"stop={stop_pct:g} target={target_pct:g} retained {retained}",
            flush=True,
        )

    if not any(survivors.values()):
        counts = ", ".join(
            f"{mode}={survivor_counts[mode]}" for mode in survivor_counts
        )
        raise RuntimeError(
            f"No max-two SHORT setup survived. Evaluated survivors: {counts}"
        )
    return survivors, net_cache


def candidate_key(
    item: PortfolioCandidate,
    objective: str,
) -> tuple[float, float, float]:
    values = item.candidate.values
    if objective == "trade_pf":
        return (
            finite_metric(values["portfolio_all_trade_pf"]),
            finite_metric(values["portfolio_all_day_pf"]),
            float(values["portfolio_all_net_pct"]),
        )
    if objective == "day_pf":
        return (
            finite_metric(values["portfolio_all_day_pf"]),
            finite_metric(values["portfolio_all_trade_pf"]),
            float(values["portfolio_all_net_pct"]),
        )
    return (
        float(values["portfolio_all_net_pct"]),
        finite_metric(values["portfolio_all_trade_pf"]),
        finite_metric(values["portfolio_all_day_pf"]),
    )


def ranking_frame(evaluated: list[PortfolioCandidate]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {"mode": item.mode, **item.candidate.values}
            for item in evaluated
        ]
    )
    if frame.empty:
        return frame
    return frame.sort_values(
        [
            "portfolio_all_trade_pf",
            "portfolio_all_day_pf",
            "portfolio_all_net_pct",
        ],
        ascending=False,
    ).reset_index(drop=True)


def current_v3_curve(all_days: set[date]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not combo.SELECTED_DAILY_OUTPUT_PATH.exists():
        raise FileNotFoundError(
            f"Missing current selected V3 curve: {combo.SELECTED_DAILY_OUTPUT_PATH}"
        )
    curve = pd.read_csv(combo.SELECTED_DAILY_OUTPUT_PATH)
    curve["day"] = pd.to_datetime(curve["day"]).dt.date
    if set(curve["day"]) != all_days or curve["day"].duplicated().any():
        raise RuntimeError("Current selected V3 curve has stale or duplicate sessions.")
    return curve, combo.period_stats(curve, all_days)


def fixed_context(curve: pd.DataFrame) -> FixedContext:
    trade_profit = float(
        curve["long_gross_profit_pct"].sum()
        + curve["short_gross_profit_pct"].sum()
    )
    trade_loss = float(
        curve["long_gross_loss_pct"].sum()
        + curve["short_gross_loss_pct"].sum()
    )
    return FixedContext(
        day_net=curve["portfolio_net_return_pct"].to_numpy(float),
        trade_profit=trade_profit,
        trade_loss=trade_loss,
        net_pct=float(curve["portfolio_net_return_pct"].sum()),
        orders=int(curve["selections"].sum()),
        fills=int(curve["fills"].sum()),
    )


def render_report(
    selected: dict[str, PortfolioCandidate],
    summaries: dict[str, dict[str, dict[str, Any]]],
    curves: dict[str, pd.DataFrame],
    short_daily: dict[str, pd.DataFrame],
    current_stats: dict[str, Any],
    baseline_stats: dict[str, Any],
    fixed_long: combo.SetupChoice,
    split_day: date,
    cost_bps: float,
) -> str:
    fixed_row = fixed_long.row
    best_trade = summaries["BEST_TRADE_PF"]["ALL"]
    best_net = summaries["BEST_NET"]["ALL"]
    lines = [
        "# FNO EMA/OI V3 09:31 Two-SHORT Expansion",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Current selected V3 and protected V2 files were not edited.",
        f"- Locked baseline: V2 `{combo.BASELINE_VARIANT}` at 09:25/09:26.",
        "- Add-on timing remains one scan ending 09:30 and one confirmation ending 09:31; orders activate afterward.",
        f"- Capacity: fixed current V3 LONG max {LONG_MAX}; searched SHORT max {SHORT_MAX} at 09:31.",
        "- A max-two selector can take one short when only one contract qualifies; it never exceeds two.",
        f"- Cost: {cost_bps:g} bps round trip.",
        "- Full-history winners are descriptive and in-sample, not honest out-of-sample validation.",
        "",
        "## Fixed 09:31 LONG Setup",
        "",
        "| Mode | Source | Max | PF | Trades | Net % | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {fixed_long.mode} | {fixed_long.source_model} | {int(fixed_row['max_per_side'])} | "
        f"{fmt(fixed_row['all_pf'])} | {int(fixed_row['all_trades'])} | "
        f"{float(fixed_row['all_net_pct']):+.3f} | {fixed_row['picker']} | "
        f"{fixed_row['price_change_pct']} | {fixed_row['oi_change_pct']} | "
        f"{fixed_row['volume_ratio']} | {fixed_row['body_ratio']} | "
        f"{fixed_row['max_wick_ratio']} | {fixed_row['stop_pct']} | "
        f"{fixed_row['target_pct']} |",
        "",
        "## Selected 09:31 SHORT2 Setups",
        "",
        "| Objective | Mode | SHORT orders/fills | Two-short days | SHORT PF | SHORT net % | Portfolio PF | Day PF | Net % | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        item = selected[objective_name]
        row = item.candidate.values
        stats = summaries[objective_name]["ALL"]
        leg = short_daily[objective_name]
        lines.append(
            f"| {objective_name} | {item.mode} | {int(row['all_orders'])}/{int(row['all_trades'])} | "
            f"{int((leg['selections'] == SHORT_MAX).sum())} | {fmt(row['all_pf'])} | "
            f"{float(row['all_net_pct']):+.3f} | {fmt(stats['trade_pf'])} | "
            f"{fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | {row['picker']} | "
            f"{row['price_change_pct']} | {row['oi_change_pct']} | {row['volume_ratio']} | "
            f"{row['body_ratio']} | {row['max_wick_ratio']} | {row['stop_pct']} | "
            f"{row['target_pct']} |"
        )

    lines += [
        "",
        "## Portfolio Comparison",
        "",
        "| Portfolio | 09:31 SHORT max | Orders | Fills | Trade PF | Day PF | Net % | Change vs current V3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| Locked V2 only | 0 | {baseline_stats['orders']} | {baseline_stats['fills']} | "
        f"{fmt(baseline_stats['trade_pf'])} | {fmt(baseline_stats['day_pf'])} | "
        f"{baseline_stats['net_pct']:+.3f} | {baseline_stats['net_pct'] - current_stats['net_pct']:+.3f} |",
        f"| Current selected V3 | 1 | {current_stats['orders']} | {current_stats['fills']} | "
        f"{fmt(current_stats['trade_pf'])} | {fmt(current_stats['day_pf'])} | "
        f"{current_stats['net_pct']:+.3f} | +0.000 |",
    ]
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        lines.append(
            f"| SHORT2 {objective_name} | 2 | {stats['orders']} | {stats['fills']} | "
            f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | "
            f"{stats['net_pct']:+.3f} | {stats['net_pct'] - current_stats['net_pct']:+.3f} |"
        )

    trade_pf_change = best_trade["trade_pf"] - current_stats["trade_pf"]
    trade_day_change = best_trade["day_pf"] - current_stats["day_pf"]
    trade_net_change = best_trade["net_pct"] - current_stats["net_pct"]
    net_change = best_net["net_pct"] - current_stats["net_pct"]
    dominates = any(
        summaries[name]["ALL"]["trade_pf"] >= current_stats["trade_pf"]
        and summaries[name]["ALL"]["day_pf"] >= current_stats["day_pf"]
        and summaries[name]["ALL"]["net_pct"] >= current_stats["net_pct"]
        and (
            summaries[name]["ALL"]["trade_pf"] > current_stats["trade_pf"]
            or summaries[name]["ALL"]["day_pf"] > current_stats["day_pf"]
            or summaries[name]["ALL"]["net_pct"] > current_stats["net_pct"]
        )
        for name in OBJECTIVES
    )
    verdict = (
        "At least one SHORT2 result dominates the current V3 across trade PF, day PF, and net return."
        if dominates
        else "No SHORT2 result dominates the current V3 across trade PF, day PF, and net return."
    )
    lines += [
        "",
        "## Verdict",
        "",
        f"- {verdict}",
        f"- The SHORT2 trade-PF winner changes trade PF by {trade_pf_change:+.3f}, "
        f"day PF by {trade_day_change:+.3f}, and net return by {trade_net_change:+.3f}% versus current V3.",
        f"- The highest-return SHORT2 setup changes cumulative net return by {net_change:+.3f}% versus current V3.",
        "- Current V3 remains selected until this experiment is explicitly chosen.",
        "",
        "## Train/Test/All",
        "",
        "| Objective | Period | Orders | Fills | Trade PF | Day PF | Net % | Positive days | Negative days | Flat days |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for period in ("TRAIN", "TEST", "ALL"):
            stats = summaries[objective_name][period]
            lines.append(
                f"| {objective_name} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | "
                f"{stats['net_pct']:+.3f} | {stats['positive_days']} | "
                f"{stats['negative_days']} | {stats['flat_days']} |"
            )

    columns = (
        "| Day | Period | V2 L O/F | V2 LONG at 09:26 | V2 S O/F | V2 SHORT at 09:26 | "
        "09:31 L O/F | 09:31 LONG | 09:31 S O/F | 09:31 SHORT2 | V2 % | Add-on % | Total O/F | Total % | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | ---: | --- | ---: | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for objective_name, curve in curves.items():
        lines += ["", f"## {objective_name} Day-Wise Table", "", columns, divider]
        for row in curve.itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            baseline_long = row.baseline_long_trade_details or row.baseline_long_status
            baseline_short = row.baseline_short_trade_details or row.baseline_short_status
            addon_long = row.addon_long_trade_details or row.addon_long_status
            addon_short = row.addon_short_trade_details or row.addon_short_status
            lines.append(
                f"| {row.day} | {period} | "
                f"{int(row.baseline_long_selections)}/{int(row.baseline_long_fills)} | {baseline_long} | "
                f"{int(row.baseline_short_selections)}/{int(row.baseline_short_fills)} | {baseline_short} | "
                f"{int(row.addon_long_selections)}/{int(row.addon_long_fills)} | {addon_long} | "
                f"{int(row.addon_short_selections)}/{int(row.addon_short_fills)} | {addon_short} | "
                f"{fmt_signed(row.baseline_net_return_pct)} | {fmt_signed(row.addon_net_return_pct)} | "
                f"{int(row.selections)}/{int(row.fills)} | {fmt_signed(row.portfolio_net_return_pct)} | "
                f"{fmt_signed(row.cumulative_net_pct)} | {fmt(row.cumulative_day_pf)} | "
                f"{fmt(row.cumulative_trade_pf)} |"
            )

    lines += [
        "",
        "## Files And Command",
        "",
        f"- Report: `{REPORT_PATH}`",
        f"- Selected experimental daily CSV: `{SELECTED_DAILY_OUTPUT_PATH}`",
        f"- All objective daily CSV: `{DAILY_OUTPUT_PATH}`",
        f"- Selected SHORT2 trade audit: `{AUDIT_OUTPUT_PATH}`",
        f"- Ranked SHORT2 candidates: `{RANKED_OUTPUT_PATH}`",
        "- Run: `cmd /c eqidv2\\bat\\run_fno_oi_ema_confirm_0925_0930_short2_expansion_v3.bat`",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--min-day-win", type=float, default=0.40)
    parser.add_argument("--max-top-profit-share", type=float, default=0.45)
    parser.add_argument("--retain-n", type=int, default=300)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    args = parser.parse_args(argv)
    if args.min_trades < 1:
        parser.error("--min-trades must be positive")
    if not 0.0 <= args.min_day_win <= 1.0:
        parser.error("--min-day-win must be between 0 and 1")
    if args.retain_n < 1:
        parser.error("--retain-n must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    split_day = pd.Timestamp(args.split_day).date()
    baseline, _ = combo.load_baseline()
    all_days = sorted(set(baseline["day"]))
    all_day_set = set(all_days)
    train_days = {day for day in all_days if day < split_day}
    test_days = all_day_set - train_days

    current_curve, current_stats = current_v3_curve(all_day_set)
    if int(current_curve["addon_short_selections"].max()) > 1:
        raise RuntimeError("Current selected V3 is no longer the max-one SHORT setup.")

    signals_all, paths = opt.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals_all = signals_all.copy()
    signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
    cache_days = sorted(set(signals_all["day"]))
    if cache_days != all_days:
        raise RuntimeError("Signal cache sessions do not match the locked V2 baseline.")
    signals = signals_all.loc[signals_all["hhmm_int"].eq(930)].copy()
    signals = signals.sort_values(
        ["day", "hhmm_int", "tradingsymbol", "sid"]
    ).reset_index(drop=True)
    if signals.empty:
        raise RuntimeError("No cached 09:30 signals found.")
    setup_v3_globals(signals, all_days)

    fixed_long_choice_value = fixed_long_choice()
    fixed_long = combo.load_addon_leg(fixed_long_choice_value, "LONG")
    if set(fixed_long["day"]) != all_day_set:
        raise RuntimeError("Current V3 LONG leg does not cover all baseline sessions.")
    if int(fixed_long["selections"].max()) > LONG_MAX:
        raise AssertionError("Current V3 LONG leg exceeded its max-one cap.")
    zero_short = combo.empty_addon(baseline["day"])
    fixed_curve = combo.build_curve(
        baseline, fixed_long, zero_short, "FIXED_V2_PLUS_0931_LONG"
    )
    fixed = fixed_context(fixed_curve)
    baseline_curve = combo.build_curve(
        baseline,
        combo.empty_addon(baseline["day"]),
        combo.empty_addon(baseline["day"]),
        "V2_BASELINE",
    )
    baseline_stats = combo.period_stats(baseline_curve, all_day_set)

    guards = v3.Guards(
        min_trades=args.min_trades,
        min_day_win=args.min_day_win,
        max_top_profit_share=args.max_top_profit_share,
    )
    print(
        f"[DATA] {len(signals):,} confirmed 09:30 candidates across "
        f"{signals['day'].nunique()} signal sessions | calendar {len(all_days)} | "
        f"train {len(train_days)} | test {len(test_days)}",
        flush=True,
    )
    survivors, net_cache = optimise_short_two(
        signals,
        paths,
        train_days,
        test_days,
        guards,
        args.cost_bps,
        args.retain_n,
        fixed,
    )
    evaluated = [
        PortfolioCandidate(mode, candidate)
        for mode, candidates in survivors.items()
        for candidate in candidates
    ]
    if not evaluated:
        raise RuntimeError("No max-two SHORT portfolio candidate could be evaluated.")

    selected = {
        objective_name: max(
            evaluated,
            key=lambda item, metric=metric: candidate_key(item, metric),
        )
        for objective_name, metric in OBJECTIVES.items()
    }
    common.atomic_write_csv(ranking_frame(evaluated), RANKED_OUTPUT_PATH)

    curves: dict[str, pd.DataFrame] = {}
    short_dailies: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    audit_parts: list[pd.DataFrame] = []
    for objective_name, item in selected.items():
        candidate = item.candidate
        bracket = (
            float(candidate.values["stop_pct"]),
            float(candidate.values["target_pct"]),
        )
        net_all = net_cache[bracket]
        short_leg = v3.daily_curve(
            signals, net_all, candidate, objective_name, all_days
        )
        curve = combo.build_curve(
            baseline, fixed_long, short_leg, objective_name
        )
        if (curve["addon_long_selections"] > LONG_MAX).any():
            raise AssertionError("SHORT2 experiment exceeded its LONG cap.")
        if (curve["addon_short_selections"] > SHORT_MAX).any():
            raise AssertionError("SHORT2 experiment exceeded its SHORT cap.")
        all_stats = combo.period_stats(curve, all_day_set)
        for metric in ("trade_pf", "day_pf", "net_pct"):
            stored = float(candidate.values[f"portfolio_all_{metric}"])
            if abs(float(all_stats[metric]) - stored) > 1e-9:
                raise AssertionError(
                    f"Stored portfolio {metric} does not reconcile for {objective_name}."
                )
        curves[objective_name] = curve
        short_dailies[objective_name] = short_leg
        summaries[objective_name] = {
            "TRAIN": combo.period_stats(curve, train_days),
            "TEST": combo.period_stats(curve, test_days),
            "ALL": all_stats,
        }
        audit_parts.append(
            v3.trade_audit(
                signals, net_all, candidate, objective_name
            ).assign(setup_leg="09:31_SHORT2", mode=item.mode)
        )

    common.atomic_write_csv(
        pd.concat(curves.values(), ignore_index=True), DAILY_OUTPUT_PATH
    )
    common.atomic_write_csv(
        curves[SELECTED_OBJECTIVE], SELECTED_DAILY_OUTPUT_PATH
    )
    common.atomic_write_csv(
        pd.concat(audit_parts, ignore_index=True), AUDIT_OUTPUT_PATH
    )
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            selected,
            summaries,
            curves,
            short_dailies,
            current_stats,
            baseline_stats,
            fixed_long_choice_value,
            split_day,
            args.cost_bps,
        ),
    )

    print(
        f"[CURRENT V3] PF={current_stats['trade_pf']:.3f} "
        f"day PF={current_stats['day_pf']:.3f} "
        f"net={current_stats['net_pct']:+.3f}%",
        flush=True,
    )
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        print(
            f"[{objective_name}] PF={stats['trade_pf']:.3f} "
            f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%",
            flush=True,
        )
    print(f"[DONE] {REPORT_PATH} ({time.monotonic() - started:.1f}s)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
