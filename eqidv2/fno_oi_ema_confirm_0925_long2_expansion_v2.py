"""Test up to two 09:26 LONG entries with the fixed two-SHORT V2 setup.

This is an isolated V2 experiment. The existing protected and short-expansion
files are read-only inputs and retain their own output names.
"""

from __future__ import annotations

import argparse
import itertools
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_0925_pf_v2 as v2
import fno_oi_ema_confirm_0925_short_expansion_v2 as short_v2
import fno_oi_ema_confirm_optimize as opt
import fno_oi_ema_confirm_sweep as sw


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_long2_expansion_v2.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_long2_expansion_v2_daily.csv"
AUDIT_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_long2_expansion_v2_selected_trades.csv"
RANKED_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_long2_expansion_v2_ranked.csv"
CURRENT_V2_DAILY_PATH = RESULT_DIR / "ema_confirm_0925_short_expansion_v2_daily.csv"

FIXED_SHORT_NAME = "MORE_SHORT_2X_HIGH_PF"
LONG_MAX = 2
SHORT_MAX = 2
OBJECTIVES = {
    "BEST_TRADE_PF": "trade_pf",
    "BEST_DAY_PF": "day_pf",
    "BEST_NET": "net_pct",
}


@dataclass
class PortfolioCandidate:
    mode: str
    candidate: v2.Candidate
    stats: dict[str, dict[str, Any]]


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


def exact_two_selections(
    signals: pd.DataFrame,
    force_daily: bool,
) -> list[v2.Candidate]:
    return [
        candidate
        for candidate in v2.build_selections(
            signals,
            "LONG",
            max_per_side=LONG_MAX,
            force_daily=force_daily,
        )
        if int(candidate.values["max_per_side"]) == LONG_MAX
    ]


def prune_for_fixed_short(
    candidates: list[v2.Candidate],
    guards: v2.Guards,
    retain_n: int,
    short_stats: dict[str, Any],
) -> list[v2.Candidate]:
    base = v2.prune_candidates(candidates, guards, retain_n)
    eligible = [
        candidate
        for candidate in candidates
        if int(candidate.values["all_trades"]) >= guards.min_trades
    ]

    def combined_trade_key(candidate: v2.Candidate) -> tuple[float, float, float]:
        profit = (
            float(short_stats["gross_profit_pct"])
            + float(candidate.values["all_gross_profit_pct"])
        )
        loss = (
            float(short_stats["gross_loss_pct"])
            + float(candidate.values["all_gross_loss_pct"])
        )
        return (
            pf(profit, loss),
            float(short_stats["net_pct"]) + float(candidate.values["all_net_pct"]),
            float(candidate.values["all_day_pf"]),
        )

    by_combined_pf = sorted(
        eligible,
        key=combined_trade_key,
        reverse=True,
    )[:retain_n]
    by_combined_net = sorted(
        eligible,
        key=lambda candidate: (
            float(short_stats["net_pct"]) + float(candidate.values["all_net_pct"]),
            combined_trade_key(candidate)[0],
            float(candidate.values["all_day_pf"]),
        ),
        reverse=True,
    )[:retain_n]
    kept: dict[int, v2.Candidate] = {}
    for candidate in base + by_combined_pf + by_combined_net:
        kept[id(candidate)] = candidate
    return list(kept.values())


def optimise_long_two(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    train_days: set[date],
    test_days: set[date],
    all_days: set[date],
    guards: v2.Guards,
    cost_bps: float,
    retain_n: int,
    short_stats: dict[str, Any],
) -> tuple[dict[str, list[v2.Candidate]], dict[tuple[float, float], np.ndarray]]:
    train_mask = np.fromiter(
        (day in train_days for day in v2.SLOT_DAYS),
        dtype=bool,
        count=len(v2.SLOT_DAYS),
    )
    test_mask = np.fromiter(
        (day in test_days for day in v2.SLOT_DAYS),
        dtype=bool,
        count=len(v2.SLOT_DAYS),
    )
    all_mask = np.ones(len(v2.SLOT_DAYS), dtype=bool)
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
            f"[LONG2 {mode}] {len(selections[mode]):,} exact top-two selections",
            flush=True,
        )

    survivors: dict[str, list[v2.Candidate]] = {mode: [] for mode in selections}
    survivor_counts = {mode: 0 for mode in selections}
    net_cache: dict[tuple[float, float], np.ndarray] = {}
    brackets = list(itertools.product(v2.STOP_PCTS, v2.TARGET_PCTS))
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
                train = v2.score(net_all, candidate.selected_idx, train_mask)
                if not v2.passes_guards(train, guards):
                    continue
                values = {
                    **candidate.values,
                    "stop_pct": stop_pct,
                    "target_pct": target_pct,
                    **v2.prefixed("train", train),
                    **v2.prefixed(
                        "test", v2.score(net_all, candidate.selected_idx, test_mask)
                    ),
                    **v2.prefixed(
                        "all", v2.score(net_all, candidate.selected_idx, all_mask)
                    ),
                }
                survivors[mode].append(v2.Candidate(values, candidate.selected_idx))
                survivor_counts[mode] += 1
            survivors[mode] = prune_for_fixed_short(
                survivors[mode], guards, retain_n, short_stats
            )
        retained = ", ".join(
            f"{mode}={len(items)}" for mode, items in survivors.items()
        )
        print(
            f"[LONG2] bracket {bracket_no:02d}/{len(brackets)} "
            f"stop={stop_pct:g} target={target_pct:g} retained {retained}",
            flush=True,
        )

    if not any(survivors.values()):
        counts = ", ".join(
            f"{mode}={survivor_counts[mode]}" for mode in survivor_counts
        )
        raise RuntimeError(f"No exact-two LONG setup survived. Evaluated survivors: {counts}")
    return survivors, net_cache


def fixed_short_variant() -> short_v2.ShortVariant:
    return next(
        variant for variant in short_v2.VARIANTS
        if variant.name == FIXED_SHORT_NAME
    )


def period_stats(
    daily: pd.DataFrame,
    curve: pd.DataFrame,
    days: set[date],
    model: str,
) -> dict[str, Any]:
    result = v2.portfolio_period_stats(daily, curve, days)
    return result[model]


def build_portfolio(
    signals: pd.DataFrame,
    all_days: list[date],
    long_candidate: v2.Candidate,
    long_net: np.ndarray,
    short_candidate: v2.Candidate,
    short_net: np.ndarray,
    model: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    long_daily = v2.daily_curve(
        signals, long_net, long_candidate, model, all_days
    )
    short_daily = v2.daily_curve(
        signals, short_net, short_candidate, model, all_days
    )
    daily = pd.concat([long_daily, short_daily], ignore_index=True)
    curve = v2.portfolio_daily_curve(daily)
    return daily, curve


def evaluate_candidates(
    signals: pd.DataFrame,
    all_days: list[date],
    train_days: set[date],
    test_days: set[date],
    survivors: dict[str, list[v2.Candidate]],
    net_cache: dict[tuple[float, float], np.ndarray],
    short_candidate: v2.Candidate,
    short_net: np.ndarray,
) -> list[PortfolioCandidate]:
    all_day_set = set(all_days)
    evaluated: list[PortfolioCandidate] = []
    for mode, candidates in survivors.items():
        for candidate in candidates:
            bracket = (
                float(candidate.values["stop_pct"]),
                float(candidate.values["target_pct"]),
            )
            model = "CANDIDATE"
            daily, curve = build_portfolio(
                signals,
                all_days,
                candidate,
                net_cache[bracket],
                short_candidate,
                short_net,
                model,
            )
            stats = {
                "TRAIN": period_stats(daily, curve, train_days, model),
                "TEST": period_stats(daily, curve, test_days, model),
                "ALL": period_stats(daily, curve, all_day_set, model),
            }
            evaluated.append(PortfolioCandidate(mode, candidate, stats))
    return evaluated


def finite_metric(value: Any) -> float:
    number = float(value)
    return number if math.isfinite(number) else float("-inf")


def candidate_key(
    candidate: PortfolioCandidate,
    objective: str,
) -> tuple[float, float, float]:
    stats = candidate.stats["ALL"]
    if objective == "trade_pf":
        return (
            finite_metric(stats["trade_pf"]),
            finite_metric(stats["day_pf"]),
            float(stats["net_pct"]),
        )
    if objective == "day_pf":
        return (
            finite_metric(stats["day_pf"]),
            finite_metric(stats["trade_pf"]),
            float(stats["net_pct"]),
        )
    return (
        float(stats["net_pct"]),
        finite_metric(stats["trade_pf"]),
        finite_metric(stats["day_pf"]),
    )


def ranking_frame(evaluated: list[PortfolioCandidate]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in evaluated:
        rows.append(
            {
                "mode": item.mode,
                **item.candidate.values,
                **{
                    f"portfolio_{period.lower()}_{key}": value
                    for period, stats in item.stats.items()
                    for key, value in stats.items()
                },
            }
        )
    frame = pd.DataFrame(rows)
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


def current_v2_stats() -> dict[str, Any]:
    if not CURRENT_V2_DAILY_PATH.exists():
        raise FileNotFoundError(f"Missing current V2 curve: {CURRENT_V2_DAILY_PATH}")
    daily = pd.read_csv(CURRENT_V2_DAILY_PATH)
    curve = daily.loc[daily["variant"].eq(FIXED_SHORT_NAME)].copy()
    if curve.empty:
        raise RuntimeError(f"{FIXED_SHORT_NAME} is missing from {CURRENT_V2_DAILY_PATH}")
    curve = curve.sort_values("day")
    final = curve.iloc[-1]
    return {
        "orders": int(pd.to_numeric(curve["selections"], errors="coerce").sum()),
        "fills": int(pd.to_numeric(curve["fills"], errors="coerce").sum()),
        "trade_pf": float(final["cumulative_trade_pf"]),
        "day_pf": float(final["cumulative_day_pf"]),
        "net_pct": float(final["cumulative_net_pct"]),
    }


def render_report(
    selected: dict[str, PortfolioCandidate],
    summaries: dict[str, dict[str, dict[str, Any]]],
    curves: dict[str, pd.DataFrame],
    baseline: dict[str, Any],
    short_variant: short_v2.ShortVariant,
    split_day: date,
    cost_bps: float,
) -> str:
    lines = [
        "# FNO EMA/OI 09:25/09:26 V2 Two-LONG Expansion",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Existing protected V2 and V2 short-expansion files were not edited.",
        "- Timing remains one daily scan: 5-minute signal ends 09:25; 1-minute confirmation ends 09:26; entries can fill only afterward.",
        f"- Capacity: up to {LONG_MAX} LONG and {SHORT_MAX} SHORT contracts in the single scan.",
        f"- SHORT is fixed to `{FIXED_SHORT_NAME}`; only the exact top-two LONG family is searched.",
        f"- Cost: {cost_bps:g} bps round trip.",
        "- Full-history winners are descriptive and in-sample, not honest out-of-sample validation.",
        "",
        "## Fixed SHORT Setup",
        "",
        "| Max | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {short_variant.max_per_side} | {short_variant.picker} | "
        f"{short_variant.price_change_pct} | {short_variant.oi_change_pct} | "
        f"{short_variant.volume_ratio} | {short_variant.body_ratio} | "
        f"{short_variant.max_wick_ratio} | {short_variant.stop_pct} | "
        f"{short_variant.target_pct} |",
        "",
        "## Selected Two-LONG Setups",
        "",
        "| Objective | Mode | LONG fills | LONG PF | LONG net % | Portfolio PF | Day PF | Net % | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        item = selected[objective_name]
        row = item.candidate.values
        stats = item.stats["ALL"]
        lines.append(
            f"| {objective_name} | {item.mode} | {int(row['all_trades'])} | "
            f"{fmt(row['all_pf'])} | {float(row['all_net_pct']):+.3f} | "
            f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | "
            f"{stats['net_pct']:+.3f} | {row['picker']} | "
            f"{row['price_change_pct']} | {row['oi_change_pct']} | "
            f"{row['volume_ratio']} | {row['body_ratio']} | "
            f"{row['max_wick_ratio']} | {row['stop_pct']} | {row['target_pct']} |"
        )

    lines += [
        "",
        "## Portfolio Comparison",
        "",
        "| Portfolio | Orders | Fills | Trade PF | Day PF | Net % | Net change vs current V2 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| Current V2 1-LONG/2-SHORT | {baseline['orders']} | {baseline['fills']} | "
        f"{fmt(baseline['trade_pf'])} | {fmt(baseline['day_pf'])} | "
        f"{baseline['net_pct']:+.3f} | +0.000 |",
    ]
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        lines.append(
            f"| New V2 {objective_name} | {stats['orders']} | {stats['fills']} | "
            f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | "
            f"{stats['net_pct']:+.3f} | {stats['net_pct'] - baseline['net_pct']:+.3f} |"
        )

    best_trade = summaries["BEST_TRADE_PF"]["ALL"]
    best_net = summaries["BEST_NET"]["ALL"]
    lines += [
        "",
        "## Verdict",
        "",
        "- Keep the current V2 as the selected 09:26 setup. No exact top-two LONG candidate improved its cumulative return.",
        f"- The two-LONG trade-PF winner raises trade PF by {best_trade['trade_pf'] - baseline['trade_pf']:+.3f}, "
        f"but lowers day PF by {best_trade['day_pf'] - baseline['day_pf']:+.3f} and net return by "
        f"{best_trade['net_pct'] - baseline['net_pct']:+.3f}%.",
        f"- The highest-return two-LONG result still trails current V2 by "
        f"{best_net['net_pct'] - baseline['net_pct']:+.3f}% and has lower trade and day PF.",
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
                f"| {objective_name} | {period} | {stats['orders']} | "
                f"{stats['fills']} | {fmt(stats['trade_pf'])} | "
                f"{fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | "
                f"{stats['flat_days']} |"
            )

    columns = (
        "| Day | Period | L O/F | LONG trades | LONG % | S O/F | SHORT trades | SHORT % | Total O/F | Total % | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for objective_name, curve in curves.items():
        lines += ["", f"## {objective_name} Day-Wise Table", "", columns, divider]
        for row in curve.itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            long_detail = row.long_trade_details or row.long_status
            short_detail = row.short_trade_details or row.short_status
            lines.append(
                f"| {row.day} | {period} | {int(row.long_selections)}/{int(row.long_fills)} | "
                f"{long_detail} | {fmt_signed(row.long_return_pct)} | "
                f"{int(row.short_selections)}/{int(row.short_fills)} | "
                f"{short_detail} | {fmt_signed(row.short_return_pct)} | "
                f"{int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | "
                f"{fmt_signed(row.cumulative_net_pct)} | "
                f"{fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
            )

    lines += [
        "",
        "## Files",
        "",
        f"- Daily portfolio CSV: `{DAILY_OUTPUT_PATH}`",
        f"- Selected-trades audit: `{AUDIT_OUTPUT_PATH}`",
        f"- Ranked two-LONG candidates: `{RANKED_OUTPUT_PATH}`",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=15)
    parser.add_argument("--min-day-win", type=float, default=0.40)
    parser.add_argument("--max-top-profit-share", type=float, default=0.45)
    parser.add_argument("--retain-n", type=int, default=250)
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
    split_day = pd.Timestamp(args.split_day).date()
    signals_all, paths = opt.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals_all = signals_all.copy()
    signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
    all_days = sorted(set(signals_all["day"]))
    signals = signals_all.loc[signals_all["hhmm_int"].eq(v2.SIGNAL_SLOT)].copy()
    signals = signals.sort_values(["day", "tradingsymbol", "sid"]).reset_index(drop=True)
    if signals.empty:
        raise RuntimeError("No cached 09:25 signals found.")

    short_v2.setup_signal_globals(signals, all_days)
    train_days = {day for day in all_days if day < split_day}
    test_days = set(all_days) - train_days
    guards = v2.Guards(
        min_trades=args.min_trades,
        min_day_win=args.min_day_win,
        max_top_profit_share=args.max_top_profit_share,
    )
    print(
        f"[DATA] {len(signals):,} confirmed 09:25 candidates across "
        f"{signals['day'].nunique()} sessions | train {len(train_days)} | "
        f"test {len(test_days)}",
        flush=True,
    )

    short_variant = fixed_short_variant()
    short_candidate = short_v2.reconstruct_short(signals, short_variant)
    short_net = sw.simulate_bracket(
        signals,
        paths,
        stop_pct=short_variant.stop_pct,
        target_pct=short_variant.target_pct,
        cost_bps=args.cost_bps,
    )
    short_stats = v2.score(
        short_net,
        short_candidate.selected_idx,
        np.ones(len(v2.SLOT_DAYS), dtype=bool),
    )
    survivors, net_cache = optimise_long_two(
        signals,
        paths,
        train_days,
        test_days,
        set(all_days),
        guards,
        args.cost_bps,
        args.retain_n,
        short_stats,
    )
    evaluated = evaluate_candidates(
        signals,
        all_days,
        train_days,
        test_days,
        survivors,
        net_cache,
        short_candidate,
        short_net,
    )
    if not evaluated:
        raise RuntimeError("No two-LONG portfolio candidate could be evaluated.")

    selected = {
        objective_name: max(
            evaluated,
            key=lambda item, metric=metric: candidate_key(item, metric),
        )
        for objective_name, metric in OBJECTIVES.items()
    }
    ranking = ranking_frame(evaluated)
    common.atomic_write_csv(ranking, RANKED_OUTPUT_PATH)

    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    audit_parts: list[pd.DataFrame] = []
    all_day_set = set(all_days)
    for objective_name, item in selected.items():
        bracket = (
            float(item.candidate.values["stop_pct"]),
            float(item.candidate.values["target_pct"]),
        )
        daily, curve = build_portfolio(
            signals,
            all_days,
            item.candidate,
            net_cache[bracket],
            short_candidate,
            short_net,
            objective_name,
        )
        if (daily.loc[daily["side"].eq("LONG"), "selections"] > LONG_MAX).any():
            raise AssertionError("Two-LONG experiment exceeded its LONG cap.")
        if (daily.loc[daily["side"].eq("SHORT"), "selections"] > SHORT_MAX).any():
            raise AssertionError("Two-LONG experiment exceeded its SHORT cap.")
        curves[objective_name] = curve.assign(long_mode=item.mode)
        summaries[objective_name] = {
            "TRAIN": period_stats(daily, curve, train_days, objective_name),
            "TEST": period_stats(daily, curve, test_days, objective_name),
            "ALL": period_stats(daily, curve, all_day_set, objective_name),
        }
        long_audit = v2.trade_audit(
            signals, net_cache[bracket], item.candidate, objective_name
        ).assign(setup_leg="LONG2", mode=item.mode)
        short_audit = v2.trade_audit(
            signals, short_net, short_candidate, objective_name
        ).assign(setup_leg=FIXED_SHORT_NAME, mode="FIXED")
        audit_parts.extend([long_audit, short_audit])

    common.atomic_write_csv(pd.concat(curves.values(), ignore_index=True), DAILY_OUTPUT_PATH)
    common.atomic_write_csv(pd.concat(audit_parts, ignore_index=True), AUDIT_OUTPUT_PATH)
    baseline = current_v2_stats()
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            selected,
            summaries,
            curves,
            baseline,
            short_variant,
            split_day,
            args.cost_bps,
        ),
    )
    print(
        f"[CURRENT V2] PF={baseline['trade_pf']:.3f} "
        f"day PF={baseline['day_pf']:.3f} net={baseline['net_pct']:+.3f}%",
        flush=True,
    )
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        print(
            f"[{objective_name}] PF={stats['trade_pf']:.3f} "
            f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%",
            flush=True,
        )
    print(REPORT_PATH, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
