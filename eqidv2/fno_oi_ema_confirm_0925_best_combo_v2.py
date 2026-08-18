"""Build V2 best-side portfolios for the 09:25/09:26 setup.

The filtered and force-daily V2 optimisers are compared independently for LONG
and SHORT. One portfolio maximises full-history trade PF; the other maximises
full-history day PF. Both are descriptive research results, not validation.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_v2_best_side_combo.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_v2_best_side_combo_daily.csv"


@dataclass(frozen=True)
class ModeFiles:
    daily: Path
    ranked_long: Path
    ranked_short: Path


@dataclass(frozen=True)
class Objective:
    source_model: str
    rank_column: str
    metric_column: str


MODES = {
    "FORCE_DAILY": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_v2_force_daily_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_v2_force_daily_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_v2_force_daily_ranked_SHORT.csv",
    ),
    "FILTERED": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_v2_once_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_v2_once_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_v2_once_ranked_SHORT.csv",
    ),
}

OBJECTIVES = {
    "BEST_TRADE_PF": Objective(
        source_model="FULL_HISTORY_MAX_PF",
        rank_column="full_history_rank",
        metric_column="all_pf",
    ),
    "BEST_DAY_PF": Objective(
        source_model="FULL_HISTORY_MAX_DAY_PF",
        rank_column="full_history_day_rank",
        metric_column="all_day_pf",
    ),
}


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


def best_mode_for_side(
    side: str,
    objective: Objective,
) -> tuple[str, ModeFiles, pd.Series]:
    candidates: list[tuple[str, ModeFiles, pd.Series]] = []
    for mode, files in MODES.items():
        ranked_path = files.ranked_long if side == "LONG" else files.ranked_short
        if not ranked_path.exists():
            continue
        ranked = pd.read_csv(ranked_path)
        rank = pd.to_numeric(ranked[objective.rank_column], errors="coerce")
        best = ranked.loc[rank.eq(1)]
        if best.empty:
            continue
        candidates.append((mode, files, best.iloc[0]))
    if not candidates:
        raise RuntimeError(
            f"No V2 {side} setup survived for objective {objective.metric_column}"
        )
    return max(candidates, key=lambda item: float(item[2][objective.metric_column]))


def load_leg(files: ModeFiles, side: str, source_model: str) -> pd.DataFrame:
    if not files.daily.exists():
        raise FileNotFoundError(f"Missing V2 optimiser output: {files.daily}")
    daily = pd.read_csv(files.daily)
    leg = daily.loc[
        daily["model"].eq(source_model) & daily["side"].eq(side)
    ].copy()
    if leg.empty:
        raise RuntimeError(f"No {source_model} {side} rows in {files.daily}")
    leg["day"] = pd.to_datetime(leg["day"]).dt.date
    if leg["day"].duplicated().any():
        raise RuntimeError(f"Duplicate {side} sessions in {files.daily}")
    numeric = (
        "net_return_pct", "selections", "fills", "gross_profit_pct",
        "gross_loss_pct",
    )
    for column in numeric:
        leg[column] = pd.to_numeric(leg[column], errors="coerce")
    return leg[
        [
            "day", "selected_symbol", "trade_details", "status",
            "net_return_pct", "selections", "fills", "gross_profit_pct",
            "gross_loss_pct",
        ]
    ]


def rename_leg(leg: pd.DataFrame, side: str) -> pd.DataFrame:
    prefix = side.lower()
    return leg.rename(
        columns={
            column: f"{prefix}_{column}"
            for column in leg.columns
            if column != "day"
        }
    )


def build_curve(
    long_leg: pd.DataFrame,
    short_leg: pd.DataFrame,
    objective_name: str,
) -> pd.DataFrame:
    curve = rename_leg(long_leg, "LONG").merge(
        rename_leg(short_leg, "SHORT"),
        on="day",
        how="outer",
        validate="one_to_one",
    )
    curve = curve.sort_values("day").reset_index(drop=True)
    for side in ("long", "short"):
        for column in ("selected_symbol", "trade_details"):
            curve[f"{side}_{column}"] = curve[f"{side}_{column}"].fillna("")
        curve[f"{side}_status"] = curve[f"{side}_status"].fillna("NO_SIGNAL")
        for column in ("selections", "fills", "gross_profit_pct", "gross_loss_pct"):
            curve[f"{side}_{column}"] = curve[f"{side}_{column}"].fillna(0.0)

    curve["selections"] = curve["long_selections"] + curve["short_selections"]
    curve["fills"] = curve["long_fills"] + curve["short_fills"]
    curve["portfolio_net_return_pct"] = (
        curve["long_net_return_pct"].fillna(0.0)
        + curve["short_net_return_pct"].fillna(0.0)
    )

    cum_day_profit = 0.0
    cum_day_loss = 0.0
    cum_trade_profit = 0.0
    cum_trade_loss = 0.0
    rows: list[dict[str, Any]] = []
    for row in curve.itertuples(index=False):
        day_net = float(row.portfolio_net_return_pct)
        day_profit = max(day_net, 0.0)
        day_loss = max(-day_net, 0.0)
        trade_profit = float(row.long_gross_profit_pct + row.short_gross_profit_pct)
        trade_loss = float(row.long_gross_loss_pct + row.short_gross_loss_pct)
        cum_day_profit += day_profit
        cum_day_loss += day_loss
        cum_trade_profit += trade_profit
        cum_trade_loss += trade_loss
        rows.append(
            {
                "objective": objective_name,
                **row._asdict(),
                "signal_end": "09:25",
                "confirmation_end": "09:26",
                "day_trade_pf": pf(trade_profit, trade_loss),
                "day_pf": pf(day_profit, day_loss),
                "cumulative_net_pct": cum_day_profit - cum_day_loss,
                "cumulative_day_pf": pf(cum_day_profit, cum_day_loss),
                "cumulative_trade_pf": pf(cum_trade_profit, cum_trade_loss),
            }
        )
    return pd.DataFrame(rows)


def period_stats(curve: pd.DataFrame, days: set[date]) -> dict[str, Any]:
    sample = curve.loc[curve["day"].isin(days)]
    trade_profit = float(
        sample["long_gross_profit_pct"].sum()
        + sample["short_gross_profit_pct"].sum()
    )
    trade_loss = float(
        sample["long_gross_loss_pct"].sum()
        + sample["short_gross_loss_pct"].sum()
    )
    day_net = sample["portfolio_net_return_pct"].to_numpy(float)
    day_profit = float(day_net[day_net > 0].sum()) if day_net.size else 0.0
    day_loss = float(-day_net[day_net < 0].sum()) if day_net.size else 0.0
    return {
        "orders": int(sample["selections"].sum()),
        "fills": int(sample["fills"].sum()),
        "trade_pf": pf(trade_profit, trade_loss),
        "net_pct": float(day_net.sum()),
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
        "day_pf": pf(day_profit, day_loss),
        "max_day_pct": float(day_net.max()) if day_net.size else float("nan"),
        "max_day_profit_share": (
            float(day_net.max() / day_profit) if day_profit > 0 else float("nan")
        ),
    }


def render_report(
    curves: dict[str, pd.DataFrame],
    setups: dict[str, dict[str, tuple[str, pd.Series]]],
    summaries: dict[str, dict[str, dict[str, Any]]],
    split_day: date,
) -> str:
    session_count = max((len(curve) for curve in curves.values()), default=0)
    max_per_side = max(
        int(row["max_per_side"])
        for objective_setups in setups.values()
        for _, row in objective_setups.values()
    )
    frequency = (
        "at most one LONG and one SHORT contract in that single daily scan"
        if max_per_side == 1
        else f"each setup selects no more than {max_per_side} LONG and {max_per_side} SHORT contracts in that single daily scan"
    )
    lines = [
        "# FNO EMA/OI 09:25/09:26 V2 Best-Side Results",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Timing: the 5-minute signal candle ends at 09:25; one 1-minute confirmation ends at 09:26; stop-entry orders activate afterward.",
        f"- Frequency: {frequency}.",
        "- Objectives: highest full-history trade PF and highest full-history day PF across V2 filtered and force-daily modes.",
        f"- Validation warning: these winners were selected after viewing all {session_count} sessions and are descriptive, not honest out-of-sample validation.",
        "",
        "## Selected Setups",
        "",
        "| Objective | Side | Mode | Max/side | All trade PF | All day PF | Trades | Net % | Train PF | Test PF | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for side in ("LONG", "SHORT"):
            mode, row = setups[objective_name][side]
            lines.append(
                f"| {objective_name} | {side} | {mode} | {int(row['max_per_side'])} | "
                f"{fmt(row['all_pf'])} | {fmt(row['all_day_pf'])} | {int(row['all_trades'])} | "
                f"{float(row['all_net_pct']):+.3f} | {fmt(row['train_pf'])} | {fmt(row['test_pf'])} | "
                f"{row['picker']} | {row['price_change_pct']} | {row['oi_change_pct']} | "
                f"{row['volume_ratio']} | {row['body_ratio']} | {row['max_wick_ratio']} | "
                f"{row['stop_pct']} | {row['target_pct']} |"
            )

    lines += [
        "",
        "## Portfolio Summary",
        "",
        "| Objective | Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF | Best day % | Best-day profit share |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for period in ("TRAIN", "TEST", "ALL"):
            stats = summaries[objective_name][period]
            lines.append(
                f"| {objective_name} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt(stats['trade_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | {stats['flat_days']} | "
                f"{fmt(stats['day_pf'])} | {fmt_signed(stats['max_day_pct'])} | "
                f"{stats['max_day_profit_share']:.1%} |"
            )

    columns = (
        "| Day | Period | L O/F | LONG contract results | LONG % | S O/F | SHORT contract results | SHORT % | "
        "Total O/F | Total % | Day trade PF | Day net PF | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for objective_name, curve in curves.items():
        lines += ["", f"## {objective_name} Day-Wise Table", "", columns, divider]
        for row in curve.itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            long_detail = row.long_trade_details or row.long_status
            short_detail = row.short_trade_details or row.short_status
            lines.append(
                f"| {row.day} | {period} | {int(row.long_selections)}/{int(row.long_fills)} | "
                f"{long_detail} | {fmt_signed(row.long_net_return_pct)} | "
                f"{int(row.short_selections)}/{int(row.short_fills)} | {short_detail} | "
                f"{fmt_signed(row.short_net_return_pct)} | {int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {fmt(row.day_trade_pf)} | "
                f"{fmt(row.day_pf)} | {fmt_signed(row.cumulative_net_pct)} | "
                f"{fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
            )

    lines += ["", "## Files", "", f"- Combined daily CSV: `{DAILY_OUTPUT_PATH}`", ""]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    split_day = pd.Timestamp(args.split_day).date()
    setups: dict[str, dict[str, tuple[str, pd.Series]]] = {}
    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}

    for objective_name, objective in OBJECTIVES.items():
        selected = {
            side: best_mode_for_side(side, objective)
            for side in ("LONG", "SHORT")
        }
        long_mode, long_files, long_setup = selected["LONG"]
        short_mode, short_files, short_setup = selected["SHORT"]
        curve = build_curve(
            load_leg(long_files, "LONG", objective.source_model),
            load_leg(short_files, "SHORT", objective.source_model),
            objective_name,
        )
        if (curve[["long_selections", "short_selections"]] > 5).any().any():
            raise AssertionError("V2 combo contains more than five selections on one side.")
        all_days = set(curve["day"])
        train_days = {day for day in all_days if day < split_day}
        test_days = all_days - train_days
        setups[objective_name] = {
            "LONG": (long_mode, long_setup),
            "SHORT": (short_mode, short_setup),
        }
        curves[objective_name] = curve
        summaries[objective_name] = {
            "TRAIN": period_stats(curve, train_days),
            "TEST": period_stats(curve, test_days),
            "ALL": period_stats(curve, all_days),
        }

    combined = pd.concat(curves.values(), ignore_index=True)
    common.atomic_write_csv(combined, DAILY_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(curves, setups, summaries, split_day),
    )
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        print(
            f"[{objective_name}] trade PF={stats['trade_pf']:.3f} "
            f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%"
        )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
