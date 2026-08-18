"""Combine the highest-PF 09:25/09:26 LONG and SHORT research setups.

The input optimisers enforce one selected contract per side and session. This
report compares their filtered and force-daily outputs, picks the highest
full-history PF independently for LONG and SHORT, and builds one day-wise
portfolio. The result is descriptive and in-sample, not a live validation.
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
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_best_side_combo.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_best_side_combo_daily.csv"
MODEL = "FULL_HISTORY_MAX_PF"


@dataclass(frozen=True)
class ModeFiles:
    daily: Path
    ranked_long: Path
    ranked_short: Path


MODES = {
    "FORCE_DAILY": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_force_daily_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_force_daily_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_force_daily_ranked_SHORT.csv",
    ),
    "FILTERED": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_once_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_once_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_once_ranked_SHORT.csv",
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


def best_mode_for_side(side: str) -> tuple[str, ModeFiles, pd.Series]:
    candidates: list[tuple[str, ModeFiles, pd.Series]] = []
    for mode, files in MODES.items():
        ranked_path = files.ranked_long if side == "LONG" else files.ranked_short
        if not ranked_path.exists():
            raise FileNotFoundError(f"Missing optimiser output: {ranked_path}")
        ranked = pd.read_csv(ranked_path)
        full_rank = pd.to_numeric(ranked["full_history_rank"], errors="coerce")
        best = ranked.loc[full_rank.eq(1)]
        if best.empty:
            raise RuntimeError(f"No full-history rank 1 {side} row in {ranked_path}")
        candidates.append((mode, files, best.iloc[0]))
    return max(candidates, key=lambda item: float(item[2]["all_pf"]))


def load_leg(files: ModeFiles, side: str) -> pd.DataFrame:
    if not files.daily.exists():
        raise FileNotFoundError(f"Missing daily optimiser output: {files.daily}")
    daily = pd.read_csv(files.daily)
    leg = daily.loc[daily["model"].eq(MODEL) & daily["side"].eq(side)].copy()
    if leg.empty:
        raise RuntimeError(f"No {MODEL} {side} rows in {files.daily}")
    leg["day"] = pd.to_datetime(leg["day"]).dt.date
    if leg["day"].duplicated().any():
        raise RuntimeError(f"Duplicate {side} sessions in {files.daily}")
    leg["net_return_pct"] = pd.to_numeric(leg["net_return_pct"], errors="coerce")
    return leg[["day", "selected_symbol", "status", "net_return_pct"]]


def build_curve(long_leg: pd.DataFrame, short_leg: pd.DataFrame) -> pd.DataFrame:
    long_leg = long_leg.rename(
        columns={
            "selected_symbol": "long_symbol",
            "status": "long_status",
            "net_return_pct": "long_return_pct",
        }
    )
    short_leg = short_leg.rename(
        columns={
            "selected_symbol": "short_symbol",
            "status": "short_status",
            "net_return_pct": "short_return_pct",
        }
    )
    curve = long_leg.merge(short_leg, on="day", how="outer", validate="one_to_one")
    curve = curve.sort_values("day").reset_index(drop=True)
    for side in ("long", "short"):
        curve[f"{side}_symbol"] = curve[f"{side}_symbol"].fillna("")
        curve[f"{side}_status"] = curve[f"{side}_status"].fillna("NO_SIGNAL")

    filled_statuses = {"WIN", "LOSS", "FLAT"}
    curve["selections"] = (
        curve["long_status"].ne("NO_SIGNAL").astype(int)
        + curve["short_status"].ne("NO_SIGNAL").astype(int)
    )
    curve["fills"] = (
        curve["long_status"].isin(filled_statuses).astype(int)
        + curve["short_status"].isin(filled_statuses).astype(int)
    )
    curve["portfolio_net_return_pct"] = (
        curve["long_return_pct"].fillna(0.0) + curve["short_return_pct"].fillna(0.0)
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
        leg_returns = [
            float(value)
            for value in (row.long_return_pct, row.short_return_pct)
            if pd.notna(value)
        ]
        trade_profit = sum(max(value, 0.0) for value in leg_returns)
        trade_loss = sum(max(-value, 0.0) for value in leg_returns)
        cum_day_profit += day_profit
        cum_day_loss += day_loss
        cum_trade_profit += trade_profit
        cum_trade_loss += trade_loss
        rows.append(
            {
                **row._asdict(),
                "signal_end": "09:25",
                "confirmation_end": "09:26",
                "day_pf": pf(day_profit, day_loss),
                "cumulative_net_pct": cum_day_profit - cum_day_loss,
                "cumulative_day_pf": pf(cum_day_profit, cum_day_loss),
                "cumulative_trade_pf": pf(cum_trade_profit, cum_trade_loss),
            }
        )
    return pd.DataFrame(rows)


def period_stats(curve: pd.DataFrame, days: set[date]) -> dict[str, Any]:
    sample = curve.loc[curve["day"].isin(days)]
    returns = pd.concat(
        [sample["long_return_pct"], sample["short_return_pct"]], ignore_index=True
    ).dropna().to_numpy(float)
    profit = float(returns[returns > 0].sum()) if returns.size else 0.0
    loss = float(-returns[returns < 0].sum()) if returns.size else 0.0
    day_net = sample["portfolio_net_return_pct"].to_numpy(float)
    day_profit = float(day_net[day_net > 0].sum()) if day_net.size else 0.0
    day_loss = float(-day_net[day_net < 0].sum()) if day_net.size else 0.0
    return {
        "orders": int(sample["selections"].sum()),
        "fills": int(sample["fills"].sum()),
        "trade_pf": pf(profit, loss),
        "net_pct": float(day_net.sum()),
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
        "day_pf": pf(day_profit, day_loss),
        "max_day_pct": float(day_net.max()) if day_net.size else float("nan"),
        "max_day_profit_share": float(day_net.max() / day_profit) if day_profit > 0 else float("nan"),
    }


def render_report(
    curve: pd.DataFrame,
    setups: dict[str, tuple[str, pd.Series]],
    summaries: dict[str, dict[str, Any]],
    split_day: date,
) -> str:
    lines = [
        "# FNO EMA/OI 09:25 Best-Side 09:26 Combo",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Timing: the 5-minute signal candle ends at 09:25; the 1-minute confirmation ends at 09:26; the stop-entry order activates afterward.",
        "- Frequency: at most one selected LONG and one selected SHORT per session.",
        "- Objective: highest full-history side PF across force-daily and filtered one-shot modes.",
        "- Validation warning: this combination was selected after seeing all 52 sessions. Treat it as a research winner, not an honest out-of-sample result.",
        "",
        "## Selected Setups",
        "",
        "| Side | Mode | All PF | All trades | All net % | Train PF | Test PF | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for side in ("LONG", "SHORT"):
        mode, row = setups[side]
        lines.append(
            f"| {side} | {mode} | {fmt(row['all_pf'])} | {int(row['all_trades'])} | "
            f"{float(row['all_net_pct']):+.3f} | {fmt(row['train_pf'])} | {fmt(row['test_pf'])} | "
            f"{row['picker']} | {row['price_change_pct']} | {row['oi_change_pct']} | "
            f"{row['volume_ratio']} | {row['body_ratio']} | {row['max_wick_ratio']} | "
            f"{row['stop_pct']} | {row['target_pct']} |"
        )

    lines += [
        "",
        "## Portfolio Summary",
        "",
        "| Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF | Best day % | Best-day profit share |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for period in ("TRAIN", "TEST", "ALL"):
        stats = summaries[period]
        lines.append(
            f"| {period} | {stats['orders']} | {stats['fills']} | {fmt(stats['trade_pf'])} | "
            f"{stats['net_pct']:+.3f} | {stats['positive_days']} | {stats['negative_days']} | "
            f"{stats['flat_days']} | {fmt(stats['day_pf'])} | {fmt_signed(stats['max_day_pct'])} | "
            f"{stats['max_day_profit_share']:.1%} |"
        )

    lines += [
        "",
        "## Day-Wise Table",
        "",
        "| Day | Period | Long symbol | Long status | Long % | Short symbol | Short status | Short % | Selections | Fills | Total % | Cum % | Day PF | Cum day PF | Cum trade PF |",
        "| --- | --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in curve.itertuples(index=False):
        period = "TEST" if row.day >= split_day else "TRAIN"
        lines.append(
            f"| {row.day} | {period} | {row.long_symbol} | {row.long_status} | "
            f"{fmt_signed(row.long_return_pct)} | {row.short_symbol} | {row.short_status} | "
            f"{fmt_signed(row.short_return_pct)} | {int(row.selections)} | {int(row.fills)} | "
            f"{fmt_signed(row.portfolio_net_return_pct)} | {fmt_signed(row.cumulative_net_pct)} | "
            f"{fmt(row.day_pf)} | {fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
        )
    lines += [
        "",
        "## Files",
        "",
        f"- Daily CSV: `{DAILY_OUTPUT_PATH}`",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    split_day = pd.Timestamp(args.split_day).date()
    selected: dict[str, tuple[str, ModeFiles, pd.Series]] = {
        side: best_mode_for_side(side) for side in ("LONG", "SHORT")
    }
    long_mode, long_files, long_setup = selected["LONG"]
    short_mode, short_files, short_setup = selected["SHORT"]
    curve = build_curve(load_leg(long_files, "LONG"), load_leg(short_files, "SHORT"))

    all_days = set(curve["day"])
    train_days = {day for day in all_days if day < split_day}
    test_days = all_days - train_days
    summaries = {
        "TRAIN": period_stats(curve, train_days),
        "TEST": period_stats(curve, test_days),
        "ALL": period_stats(curve, all_days),
    }
    setups = {
        "LONG": (long_mode, long_setup),
        "SHORT": (short_mode, short_setup),
    }
    common.atomic_write_csv(curve, DAILY_OUTPUT_PATH)
    common.atomic_write_text(REPORT_PATH, render_report(curve, setups, summaries, split_day))
    print(
        f"[DONE] LONG={long_mode} PF={float(long_setup['all_pf']):.3f} | "
        f"SHORT={short_mode} PF={float(short_setup['all_pf']):.3f} | "
        f"portfolio PF={summaries['ALL']['trade_pf']:.3f} "
        f"net={summaries['ALL']['net_pct']:+.3f}%"
    )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
