"""Stack the 09:30/09:31 V3 add-on onto the locked V2 baseline.

The baseline is exactly MORE_SHORT_2X_HIGH_PF from the V2 short-expansion
report. V3 never re-optimises or rewrites those 09:25/09:26 trades. It compares
the filtered and force-daily 09:30/09:31 add-on winners, with at most two LONG
and two SHORT orders in the add-on scan.
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


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_0930_v3_best_side_combo.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_0930_v3_best_side_combo_daily.csv"
SELECTED_DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_0930_v3_selected_daily.csv"
SELECTED_OBJECTIVE = "BEST_TRADE_PF"

BASELINE_VARIANT = "MORE_SHORT_2X_HIGH_PF"
BASELINE_PORTFOLIO_PATH = RESULT_DIR / "ema_confirm_0925_short_expansion_v2_daily.csv"
BASELINE_LONG_PATH = RESULT_DIR / "ema_confirm_0925_v2_force_daily_daily_pf.csv"
BASELINE_SHORT_AUDIT_PATH = RESULT_DIR / "ema_confirm_0925_short_expansion_v2_selected_trades.csv"
BASELINE_EXPECTED = {
    "trade_pf": 3.081,
    "day_pf": 5.519,
    "net_pct": 53.546,
}
MAX_ADDON_PER_SIDE = 2


@dataclass(frozen=True)
class ModeFiles:
    daily: Path
    ranked_long: Path
    ranked_short: Path


@dataclass(frozen=True)
class SetupChoice:
    mode: str
    files: ModeFiles
    source_model: str
    row: pd.Series


MODES = {
    "FORCE_DAILY": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_0930_v3_force_daily_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_0930_v3_force_daily_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_0930_v3_force_daily_ranked_SHORT.csv",
    ),
    "FILTERED": ModeFiles(
        daily=RESULT_DIR / "ema_confirm_0925_0930_v3_once_daily_pf.csv",
        ranked_long=RESULT_DIR / "ema_confirm_0925_0930_v3_once_ranked_LONG.csv",
        ranked_short=RESULT_DIR / "ema_confirm_0925_0930_v3_once_ranked_SHORT.csv",
    ),
}

SOURCE_MODELS = (
    ("FULL_HISTORY_MAX_PF", "full_history_rank"),
    ("FULL_HISTORY_MAX_DAY_PF", "full_history_day_rank"),
)

OBJECTIVES = {
    "BEST_TRADE_PF": "trade_pf",
    "BEST_DAY_PF": "day_pf",
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


def timed_details(value: Any, confirmation: str) -> str:
    if value is None or pd.isna(value) or not str(value).strip():
        return ""
    return "; ".join(
        f"[{confirmation}] {part.strip()}"
        for part in str(value).split(";")
        if part.strip()
    )


def setup_options(side: str) -> list[SetupChoice]:
    options: list[SetupChoice] = []
    for mode, files in MODES.items():
        ranked_path = files.ranked_long if side == "LONG" else files.ranked_short
        if not ranked_path.exists():
            continue
        ranked = pd.read_csv(ranked_path)
        for source_model, rank_column in SOURCE_MODELS:
            rank = pd.to_numeric(ranked[rank_column], errors="coerce")
            best = ranked.loc[rank.eq(1)]
            if best.empty:
                continue
            row = best.iloc[0]
            max_per_side = int(row["max_per_side"])
            if max_per_side > MAX_ADDON_PER_SIDE:
                raise RuntimeError(
                    f"Stale V3 output allows {max_per_side} {side} orders at 09:31; "
                    f"rerun with --signal-slot 930 --max-per-side {MAX_ADDON_PER_SIDE}."
                )
            options.append(SetupChoice(mode, files, source_model, row))
    if not options:
        raise RuntimeError(f"No 09:31 V3 {side} setup survived the optimiser guards.")
    return options


def load_addon_leg(choice: SetupChoice, side: str) -> pd.DataFrame:
    if not choice.files.daily.exists():
        raise FileNotFoundError(f"Missing V3 optimiser output: {choice.files.daily}")
    daily = pd.read_csv(choice.files.daily)
    leg = daily.loc[
        daily["model"].eq(choice.source_model) & daily["side"].eq(side)
    ].copy()
    if leg.empty:
        raise RuntimeError(
            f"No {choice.source_model} {side} rows in {choice.files.daily}"
        )
    signal_ends = set(leg["signal_end"].dropna().astype(str))
    confirmation_ends = set(leg["confirmation_end"].dropna().astype(str))
    if signal_ends != {"09:30"} or confirmation_ends != {"09:31"}:
        raise RuntimeError(
            f"{choice.files.daily} is not a 09:30/09:31-only add-on run. "
            "Run the V3 BAT again."
        )
    leg["day"] = pd.to_datetime(leg["day"]).dt.date
    if leg["day"].duplicated().any():
        raise RuntimeError(f"Duplicate {side} sessions in {choice.files.daily}")
    for column in (
        "net_return_pct",
        "selections",
        "fills",
        "gross_profit_pct",
        "gross_loss_pct",
    ):
        leg[column] = pd.to_numeric(leg[column], errors="coerce")
    return leg[
        [
            "day",
            "selected_symbol",
            "trade_details",
            "status",
            "net_return_pct",
            "selections",
            "fills",
            "gross_profit_pct",
            "gross_loss_pct",
        ]
    ]


def load_baseline() -> tuple[pd.DataFrame, dict[str, Any]]:
    required = (
        BASELINE_PORTFOLIO_PATH,
        BASELINE_LONG_PATH,
        BASELINE_SHORT_AUDIT_PATH,
    )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing protected V2 baseline output(s): " + ", ".join(missing))

    portfolio = pd.read_csv(BASELINE_PORTFOLIO_PATH)
    baseline = portfolio.loc[portfolio["variant"].eq(BASELINE_VARIANT)].copy()
    if baseline.empty:
        raise RuntimeError(
            f"{BASELINE_VARIANT} is missing from {BASELINE_PORTFOLIO_PATH}"
        )
    baseline["day"] = pd.to_datetime(baseline["day"]).dt.date
    if baseline["day"].duplicated().any():
        raise RuntimeError("The V2 baseline contains duplicate sessions.")

    long_daily = pd.read_csv(BASELINE_LONG_PATH)
    long_daily = long_daily.loc[
        long_daily["model"].eq("FULL_HISTORY_MAX_PF")
        & long_daily["side"].eq("LONG")
    ].copy()
    long_daily["day"] = pd.to_datetime(long_daily["day"]).dt.date
    for column in ("selections", "fills", "gross_profit_pct", "gross_loss_pct"):
        long_daily[column] = pd.to_numeric(long_daily[column], errors="coerce").fillna(0.0)
    long_daily = long_daily[
        ["day", "selections", "fills", "gross_profit_pct", "gross_loss_pct"]
    ].rename(
        columns={
            "selections": "audit_long_selections",
            "fills": "audit_long_fills",
            "gross_profit_pct": "baseline_long_gross_profit_pct",
            "gross_loss_pct": "baseline_long_gross_loss_pct",
        }
    )

    short_audit = pd.read_csv(BASELINE_SHORT_AUDIT_PATH)
    short_audit = short_audit.loc[short_audit["model"].eq(BASELINE_VARIANT)].copy()
    short_audit["day"] = pd.to_datetime(short_audit["day"]).dt.date
    short_net = pd.to_numeric(short_audit["net_return_pct"], errors="coerce")
    short_audit["filled"] = short_net.notna().astype(int)
    short_audit["gross_profit_pct"] = short_net.where(short_net > 0, 0.0).fillna(0.0)
    short_audit["gross_loss_pct"] = (-short_net.where(short_net < 0, 0.0)).fillna(0.0)
    short_daily = short_audit.groupby("day", as_index=False).agg(
        audit_short_selections=("sid", "size"),
        audit_short_fills=("filled", "sum"),
        baseline_short_gross_profit_pct=("gross_profit_pct", "sum"),
        baseline_short_gross_loss_pct=("gross_loss_pct", "sum"),
    )

    columns = [
        "day",
        "long_symbol",
        "long_status",
        "long_return_pct",
        "long_selections",
        "long_fills",
        "long_trade_details",
        "short_symbol",
        "short_status",
        "short_return_pct",
        "short_selections",
        "short_fills",
        "short_trade_details",
    ]
    baseline = baseline[columns].merge(
        long_daily, on="day", how="left", validate="one_to_one"
    ).merge(short_daily, on="day", how="left", validate="one_to_one")

    for column in (
        "long_return_pct",
        "long_selections",
        "long_fills",
        "short_return_pct",
        "short_selections",
        "short_fills",
        "audit_long_selections",
        "audit_long_fills",
        "audit_short_selections",
        "audit_short_fills",
        "baseline_long_gross_profit_pct",
        "baseline_long_gross_loss_pct",
        "baseline_short_gross_profit_pct",
        "baseline_short_gross_loss_pct",
    ):
        baseline[column] = pd.to_numeric(baseline[column], errors="coerce").fillna(0.0)

    if not np.array_equal(
        baseline["long_selections"].to_numpy(int),
        baseline["audit_long_selections"].to_numpy(int),
    ) or not np.array_equal(
        baseline["long_fills"].to_numpy(int),
        baseline["audit_long_fills"].to_numpy(int),
    ):
        raise AssertionError("Protected V2 LONG orders do not reconcile.")
    if not np.array_equal(
        baseline["short_selections"].to_numpy(int),
        baseline["audit_short_selections"].to_numpy(int),
    ) or not np.array_equal(
        baseline["short_fills"].to_numpy(int),
        baseline["audit_short_fills"].to_numpy(int),
    ):
        raise AssertionError("Protected V2 SHORT orders do not reconcile.")

    baseline = baseline.drop(
        columns=[
            "audit_long_selections",
            "audit_long_fills",
            "audit_short_selections",
            "audit_short_fills",
        ]
    )
    baseline = baseline.rename(
        columns={
            column: f"baseline_{column}"
            for column in baseline.columns
            if column != "day" and not column.startswith("baseline_")
        }
    )
    baseline["baseline_long_trade_details"] = baseline[
        "baseline_long_trade_details"
    ].map(lambda value: timed_details(value, "09:26"))
    baseline["baseline_short_trade_details"] = baseline[
        "baseline_short_trade_details"
    ].map(lambda value: timed_details(value, "09:26"))

    long_profit = float(baseline["baseline_long_gross_profit_pct"].sum())
    long_loss = float(baseline["baseline_long_gross_loss_pct"].sum())
    short_profit = float(baseline["baseline_short_gross_profit_pct"].sum())
    short_loss = float(baseline["baseline_short_gross_loss_pct"].sum())
    metadata = {
        "long_orders": int(baseline["baseline_long_selections"].sum()),
        "long_fills": int(baseline["baseline_long_fills"].sum()),
        "long_pf": pf(long_profit, long_loss),
        "long_net_pct": long_profit - long_loss,
        "short_orders": int(baseline["baseline_short_selections"].sum()),
        "short_fills": int(baseline["baseline_short_fills"].sum()),
        "short_pf": pf(short_profit, short_loss),
        "short_net_pct": short_profit - short_loss,
    }
    return baseline.sort_values("day").reset_index(drop=True), metadata


def rename_addon_leg(leg: pd.DataFrame, side: str) -> pd.DataFrame:
    prefix = f"addon_{side.lower()}"
    return leg.rename(
        columns={
            column: (
                f"{prefix}_return_pct"
                if column == "net_return_pct"
                else f"{prefix}_{column}"
            )
            for column in leg.columns
            if column != "day"
        }
    )


def build_curve(
    baseline: pd.DataFrame,
    addon_long: pd.DataFrame,
    addon_short: pd.DataFrame,
    objective_name: str,
) -> pd.DataFrame:
    curve = baseline.merge(
        rename_addon_leg(addon_long, "LONG"),
        on="day",
        how="outer",
        validate="one_to_one",
    ).merge(
        rename_addon_leg(addon_short, "SHORT"),
        on="day",
        how="outer",
        validate="one_to_one",
    )
    curve = curve.sort_values("day").reset_index(drop=True)

    for side in ("long", "short"):
        for family in ("baseline", "addon"):
            text_columns = (
                f"{family}_{side}_selected_symbol" if family == "addon" else f"{family}_{side}_symbol",
                f"{family}_{side}_trade_details",
            )
            for column in text_columns:
                curve[column] = curve[column].fillna("")
            status_column = f"{family}_{side}_status"
            curve[status_column] = curve[status_column].fillna("NO_SIGNAL")
            for suffix in (
                "return_pct",
                "selections",
                "fills",
                "gross_profit_pct",
                "gross_loss_pct",
            ):
                column = f"{family}_{side}_{suffix}"
                curve[column] = pd.to_numeric(curve[column], errors="coerce").fillna(0.0)

        curve[f"{side}_selections"] = (
            curve[f"baseline_{side}_selections"] + curve[f"addon_{side}_selections"]
        )
        curve[f"{side}_fills"] = (
            curve[f"baseline_{side}_fills"] + curve[f"addon_{side}_fills"]
        )
        curve[f"{side}_return_pct"] = (
            curve[f"baseline_{side}_return_pct"] + curve[f"addon_{side}_return_pct"]
        )
        curve[f"{side}_gross_profit_pct"] = (
            curve[f"baseline_{side}_gross_profit_pct"]
            + curve[f"addon_{side}_gross_profit_pct"]
        )
        curve[f"{side}_gross_loss_pct"] = (
            curve[f"baseline_{side}_gross_loss_pct"]
            + curve[f"addon_{side}_gross_loss_pct"]
        )

    curve["baseline_net_return_pct"] = (
        curve["baseline_long_return_pct"] + curve["baseline_short_return_pct"]
    )
    curve["addon_net_return_pct"] = (
        curve["addon_long_return_pct"] + curve["addon_short_return_pct"]
    )
    curve["selections"] = curve["long_selections"] + curve["short_selections"]
    curve["fills"] = curve["long_fills"] + curve["short_fills"]
    curve["portfolio_net_return_pct"] = (
        curve["baseline_net_return_pct"] + curve["addon_net_return_pct"]
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
                "signal_end": "09:25,09:30",
                "confirmation_end": "09:26,09:31",
                "day_trade_pf": pf(trade_profit, trade_loss),
                "day_pf": pf(day_profit, day_loss),
                "cumulative_net_pct": cum_day_profit - cum_day_loss,
                "cumulative_day_pf": pf(cum_day_profit, cum_day_loss),
                "cumulative_trade_pf": pf(cum_trade_profit, cum_trade_loss),
            }
        )
    return pd.DataFrame(rows)


def empty_addon(days: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": days,
            "selected_symbol": "",
            "trade_details": "",
            "status": "NO_SIGNAL",
            "net_return_pct": 0.0,
            "selections": 0,
            "fills": 0,
            "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0,
        }
    )


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
    }


def objective_key(stats: dict[str, Any], metric: str) -> tuple[float, float, float]:
    secondary = "day_pf" if metric == "trade_pf" else "trade_pf"
    return (
        float(stats[metric]),
        float(stats[secondary]),
        float(stats["net_pct"]),
    )


def render_report(
    curves: dict[str, pd.DataFrame],
    setups: dict[str, dict[str, SetupChoice]],
    summaries: dict[str, dict[str, dict[str, Any]]],
    baseline_summary: dict[str, dict[str, Any]],
    baseline_sides: dict[str, Any],
    split_day: date,
) -> str:
    baseline_all = baseline_summary["ALL"]
    chosen = summaries[SELECTED_OBJECTIVE]["ALL"]
    chosen_long = setups[SELECTED_OBJECTIVE]["LONG"]
    chosen_short = setups[SELECTED_OBJECTIVE]["SHORT"]
    lines = [
        "# FNO EMA/OI V3: Locked V2 Baseline + 09:31 Add-On",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Locked baseline: V2 `{BASELINE_VARIANT}` at 09:25/09:26. Its trades are loaded unchanged.",
        "- Add-on timing: one additional 5-minute scan ends 09:30; its 1-minute confirmation ends 09:31; orders activate afterward.",
        f"- Add-on cap: at most {MAX_ADDON_PER_SIDE} LONG and {MAX_ADDON_PER_SIDE} SHORT orders at 09:31.",
        f"- Official V3 selection: `{SELECTED_OBJECTIVE}`.",
        "- The two objectives compare the available filtered and force-daily 09:31 winner combinations on the complete history.",
        "- Validation warning: add-on winners are descriptive, full-history selections, not honest out-of-sample validation.",
        "",
        "## Chosen V3",
        "",
        "| Selection | 09:31 LONG max | 09:31 SHORT max | Orders | Fills | Trade PF | Day PF | Net % | Uplift vs V2 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {SELECTED_OBJECTIVE} | {int(chosen_long.row['max_per_side'])} | "
        f"{int(chosen_short.row['max_per_side'])} | {chosen['orders']} | {chosen['fills']} | "
        f"{fmt(chosen['trade_pf'])} | {fmt(chosen['day_pf'])} | {chosen['net_pct']:+.3f} | "
        f"{chosen['net_pct'] - baseline_all['net_pct']:+.3f} |",
        "",
        "## Locked V2 Baseline",
        "",
        "| System | 09:26 LONG max | 09:26 SHORT max | SHORT fills | SHORT PF | Orders | Fills | Trade PF | Day PF | Net % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {BASELINE_VARIANT} | 1 | 2 | {baseline_sides['short_fills']} | "
        f"{fmt(baseline_sides['short_pf'])} | {baseline_all['orders']} | {baseline_all['fills']} | "
        f"{fmt(baseline_all['trade_pf'])} | {fmt(baseline_all['day_pf'])} | "
        f"{baseline_all['net_pct']:+.3f} |",
        "",
        "## Selected 09:31 Add-On Setups",
        "",
        "| Objective | Side | Mode | Source winner | Max at 09:31 | Add-on PF | Add-on day PF | Trades | Net % | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for side in ("LONG", "SHORT"):
            choice = setups[objective_name][side]
            row = choice.row
            lines.append(
                f"| {objective_name} | {side} | {choice.mode} | {choice.source_model} | "
                f"{int(row['max_per_side'])} | {fmt(row['all_pf'])} | {fmt(row['all_day_pf'])} | "
                f"{int(row['all_trades'])} | {float(row['all_net_pct']):+.3f} | {row['picker']} | "
                f"{row['price_change_pct']} | {row['oi_change_pct']} | {row['volume_ratio']} | "
                f"{row['body_ratio']} | {row['max_wick_ratio']} | {row['stop_pct']} | {row['target_pct']} |"
            )

    lines += [
        "",
        "## Portfolio Comparison",
        "",
        "| Portfolio | Orders | Fills | Trade PF | Day PF | Net % | Net uplift vs V2 | Positive days | Negative days | Flat days | Best day % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| V2 {BASELINE_VARIANT} | {baseline_all['orders']} | {baseline_all['fills']} | "
        f"{fmt(baseline_all['trade_pf'])} | {fmt(baseline_all['day_pf'])} | "
        f"{baseline_all['net_pct']:+.3f} | +0.000 | {baseline_all['positive_days']} | "
        f"{baseline_all['negative_days']} | {baseline_all['flat_days']} | "
        f"{fmt_signed(baseline_all['max_day_pct'])} |",
    ]
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        selected_label = " (CHOSEN)" if objective_name == SELECTED_OBJECTIVE else ""
        lines.append(
            f"| V3 {objective_name}{selected_label} | {stats['orders']} | {stats['fills']} | "
            f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | "
            f"{stats['net_pct'] - baseline_all['net_pct']:+.3f} | {stats['positive_days']} | "
            f"{stats['negative_days']} | {stats['flat_days']} | {fmt_signed(stats['max_day_pct'])} |"
        )

    lines += [
        "",
        "## Combined Period Results",
        "",
        "| Objective | Period | Orders | Fills | Trade PF | Day PF | Net % | Positive days | Negative days | Flat days |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for period in ("TRAIN", "TEST", "ALL"):
            stats = summaries[objective_name][period]
            lines.append(
                f"| {objective_name} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | {stats['flat_days']} |"
            )

    columns = (
        "| Day | Period | V2 L O/F | V2 LONG at 09:26 | V2 S O/F | V2 SHORT at 09:26 | "
        "09:31 L O/F | 09:31 LONG | 09:31 S O/F | 09:31 SHORT | V2 % | Add-on % | Total O/F | Total % | Cum % | Cum day PF | Cum trade PF |"
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
                f"| {row.day} | {period} | {int(row.baseline_long_selections)}/{int(row.baseline_long_fills)} | "
                f"{baseline_long} | {int(row.baseline_short_selections)}/{int(row.baseline_short_fills)} | "
                f"{baseline_short} | {int(row.addon_long_selections)}/{int(row.addon_long_fills)} | "
                f"{addon_long} | {int(row.addon_short_selections)}/{int(row.addon_short_fills)} | "
                f"{addon_short} | {fmt_signed(row.baseline_net_return_pct)} | "
                f"{fmt_signed(row.addon_net_return_pct)} | {int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {fmt_signed(row.cumulative_net_pct)} | "
                f"{fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
            )

    lines += [
        "",
        "## Files",
        "",
        f"- Official selected V3 daily CSV: `{SELECTED_DAILY_OUTPUT_PATH}`",
        f"- Combined V3 daily CSV: `{DAILY_OUTPUT_PATH}`",
        f"- Locked V2 source curve: `{BASELINE_PORTFOLIO_PATH}`",
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
    baseline, baseline_sides = load_baseline()
    all_days = set(baseline["day"])
    train_days = {day for day in all_days if day < split_day}
    test_days = all_days - train_days
    zero_addon = empty_addon(baseline["day"])
    baseline_curve = build_curve(baseline, zero_addon, zero_addon, "V2_BASELINE")
    baseline_summary = {
        "TRAIN": period_stats(baseline_curve, train_days),
        "TEST": period_stats(baseline_curve, test_days),
        "ALL": period_stats(baseline_curve, all_days),
    }
    for metric, expected in BASELINE_EXPECTED.items():
        actual = float(baseline_summary["ALL"][metric])
        if abs(actual - expected) > 0.0015:
            raise AssertionError(
                f"Protected V2 {metric} changed: expected {expected:.3f}, got {actual:.3f}."
            )

    options = {side: setup_options(side) for side in ("LONG", "SHORT")}
    leg_cache: dict[tuple[str, str, str], pd.DataFrame] = {}

    def cached_leg(choice: SetupChoice, side: str) -> pd.DataFrame:
        key = (choice.mode, choice.source_model, side)
        if key not in leg_cache:
            leg_cache[key] = load_addon_leg(choice, side)
            if set(leg_cache[key]["day"]) != all_days:
                raise RuntimeError(
                    f"{choice.files.daily} does not cover the same sessions as the V2 baseline."
                )
        return leg_cache[key]

    setups: dict[str, dict[str, SetupChoice]] = {}
    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    for objective_name, metric in OBJECTIVES.items():
        best: tuple[
            tuple[float, float, float],
            SetupChoice,
            SetupChoice,
            pd.DataFrame,
            dict[str, Any],
        ] | None = None
        for long_choice, short_choice in itertools.product(
            options["LONG"], options["SHORT"]
        ):
            curve = build_curve(
                baseline,
                cached_leg(long_choice, "LONG"),
                cached_leg(short_choice, "SHORT"),
                objective_name,
            )
            if (
                (curve["addon_long_selections"] > MAX_ADDON_PER_SIDE).any()
                or (curve["addon_short_selections"] > MAX_ADDON_PER_SIDE).any()
            ):
                raise AssertionError("The 09:31 add-on exceeded its two-per-side cap.")
            stats = period_stats(curve, all_days)
            candidate = (
                objective_key(stats, metric),
                long_choice,
                short_choice,
                curve,
                stats,
            )
            if best is None or candidate[0] > best[0]:
                best = candidate
        if best is None:
            raise RuntimeError(f"No setup combination exists for {objective_name}.")
        _, long_choice, short_choice, curve, all_stats = best
        setups[objective_name] = {"LONG": long_choice, "SHORT": short_choice}
        curves[objective_name] = curve
        summaries[objective_name] = {
            "TRAIN": period_stats(curve, train_days),
            "TEST": period_stats(curve, test_days),
            "ALL": all_stats,
        }

    combined = pd.concat(curves.values(), ignore_index=True)
    common.atomic_write_csv(combined, DAILY_OUTPUT_PATH)
    common.atomic_write_csv(curves[SELECTED_OBJECTIVE], SELECTED_DAILY_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            curves,
            setups,
            summaries,
            baseline_summary,
            baseline_sides,
            split_day,
        ),
    )
    baseline_all = baseline_summary["ALL"]
    print(
        f"[V2 BASELINE] trade PF={baseline_all['trade_pf']:.3f} "
        f"day PF={baseline_all['day_pf']:.3f} net={baseline_all['net_pct']:+.3f}%"
    )
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        print(
            f"[{objective_name}] trade PF={stats['trade_pf']:.3f} "
            f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%"
        )
    selected = summaries[SELECTED_OBJECTIVE]["ALL"]
    print(
        f"[SELECTED V3] {SELECTED_OBJECTIVE} trade PF={selected['trade_pf']:.3f} "
        f"day PF={selected['day_pf']:.3f} net={selected['net_pct']:+.3f}%"
    )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
