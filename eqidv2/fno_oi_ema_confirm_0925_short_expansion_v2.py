"""Compare protected 09:26 LONG with higher-trade SHORT variants.

The protected LONG leg is loaded from the original one-per-side force-daily
FULL_HISTORY_MAX_PF output. SHORT variants are reconstructed from the same
09:25 signal / 09:26 confirmation cache so that timing remains fixed.
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
import fno_oi_ema_confirm_0925_pf_v2 as v2
import fno_oi_ema_confirm_sweep as sw
import fno_oi_ema_confirm_optimize as opt


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_short_expansion_v2.md"
DAILY_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_short_expansion_v2_daily.csv"
AUDIT_OUTPUT_PATH = RESULT_DIR / "ema_confirm_0925_short_expansion_v2_selected_trades.csv"
PROTECTED_LONG_DAILY = RESULT_DIR / "ema_confirm_0925_force_daily_daily_pf.csv"
SIGNAL_SLOT = 925


@dataclass(frozen=True)
class ShortVariant:
    name: str
    mode: str
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    body_ratio: float
    max_wick_ratio: float
    min_traded_value: float
    picker: str
    max_per_side: int
    stop_pct: float
    target_pct: float


VARIANTS = (
    ShortVariant(
        name="PROTECTED_SHORT_1X",
        mode="FILTERED",
        price_change_pct=0.4,
        oi_change_pct=0.75,
        volume_ratio=1.0,
        body_ratio=0.4,
        max_wick_ratio=0.3,
        min_traded_value=0.0,
        picker="max_oi",
        max_per_side=1,
        stop_pct=0.4,
        target_pct=2.0,
    ),
    ShortVariant(
        name="MORE_SHORT_1X",
        mode="FILTERED",
        price_change_pct=0.3,
        oi_change_pct=0.75,
        volume_ratio=1.0,
        body_ratio=0.4,
        max_wick_ratio=0.3,
        min_traded_value=0.0,
        picker="max_oi",
        max_per_side=1,
        stop_pct=0.4,
        target_pct=2.0,
    ),
    ShortVariant(
        name="MORE_SHORT_2X_HIGH_PF",
        mode="FILTERED",
        price_change_pct=0.3,
        oi_change_pct=0.5,
        volume_ratio=3.0,
        body_ratio=0.4,
        max_wick_ratio=0.5,
        min_traded_value=0.0,
        picker="max_volume",
        max_per_side=2,
        stop_pct=0.75,
        target_pct=2.0,
    ),
    ShortVariant(
        name="MORE_SHORT_2X_MAX_TRADES",
        mode="FILTERED",
        price_change_pct=0.3,
        oi_change_pct=0.5,
        volume_ratio=1.5,
        body_ratio=0.4,
        max_wick_ratio=0.5,
        min_traded_value=0.0,
        picker="max_volume",
        max_per_side=2,
        stop_pct=0.75,
        target_pct=2.0,
    ),
)


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


def setup_signal_globals(signals: pd.DataFrame, all_days: list[date]) -> None:
    day_code = {day: idx for idx, day in enumerate(all_days)}
    v2.SLOT_DAYS = signals["day"].to_numpy()
    v2.SLOT_DAY_IDX = np.array([day_code[day] for day in v2.SLOT_DAYS], dtype=int)
    v2.ALL_DAY_VALUES = all_days


def reconstruct_short(signals: pd.DataFrame, variant: ShortVariant) -> v2.Candidate:
    selection = v2.build_selections(
        signals,
        "SHORT",
        max_per_side=variant.max_per_side,
        force_daily=False,
    )
    for candidate in selection:
        values = candidate.values
        if (
            values["picker"] == variant.picker
            and int(values["max_per_side"]) == variant.max_per_side
            and abs(float(values["price_change_pct"]) - variant.price_change_pct) < 1e-9
            and abs(float(values["oi_change_pct"]) - variant.oi_change_pct) < 1e-9
            and abs(float(values["volume_ratio"]) - variant.volume_ratio) < 1e-9
            and abs(float(values["body_ratio"]) - variant.body_ratio) < 1e-9
            and abs(float(values["max_wick_ratio"]) - variant.max_wick_ratio) < 1e-9
            and abs(float(values["min_traded_value"]) - variant.min_traded_value) < 1e-6
        ):
            candidate.values = {
                **candidate.values,
                "stop_pct": variant.stop_pct,
                "target_pct": variant.target_pct,
            }
            return candidate
    raise RuntimeError(f"Could not reconstruct SHORT variant {variant.name}")


def protected_long_curve(all_days: list[date], model: str) -> pd.DataFrame:
    daily = pd.read_csv(PROTECTED_LONG_DAILY)
    leg = daily.loc[
        daily["model"].eq("FULL_HISTORY_MAX_PF") & daily["side"].eq("LONG")
    ].copy()
    if leg.empty:
        raise RuntimeError(f"Protected LONG leg missing in {PROTECTED_LONG_DAILY}")
    leg["day"] = pd.to_datetime(leg["day"]).dt.date
    leg = leg.set_index("day").reindex(all_days).reset_index(names="day")
    leg["model"] = model
    leg["side"] = "LONG"
    leg["signal_end"] = "09:25"
    leg["confirmation_end"] = "09:26"
    for column in ("selected_symbol", "selected_sid"):
        leg[column] = leg[column].fillna("")
    leg["status"] = leg["status"].fillna("NO_SIGNAL")
    for column in ("gross_profit_pct", "gross_loss_pct"):
        leg[column] = pd.to_numeric(leg[column], errors="coerce").fillna(0.0)
    leg["net_return_pct"] = pd.to_numeric(leg["net_return_pct"], errors="coerce")
    leg["selections"] = leg["status"].ne("NO_SIGNAL").astype(int)
    leg["fills"] = leg["status"].isin(["WIN", "LOSS", "FLAT"]).astype(int)
    leg["trade_details"] = [
        "" if not symbol else (
            f"{symbol}=NO_FILL" if pd.isna(net) else f"{symbol}={float(net):+.3f}%"
        )
        for symbol, net in zip(leg["selected_symbol"].astype(str), leg["net_return_pct"])
    ]
    return leg[
        [
            "model", "side", "day", "signal_end", "confirmation_end",
            "selected_symbol", "selected_sid", "trade_details", "status",
            "selections", "fills", "net_return_pct", "gross_profit_pct",
            "gross_loss_pct",
        ]
    ]


def short_curve(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    all_days: list[date],
    variant: ShortVariant,
    cost_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidate = reconstruct_short(signals, variant)
    net_all = sw.simulate_bracket(
        signals,
        paths,
        stop_pct=variant.stop_pct,
        target_pct=variant.target_pct,
        cost_bps=cost_bps,
    )
    daily = v2.daily_curve(signals, net_all, candidate, variant.name, all_days)
    audit = v2.trade_audit(signals, net_all, candidate, variant.name)
    return daily, audit


def combine_daily(long_leg: pd.DataFrame, short_leg: pd.DataFrame) -> pd.DataFrame:
    daily = pd.concat([long_leg, short_leg], ignore_index=True)
    return v2.portfolio_daily_curve(daily)


def period_stats(daily: pd.DataFrame, curve: pd.DataFrame, days: set[date]) -> dict[str, Any]:
    sample = curve.loc[curve["day"].isin(days)]
    legs = daily.loc[daily["day"].isin(days)]
    trade_profit = float(legs["gross_profit_pct"].sum())
    trade_loss = float(legs["gross_loss_pct"].sum())
    day_net = sample["portfolio_net_return_pct"].to_numpy(float)
    day_profit = float(day_net[day_net > 0].sum()) if day_net.size else 0.0
    day_loss = float(-day_net[day_net < 0].sum()) if day_net.size else 0.0
    return {
        "orders": int(sample["selections"].sum()),
        "fills": int(sample["fills"].sum()),
        "trade_pf": pf(trade_profit, trade_loss),
        "net_pct": float(day_net.sum()) if day_net.size else 0.0,
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
        "day_pf": pf(day_profit, day_loss),
    }


def side_stats(daily: pd.DataFrame) -> dict[str, Any]:
    profit = float(daily["gross_profit_pct"].sum())
    loss = float(daily["gross_loss_pct"].sum())
    return {
        "orders": int(daily["selections"].sum()),
        "fills": int(daily["fills"].sum()),
        "pf": pf(profit, loss),
        "net_pct": float(daily["net_return_pct"].fillna(0.0).sum()),
        "positive_days": int((daily["net_return_pct"].fillna(0.0) > 0).sum()),
        "negative_days": int((daily["net_return_pct"].fillna(0.0) < 0).sum()),
    }


def render_report(
    summaries: dict[str, dict[str, dict[str, Any]]],
    short_summaries: dict[str, dict[str, Any]],
    curves: dict[str, pd.DataFrame],
    split_day: date,
    cost_bps: float,
) -> str:
    lines = [
        "# FNO EMA/OI 09:25/09:26 V2 SHORT Expansion",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Protected files were not edited.",
        "- Baseline LONG is the protected one-per-side LONG from `ema_confirm_0925_force_daily_daily_pf.csv`.",
        "- Timing remains fixed: 5-minute signal ends 09:25; 1-minute confirmation ends 09:26; entry can fill only after that.",
        f"- Cost: {cost_bps:g} bps.",
        "",
        "## SHORT Variants",
        "",
        "| Variant | Max short/day | SHORT fills | SHORT PF | SHORT net % | Short +days | Short -days |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for variant in VARIANTS:
        stats = short_summaries[variant.name]
        lines.append(
            f"| {variant.name} | {variant.max_per_side} | {stats['fills']} | "
            f"{fmt(stats['pf'])} | {stats['net_pct']:+.3f} | "
            f"{stats['positive_days']} | {stats['negative_days']} |"
        )

    lines += [
        "",
        "## Combined Portfolio",
        "",
        "| Variant | Period | Orders | Fills | Trade PF | Day PF | Net % | Positive days | Negative days | Flat days |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for variant in VARIANTS:
        for period in ("TRAIN", "TEST", "ALL"):
            stats = summaries[variant.name][period]
            lines.append(
                f"| {variant.name} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | {stats['flat_days']} |"
            )

    lines += [
        "",
        "## Day-Wise Tables",
        "",
    ]
    columns = (
        "| Day | Period | L O/F | LONG % | S O/F | SHORT % | Total % | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    for variant in VARIANTS:
        lines += [f"### {variant.name}", "", columns, divider]
        for row in curves[variant.name].itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            lines.append(
                f"| {row.day} | {period} | {int(row.long_selections)}/{int(row.long_fills)} | "
                f"{fmt_signed(row.long_return_pct)} | {int(row.short_selections)}/{int(row.short_fills)} | "
                f"{fmt_signed(row.short_return_pct)} | {fmt_signed(row.portfolio_net_return_pct)} | "
                f"{fmt_signed(row.cumulative_net_pct)} | {fmt(row.cumulative_day_pf)} | "
                f"{fmt(row.cumulative_trade_pf)} |"
            )
        lines.append("")

    lines += [
        "## Files",
        "",
        f"- Daily CSV: `{DAILY_OUTPUT_PATH}`",
        f"- Selected trades audit: `{AUDIT_OUTPUT_PATH}`",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    split_day = pd.Timestamp(args.split_day).date()
    signals_all, paths = opt.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals_all = signals_all.copy()
    signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
    all_days = sorted(set(signals_all["day"]))
    signals = signals_all.loc[signals_all["hhmm_int"].eq(SIGNAL_SLOT)].copy()
    signals = signals.sort_values(["day", "tradingsymbol", "sid"]).reset_index(drop=True)
    if signals.empty:
        raise RuntimeError("No cached 09:25 signals found.")
    setup_signal_globals(signals, all_days)

    train_days = {day for day in all_days if day < split_day}
    test_days = set(all_days) - train_days
    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    short_summaries: dict[str, dict[str, Any]] = {}
    audits: list[pd.DataFrame] = []

    for variant in VARIANTS:
        long_leg = protected_long_curve(all_days, variant.name)
        short_leg, audit = short_curve(signals, paths, all_days, variant, args.cost_bps)
        daily = pd.concat([long_leg, short_leg], ignore_index=True)
        curves[variant.name] = combine_daily(long_leg, short_leg)
        summaries[variant.name] = {
            "TRAIN": period_stats(daily, curves[variant.name], train_days),
            "TEST": period_stats(daily, curves[variant.name], test_days),
            "ALL": period_stats(daily, curves[variant.name], set(all_days)),
        }
        short_summaries[variant.name] = side_stats(short_leg)
        audits.append(audit)

    combined = pd.concat(
        [curve.assign(variant=name) for name, curve in curves.items()],
        ignore_index=True,
    )
    common.atomic_write_csv(combined, DAILY_OUTPUT_PATH)
    if audits:
        common.atomic_write_csv(pd.concat(audits, ignore_index=True), AUDIT_OUTPUT_PATH)
    common.atomic_write_text(
        REPORT_PATH,
        render_report(summaries, short_summaries, curves, split_day, args.cost_bps),
    )

    for variant in VARIANTS:
        all_stats = summaries[variant.name]["ALL"]
        short_stats = short_summaries[variant.name]
        print(
            f"[{variant.name}] short fills={short_stats['fills']} "
            f"short PF={short_stats['pf']:.3f} combo PF={all_stats['trade_pf']:.3f} "
            f"day PF={all_stats['day_pf']:.3f} net={all_stats['net_pct']:+.3f}%"
        )
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
