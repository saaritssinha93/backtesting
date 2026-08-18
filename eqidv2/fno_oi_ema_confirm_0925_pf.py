"""Optimise the one-shot 09:25 FNO EMA/OI confirmation setup.

The timing is deliberately fixed:

* the 5-minute signal candle ends at 09:25;
* the confirming 1-minute candle runs 09:25-09:26 and is known at 09:26;
* a stop-entry order may fill only on a later 1-minute bar; and
* at most one contract is selected per side per session.

Kite stores 1-minute candles by start time. The shared signal cache therefore
stores the 09:25 row for the candle that becomes known at 09:26, and its saved
forward path begins with the next row.
"""

from __future__ import annotations

import argparse
import itertools
import math
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_optimize as opt
import fno_oi_ema_confirm_sweep as sw


SESSION = "fno_oi_ema_confirm_0925_pf"
SIGNAL_SLOT = 925
CONFIRMATION_END = 926
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_pf.md"
FORCE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_force_daily_pf.md"
DAYWISE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_daywise_detailed.md"
FORCE_DAYWISE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_force_daily_daywise_detailed.md"

GRID: dict[str, list[float]] = {
    "price_change_pct": [0.20, 0.30, 0.40, 0.50, 0.65, 0.80],
    "oi_change_pct": [0.10, 0.25, 0.40, 0.50, 0.75, 1.00],
    "volume_ratio": [1.0, 1.5, 2.0, 3.0],
    "body_ratio": [0.40, 0.50, 0.60],
    "max_wick_ratio": [0.30, 0.50],
    "min_traded_value": [0.0, 1e7],
}

PICKERS = ("max_oi", "max_volume", "max_move", "max_body", "max_liquidity")
STOP_PCTS = (0.30, 0.40, 0.50, 0.75, 1.00)
TARGET_PCTS = (0.50, 0.75, 1.00, 1.50, 2.00, 2.50, 3.00)


@dataclass(frozen=True)
class Guards:
    min_trades: int
    min_day_win: float
    max_top_profit_share: float


@dataclass
class Candidate:
    values: dict[str, Any]
    selected_idx: np.ndarray


def _pf(profit: float, loss: float) -> float:
    if loss > 0:
        return float(profit / loss)
    return float("inf") if profit > 0 else float("nan")


def score(net_all: np.ndarray, selected_idx: np.ndarray, period_days: set[date]) -> dict[str, Any]:
    selected = selected_idx[
        np.fromiter(
            (d in period_days for d in SLOT_DAYS[selected_idx]),
            dtype=bool,
            count=len(selected_idx),
        )
    ]
    selected_net = net_all[selected]
    filled = np.isfinite(selected_net)
    net = selected_net[filled]
    profit = float(net[net > 0].sum()) if net.size else 0.0
    loss = float(-net[net < 0].sum()) if net.size else 0.0

    robust_pf = float("nan")
    if net.size and (net > 0).any():
        without_best = np.delete(net, int(np.argmax(net)))
        robust_profit = float(without_best[without_best > 0].sum())
        robust_loss = float(-without_best[without_best < 0].sum())
        robust_pf = _pf(robust_profit, robust_loss)

    return {
        "signal_days": int(selected.size),
        "trades": int(net.size),
        "no_fills": int(selected.size - net.size),
        "wins": int((net > 0).sum()),
        "losses": int((net < 0).sum()),
        "win_rate": float((net > 0).mean()) if net.size else float("nan"),
        "gross_profit_pct": profit,
        "gross_loss_pct": loss,
        "pf": _pf(profit, loss),
        "net_pct": float(net.sum()) if net.size else 0.0,
        "mean_pct": float(net.mean()) if net.size else float("nan"),
        "robust_pf": robust_pf,
        "top_profit_share": float(net.max() / profit) if profit > 0 else float("nan"),
    }


def passes_guards(stats: dict[str, Any], guards: Guards) -> bool:
    return bool(
        stats["trades"] >= guards.min_trades
        and math.isfinite(stats["pf"])
        and math.isfinite(stats["robust_pf"])
        and stats["pf"] > 1.0
        and stats["robust_pf"] > 1.0
        and stats["win_rate"] >= guards.min_day_win
        and stats["top_profit_share"] <= guards.max_top_profit_share
    )


def picker_orders(signals: pd.DataFrame) -> dict[str, np.ndarray]:
    tval = signals["traded_value"].to_numpy(float)
    symbols = signals["tradingsymbol"].astype(str).to_numpy()
    values = {
        "max_oi": signals["oi_change_pct"].to_numpy(float),
        "max_volume": signals["volume_ratio"].to_numpy(float),
        "max_move": np.abs(signals["price_change_pct"].to_numpy(float)),
        "max_body": signals["body_ratio"].to_numpy(float),
        "max_liquidity": tval,
    }
    return {
        name: np.lexsort((symbols, -tval, -metric))
        for name, metric in values.items()
    }


def select_once_per_day(
    eligible: np.ndarray,
    order: np.ndarray,
    day_idx: np.ndarray,
) -> np.ndarray:
    """Pick the first eligible pre-ranked contract on each day.

    Ranking uses only information available with the confirmation candle. Fill
    status and later P&L never participate in the choice.
    """

    ranked = order[eligible[order]]
    if ranked.size == 0:
        return np.array([], dtype=int)
    _, first = np.unique(day_idx[ranked], return_index=True)
    selected = ranked[first]
    return selected[np.argsort(day_idx[selected], kind="stable")]


def build_force_daily_selections(signals: pd.DataFrame, side: str) -> list[Candidate]:
    eligible = signals["side"].eq(side).to_numpy()
    orders = picker_orders(signals)
    selections: list[Candidate] = []
    for picker in PICKERS:
        selected = select_once_per_day(eligible, orders[picker], SLOT_DAY_IDX)
        if selected.size:
            selections.append(
                Candidate(
                    {
                        "side": side,
                        "price_change_pct": 0.0,
                        "oi_change_pct": 0.0,
                        "volume_ratio": 0.0,
                        "body_ratio": 0.0,
                        "max_wick_ratio": 999.0,
                        "min_traded_value": 0.0,
                        "picker": picker,
                    },
                    selected,
                )
            )
    return selections


def build_selections(signals: pd.DataFrame, side: str, force_daily: bool = False) -> list[Candidate]:
    if force_daily:
        return build_force_daily_selections(signals, side)

    cols = {
        "side": signals["side"].eq(side).to_numpy(),
        "price": signals["price_change_pct"].to_numpy(float),
        "oi": signals["oi_change_pct"].to_numpy(float),
        "vol": signals["volume_ratio"].to_numpy(float),
        "body": signals["body_ratio"].to_numpy(float),
        "wick": signals["wick_ratio"].to_numpy(float),
        "tval": signals["traded_value"].to_numpy(float),
    }
    orders = picker_orders(signals)
    combos = itertools.product(*(GRID[key] for key in GRID))
    selections: list[Candidate] = []

    for price, oi, vol, body, wick, min_tval in combos:
        eligible = (
            cols["side"]
            & (cols["oi"] >= oi)
            & (cols["vol"] >= vol)
            & (cols["body"] >= body)
            & (cols["wick"] <= wick)
            & (cols["tval"] >= min_tval)
            & ((cols["price"] >= price) if side == "LONG" else (cols["price"] <= -price))
        )
        if not eligible.any():
            continue
        base = {
            "side": side,
            "price_change_pct": price,
            "oi_change_pct": oi,
            "volume_ratio": vol,
            "body_ratio": body,
            "max_wick_ratio": wick,
            "min_traded_value": min_tval,
        }
        for picker in PICKERS:
            selected = select_once_per_day(eligible, orders[picker], SLOT_DAY_IDX)
            if selected.size:
                selections.append(
                    Candidate({**base, "picker": picker}, selected)
                )
    return selections


def prefixed(prefix: str, values: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in values.items()}


def optimise_side(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    side: str,
    train_days: set[date],
    test_days: set[date],
    all_days: set[date],
    guards: Guards,
    cost_bps: float,
    force_daily: bool = False,
) -> tuple[pd.DataFrame, list[Candidate], dict[tuple[float, float], np.ndarray]]:
    selections = build_selections(signals, side, force_daily=force_daily)
    brackets = list(itertools.product(STOP_PCTS, TARGET_PCTS))
    net_cache: dict[tuple[float, float], np.ndarray] = {}
    survivors: list[Candidate] = []

    print(
        f"[{side}] {len(selections):,} once-daily filter/picker selections x "
        f"{len(brackets)} brackets",
        flush=True,
    )
    for bracket_no, (stop_pct, target_pct) in enumerate(brackets, start=1):
        net_all = sw.simulate_bracket(
            signals,
            paths,
            stop_pct=stop_pct,
            target_pct=target_pct,
            cost_bps=cost_bps,
        )
        net_cache[(stop_pct, target_pct)] = net_all
        for candidate in selections:
            train = score(net_all, candidate.selected_idx, train_days)
            if not passes_guards(train, guards):
                continue
            values = {
                **candidate.values,
                "stop_pct": stop_pct,
                "target_pct": target_pct,
                **prefixed("train", train),
                **prefixed("test", score(net_all, candidate.selected_idx, test_days)),
                **prefixed("all", score(net_all, candidate.selected_idx, all_days)),
            }
            survivors.append(Candidate(values, candidate.selected_idx))
        print(
            f"[{side}] bracket {bracket_no:02d}/{len(brackets)} "
            f"stop={stop_pct:g} target={target_pct:g} survivors={len(survivors):,}",
            flush=True,
        )

    if not survivors:
        return pd.DataFrame(), [], net_cache

    survivors.sort(
        key=lambda item: (
            item.values["train_robust_pf"],
            item.values["train_pf"],
            item.values["train_trades"],
            item.values["train_net_pct"],
        ),
        reverse=True,
    )
    for rank, item in enumerate(survivors, start=1):
        item.values["honest_rank"] = rank

    full_ranked = sorted(
        (
            item for item in survivors
            if item.values["all_trades"] >= guards.min_trades
            and math.isfinite(item.values["all_pf"])
        ),
        key=lambda item: (
            item.values["all_pf"],
            item.values["all_robust_pf"],
            item.values["all_trades"],
            item.values["all_net_pct"],
        ),
        reverse=True,
    )
    for rank, item in enumerate(full_ranked, start=1):
        item.values["full_history_rank"] = rank

    for item in survivors:
        item.values.setdefault("full_history_rank", np.nan)

    frame = pd.DataFrame([item.values for item in survivors])
    return frame, survivors, net_cache


def best_candidates(candidates: list[Candidate]) -> list[tuple[str, Candidate]]:
    if not candidates:
        return []
    picks = [("TRAIN_ROBUST", candidates[0])]
    full = [c for c in candidates if c.values.get("full_history_rank") == 1]
    if full and full[0] is not candidates[0]:
        picks.append(("FULL_HISTORY_MAX_PF", full[0]))
    return picks


def daily_curve(
    signals: pd.DataFrame,
    net_all: np.ndarray,
    candidate: Candidate,
    model: str,
    calendar_days: Iterable[date],
) -> pd.DataFrame:
    selected_by_day = {
        SLOT_DAYS[idx]: idx for idx in candidate.selected_idx
    }
    rows: list[dict[str, Any]] = []
    cum_profit = 0.0
    cum_loss = 0.0

    for day_no, day in enumerate(calendar_days):
        idx = selected_by_day.get(day)
        selected = idx is not None
        value = float(net_all[idx]) if selected and np.isfinite(net_all[idx]) else float("nan")
        profit = max(value, 0.0) if math.isfinite(value) else 0.0
        loss = max(-value, 0.0) if math.isfinite(value) else 0.0
        cum_profit += profit
        cum_loss += loss
        day_pf = _pf(profit, loss)
        cum_pf = _pf(cum_profit, cum_loss)
        if not selected:
            status = "NO_SIGNAL"
        elif not math.isfinite(value):
            status = "NO_FILL"
        elif value > 0:
            status = "WIN"
        elif value < 0:
            status = "LOSS"
        else:
            status = "FLAT"
        row = {
            "model": model,
            "side": candidate.values["side"],
            "day": day,
            "signal_end": "09:25",
            "confirmation_end": "09:26",
            "selected_symbol": signals.iloc[idx]["tradingsymbol"] if selected else "",
            "selected_sid": int(signals.iloc[idx]["sid"]) if selected else np.nan,
            "status": status,
            "net_return_pct": value if math.isfinite(value) else np.nan,
            "gross_profit_pct": profit,
            "gross_loss_pct": loss,
            "day_pf": day_pf,
            "day_pf_label": "INF" if math.isinf(day_pf) else (f"{day_pf:.3f}" if math.isfinite(day_pf) else ""),
            "cumulative_profit_pct": cum_profit,
            "cumulative_loss_pct": cum_loss,
            "cumulative_net_pct": cum_profit - cum_loss,
            "cumulative_pf": cum_pf,
            "cumulative_pf_label": "INF" if math.isinf(cum_pf) else (f"{cum_pf:.3f}" if math.isfinite(cum_pf) else ""),
            "picker": candidate.values["picker"],
            "stop_pct": candidate.values["stop_pct"],
            "target_pct": candidate.values["target_pct"],
        }
        rows.append(row)
    return pd.DataFrame(rows)


def portfolio_daily_curve(daily: pd.DataFrame) -> pd.DataFrame:
    """Combine the selected LONG and SHORT legs into one chronological curve."""

    rows: list[dict[str, Any]] = []
    for model, model_rows in daily.groupby("model", sort=False):
        cum_day_profit = 0.0
        cum_day_loss = 0.0
        cum_trade_profit = 0.0
        cum_trade_loss = 0.0
        for day, day_rows in model_rows.groupby("day", sort=True):
            by_side = {row.side: row for row in day_rows.itertuples(index=False)}
            long_row = by_side.get("LONG")
            short_row = by_side.get("SHORT")
            leg_rows = [row for row in (long_row, short_row) if row is not None]
            leg_returns = [
                float(row.net_return_pct)
                for row in leg_rows
                if pd.notna(row.net_return_pct)
            ]
            day_net = float(sum(leg_returns))
            day_profit = max(day_net, 0.0)
            day_loss = max(-day_net, 0.0)
            trade_profit = float(sum(float(row.gross_profit_pct) for row in leg_rows))
            trade_loss = float(sum(float(row.gross_loss_pct) for row in leg_rows))
            cum_day_profit += day_profit
            cum_day_loss += day_loss
            cum_trade_profit += trade_profit
            cum_trade_loss += trade_loss
            day_pf = _pf(day_profit, day_loss)
            cum_day_pf = _pf(cum_day_profit, cum_day_loss)
            cum_trade_pf = _pf(cum_trade_profit, cum_trade_loss)
            fills = sum(row.status in {"WIN", "LOSS", "FLAT"} for row in leg_rows)
            selections = sum(row.status != "NO_SIGNAL" for row in leg_rows)
            rows.append(
                {
                    "model": model,
                    "day": day,
                    "signal_end": "09:25",
                    "confirmation_end": "09:26",
                    "long_symbol": long_row.selected_symbol if long_row is not None else "",
                    "long_status": long_row.status if long_row is not None else "NO_SIGNAL",
                    "long_return_pct": long_row.net_return_pct if long_row is not None else np.nan,
                    "short_symbol": short_row.selected_symbol if short_row is not None else "",
                    "short_status": short_row.status if short_row is not None else "NO_SIGNAL",
                    "short_return_pct": short_row.net_return_pct if short_row is not None else np.nan,
                    "selections": selections,
                    "fills": fills,
                    "portfolio_net_return_pct": day_net,
                    "day_pf": day_pf,
                    "day_pf_label": "INF" if math.isinf(day_pf) else (
                        f"{day_pf:.3f}" if math.isfinite(day_pf) else ""
                    ),
                    "cumulative_net_pct": cum_day_profit - cum_day_loss,
                    "cumulative_day_pf": cum_day_pf,
                    "cumulative_day_pf_label": "INF" if math.isinf(cum_day_pf) else (
                        f"{cum_day_pf:.3f}" if math.isfinite(cum_day_pf) else ""
                    ),
                    "cumulative_trade_pf": cum_trade_pf,
                    "cumulative_trade_pf_label": "INF" if math.isinf(cum_trade_pf) else (
                        f"{cum_trade_pf:.3f}" if math.isfinite(cum_trade_pf) else ""
                    ),
                }
            )
    return pd.DataFrame(rows)


def portfolio_period_stats(
    daily: pd.DataFrame,
    portfolio_daily: pd.DataFrame,
    period_days: set[date],
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for model in daily["model"].drop_duplicates():
        legs = daily.loc[daily["model"].eq(model) & daily["day"].isin(period_days)]
        days = portfolio_daily.loc[
            portfolio_daily["model"].eq(model) & portfolio_daily["day"].isin(period_days)
        ]
        filled = legs[legs["status"].isin(["WIN", "LOSS", "FLAT"])]
        trade_profit = float(filled["gross_profit_pct"].sum())
        trade_loss = float(filled["gross_loss_pct"].sum())
        day_net = days["portfolio_net_return_pct"].to_numpy(float)
        day_profit = float(day_net[day_net > 0].sum())
        day_loss = float(-day_net[day_net < 0].sum())
        results[str(model)] = {
            "orders": int(legs["status"].ne("NO_SIGNAL").sum()),
            "fills": int(len(filled)),
            "trade_pf": _pf(trade_profit, trade_loss),
            "net_pct": float(filled["net_return_pct"].sum()),
            "positive_days": int((day_net > 0).sum()),
            "negative_days": int((day_net < 0).sum()),
            "flat_days": int((day_net == 0).sum()),
            "day_pf": _pf(day_profit, day_loss),
        }
    return results


def fmt_num(value: Any, digits: int = 3) -> str:
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
    formatted = fmt_num(value, digits)
    if not formatted:
        return ""
    return formatted if formatted == "INF" else f"{float(value):+.{digits}f}"


def fmt_symbol(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def render_daywise_report(
    portfolio_daily: pd.DataFrame,
    portfolio: dict[str, dict[str, dict[str, Any]]],
    meta: dict[str, Any],
) -> str:
    mode = "Force-Daily" if meta["force_daily"] else "Filtered One-Shot"
    table_rule = (
        "one LONG and one SHORT are selected per session whenever that side has a confirmed candidate"
        if meta["force_daily"]
        else "at most one filtered LONG and one filtered SHORT are selected per session"
    )
    lines = [
        f"# FNO EMA/OI 09:25 {mode} Day-Wise Detail",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Timing: 5-minute signal candle ends 09:25; 1-minute confirmation ends 09:26; entry order activates afterward.",
        f"- Table rule: {table_rule}.",
        "- `NO_SIGNAL` means no confirmed 09:25 setup for that side; `NO_FILL` means the stop-entry order did not trigger later.",
        f"- Train: {meta['train_from']} to {meta['train_to']}; test: {meta['test_from']} to {meta['test_to']}.",
        "",
        "## Portfolio Summary",
        "",
        "| Selection | Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model in ("TRAIN_ROBUST", "FULL_HISTORY_MAX_PF"):
        for period in ("TRAIN", "TEST", "ALL"):
            stats = portfolio.get(period, {}).get(model)
            if stats is None:
                continue
            lines.append(
                f"| {model} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt_num(stats['trade_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | {stats['flat_days']} | "
                f"{fmt_num(stats['day_pf'])} |"
            )

    columns = (
        "| Date | Period | Long symbol | Long status | Long % | Short symbol | Short status | Short % | "
        "Selections | Fills | Day net % | Day PF | Cum net % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    test_from = meta["test_from"]
    for model in ("TRAIN_ROBUST", "FULL_HISTORY_MAX_PF"):
        model_rows = portfolio_daily.loc[portfolio_daily["model"].eq(model)].sort_values("day")
        if model_rows.empty:
            continue
        lines += ["", f"## {model} Day-Wise Table", "", columns, divider]
        for row in model_rows.itertuples(index=False):
            period = "TEST" if row.day >= test_from else "TRAIN"
            lines.append(
                f"| {row.day} | {period} | {fmt_symbol(row.long_symbol)} | {row.long_status} | "
                f"{fmt_signed(row.long_return_pct)} | {fmt_symbol(row.short_symbol)} | {row.short_status} | "
                f"{fmt_signed(row.short_return_pct)} | {int(row.selections)} | {int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {row.day_pf_label} | "
                f"{fmt_signed(row.cumulative_net_pct)} | {row.cumulative_day_pf_label} | "
                f"{row.cumulative_trade_pf_label} |"
            )

    lines.append("")
    return "\n".join(lines)


def render_report(
    ranked: dict[str, pd.DataFrame],
    selected: dict[str, list[tuple[str, Candidate]]],
    portfolio: dict[str, dict[str, dict[str, Any]]],
    meta: dict[str, Any],
) -> str:
    frequency = (
        "one selected contract per side per session whenever a confirmed candidate exists"
        if meta["force_daily"]
        else "at most one selected contract per side per session"
    )
    lines = [
        "# FNO EMA/OI 09:25 One-Shot PF Optimisation",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Timing: 5-minute candle ends 09:25; 1-minute confirmation ends 09:26; entry order activates afterward.",
        f"- Frequency: {frequency}.",
        f"- Data: {meta['slot_signals']} confirmed 09:25 candidates across {meta['slot_days']} sessions "
        f"({meta['all_days']} total cached sessions).",
        f"- Train: {meta['train_from']} to {meta['train_to']} ({meta['train_days']} sessions); "
        f"test: {meta['test_from']} to {meta['test_to']} ({meta['test_days']} sessions).",
        f"- Cost: {meta['cost_bps']} bps round trip.",
        f"- Force daily: {'yes' if meta['force_daily'] else 'no'}.",
        f"- Train guards: >= {meta['min_trades']} filled trades, win rate >= {meta['min_day_win']:.0%}, "
        f"best trade <= {meta['max_top_profit_share']:.0%} of gross profit, PF after removing best day > 1.",
        "",
        "Per-day PF is mechanically INF on a winning one-trade day and 0 on a losing one-trade day. "
        "The chronological cumulative PF column is the useful series.",
        "",
        "## Best configurations",
        "",
        "| Side | Selection | Train robust PF | Train PF | Test PF | All PF | All trades | All net % | "
        "Win rate | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for side in ("LONG", "SHORT"):
        for model, candidate in selected[side]:
            r = candidate.values
            lines.append(
                f"| {side} | {model} | {fmt_num(r['train_robust_pf'])} | {fmt_num(r['train_pf'])} | "
                f"{fmt_num(r['test_pf'])} | {fmt_num(r['all_pf'])} | {int(r['all_trades'])} | "
                f"{r['all_net_pct']:+.3f} | {r['all_win_rate']:.1%} | {r['picker']} | "
                f"{r['price_change_pct']} | {r['oi_change_pct']} | {r['volume_ratio']} | "
                f"{r['body_ratio']} | {r['max_wick_ratio']} | {r['stop_pct']} | {r['target_pct']} |"
            )
        if ranked[side].empty:
            lines.append(f"| {side} | No candidate survived train guards | | | | | | | | | | | | | | | |")
    lines += [
        "",
        "`TRAIN_ROBUST` is selected without using the test window. `FULL_HISTORY_MAX_PF` is descriptive and "
        "in-sample; it should not be treated as validated.",
        "",
        "## Combined LONG + SHORT portfolio",
        "",
        "Trade PF uses each filled leg separately. Day PF first nets the LONG and SHORT returns within each session.",
        "",
        "| Selection | Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model in ("TRAIN_ROBUST", "FULL_HISTORY_MAX_PF"):
        for period in ("TRAIN", "TEST", "ALL"):
            stats = portfolio.get(period, {}).get(model)
            if stats is None:
                continue
            lines.append(
                f"| {model} | {period} | {stats['orders']} | {stats['fills']} | "
                f"{fmt_num(stats['trade_pf'])} | {stats['net_pct']:+.3f} | "
                f"{stats['positive_days']} | {stats['negative_days']} | {stats['flat_days']} | "
                f"{fmt_num(stats['day_pf'])} |"
            )
    lines += [
        "",
        "## Files",
        "",
        "- Ranked LONG/SHORT CSV files contain train, test, and full-history metrics.",
        "- Daily PF CSV contains every cached session for each selected model, including no-signal and no-fill days.",
        "- Cumulative PF CSV is the compact chronological curve for charting.",
        "- Portfolio daily PF CSV combines the LONG and SHORT legs and includes cumulative trade PF and day PF.",
        "- Day-wise detailed Markdown expands the combined portfolio CSV into an audit-friendly table.",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17", help="First out-of-sample session.")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=15)
    parser.add_argument("--min-day-win", type=float, default=0.40)
    parser.add_argument("--max-top-profit-share", type=float, default=0.45)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--force-daily",
        action="store_true",
        help="Pick the best-ranked confirmed candidate for each side/day without threshold filters.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    global SLOT_DAYS, SLOT_DAY_IDX, ALL_DAY_VALUES

    args = parse_args(argv)
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)
    session_name = f"{SESSION}_force_daily" if args.force_daily else SESSION
    report_path = FORCE_REPORT_PATH if args.force_daily else REPORT_PATH
    daywise_report_path = (
        FORCE_DAYWISE_REPORT_PATH if args.force_daily else DAYWISE_REPORT_PATH
    )
    file_prefix = "ema_confirm_0925_force_daily" if args.force_daily else "ema_confirm_0925_once"
    common.publish_status(session_name, "RUNNING", signal_slot="0925", confirmation_end="0926")

    try:
        signals_all, paths = opt.load_signals(
            args.square_off, args.max_forward_bars, args.rebuild_cache
        )
        signals_all = signals_all.copy()
        signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
        ALL_DAY_VALUES = sorted(set(signals_all["day"]))
        signals = signals_all.loc[signals_all["hhmm_int"].eq(SIGNAL_SLOT)].copy()
        signals = signals.sort_values(["day", "tradingsymbol", "sid"]).reset_index(drop=True)
        if signals.empty:
            raise RuntimeError("No cached signals exist for the 09:25 slot.")
        if not signals["hhmm_int"].eq(SIGNAL_SLOT).all():
            raise AssertionError("Non-09:25 signal leaked into the one-shot optimiser.")

        SLOT_DAYS = signals["day"].to_numpy()
        all_day_code = {day: idx for idx, day in enumerate(ALL_DAY_VALUES)}
        SLOT_DAY_IDX = np.array([all_day_code[day] for day in SLOT_DAYS], dtype=int)

        split = pd.Timestamp(args.split_day).date()
        train_days = {day for day in ALL_DAY_VALUES if day < split}
        test_days = {day for day in ALL_DAY_VALUES if day >= split}
        all_days = set(ALL_DAY_VALUES)
        guards = Guards(
            min_trades=args.min_trades,
            min_day_win=args.min_day_win,
            max_top_profit_share=args.max_top_profit_share,
        )
        print(
            f"[DATA] {len(signals):,} confirmed candidates at 09:25 across "
            f"{signals['day'].nunique()} sessions | train {len(train_days)} | test {len(test_days)}",
            flush=True,
        )

        ranked: dict[str, pd.DataFrame] = {}
        candidates: dict[str, list[Candidate]] = {}
        net_caches: dict[str, dict[tuple[float, float], np.ndarray]] = {}
        for side in ("LONG", "SHORT"):
            frame, side_candidates, net_cache = optimise_side(
                signals,
                paths,
                side,
                train_days,
                test_days,
                all_days,
                guards,
                args.cost_bps,
                args.force_daily,
            )
            ranked[side] = frame
            candidates[side] = side_candidates
            net_caches[side] = net_cache
            if not frame.empty:
                keep_ids = set(frame.head(args.top_n).index)
                full_ids = set(frame.nsmallest(args.top_n, "full_history_rank").index)
                output = frame.loc[sorted(keep_ids | full_ids)].copy()
                common.atomic_write_csv(
                    output,
                    RESULT_DIR / f"{file_prefix}_ranked_{side}.csv",
                )

        selected = {side: best_candidates(candidates[side]) for side in ("LONG", "SHORT")}
        daily_parts = []
        for side in ("LONG", "SHORT"):
            for model, candidate in selected[side]:
                bracket = (candidate.values["stop_pct"], candidate.values["target_pct"])
                daily_parts.append(
                    daily_curve(
                        signals,
                        net_caches[side][bracket],
                        candidate,
                        model,
                        ALL_DAY_VALUES,
                    )
                )
        daily = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame()
        portfolio_daily = pd.DataFrame()
        if not daily.empty:
            common.atomic_write_csv(daily, RESULT_DIR / f"{file_prefix}_daily_pf.csv")
            cumulative = daily[
                [
                    "model", "side", "day", "status", "net_return_pct",
                    "cumulative_net_pct", "cumulative_profit_pct",
                    "cumulative_loss_pct", "cumulative_pf", "cumulative_pf_label",
                ]
            ].copy()
            common.atomic_write_csv(
                cumulative, RESULT_DIR / f"{file_prefix}_cumulative_pf.csv"
            )
            portfolio_daily = portfolio_daily_curve(daily)
            common.atomic_write_csv(
                portfolio_daily, RESULT_DIR / f"{file_prefix}_portfolio_daily_pf.csv"
            )

        portfolio = {
            "TRAIN": portfolio_period_stats(daily, portfolio_daily, train_days),
            "TEST": portfolio_period_stats(daily, portfolio_daily, test_days),
            "ALL": portfolio_period_stats(daily, portfolio_daily, all_days),
        } if not daily.empty else {}

        meta = {
            "slot_signals": len(signals),
            "slot_days": signals["day"].nunique(),
            "all_days": len(ALL_DAY_VALUES),
            "train_from": min(train_days) if train_days else None,
            "train_to": max(train_days) if train_days else None,
            "train_days": len(train_days),
            "test_from": min(test_days) if test_days else None,
            "test_to": max(test_days) if test_days else None,
            "test_days": len(test_days),
            "cost_bps": args.cost_bps,
            "force_daily": args.force_daily,
            "min_trades": args.min_trades,
            "min_day_win": args.min_day_win,
            "max_top_profit_share": args.max_top_profit_share,
        }
        common.atomic_write_text(report_path, render_report(ranked, selected, portfolio, meta))
        if not portfolio_daily.empty:
            common.atomic_write_text(
                daywise_report_path,
                render_daywise_report(portfolio_daily, portfolio, meta),
            )
        elapsed = time.monotonic() - started
        common.publish_status(
            session_name,
            "SUCCESS",
            signal_slot="0925",
            confirmation_end="0926",
            elapsed_sec=round(elapsed, 1),
            report=str(report_path),
        )
        print(f"[DONE] {report_path} ({elapsed:.1f}s)", flush=True)
        return 0
    except Exception as exc:
        common.publish_status(session_name, "FAILED", error=str(exc))
        raise


SLOT_DAYS = np.array([], dtype=object)
SLOT_DAY_IDX = np.array([], dtype=int)
ALL_DAY_VALUES: list[date] = []


if __name__ == "__main__":
    raise SystemExit(main())
