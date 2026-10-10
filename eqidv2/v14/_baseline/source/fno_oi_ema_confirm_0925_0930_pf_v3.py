"""Optimise the V3 FNO EMA/OI confirmation setup for selected morning scans.

The available timing windows are deliberately fixed:

* 5-minute signal candles end at 09:25 and 09:30;
* confirming 1-minute candles end at 09:26 and 09:31 respectively;
* each scan is evaluated exactly once when its confirmation is known;
* a stop-entry order may fill only on a later 1-minute bar; and
* between one and five contracts may be selected per side and scan.

Kite stores 1-minute candles by start time. The shared signal cache therefore
stores the 09:25 and 09:30 rows for candles known at 09:26 and 09:31, and each
saved forward path begins with the next row.
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


SESSION = "fno_oi_ema_confirm_0925_0930_pf_v3"
SIGNAL_SLOTS = (925, 930)
CONFIRMATION_ENDS = {925: 926, 930: 931}
SIGNAL_END_LABEL = "09:25,09:30"
CONFIRMATION_END_LABEL = "09:26,09:31"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_0930_v3_pf.md"
FORCE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_0930_v3_force_daily_pf.md"
DAYWISE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_0930_v3_daywise_detailed.md"
FORCE_DAYWISE_REPORT_PATH = common.LATEST_DIR / "latest_fno_oi_ema_confirm_0925_0930_v3_force_daily_daywise_detailed.md"
MAX_PER_SIDE_LIMIT = 5
MODEL_ORDER = (
    "TRAIN_ROBUST",
    "FULL_HISTORY_MAX_PF",
    "FULL_HISTORY_MAX_DAY_PF",
)


def format_hhmm(value: int) -> str:
    return f"{int(value) // 100:02d}:{int(value) % 100:02d}"

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


def score(
    net_all: np.ndarray,
    selected_idx: np.ndarray,
    period_mask: np.ndarray,
) -> dict[str, Any]:
    selected = selected_idx[period_mask[selected_idx]]
    selected_net = net_all[selected]
    filled = np.isfinite(selected_net)
    net = selected_net[filled]
    profit = float(net[net > 0].sum()) if net.size else 0.0
    loss = float(-net[net < 0].sum()) if net.size else 0.0

    day_net = np.array([], dtype=float)
    if selected.size:
        _, selected_inverse = np.unique(SLOT_DAY_IDX[selected], return_inverse=True)
        day_net = np.zeros(int(selected_inverse.max()) + 1, dtype=float)
        np.add.at(day_net, selected_inverse[filled], net)
    day_profit = float(day_net[day_net > 0].sum()) if day_net.size else 0.0
    day_loss = float(-day_net[day_net < 0].sum()) if day_net.size else 0.0

    robust_pf = float("nan")
    if net.size and (net > 0).any():
        without_best = np.delete(net, int(np.argmax(net)))
        robust_profit = float(without_best[without_best > 0].sum())
        robust_loss = float(-without_best[without_best < 0].sum())
        robust_pf = _pf(robust_profit, robust_loss)

    return {
        "signal_days": int(np.unique(SLOT_DAY_IDX[selected]).size),
        "orders": int(selected.size),
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
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
        "day_win_rate": float((day_net > 0).mean()) if day_net.size else float("nan"),
        "day_pf": _pf(day_profit, day_loss),
    }


def passes_guards(stats: dict[str, Any], guards: Guards) -> bool:
    return bool(
        stats["trades"] >= guards.min_trades
        and math.isfinite(stats["pf"])
        and math.isfinite(stats["robust_pf"])
        and stats["pf"] > 1.0
        and stats["robust_pf"] > 1.0
        and stats["day_win_rate"] >= guards.min_day_win
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


def select_up_to_per_day(
    eligible: np.ndarray,
    order: np.ndarray,
    day_idx: np.ndarray,
    max_per_day: int,
) -> list[np.ndarray]:
    """Return top-1 through top-N eligible contracts for every day.

    Ranking uses only information available with the confirmation candle. Fill
    status and later P&L never participate in the choice.
    """

    ranked = order[eligible[order]]
    if ranked.size == 0:
        return [np.array([], dtype=int) for _ in range(max_per_day)]

    # Group by session while preserving each picker's rank within the session.
    ranked_days = day_idx[ranked]
    grouped_order = np.argsort(ranked_days, kind="stable")
    grouped = ranked[grouped_order]
    grouped_days = ranked_days[grouped_order]
    starts = np.r_[0, np.flatnonzero(grouped_days[1:] != grouped_days[:-1]) + 1]
    lengths = np.diff(np.r_[starts, grouped.size])
    rank_within_day = np.arange(grouped.size) - np.repeat(starts, lengths)
    return [grouped[rank_within_day < count] for count in range(1, max_per_day + 1)]


def build_force_daily_selections(
    signals: pd.DataFrame,
    side: str,
    max_per_side: int,
) -> list[Candidate]:
    eligible = signals["side"].eq(side).to_numpy()
    orders = picker_orders(signals)
    selections: list[Candidate] = []
    for picker in PICKERS:
        ranked_counts = select_up_to_per_day(
            eligible, orders[picker], SLOT_SCAN_IDX, max_per_side
        )
        for selection_count, selected in enumerate(ranked_counts, start=1):
            if not selected.size:
                continue
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
                        "max_per_side": selection_count,
                    },
                    selected,
                )
            )
    return selections


def build_selections(
    signals: pd.DataFrame,
    side: str,
    max_per_side: int,
    force_daily: bool = False,
) -> list[Candidate]:
    if force_daily:
        return build_force_daily_selections(signals, side, max_per_side)

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
            ranked_counts = select_up_to_per_day(
                eligible, orders[picker], SLOT_SCAN_IDX, max_per_side
            )
            for selection_count, selected in enumerate(ranked_counts, start=1):
                if not selected.size:
                    continue
                selections.append(
                    Candidate(
                        {
                            **base,
                            "picker": picker,
                            "max_per_side": selection_count,
                        },
                        selected,
                    )
                )
    return selections


def prefixed(prefix: str, values: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in values.items()}


def honest_key(item: Candidate) -> tuple[float, float, int, float]:
    return (
        float(item.values["train_robust_pf"]),
        float(item.values["train_pf"]),
        int(item.values["train_trades"]),
        float(item.values["train_net_pct"]),
    )


def full_trade_key(item: Candidate) -> tuple[float, float, int, float]:
    return (
        float(item.values["all_pf"]),
        float(item.values["all_robust_pf"]),
        int(item.values["all_trades"]),
        float(item.values["all_net_pct"]),
    )


def full_day_key(item: Candidate) -> tuple[float, float, int, float]:
    return (
        float(item.values["all_day_pf"]),
        float(item.values["all_pf"]),
        int(item.values["all_trades"]),
        float(item.values["all_net_pct"]),
    )


def prune_candidates(
    candidates: list[Candidate],
    guards: Guards,
    retain_n: int,
) -> list[Candidate]:
    """Keep the exact leaders needed for all three output rankings."""

    honest = sorted(candidates, key=honest_key, reverse=True)[:retain_n]
    full_trade = sorted(
        (
            item for item in candidates
            if item.values["all_trades"] >= guards.min_trades
            and math.isfinite(item.values["all_pf"])
        ),
        key=full_trade_key,
        reverse=True,
    )[:retain_n]
    full_day = sorted(
        (
            item for item in candidates
            if item.values["all_trades"] >= guards.min_trades
            and math.isfinite(item.values["all_day_pf"])
        ),
        key=full_day_key,
        reverse=True,
    )[:retain_n]
    kept: dict[int, Candidate] = {}
    for item in honest + full_trade + full_day:
        kept[id(item)] = item
    return list(kept.values())


def optimise_side(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    side: str,
    train_days: set[date],
    test_days: set[date],
    all_days: set[date],
    guards: Guards,
    cost_bps: float,
    max_per_side: int,
    retain_n: int,
    force_daily: bool = False,
) -> tuple[pd.DataFrame, list[Candidate], dict[tuple[float, float], np.ndarray]]:
    selections = build_selections(
        signals, side, max_per_side=max_per_side, force_daily=force_daily
    )
    train_mask = np.fromiter(
        (day in train_days for day in SLOT_DAYS), dtype=bool, count=len(SLOT_DAYS)
    )
    test_mask = np.fromiter(
        (day in test_days for day in SLOT_DAYS), dtype=bool, count=len(SLOT_DAYS)
    )
    all_mask = np.ones(len(SLOT_DAYS), dtype=bool)
    selections = [
        candidate
        for candidate in selections
        if int(train_mask[candidate.selected_idx].sum()) >= guards.min_trades
    ]
    brackets = list(itertools.product(STOP_PCTS, TARGET_PCTS))
    net_cache: dict[tuple[float, float], np.ndarray] = {}
    survivors: list[Candidate] = []
    survivor_count = 0

    print(
        f"[{side}] {len(selections):,} top-1..{max_per_side} filter/picker selections x "
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
            train = score(net_all, candidate.selected_idx, train_mask)
            if not passes_guards(train, guards):
                continue
            values = {
                **candidate.values,
                "stop_pct": stop_pct,
                "target_pct": target_pct,
                **prefixed("train", train),
                **prefixed("test", score(net_all, candidate.selected_idx, test_mask)),
                **prefixed("all", score(net_all, candidate.selected_idx, all_mask)),
            }
            survivors.append(Candidate(values, candidate.selected_idx))
            survivor_count += 1
        survivors = prune_candidates(survivors, guards, retain_n)
        print(
            f"[{side}] bracket {bracket_no:02d}/{len(brackets)} "
            f"stop={stop_pct:g} target={target_pct:g} "
            f"survivors={survivor_count:,} retained={len(survivors):,}",
            flush=True,
        )

    if not survivors:
        return pd.DataFrame(), [], net_cache

    survivors.sort(key=honest_key, reverse=True)
    for rank, item in enumerate(survivors[:retain_n], start=1):
        item.values["honest_rank"] = rank

    full_ranked = sorted(
        (
            item for item in survivors
            if item.values["all_trades"] >= guards.min_trades
            and math.isfinite(item.values["all_pf"])
        ),
        key=full_trade_key,
        reverse=True,
    )[:retain_n]
    for rank, item in enumerate(full_ranked, start=1):
        item.values["full_history_rank"] = rank

    full_day_ranked = sorted(
        (
            item for item in survivors
            if item.values["all_trades"] >= guards.min_trades
            and math.isfinite(item.values["all_day_pf"])
        ),
        key=full_day_key,
        reverse=True,
    )[:retain_n]
    for rank, item in enumerate(full_day_ranked, start=1):
        item.values["full_history_day_rank"] = rank

    for item in survivors:
        item.values.setdefault("honest_rank", np.nan)
        item.values.setdefault("full_history_rank", np.nan)
        item.values.setdefault("full_history_day_rank", np.nan)

    frame = pd.DataFrame([item.values for item in survivors])
    return frame, survivors, net_cache


def best_candidates(candidates: list[Candidate]) -> list[tuple[str, Candidate]]:
    if not candidates:
        return []
    picks = [("TRAIN_ROBUST", candidates[0])]
    full = [c for c in candidates if c.values.get("full_history_rank") == 1]
    if full:
        picks.append(("FULL_HISTORY_MAX_PF", full[0]))
    full_day = [c for c in candidates if c.values.get("full_history_day_rank") == 1]
    if full_day:
        picks.append(("FULL_HISTORY_MAX_DAY_PF", full_day[0]))
    return picks


def daily_curve(
    signals: pd.DataFrame,
    net_all: np.ndarray,
    candidate: Candidate,
    model: str,
    calendar_days: Iterable[date],
) -> pd.DataFrame:
    selected_by_day: dict[date, list[int]] = {}
    for idx in candidate.selected_idx:
        selected_by_day.setdefault(SLOT_DAYS[idx], []).append(int(idx))
    rows: list[dict[str, Any]] = []
    cum_profit = 0.0
    cum_loss = 0.0

    for day in calendar_days:
        indices = np.asarray(selected_by_day.get(day, []), dtype=int)
        selections = int(indices.size)
        values = net_all[indices] if selections else np.array([], dtype=float)
        filled_mask = np.isfinite(values)
        filled_values = values[filled_mask]
        fills = int(filled_values.size)
        no_fills = selections - fills
        wins = int((filled_values > 0).sum())
        losses = int((filled_values < 0).sum())
        flats = int((filled_values == 0).sum())
        value = float(filled_values.sum()) if fills else float("nan")
        profit = float(filled_values[filled_values > 0].sum()) if fills else 0.0
        loss = float(-filled_values[filled_values < 0].sum()) if fills else 0.0
        cum_profit += profit
        cum_loss += loss
        day_pf = _pf(profit, loss)
        cum_pf = _pf(cum_profit, cum_loss)

        selected_rows = signals.iloc[indices] if selections else signals.iloc[[]]
        symbols = selected_rows["tradingsymbol"].astype(str).tolist()
        sids = selected_rows["sid"].astype(int).astype(str).tolist()
        slots = selected_rows["hhmm_int"].astype(int).tolist()
        details = []
        for symbol, slot, result in zip(symbols, slots, values):
            result_text = "NO_FILL" if not math.isfinite(float(result)) else f"{float(result):+.3f}%"
            confirmation = format_hhmm(CONFIRMATION_ENDS[slot])
            details.append(f"[{confirmation}] {symbol}={result_text}")

        if not selections:
            status = "NO_SIGNAL"
        elif not fills:
            status = "NO_FILL"
        elif selections == 1 and no_fills == 0 and value > 0:
            status = "WIN"
        elif selections == 1 and no_fills == 0 and value < 0:
            status = "LOSS"
        elif selections == 1 and no_fills == 0:
            status = "FLAT"
        else:
            status = f"{wins}W/{losses}L/{flats}F/{no_fills}NF"
        row = {
            "model": model,
            "side": candidate.values["side"],
            "day": day,
            "signal_end": SIGNAL_END_LABEL,
            "confirmation_end": CONFIRMATION_END_LABEL,
            "selected_symbol": "; ".join(symbols),
            "selected_sid": "; ".join(sids),
            "trade_details": "; ".join(details),
            "status": status,
            "selections": selections,
            "fills": fills,
            "wins": wins,
            "losses": losses,
            "flats": flats,
            "no_fills": no_fills,
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
            "max_per_side": candidate.values["max_per_side"],
            "stop_pct": candidate.values["stop_pct"],
            "target_pct": candidate.values["target_pct"],
        }
        rows.append(row)
    return pd.DataFrame(rows)


def trade_audit(
    signals: pd.DataFrame,
    net_all: np.ndarray,
    candidate: Candidate,
    model: str,
) -> pd.DataFrame:
    indices = candidate.selected_idx
    if not indices.size:
        return pd.DataFrame()
    audit = signals.iloc[indices][
        [
            "day", "hhmm_int", "sid", "tradingsymbol", "side", "price_change_pct",
            "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio",
            "traded_value",
        ]
    ].copy()
    audit.insert(0, "model", model)
    signal_slot = audit.pop("hhmm_int").astype(int)
    audit.insert(2, "signal_end", signal_slot.map(format_hhmm))
    audit.insert(3, "confirmation_end", signal_slot.map(CONFIRMATION_ENDS).map(format_hhmm))
    audit.insert(
        4,
        "rank_within_scan",
        audit.groupby(["day", "signal_end"], sort=False).cumcount() + 1,
    )
    returns = net_all[indices]
    audit["status"] = [
        "NO_FILL" if not math.isfinite(float(value)) else (
            "WIN" if value > 0 else "LOSS" if value < 0 else "FLAT"
        )
        for value in returns
    ]
    audit["net_return_pct"] = returns
    audit["picker"] = candidate.values["picker"]
    audit["max_per_side"] = candidate.values["max_per_side"]
    audit["filter_price_change_pct"] = candidate.values["price_change_pct"]
    audit["filter_oi_change_pct"] = candidate.values["oi_change_pct"]
    audit["filter_volume_ratio"] = candidate.values["volume_ratio"]
    audit["filter_body_ratio"] = candidate.values["body_ratio"]
    audit["filter_max_wick_ratio"] = candidate.values["max_wick_ratio"]
    audit["filter_min_traded_value"] = candidate.values["min_traded_value"]
    audit["stop_pct"] = candidate.values["stop_pct"]
    audit["target_pct"] = candidate.values["target_pct"]
    return audit.reset_index(drop=True)


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
            day_trade_pf = _pf(trade_profit, trade_loss)
            cum_day_pf = _pf(cum_day_profit, cum_day_loss)
            cum_trade_pf = _pf(cum_trade_profit, cum_trade_loss)
            fills = sum(int(row.fills) for row in leg_rows)
            selections = sum(int(row.selections) for row in leg_rows)
            rows.append(
                {
                    "model": model,
                    "day": day,
                    "signal_end": SIGNAL_END_LABEL,
                    "confirmation_end": CONFIRMATION_END_LABEL,
                    "long_symbol": long_row.selected_symbol if long_row is not None else "",
                    "long_status": long_row.status if long_row is not None else "NO_SIGNAL",
                    "long_return_pct": long_row.net_return_pct if long_row is not None else np.nan,
                    "long_selections": int(long_row.selections) if long_row is not None else 0,
                    "long_fills": int(long_row.fills) if long_row is not None else 0,
                    "long_trade_details": long_row.trade_details if long_row is not None else "",
                    "short_symbol": short_row.selected_symbol if short_row is not None else "",
                    "short_status": short_row.status if short_row is not None else "NO_SIGNAL",
                    "short_return_pct": short_row.net_return_pct if short_row is not None else np.nan,
                    "short_selections": int(short_row.selections) if short_row is not None else 0,
                    "short_fills": int(short_row.fills) if short_row is not None else 0,
                    "short_trade_details": short_row.trade_details if short_row is not None else "",
                    "selections": selections,
                    "fills": fills,
                    "portfolio_net_return_pct": day_net,
                    "day_trade_pf": day_trade_pf,
                    "day_trade_pf_label": "INF" if math.isinf(day_trade_pf) else (
                        f"{day_trade_pf:.3f}" if math.isfinite(day_trade_pf) else ""
                    ),
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
        trade_profit = float(legs["gross_profit_pct"].sum())
        trade_loss = float(legs["gross_loss_pct"].sum())
        day_net = days["portfolio_net_return_pct"].to_numpy(float)
        day_profit = float(day_net[day_net > 0].sum())
        day_loss = float(-day_net[day_net < 0].sum())
        results[str(model)] = {
            "orders": int(legs["selections"].sum()),
            "fills": int(legs["fills"].sum()),
            "trade_pf": _pf(trade_profit, trade_loss),
            "net_pct": float(days["portfolio_net_return_pct"].sum()),
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
    mode = "V3 Force-Daily" if meta["force_daily"] else "V3 Filtered"
    table_rule = (
        f"top 1 through top {meta['max_per_side']} confirmed contracts are tested for each side/scan"
        if meta["force_daily"]
        else f"filters and ranking may select up to {meta['max_per_side']} LONG and {meta['max_per_side']} SHORT contracts per scan"
    )
    lines = [
        f"# FNO EMA/OI {meta['signal_end_label']} {mode} Day-Wise Detail",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Timing: 5-minute scan(s) end {meta['signal_end_label']}; confirmation(s) end {meta['confirmation_end_label']}; each entry order activates afterward.",
        f"- Table rule: {table_rule}.",
        "- `NO_SIGNAL` means neither scan produced a selected setup for that side; `NO_FILL` means a stop-entry order did not trigger later.",
        "- Day trade PF uses every filled contract before side/day netting; day net PF uses the final net session return.",
        f"- Train: {meta['train_from']} to {meta['train_to']}; test: {meta['test_from']} to {meta['test_to']}.",
        "",
        "## Portfolio Summary",
        "",
        "| Selection | Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model in MODEL_ORDER:
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
        "| Date | Period | L O/F | LONG contract results | LONG % | S O/F | SHORT contract results | SHORT % | "
        "Total O/F | Day net % | Day trade PF | Day net PF | Cum net % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    test_from = meta["test_from"]
    for model in MODEL_ORDER:
        model_rows = portfolio_daily.loc[portfolio_daily["model"].eq(model)].sort_values("day")
        if model_rows.empty:
            continue
        lines += ["", f"## {model} Day-Wise Table", "", columns, divider]
        for row in model_rows.itertuples(index=False):
            period = "TEST" if row.day >= test_from else "TRAIN"
            long_detail = fmt_symbol(row.long_trade_details) or row.long_status
            short_detail = fmt_symbol(row.short_trade_details) or row.short_status
            lines.append(
                f"| {row.day} | {period} | {int(row.long_selections)}/{int(row.long_fills)} | "
                f"{long_detail} | {fmt_signed(row.long_return_pct)} | "
                f"{int(row.short_selections)}/{int(row.short_fills)} | {short_detail} | "
                f"{fmt_signed(row.short_return_pct)} | {int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {row.day_trade_pf_label} | {row.day_pf_label} | "
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
        f"top 1 through top {meta['max_per_side']} confirmed contracts tested per side/scan without threshold filters"
        if meta["force_daily"]
        else f"up to {meta['max_per_side']} filtered contracts selected per side/scan"
    )
    lines = [
        f"# FNO EMA/OI {meta['signal_end_label']} -> {meta['confirmation_end_label']} V3 PF Optimisation",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Timing: 5-minute scan(s) end {meta['signal_end_label']}; confirmation(s) end {meta['confirmation_end_label']}; each entry order activates afterward.",
        f"- Frequency: {frequency}.",
        f"- Data: {meta['slot_signals']} confirmed candidates across the active scan(s) and {meta['slot_days']} sessions "
        f"({meta['all_days']} total cached sessions).",
        f"- Train: {meta['train_from']} to {meta['train_to']} ({meta['train_days']} sessions); "
        f"test: {meta['test_from']} to {meta['test_to']} ({meta['test_days']} sessions).",
        f"- Cost: {meta['cost_bps']} bps round trip.",
        f"- Force daily: {'yes' if meta['force_daily'] else 'no'}.",
        f"- Train guards: >= {meta['min_trades']} filled trades, day win rate >= {meta['min_day_win']:.0%}, "
        f"best trade <= {meta['max_top_profit_share']:.0%} of gross profit, PF after removing best trade > 1.",
        "",
        "Day trade PF uses all filled contracts in that session. Cumulative trade PF compounds the same gross-profit/gross-loss accounting chronologically.",
        "",
        "## Best configurations",
        "",
        "| Side | Selection | Max/scan | Train robust PF | Train PF | Test PF | All trade PF | All day PF | All trades | All net % | "
        "Trade win | Day win | Picker | Price | OI | Vol | Body | Wick | Stop | Target |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for side in ("LONG", "SHORT"):
        for model, candidate in selected[side]:
            r = candidate.values
            lines.append(
                f"| {side} | {model} | {int(r['max_per_side'])} | {fmt_num(r['train_robust_pf'])} | "
                f"{fmt_num(r['train_pf'])} | {fmt_num(r['test_pf'])} | {fmt_num(r['all_pf'])} | "
                f"{fmt_num(r['all_day_pf'])} | {int(r['all_trades'])} | {r['all_net_pct']:+.3f} | "
                f"{r['all_win_rate']:.1%} | {r['all_day_win_rate']:.1%} | {r['picker']} | "
                f"{r['price_change_pct']} | {r['oi_change_pct']} | {r['volume_ratio']} | "
                f"{r['body_ratio']} | {r['max_wick_ratio']} | {r['stop_pct']} | {r['target_pct']} |"
            )
        if ranked[side].empty:
            lines.append(f"| {side} | No candidate survived train guards | | | | | | | | | | | | | | | | | | |")
    lines += [
        "",
        "`TRAIN_ROBUST` is selected without using the test window. Both `FULL_HISTORY` selections are descriptive and in-sample; they should not be treated as validated.",
        "",
        "## Combined LONG + SHORT portfolio",
        "",
        "Trade PF uses each filled leg separately. Day PF first nets the LONG and SHORT returns within each session.",
        "",
        "| Selection | Period | Orders | Fills | Trade PF | Net % | Positive days | Negative days | Flat days | Day PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model in MODEL_ORDER:
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
        "- Selected-trades CSV retains every chosen contract, its side rank, fill status, and return.",
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
    parser.add_argument(
        "--max-per-side",
        type=int,
        default=MAX_PER_SIDE_LIMIT,
        help="Maximum ranked contracts per side/session to test (1-5).",
    )
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument(
        "--signal-slot",
        action="append",
        type=int,
        choices=SIGNAL_SLOTS,
        help="Scan slot to optimise. Repeat for both; default uses 09:25 and 09:30.",
    )
    parser.add_argument(
        "--force-daily",
        action="store_true",
        help="Rank confirmed candidates for each side/scan without threshold filters.",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.max_per_side <= MAX_PER_SIDE_LIMIT:
        parser.error(f"--max-per-side must be between 1 and {MAX_PER_SIDE_LIMIT}")
    if not 0.0 <= args.min_day_win <= 1.0:
        parser.error("--min-day-win must be between 0 and 1")
    return args


def main(argv: list[str] | None = None) -> int:
    global SLOT_DAYS, SLOT_DAY_IDX, SLOT_SCAN_IDX, ALL_DAY_VALUES
    global SIGNAL_END_LABEL, CONFIRMATION_END_LABEL

    args = parse_args(argv)
    active_slots = tuple(dict.fromkeys(args.signal_slot or SIGNAL_SLOTS))
    SIGNAL_END_LABEL = ",".join(format_hhmm(slot) for slot in active_slots)
    CONFIRMATION_END_LABEL = ",".join(
        format_hhmm(CONFIRMATION_ENDS[slot]) for slot in active_slots
    )
    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)
    session_name = f"{SESSION}_force_daily" if args.force_daily else SESSION
    report_path = FORCE_REPORT_PATH if args.force_daily else REPORT_PATH
    daywise_report_path = (
        FORCE_DAYWISE_REPORT_PATH if args.force_daily else DAYWISE_REPORT_PATH
    )
    file_prefix = (
        "ema_confirm_0925_0930_v3_force_daily"
        if args.force_daily
        else "ema_confirm_0925_0930_v3_once"
    )
    common.publish_status(
        session_name,
        "RUNNING",
        signal_slots=SIGNAL_END_LABEL.replace(":", ""),
        confirmation_ends=CONFIRMATION_END_LABEL.replace(":", ""),
    )

    try:
        signals_all, paths = opt.load_signals(
            args.square_off, args.max_forward_bars, args.rebuild_cache
        )
        signals_all = signals_all.copy()
        signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
        ALL_DAY_VALUES = sorted(set(signals_all["day"]))
        signals = signals_all.loc[signals_all["hhmm_int"].isin(active_slots)].copy()
        signals = signals.sort_values(
            ["day", "hhmm_int", "tradingsymbol", "sid"]
        ).reset_index(drop=True)
        if signals.empty:
            raise RuntimeError(f"No cached signals exist for {SIGNAL_END_LABEL}.")
        if not signals["hhmm_int"].isin(active_slots).all():
            raise AssertionError("A signal outside the active V3 scan window leaked in.")

        SLOT_DAYS = signals["day"].to_numpy()
        all_day_code = {day: idx for idx, day in enumerate(ALL_DAY_VALUES)}
        SLOT_DAY_IDX = np.array([all_day_code[day] for day in SLOT_DAYS], dtype=int)
        scan_keys = list(zip(signals["day"], signals["hhmm_int"].astype(int)))
        unique_scans = {key: idx for idx, key in enumerate(dict.fromkeys(scan_keys))}
        SLOT_SCAN_IDX = np.array([unique_scans[key] for key in scan_keys], dtype=int)

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
            f"[DATA] {len(signals):,} confirmed candidates at {SIGNAL_END_LABEL} across "
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
                args.max_per_side,
                max(args.top_n, 50),
                args.force_daily,
            )
            ranked[side] = frame
            candidates[side] = side_candidates
            net_caches[side] = net_cache
            if not frame.empty:
                keep_ids = set(frame.head(args.top_n).index)
                full_ids = set(frame.nsmallest(args.top_n, "full_history_rank").index)
                day_ids = set(frame.nsmallest(args.top_n, "full_history_day_rank").index)
                output = frame.loc[sorted(keep_ids | full_ids | day_ids)].copy()
                common.atomic_write_csv(
                    output,
                    RESULT_DIR / f"{file_prefix}_ranked_{side}.csv",
                )

        selected = {side: best_candidates(candidates[side]) for side in ("LONG", "SHORT")}
        daily_parts = []
        audit_parts = []
        for side in ("LONG", "SHORT"):
            for model, candidate in selected[side]:
                bracket = (candidate.values["stop_pct"], candidate.values["target_pct"])
                net_all = net_caches[side][bracket]
                daily_parts.append(
                    daily_curve(
                        signals,
                        net_all,
                        candidate,
                        model,
                        ALL_DAY_VALUES,
                    )
                )
                audit_parts.append(trade_audit(signals, net_all, candidate, model))
        daily = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame()
        audit = pd.concat(audit_parts, ignore_index=True) if audit_parts else pd.DataFrame()
        portfolio_daily = pd.DataFrame()
        if not daily.empty:
            daily_limit = args.max_per_side * len(active_slots)
            if (daily["selections"] > daily_limit).any():
                raise AssertionError("V3 exceeded --max-per-side in one of its scan windows.")
            common.atomic_write_csv(daily, RESULT_DIR / f"{file_prefix}_daily_pf.csv")
            if not audit.empty:
                common.atomic_write_csv(
                    audit, RESULT_DIR / f"{file_prefix}_selected_trades.csv"
                )
            cumulative = daily[
                [
                    "model", "side", "day", "status", "selections", "fills",
                    "net_return_pct", "day_pf", "day_pf_label",
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
            "max_per_side": args.max_per_side,
            "min_trades": args.min_trades,
            "min_day_win": args.min_day_win,
            "max_top_profit_share": args.max_top_profit_share,
            "signal_slots": active_slots,
            "signal_end_label": SIGNAL_END_LABEL,
            "confirmation_end_label": CONFIRMATION_END_LABEL,
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
            signal_slots=SIGNAL_END_LABEL.replace(":", ""),
            confirmation_ends=CONFIRMATION_END_LABEL.replace(":", ""),
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
SLOT_SCAN_IDX = np.array([], dtype=int)
ALL_DAY_VALUES: list[date] = []


if __name__ == "__main__":
    raise SystemExit(main())
