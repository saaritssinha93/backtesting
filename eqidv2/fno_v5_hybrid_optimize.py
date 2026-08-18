"""Train-only portfolio optimiser for the corrected hybrid FNO V5 strategy.

The protected V5 setup book is never modified by this module. Candidate legs
use the same five signal slots, one-minute confirmation paths, NSE-equity
prices/indicators, futures OI, delayed stop-entry simulation and per-slot caps
as live V5. Selection and portfolio fitting use the train window only; the
frozen shortlist is scored on test after ranking is complete.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_ema_confirm_sweep as simulator
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as replay
import fno_v5_live_config as config


SESSION = "fno_v5_hybrid_optimize"
OBJECTIVE = "TRAIN_ONLY_ROBUST_V5_OPTIMIZER"
RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = common.LATEST_DIR / "latest_fno_v5_hybrid_optimize.md"
RANKED_PATH = RESULT_DIR / "v5_hybrid_optimize_ranked_portfolios.csv"
SETUPS_PATH = RESULT_DIR / "v5_hybrid_optimize_primary_setups.csv"
TRADES_PATH = RESULT_DIR / "v5_hybrid_optimize_primary_trades.csv"
DAILY_PATH = RESULT_DIR / "v5_hybrid_optimize_primary_daily.csv"

SIGNAL_SLOTS = tuple(
    int(value.replace(":", "")) for value in config.SIGNAL_TO_CONFIRMATION
)
SIDES = ("LONG", "SHORT")
PICKERS = (
    "max_oi",
    "max_volume",
    "max_move",
    "max_body",
    "max_liquidity",
)
FILTER_GRID: dict[str, tuple[float, ...]] = {
    "price_change_pct": (0.20, 0.30, 0.40, 0.50, 0.65, 0.80),
    "oi_change_pct": (0.10, 0.25, 0.40, 0.50, 0.75, 1.00),
    "volume_ratio": (1.0, 1.5, 2.0, 3.0),
    "body_ratio": (0.40, 0.50, 0.60),
    "max_wick_ratio": (0.30, 0.50),
    "min_traded_value": (0.0, 1e7),
}
STOP_PCTS = (0.30, 0.40, 0.50, 0.75, 1.00)
TARGET_PCTS = (0.50, 0.75, 1.00, 1.50, 2.00, 2.50, 3.00)


@dataclass(frozen=True)
class OptimizerGuards:
    min_leg_train_fills: int = 8
    min_portfolio_train_fills: int = 35
    min_portfolio_train_days: int = 20
    min_day_win: float = 0.45
    max_top_day_share: float = 0.35
    min_worst_fold_pf: float = 0.80


@dataclass
class SelectionChoice:
    setup: config.SetupSpec
    selected_idx: np.ndarray
    selected_sids: tuple[int, ...]
    train_selected_sids: tuple[int, ...]
    is_current: bool = False


@dataclass
class LegCandidate:
    candidate_id: str
    setup: config.SetupSpec
    slot_signals: pd.DataFrame
    selected_idx: np.ndarray
    net_selected: np.ndarray
    train_net: np.ndarray
    train_day_idx: np.ndarray
    train_metrics: dict[str, Any]
    is_current: bool = False


@dataclass
class PortfolioState:
    choices: tuple[LegCandidate | None, ...]
    train_net: np.ndarray
    train_day_idx: np.ndarray
    train_orders: int
    train_metrics: dict[str, Any]


def profit_factor(profit: float, loss: float) -> float:
    if loss > 0:
        return float(profit / loss)
    return float("inf") if profit > 0 else float("nan")


def finite_rank(value: Any, cap: float = 20.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(number):
        return 0.0
    if math.isinf(number):
        return cap if number > 0 else 0.0
    return max(0.0, min(number, cap))


def score_vectors(
    net: np.ndarray,
    day_idx: np.ndarray,
    n_days: int,
    *,
    orders: int | None = None,
) -> dict[str, Any]:
    """Score filled returns and remove the best whole day for robustness."""

    values = np.asarray(net, dtype=float)
    codes = np.asarray(day_idx, dtype=int)
    finite = np.isfinite(values)
    values = values[finite]
    codes = codes[finite]
    order_count = int(values.size if orders is None else orders)
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0

    day_net = np.bincount(codes, weights=values, minlength=n_days)
    day_fills = np.bincount(codes, minlength=n_days)
    active = day_fills > 0
    active_net = day_net[active]
    day_profit = float(active_net[active_net > 0].sum())
    day_loss = float(-active_net[active_net < 0].sum())
    total = float(values.sum()) if values.size else 0.0
    best_day_idx = int(np.argmax(day_net)) if active_net.size else -1
    best_day = float(day_net[best_day_idx]) if best_day_idx >= 0 else 0.0

    robust_values = values[codes != best_day_idx] if best_day_idx >= 0 else values
    robust_profit = float(robust_values[robust_values > 0].sum())
    robust_loss = float(-robust_values[robust_values < 0].sum())
    robust_day_net = (
        np.delete(active_net, int(np.argmax(active_net)))
        if active_net.size
        else active_net
    )
    robust_day_profit = float(robust_day_net[robust_day_net > 0].sum())
    robust_day_loss = float(-robust_day_net[robust_day_net < 0].sum())

    fold_pfs: list[float] = []
    fold_nets: list[float] = []
    for fold_days in np.array_split(np.arange(n_days), 3):
        fold_mask = np.isin(codes, fold_days)
        fold_values = values[fold_mask]
        fold_profit = float(fold_values[fold_values > 0].sum())
        fold_loss = float(-fold_values[fold_values < 0].sum())
        fold_pfs.append(profit_factor(fold_profit, fold_loss))
        fold_nets.append(float(fold_values.sum()))
    finite_fold_pfs = [finite_rank(value) for value in fold_pfs]

    return {
        "orders": order_count,
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "profit": profit,
        "loss": loss,
        "pf": profit_factor(profit, loss),
        "net_pct": total,
        "active_days": int(active.sum()),
        "positive_days": int((active_net > 0).sum()),
        "negative_days": int((active_net < 0).sum()),
        "flat_days": int((active_net == 0).sum()),
        "day_win_rate": (
            float((active_net > 0).mean()) if active_net.size else float("nan")
        ),
        "day_pf": profit_factor(day_profit, day_loss),
        "robust_trade_pf": profit_factor(robust_profit, robust_loss),
        "robust_day_pf": profit_factor(robust_day_profit, robust_day_loss),
        "top_day_share": (
            float(best_day / total) if total > 0 else float("inf")
        ),
        "best_day_pct": best_day,
        "worst_fold_pf": min(finite_fold_pfs) if finite_fold_pfs else 0.0,
        "positive_folds": int(sum(value > 0 for value in fold_nets)),
        "worst_fold_net_pct": min(fold_nets) if fold_nets else 0.0,
    }


def score_audit(audit: pd.DataFrame, days: list[date]) -> dict[str, Any]:
    day_code = {day: idx for idx, day in enumerate(days)}
    selected = audit.loc[audit["day"].isin(day_code)].copy()
    filled = selected.loc[selected["filled"]].copy()
    codes = np.array([day_code[day] for day in filled["day"]], dtype=int)
    return score_vectors(
        filled["net_return_pct"].to_numpy(float),
        codes,
        len(days),
        orders=len(selected),
    )


def _picker_orders(signals: pd.DataFrame) -> dict[str, np.ndarray]:
    traded_value = signals["traded_value"].to_numpy(float)
    symbols = signals["tradingsymbol"].astype(str).to_numpy()
    values = {
        "max_oi": signals["oi_change_pct"].to_numpy(float),
        "max_volume": signals["volume_ratio"].to_numpy(float),
        "max_move": np.abs(signals["price_change_pct"].to_numpy(float)),
        "max_body": signals["body_ratio"].to_numpy(float),
        "max_liquidity": traded_value,
    }
    return {
        name: np.lexsort((symbols, -traded_value, -metric))
        for name, metric in values.items()
    }


def select_up_to_per_day(
    eligible: np.ndarray,
    order: np.ndarray,
    day_idx: np.ndarray,
    max_per_day: int,
) -> list[np.ndarray]:
    ranked = order[eligible[order]]
    if ranked.size == 0:
        return [np.array([], dtype=int) for _ in range(max_per_day)]
    ranked_days = day_idx[ranked]
    grouped_order = np.argsort(ranked_days, kind="stable")
    grouped = ranked[grouped_order]
    grouped_days = ranked_days[grouped_order]
    starts = np.r_[0, np.flatnonzero(grouped_days[1:] != grouped_days[:-1]) + 1]
    lengths = np.diff(np.r_[starts, grouped.size])
    rank_within_day = np.arange(grouped.size) - np.repeat(starts, lengths)
    return [
        grouped[rank_within_day < count]
        for count in range(1, max_per_day + 1)
    ]


def _setup_distance(
    setup: config.SetupSpec,
    current: config.SetupSpec | None,
) -> tuple[float, ...]:
    if current is not None:
        return (
            0.0 if setup.mode == current.mode else 1.0,
            0.0 if setup.picker == current.picker else 1.0,
            abs(setup.max_entries - current.max_entries),
            abs(setup.price_change_pct - current.price_change_pct),
            abs(setup.oi_change_pct - current.oi_change_pct),
            abs(setup.volume_ratio - current.volume_ratio),
            abs(setup.body_ratio - current.body_ratio),
            abs(setup.max_wick_ratio - current.max_wick_ratio),
            abs(setup.min_traded_value - current.min_traded_value),
        )
    return (
        0.0 if setup.mode == "FILTERED" else 1.0,
        setup.price_change_pct,
        setup.oi_change_pct,
        setup.volume_ratio,
        setup.body_ratio,
        -setup.max_wick_ratio,
        setup.min_traded_value,
    )


def _new_setup(
    slot: int,
    side: str,
    *,
    mode: str,
    max_entries: int,
    picker: str,
    price_change_pct: float,
    oi_change_pct: float,
    volume_ratio: float,
    body_ratio: float,
    max_wick_ratio: float,
    min_traded_value: float,
    stop_pct: float = 1.0,
    target_pct: float = 1.0,
    source_version: str = "V5_HYBRID_TRAIN_ONLY_GRID",
) -> config.SetupSpec:
    signal_end = f"{slot // 100:02d}:{slot % 100:02d}"
    return config.SetupSpec(
        signal_end=signal_end,
        confirmation_end=config.SIGNAL_TO_CONFIRMATION[signal_end],
        side=side,
        mode=mode,
        max_entries=max_entries,
        picker=picker,
        price_change_pct=float(price_change_pct),
        oi_change_pct=float(oi_change_pct),
        volume_ratio=float(volume_ratio),
        body_ratio=float(body_ratio),
        max_wick_ratio=float(max_wick_ratio),
        min_traded_value=float(min_traded_value),
        stop_pct=float(stop_pct),
        target_pct=float(target_pct),
        source_version=source_version,
    )


def build_selection_choices(
    signals: pd.DataFrame,
    slot: int,
    side: str,
    train_days: set[date],
    *,
    min_train_orders: int,
    search_profile: str,
) -> list[SelectionChoice]:
    """Generate distinct train-eligible choices without looking at outcomes."""

    current = config.setup_for(f"{slot // 100:02d}:{slot % 100:02d}", side)
    day_values = signals["day"].to_numpy()
    _, day_idx = np.unique(day_values, return_inverse=True)
    train_row = np.fromiter(
        (day in train_days for day in day_values), dtype=bool, count=len(signals)
    )
    side_mask = signals["side"].eq(side).to_numpy()
    columns = {
        "price": signals["price_change_pct"].to_numpy(float),
        "oi": signals["oi_change_pct"].to_numpy(float),
        "volume": signals["volume_ratio"].to_numpy(float),
        "body": signals["body_ratio"].to_numpy(float),
        "wick": signals["wick_ratio"].to_numpy(float),
        "traded_value": signals["traded_value"].to_numpy(float),
    }
    orders = _picker_orders(signals)
    side_cap = 1 if side == "LONG" else 2
    sids = signals["sid"].to_numpy(int)
    choices: dict[tuple[str, str, int, tuple[int, ...]], SelectionChoice] = {}

    def retain(setup: config.SetupSpec, selected_idx: np.ndarray) -> None:
        if selected_idx.size == 0:
            return
        selected_sids = tuple(sorted(int(value) for value in sids[selected_idx]))
        train_selected_sids = tuple(
            sorted(int(value) for value in sids[selected_idx][train_row[selected_idx]])
        )
        is_current = current is not None and setup == current
        if int(train_row[selected_idx].sum()) < min_train_orders and not is_current:
            return
        # Thresholds that make exactly the same train selections are not
        # distinguishable during fitting. Keep one conservative representative
        # instead of letting arbitrary test-only differences enter the beam.
        key = (setup.mode, setup.picker, setup.max_entries, train_selected_sids)
        candidate = SelectionChoice(
            setup=setup,
            selected_idx=np.asarray(selected_idx, dtype=int),
            selected_sids=selected_sids,
            train_selected_sids=train_selected_sids,
            is_current=is_current,
        )
        previous = choices.get(key)
        if previous is None or _setup_distance(setup, current) < _setup_distance(
            previous.setup, current
        ):
            choices[key] = candidate

    if search_profile == "full-grid":
        filter_values: Iterable[tuple[float, ...]] = itertools.product(
            *(FILTER_GRID[name] for name in FILTER_GRID)
        )
    else:
        filter_values = ()

    for values in filter_values:
        price, oi, volume, body, wick, traded_value = values
        eligible = (
            side_mask
            & (columns["oi"] >= oi)
            & (columns["volume"] >= volume)
            & (columns["body"] >= body)
            & (columns["wick"] <= wick)
            & (columns["traded_value"] >= traded_value)
            & (
                (columns["price"] >= price)
                if side == "LONG"
                else (columns["price"] <= -price)
            )
        )
        if not eligible.any():
            continue
        for picker in PICKERS:
            ranked = select_up_to_per_day(
                eligible, orders[picker], day_idx, side_cap
            )
            for max_entries, selected_idx in enumerate(ranked, start=1):
                retain(
                    _new_setup(
                        slot,
                        side,
                        mode="FILTERED",
                        max_entries=max_entries,
                        picker=picker,
                        price_change_pct=price,
                        oi_change_pct=oi,
                        volume_ratio=volume,
                        body_ratio=body,
                        max_wick_ratio=wick,
                        min_traded_value=traded_value,
                    ),
                    selected_idx,
                )

    # FORCE_DAILY remains an option only where current V5 already uses it.
    if current is not None and current.mode == "FORCE_DAILY":
        for picker in PICKERS:
            selected_idx = select_up_to_per_day(
                side_mask, orders[picker], day_idx, current.max_entries
            )[current.max_entries - 1]
            retain(
                _new_setup(
                    slot,
                    side,
                    mode="FORCE_DAILY",
                    max_entries=current.max_entries,
                    picker=picker,
                    price_change_pct=0.0,
                    oi_change_pct=0.0,
                    volume_ratio=0.0,
                    body_ratio=0.0,
                    max_wick_ratio=999.0,
                    min_traded_value=0.0,
                ),
                selected_idx,
            )

    if current is not None:
        selected = replay.select_setup_rows(signals, current)
        sid_to_idx = {int(sid): idx for idx, sid in enumerate(sids)}
        selected_idx = np.array(
            [sid_to_idx[int(sid)] for sid in selected["sid"]], dtype=int
        )
        retain(current, selected_idx)

    return list(choices.values())


def _candidate_id(setup: config.SetupSpec, selected_sids: Iterable[int]) -> str:
    payload = {
        "setup": asdict(setup),
        "selected_sids": list(selected_sids),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    return hashlib.sha256(encoded).hexdigest()[:16]


def _leg_guard(stats: dict[str, Any], guards: OptimizerGuards) -> bool:
    return bool(
        stats["fills"] >= guards.min_leg_train_fills
        and stats["wins"] >= 2
        and stats["losses"] >= 2
        and stats["net_pct"] > 0
        and finite_rank(stats["pf"]) > 1.0
        and finite_rank(stats["robust_trade_pf"]) > 0.90
        and stats["day_win_rate"] >= 0.40
        and stats["top_day_share"] <= 0.65
    )


def _balanced_key(candidate: LegCandidate) -> tuple[float, ...]:
    stats = candidate.train_metrics
    robust = min(
        finite_rank(stats["robust_trade_pf"]),
        finite_rank(stats["robust_day_pf"]),
        finite_rank(stats["worst_fold_pf"]),
    )
    return (
        robust,
        finite_rank(stats["pf"]),
        finite_rank(stats["day_pf"]),
        float(stats["net_pct"]),
        int(stats["fills"]),
    )


def retain_leg_candidates(
    candidates: list[LegCandidate],
    retain_n: int,
) -> list[LegCandidate]:
    if len(candidates) <= retain_n:
        return sorted(candidates, key=_balanced_key, reverse=True)
    rankings = (
        sorted(candidates, key=_balanced_key, reverse=True),
        sorted(
            candidates,
            key=lambda item: (
                finite_rank(item.train_metrics["pf"]),
                item.train_metrics["fills"],
                item.train_metrics["net_pct"],
            ),
            reverse=True,
        ),
        sorted(
            candidates,
            key=lambda item: (
                finite_rank(item.train_metrics["day_pf"]),
                item.train_metrics["active_days"],
                item.train_metrics["net_pct"],
            ),
            reverse=True,
        ),
        sorted(
            candidates,
            key=lambda item: (
                item.train_metrics["net_pct"],
                item.train_metrics["fills"],
                finite_rank(item.train_metrics["pf"]),
            ),
            reverse=True,
        ),
    )
    kept: dict[str, LegCandidate] = {}
    cursors = [0] * len(rankings)
    while len(kept) < retain_n:
        added = False
        for rank_no, ranking in enumerate(rankings):
            while (
                cursors[rank_no] < len(ranking)
                and ranking[cursors[rank_no]].candidate_id in kept
            ):
                cursors[rank_no] += 1
            if cursors[rank_no] < len(ranking):
                item = ranking[cursors[rank_no]]
                cursors[rank_no] += 1
                if item.candidate_id not in kept:
                    kept[item.candidate_id] = item
                    added = True
                    if len(kept) >= retain_n:
                        break
        if not added:
            break

    current = next((item for item in candidates if item.is_current), None)
    if current is not None:
        kept[current.candidate_id] = current
    return sorted(kept.values(), key=_balanced_key, reverse=True)


def optimise_leg(
    signals: pd.DataFrame,
    bracket_net: dict[tuple[float, float], np.ndarray],
    slot: int,
    side: str,
    train_days: set[date],
    all_day_code: dict[date, int],
    guards: OptimizerGuards,
    *,
    retain_n: int,
    search_profile: str,
    candidate_source_version: str = "V5_HYBRID_TRAIN_ONLY_OPTIMIZED",
) -> tuple[list[LegCandidate], int]:
    choices = build_selection_choices(
        signals,
        slot,
        side,
        train_days,
        min_train_orders=guards.min_leg_train_fills,
        search_profile=search_profile,
    )
    day_values = signals["day"].to_numpy()
    day_codes = np.array([all_day_code[day] for day in day_values], dtype=int)
    train_row = np.fromiter(
        (day in train_days for day in day_values), dtype=bool, count=len(signals)
    )
    train_day_code = {day: idx for idx, day in enumerate(sorted(train_days))}
    survivors: list[LegCandidate] = []
    evaluated = 0
    behavior_seen: dict[tuple[tuple[int, ...], float, float], LegCandidate] = {}

    for choice in choices:
        selected_idx = choice.selected_idx
        selected_train = train_row[selected_idx]
        for (stop_pct, target_pct), net_all in bracket_net.items():
            evaluated += 1
            net_selected = net_all[selected_idx]
            train_values = net_selected[selected_train]
            train_days_selected = day_values[selected_idx][selected_train]
            finite = np.isfinite(train_values)
            train_net = train_values[finite]
            train_idx = np.array(
                [train_day_code[day] for day in train_days_selected[finite]],
                dtype=int,
            )
            stats = score_vectors(
                train_net,
                train_idx,
                len(train_days),
                orders=int(selected_train.sum()),
            )
            is_current = bool(
                choice.is_current
                and math.isclose(stop_pct, choice.setup.stop_pct)
                and math.isclose(target_pct, choice.setup.target_pct)
            )
            if not is_current and not _leg_guard(stats, guards):
                continue
            setup = _new_setup(
                slot,
                side,
                mode=choice.setup.mode,
                max_entries=choice.setup.max_entries,
                picker=choice.setup.picker,
                price_change_pct=choice.setup.price_change_pct,
                oi_change_pct=choice.setup.oi_change_pct,
                volume_ratio=choice.setup.volume_ratio,
                body_ratio=choice.setup.body_ratio,
                max_wick_ratio=choice.setup.max_wick_ratio,
                min_traded_value=choice.setup.min_traded_value,
                stop_pct=stop_pct,
                target_pct=target_pct,
                source_version=(
                    choice.setup.source_version
                    if is_current
                    else candidate_source_version
                ),
            )
            candidate = LegCandidate(
                candidate_id=_candidate_id(setup, choice.selected_sids),
                setup=setup,
                slot_signals=signals,
                selected_idx=selected_idx,
                net_selected=net_selected,
                train_net=train_net,
                train_day_idx=train_idx,
                train_metrics=stats,
                is_current=is_current,
            )
            behavior_key = (choice.train_selected_sids, stop_pct, target_pct)
            previous = behavior_seen.get(behavior_key)
            if previous is None or _setup_distance(
                candidate.setup, config.setup_for(candidate.setup.signal_end, side)
            ) < _setup_distance(
                previous.setup, config.setup_for(previous.setup.signal_end, side)
            ):
                behavior_seen[behavior_key] = candidate

    survivors.extend(behavior_seen.values())
    return retain_leg_candidates(survivors, retain_n), evaluated


def _state_signature(state: PortfolioState) -> tuple[str, ...]:
    return tuple(
        choice.candidate_id if choice is not None else "NONE"
        for choice in state.choices
    )


def _state_keys(
    state: PortfolioState,
    target_fills: int,
) -> tuple[tuple[float, ...], ...]:
    stats = state.train_metrics
    sample = min(1.0, stats["fills"] / max(target_fills, 1))
    concentration = (
        1.0
        if stats["top_day_share"] <= 0.35
        else max(0.25, 1.0 - stats["top_day_share"])
    )
    robust = min(
        finite_rank(stats["robust_trade_pf"]),
        finite_rank(stats["robust_day_pf"]),
    )
    balanced = robust * sample * concentration
    common_tail = (
        int(stats["fills"]),
        float(stats["net_pct"]),
        -len([choice for choice in state.choices if choice is not None]),
    )
    return (
        (balanced, finite_rank(stats["pf"]), *common_tail),
        (finite_rank(stats["pf"]) * sample, robust, *common_tail),
        (finite_rank(stats["day_pf"]) * sample, robust, *common_tail),
        (float(stats["net_pct"]) * sample, robust, *common_tail),
    )


def retain_states(
    states: list[PortfolioState],
    beam_width: int,
    target_fills: int,
) -> list[PortfolioState]:
    unique = {_state_signature(state): state for state in states}
    values = list(unique.values())
    if len(values) <= beam_width:
        return sorted(
            values,
            key=lambda state: _state_keys(state, target_fills)[0],
            reverse=True,
        )
    rankings = [
        sorted(
            values,
            key=lambda state, key_no=key_no: _state_keys(
                state, target_fills
            )[key_no],
            reverse=True,
        )
        for key_no in range(4)
    ]
    kept: dict[tuple[str, ...], PortfolioState] = {}
    cursor = 0
    while len(kept) < beam_width:
        added = False
        for ranking in rankings:
            if cursor >= len(ranking):
                continue
            state = ranking[cursor]
            signature = _state_signature(state)
            if signature not in kept:
                kept[signature] = state
                added = True
                if len(kept) >= beam_width:
                    break
        if not added and all(cursor >= len(ranking) - 1 for ranking in rankings):
            break
        cursor += 1
    return sorted(
        kept.values(),
        key=lambda state: _state_keys(state, target_fills)[0],
        reverse=True,
    )


def beam_portfolios(
    legs: list[tuple[tuple[int, str], list[LegCandidate]]],
    train_days: list[date],
    guards: OptimizerGuards,
    *,
    beam_width: int,
) -> list[PortfolioState]:
    empty_metrics = score_vectors(
        np.array([], dtype=float),
        np.array([], dtype=int),
        len(train_days),
        orders=0,
    )
    beam = [
        PortfolioState(
            choices=(),
            train_net=np.array([], dtype=float),
            train_day_idx=np.array([], dtype=int),
            train_orders=0,
            train_metrics=empty_metrics,
        )
    ]
    for leg_no, (leg_key, candidates) in enumerate(legs, start=1):
        expanded: list[PortfolioState] = []
        for state in beam:
            for candidate in [None, *candidates]:
                if candidate is None:
                    net = state.train_net
                    day_idx = state.train_day_idx
                    orders = state.train_orders
                else:
                    net = np.concatenate((state.train_net, candidate.train_net))
                    day_idx = np.concatenate(
                        (state.train_day_idx, candidate.train_day_idx)
                    )
                    orders = state.train_orders + int(
                        candidate.train_metrics["orders"]
                    )
                metrics = score_vectors(
                    net, day_idx, len(train_days), orders=orders
                )
                expanded.append(
                    PortfolioState(
                        choices=(*state.choices, candidate),
                        train_net=net,
                        train_day_idx=day_idx,
                        train_orders=orders,
                        train_metrics=metrics,
                    )
                )
        beam = retain_states(
            expanded,
            beam_width,
            guards.min_portfolio_train_fills,
        )
        print(
            f"[BEAM {leg_no:02d}/{len(legs)}] {leg_key[0]:04d} "
            f"{leg_key[1]} options={len(candidates) + 1} retained={len(beam)}",
            flush=True,
        )
    return beam


def passes_portfolio_guards(
    stats: dict[str, Any], guards: OptimizerGuards
) -> bool:
    return bool(
        stats["fills"] >= guards.min_portfolio_train_fills
        and stats["active_days"] >= guards.min_portfolio_train_days
        and stats["wins"] >= 2
        and stats["losses"] >= 2
        and stats["net_pct"] > 0
        and finite_rank(stats["pf"]) > 1.0
        and finite_rank(stats["day_pf"]) > 1.0
        and finite_rank(stats["robust_trade_pf"]) > 1.0
        and finite_rank(stats["robust_day_pf"]) > 1.0
        and stats["day_win_rate"] >= guards.min_day_win
        and stats["top_day_share"] <= guards.max_top_day_share
        and stats["positive_folds"] == 3
        and stats["worst_fold_pf"] >= guards.min_worst_fold_pf
    )


def portfolio_key(state: PortfolioState) -> tuple[float, ...]:
    stats = state.train_metrics
    return (
        min(
            finite_rank(stats["robust_trade_pf"]),
            finite_rank(stats["robust_day_pf"]),
            finite_rank(stats["worst_fold_pf"]),
        ),
        finite_rank(stats["pf"]),
        finite_rank(stats["day_pf"]),
        float(stats["net_pct"]),
        int(stats["fills"]),
        -len([choice for choice in state.choices if choice is not None]),
    )


def state_audit(state: PortfolioState) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for candidate in state.choices:
        if candidate is None:
            continue
        rows = candidate.slot_signals.iloc[candidate.selected_idx].copy()
        rows["net_return_pct"] = candidate.net_selected
        rows["filled"] = rows["net_return_pct"].notna()
        rows["setup_id"] = candidate.setup.setup_id
        rows["confirmation_end"] = candidate.setup.confirmation_end
        rows["setup_mode"] = candidate.setup.mode
        rows["picker"] = candidate.setup.picker
        rows["max_entries"] = candidate.setup.max_entries
        rows["stop_pct"] = candidate.setup.stop_pct
        rows["target_pct"] = candidate.setup.target_pct
        rows["candidate_id"] = candidate.candidate_id
        parts.append(rows)
    if not parts:
        return pd.DataFrame()
    audit = pd.concat(parts, ignore_index=True, sort=False)
    audit["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    audit["objective"] = OBJECTIVE
    return audit.sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
        kind="stable",
    ).reset_index(drop=True)


def evaluate_state(
    state: PortfolioState,
    train_days: list[date],
    test_days: list[date],
    all_days: list[date],
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    audit = state_audit(state)
    return audit, {
        "TRAIN": score_audit(audit, train_days),
        "TEST": score_audit(audit, test_days),
        "ALL": score_audit(audit, all_days),
    }


def baseline_results(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    train_days: list[date],
    test_days: list[date],
    all_days: list[date],
    cost_bps: float,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    audit = replay.replay_setups(
        signals,
        paths,
        cost_bps=cost_bps,
        setups=config.ACTIVE_SETUPS,
    )
    return audit, {
        "TRAIN": score_audit(audit, train_days),
        "TEST": score_audit(audit, test_days),
        "ALL": score_audit(audit, all_days),
    }


def _fmt(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isnan(number):
        return ""
    if math.isinf(number):
        return "INF"
    return f"{number:.{digits}f}"


def _fmt_signed(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number):
        return _fmt(number, digits)
    return f"{number:+.{digits}f}"


def setup_frame(state: PortfolioState) -> pd.DataFrame:
    rows = []
    for candidate in state.choices:
        if candidate is None:
            continue
        rows.append(
            {
                **asdict(candidate.setup),
                "candidate_id": candidate.candidate_id,
                "train_pf": candidate.train_metrics["pf"],
                "train_robust_pf": candidate.train_metrics["robust_trade_pf"],
                "train_fills": candidate.train_metrics["fills"],
                "train_net_pct": candidate.train_metrics["net_pct"],
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
                "objective": OBJECTIVE,
            }
        )
    return pd.DataFrame(rows)


def ranked_frame(
    frozen: list[PortfolioState],
    evaluations: list[dict[str, dict[str, Any]]],
) -> pd.DataFrame:
    rows = []
    for rank, (state, periods) in enumerate(zip(frozen, evaluations), start=1):
        active = [choice for choice in state.choices if choice is not None]
        row: dict[str, Any] = {
            "train_rank": rank,
            "objective": OBJECTIVE,
            "active_legs": len(active),
            "setup_ids": ",".join(choice.setup.setup_id for choice in active),
            "candidate_ids": ",".join(choice.candidate_id for choice in active),
        }
        for period, stats in periods.items():
            for key in (
                "orders",
                "fills",
                "pf",
                "day_pf",
                "robust_trade_pf",
                "robust_day_pf",
                "net_pct",
                "active_days",
                "day_win_rate",
                "top_day_share",
                "worst_fold_pf",
                "positive_folds",
            ):
                row[f"{period.lower()}_{key}"] = stats[key]
        rows.append(row)
    return pd.DataFrame(rows)


def render_report(
    primary: PortfolioState,
    primary_periods: dict[str, dict[str, Any]],
    baseline: dict[str, dict[str, Any]],
    ranked: pd.DataFrame,
    setups: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    split_day: date,
    guards: OptimizerGuards,
    evaluated: int,
    beam_width: int,
    cost_bps: float,
    search_profile: str,
) -> str:
    lines = [
        "# FNO V5 Hybrid Train-Only Optimisation",
        "",
        f"Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"Train/test split: `{split_day.isoformat()}`",
        f"Round-trip cost: `{cost_bps:g} bps`",
        f"Search profile: `{search_profile}`",
        f"Candidate evaluations: `{evaluated:,}`; beam width: `{beam_width:,}`",
        f"Objective: `{OBJECTIVE}`",
        "",
        "The primary portfolio was ranked using train data only. Test was scored after the frozen train shortlist was complete. Protected V5 and live configuration were not changed.",
        "",
        "## Baseline vs Primary",
        "",
        "Strategy | Period | Orders/Fills | Trade PF | Day PF | Robust PF | Net % | Active days",
        "--- | --- | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for label, periods in (("Current V5", baseline), ("Optimized primary", primary_periods)):
        for period in ("TRAIN", "TEST", "ALL"):
            stats = periods[period]
            lines.append(
                f"{label} | {period} | {stats['orders']}/{stats['fills']} | "
                f"{_fmt(stats['pf'])} | {_fmt(stats['day_pf'])} | "
                f"{_fmt(stats['robust_trade_pf'])} | "
                f"{_fmt_signed(stats['net_pct'])}% | {stats['active_days']}"
            )

    lines += [
        "",
        "## Primary Setups",
        "",
        "Entry | Side | Mode | Max | Picker | Price | OI | Vol | Body | Wick | Stop | Target | Train fills | Train PF",
        "--- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.confirmation_end} | {row.side} | {row.mode} | "
            f"{row.max_entries} | {row.picker} | {_fmt(row.price_change_pct, 2)} | "
            f"{_fmt(row.oi_change_pct, 2)} | {_fmt(row.volume_ratio, 2)} | "
            f"{_fmt(row.body_ratio, 2)} | {_fmt(row.max_wick_ratio, 2)} | "
            f"{_fmt(row.stop_pct, 2)}% | {_fmt(row.target_pct, 2)}% | "
            f"{row.train_fills} | {_fmt(row.train_pf)}"
        )

    lines += [
        "",
        "## Frozen Train Shortlist",
        "",
        "Rank | Legs | Train fills | Train PF | Train robust PF | Test fills | Test PF | Test net % | All PF | All net %",
        "---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in ranked.head(20).itertuples(index=False):
        lines.append(
            f"{row.train_rank} | {row.active_legs} | {row.train_fills} | "
            f"{_fmt(row.train_pf)} | {_fmt(row.train_robust_trade_pf)} | "
            f"{row.test_fills} | {_fmt(row.test_pf)} | "
            f"{_fmt_signed(row.test_net_pct)}% | {_fmt(row.all_pf)} | "
            f"{_fmt_signed(row.all_net_pct)}%"
        )

    lines += [
        "",
        "## Daywise Primary",
        "",
        "Day | Period | L O/F | Long % | S O/F | Short % | Total % | Cum % | Cum trade PF | Cum day PF",
        "--- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for row in daily.itertuples(index=False):
        lines.append(
            f"{row.day} | {row.period} | {row.long_selections}/{row.long_fills} | "
            f"{_fmt_signed(row.long_return_pct)}% | "
            f"{row.short_selections}/{row.short_fills} | "
            f"{_fmt_signed(row.short_return_pct)}% | "
            f"{_fmt_signed(row.portfolio_net_return_pct)}% | "
            f"{_fmt_signed(row.cumulative_net_pct)}% | "
            f"{_fmt(row.cumulative_trade_pf)} | {_fmt(row.cumulative_day_pf)}"
        )

    lines += [
        "",
        "## Guards and Limitations",
        "",
        f"- Final train fills >= {guards.min_portfolio_train_fills}; active days >= {guards.min_portfolio_train_days}; day win >= {guards.min_day_win:.0%}.",
        f"- Best train day <= {guards.max_top_day_share:.0%} of train net; both trade and day PF remain above 1 after removing the best train day.",
        f"- All three contiguous train folds must be profitable; worst fold PF >= {guards.min_worst_fold_pf:.2f}.",
        "- Futures files contribute OI and OI percentage change only. NSE equity data supplies price, volume, indicators, confirmation, trigger and exits.",
        "- Historical OI still uses the available 26AUG contract across this 52-session sample rather than a rolling near-month history.",
        "- The sample is small. The frozen test is evidence, not proof, and the shortlist test columns must not be used to reselect a winner.",
        "",
        "## Outputs",
        "",
        f"- Ranked frozen portfolios: `{RANKED_PATH}`",
        f"- Primary setups: `{SETUPS_PATH}`",
        f"- Primary trades: `{TRADES_PATH}`",
        f"- Primary daywise curve: `{DAILY_PATH}`",
    ]
    return "\n".join(lines) + "\n"


def validate_output_isolation() -> None:
    protected = {
        config.SELECTED_DAILY_PATH.resolve(),
        Path(config.__file__).resolve(),
    }
    outputs = {
        REPORT_PATH.resolve(),
        RANKED_PATH.resolve(),
        SETUPS_PATH.resolve(),
        TRADES_PATH.resolve(),
        DAILY_PATH.resolve(),
    }
    overlap = protected & outputs
    if overlap:
        raise AssertionError(f"Optimizer output overlaps protected V5: {overlap}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--leg-retain-n", type=int, default=12)
    parser.add_argument("--beam-width", type=int, default=1200)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--min-leg-train-fills", type=int, default=8)
    parser.add_argument("--min-portfolio-train-fills", type=int, default=35)
    parser.add_argument("--min-portfolio-train-days", type=int, default=20)
    parser.add_argument("--min-day-win", type=float, default=0.45)
    parser.add_argument("--max-top-day-share", type=float, default=0.35)
    parser.add_argument("--min-worst-fold-pf", type=float, default=0.80)
    parser.add_argument(
        "--search-profile",
        choices=("conservative", "full-grid"),
        default="conservative",
        help="Conservative tunes current V5 brackets/leg inclusion; full-grid also refits filters and pickers.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_output_isolation()
    started = time.monotonic()
    common.publish_status(SESSION, "RUNNING")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)

    guards = OptimizerGuards(
        min_leg_train_fills=args.min_leg_train_fills,
        min_portfolio_train_fills=args.min_portfolio_train_fills,
        min_portfolio_train_days=args.min_portfolio_train_days,
        min_day_win=args.min_day_win,
        max_top_day_share=args.max_top_day_share,
        min_worst_fold_pf=args.min_worst_fold_pf,
    )
    signals, paths = signal_cache.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    contracts = set(signals["data_contract"].dropna().astype(str))
    if contracts != {hybrid.DATA_CONTRACT_VERSION}:
        raise RuntimeError(f"Unexpected hybrid signal contract: {sorted(contracts)}")
    all_days = sorted(set(signals["day"]))
    split_day = pd.Timestamp(args.split_day).date()
    train_days = [day for day in all_days if day < split_day]
    test_days = [day for day in all_days if day >= split_day]
    if not train_days or not test_days:
        raise RuntimeError("Both train and test windows are required.")
    train_set = set(train_days)
    all_day_code = {day: idx for idx, day in enumerate(all_days)}
    print(
        f"[DATA] {len(signals):,} signals | train={len(train_days)} "
        f"test={len(test_days)} | contract={hybrid.DATA_CONTRACT_VERSION}",
        flush=True,
    )

    leg_searches: list[tuple[tuple[int, str], list[LegCandidate]]] = []
    evaluated = 0
    for slot in SIGNAL_SLOTS:
        if args.search_profile == "conservative" and not any(
            config.setup_for(f"{slot // 100:02d}:{slot % 100:02d}", side)
            for side in SIDES
        ):
            for side in SIDES:
                leg_searches.append(((slot, side), []))
                print(f"[LEG {slot:04d} {side}] inactive in conservative profile", flush=True)
            continue
        slot_signals = signals.loc[signals["hhmm_int"].eq(slot)].copy()
        slot_signals = slot_signals.sort_values(
            ["day", "tradingsymbol", "sid"], kind="stable"
        ).reset_index(drop=True)
        bracket_net = {
            (stop_pct, target_pct): simulator.simulate_bracket(
                slot_signals,
                paths,
                stop_pct=stop_pct,
                target_pct=target_pct,
                cost_bps=args.cost_bps,
            )
            for stop_pct, target_pct in itertools.product(STOP_PCTS, TARGET_PCTS)
        }
        for side in SIDES:
            candidates, count = optimise_leg(
                slot_signals,
                bracket_net,
                slot,
                side,
                train_set,
                all_day_code,
                guards,
                retain_n=args.leg_retain_n,
                search_profile=args.search_profile,
            )
            evaluated += count
            leg_searches.append(((slot, side), candidates))
            print(
                f"[LEG {slot:04d} {side}] retained={len(candidates)} "
                f"evaluated={count:,}",
                flush=True,
            )

    beam = beam_portfolios(
        leg_searches,
        train_days,
        guards,
        beam_width=args.beam_width,
    )
    valid = [state for state in beam if passes_portfolio_guards(state.train_metrics, guards)]
    if not valid:
        raise RuntimeError("No train-only portfolio survived the optimizer guards.")
    valid.sort(key=portfolio_key, reverse=True)
    frozen = valid[: args.top_n]

    evaluations: list[dict[str, dict[str, Any]]] = []
    audits: list[pd.DataFrame] = []
    for state in frozen:
        audit, periods = evaluate_state(state, train_days, test_days, all_days)
        audits.append(audit)
        evaluations.append(periods)
    primary = frozen[0]
    primary_audit = audits[0]
    primary_periods = evaluations[0]
    _, baseline = baseline_results(
        signals,
        paths,
        train_days,
        test_days,
        all_days,
        args.cost_bps,
    )

    ranked = ranked_frame(frozen, evaluations)
    setups = setup_frame(primary)
    daily = replay.build_daily_curve(
        primary_audit, all_days, split_day=split_day
    )
    daily["objective"] = OBJECTIVE
    common.atomic_write_csv(ranked, RANKED_PATH)
    common.atomic_write_csv(setups, SETUPS_PATH)
    common.atomic_write_csv(primary_audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    report = render_report(
        primary,
        primary_periods,
        baseline,
        ranked,
        setups,
        daily,
        split_day=split_day,
        guards=guards,
        evaluated=evaluated,
        beam_width=args.beam_width,
        cost_bps=args.cost_bps,
        search_profile=args.search_profile,
    )
    common.atomic_write_text(REPORT_PATH, report)

    all_stats = primary_periods["ALL"]
    test_stats = primary_periods["TEST"]
    duration = time.monotonic() - started
    common.publish_status(
        SESSION,
        "SUCCESS",
        duration_sec=round(duration, 1),
        evaluated=evaluated,
        all_pf=all_stats["pf"],
        test_pf=test_stats["pf"],
    )
    print(
        f"[PRIMARY] ALL PF={all_stats['pf']:.3f} net={all_stats['net_pct']:+.3f}% "
        f"| TEST PF={test_stats['pf']:.3f} net={test_stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {duration:.1f}s | {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
