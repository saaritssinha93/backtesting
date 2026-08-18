"""Build V4 by stacking a 09:35/09:36 add-on onto locked current V3.

The current selected V3 curve is a read-only baseline. V4 adds exactly one
new daily scan: the 5-minute signal candle ends at 09:35, its 1-minute
confirmation ends at 09:36, and stop-entry orders may fill only afterward.
The new scan allows at most one LONG and two SHORT contracts.
"""

from __future__ import annotations

import argparse
import itertools
import math
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_0925_0930_best_combo_v3 as current_v3
import fno_oi_ema_confirm_0925_0930_pf_v3 as optimiser
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_ema_confirm_sweep as simulator


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = (
    common.LATEST_DIR
    / "latest_fno_oi_ema_confirm_0925_0930_0935_v4_best_full.md"
)
DAILY_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_best_full_daily.csv"
)
SELECTED_DAILY_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_selected_daily.csv"
)
AUDIT_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_selected_trades.csv"
)
RANKED_LONG_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_ranked_LONG.csv"
)
RANKED_SHORT_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_ranked_SHORT.csv"
)
SETUPS_OUTPUT_PATH = (
    RESULT_DIR / "ema_confirm_0925_0930_0935_v4_selected_setups.csv"
)

V4_SIGNAL_SLOT = 935
V4_CONFIRMATION_END = 936
LONG_MAX = 1
SHORT_MAX = 2
CURRENT_V3_EXPECTED = {
    "trade_pf": 3.752,
    "day_pf": 10.707,
    "net_pct": 80.415,
}
SELECTED_OBJECTIVE = "BEST_TRADE_PF"
OBJECTIVES = {
    "BEST_TRADE_PF": "trade_pf",
    "BEST_DAY_PF": "day_pf",
    "BEST_NET": "net_pct",
}


@dataclass(frozen=True)
class FixedContext:
    day_net: np.ndarray
    trade_profit: float
    trade_loss: float
    net_pct: float
    orders: int
    fills: int


@dataclass
class LegChoice:
    side: str
    mode: str
    candidate: optimiser.Candidate | None
    day_net: np.ndarray

    @property
    def values(self) -> dict[str, Any]:
        return self.candidate.values if self.candidate is not None else {}

    @property
    def profit(self) -> float:
        return float(self.values.get("all_gross_profit_pct", 0.0))

    @property
    def loss(self) -> float:
        return float(self.values.get("all_gross_loss_pct", 0.0))

    @property
    def net_pct(self) -> float:
        return float(self.values.get("all_net_pct", 0.0))

    @property
    def orders(self) -> int:
        return int(self.values.get("all_orders", 0))

    @property
    def fills(self) -> int:
        return int(self.values.get("all_trades", 0))


@dataclass
class CombinationChoice:
    long: LegChoice
    short: LegChoice
    estimated: dict[str, Any]


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


def setup_timing_globals(signals: pd.DataFrame, all_days: list[date]) -> None:
    optimiser.SLOT_DAYS = signals["day"].to_numpy()
    day_code = {day: idx for idx, day in enumerate(all_days)}
    optimiser.SLOT_DAY_IDX = np.array(
        [day_code[day] for day in optimiser.SLOT_DAYS], dtype=int
    )
    scan_keys = list(zip(signals["day"], signals["hhmm_int"].astype(int)))
    unique_scans = {key: idx for idx, key in enumerate(dict.fromkeys(scan_keys))}
    optimiser.SLOT_SCAN_IDX = np.array(
        [unique_scans[key] for key in scan_keys], dtype=int
    )
    optimiser.ALL_DAY_VALUES = list(all_days)
    optimiser.SIGNAL_END_LABEL = "09:35"
    optimiser.CONFIRMATION_END_LABEL = "09:36"
    optimiser.CONFIRMATION_ENDS = {
        **optimiser.CONFIRMATION_ENDS,
        V4_SIGNAL_SLOT: V4_CONFIRMATION_END,
    }


def load_current_v3() -> tuple[pd.DataFrame, dict[str, Any]]:
    path = current_v3.SELECTED_DAILY_OUTPUT_PATH
    if not path.exists():
        raise FileNotFoundError(f"Missing current selected V3 curve: {path}")
    curve = pd.read_csv(path)
    curve["day"] = pd.to_datetime(curve["day"]).dt.date
    if curve["day"].duplicated().any():
        raise RuntimeError("Current selected V3 curve contains duplicate sessions.")
    all_days = set(curve["day"])
    stats = current_v3.period_stats(curve, all_days)
    for metric, expected in CURRENT_V3_EXPECTED.items():
        actual = float(stats[metric])
        if abs(actual - expected) > 0.0015:
            raise AssertionError(
                f"Current V3 {metric} changed: expected {expected:.3f}, "
                f"got {actual:.3f}."
            )
    return curve.sort_values("day").reset_index(drop=True), stats


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


def leg_day_net(
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
            optimiser.SLOT_DAY_IDX[selected_idx[filled]],
            values[filled],
        )
    return result


def add_fixed_portfolio_values(
    values: dict[str, Any],
    selected_idx: np.ndarray,
    net_all: np.ndarray,
    fixed: FixedContext,
) -> None:
    combined_profit = fixed.trade_profit + float(values["all_gross_profit_pct"])
    combined_loss = fixed.trade_loss + float(values["all_gross_loss_pct"])
    combined_day = fixed.day_net + leg_day_net(
        net_all, selected_idx, fixed.day_net.size
    )
    day_profit = float(combined_day[combined_day > 0].sum())
    day_loss = float(-combined_day[combined_day < 0].sum())
    values.update(
        {
            "fixed_all_trade_pf": pf(combined_profit, combined_loss),
            "fixed_all_day_pf": pf(day_profit, day_loss),
            "fixed_all_net_pct": fixed.net_pct + float(values["all_net_pct"]),
            "fixed_all_orders": fixed.orders + int(values["all_orders"]),
            "fixed_all_fills": fixed.fills + int(values["all_trades"]),
        }
    )


def exact_cap_selections(
    signals: pd.DataFrame,
    side: str,
    max_per_side: int,
    force_daily: bool,
) -> list[optimiser.Candidate]:
    return [
        candidate
        for candidate in optimiser.build_selections(
            signals,
            side,
            max_per_side=max_per_side,
            force_daily=force_daily,
        )
        if int(candidate.values["max_per_side"]) == max_per_side
    ]


def prune_for_fixed_v3(
    candidates: list[optimiser.Candidate],
    guards: optimiser.Guards,
    retain_n: int,
) -> list[optimiser.Candidate]:
    base = optimiser.prune_candidates(candidates, guards, retain_n)
    eligible = [
        candidate
        for candidate in candidates
        if int(candidate.values["all_trades"]) >= guards.min_trades
    ]
    by_trade = sorted(
        eligible,
        key=lambda candidate: (
            finite_metric(candidate.values["fixed_all_trade_pf"]),
            finite_metric(candidate.values["fixed_all_day_pf"]),
            float(candidate.values["fixed_all_net_pct"]),
        ),
        reverse=True,
    )[:retain_n]
    by_day = sorted(
        eligible,
        key=lambda candidate: (
            finite_metric(candidate.values["fixed_all_day_pf"]),
            finite_metric(candidate.values["fixed_all_trade_pf"]),
            float(candidate.values["fixed_all_net_pct"]),
        ),
        reverse=True,
    )[:retain_n]
    by_net = sorted(
        eligible,
        key=lambda candidate: (
            float(candidate.values["fixed_all_net_pct"]),
            finite_metric(candidate.values["fixed_all_trade_pf"]),
            finite_metric(candidate.values["fixed_all_day_pf"]),
        ),
        reverse=True,
    )[:retain_n]
    kept: dict[int, optimiser.Candidate] = {}
    for candidate in base + by_trade + by_day + by_net:
        kept[id(candidate)] = candidate
    return list(kept.values())


def optimise_both_sides(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    train_days: set[date],
    test_days: set[date],
    guards: optimiser.Guards,
    cost_bps: float,
    retain_n: int,
    fixed: FixedContext,
) -> tuple[
    dict[str, dict[str, list[optimiser.Candidate]]],
    dict[tuple[float, float], np.ndarray],
]:
    train_mask = np.fromiter(
        (day in train_days for day in optimiser.SLOT_DAYS),
        dtype=bool,
        count=len(optimiser.SLOT_DAYS),
    )
    test_mask = np.fromiter(
        (day in test_days for day in optimiser.SLOT_DAYS),
        dtype=bool,
        count=len(optimiser.SLOT_DAYS),
    )
    all_mask = np.ones(len(optimiser.SLOT_DAYS), dtype=bool)
    side_caps = {"LONG": LONG_MAX, "SHORT": SHORT_MAX}
    selections: dict[str, dict[str, list[optimiser.Candidate]]] = {}
    for side, cap in side_caps.items():
        selections[side] = {
            "FILTERED": exact_cap_selections(
                signals, side, cap, force_daily=False
            ),
            "FORCE_DAILY": exact_cap_selections(
                signals, side, cap, force_daily=True
            ),
        }
        for mode in selections[side]:
            selections[side][mode] = [
                candidate
                for candidate in selections[side][mode]
                if int(train_mask[candidate.selected_idx].sum())
                >= guards.min_trades
            ]
            print(
                f"[{side} {mode}] {len(selections[side][mode]):,} "
                f"exact max-{cap} selections",
                flush=True,
            )

    survivors = {
        side: {mode: [] for mode in selections[side]}
        for side in selections
    }
    survivor_counts = {
        side: {mode: 0 for mode in selections[side]}
        for side in selections
    }
    net_cache: dict[tuple[float, float], np.ndarray] = {}
    brackets = list(itertools.product(optimiser.STOP_PCTS, optimiser.TARGET_PCTS))
    for bracket_no, (stop_pct, target_pct) in enumerate(brackets, start=1):
        net_all = simulator.simulate_bracket(
            signals,
            paths,
            stop_pct=stop_pct,
            target_pct=target_pct,
            cost_bps=cost_bps,
        )
        net_cache[(stop_pct, target_pct)] = net_all
        for side in ("LONG", "SHORT"):
            for mode, mode_selections in selections[side].items():
                for candidate in mode_selections:
                    train = optimiser.score(
                        net_all, candidate.selected_idx, train_mask
                    )
                    if not optimiser.passes_guards(train, guards):
                        continue
                    values = {
                        **candidate.values,
                        "stop_pct": stop_pct,
                        "target_pct": target_pct,
                        **optimiser.prefixed("train", train),
                        **optimiser.prefixed(
                            "test",
                            optimiser.score(
                                net_all, candidate.selected_idx, test_mask
                            ),
                        ),
                        **optimiser.prefixed(
                            "all",
                            optimiser.score(
                                net_all, candidate.selected_idx, all_mask
                            ),
                        ),
                    }
                    add_fixed_portfolio_values(
                        values,
                        candidate.selected_idx,
                        net_all,
                        fixed,
                    )
                    survivors[side][mode].append(
                        optimiser.Candidate(values, candidate.selected_idx)
                    )
                    survivor_counts[side][mode] += 1
                survivors[side][mode] = prune_for_fixed_v3(
                    survivors[side][mode], guards, retain_n
                )
        retained = ", ".join(
            f"{side}-{mode}={len(survivors[side][mode])}"
            for side in ("LONG", "SHORT")
            for mode in ("FILTERED", "FORCE_DAILY")
        )
        print(
            f"[V4] bracket {bracket_no:02d}/{len(brackets)} "
            f"stop={stop_pct:g} target={target_pct:g} retained {retained}",
            flush=True,
        )

    for side in ("LONG", "SHORT"):
        if not any(survivors[side].values()):
            counts = ", ".join(
                f"{mode}={survivor_counts[side][mode]}"
                for mode in survivor_counts[side]
            )
            print(
                f"[{side}] no guarded candidate survived; NONE remains available "
                f"({counts})",
                flush=True,
            )
    return survivors, net_cache


def ranking_frame(
    side_survivors: dict[str, list[optimiser.Candidate]],
) -> pd.DataFrame:
    rows = [
        {"mode": mode, **candidate.values}
        for mode, candidates in side_survivors.items()
        for candidate in candidates
    ]
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(
        ["fixed_all_trade_pf", "fixed_all_day_pf", "fixed_all_net_pct"],
        ascending=False,
    ).reset_index(drop=True)


def leg_choices(
    side: str,
    side_survivors: dict[str, list[optimiser.Candidate]],
    net_cache: dict[tuple[float, float], np.ndarray],
    day_count: int,
) -> list[LegChoice]:
    choices = [
        LegChoice(side=side, mode="NONE", candidate=None, day_net=np.zeros(day_count))
    ]
    seen: set[tuple[float, float, bytes]] = set()
    for mode in ("FILTERED", "FORCE_DAILY"):
        for candidate in side_survivors[mode]:
            stop_pct = float(candidate.values["stop_pct"])
            target_pct = float(candidate.values["target_pct"])
            signature = (
                stop_pct,
                target_pct,
                candidate.selected_idx.astype(np.int32).tobytes(),
            )
            if signature in seen:
                continue
            seen.add(signature)
            net_all = net_cache[(stop_pct, target_pct)]
            choices.append(
                LegChoice(
                    side=side,
                    mode=mode,
                    candidate=candidate,
                    day_net=leg_day_net(
                        net_all, candidate.selected_idx, day_count
                    ),
                )
            )
    return choices


def best_vector_index(
    primary: np.ndarray,
    secondary: np.ndarray,
    tertiary: np.ndarray,
) -> int:
    return int(np.lexsort((tertiary, secondary, primary))[-1])


def choose_combinations(
    long_choices: list[LegChoice],
    short_choices: list[LegChoice],
    fixed: FixedContext,
) -> dict[str, CombinationChoice]:
    short_profit = np.array([choice.profit for choice in short_choices])
    short_loss = np.array([choice.loss for choice in short_choices])
    short_net = np.array([choice.net_pct for choice in short_choices])
    short_orders = np.array([choice.orders for choice in short_choices], dtype=int)
    short_fills = np.array([choice.fills for choice in short_choices], dtype=int)
    short_days = np.stack([choice.day_net for choice in short_choices])
    best: dict[str, tuple[tuple[float, float, float], CombinationChoice]] = {}

    for long_choice in long_choices:
        trade_profit = fixed.trade_profit + long_choice.profit + short_profit
        trade_loss = fixed.trade_loss + long_choice.loss + short_loss
        trade_pf = np.divide(
            trade_profit,
            trade_loss,
            out=np.full_like(trade_profit, np.inf),
            where=trade_loss > 0,
        )
        combined_days = fixed.day_net + long_choice.day_net + short_days
        day_profit = np.where(combined_days > 0, combined_days, 0.0).sum(axis=1)
        day_loss = np.where(combined_days < 0, -combined_days, 0.0).sum(axis=1)
        day_pf = np.divide(
            day_profit,
            day_loss,
            out=np.full_like(day_profit, np.inf),
            where=day_loss > 0,
        )
        net_pct = fixed.net_pct + long_choice.net_pct + short_net
        objective_vectors = {
            "BEST_TRADE_PF": (trade_pf, day_pf, net_pct),
            "BEST_DAY_PF": (day_pf, trade_pf, net_pct),
            "BEST_NET": (net_pct, trade_pf, day_pf),
        }
        for objective_name, vectors in objective_vectors.items():
            short_idx = best_vector_index(*vectors)
            key = tuple(float(vector[short_idx]) for vector in vectors)
            estimated = {
                "trade_pf": float(trade_pf[short_idx]),
                "day_pf": float(day_pf[short_idx]),
                "net_pct": float(net_pct[short_idx]),
                "orders": int(
                    fixed.orders + long_choice.orders + short_orders[short_idx]
                ),
                "fills": int(
                    fixed.fills + long_choice.fills + short_fills[short_idx]
                ),
            }
            choice = CombinationChoice(
                long=long_choice,
                short=short_choices[short_idx],
                estimated=estimated,
            )
            if objective_name not in best or key > best[objective_name][0]:
                best[objective_name] = (key, choice)
    return {name: value[1] for name, value in best.items()}


def empty_leg(days: Iterable[date]) -> pd.DataFrame:
    day_values = list(days)
    return pd.DataFrame(
        {
            "day": day_values,
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


def daily_leg(
    signals: pd.DataFrame,
    choice: LegChoice,
    net_cache: dict[tuple[float, float], np.ndarray],
    model: str,
    all_days: list[date],
) -> pd.DataFrame:
    if choice.candidate is None:
        return empty_leg(all_days)
    bracket = (
        float(choice.values["stop_pct"]),
        float(choice.values["target_pct"]),
    )
    return optimiser.daily_curve(
        signals,
        net_cache[bracket],
        choice.candidate,
        model,
        all_days,
    )


def trade_audit(
    signals: pd.DataFrame,
    choice: LegChoice,
    net_cache: dict[tuple[float, float], np.ndarray],
    model: str,
) -> pd.DataFrame:
    if choice.candidate is None:
        return pd.DataFrame()
    bracket = (
        float(choice.values["stop_pct"]),
        float(choice.values["target_pct"]),
    )
    return optimiser.trade_audit(
        signals, net_cache[bracket], choice.candidate, model
    ).assign(setup_leg=f"09:36_{choice.side}", mode=choice.mode)


def rename_v4_leg(leg: pd.DataFrame, side: str) -> pd.DataFrame:
    prefix = f"v4_{side.lower()}"
    core = leg[
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
    ].copy()
    return core.rename(
        columns={
            column: (
                f"{prefix}_return_pct"
                if column == "net_return_pct"
                else f"{prefix}_{column}"
            )
            for column in core.columns
            if column != "day"
        }
    )


def build_v4_curve(
    locked_v3: pd.DataFrame,
    long_leg: pd.DataFrame,
    short_leg: pd.DataFrame,
    objective_name: str,
) -> pd.DataFrame:
    detail_columns = [
        "day",
        "baseline_long_status",
        "baseline_long_selections",
        "baseline_long_fills",
        "baseline_long_trade_details",
        "baseline_short_status",
        "baseline_short_selections",
        "baseline_short_fills",
        "baseline_short_trade_details",
        "addon_long_status",
        "addon_long_selections",
        "addon_long_fills",
        "addon_long_trade_details",
        "addon_short_status",
        "addon_short_selections",
        "addon_short_fills",
        "addon_short_trade_details",
        "long_selections",
        "long_fills",
        "long_return_pct",
        "long_gross_profit_pct",
        "long_gross_loss_pct",
        "short_selections",
        "short_fills",
        "short_return_pct",
        "short_gross_profit_pct",
        "short_gross_loss_pct",
        "selections",
        "fills",
        "portfolio_net_return_pct",
    ]
    base = locked_v3[detail_columns].copy().rename(
        columns={
            "long_selections": "v3_long_selections",
            "long_fills": "v3_long_fills",
            "long_return_pct": "v3_long_return_pct",
            "long_gross_profit_pct": "v3_long_gross_profit_pct",
            "long_gross_loss_pct": "v3_long_gross_loss_pct",
            "short_selections": "v3_short_selections",
            "short_fills": "v3_short_fills",
            "short_return_pct": "v3_short_return_pct",
            "short_gross_profit_pct": "v3_short_gross_profit_pct",
            "short_gross_loss_pct": "v3_short_gross_loss_pct",
            "selections": "v3_selections",
            "fills": "v3_fills",
            "portfolio_net_return_pct": "v3_net_return_pct",
        }
    )
    for family in ("baseline", "addon"):
        for side in ("long", "short"):
            base[f"{family}_{side}_trade_details"] = base[
                f"{family}_{side}_trade_details"
            ].fillna("")
            base[f"{family}_{side}_status"] = base[
                f"{family}_{side}_status"
            ].fillna("NO_SIGNAL")
    curve = base.merge(
        rename_v4_leg(long_leg, "LONG"),
        on="day",
        how="outer",
        validate="one_to_one",
    ).merge(
        rename_v4_leg(short_leg, "SHORT"),
        on="day",
        how="outer",
        validate="one_to_one",
    )
    curve = curve.sort_values("day").reset_index(drop=True)
    for side in ("long", "short"):
        for column in (
            f"v4_{side}_selected_symbol",
            f"v4_{side}_trade_details",
        ):
            curve[column] = curve[column].fillna("")
        curve[f"v4_{side}_status"] = curve[f"v4_{side}_status"].fillna(
            "NO_SIGNAL"
        )
        for suffix in (
            "return_pct",
            "selections",
            "fills",
            "gross_profit_pct",
            "gross_loss_pct",
        ):
            column = f"v4_{side}_{suffix}"
            curve[column] = pd.to_numeric(
                curve[column], errors="coerce"
            ).fillna(0.0)
        curve[f"{side}_selections"] = (
            curve[f"v3_{side}_selections"] + curve[f"v4_{side}_selections"]
        )
        curve[f"{side}_fills"] = (
            curve[f"v3_{side}_fills"] + curve[f"v4_{side}_fills"]
        )
        curve[f"{side}_return_pct"] = (
            curve[f"v3_{side}_return_pct"] + curve[f"v4_{side}_return_pct"]
        )
        curve[f"{side}_gross_profit_pct"] = (
            curve[f"v3_{side}_gross_profit_pct"]
            + curve[f"v4_{side}_gross_profit_pct"]
        )
        curve[f"{side}_gross_loss_pct"] = (
            curve[f"v3_{side}_gross_loss_pct"]
            + curve[f"v4_{side}_gross_loss_pct"]
        )
    curve["v4_addon_net_return_pct"] = (
        curve["v4_long_return_pct"] + curve["v4_short_return_pct"]
    )
    curve["selections"] = curve["v3_selections"] + (
        curve["v4_long_selections"] + curve["v4_short_selections"]
    )
    curve["fills"] = curve["v3_fills"] + (
        curve["v4_long_fills"] + curve["v4_short_fills"]
    )
    curve["portfolio_net_return_pct"] = (
        curve["v3_net_return_pct"] + curve["v4_addon_net_return_pct"]
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
        trade_profit = float(
            row.long_gross_profit_pct + row.short_gross_profit_pct
        )
        trade_loss = float(
            row.long_gross_loss_pct + row.short_gross_loss_pct
        )
        cum_day_profit += day_profit
        cum_day_loss += day_loss
        cum_trade_profit += trade_profit
        cum_trade_loss += trade_loss
        rows.append(
            {
                "objective": objective_name,
                **row._asdict(),
                "signal_end": "09:25,09:30,09:35",
                "confirmation_end": "09:26,09:31,09:36",
                "day_trade_pf": pf(trade_profit, trade_loss),
                "day_pf": pf(day_profit, day_loss),
                "cumulative_net_pct": cum_day_profit - cum_day_loss,
                "cumulative_day_pf": pf(cum_day_profit, cum_day_loss),
                "cumulative_trade_pf": pf(
                    cum_trade_profit, cum_trade_loss
                ),
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
        "day_pf": pf(day_profit, day_loss),
        "net_pct": float(day_net.sum()),
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
        "max_day_pct": float(day_net.max()) if day_net.size else float("nan"),
    }


def setup_row(
    objective_name: str,
    choice: LegChoice,
    daily: pd.DataFrame,
) -> dict[str, Any]:
    values = choice.values
    return {
        "objective": objective_name,
        "side": choice.side,
        "mode": choice.mode,
        "max_per_side": int(values.get("max_per_side", 0)),
        "orders": int(values.get("all_orders", 0)),
        "fills": int(values.get("all_trades", 0)),
        "pf": values.get("all_pf", np.nan),
        "day_pf": values.get("all_day_pf", np.nan),
        "net_pct": values.get("all_net_pct", 0.0),
        "max_cap_days": int(
            (daily["selections"] == int(values.get("max_per_side", 0))).sum()
        )
        if choice.candidate is not None
        else 0,
        "picker": values.get("picker", ""),
        "price_change_pct": values.get("price_change_pct", np.nan),
        "oi_change_pct": values.get("oi_change_pct", np.nan),
        "volume_ratio": values.get("volume_ratio", np.nan),
        "body_ratio": values.get("body_ratio", np.nan),
        "max_wick_ratio": values.get("max_wick_ratio", np.nan),
        "min_traded_value": values.get("min_traded_value", np.nan),
        "stop_pct": values.get("stop_pct", np.nan),
        "target_pct": values.get("target_pct", np.nan),
    }


def render_report(
    selected: dict[str, CombinationChoice],
    setup_rows: pd.DataFrame,
    summaries: dict[str, dict[str, dict[str, Any]]],
    curves: dict[str, pd.DataFrame],
    current_stats: dict[str, Any],
    split_day: date,
    meta: dict[str, Any],
) -> str:
    chosen = summaries[SELECTED_OBJECTIVE]["ALL"]
    lines = [
        "# FNO EMA/OI V4: Locked V3 + 09:36 Add-On",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Current selected V3 is locked and was not edited or re-optimised.",
        "- Existing V3 timing remains 09:25/09:26 and 09:30/09:31.",
        "- V4 adds one scan ending 09:35 and one 1-minute confirmation ending 09:36; orders activate afterward.",
        f"- V4 09:36 cap: max {LONG_MAX} LONG and max {SHORT_MAX} SHORT contracts.",
        f"- Search: {meta['signal_rows']} confirmed 09:35 candidates across "
        f"{meta['signal_days']} signal sessions; {meta['long_choices']} unique guarded LONG choices "
        f"x {meta['short_choices']} unique guarded SHORT choices.",
        f"- Cost: {meta['cost_bps']:g} bps round trip.",
        f"- Official V4 selection: `{SELECTED_OBJECTIVE}`.",
        "- Full-history winners are descriptive and in-sample, not honest out-of-sample validation.",
        "",
        "## Chosen V4",
        "",
        "| Selection | 09:36 LONG max | 09:36 SHORT max | Orders | Fills | Trade PF | Day PF | Net % | Uplift vs V3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {SELECTED_OBJECTIVE} | {int(selected[SELECTED_OBJECTIVE].long.values.get('max_per_side', 0))} | "
        f"{int(selected[SELECTED_OBJECTIVE].short.values.get('max_per_side', 0))} | "
        f"{chosen['orders']} | {chosen['fills']} | {fmt(chosen['trade_pf'])} | "
        f"{fmt(chosen['day_pf'])} | {chosen['net_pct']:+.3f} | "
        f"{chosen['net_pct'] - current_stats['net_pct']:+.3f} |",
        "",
        "## Selected 09:36 Setups",
        "",
        "| Objective | Side | Mode | Max | Orders/Fills | Max-cap days | Leg PF | Leg day PF | Leg net % | Picker | Price | OI | Vol | Body | Wick | Value | Stop | Target |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for side in ("LONG", "SHORT"):
            row = setup_rows.loc[
                setup_rows["objective"].eq(objective_name)
                & setup_rows["side"].eq(side)
            ].iloc[0]
            lines.append(
                f"| {objective_name} | {side} | {row['mode']} | {int(row['max_per_side'])} | "
                f"{int(row['orders'])}/{int(row['fills'])} | {int(row['max_cap_days'])} | "
                f"{fmt(row['pf'])} | {fmt(row['day_pf'])} | {float(row['net_pct']):+.3f} | "
                f"{row['picker']} | {fmt(row['price_change_pct'])} | {fmt(row['oi_change_pct'])} | "
                f"{fmt(row['volume_ratio'])} | {fmt(row['body_ratio'])} | "
                f"{fmt(row['max_wick_ratio'])} | {fmt(row['min_traded_value'], 0)} | "
                f"{fmt(row['stop_pct'])} | {fmt(row['target_pct'])} |"
            )

    lines += [
        "",
        "## Full Portfolio Comparison",
        "",
        "| Portfolio | Orders | Fills | Trade PF | Day PF | Net % | Uplift vs V3 | Positive | Negative | Flat |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| Current selected V3 | {current_stats['orders']} | {current_stats['fills']} | "
        f"{fmt(current_stats['trade_pf'])} | {fmt(current_stats['day_pf'])} | "
        f"{current_stats['net_pct']:+.3f} | +0.000 | {current_stats['positive_days']} | "
        f"{current_stats['negative_days']} | {current_stats['flat_days']} |",
    ]
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        chosen_label = " (CHOSEN)" if objective_name == SELECTED_OBJECTIVE else ""
        lines.append(
            f"| V4 {objective_name}{chosen_label} | {stats['orders']} | {stats['fills']} | "
            f"{fmt(stats['trade_pf'])} | {fmt(stats['day_pf'])} | {stats['net_pct']:+.3f} | "
            f"{stats['net_pct'] - current_stats['net_pct']:+.3f} | {stats['positive_days']} | "
            f"{stats['negative_days']} | {stats['flat_days']} |"
        )

    trade = summaries["BEST_TRADE_PF"]["ALL"]
    day = summaries["BEST_DAY_PF"]["ALL"]
    net = summaries["BEST_NET"]["ALL"]
    lines += [
        "",
        "## Verdict",
        "",
        f"- Best trade PF changes from {current_stats['trade_pf']:.3f} to {trade['trade_pf']:.3f} "
        f"({trade['trade_pf'] - current_stats['trade_pf']:+.3f}) and cumulative return by "
        f"{trade['net_pct'] - current_stats['net_pct']:+.3f}%.",
        f"- Best day PF changes from {current_stats['day_pf']:.3f} to {day['day_pf']:.3f} "
        f"({day['day_pf'] - current_stats['day_pf']:+.3f}).",
        f"- Highest cumulative return is {net['net_pct']:+.3f}%, a change of "
        f"{net['net_pct'] - current_stats['net_pct']:+.3f}% versus current V3.",
        "- Current V3 remains protected; V4 is written only to new files.",
        "",
        "## Train/Test/All",
        "",
        "| Objective | Period | Orders | Fills | Trade PF | Day PF | Net % | Positive | Negative | Flat |",
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
        "| Day | Period | 09:26 LONG | 09:26 SHORT | 09:31 LONG | 09:31 SHORT | "
        "09:36 LONG | 09:36 SHORT | V3 % | 09:36 % | Total O/F | Total % | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for objective_name, curve in curves.items():
        lines += ["", f"## {objective_name} Day-Wise Table", "", columns, divider]
        for row in curve.itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            v2_long = row.baseline_long_trade_details or row.baseline_long_status
            v2_short = row.baseline_short_trade_details or row.baseline_short_status
            v3_long = row.addon_long_trade_details or row.addon_long_status
            v3_short = row.addon_short_trade_details or row.addon_short_status
            v4_long = row.v4_long_trade_details or row.v4_long_status
            v4_short = row.v4_short_trade_details or row.v4_short_status
            lines.append(
                f"| {row.day} | {period} | {v2_long} | {v2_short} | {v3_long} | {v3_short} | "
                f"{v4_long} | {v4_short} | {fmt_signed(row.v3_net_return_pct)} | "
                f"{fmt_signed(row.v4_addon_net_return_pct)} | {int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {fmt_signed(row.cumulative_net_pct)} | "
                f"{fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
            )

    lines += [
        "",
        "## Files And Command",
        "",
        f"- Report: `{REPORT_PATH}`",
        f"- Official selected V4 daily CSV: `{SELECTED_DAILY_OUTPUT_PATH}`",
        f"- All objective daily CSV: `{DAILY_OUTPUT_PATH}`",
        f"- Selected 09:36 trade audit: `{AUDIT_OUTPUT_PATH}`",
        f"- Selected setup table: `{SETUPS_OUTPUT_PATH}`",
        f"- Ranked LONG candidates: `{RANKED_LONG_OUTPUT_PATH}`",
        f"- Ranked SHORT candidates: `{RANKED_SHORT_OUTPUT_PATH}`",
        "- Run: `cmd /c eqidv2\\bat\\run_fno_oi_ema_confirm_0925_0930_0935_v4.bat`",
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
    parser.add_argument("--retain-n", type=int, default=400)
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
    locked_v3, current_stats = load_current_v3()
    all_days = sorted(set(locked_v3["day"]))
    all_day_set = set(all_days)
    split_day = pd.Timestamp(args.split_day).date()
    train_days = {day for day in all_days if day < split_day}
    test_days = all_day_set - train_days
    fixed = fixed_context(locked_v3)

    signals_all, paths = signal_cache.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals_all = signals_all.copy()
    signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
    if sorted(set(signals_all["day"])) != all_days:
        raise RuntimeError("Signal cache sessions do not match current V3.")
    signals = signals_all.loc[
        signals_all["hhmm_int"].eq(V4_SIGNAL_SLOT)
    ].copy()
    signals = signals.sort_values(
        ["day", "hhmm_int", "tradingsymbol", "sid"]
    ).reset_index(drop=True)
    if signals.empty:
        raise RuntimeError("No cached 09:35 signals found.")
    setup_timing_globals(signals, all_days)

    guards = optimiser.Guards(
        min_trades=args.min_trades,
        min_day_win=args.min_day_win,
        max_top_profit_share=args.max_top_profit_share,
    )
    print(
        f"[DATA] {len(signals):,} confirmed 09:35 candidates across "
        f"{signals['day'].nunique()} signal sessions | calendar {len(all_days)} | "
        f"train {len(train_days)} | test {len(test_days)}",
        flush=True,
    )
    survivors, net_cache = optimise_both_sides(
        signals,
        paths,
        train_days,
        test_days,
        guards,
        args.cost_bps,
        args.retain_n,
        fixed,
    )
    common.atomic_write_csv(
        ranking_frame(survivors["LONG"]), RANKED_LONG_OUTPUT_PATH
    )
    common.atomic_write_csv(
        ranking_frame(survivors["SHORT"]), RANKED_SHORT_OUTPUT_PATH
    )

    long_choices = leg_choices(
        "LONG", survivors["LONG"], net_cache, len(all_days)
    )
    short_choices = leg_choices(
        "SHORT", survivors["SHORT"], net_cache, len(all_days)
    )
    print(
        f"[COMBINE] {len(long_choices):,} unique LONG choices x "
        f"{len(short_choices):,} unique SHORT choices",
        flush=True,
    )
    selected = choose_combinations(long_choices, short_choices, fixed)

    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    setup_parts: list[dict[str, Any]] = []
    audit_parts: list[pd.DataFrame] = []
    for objective_name, combination in selected.items():
        long_daily = daily_leg(
            signals, combination.long, net_cache, objective_name, all_days
        )
        short_daily = daily_leg(
            signals, combination.short, net_cache, objective_name, all_days
        )
        if (long_daily["selections"] > LONG_MAX).any():
            raise AssertionError("V4 exceeded its max-one LONG cap.")
        if (short_daily["selections"] > SHORT_MAX).any():
            raise AssertionError("V4 exceeded its max-two SHORT cap.")
        curve = build_v4_curve(
            locked_v3, long_daily, short_daily, objective_name
        )
        all_stats = period_stats(curve, all_day_set)
        for metric in ("trade_pf", "day_pf", "net_pct"):
            if abs(
                float(all_stats[metric])
                - float(combination.estimated[metric])
            ) > 1e-9:
                raise AssertionError(
                    f"V4 {objective_name} {metric} does not reconcile."
                )
        curves[objective_name] = curve
        summaries[objective_name] = {
            "TRAIN": period_stats(curve, train_days),
            "TEST": period_stats(curve, test_days),
            "ALL": all_stats,
        }
        setup_parts.extend(
            [
                setup_row(
                    objective_name, combination.long, long_daily
                ),
                setup_row(
                    objective_name, combination.short, short_daily
                ),
            ]
        )
        for choice in (combination.long, combination.short):
            audit = trade_audit(
                signals, choice, net_cache, objective_name
            )
            if not audit.empty:
                audit_parts.append(audit)

    setups = pd.DataFrame(setup_parts)
    common.atomic_write_csv(setups, SETUPS_OUTPUT_PATH)
    common.atomic_write_csv(
        pd.concat(curves.values(), ignore_index=True), DAILY_OUTPUT_PATH
    )
    common.atomic_write_csv(
        curves[SELECTED_OBJECTIVE], SELECTED_DAILY_OUTPUT_PATH
    )
    common.atomic_write_csv(
        pd.concat(audit_parts, ignore_index=True)
        if audit_parts
        else pd.DataFrame(),
        AUDIT_OUTPUT_PATH,
    )
    meta = {
        "signal_rows": len(signals),
        "signal_days": int(signals["day"].nunique()),
        "long_choices": len(long_choices),
        "short_choices": len(short_choices),
        "cost_bps": args.cost_bps,
    }
    common.atomic_write_text(
        REPORT_PATH,
        render_report(
            selected,
            setups,
            summaries,
            curves,
            current_stats,
            split_day,
            meta,
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
