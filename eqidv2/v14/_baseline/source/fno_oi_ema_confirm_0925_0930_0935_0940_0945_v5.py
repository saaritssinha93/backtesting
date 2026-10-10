"""Backtest current V5 with cash-equity prices and mapped futures OI.

The active V2-through-V5 setup book is replayed as one coherent strategy. NFO
files provide only OI and OI percentage change. NSE equities provide 5-minute
price, volume and indicators, causally aggregated from five historical
end-labelled 1-minute rows. The same equity minute data supplies confirmation,
entry and exit paths.
"""

from __future__ import annotations

import argparse
import itertools
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_0925_0930_0935_v4 as current_v4
import fno_oi_ema_confirm_0925_0930_pf_v3 as optimiser
import fno_oi_ema_confirm_optimize as signal_cache
import fno_oi_hybrid_data as hybrid
import fno_v5_hybrid_backtest as hybrid_replay
import fno_v5_hybrid_optimize as hybrid_optimiser


RESULT_DIR = common.FNO_ROOT / "strategy_research"
REPORT_PATH = (
    common.LATEST_DIR
    / "latest_fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5_best_full.md"
)
DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_best_full_daily.csv"
)
SELECTED_DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_daily.csv"
)
AUDIT_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_trades.csv"
)
SETUPS_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_setups.csv"
)
ROLLING_AUDIT_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_latest_replay_trades.csv"
)
ROLLING_SETUPS_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_latest_replay_setups.csv"
)
FULL_HISTORY_REPORT_PATH = (
    common.LATEST_DIR
    / "latest_fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5_full_history_optimised.md"
)
FULL_HISTORY_DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_all_objectives_daily.csv"
)
FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_daily.csv"
)
FULL_HISTORY_AUDIT_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_trades.csv"
)
FULL_HISTORY_SETUPS_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_setups.csv"
)
FULL_HISTORY_RANKED_OUTPUT_PATH = (
    RESULT_DIR
    / "ema_confirm_0925_0930_0935_0940_0945_v5_full_history_ranked_portfolios.csv"
)

V5_WINDOWS = {940: 941, 945: 946}
LONG_MAX = 1
SHORT_MAX = 2
CURRENT_V4_EXPECTED = {
    "trade_pf": 3.811,
    "day_pf": 11.784,
    "net_pct": 88.260,
}
SELECTED_OBJECTIVE = hybrid_replay.OBJECTIVE
FULL_HISTORY_SELECTED_OBJECTIVE = "BEST_TRADE_PF"
MODE_CURRENT_REPLAY = "current-replay"
MODE_FULL_HISTORY = "full-history"
OBJECTIVES = {
    "BEST_TRADE_PF": "trade_pf",
    "BEST_DAY_PF": "day_pf",
    "BEST_NET": "net_pct",
}


def ranked_path(slot: int, side: str) -> Path:
    return (
        RESULT_DIR
        / f"ema_confirm_v5_full_history_{slot:04d}_ranked_{side}.csv"
    )


def portfolio_path(slot: int) -> Path:
    return (
        RESULT_DIR
        / f"ema_confirm_v5_full_history_{slot:04d}_retained_portfolios.csv"
    )


@dataclass
class WindowSearch:
    slot: int
    signals: pd.DataFrame
    survivors: dict[str, dict[str, list[optimiser.Candidate]]]
    net_cache: dict[tuple[float, float], np.ndarray]
    long_choices: list[current_v4.LegChoice]
    short_choices: list[current_v4.LegChoice]


@dataclass
class WindowChoice:
    slot: int
    long: current_v4.LegChoice
    short: current_v4.LegChoice
    day_net: np.ndarray

    @property
    def profit(self) -> float:
        return self.long.profit + self.short.profit

    @property
    def loss(self) -> float:
        return self.long.loss + self.short.loss

    @property
    def net_pct(self) -> float:
        return self.long.net_pct + self.short.net_pct

    @property
    def orders(self) -> int:
        return self.long.orders + self.short.orders

    @property
    def fills(self) -> int:
        return self.long.fills + self.short.fills


@dataclass
class V5Choice:
    first: WindowChoice
    second: WindowChoice
    estimated: dict[str, Any]


def pf(profit: float, loss: float) -> float:
    return current_v4.pf(profit, loss)


def fmt(value: Any, digits: int = 3) -> str:
    return current_v4.fmt(value, digits)


def fmt_signed(value: Any, digits: int = 3) -> str:
    return current_v4.fmt_signed(value, digits)


def pf_array(profit: np.ndarray, loss: np.ndarray) -> np.ndarray:
    result = np.full(profit.shape, np.nan, dtype=float)
    np.divide(profit, loss, out=result, where=loss > 0)
    result[(loss <= 0) & (profit > 0)] = np.inf
    return result


def clean_rank_value(values: np.ndarray) -> np.ndarray:
    return np.nan_to_num(
        values,
        nan=-np.inf,
        posinf=np.finfo(float).max,
        neginf=-np.inf,
    )


def top_lex_indices(
    primary: np.ndarray,
    secondary: np.ndarray,
    tertiary: np.ndarray,
    retain_n: int,
) -> np.ndarray:
    order = np.lexsort(
        (
            clean_rank_value(tertiary),
            clean_rank_value(secondary),
            clean_rank_value(primary),
        )
    )
    return order[-min(retain_n, order.size) :]


def setup_slot_globals(
    signals: pd.DataFrame,
    all_days: list[date],
    slot: int,
) -> None:
    confirmation = V5_WINDOWS[slot]
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
    optimiser.SIGNAL_END_LABEL = optimiser.format_hhmm(slot)
    optimiser.CONFIRMATION_END_LABEL = optimiser.format_hhmm(confirmation)
    optimiser.CONFIRMATION_ENDS = {
        **optimiser.CONFIRMATION_ENDS,
        **V5_WINDOWS,
    }


def load_current_v4() -> tuple[pd.DataFrame, dict[str, Any]]:
    path = current_v4.SELECTED_DAILY_OUTPUT_PATH
    if not path.exists():
        raise FileNotFoundError(f"Missing current selected V4 curve: {path}")
    curve = pd.read_csv(path)
    curve["day"] = pd.to_datetime(curve["day"]).dt.date
    if curve["day"].duplicated().any():
        raise RuntimeError("Current V4 curve contains duplicate sessions.")
    stats = current_v4.period_stats(curve, set(curve["day"]))
    for metric, expected in CURRENT_V4_EXPECTED.items():
        actual = float(stats[metric])
        if abs(actual - expected) > 0.0015:
            raise AssertionError(
                f"Current V4 {metric} changed: expected {expected:.3f}, "
                f"got {actual:.3f}."
            )
    return curve.sort_values("day").reset_index(drop=True), stats


def optimise_window(
    slot: int,
    signals_all: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    all_days: list[date],
    train_days: set[date],
    test_days: set[date],
    guards: optimiser.Guards,
    cost_bps: float,
    retain_n: int,
    fixed: current_v4.FixedContext,
) -> WindowSearch:
    signals = signals_all.loc[signals_all["hhmm_int"].eq(slot)].copy()
    signals = signals.sort_values(
        ["day", "hhmm_int", "tradingsymbol", "sid"]
    ).reset_index(drop=True)
    if signals.empty:
        raise RuntimeError(f"No cached {optimiser.format_hhmm(slot)} signals found.")
    setup_slot_globals(signals, all_days, slot)
    print(
        f"[WINDOW {optimiser.format_hhmm(slot)}] {len(signals):,} candidates "
        f"across {signals['day'].nunique()} signal sessions",
        flush=True,
    )
    survivors, net_cache = current_v4.optimise_both_sides(
        signals,
        paths,
        train_days,
        test_days,
        guards,
        cost_bps,
        retain_n,
        fixed,
    )
    common.atomic_write_csv(
        current_v4.ranking_frame(survivors["LONG"]),
        ranked_path(slot, "LONG"),
    )
    common.atomic_write_csv(
        current_v4.ranking_frame(survivors["SHORT"]),
        ranked_path(slot, "SHORT"),
    )
    long_choices = current_v4.leg_choices(
        "LONG", survivors["LONG"], net_cache, len(all_days)
    )
    short_choices = current_v4.leg_choices(
        "SHORT", survivors["SHORT"], net_cache, len(all_days)
    )
    print(
        f"[WINDOW {optimiser.format_hhmm(slot)}] "
        f"{len(long_choices):,} unique LONG x {len(short_choices):,} unique SHORT",
        flush=True,
    )
    return WindowSearch(
        slot=slot,
        signals=signals,
        survivors=survivors,
        net_cache=net_cache,
        long_choices=long_choices,
        short_choices=short_choices,
    )


def window_metrics(
    choice: WindowChoice,
    fixed: current_v4.FixedContext,
) -> dict[str, Any]:
    day_profit = float(choice.day_net[choice.day_net > 0].sum())
    day_loss = float(-choice.day_net[choice.day_net < 0].sum())
    combined_day = fixed.day_net + choice.day_net
    fixed_day_profit = float(combined_day[combined_day > 0].sum())
    fixed_day_loss = float(-combined_day[combined_day < 0].sum())
    return {
        "slot": choice.slot,
        "long_mode": choice.long.mode,
        "long_max": int(choice.long.values.get("max_per_side", 0)),
        "long_orders": choice.long.orders,
        "long_fills": choice.long.fills,
        "short_mode": choice.short.mode,
        "short_max": int(choice.short.values.get("max_per_side", 0)),
        "short_orders": choice.short.orders,
        "short_fills": choice.short.fills,
        "orders": choice.orders,
        "fills": choice.fills,
        "standalone_trade_pf": pf(choice.profit, choice.loss),
        "standalone_day_pf": pf(day_profit, day_loss),
        "standalone_net_pct": choice.net_pct,
        "fixed_trade_pf": pf(
            fixed.trade_profit + choice.profit,
            fixed.trade_loss + choice.loss,
        ),
        "fixed_day_pf": pf(fixed_day_profit, fixed_day_loss),
        "fixed_net_pct": fixed.net_pct + choice.net_pct,
    }


def retain_window_portfolios(
    search: WindowSearch,
    fixed: current_v4.FixedContext,
    retain_n: int,
) -> list[WindowChoice]:
    longs = search.long_choices
    shorts = search.short_choices
    n_long = len(longs)
    n_short = len(shorts)
    pair_count = n_long * n_short
    fixed_trade_pf = np.empty(pair_count)
    fixed_day_pf = np.empty(pair_count)
    fixed_net_pct = np.empty(pair_count)
    standalone_trade_pf = np.empty(pair_count)
    standalone_day_pf = np.empty(pair_count)
    standalone_net_pct = np.empty(pair_count)

    short_profit = np.array([choice.profit for choice in shorts])
    short_loss = np.array([choice.loss for choice in shorts])
    short_net = np.array([choice.net_pct for choice in shorts])
    short_days = np.stack([choice.day_net for choice in shorts])
    for long_idx, long_choice in enumerate(longs):
        start = long_idx * n_short
        stop = start + n_short
        pair_profit = long_choice.profit + short_profit
        pair_loss = long_choice.loss + short_loss
        pair_net = long_choice.net_pct + short_net
        pair_days = long_choice.day_net + short_days
        day_profit = np.where(pair_days > 0, pair_days, 0.0).sum(axis=1)
        day_loss = np.where(pair_days < 0, -pair_days, 0.0).sum(axis=1)
        combined_days = fixed.day_net + pair_days
        combined_day_profit = np.where(
            combined_days > 0, combined_days, 0.0
        ).sum(axis=1)
        combined_day_loss = np.where(
            combined_days < 0, -combined_days, 0.0
        ).sum(axis=1)
        standalone_trade_pf[start:stop] = pf_array(pair_profit, pair_loss)
        standalone_day_pf[start:stop] = pf_array(day_profit, day_loss)
        standalone_net_pct[start:stop] = pair_net
        fixed_trade_pf[start:stop] = pf_array(
            fixed.trade_profit + pair_profit,
            fixed.trade_loss + pair_loss,
        )
        fixed_day_pf[start:stop] = pf_array(
            combined_day_profit, combined_day_loss
        )
        fixed_net_pct[start:stop] = fixed.net_pct + pair_net

    rankings = (
        (fixed_trade_pf, fixed_day_pf, fixed_net_pct),
        (fixed_day_pf, fixed_trade_pf, fixed_net_pct),
        (fixed_net_pct, fixed_trade_pf, fixed_day_pf),
        (standalone_trade_pf, standalone_day_pf, standalone_net_pct),
        (standalone_day_pf, standalone_trade_pf, standalone_net_pct),
        (standalone_net_pct, standalone_trade_pf, standalone_day_pf),
    )
    kept = {0}
    for ranking in rankings:
        kept.update(top_lex_indices(*ranking, retain_n).tolist())

    choices: list[WindowChoice] = []
    seen: set[tuple[float, float, int, int, bytes]] = set()
    for flat_idx in sorted(kept):
        long_idx, short_idx = divmod(flat_idx, n_short)
        long_choice = longs[long_idx]
        short_choice = shorts[short_idx]
        day_net = long_choice.day_net + short_choice.day_net
        signature = (
            round(long_choice.profit + short_choice.profit, 12),
            round(long_choice.loss + short_choice.loss, 12),
            long_choice.orders + short_choice.orders,
            long_choice.fills + short_choice.fills,
            np.round(day_net, 12).tobytes(),
        )
        if signature in seen:
            continue
        seen.add(signature)
        choices.append(
            WindowChoice(
                slot=search.slot,
                long=long_choice,
                short=short_choice,
                day_net=day_net,
            )
        )
    common.atomic_write_csv(
        pd.DataFrame([window_metrics(choice, fixed) for choice in choices]),
        portfolio_path(search.slot),
    )
    print(
        f"[WINDOW {optimiser.format_hhmm(search.slot)}] retained "
        f"{len(choices):,} unique LONG+SHORT portfolios from {pair_count:,} pairs",
        flush=True,
    )
    return choices


def choose_v5(
    first_choices: list[WindowChoice],
    second_choices: list[WindowChoice],
    fixed: current_v4.FixedContext,
) -> dict[str, V5Choice]:
    second_profit = np.array([choice.profit for choice in second_choices])
    second_loss = np.array([choice.loss for choice in second_choices])
    second_net = np.array([choice.net_pct for choice in second_choices])
    second_orders = np.array([choice.orders for choice in second_choices], dtype=int)
    second_fills = np.array([choice.fills for choice in second_choices], dtype=int)
    second_days = np.stack([choice.day_net for choice in second_choices])
    best: dict[str, tuple[tuple[float, float, float], V5Choice]] = {}

    for first in first_choices:
        trade_profit = fixed.trade_profit + first.profit + second_profit
        trade_loss = fixed.trade_loss + first.loss + second_loss
        trade_pf = pf_array(trade_profit, trade_loss)
        combined_days = fixed.day_net + first.day_net + second_days
        day_profit = np.where(combined_days > 0, combined_days, 0.0).sum(axis=1)
        day_loss = np.where(combined_days < 0, -combined_days, 0.0).sum(axis=1)
        day_pf = pf_array(day_profit, day_loss)
        net_pct = fixed.net_pct + first.net_pct + second_net
        objective_vectors = {
            "BEST_TRADE_PF": (trade_pf, day_pf, net_pct),
            "BEST_DAY_PF": (day_pf, trade_pf, net_pct),
            "BEST_NET": (net_pct, trade_pf, day_pf),
        }
        for objective_name, vectors in objective_vectors.items():
            second_idx = current_v4.best_vector_index(*vectors)
            key = tuple(float(vector[second_idx]) for vector in vectors)
            choice = V5Choice(
                first=first,
                second=second_choices[second_idx],
                estimated={
                    "trade_pf": float(trade_pf[second_idx]),
                    "day_pf": float(day_pf[second_idx]),
                    "net_pct": float(net_pct[second_idx]),
                    "orders": int(
                        fixed.orders + first.orders + second_orders[second_idx]
                    ),
                    "fills": int(
                        fixed.fills + first.fills + second_fills[second_idx]
                    ),
                },
            )
            if objective_name not in best or key > best[objective_name][0]:
                best[objective_name] = (key, choice)
    return {name: value[1] for name, value in best.items()}


def daily_leg(
    search: WindowSearch,
    choice: current_v4.LegChoice,
    model: str,
    all_days: list[date],
) -> pd.DataFrame:
    setup_slot_globals(search.signals, all_days, search.slot)
    return current_v4.daily_leg(
        search.signals,
        choice,
        search.net_cache,
        model,
        all_days,
    )


def trade_audit(
    search: WindowSearch,
    choice: current_v4.LegChoice,
    model: str,
    all_days: list[date],
) -> pd.DataFrame:
    setup_slot_globals(search.signals, all_days, search.slot)
    audit = current_v4.trade_audit(
        search.signals,
        choice,
        search.net_cache,
        model,
    )
    if audit.empty:
        return audit
    confirmation = V5_WINDOWS[search.slot]
    audit["setup_leg"] = (
        f"{optimiser.format_hhmm(confirmation)}_{choice.side}"
    )
    audit["v5_signal_slot"] = optimiser.format_hhmm(search.slot)
    audit["v5_confirmation_end"] = optimiser.format_hhmm(confirmation)
    return audit


def rename_leg(
    leg: pd.DataFrame,
    confirmation: int,
    side: str,
) -> pd.DataFrame:
    prefix = f"v5_{confirmation:04d}_{side.lower()}"
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


def build_v5_curve(
    locked_v4: pd.DataFrame,
    legs: dict[tuple[int, str], pd.DataFrame],
    objective_name: str,
) -> pd.DataFrame:
    detail_columns = [
        "day",
        "baseline_long_status",
        "baseline_long_trade_details",
        "baseline_short_status",
        "baseline_short_trade_details",
        "addon_long_status",
        "addon_long_trade_details",
        "addon_short_status",
        "addon_short_trade_details",
        "v4_long_status",
        "v4_long_trade_details",
        "v4_short_status",
        "v4_short_trade_details",
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
    base = locked_v4[detail_columns].copy().rename(
        columns={
            "long_selections": "v4_total_long_selections",
            "long_fills": "v4_total_long_fills",
            "long_return_pct": "v4_total_long_return_pct",
            "long_gross_profit_pct": "v4_total_long_gross_profit_pct",
            "long_gross_loss_pct": "v4_total_long_gross_loss_pct",
            "short_selections": "v4_total_short_selections",
            "short_fills": "v4_total_short_fills",
            "short_return_pct": "v4_total_short_return_pct",
            "short_gross_profit_pct": "v4_total_short_gross_profit_pct",
            "short_gross_loss_pct": "v4_total_short_gross_loss_pct",
            "selections": "v4_total_selections",
            "fills": "v4_total_fills",
            "portfolio_net_return_pct": "v4_net_return_pct",
        }
    )
    for family in ("baseline", "addon", "v4"):
        for side in ("long", "short"):
            base[f"{family}_{side}_trade_details"] = base[
                f"{family}_{side}_trade_details"
            ].fillna("")
            base[f"{family}_{side}_status"] = base[
                f"{family}_{side}_status"
            ].fillna("NO_SIGNAL")

    curve = base
    for slot, confirmation in V5_WINDOWS.items():
        for side in ("LONG", "SHORT"):
            curve = curve.merge(
                rename_leg(legs[(slot, side)], confirmation, side),
                on="day",
                how="outer",
                validate="one_to_one",
            )
    curve = curve.sort_values("day").reset_index(drop=True)

    for confirmation in V5_WINDOWS.values():
        for side in ("long", "short"):
            prefix = f"v5_{confirmation:04d}_{side}"
            for column in (
                f"{prefix}_selected_symbol",
                f"{prefix}_trade_details",
            ):
                curve[column] = curve[column].fillna("")
            curve[f"{prefix}_status"] = curve[f"{prefix}_status"].fillna(
                "NO_SIGNAL"
            )
            for suffix in (
                "return_pct",
                "selections",
                "fills",
                "gross_profit_pct",
                "gross_loss_pct",
            ):
                column = f"{prefix}_{suffix}"
                curve[column] = pd.to_numeric(
                    curve[column], errors="coerce"
                ).fillna(0.0)

    for side in ("long", "short"):
        new_selections = sum(
            curve[f"v5_{confirmation:04d}_{side}_selections"]
            for confirmation in V5_WINDOWS.values()
        )
        new_fills = sum(
            curve[f"v5_{confirmation:04d}_{side}_fills"]
            for confirmation in V5_WINDOWS.values()
        )
        new_return = sum(
            curve[f"v5_{confirmation:04d}_{side}_return_pct"]
            for confirmation in V5_WINDOWS.values()
        )
        new_profit = sum(
            curve[f"v5_{confirmation:04d}_{side}_gross_profit_pct"]
            for confirmation in V5_WINDOWS.values()
        )
        new_loss = sum(
            curve[f"v5_{confirmation:04d}_{side}_gross_loss_pct"]
            for confirmation in V5_WINDOWS.values()
        )
        curve[f"{side}_selections"] = (
            curve[f"v4_total_{side}_selections"] + new_selections
        )
        curve[f"{side}_fills"] = curve[f"v4_total_{side}_fills"] + new_fills
        curve[f"{side}_return_pct"] = (
            curve[f"v4_total_{side}_return_pct"] + new_return
        )
        curve[f"{side}_gross_profit_pct"] = (
            curve[f"v4_total_{side}_gross_profit_pct"] + new_profit
        )
        curve[f"{side}_gross_loss_pct"] = (
            curve[f"v4_total_{side}_gross_loss_pct"] + new_loss
        )

    curve["v5_addon_net_return_pct"] = sum(
        curve[f"v5_{confirmation:04d}_{side}_return_pct"]
        for confirmation in V5_WINDOWS.values()
        for side in ("long", "short")
    )
    curve["selections"] = curve["v4_total_selections"] + sum(
        curve[f"v5_{confirmation:04d}_{side}_selections"]
        for confirmation in V5_WINDOWS.values()
        for side in ("long", "short")
    )
    curve["fills"] = curve["v4_total_fills"] + sum(
        curve[f"v5_{confirmation:04d}_{side}_fills"]
        for confirmation in V5_WINDOWS.values()
        for side in ("long", "short")
    )
    curve["portfolio_net_return_pct"] = (
        curve["v4_net_return_pct"] + curve["v5_addon_net_return_pct"]
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
                "signal_end": "09:25,09:30,09:35,09:40,09:45",
                "confirmation_end": "09:26,09:31,09:36,09:41,09:46",
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


def setup_row(
    objective_name: str,
    slot: int,
    choice: current_v4.LegChoice,
    daily: pd.DataFrame,
) -> dict[str, Any]:
    row = current_v4.setup_row(objective_name, choice, daily)
    row.update(
        {
            "signal_end": optimiser.format_hhmm(slot),
            "entry_end": optimiser.format_hhmm(V5_WINDOWS[slot]),
        }
    )
    return row


def render_report(
    selected: dict[str, V5Choice],
    setups: pd.DataFrame,
    summaries: dict[str, dict[str, dict[str, Any]]],
    curves: dict[str, pd.DataFrame],
    current_stats: dict[str, Any],
    split_day: date,
    meta: dict[str, Any],
) -> str:
    chosen = summaries[FULL_HISTORY_SELECTED_OBJECTIVE]["ALL"]
    lines = [
        "# FNO EMA/OI V5: Locked V4 + 09:41/09:46 Add-Ons",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        "- Current selected V4 is locked and was not edited or re-optimised.",
        "- V4 retains entries after confirmations ending 09:26, 09:31, and 09:36.",
        "- V5 adds scans ending 09:40 and 09:45, confirmed at 09:41 and 09:46; orders activate afterward.",
        f"- Each new window permits max {LONG_MAX} LONG and max {SHORT_MAX} SHORT contracts.",
        f"- 09:41 beam: {meta['first_choices']} retained portfolios; 09:46 beam: "
        f"{meta['second_choices']} retained portfolios; joint combinations are scored on all 52 sessions.",
        f"- Cost: {meta['cost_bps']:g} bps round trip.",
        f"- Full-history research selection: `{FULL_HISTORY_SELECTED_OBJECTIVE}`.",
        "- Full-history winners are descriptive and in-sample, not honest out-of-sample validation.",
        "",
        "## Chosen V5",
        "",
        "| Selection | Orders | Fills | Trade PF | Day PF | Net % | Uplift vs V4 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| {FULL_HISTORY_SELECTED_OBJECTIVE} | {chosen['orders']} | {chosen['fills']} | "
        f"{fmt(chosen['trade_pf'])} | {fmt(chosen['day_pf'])} | "
        f"{chosen['net_pct']:+.3f} | {chosen['net_pct'] - current_stats['net_pct']:+.3f} |",
        "",
        "## Selected New Setups",
        "",
        "| Objective | Entry | Side | Mode | Max | Orders/Fills | Max-cap days | Leg PF | Leg day PF | Leg net % | Picker | Price | OI | Vol | Body | Wick | Value | Stop | Target |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for objective_name in OBJECTIVES:
        for entry in ("09:41", "09:46"):
            for side in ("LONG", "SHORT"):
                row = setups.loc[
                    setups["objective"].eq(objective_name)
                    & setups["entry_end"].eq(entry)
                    & setups["side"].eq(side)
                ].iloc[0]
                lines.append(
                    f"| {objective_name} | {entry} | {side} | {row['mode']} | "
                    f"{int(row['max_per_side'])} | {int(row['orders'])}/{int(row['fills'])} | "
                    f"{int(row['max_cap_days'])} | {fmt(row['pf'])} | {fmt(row['day_pf'])} | "
                    f"{float(row['net_pct']):+.3f} | {row['picker']} | "
                    f"{fmt(row['price_change_pct'])} | {fmt(row['oi_change_pct'])} | "
                    f"{fmt(row['volume_ratio'])} | {fmt(row['body_ratio'])} | "
                    f"{fmt(row['max_wick_ratio'])} | {fmt(row['min_traded_value'], 0)} | "
                    f"{fmt(row['stop_pct'])} | {fmt(row['target_pct'])} |"
                )

    lines += [
        "",
        "## Full Portfolio Comparison",
        "",
        "| Portfolio | Orders | Fills | Trade PF | Day PF | Net % | Uplift vs V4 | Positive | Negative | Flat |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        f"| Current selected V4 | {current_stats['orders']} | {current_stats['fills']} | "
        f"{fmt(current_stats['trade_pf'])} | {fmt(current_stats['day_pf'])} | "
        f"{current_stats['net_pct']:+.3f} | +0.000 | {current_stats['positive_days']} | "
        f"{current_stats['negative_days']} | {current_stats['flat_days']} |",
    ]
    for objective_name in OBJECTIVES:
        stats = summaries[objective_name]["ALL"]
        chosen_label = (
            " (CHOSEN)"
            if objective_name == FULL_HISTORY_SELECTED_OBJECTIVE
            else ""
        )
        lines.append(
            f"| V5 {objective_name}{chosen_label} | {stats['orders']} | {stats['fills']} | "
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
        f"({trade['trade_pf'] - current_stats['trade_pf']:+.3f}); net changes by "
        f"{trade['net_pct'] - current_stats['net_pct']:+.3f}%.",
        f"- Best day PF changes from {current_stats['day_pf']:.3f} to {day['day_pf']:.3f} "
        f"({day['day_pf'] - current_stats['day_pf']:+.3f}).",
        f"- Highest cumulative return is {net['net_pct']:+.3f}%, a change of "
        f"{net['net_pct'] - current_stats['net_pct']:+.3f}% versus current V4.",
        "- Current V4 remains protected; V5 is written only to new files.",
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
        "| Day | Period | 09:41 LONG | 09:41 SHORT | 09:46 LONG | 09:46 SHORT | "
        "V4 % | New % | Total O/F | Total % | Cum % | Cum day PF | Cum trade PF |"
    )
    divider = (
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for objective_name, curve in curves.items():
        lines += ["", f"## {objective_name} Day-Wise Table", "", columns, divider]
        for row in curve.itertuples(index=False):
            period = "TEST" if row.day >= split_day else "TRAIN"
            details: dict[tuple[int, str], str] = {}
            for confirmation in V5_WINDOWS.values():
                for side in ("long", "short"):
                    trade_details = getattr(
                        row, f"v5_{confirmation:04d}_{side}_trade_details"
                    )
                    status = getattr(row, f"v5_{confirmation:04d}_{side}_status")
                    details[(confirmation, side)] = trade_details or status
            lines.append(
                f"| {row.day} | {period} | {details[(941, 'long')]} | "
                f"{details[(941, 'short')]} | {details[(946, 'long')]} | "
                f"{details[(946, 'short')]} | {fmt_signed(row.v4_net_return_pct)} | "
                f"{fmt_signed(row.v5_addon_net_return_pct)} | {int(row.selections)}/{int(row.fills)} | "
                f"{fmt_signed(row.portfolio_net_return_pct)} | {fmt_signed(row.cumulative_net_pct)} | "
                f"{fmt(row.cumulative_day_pf)} | {fmt(row.cumulative_trade_pf)} |"
            )

    lines += [
        "",
        "## Files And Command",
        "",
        f"- Report: `{FULL_HISTORY_REPORT_PATH}`",
        f"- Selected research daily CSV: `{FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH}`",
        f"- All objective daily CSV: `{FULL_HISTORY_DAILY_OUTPUT_PATH}`",
        f"- Selected new-window trade audit: `{FULL_HISTORY_AUDIT_OUTPUT_PATH}`",
        f"- Selected setup table: `{FULL_HISTORY_SETUPS_OUTPUT_PATH}`",
        f"- 09:41 retained portfolios: `{portfolio_path(940)}`",
        f"- 09:46 retained portfolios: `{portfolio_path(945)}`",
        "- Run: `cmd /c eqidv2\\bat\\run_fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5_full_history.bat`",
        "",
    ]
    return "\n".join(lines)


def validate_cash_equity_signal_contract(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
) -> dict[str, Any]:
    """Reject any V5 cache that cannot prove cash-equity price execution."""

    required = {
        "sid",
        "day",
        "hhmm_int",
        "tradingsymbol",
        "instrument_token",
        "exchange",
        "futures_tradingsymbol",
        "futures_instrument_token",
        "data_contract",
        "price_source",
        "oi_source",
        "oi",
        "prev_oi",
        "oi_change_pct",
        "price_change_pct",
        "volume_ratio",
        "body_ratio",
        "wick_ratio",
        "trigger",
        "traded_value",
    }
    missing = required - set(signals.columns)
    if missing:
        raise RuntimeError(
            "V5 hybrid signal cache is missing contract fields: "
            f"{sorted(missing)}. Rebuild the cache."
        )
    if signals.empty:
        raise RuntimeError("V5 hybrid signal cache is empty.")

    expected_contract = {hybrid.DATA_CONTRACT_VERSION}
    contracts = set(signals["data_contract"].dropna().astype(str))
    if contracts != expected_contract:
        raise RuntimeError(
            f"Unexpected V5 data contract: {sorted(contracts)}; "
            f"expected {sorted(expected_contract)}"
        )
    exchanges = set(signals["exchange"].dropna().astype(str).str.upper())
    if exchanges != {"NSE"}:
        raise RuntimeError(f"V5 price instruments are not NSE equities: {exchanges}")
    price_sources = set(signals["price_source"].dropna().astype(str))
    allowed_price_sources = {
        "NSE_EQUITY",
        hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION,
    }
    if not price_sources or not price_sources.issubset(allowed_price_sources):
        raise RuntimeError(f"Unexpected V5 price sources: {sorted(price_sources)}")
    oi_sources = set(signals["oi_source"].dropna().astype(str))
    if oi_sources != {"NFO_FUTURE"}:
        raise RuntimeError(f"Unexpected V5 OI sources: {sorted(oi_sources)}")

    equity_symbols = signals["tradingsymbol"].astype(str).str.upper()
    futures_symbols = signals["futures_tradingsymbol"].astype(str).str.upper()
    if equity_symbols.str.contains(r"\d{2}[A-Z]{3}FUT$", regex=True).any():
        raise RuntimeError("A futures contract leaked into the V5 equity symbol field.")
    if equity_symbols.eq(futures_symbols).any():
        raise RuntimeError("V5 equity and futures provenance symbols are identical.")

    oi = pd.to_numeric(signals["oi"], errors="coerce").to_numpy(float)
    prev_oi = pd.to_numeric(signals["prev_oi"], errors="coerce").to_numpy(float)
    oi_change = pd.to_numeric(
        signals["oi_change_pct"], errors="coerce"
    ).to_numpy(float)
    if not (
        np.isfinite(oi).all()
        and np.isfinite(prev_oi).all()
        and np.isfinite(oi_change).all()
        and (oi > 0).all()
        and (prev_oi > 0).all()
    ):
        raise RuntimeError("V5 futures OI provenance contains invalid values.")
    recomputed = (oi / prev_oi - 1.0) * 100.0
    if not np.allclose(recomputed, oi_change, rtol=1e-9, atol=1e-9):
        raise RuntimeError("V5 oi_change_pct does not reconcile to oi and prev_oi.")

    forbidden = {
        "futures_open",
        "futures_high",
        "futures_low",
        "futures_close",
        "futures_volume",
        "futures_ema9",
        "futures_ema20",
        "futures_ema50",
        "futures_trigger",
        "futures_traded_value",
    }
    leaked = forbidden & set(signals.columns)
    if leaked:
        raise RuntimeError(
            f"Forbidden futures price/volume fields leaked into V5: {sorted(leaked)}"
        )

    signal_sids = set(pd.to_numeric(signals["sid"]).astype(int))
    missing_paths = signal_sids - set(paths)
    if missing_paths:
        raise RuntimeError(
            f"V5 is missing equity one-minute forward paths for {len(missing_paths)} signals."
        )
    for sid in signal_sids:
        path = paths[sid]
        if set(path) != {"high", "low", "close"}:
            raise RuntimeError(f"V5 path {sid} has unexpected fields: {sorted(path)}")
        lengths = {len(np.asarray(path[field])) for field in ("high", "low", "close")}
        if len(lengths) != 1 or not lengths or next(iter(lengths)) < 1:
            raise RuntimeError(f"V5 equity path {sid} is empty or misaligned.")

    return {
        "signals": int(len(signals)),
        "sessions": int(signals["day"].nunique()),
        "equities": int(equity_symbols.nunique()),
        "futures_contracts": int(futures_symbols.nunique()),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "price_sources": sorted(price_sources),
        "oi_fields": ["oi", "prev_oi", "oi_change_pct"],
    }


def _full_history_metric(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return -float("inf")
    return number if not np.isnan(number) else -float("inf")


def _full_history_state_key(
    state: hybrid_optimiser.PortfolioState,
    objective_name: str,
) -> tuple[float, ...]:
    stats = state.train_metrics
    if objective_name == "BEST_TRADE_PF":
        fields = ("pf", "day_pf", "net_pct")
    elif objective_name == "BEST_DAY_PF":
        fields = ("day_pf", "pf", "net_pct")
    elif objective_name == "BEST_NET":
        fields = ("net_pct", "pf", "day_pf")
    else:
        raise ValueError(f"Unknown full-history objective: {objective_name}")
    return (
        *(_full_history_metric(stats[field]) for field in fields),
        int(stats["fills"]),
        -len([choice for choice in state.choices if choice is not None]),
    )


def render_cash_equity_full_history_report(
    selected: dict[str, hybrid_optimiser.PortfolioState],
    periods: dict[str, dict[str, dict[str, Any]]],
    setups: pd.DataFrame,
    baseline: dict[str, dict[str, Any]],
    contract_meta: dict[str, Any],
    *,
    split_day: date,
    cost_bps: float,
    evaluated: int,
    beam_width: int,
) -> str:
    lines = [
        "# FNO V5 Full-History Cash-Equity Optimisation",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Data contract: `{hybrid.DATA_CONTRACT_VERSION}`.",
        "- Every V5 window (09:25 through 09:45) is rebuilt from the hybrid cache; no saved V2/V3/V4 curve is used.",
        "- Price, volume, EMA9/20/50, confirmation, trigger, entry, stop, target and square-off paths are NSE cash-equity data.",
        "- Futures contribute only `oi`, `prev_oi`, and `oi_change_pct`; futures OHLCV and indicators are excluded at the join boundary.",
        "- Historical equity 5-minute candles are causal five-row aggregates of completed NSE equity 1-minute candles.",
        "- Live parity: scanning requires final+complete FnO and cash 5-minute markers; only 5-minute candidates fetch equity 1-minute confirmation bars, which are persisted and reread before entry.",
        f"- Contract audit: {contract_meta['signals']:,} candidates, {contract_meta['sessions']} sessions, {contract_meta['equities']} equities, {contract_meta['futures_contracts']} OI contracts.",
        f"- Cost: {cost_bps:g} bps round trip; candidate evaluations: {evaluated:,}; beam width: {beam_width:,}.",
        f"- Train/test labels split at `{split_day.isoformat()}`; selection below deliberately uses ALL sessions.",
        "- These are descriptive in-sample maxima, not honest out-of-sample estimates.",
        "",
        "## Full Portfolio Comparison",
        "",
        "Portfolio | Orders/Fills | Trade PF | Day PF | Net % | Active days",
        "--- | ---: | ---: | ---: | ---: | ---:",
    ]
    base = baseline["ALL"]
    lines.append(
        f"Current frozen V5 | {base['orders']}/{base['fills']} | {fmt(base['pf'])} | "
        f"{fmt(base['day_pf'])} | {base['net_pct']:+.3f}% | {base['active_days']}"
    )
    for objective_name in OBJECTIVES:
        stats = periods[objective_name]["ALL"]
        lines.append(
            f"{objective_name} | {stats['orders']}/{stats['fills']} | "
            f"{fmt(stats['pf'])} | {fmt(stats['day_pf'])} | "
            f"{stats['net_pct']:+.3f}% | {stats['active_days']}"
        )

    lines += [
        "",
        "## Train/Test/All Diagnostics",
        "",
        "Objective | Period | Orders/Fills | Trade PF | Day PF | Net % | Positive/Negative days",
        "--- | --- | ---: | ---: | ---: | ---: | ---:",
    ]
    for objective_name in OBJECTIVES:
        for period_name in ("TRAIN", "TEST", "ALL"):
            stats = periods[objective_name][period_name]
            lines.append(
                f"{objective_name} | {period_name} | {stats['orders']}/{stats['fills']} | "
                f"{fmt(stats['pf'])} | {fmt(stats['day_pf'])} | "
                f"{stats['net_pct']:+.3f}% | "
                f"{stats['positive_days']}/{stats['negative_days']}"
            )

    lines += [
        "",
        "## Selected Setups",
        "",
        "Objective | Entry | Side | Mode | Max | Picker | Price | OI | Vol | Body | Wick | Stop | Target | Full fills | Full PF |",
        "--- | --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.objective} | {row.confirmation_end} | {row.side} | {row.mode} | "
            f"{row.max_entries} | {row.picker} | {row.price_change_pct:.2f} | "
            f"{row.oi_change_pct:.2f} | {row.volume_ratio:.2f} | "
            f"{row.body_ratio:.2f} | {row.max_wick_ratio:.2f} | "
            f"{row.stop_pct:.2f}% | {row.target_pct:.2f}% | "
            f"{row.full_fills} | {fmt(row.full_pf)} |"
        )

    lines += [
        "",
        "## Outputs",
        "",
        f"- Report: `{FULL_HISTORY_REPORT_PATH}`",
        f"- Ranked selected portfolios: `{FULL_HISTORY_RANKED_OUTPUT_PATH}`",
        f"- All objective daily curves: `{FULL_HISTORY_DAILY_OUTPUT_PATH}`",
        f"- Best-trade-PF daily curve: `{FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH}`",
        f"- Selected trade audits: `{FULL_HISTORY_AUDIT_OUTPUT_PATH}`",
        f"- Selected setups: `{FULL_HISTORY_SETUPS_OUTPUT_PATH}`",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(MODE_CURRENT_REPLAY, MODE_FULL_HISTORY),
        default=MODE_CURRENT_REPLAY,
        help=(
            "current-replay replays the frozen live V5 book; full-history runs "
            "the descriptive all-session layered optimiser"
        ),
    )
    parser.add_argument("--split-day", default="2026-07-17")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--min-day-win", type=float, default=0.40)
    parser.add_argument("--max-top-profit-share", type=float, default=0.45)
    parser.add_argument("--retain-n", type=int, default=1000)
    parser.add_argument("--window-retain-n", type=int, default=2000)
    parser.add_argument("--full-history-leg-retain-n", type=int, default=12)
    parser.add_argument("--full-history-beam-width", type=int, default=1200)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--rebuild-cache", action="store_true")
    args = parser.parse_args(argv)
    if args.min_trades < 1:
        parser.error("--min-trades must be positive")
    if not 0.0 <= args.min_day_win <= 1.0:
        parser.error("--min-day-win must be between 0 and 1")
    if args.retain_n < 1 or args.window_retain_n < 1:
        parser.error("retention counts must be positive")
    if args.full_history_leg_retain_n < 1 or args.full_history_beam_width < 1:
        parser.error("full-history retention counts must be positive")
    return args


def _prohibited_mixed_curve_optimise(args: argparse.Namespace) -> int:
    """Run the descriptive all-session optimiser into research-only outputs."""
    raise RuntimeError(
        "The saved-V4 layered optimiser is prohibited because it cannot prove "
        "cash-equity execution for every baseline leg."
    )
    started = time.monotonic()
    locked_v4, current_stats = load_current_v4()
    all_days = sorted(set(locked_v4["day"]))
    all_day_set = set(all_days)
    split_day = pd.Timestamp(args.split_day).date()
    train_days = {day for day in all_days if day < split_day}
    test_days = all_day_set - train_days
    fixed = current_v4.fixed_context(locked_v4)

    signals_all, paths = signal_cache.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals_all = signals_all.copy()
    signals_all["day"] = pd.to_datetime(signals_all["day"]).dt.date
    if sorted(set(signals_all["day"])) != all_days:
        raise RuntimeError("Signal cache sessions do not match current V4.")
    guards = optimiser.Guards(
        min_trades=args.min_trades,
        min_day_win=args.min_day_win,
        max_top_profit_share=args.max_top_profit_share,
    )

    searches: dict[int, WindowSearch] = {}
    beams: dict[int, list[WindowChoice]] = {}
    for slot in V5_WINDOWS:
        search = optimise_window(
            slot,
            signals_all,
            paths,
            all_days,
            train_days,
            test_days,
            guards,
            args.cost_bps,
            args.retain_n,
            fixed,
        )
        searches[slot] = search
        beams[slot] = retain_window_portfolios(
            search, fixed, args.window_retain_n
        )

    selected = choose_v5(beams[940], beams[945], fixed)
    print(
        f"[JOINT] {len(beams[940]):,} x {len(beams[945]):,} retained "
        f"window portfolios",
        flush=True,
    )

    curves: dict[str, pd.DataFrame] = {}
    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    setup_parts: list[dict[str, Any]] = []
    audit_parts: list[pd.DataFrame] = []
    for objective_name, choice in selected.items():
        legs: dict[tuple[int, str], pd.DataFrame] = {}
        for window_choice in (choice.first, choice.second):
            search = searches[window_choice.slot]
            for side, leg_choice in (
                ("LONG", window_choice.long),
                ("SHORT", window_choice.short),
            ):
                leg_daily = daily_leg(
                    search, leg_choice, objective_name, all_days
                )
                cap = LONG_MAX if side == "LONG" else SHORT_MAX
                if (leg_daily["selections"] > cap).any():
                    raise AssertionError(
                        f"V5 exceeded the {optimiser.format_hhmm(window_choice.slot)} "
                        f"{side} cap."
                    )
                legs[(window_choice.slot, side)] = leg_daily
                setup_parts.append(
                    setup_row(
                        objective_name,
                        window_choice.slot,
                        leg_choice,
                        leg_daily,
                    )
                )
                audit = trade_audit(
                    search,
                    leg_choice,
                    objective_name,
                    all_days,
                )
                if not audit.empty:
                    audit_parts.append(audit)

        curve = build_v5_curve(locked_v4, legs, objective_name)
        all_stats = current_v4.period_stats(curve, all_day_set)
        for metric in ("trade_pf", "day_pf", "net_pct"):
            if abs(
                float(all_stats[metric]) - float(choice.estimated[metric])
            ) > 1e-9:
                raise AssertionError(
                    f"V5 {objective_name} {metric} does not reconcile."
                )
        curves[objective_name] = curve
        summaries[objective_name] = {
            "TRAIN": current_v4.period_stats(curve, train_days),
            "TEST": current_v4.period_stats(curve, test_days),
            "ALL": all_stats,
        }

    setups = pd.DataFrame(setup_parts)
    common.atomic_write_csv(setups, FULL_HISTORY_SETUPS_OUTPUT_PATH)
    common.atomic_write_csv(
        pd.concat(curves.values(), ignore_index=True), FULL_HISTORY_DAILY_OUTPUT_PATH
    )
    common.atomic_write_csv(
        curves[FULL_HISTORY_SELECTED_OBJECTIVE],
        FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH,
    )
    common.atomic_write_csv(
        pd.concat(audit_parts, ignore_index=True)
        if audit_parts
        else pd.DataFrame(),
        FULL_HISTORY_AUDIT_OUTPUT_PATH,
    )
    meta = {
        "first_choices": len(beams[940]),
        "second_choices": len(beams[945]),
        "cost_bps": args.cost_bps,
    }
    common.atomic_write_text(
        FULL_HISTORY_REPORT_PATH,
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
        f"[CURRENT V4] PF={current_stats['trade_pf']:.3f} "
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
    print(
        f"[DONE] {FULL_HISTORY_REPORT_PATH} "
        f"({time.monotonic() - started:.1f}s)",
        flush=True,
    )
    return 0


def full_history_optimise(args: argparse.Namespace) -> int:
    """Optimise all five V5 windows using cash-equity execution data only."""

    started = time.monotonic()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.LATEST_DIR.mkdir(parents=True, exist_ok=True)

    signals, paths = signal_cache.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    contract_meta = validate_cash_equity_signal_contract(signals, paths)
    all_days = sorted(set(signals["day"]))
    split_day = pd.Timestamp(args.split_day).date()
    train_days = [day for day in all_days if day < split_day]
    test_days = [day for day in all_days if day >= split_day]
    if not train_days or not test_days:
        raise RuntimeError("Both train and test label windows are required.")

    # Full-history mode intentionally fits on every session. Train and test are
    # retained only as diagnostic labels, never as selection inputs.
    fit_days = set(all_days)
    all_day_code = {day: idx for idx, day in enumerate(all_days)}
    guards = hybrid_optimiser.OptimizerGuards(
        min_leg_train_fills=args.min_trades,
        min_portfolio_train_fills=max(35, args.min_trades),
        min_portfolio_train_days=min(20, len(all_days)),
        min_day_win=args.min_day_win,
        max_top_day_share=args.max_top_profit_share,
        min_worst_fold_pf=0.80,
    )
    print(
        f"[DATA] {len(signals):,} cash-equity candidates | "
        f"sessions={len(all_days)} | contract={hybrid.DATA_CONTRACT_VERSION}",
        flush=True,
    )

    leg_searches: list[
        tuple[tuple[int, str], list[hybrid_optimiser.LegCandidate]]
    ] = []
    evaluated = 0
    for slot in hybrid_optimiser.SIGNAL_SLOTS:
        slot_signals = signals.loc[signals["hhmm_int"].eq(slot)].copy()
        slot_signals = slot_signals.sort_values(
            ["day", "tradingsymbol", "sid"], kind="stable"
        ).reset_index(drop=True)
        bracket_net = {
            (stop_pct, target_pct): hybrid_optimiser.simulator.simulate_bracket(
                slot_signals,
                paths,
                stop_pct=stop_pct,
                target_pct=target_pct,
                cost_bps=args.cost_bps,
            )
            for stop_pct, target_pct in itertools.product(
                hybrid_optimiser.STOP_PCTS,
                hybrid_optimiser.TARGET_PCTS,
            )
        }
        for side in hybrid_optimiser.SIDES:
            candidates, count = hybrid_optimiser.optimise_leg(
                slot_signals,
                bracket_net,
                slot,
                side,
                fit_days,
                all_day_code,
                guards,
                retain_n=args.full_history_leg_retain_n,
                search_profile="full-grid",
                candidate_source_version="V5_CASH_EQUITY_FULL_HISTORY_OPTIMIZED",
            )
            evaluated += count
            leg_searches.append(((slot, side), candidates))
            print(
                f"[LEG {slot:04d} {side}] retained={len(candidates)} "
                f"evaluated={count:,}",
                flush=True,
            )

    beam = hybrid_optimiser.beam_portfolios(
        leg_searches,
        all_days,
        guards,
        beam_width=args.full_history_beam_width,
    )
    valid = [
        state
        for state in beam
        if hybrid_optimiser.passes_portfolio_guards(
            state.train_metrics, guards
        )
    ]
    if not valid:
        raise RuntimeError(
            "No all-session cash-equity portfolio survived the optimizer guards."
        )
    selected = {
        objective_name: max(
            valid,
            key=lambda state, name=objective_name: _full_history_state_key(
                state, name
            ),
        )
        for objective_name in OBJECTIVES
    }

    _, baseline = hybrid_optimiser.baseline_results(
        signals,
        paths,
        train_days,
        test_days,
        all_days,
        args.cost_bps,
    )
    periods: dict[str, dict[str, dict[str, Any]]] = {}
    setup_parts: list[pd.DataFrame] = []
    audit_parts: list[pd.DataFrame] = []
    daily_parts: list[pd.DataFrame] = []
    ranked_rows: list[dict[str, Any]] = []
    selected_daily: pd.DataFrame | None = None

    for objective_name, state in selected.items():
        audit = hybrid_optimiser.state_audit(state)
        audit["objective"] = objective_name
        objective_periods = {
            "TRAIN": hybrid_optimiser.score_audit(audit, train_days),
            "TEST": hybrid_optimiser.score_audit(audit, test_days),
            "ALL": hybrid_optimiser.score_audit(audit, all_days),
        }
        periods[objective_name] = objective_periods
        audit_parts.append(audit)

        setup = hybrid_optimiser.setup_frame(state).rename(
            columns={
                "train_pf": "full_pf",
                "train_robust_pf": "full_robust_pf",
                "train_fills": "full_fills",
                "train_net_pct": "full_net_pct",
            }
        )
        setup["objective"] = objective_name
        setup_parts.append(setup)

        daily = hybrid_replay.build_daily_curve(
            audit, all_days, split_day=split_day
        )
        daily["objective"] = objective_name
        daily_parts.append(daily)
        if objective_name == FULL_HISTORY_SELECTED_OBJECTIVE:
            selected_daily = daily

        active = [choice for choice in state.choices if choice is not None]
        ranked_row: dict[str, Any] = {
            "objective": objective_name,
            "objective_rank": 1,
            "active_legs": len(active),
            "setup_ids": ",".join(choice.setup.setup_id for choice in active),
            "candidate_ids": ",".join(choice.candidate_id for choice in active),
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "price_instrument": "NSE_EQUITY",
            "oi_instrument": "NFO_FUTURE",
        }
        for period_name, stats in objective_periods.items():
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
                ranked_row[f"{period_name.lower()}_{key}"] = stats[key]
        ranked_rows.append(ranked_row)

    setups = pd.concat(setup_parts, ignore_index=True, sort=False)
    audits = pd.concat(audit_parts, ignore_index=True, sort=False)
    all_daily = pd.concat(daily_parts, ignore_index=True, sort=False)
    ranked = pd.DataFrame(ranked_rows)
    if selected_daily is None:
        raise AssertionError("Best-trade-PF daily curve was not built.")

    common.atomic_write_csv(ranked, FULL_HISTORY_RANKED_OUTPUT_PATH)
    common.atomic_write_csv(setups, FULL_HISTORY_SETUPS_OUTPUT_PATH)
    common.atomic_write_csv(audits, FULL_HISTORY_AUDIT_OUTPUT_PATH)
    common.atomic_write_csv(all_daily, FULL_HISTORY_DAILY_OUTPUT_PATH)
    common.atomic_write_csv(
        selected_daily, FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH
    )
    common.atomic_write_text(
        FULL_HISTORY_REPORT_PATH,
        render_cash_equity_full_history_report(
            selected,
            periods,
            setups,
            baseline,
            contract_meta,
            split_day=split_day,
            cost_bps=args.cost_bps,
            evaluated=evaluated,
            beam_width=args.full_history_beam_width,
        ),
    )

    base = baseline["ALL"]
    print(
        f"[CURRENT V5] orders/fills={base['orders']}/{base['fills']} "
        f"PF={base['pf']:.3f} day PF={base['day_pf']:.3f} "
        f"net={base['net_pct']:+.3f}%",
        flush=True,
    )
    for objective_name in OBJECTIVES:
        stats = periods[objective_name]["ALL"]
        print(
            f"[{objective_name}] orders/fills={stats['orders']}/{stats['fills']} "
            f"PF={stats['pf']:.3f} day PF={stats['day_pf']:.3f} "
            f"net={stats['net_pct']:+.3f}%",
            flush=True,
        )
    print(
        f"[DONE] {FULL_HISTORY_REPORT_PATH} "
        f"({time.monotonic() - started:.1f}s)",
        flush=True,
    )
    return 0


def legacy_layered_optimise_main(argv: list[str] | None = None) -> int:
    """Compatibility entry point for callers of the former function name."""
    args = parse_args(argv)
    args.mode = MODE_FULL_HISTORY
    return full_history_optimise(args)


def current_replay(args: argparse.Namespace) -> int:
    """Replay the complete current V5 book under the hybrid data contract."""
    started = time.monotonic()
    signals, paths = signal_cache.load_signals(
        args.square_off, args.max_forward_bars, args.rebuild_cache
    )
    signals = signals.copy()
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    validate_cash_equity_signal_contract(signals, paths)
    days = sorted(set(signals["day"]))
    if not days:
        raise RuntimeError("Hybrid signal cache contains no sessions.")
    split_day = pd.Timestamp(args.split_day).date()
    audit = hybrid_replay.replay_setups(
        signals,
        paths,
        cost_bps=args.cost_bps,
    )
    if audit.empty:
        raise RuntimeError("Current V5 setup book selected no hybrid trades.")
    daily = hybrid_replay.build_daily_curve(audit, days, split_day=split_day)
    stats = hybrid_replay.summary_stats(daily, audit)
    setups = hybrid_replay.setup_summary(audit)
    report = hybrid_replay.render_report(
        daily,
        audit,
        setups,
        stats,
        split_day=split_day,
    )
    hybrid_replay.write_outputs(
        daily,
        audit,
        setups,
        report,
        # Routine replays are rolling diagnostics. Promotion of a selected
        # curve is an explicit operation and must never happen implicitly.
        daily_paths=(DAILY_OUTPUT_PATH,),
        audit_path=ROLLING_AUDIT_OUTPUT_PATH,
        setup_path=ROLLING_SETUPS_OUTPUT_PATH,
        report_path=REPORT_PATH,
    )
    print(
        f"[HYBRID V5] sessions={stats['sessions']} orders/fills="
        f"{stats['orders']}/{stats['fills']} PF={stats['trade_pf']:.3f} "
        f"day PF={stats['day_pf']:.3f} net={stats['net_pct']:+.3f}%",
        flush=True,
    )
    print(f"[DONE] {REPORT_PATH} ({time.monotonic() - started:.1f}s)", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    print(f"[MODE] {args.mode}", flush=True)
    if args.mode == MODE_FULL_HISTORY:
        return full_history_optimise(args)
    return current_replay(args)


if __name__ == "__main__":
    raise SystemExit(main())
