"""Fresh G14 candidate execution with the inherited G7 V4 Lock2 risk rules.

Selection happens upstream.  In particular, this module never reads an old
selected-order book to assign fills to a newly selected stock.  The supplied
orders must already be ranked within each setup/day using the proposed entry
features and must carry a completed one-minute confirmation and stop trigger.

No live or paper-trading code is imported or changed by this module.
"""
from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as native_engine
from g3_v18_20261102 import data as source_data
from research import g7_v4_filtered_backtest as g7_settings
from research import g7_v4_selected_backtest as g7_exits
from research import slot_win70_backtest as g7_source
from research.g7_v4_practical_research import execute as allocate_g7
from research.g8_exit_engine import _flags


IST = "Asia/Kolkata"
REQUIRED_ORDER_COLUMNS = frozenset({
    "sid", "day", "tradingsymbol", "setup_id", "side", "signal_ts",
    "confirmation_ts", "confirmation_high", "confirmation_low", "trigger",
    "native_stop_pct", "native_target_pct",
})
REQUIRED_MINUTE_COLUMNS = frozenset({"tradingsymbol", "open", "high", "low", "close"})
MORNING_SLOTS = frozenset({"09:20", "09:25", "09:30", "09:35", "09:40", "09:45"})
BASE_ENTRY_COST_BPS = 5.0


@lru_cache(maxsize=1)
def _pinned_base_config() -> dict:
    """Load the hash-verified G7 V4 parent allocation configuration once."""
    _, _, config = g7_source.source_inputs()
    return config


def _protection_settings() -> dict:
    """Use the published G7 V4 +0.6% completed-close / +0.1% policy."""
    return g7_settings.protection_settings({"exit_policy": deepcopy(g7_settings.PROTECTION)})


def _check_orders(orders: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_ORDER_COLUMNS.difference(orders.columns)
    if missing:
        raise ValueError("G14 order columns missing: " + ", ".join(sorted(missing)))
    if not orders.index.is_unique or orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError("G14 orders require a unique index and globally unique nonmissing sid")
    if not orders.side.isin(("LONG", "SHORT")).all():
        raise ValueError("G14 order side must be LONG or SHORT")
    work = orders.copy(deep=True).reset_index(drop=True)
    work["sid"] = pd.to_numeric(work.sid, errors="raise").astype("int64")
    if not work.sid.eq(orders.sid.to_numpy()).all():
        raise ValueError("G14 sid must be an exact integer")
    work["day"] = work.day.astype(str).str[:10]
    signal = pd.to_datetime(work.signal_ts, errors="raise", utc=True).dt.tz_convert(IST)
    confirmed = pd.to_datetime(work.confirmation_ts, errors="raise", utc=True).dt.tz_convert(IST)
    if not (signal.dt.strftime("%Y-%m-%d").eq(work.day)
            & confirmed.eq(signal + pd.Timedelta(minutes=1))
            & signal.dt.strftime("%H:%M").isin(MORNING_SLOTS)).all():
        raise ValueError("G14 orders must use one of six signal clocks and exact next-minute confirmation")
    if not work.setup_id.eq(confirmed.dt.strftime("%H%M") + "_" + work.side).all():
        raise ValueError("G14 setup ID differs from confirmation clock and side")
    for field in ("trigger", "native_stop_pct", "native_target_pct"):
        values = pd.to_numeric(work[field], errors="coerce")
        if not np.isfinite(values).all() or values.le(0).any():
            raise ValueError(f"G14 {field} must be finite and positive")
    if not np.allclose(work.native_stop_pct.astype(float), 1.2, rtol=0, atol=1e-12):
        raise ValueError("G14 G7 V4 execution requires its inherited 1.2% initial stop")
    high = pd.to_numeric(work.confirmation_high, errors="coerce")
    low = pd.to_numeric(work.confirmation_low, errors="coerce")
    trigger = pd.to_numeric(work.trigger, errors="coerce")
    expected_trigger = np.where(work.side.eq("LONG"), high, low)
    if (not np.isfinite(high).all() or not np.isfinite(low).all() or
            high.lt(low).any() or not np.allclose(trigger, expected_trigger, rtol=0, atol=1e-8)):
        raise ValueError("G14 trigger must equal the known confirmation high for LONG or low for SHORT")
    work["signal_ts"] = signal
    work["confirmation_ts"] = confirmed
    work["scope"] = "BACKTEST_ONLY"
    work["execution_authority"] = False
    return work


def _check_minutes(day: str, orders: pd.DataFrame, minutes: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_MINUTE_COLUMNS.difference(minutes.columns)
    if missing or not {"ts", "date"}.intersection(minutes.columns):
        raise ValueError("G14 minutes require symbol, timestamp, and OHLC")
    # Validate only symbols that could actually enter.  A defective selected
    # symbol cannot silently disappear from the losing-trade inventory.
    subset = minutes.loc[minutes.tradingsymbol.isin(orders.tradingsymbol)].copy()
    if set(subset.tradingsymbol) != set(orders.tradingsymbol):
        raise ValueError("Missing execution minute history for a selected G14 symbol")
    stamp_field = "ts" if "ts" in subset else "date"
    stamps = pd.to_datetime(subset[stamp_field], errors="raise", utc=True).dt.tz_convert(IST)
    subset = subset.loc[stamps.dt.strftime("%Y-%m-%d").eq(day)].copy()
    if set(subset.tradingsymbol) != set(orders.tradingsymbol):
        raise ValueError("Missing dated execution minute history for a selected G14 symbol")
    return source_data._normalize_minutes(subset, day)


def _empty_book(orders: pd.DataFrame, cost: float) -> pd.DataFrame:
    empty = orders.copy().iloc[0:0]
    for field, dtype in (("sid", "int64"), ("day", str), ("setup_id", str),
                         ("tradingsymbol", str), ("side", str)):
        if field not in empty:
            empty[field] = pd.Series(dtype=dtype)
    for field, dtype in (("filled", bool), ("g8_baseline_filled", bool),
                         ("portfolio_executed", bool),
                         ("portfolio_net_profit_rupees", float),
                         ("portfolio_gross_profit_rupees", float),
                         ("portfolio_cost_rupees", float)):
        empty[field] = pd.Series(dtype=dtype)
    empty["model_cost_bps_round_trip"] = pd.Series(dtype=float)
    empty["strategy"] = pd.Series(dtype=str)
    empty["strategy_version"] = pd.Series(dtype=str)
    empty["scope"] = pd.Series(dtype=str)
    empty["execution_authority"] = pd.Series(dtype=bool)
    return empty


def simulate_allocated(
    orders: pd.DataFrame,
    minutes: pd.DataFrame,
    cost_bps: Sequence[float] = (5.0, 9.0, 15.0),
    *,
    allowed: pd.Series | None = None,
    base_config: Mapping | None = None,
    policy: str = "g14_six_feature",
) -> dict[float, pd.DataFrame]:
    """Return a full reallocated G14 opportunity book at each specified cost.

    ``orders`` is the complete *newly selected* pre-portfolio inventory.  It
    needs one row per selected stock/setup/day, native trigger, target and stop.
    ``minutes`` contains actual contiguous dated one-minute stock OHLC through
    15:15.  ``allowed`` is optional SID-indexed pre-entry permission; absent it,
    every supplied order is permitted.  An external market-direction veto must
    be passed explicitly rather than silently inherited from G7.

    All fills and V4 exits are newly simulated.  G7 portfolio admission is
    recomputed independently for 5/9/15 bps, including integer share sizing,
    conservative entry clock and the causal two-realized-loss daily lock.
    Only one trading session is accepted per call; callers concatenate results
    after evaluating every available session, including zero-order days.
    """
    if not isinstance(orders, pd.DataFrame) or not isinstance(minutes, pd.DataFrame):
        raise TypeError("G14 orders and minutes must be DataFrames")
    costs = tuple(float(value) for value in cost_bps)
    if not costs or len(set(costs)) != len(costs) or not np.isfinite(costs).all() or min(costs) < 0:
        raise ValueError("G14 cost_bps must be unique finite nonnegative values")
    if orders.empty:
        return {cost: _empty_book(orders, cost) for cost in costs}
    work = _check_orders(orders)
    days = work.day.unique()
    if len(days) != 1:
        raise ValueError("Call G14 execution separately for each trading day")
    if allowed is None:
        permission = pd.Series(True, index=work.sid, dtype=bool)
    else:
        if not isinstance(allowed, pd.Series) or not allowed.index.is_unique or set(allowed.index) != set(work.sid):
            raise ValueError("allowed must contain exactly one SID-indexed decision per selected order")
        permission = _flags(allowed, "G14 pre-entry permission")
    config = deepcopy(dict(base_config if base_config is not None else _pinned_base_config()))
    if (float(config["initial_stop_pct"]) != 1.2 or
            float(config["tightened_stop_pct"]) != 1.0 or
            float(config["tighten_after_minutes"]) != 120):
        raise ValueError("G14 G7 V4 base stop/timed-tightening configuration differs")
    day = str(days[0])
    relevant_minutes = _check_minutes(day, work, minutes)
    paths = source_data.execution_paths(work, relevant_minutes)
    native_engine.v9.validate_paths(work, paths)
    native = native_engine.v9.v5.simulate_native(
        work, paths, cost_bps=BASE_ENTRY_COST_BPS, max_entry_delay_minutes=10)
    # Native v5 emits only fields observed in its result records.  A day whose
    # every order is unfilled still needs the stable schema for V4 replay.
    for field, value in (("entry_price", np.nan), ("entry_ts", pd.NaT),
                         ("entry_path_index", np.nan), ("exit_price", np.nan),
                         ("exit_ts", pd.NaT), ("gross_return_pct", np.nan),
                         ("cost_pct", np.nan), ("net_return_pct", np.nan)):
        if field not in native:
            native[field] = value
    native["g8_baseline_filled"] = _flags(native.filled, "native fills").to_numpy(bool)
    protected = g7_exits.replay(native, paths, config, _protection_settings())
    books: dict[float, pd.DataFrame] = {}
    for cost in costs:
        book = allocate_g7(dict(raw=protected, allowed=permission, integer=True,
                                clock=True, limit=2), config, policy, cost=cost)
        if not np.array_equal(book.sid.to_numpy(), work.sid.to_numpy()):
            raise ValueError("G14 allocator changed selected opportunity inventory")
        for field in ("entry_price", "exit_price", "gross_return_pct"):
            if not np.allclose(pd.to_numeric(book[field], errors="coerce"),
                               pd.to_numeric(protected[field], errors="coerce"),
                               rtol=0, atol=1e-8, equal_nan=True):
                raise ValueError("G14 allocator changed native fill/exit: " + field)
        book["strategy"] = "V13-V10-G-14"
        book["strategy_version"] = "V13-V10-G-14-SIX-FEATURE-RESEARCH"
        book["scope"] = "BACKTEST_ONLY"
        book["execution_authority"] = False
        book["model_cost_bps_round_trip"] = cost
        books[cost] = book
    return books


def simulate_day(
    selected: pd.DataFrame,
    minutes: pd.DataFrame,
    base_config: Mapping,
    cost_bps: float = 5.0,
    policy: str = "g14_six_feature",
) -> pd.DataFrame:
    """Stable single-cost, single-day facade for the G14 research runner."""
    return simulate_allocated(selected, minutes, (cost_bps,),
                              base_config=base_config, policy=policy)[float(cost_bps)]

\n