"""Execution-safe option replay for frozen V13-v5 stock signals.

This V13-v6 runner fixes the V13-v5 data-root binding defect and fails closed
when the requested option quantity cannot be supported by the observed candle
volume.  Stops gap through at the adverse open, prices are tick-rounded, costs
are charged on both entry and exit, and optional risk/premium budgets determine
integer lots.

The signal ledger and V13-v5 files are never modified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_v5_options_backtest as legacy


SCHEMA_VERSION = "FNO_V13_V6_OPTIONS_EXECUTION_V2"
DEFAULT_DATA_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v13_corrected_v5"
    / "derivative_market_data"
)
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v6_options"


@dataclass(frozen=True)
class OptionExecutionConfig:
    requested_lots: int = 3
    initial_stop_pct: float = 17.5
    first_target_pct: float = 22.5
    runner_target_pct: float = 22.5
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    tick_size: float = 0.05
    adverse_ticks_each_side: int = 0
    max_volume_participation: float = 1.0
    entry_capacity_policy: str = "reject"
    risk_budget_rupees: float | None = None
    max_premium_outlay_rupees: float | None = None
    square_off_hhmm: str = "15:15"
    square_off_execution_delay_minutes: int = 1
    max_square_off_delay_minutes: int = 15

    def validate(self) -> None:
        if self.requested_lots <= 0:
            raise ValueError("requested_lots must be positive")
        for name in ("initial_stop_pct", "first_target_pct", "runner_target_pct"):
            if not np.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if self.runner_target_pct < self.first_target_pct:
            raise ValueError("runner_target_pct cannot be below first_target_pct")
        if self.tick_size <= 0 or not np.isfinite(self.tick_size):
            raise ValueError("tick_size must be positive and finite")
        if self.adverse_ticks_each_side < 0:
            raise ValueError("adverse_ticks_each_side cannot be negative")
        if not 0 < self.max_volume_participation <= 1:
            raise ValueError("max_volume_participation must be in (0, 1]")
        if self.entry_capacity_policy not in {"reject", "resize"}:
            raise ValueError("entry_capacity_policy must be 'reject' or 'resize'")
        for name in ("risk_budget_rupees", "max_premium_outlay_rupees"):
            value = getattr(self, name)
            if value is not None and (not np.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite when configured")
        if self.entry_cost_bps < 0 or self.exit_cost_bps < 0:
            raise ValueError("cost bps cannot be negative")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def option_path(symbol: str, raw_options_dir: Path) -> Path:
    """Resolve against the explicit run root; no import-time default binding."""

    return raw_options_dir / f"{common.safe_contract_stem(symbol)}_1minute.parquet"


def load_option_candles(
    symbol: str,
    raw_options_dir: Path,
    cache: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    key = str(symbol).strip().upper()
    path = option_path(key, raw_options_dir).resolve()
    cache_key = str(path)
    if cache_key not in cache:
        cache[cache_key] = (
            legacy.normalize_option_candles(pd.read_parquet(path))
            if path.is_file()
            else pd.DataFrame()
        )
    return cache[cache_key]


def load_coverage_roots(data_roots: list[Path]) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    parts: list[pd.DataFrame] = []
    sources: list[dict[str, str]] = []
    for priority, supplied_root in enumerate(data_roots):
        root = supplied_root.resolve()
        coverage_path = root / "audit" / "option_trade_coverage_and_capital.csv"
        raw_options_dir = root / "raw_options_1m"
        if not coverage_path.is_file():
            raise FileNotFoundError(f"Missing option coverage file: {coverage_path}")
        coverage = pd.read_csv(coverage_path)
        if "trade_id" not in coverage:
            raise ValueError(f"Coverage file lacks trade_id: {coverage_path}")
        coverage["_data_root"] = str(root)
        coverage["_raw_options_dir"] = str(raw_options_dir)
        coverage["_source_priority"] = priority
        parts.append(coverage)
        sources.append(
            {
                "data_root": str(root),
                "coverage_path": str(coverage_path),
                "coverage_sha256": _sha256(coverage_path),
                "raw_options_dir": str(raw_options_dir),
            }
        )
    combined = pd.concat(parts, ignore_index=True)
    combined = combined.sort_values("_source_priority", kind="stable")
    combined = combined.drop_duplicates("trade_id", keep="last").reset_index(drop=True)
    return combined, sources


def load_explicit_coverage(path: Path) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Load an audited shadow coverage map, such as the causal selector output."""

    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Missing explicit option coverage file: {path}")
    coverage = pd.read_csv(path)
    required = {"trade_id", "coverage_state", "option_tradingsymbol", "_raw_options_dir"}
    missing = sorted(required.difference(coverage.columns))
    if missing:
        raise ValueError(f"Explicit coverage lacks columns {missing}: {path}")
    coverage = coverage.drop_duplicates("trade_id", keep="last").reset_index(drop=True)
    return coverage, [{"coverage_path": str(path), "coverage_sha256": _sha256(path)}]


def _round_nearest_tick(value: float, tick_size: float) -> float:
    return float(math.floor(value / tick_size + 0.5 + 1e-12) * tick_size)


def _round_down_tick(value: float, tick_size: float) -> float:
    return float(math.floor(value / tick_size + 1e-12) * tick_size)


def _round_up_tick(value: float, tick_size: float) -> float:
    return float(math.ceil(value / tick_size - 1e-12) * tick_size)


def _available_lots(volume: float, one_lot_quantity: int, participation: float) -> int:
    if not np.isfinite(volume) or volume <= 0 or one_lot_quantity <= 0:
        return 0
    return max(0, int(math.floor(volume * participation / one_lot_quantity + 1e-12)))


def _entry_candle(candles: pd.DataFrame, entry_ts: pd.Timestamp) -> pd.Series | None:
    exact = candles.loc[candles["timestamp"].eq(entry_ts)]
    return None if exact.empty else exact.iloc[0]


def _sized_lots(
    *,
    entry_premium: float,
    one_lot_quantity: int,
    entry_volume: float,
    config: OptionExecutionConfig,
) -> tuple[int, str, dict[str, float]]:
    requested = config.requested_lots
    risk_per_lot = (
        entry_premium * one_lot_quantity * config.initial_stop_pct / 100.0
        + entry_lot_cost(
            entry=entry_premium, quantity=one_lot_quantity, config=config
        )
        + 2.0 * config.adverse_ticks_each_side * config.tick_size * one_lot_quantity
    )
    premium_per_lot = entry_premium * one_lot_quantity
    budget_lots = requested
    if config.risk_budget_rupees is not None:
        budget_lots = min(budget_lots, int(config.risk_budget_rupees // risk_per_lot))
    if config.max_premium_outlay_rupees is not None:
        budget_lots = min(
            budget_lots, int(config.max_premium_outlay_rupees // premium_per_lot)
        )
    if budget_lots <= 0:
        return 0, "RISK_OR_PREMIUM_BUDGET_BELOW_ONE_LOT", {
            "risk_per_lot_rupees": risk_per_lot,
            "premium_per_lot_rupees": premium_per_lot,
        }
    capacity_lots = _available_lots(
        entry_volume, one_lot_quantity, config.max_volume_participation
    )
    if capacity_lots < budget_lots:
        if config.entry_capacity_policy == "reject":
            return 0, "ENTRY_VOLUME_CAPACITY_INSUFFICIENT", {
                "risk_per_lot_rupees": risk_per_lot,
                "premium_per_lot_rupees": premium_per_lot,
                "entry_capacity_lots": float(capacity_lots),
            }
        budget_lots = capacity_lots
    if budget_lots <= 0:
        return 0, "ENTRY_VOLUME_CAPACITY_BELOW_ONE_LOT", {
            "risk_per_lot_rupees": risk_per_lot,
            "premium_per_lot_rupees": premium_per_lot,
            "entry_capacity_lots": float(capacity_lots),
        }
    return budget_lots, "", {
        "risk_per_lot_rupees": risk_per_lot,
        "premium_per_lot_rupees": premium_per_lot,
        "entry_capacity_lots": float(capacity_lots),
    }


def entry_exit_cost(
    *, entry_value: float, exit_value: float, config: OptionExecutionConfig
) -> tuple[float, float]:
    return (
        entry_value * config.entry_cost_bps / 10_000.0,
        exit_value * config.exit_cost_bps / 10_000.0,
    )


def entry_lot_cost(
    *, entry: float, quantity: int, config: OptionExecutionConfig
) -> float:
    value = entry * quantity
    entry_cost, estimated_exit_cost = entry_exit_cost(
        entry_value=value,
        exit_value=value * (1.0 - config.initial_stop_pct / 100.0),
        config=config,
    )
    return entry_cost + estimated_exit_cost


def _base_result(
    row: pd.Series,
    variant: legacy.OptionExitVariant,
    config: OptionExecutionConfig,
) -> dict[str, Any]:
    result = legacy._base_result(
        row,
        variant.name,
        option_lots=config.requested_lots,
        initial_stop_pct=config.initial_stop_pct,
        first_target_pct=config.first_target_pct,
        runner_target_pct=config.runner_target_pct,
    )
    result.update(
        {
            "schema_version": SCHEMA_VERSION,
            "requested_option_lots": config.requested_lots,
            "sizing_reason": "",
            "risk_per_lot_rupees": np.nan,
            "entry_capacity_lots": np.nan,
            "max_volume_participation": config.max_volume_participation,
            "entry_cost_rupees": np.nan,
            "exit_cost_rupees": np.nan,
            "exit_gap_through": False,
            "exit_gap_rupees_per_unit": 0.0,
            "tick_size": config.tick_size,
            "exit_filled_quantity": 0,
            "unfilled_exit_quantity": 0,
            "exit_fill_count": 0,
            "partial_exit": False,
            "realized_gross_pnl_rupees": np.nan,
            "realized_net_pnl_rupees": np.nan,
            "execution_fills_json": "[]",
        }
    )
    return result


def _finish(
    result: dict[str, Any],
    *,
    exit_ts: Any,
    exit_premium: float,
    exit_reason: str,
    exit_bar_volume: float,
    quantity: int,
    config: OptionExecutionConfig,
) -> dict[str, Any]:
    entry = float(result["entry_premium"])
    exit_premium = max(config.tick_size, _round_down_tick(exit_premium, config.tick_size))
    entry_value = entry * quantity
    exit_value = exit_premium * quantity
    entry_cost, exit_cost = entry_exit_cost(
        entry_value=entry_value, exit_value=exit_value, config=config
    )
    gross = exit_value - entry_value
    total_cost = entry_cost + exit_cost
    result.update(
        {
            "execution_status": "EXECUTED",
            "exit_ts": legacy._as_ist(exit_ts),
            "exit_premium": exit_premium,
            "exit_reason": exit_reason,
            "exit_bar_volume": float(exit_bar_volume),
            "holding_minutes": float(
                (legacy._as_ist(exit_ts) - legacy._as_ist(result["entry_ts"])).total_seconds()
                / 60.0
            ),
            "exit_premium_value_rupees": exit_value,
            "gross_pnl_rupees": gross,
            "entry_cost_rupees": entry_cost,
            "exit_cost_rupees": exit_cost,
            "cost_bps": config.entry_cost_bps + config.exit_cost_bps,
            "cost_proxy_rupees": total_cost,
            "net_pnl_rupees": gross - total_cost,
            "gross_return_pct": gross / entry_value * 100.0,
            "net_return_pct": (gross - total_cost) / entry_value * 100.0,
        }
    )
    return result


def _has_exit_capacity(
    volume: float,
    quantity: int,
    one_lot_quantity: int,
    config: OptionExecutionConfig,
    *,
    quantity_already_consumed: int = 0,
) -> bool:
    available_quantity = (
        _available_lots(volume, one_lot_quantity, config.max_volume_participation)
        * one_lot_quantity
    )
    return available_quantity - quantity_already_consumed >= quantity


def _capacity_failure(
    result: dict[str, Any], reason: str, *, stamp: Any, volume: float
) -> dict[str, Any]:
    result.update(
        {
            "execution_status": "NON_EXECUTABLE_EXIT_CAPACITY",
            "skip_reason": reason,
            "exit_ts": legacy._as_ist(stamp),
            "exit_bar_volume": float(volume),
        }
    )
    return result


def simulate_option_native_trade_full_fill(
    row: pd.Series,
    candles: pd.DataFrame,
    variant: legacy.OptionExitVariant,
    config: OptionExecutionConfig,
) -> dict[str, Any]:
    config.validate()
    result = _base_result(row, variant, config)
    if row.get("coverage_state") != legacy.DEFAULT_READY_COVERAGE_STATE:
        result["skip_reason"] = str(row.get("coverage_state", ""))
        return result
    if candles.empty:
        result["skip_reason"] = "MISSING_OPTION_CANDLE_FILE"
        return result

    entry_ts = legacy._as_ist(row["reference_entry_bar"])
    source_entry = float(pd.to_numeric(row.get("reference_entry_premium"), errors="coerce"))
    if not np.isfinite(source_entry) or source_entry <= 0:
        result["execution_status"] = "SKIPPED_EXECUTION_INPUT"
        result["skip_reason"] = "INVALID_REFERENCE_ENTRY_PREMIUM"
        return result
    one_lot_value = pd.to_numeric(row.get("quantity"), errors="coerce")
    if pd.isna(one_lot_value) or not np.isfinite(float(one_lot_value)) or float(one_lot_value) <= 0:
        result["execution_status"] = "SKIPPED_EXECUTION_INPUT"
        result["skip_reason"] = "INVALID_ONE_LOT_QUANTITY"
        return result
    entry = _round_up_tick(source_entry, config.tick_size) + (
        config.adverse_ticks_each_side * config.tick_size
    )
    one_lot_quantity = int(float(one_lot_value))
    candle = _entry_candle(candles, entry_ts)
    if candle is None:
        result["execution_status"] = "SKIPPED_EXECUTION_INPUT"
        result["skip_reason"] = "MISSING_EXACT_ENTRY_CANDLE"
        return result
    entry_volume = float(candle["volume"])
    lots, sizing_reason, sizing = _sized_lots(
        entry_premium=entry,
        one_lot_quantity=one_lot_quantity,
        entry_volume=entry_volume,
        config=config,
    )
    result.update(sizing)
    result["sizing_reason"] = sizing_reason
    if lots <= 0:
        result["execution_status"] = "SKIPPED_ENTRY_SIZING_OR_CAPACITY"
        result["skip_reason"] = sizing_reason
        return result
    quantity = lots * one_lot_quantity
    stop = _round_down_tick(entry * (1.0 - config.initial_stop_pct / 100.0), config.tick_size)
    t1 = _round_nearest_tick(entry * (1.0 + config.first_target_pct / 100.0), config.tick_size)
    runner = _round_nearest_tick(entry * (1.0 + config.runner_target_pct / 100.0), config.tick_size)
    result.update(
        {
            "option_lots": lots,
            "quantity": quantity,
            "entry_ts": entry_ts,
            "entry_premium": entry,
            "entry_bar_volume": entry_volume,
            "entry_premium_outlay_rupees": entry * quantity,
            "initial_stop_premium": stop,
            "first_target_premium": t1,
            "runner_target_premium": runner,
        }
    )

    square_off = legacy.square_off_timestamp(
        row["day"], config.square_off_hhmm, config.square_off_execution_delay_minutes
    )
    path = candles.loc[
        candles["timestamp"].dt.date.eq(entry_ts.date())
        & candles["timestamp"].ge(entry_ts)
        & candles["timestamp"].lt(square_off)
        & candles["volume"].gt(0)
    ].copy()
    if path.empty:
        result["execution_status"] = "SKIPPED_EXECUTION_INPUT"
        result["skip_reason"] = "NO_POSITIVE_VOLUME_OPTION_PATH_AFTER_ENTRY"
        return result

    armed = False
    for bar in path.to_dict("records"):
        low, high = float(bar["low"]), float(bar["high"])
        stamp, volume = bar["timestamp"], float(bar["volume"])
        entry_quantity_consumed = quantity if stamp == entry_ts else 0
        if not armed:
            hit_stop, hit_t1 = low <= stop, high >= t1
            if hit_stop:
                result["same_bar_ambiguous"] = bool(hit_t1)
                result["stop_hit"] = True
                if not _has_exit_capacity(
                    volume,
                    quantity,
                    one_lot_quantity,
                    config,
                    quantity_already_consumed=entry_quantity_consumed,
                ):
                    return _capacity_failure(
                        result,
                        "STOP_BAR_VOLUME_CAPACITY_INSUFFICIENT",
                        stamp=stamp,
                        volume=volume,
                    )
                activation_bar = stamp == entry_ts
                raw_exit = stop
                if not activation_bar and float(bar["open"]) < stop:
                    raw_exit = float(bar["open"])
                    result["exit_gap_through"] = True
                    result["exit_gap_rupees_per_unit"] = stop - raw_exit
                raw_exit -= config.adverse_ticks_each_side * config.tick_size
                return _finish(
                    result,
                    exit_ts=stamp,
                    exit_premium=raw_exit,
                    exit_reason=(
                        "FULL_STOP_BEFORE_OR_WITH_T1" if hit_t1 else "FULL_STOP"
                    ),
                    exit_bar_volume=volume,
                    quantity=quantity,
                    config=config,
                )
            if hit_t1:
                result["target_hit"] = True
                if variant.t1_exits_full_lot:
                    if not _has_exit_capacity(
                        volume,
                        quantity,
                        one_lot_quantity,
                        config,
                        quantity_already_consumed=entry_quantity_consumed,
                    ):
                        return _capacity_failure(
                            result,
                            "TARGET_BAR_VOLUME_CAPACITY_INSUFFICIENT",
                            stamp=stamp,
                            volume=volume,
                        )
                    return _finish(
                        result,
                        exit_ts=stamp,
                        exit_premium=t1 - config.adverse_ticks_each_side * config.tick_size,
                        exit_reason="FULL_LOT_T1",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        config=config,
                    )
                armed = True
                if low <= entry:
                    if not _has_exit_capacity(
                        volume,
                        quantity,
                        one_lot_quantity,
                        config,
                        quantity_already_consumed=entry_quantity_consumed,
                    ):
                        return _capacity_failure(
                            result,
                            "BREAKEVEN_BAR_VOLUME_CAPACITY_INSUFFICIENT",
                            stamp=stamp,
                            volume=volume,
                        )
                    result["breakeven_exit"] = True
                    return _finish(
                        result,
                        exit_ts=stamp,
                        exit_premium=entry - config.adverse_ticks_each_side * config.tick_size,
                        exit_reason="T1_THEN_BREAKEVEN_SAME_BAR",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        config=config,
                    )
                if high >= runner:
                    if not _has_exit_capacity(
                        volume,
                        quantity,
                        one_lot_quantity,
                        config,
                        quantity_already_consumed=entry_quantity_consumed,
                    ):
                        return _capacity_failure(
                            result,
                            "RUNNER_BAR_VOLUME_CAPACITY_INSUFFICIENT",
                            stamp=stamp,
                            volume=volume,
                        )
                    result["runner_target_hit"] = True
                    return _finish(
                        result,
                        exit_ts=stamp,
                        exit_premium=runner - config.adverse_ticks_each_side * config.tick_size,
                        exit_reason="RUNNER_TARGET_SAME_BAR_AFTER_T1",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        config=config,
                    )
        else:
            hit_be, hit_runner = low <= entry, high >= runner
            if hit_be or hit_runner:
                if not _has_exit_capacity(volume, quantity, one_lot_quantity, config):
                    return _capacity_failure(
                        result,
                        "ARMED_EXIT_BAR_VOLUME_CAPACITY_INSUFFICIENT",
                        stamp=stamp,
                        volume=volume,
                    )
                if hit_be:
                    result["same_bar_ambiguous"] = bool(hit_runner)
                    result["breakeven_exit"] = True
                    exit_reason = (
                        "BREAKEVEN_BEFORE_OR_WITH_RUNNER"
                        if hit_runner
                        else "T1_THEN_BREAKEVEN"
                    )
                    exit_price = entry
                else:
                    result["runner_target_hit"] = True
                    exit_reason = "RUNNER_TARGET"
                    exit_price = runner
                return _finish(
                    result,
                    exit_ts=stamp,
                    exit_premium=exit_price
                    - config.adverse_ticks_each_side * config.tick_size,
                    exit_reason=exit_reason,
                    exit_bar_volume=volume,
                    quantity=quantity,
                    config=config,
                )

    candidates = candles.loc[
        candles["timestamp"].dt.date.eq(entry_ts.date())
        & candles["timestamp"].ge(square_off)
        & candles["timestamp"].le(
            square_off + pd.Timedelta(minutes=config.max_square_off_delay_minutes)
        )
        & candles["volume"].gt(0)
    ]
    candidates = candidates.loc[
        candidates["volume"].map(
            lambda volume: _has_exit_capacity(
                float(volume), quantity, one_lot_quantity, config
            )
        )
    ]
    if candidates.empty:
        result["execution_status"] = "NO_CAPACITY_SQUARE_OFF_CANDLE"
        result["skip_reason"] = "NO_FULL_QUANTITY_CAPACITY_AT_OR_AFTER_SQUARE_OFF"
        return result
    exit_row = candidates.iloc[0]
    result["square_off_delay_minutes"] = float(
        (exit_row["timestamp"] - square_off).total_seconds() / 60.0
    )
    return _finish(
        result,
        exit_ts=exit_row["timestamp"],
        exit_premium=float(exit_row["open"])
        - config.adverse_ticks_each_side * config.tick_size,
        exit_reason="TIME_EXIT_WITH_CAPACITY",
        exit_bar_volume=float(exit_row["volume"]),
        quantity=quantity,
        config=config,
    )


def _fill_record(
    *,
    row: pd.Series,
    variant: legacy.OptionExitVariant,
    side: str,
    stamp: Any,
    premium: float,
    lots: int,
    one_lot_quantity: int,
    reason: str,
    bar_volume: float,
    capacity_quantity: int,
    capacity_consumed_before: int,
    cost_bps: float,
) -> dict[str, Any]:
    quantity = lots * one_lot_quantity
    return {
        "trade_id": str(row.get("trade_id", "")),
        "variant": variant.name,
        "side": side,
        "fill_ts": legacy._as_ist(stamp).isoformat(),
        "fill_premium": float(premium),
        "lots": int(lots),
        "quantity": int(quantity),
        "reason": reason,
        "bar_volume": float(bar_volume),
        "capacity_quantity": int(capacity_quantity),
        "capacity_consumed_before": int(capacity_consumed_before),
        "notional_rupees": float(premium * quantity),
        "cost_rupees": float(premium * quantity * cost_bps / 10_000.0),
    }


def _finish_from_partial_fills(
    result: dict[str, Any],
    *,
    fills: list[dict[str, Any]],
    total_quantity: int,
    terminal_reason: str,
    config: OptionExecutionConfig,
) -> dict[str, Any]:
    exit_fills = [fill for fill in fills if fill["side"] == "SELL"]
    filled_quantity = int(sum(int(fill["quantity"]) for fill in exit_fills))
    remaining_quantity = max(0, int(total_quantity - filled_quantity))
    exit_value = float(sum(float(fill["notional_rupees"]) for fill in exit_fills))
    entry = float(result["entry_premium"])
    entry_value = float(entry * total_quantity)
    entry_cost = entry_value * config.entry_cost_bps / 10_000.0
    exit_cost = exit_value * config.exit_cost_bps / 10_000.0
    realized_entry_value = entry * filled_quantity
    realized_entry_cost = realized_entry_value * config.entry_cost_bps / 10_000.0
    realized_gross = exit_value - realized_entry_value
    realized_net = realized_gross - realized_entry_cost - exit_cost

    result.update(
        {
            "exit_filled_quantity": filled_quantity,
            "unfilled_exit_quantity": remaining_quantity,
            "exit_fill_count": len(exit_fills),
            "partial_exit": bool(
                exit_fills
                and (
                    len(exit_fills) > 1
                    or any(int(fill["quantity"]) < total_quantity for fill in exit_fills)
                )
            ),
            "exit_reason": terminal_reason,
            "entry_cost_rupees": entry_cost,
            "exit_cost_rupees": exit_cost,
            "cost_bps": config.entry_cost_bps + config.exit_cost_bps,
            "realized_gross_pnl_rupees": realized_gross,
            "realized_net_pnl_rupees": realized_net,
            "execution_fills_json": json.dumps(fills, separators=(",", ":")),
        }
    )
    if exit_fills:
        last_fill = exit_fills[-1]
        exit_ts = legacy._as_ist(last_fill["fill_ts"])
        result.update(
            {
                "exit_ts": exit_ts,
                "exit_premium": exit_value / filled_quantity,
                "exit_bar_volume": float(last_fill["bar_volume"]),
                "holding_minutes": float(
                    (exit_ts - legacy._as_ist(result["entry_ts"])).total_seconds()
                    / 60.0
                ),
                "exit_premium_value_rupees": exit_value,
            }
        )

    if remaining_quantity:
        result["execution_status"] = (
            "PARTIALLY_EXECUTED_OPEN_REMAINDER"
            if filled_quantity
            else "UNEXECUTED_OPEN_REMAINDER"
        )
        result["skip_reason"] = "EXIT_WINDOW_ENDED_WITH_OPEN_QUANTITY"
        result["gross_pnl_rupees"] = np.nan
        result["cost_proxy_rupees"] = np.nan
        result["net_pnl_rupees"] = np.nan
        result["gross_return_pct"] = np.nan
        result["net_return_pct"] = np.nan
        return result

    gross = exit_value - entry_value
    total_cost = entry_cost + exit_cost
    result.update(
        {
            "execution_status": "EXECUTED",
            "gross_pnl_rupees": gross,
            "cost_proxy_rupees": total_cost,
            "net_pnl_rupees": gross - total_cost,
            "gross_return_pct": gross / entry_value * 100.0,
            "net_return_pct": (gross - total_cost) / entry_value * 100.0,
        }
    )
    return result


def simulate_option_native_trade(
    row: pd.Series,
    candles: pd.DataFrame,
    variant: legacy.OptionExitVariant,
    config: OptionExecutionConfig,
) -> dict[str, Any]:
    """Replay one option trade with causal, whole-lot partial fills.

    Price events remain stop-first on an ambiguous candle. Once a protective
    stop or breakeven is triggered, any unfilled balance becomes a market exit
    and consumes capacity on later candles at their adverse open. Target
    balances remain limit orders until another protective event or square-off.
    """

    config.validate()
    result = _base_result(row, variant, config)
    if row.get("coverage_state") != legacy.DEFAULT_READY_COVERAGE_STATE:
        result["skip_reason"] = str(row.get("coverage_state", ""))
        return result
    if candles.empty:
        result["skip_reason"] = "MISSING_OPTION_CANDLE_FILE"
        return result

    entry_ts = legacy._as_ist(row["reference_entry_bar"])
    source_entry = float(pd.to_numeric(row.get("reference_entry_premium"), errors="coerce"))
    one_lot_value = pd.to_numeric(row.get("quantity"), errors="coerce")
    if not np.isfinite(source_entry) or source_entry <= 0:
        result.update(
            execution_status="SKIPPED_EXECUTION_INPUT",
            skip_reason="INVALID_REFERENCE_ENTRY_PREMIUM",
        )
        return result
    if pd.isna(one_lot_value) or not np.isfinite(float(one_lot_value)) or float(one_lot_value) <= 0:
        result.update(
            execution_status="SKIPPED_EXECUTION_INPUT",
            skip_reason="INVALID_ONE_LOT_QUANTITY",
        )
        return result

    entry = _round_up_tick(source_entry, config.tick_size) + (
        config.adverse_ticks_each_side * config.tick_size
    )
    one_lot_quantity = int(float(one_lot_value))
    entry_bar = _entry_candle(candles, entry_ts)
    if entry_bar is None:
        result.update(
            execution_status="SKIPPED_EXECUTION_INPUT",
            skip_reason="MISSING_EXACT_ENTRY_CANDLE",
        )
        return result
    entry_volume = float(entry_bar["volume"])
    lots, sizing_reason, sizing = _sized_lots(
        entry_premium=entry,
        one_lot_quantity=one_lot_quantity,
        entry_volume=entry_volume,
        config=config,
    )
    result.update(sizing)
    result["sizing_reason"] = sizing_reason
    if lots <= 0:
        result.update(
            execution_status="SKIPPED_ENTRY_SIZING_OR_CAPACITY",
            skip_reason=sizing_reason,
        )
        return result

    quantity = lots * one_lot_quantity
    stop = _round_down_tick(entry * (1.0 - config.initial_stop_pct / 100.0), config.tick_size)
    t1 = _round_nearest_tick(entry * (1.0 + config.first_target_pct / 100.0), config.tick_size)
    runner = _round_nearest_tick(entry * (1.0 + config.runner_target_pct / 100.0), config.tick_size)
    result.update(
        {
            "option_lots": lots,
            "quantity": quantity,
            "entry_ts": entry_ts,
            "entry_premium": entry,
            "entry_bar_volume": entry_volume,
            "entry_premium_outlay_rupees": entry * quantity,
            "initial_stop_premium": stop,
            "first_target_premium": t1,
            "runner_target_premium": runner,
            "unfilled_exit_quantity": quantity,
        }
    )

    entry_capacity_quantity = (
        _available_lots(entry_volume, one_lot_quantity, config.max_volume_participation)
        * one_lot_quantity
    )
    fills = [
        _fill_record(
            row=row,
            variant=variant,
            side="BUY",
            stamp=entry_ts,
            premium=entry,
            lots=lots,
            one_lot_quantity=one_lot_quantity,
            reason="ENTRY",
            bar_volume=entry_volume,
            capacity_quantity=entry_capacity_quantity,
            capacity_consumed_before=0,
            cost_bps=config.entry_cost_bps,
        )
    ]
    remaining = quantity
    used_quantity_by_stamp: dict[pd.Timestamp, int] = {entry_ts: quantity}
    exit_reasons: list[str] = []
    armed = False
    market_exit_reason: str | None = None

    def execute_fill(bar: dict[str, Any], raw_price: float, reason: str) -> int:
        nonlocal remaining
        stamp = legacy._as_ist(bar["timestamp"])
        capacity_quantity = (
            _available_lots(
                float(bar["volume"]), one_lot_quantity, config.max_volume_participation
            )
            * one_lot_quantity
        )
        consumed = used_quantity_by_stamp.get(stamp, 0)
        available = max(0, capacity_quantity - consumed)
        fill_lots = min(remaining // one_lot_quantity, available // one_lot_quantity)
        if fill_lots <= 0:
            return 0
        price = max(
            config.tick_size,
            _round_down_tick(
                raw_price - config.adverse_ticks_each_side * config.tick_size,
                config.tick_size,
            ),
        )
        fill = _fill_record(
            row=row,
            variant=variant,
            side="SELL",
            stamp=stamp,
            premium=price,
            lots=fill_lots,
            one_lot_quantity=one_lot_quantity,
            reason=reason,
            bar_volume=float(bar["volume"]),
            capacity_quantity=capacity_quantity,
            capacity_consumed_before=consumed,
            cost_bps=config.exit_cost_bps,
        )
        fills.append(fill)
        filled_quantity = fill_lots * one_lot_quantity
        used_quantity_by_stamp[stamp] = consumed + filled_quantity
        remaining -= filled_quantity
        exit_reasons.append(reason)
        return filled_quantity

    square_off = legacy.square_off_timestamp(
        row["day"], config.square_off_hhmm, config.square_off_execution_delay_minutes
    )
    path = candles.loc[
        candles["timestamp"].dt.date.eq(entry_ts.date())
        & candles["timestamp"].ge(entry_ts)
        & candles["timestamp"].lt(square_off)
        & candles["volume"].gt(0)
    ]
    for bar in path.to_dict("records"):
        low, high = float(bar["low"]), float(bar["high"])
        stamp = legacy._as_ist(bar["timestamp"])

        if market_exit_reason is not None:
            execute_fill(bar, float(bar["open"]), f"{market_exit_reason}_RESIDUAL")
            if remaining == 0:
                break
            continue

        if not armed:
            hit_stop, hit_t1 = low <= stop, high >= t1
            if hit_stop:
                result["same_bar_ambiguous"] = bool(hit_t1)
                result["stop_hit"] = True
                activation_bar = stamp == entry_ts
                raw_exit = stop
                if not activation_bar and float(bar["open"]) < stop:
                    raw_exit = float(bar["open"])
                    result["exit_gap_through"] = True
                    result["exit_gap_rupees_per_unit"] = stop - raw_exit
                market_exit_reason = (
                    "FULL_STOP_BEFORE_OR_WITH_T1" if hit_t1 else "FULL_STOP"
                )
                execute_fill(bar, raw_exit, market_exit_reason)
            elif hit_t1:
                result["target_hit"] = True
                if variant.t1_exits_full_lot:
                    execute_fill(bar, t1, "FULL_LOT_T1")
                else:
                    armed = True
                    if low <= entry:
                        result["breakeven_exit"] = True
                        market_exit_reason = "T1_THEN_BREAKEVEN_SAME_BAR"
                        raw_exit = entry
                        if stamp != entry_ts and float(bar["open"]) < entry:
                            raw_exit = float(bar["open"])
                            result["exit_gap_through"] = True
                            result["exit_gap_rupees_per_unit"] = entry - raw_exit
                        execute_fill(bar, raw_exit, market_exit_reason)
                    elif high >= runner:
                        result["runner_target_hit"] = True
                        execute_fill(bar, runner, "RUNNER_TARGET_SAME_BAR_AFTER_T1")
        else:
            hit_be, hit_runner = low <= entry, high >= runner
            if hit_be:
                result["same_bar_ambiguous"] = bool(hit_runner)
                result["breakeven_exit"] = True
                market_exit_reason = (
                    "BREAKEVEN_BEFORE_OR_WITH_RUNNER" if hit_runner else "T1_THEN_BREAKEVEN"
                )
                raw_exit = min(entry, float(bar["open"]))
                if raw_exit < entry:
                    result["exit_gap_through"] = True
                    result["exit_gap_rupees_per_unit"] = entry - raw_exit
                execute_fill(bar, raw_exit, market_exit_reason)
            elif hit_runner:
                result["runner_target_hit"] = True
                execute_fill(bar, runner, "RUNNER_TARGET")

        if remaining == 0:
            break

    if remaining:
        candidates = candles.loc[
            candles["timestamp"].dt.date.eq(entry_ts.date())
            & candles["timestamp"].ge(square_off)
            & candles["timestamp"].le(
                square_off + pd.Timedelta(minutes=config.max_square_off_delay_minutes)
            )
            & candles["volume"].gt(0)
        ]
        for bar in candidates.to_dict("records"):
            before = remaining
            execute_fill(bar, float(bar["open"]), "TIME_EXIT_PARTIAL_CAPACITY")
            if remaining < before:
                result["square_off_delay_minutes"] = float(
                    (legacy._as_ist(bar["timestamp"]) - square_off).total_seconds() / 60.0
                )
            if remaining == 0:
                break

    terminal_reason = "|".join(dict.fromkeys(exit_reasons))
    if remaining:
        terminal_reason = terminal_reason or (
            f"{market_exit_reason}_NO_CAPACITY" if market_exit_reason else "NO_EXIT_CAPACITY"
        )
    elif terminal_reason.endswith("TIME_EXIT_PARTIAL_CAPACITY"):
        terminal_reason = terminal_reason.replace(
            "TIME_EXIT_PARTIAL_CAPACITY", "TIME_EXIT_WITH_PARTIAL_FILLS"
        )
    return _finish_from_partial_fills(
        result,
        fills=fills,
        total_quantity=quantity,
        terminal_reason=terminal_reason,
        config=config,
    )


def add_excursions(
    trades: pd.DataFrame, candles_by_trade_id: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    out = trades.copy()
    for index, row in out.loc[out["execution_status"].eq("EXECUTED")].iterrows():
        candles = candles_by_trade_id.get(str(row["trade_id"]))
        if candles is None or candles.empty:
            continue
        entry_ts = legacy._as_ist(row["entry_ts"])
        exit_ts = legacy._as_ist(row["exit_ts"])
        path = candles.loc[
            candles["timestamp"].dt.date.eq(entry_ts.date())
            & candles["timestamp"].ge(entry_ts)
            & candles["timestamp"].le(exit_ts)
            & candles["volume"].gt(0)
        ]
        if path.empty:
            continue
        premium = float(row["entry_premium"])
        out.loc[index, "mfe_pct"] = (float(path["high"].max()) / premium - 1.0) * 100.0
        out.loc[index, "mae_pct"] = (float(path["low"].min()) / premium - 1.0) * 100.0
    return out


def fill_ledger_frame(trades: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_id",
        "variant",
        "side",
        "fill_ts",
        "fill_premium",
        "lots",
        "quantity",
        "reason",
        "bar_volume",
        "capacity_quantity",
        "capacity_consumed_before",
        "notional_rupees",
        "cost_rupees",
    ]
    records: list[dict[str, Any]] = []
    if "execution_fills_json" not in trades:
        return pd.DataFrame(columns=columns)
    for payload in trades["execution_fills_json"].fillna("[]"):
        try:
            parsed = json.loads(str(payload))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(parsed, list):
            records.extend(item for item in parsed if isinstance(item, dict))
    return pd.DataFrame.from_records(records, columns=columns)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        action="append",
        dest="data_roots",
        help=(
            "Derivative package root. Repeat for daily/incremental packages; "
            "later roots replace duplicate trade_id rows."
        ),
    )
    parser.add_argument(
        "--coverage-csv",
        type=Path,
        help="Audited coverage override (for example the V13-v6 causal selector output).",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--requested-lots", type=int, default=3)
    parser.add_argument("--risk-budget-rupees", type=float)
    parser.add_argument("--max-premium-outlay-rupees", type=float)
    parser.add_argument("--max-volume-participation", type=float, default=1.0)
    parser.add_argument("--entry-capacity-policy", choices=("reject", "resize"), default="reject")
    parser.add_argument("--entry-cost-bps", type=float, default=5.0)
    parser.add_argument("--exit-cost-bps", type=float, default=5.0)
    parser.add_argument("--tick-size", type=float, default=0.05)
    parser.add_argument("--adverse-ticks-each-side", type=int, default=0)
    parser.add_argument("--initial-stop-pct", type=float, default=17.5)
    parser.add_argument("--first-target-pct", type=float, default=22.5)
    parser.add_argument("--runner-target-pct", type=float, default=22.5)
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = OptionExecutionConfig(
        requested_lots=args.requested_lots,
        risk_budget_rupees=args.risk_budget_rupees,
        max_premium_outlay_rupees=args.max_premium_outlay_rupees,
        max_volume_participation=args.max_volume_participation,
        entry_capacity_policy=args.entry_capacity_policy,
        entry_cost_bps=args.entry_cost_bps,
        exit_cost_bps=args.exit_cost_bps,
        tick_size=args.tick_size,
        adverse_ticks_each_side=args.adverse_ticks_each_side,
        initial_stop_pct=args.initial_stop_pct,
        first_target_pct=args.first_target_pct,
        runner_target_pct=args.runner_target_pct,
    )
    config.validate()
    if args.coverage_csv is not None and args.data_roots:
        raise ValueError("Use either --coverage-csv or --data-root, not both")
    if args.coverage_csv is not None:
        coverage, source_records = load_explicit_coverage(args.coverage_csv)
    else:
        data_roots = args.data_roots or [DEFAULT_DATA_ROOT]
        coverage, source_records = load_coverage_roots(data_roots)
    cache: dict[str, pd.DataFrame] = {}
    candles_by_trade_id: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for _, row in coverage.iterrows():
        symbol = str(row.get("option_tradingsymbol", "")).strip().upper()
        raw_options_dir = Path(str(row["_raw_options_dir"]))
        candles = (
            load_option_candles(symbol, raw_options_dir, cache)
            if symbol
            else pd.DataFrame()
        )
        candles_by_trade_id[str(row["trade_id"])] = candles
        for variant in legacy.OPTION_VARIANTS:
            rows.append(simulate_option_native_trade(row, candles, variant, config))
    trades = add_excursions(pd.DataFrame(rows), candles_by_trade_id)
    fills = fill_ledger_frame(trades)
    summary = legacy.summary_frame(trades, input_count=len(coverage))
    daily = legacy.daily_frame(trades)
    periods = legacy.period_frame(trades)

    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v6_options_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing V13-v6 options run: {run_dir}")
    run_dir.mkdir(parents=True)
    trades_path = run_dir / "fno_v13_v6_options_trades.csv"
    daily_path = run_dir / "fno_v13_v6_options_daily.csv"
    periods_path = run_dir / "fno_v13_v6_options_periods.csv"
    summary_path = run_dir / "fno_v13_v6_options_summary.csv"
    fills_path = run_dir / "fno_v13_v6_options_fills.csv"
    manifest_path = run_dir / "manifest.json"
    common.atomic_write_csv(trades, trades_path)
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_csv(periods, periods_path)
    common.atomic_write_csv(summary, summary_path)
    common.atomic_write_csv(fills, fills_path)
    parsed_days = pd.to_datetime(coverage.get("day"), errors="coerce")
    data_through_date = (
        parsed_days.max().date().isoformat() if parsed_days.notna().any() else None
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "data_sources": source_records,
        "deduplicated_coverage_rows": int(len(coverage)),
        "data_through_date": data_through_date,
        "config": asdict(config),
        "outputs": {
            "trades": {"path": str(trades_path), "sha256": _sha256(trades_path)},
            "daily": {"path": str(daily_path), "sha256": _sha256(daily_path)},
            "periods": {"path": str(periods_path), "sha256": _sha256(periods_path)},
            "summary": {"path": str(summary_path), "sha256": _sha256(summary_path)},
            "fills": {"path": str(fills_path), "sha256": _sha256(fills_path)},
        },
    }
    common.atomic_write_json(manifest_path, manifest)
    print(summary.to_string(index=False))
    print(f"[V13-v6 options][RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
