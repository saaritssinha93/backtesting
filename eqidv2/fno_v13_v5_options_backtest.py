"""Three-lot ATM option replay for the V13-v5 corrected signal ledger.

This runner does not change the V13-v5 stock signal logic.  It consumes the
derivative market-data package created by ``fno_v13_v5_derivative_data.py`` and
replays mapped September ATM CE/PE contracts with three exchange lots by default.

The original V13-v5 exit books 10% at T1 and keeps 90% for a runner.  Option
lots cannot be split below the exchange lot size, so this runner reports two valid full-position
variants:

* OPTION_FULL_LOT_AT_T1: exit the full position at +22.5%, stop at -17.5%, or time.
* OPTION_T1_ARM_BE_RUNNER: T1 only arms breakeven, then full position seeks +22.5%.

It also reports CASH_EVENT_REPRICE_STOCK_EXIT as a reference: buy the mapped
option at the causal stock-entry mark and sell it at the causal stock-exit mark.
That reference is not an option-native stop/target backtest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


SCHEMA_VERSION = "FNO_V13_V5_OPTIONS_BACKTEST_V2"
DATASET_VERSION = "FNO_V13_V5_DERIVATIVE_MARKET_DATA_V1"

V13_V5_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v5"
DATA_ROOT = V13_V5_ROOT / "derivative_market_data"
OPTION_COVERAGE_PATH = DATA_ROOT / "audit" / "option_trade_coverage_and_capital.csv"
RAW_OPTIONS_DIR = DATA_ROOT / "raw_options_1m"
OUTPUT_ROOT = V13_V5_ROOT / "options_backtest"
TRADES_PATH = OUTPUT_ROOT / "fno_v13_v5_options_backtest_trades.csv"
DAILY_PATH = OUTPUT_ROOT / "fno_v13_v5_options_backtest_daily.csv"
SUMMARY_PATH = OUTPUT_ROOT / "fno_v13_v5_options_backtest_summary.csv"
PERIOD_PATH = OUTPUT_ROOT / "fno_v13_v5_options_backtest_periods.csv"
PROVENANCE_PATH = OUTPUT_ROOT / "fno_v13_v5_options_backtest_provenance.json"
REPORT_PATH = OUTPUT_ROOT / "V13_V5_OPTIONS_BACKTEST_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name("V13_V5_OPTIONS_BACKTEST_RESULTS.md")

DEFAULT_READY_COVERAGE_STATE = "READY"
DEFAULT_COST_BPS = 5.0
DEFAULT_OPTION_LOTS = 3
DEFAULT_INITIAL_STOP_PCT = 17.5
DEFAULT_FIRST_TARGET_PCT = 22.5
DEFAULT_RUNNER_TARGET_PCT = 22.5
DEFAULT_SQUARE_OFF_HHMM = "15:15"
DEFAULT_SQUARE_OFF_EXECUTION_DELAY_MINUTES = 1
DEFAULT_MAX_SQUARE_OFF_DELAY_MINUTES = 15
TRAIN_END = date(2026, 8, 13)
VALIDATION_END = date(2026, 8, 26)


@dataclass(frozen=True)
class OptionExitVariant:
    name: str
    description: str
    t1_exits_full_lot: bool
    t1_arms_breakeven: bool


OPTION_VARIANTS: tuple[OptionExitVariant, ...] = (
    OptionExitVariant(
        name="OPTION_FULL_LOT_AT_T1",
        description="Full option position exits at the first target.",
        t1_exits_full_lot=True,
        t1_arms_breakeven=False,
    ),
    OptionExitVariant(
        name="OPTION_T1_ARM_BE_RUNNER",
        description="Full option position keeps running after T1; T1 arms breakeven.",
        t1_exits_full_lot=False,
        t1_arms_breakeven=True,
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _period(day_value: Any) -> str:
    day = pd.Timestamp(day_value).date()
    if day <= TRAIN_END:
        return "TRAIN"
    if day <= VALIDATION_END:
        return "VALIDATION"
    return "PSEUDO_TEST"


def _option_path(symbol: str, root: Path = RAW_OPTIONS_DIR) -> Path:
    return root / f"{common.safe_contract_stem(symbol)}_1minute.parquet"


def normalize_option_candles(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
        )
    out = frame.copy()
    if "timestamp" not in out.columns:
        raise ValueError("Option candles require a timestamp column.")
    out["timestamp"] = common._to_ist(out["timestamp"])
    for column in ("open", "high", "low", "close", "volume"):
        if column not in out.columns:
            raise ValueError(f"Option candles missing {column!r}.")
        out[column] = pd.to_numeric(out[column], errors="coerce")
    if "oi" not in out.columns:
        out["oi"] = np.nan
    out["oi"] = pd.to_numeric(out["oi"], errors="coerce")
    valid = out["timestamp"].notna() & out["volume"].ge(0)
    for column in ("open", "high", "low", "close"):
        valid &= out[column].gt(0) & np.isfinite(out[column])
    return (
        out.loc[valid, ["timestamp", "open", "high", "low", "close", "volume", "oi"]]
        .drop_duplicates("timestamp", keep="last")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )


def load_option_candles(symbol: str, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    key = str(symbol).strip().upper()
    if key not in cache:
        path = _option_path(key)
        cache[key] = normalize_option_candles(pd.read_parquet(path)) if path.exists() else pd.DataFrame()
    return cache[key]


def first_traded_open_at_or_after(
    candles: pd.DataFrame,
    event_ts: Any,
    *,
    max_delay_minutes: int,
) -> tuple[pd.Series | None, float]:
    if candles.empty:
        return None, math.nan
    event = _as_ist(event_ts)
    candidates = candles.loc[
        candles["timestamp"].dt.date.eq(event.date())
        & candles["timestamp"].ge(event)
        & candles["timestamp"].le(event + pd.Timedelta(minutes=max_delay_minutes))
        & candles["volume"].gt(0)
    ]
    if candidates.empty:
        return None, math.nan
    row = candidates.iloc[0]
    delay = float((row["timestamp"] - event).total_seconds() / 60.0)
    return row, delay


def square_off_timestamp(day_value: Any, hhmm: str, execution_delay_minutes: int) -> pd.Timestamp:
    normalized = str(hhmm).replace(":", "").strip()
    hour = int(normalized[:2])
    minute = int(normalized[2:])
    base = pd.Timestamp(pd.Timestamp(day_value).date()).tz_localize(common.IST)
    return base + pd.Timedelta(hours=hour, minutes=minute + execution_delay_minutes)


def _base_result(
    row: pd.Series,
    variant_name: str,
    *,
    option_lots: int = DEFAULT_OPTION_LOTS,
    initial_stop_pct: float = DEFAULT_INITIAL_STOP_PCT,
    first_target_pct: float = DEFAULT_FIRST_TARGET_PCT,
    runner_target_pct: float = DEFAULT_RUNNER_TARGET_PCT,
) -> dict[str, Any]:
    base_quantity = pd.to_numeric(row.get("quantity"), errors="coerce")
    effective_quantity = (
        int(float(base_quantity) * int(option_lots))
        if pd.notna(base_quantity) and np.isfinite(float(base_quantity))
        else np.nan
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
        "exit_variant": variant_name,
        "trade_id": row.get("trade_id", ""),
        "sid": row.get("sid", np.nan),
        "day": pd.Timestamp(row.get("day")).date(),
        "period": _period(row.get("day")),
        "profile": row.get("profile", ""),
        "strategy_version": row.get("strategy_version", ""),
        "equity_symbol": row.get("equity_symbol", ""),
        "equity_side": row.get("equity_side", ""),
        "option_tradingsymbol": row.get("option_tradingsymbol", ""),
        "option_type": row.get("required_option_type", ""),
        "option_strike": pd.to_numeric(row.get("option_strike"), errors="coerce"),
        "lot_size": pd.to_numeric(row.get("lot_size"), errors="coerce"),
        "option_lots": int(option_lots),
        "one_lot_quantity": base_quantity,
        "quantity": effective_quantity,
        "coverage_state": row.get("coverage_state", ""),
        "mapping_status": row.get("mapping_status", ""),
        "execution_status": "SKIPPED_NOT_READY_COVERAGE",
        "skip_reason": "",
        "entry_ts": pd.NaT,
        "entry_premium": np.nan,
        "exit_ts": pd.NaT,
        "exit_premium": np.nan,
        "exit_reason": "",
        "holding_minutes": np.nan,
        "initial_stop_pct": float(initial_stop_pct),
        "first_target_pct": float(first_target_pct),
        "runner_target_pct": float(runner_target_pct),
        "initial_stop_premium": np.nan,
        "first_target_premium": np.nan,
        "runner_target_premium": np.nan,
        "target_hit": False,
        "runner_target_hit": False,
        "stop_hit": False,
        "breakeven_exit": False,
        "same_bar_ambiguous": False,
        "mfe_pct": np.nan,
        "mae_pct": np.nan,
        "entry_bar_volume": np.nan,
        "exit_bar_volume": np.nan,
        "entry_premium_outlay_rupees": np.nan,
        "exit_premium_value_rupees": np.nan,
        "gross_pnl_rupees": np.nan,
        "cost_bps": DEFAULT_COST_BPS,
        "cost_proxy_rupees": np.nan,
        "net_pnl_rupees": np.nan,
        "gross_return_pct": np.nan,
        "net_return_pct": np.nan,
    }


def _finish_executed(
    result: dict[str, Any],
    *,
    exit_ts: Any,
    exit_premium: float,
    exit_reason: str,
    exit_bar_volume: float,
    quantity: int,
    cost_bps: float,
) -> dict[str, Any]:
    entry = float(result["entry_premium"])
    outlay = entry * quantity
    exit_value = float(exit_premium) * quantity
    gross = exit_value - outlay
    cost = outlay * float(cost_bps) / 10_000.0
    net = gross - cost
    result.update(
        {
            "execution_status": "EXECUTED",
            "exit_ts": _as_ist(exit_ts),
            "exit_premium": float(exit_premium),
            "exit_reason": exit_reason,
            "exit_bar_volume": float(exit_bar_volume),
            "holding_minutes": float(
                (_as_ist(exit_ts) - _as_ist(result["entry_ts"])).total_seconds() / 60.0
            ),
            "exit_premium_value_rupees": exit_value,
            "gross_pnl_rupees": gross,
            "cost_bps": float(cost_bps),
            "cost_proxy_rupees": cost,
            "net_pnl_rupees": net,
            "gross_return_pct": gross / outlay * 100.0 if outlay else np.nan,
            "net_return_pct": net / outlay * 100.0 if outlay else np.nan,
        }
    )
    return result


def simulate_option_native_trade(
    row: pd.Series,
    candles: pd.DataFrame,
    variant: OptionExitVariant,
    *,
    cost_bps: float,
    option_lots: int = DEFAULT_OPTION_LOTS,
    initial_stop_pct: float = DEFAULT_INITIAL_STOP_PCT,
    first_target_pct: float = DEFAULT_FIRST_TARGET_PCT,
    runner_target_pct: float = DEFAULT_RUNNER_TARGET_PCT,
    square_off_hhmm: str = DEFAULT_SQUARE_OFF_HHMM,
    square_off_execution_delay_minutes: int = DEFAULT_SQUARE_OFF_EXECUTION_DELAY_MINUTES,
    max_square_off_delay_minutes: int = DEFAULT_MAX_SQUARE_OFF_DELAY_MINUTES,
) -> dict[str, Any]:
    result = _base_result(
        row,
        variant.name,
        option_lots=option_lots,
        initial_stop_pct=initial_stop_pct,
        first_target_pct=first_target_pct,
        runner_target_pct=runner_target_pct,
    )
    if row.get("coverage_state") != DEFAULT_READY_COVERAGE_STATE:
        result["skip_reason"] = str(row.get("coverage_state", ""))
        return result
    if candles.empty:
        result["skip_reason"] = "MISSING_OPTION_CANDLE_FILE"
        return result

    quantity = int(float(result["quantity"]))
    entry_ts = _as_ist(row["reference_entry_bar"])
    entry_premium = float(row["reference_entry_premium"])
    result.update(
        {
            "entry_ts": entry_ts,
            "entry_premium": entry_premium,
            "entry_bar_volume": float(row.get("entry_bar_volume", np.nan)),
            "entry_premium_outlay_rupees": entry_premium * quantity,
            "initial_stop_premium": entry_premium * (1.0 - float(initial_stop_pct) / 100.0),
            "first_target_premium": entry_premium * (1.0 + float(first_target_pct) / 100.0),
            "runner_target_premium": entry_premium * (1.0 + float(runner_target_pct) / 100.0),
        }
    )

    stop = float(result["initial_stop_premium"])
    t1 = float(result["first_target_premium"])
    runner = float(result["runner_target_premium"])
    square_off = square_off_timestamp(
        row["day"], square_off_hhmm, square_off_execution_delay_minutes
    )
    path = candles.loc[
        candles["timestamp"].dt.date.eq(entry_ts.date())
        & candles["timestamp"].ge(entry_ts)
        & candles["timestamp"].lt(square_off)
        & candles["volume"].gt(0)
    ].copy()
    if path.empty:
        result["skip_reason"] = "NO_POSITIVE_VOLUME_OPTION_PATH_AFTER_ENTRY"
        return result

    armed = False
    first_target_ts = pd.NaT
    for candle in path.to_dict("records"):
        low = float(candle["low"])
        high = float(candle["high"])
        stamp = candle["timestamp"]
        volume = float(candle["volume"])
        if not armed:
            hit_stop = low <= stop
            hit_t1 = high >= t1
            if hit_stop and hit_t1:
                result["same_bar_ambiguous"] = True
                result["stop_hit"] = True
                return _finish_executed(
                    result,
                    exit_ts=stamp,
                    exit_premium=stop,
                    exit_reason="FULL_STOP_BEFORE_OR_WITH_T1",
                    exit_bar_volume=volume,
                    quantity=quantity,
                    cost_bps=cost_bps,
                )
            if hit_stop:
                result["stop_hit"] = True
                return _finish_executed(
                    result,
                    exit_ts=stamp,
                    exit_premium=stop,
                    exit_reason="FULL_STOP",
                    exit_bar_volume=volume,
                    quantity=quantity,
                    cost_bps=cost_bps,
                )
            if hit_t1:
                result["target_hit"] = True
                first_target_ts = stamp
                if variant.t1_exits_full_lot:
                    return _finish_executed(
                        result,
                        exit_ts=stamp,
                        exit_premium=t1,
                        exit_reason="FULL_LOT_T1",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        cost_bps=cost_bps,
                    )
                armed = True
                if low <= entry_premium:
                    result["breakeven_exit"] = True
                    return _finish_executed(
                        result,
                        exit_ts=stamp,
                        exit_premium=entry_premium,
                        exit_reason="T1_THEN_BREAKEVEN_SAME_BAR",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        cost_bps=cost_bps,
                    )
                if high >= runner:
                    result["runner_target_hit"] = True
                    return _finish_executed(
                        result,
                        exit_ts=stamp,
                        exit_premium=runner,
                        exit_reason="RUNNER_TARGET_SAME_BAR_AFTER_T1",
                        exit_bar_volume=volume,
                        quantity=quantity,
                        cost_bps=cost_bps,
                    )
        else:
            hit_be = low <= entry_premium
            hit_runner = high >= runner
            if hit_be and hit_runner:
                result["same_bar_ambiguous"] = True
                result["breakeven_exit"] = True
                return _finish_executed(
                    result,
                    exit_ts=stamp,
                    exit_premium=entry_premium,
                    exit_reason="BREAKEVEN_BEFORE_OR_WITH_RUNNER",
                    exit_bar_volume=volume,
                    quantity=quantity,
                    cost_bps=cost_bps,
                )
            if hit_be:
                result["breakeven_exit"] = True
                return _finish_executed(
                    result,
                    exit_ts=stamp,
                    exit_premium=entry_premium,
                    exit_reason="T1_THEN_BREAKEVEN",
                    exit_bar_volume=volume,
                    quantity=quantity,
                    cost_bps=cost_bps,
                )
            if hit_runner:
                result["runner_target_hit"] = True
                return _finish_executed(
                    result,
                    exit_ts=stamp,
                    exit_premium=runner,
                    exit_reason="RUNNER_TARGET",
                    exit_bar_volume=volume,
                    quantity=quantity,
                    cost_bps=cost_bps,
                )

    exit_row, delay = first_traded_open_at_or_after(
        candles, square_off, max_delay_minutes=max_square_off_delay_minutes
    )
    if exit_row is None:
        result["execution_status"] = "NO_SQUARE_OFF_CANDLE"
        result["skip_reason"] = "NO_POSITIVE_VOLUME_OPTION_OPEN_AT_OR_AFTER_SQUARE_OFF"
        return result
    result["target_hit"] = bool(result["target_hit"])
    result["runner_target_hit"] = bool(result["runner_target_hit"])
    result["square_off_delay_minutes"] = delay
    if pd.notna(first_target_ts):
        result["first_target_ts"] = first_target_ts
    return _finish_executed(
        result,
        exit_ts=exit_row["timestamp"],
        exit_premium=float(exit_row["open"]),
        exit_reason="TIME_EXIT_1515_PLUS_1M_NO_NATIVE_EXIT",
        exit_bar_volume=float(exit_row["volume"]),
        quantity=quantity,
        cost_bps=cost_bps,
    )


def cash_event_reference_trade(
    row: pd.Series,
    *,
    cost_bps: float,
    option_lots: int = DEFAULT_OPTION_LOTS,
) -> dict[str, Any]:
    result = _base_result(
        row,
        "CASH_EVENT_REPRICE_STOCK_EXIT",
        option_lots=option_lots,
        initial_stop_pct=np.nan,
        first_target_pct=np.nan,
        runner_target_pct=np.nan,
    )
    if row.get("coverage_state") != DEFAULT_READY_COVERAGE_STATE:
        result["skip_reason"] = str(row.get("coverage_state", ""))
        return result
    quantity = int(float(result["quantity"]))
    entry = float(row["reference_entry_premium"])
    exit_ = float(row["reference_exit_premium"])
    result.update(
        {
            "entry_ts": _as_ist(row["reference_entry_bar"]),
            "entry_premium": entry,
            "entry_bar_volume": float(row.get("entry_bar_volume", np.nan)),
            "entry_premium_outlay_rupees": entry * quantity,
            "target_hit": False,
            "runner_target_hit": False,
            "stop_hit": False,
        }
    )
    return _finish_executed(
        result,
        exit_ts=row["reference_exit_bar"],
        exit_premium=exit_,
        exit_reason="STOCK_STRATEGY_EXIT_REPRICE",
        exit_bar_volume=np.nan,
        quantity=quantity,
        cost_bps=cost_bps,
    )


def add_excursions(trades: pd.DataFrame, option_cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = trades.copy()
    for index, row in out.loc[out["execution_status"].eq("EXECUTED")].iterrows():
        symbol = str(row["option_tradingsymbol"])
        candles = option_cache.get(symbol)
        if candles is None:
            candles = load_option_candles(symbol, option_cache)
        if candles.empty:
            continue
        entry = _as_ist(row["entry_ts"])
        exit_ = _as_ist(row["exit_ts"])
        path = candles.loc[
            candles["timestamp"].dt.date.eq(entry.date())
            & candles["timestamp"].ge(entry)
            & candles["timestamp"].le(exit_)
            & candles["volume"].gt(0)
        ]
        if path.empty:
            continue
        premium = float(row["entry_premium"])
        out.loc[index, "mfe_pct"] = (float(path["high"].max()) / premium - 1.0) * 100.0
        out.loc[index, "mae_pct"] = (float(path["low"].min()) / premium - 1.0) * 100.0
    return out


def peak_concurrent_premium(trades: pd.DataFrame) -> tuple[float, int, str]:
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    if executed.empty:
        return 0.0, 0, ""
    events: list[tuple[pd.Timestamp, int, float, int]] = []
    for row in executed.to_dict("records"):
        outlay = float(row["entry_premium_outlay_rupees"])
        events.append((_as_ist(row["entry_ts"]), 1, outlay, 1))
        events.append((_as_ist(row["exit_ts"]), 0, -outlay, -1))
    current_cash = 0.0
    current_positions = 0
    peak_cash = 0.0
    peak_positions = 0
    peak_stamp = ""
    for stamp, ordering, cash_delta, position_delta in sorted(events, key=lambda item: (item[0], item[1])):
        current_cash += cash_delta
        current_positions += position_delta
        if current_cash > peak_cash:
            peak_cash = current_cash
            peak_positions = current_positions
            peak_stamp = stamp.isoformat()
    return float(peak_cash), int(peak_positions), peak_stamp


def profit_factor(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(clean.loc[clean > 0].sum())
    losses = float(-clean.loc[clean < 0].sum())
    if losses:
        return gains / losses
    return float("inf") if gains else float("nan")


def summary_frame(trades: pd.DataFrame, input_count: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for variant, group in trades.groupby("exit_variant", sort=False):
        executed = group.loc[group["execution_status"].eq("EXECUTED")].copy()
        pnl = pd.to_numeric(executed["net_pnl_rupees"], errors="coerce")
        gross = pd.to_numeric(executed["gross_pnl_rupees"], errors="coerce")
        outlay = pd.to_numeric(executed["entry_premium_outlay_rupees"], errors="coerce")
        trading_days = int(executed["day"].nunique(dropna=True))
        peak_cash, peak_positions, peak_stamp = peak_concurrent_premium(group)
        rows.append(
            {
                "exit_variant": variant,
                "input_v13_v5_trades": int(input_count),
                "backtest_rows": int(len(group)),
                "executed_option_trades": int(len(executed)),
                "trading_days_with_executed_trades": trading_days,
                "average_trades_per_trading_day": (
                    float(len(executed) / trading_days) if trading_days else np.nan
                ),
                "coverage_pct_of_v13_v5_trades": (
                    len(executed) / input_count * 100.0 if input_count else 0.0
                ),
                "wins": int((pnl > 0).sum()),
                "losses": int((pnl < 0).sum()),
                "breakeven": int((pnl == 0).sum()),
                "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else np.nan,
                "target_hit_rate_pct": (
                    float(executed["target_hit"].astype(bool).mean() * 100.0)
                    if len(executed)
                    else np.nan
                ),
                "runner_target_hit_rate_pct": (
                    float(executed["runner_target_hit"].astype(bool).mean() * 100.0)
                    if len(executed)
                    else np.nan
                ),
                "stop_hit_rate_pct": (
                    float(executed["stop_hit"].astype(bool).mean() * 100.0)
                    if len(executed)
                    else np.nan
                ),
                "gross_pnl_rupees": float(gross.sum()) if len(gross) else 0.0,
                "cost_proxy_rupees": float(pd.to_numeric(executed["cost_proxy_rupees"], errors="coerce").sum()),
                "net_pnl_rupees": float(pnl.sum()) if len(pnl) else 0.0,
                "average_trade_net_pnl_rupees": float(pnl.mean()) if len(pnl) else np.nan,
                "average_trade_net_return_pct": (
                    float(pd.to_numeric(executed["net_return_pct"], errors="coerce").mean())
                    if len(executed)
                    else np.nan
                ),
                "profit_factor": profit_factor(pnl),
                "sum_entry_premium_outlay_rupees": float(outlay.sum()) if len(outlay) else 0.0,
                "net_return_on_sum_entry_premium_pct": (
                    float(pnl.sum() / outlay.sum() * 100.0) if len(outlay) and outlay.sum() else np.nan
                ),
                "peak_concurrent_premium_outlay_rupees": peak_cash,
                "maximum_concurrent_positions": peak_positions,
                "peak_concurrent_timestamp": peak_stamp,
                "net_return_on_peak_premium_pct": (
                    float(pnl.sum() / peak_cash * 100.0) if peak_cash else np.nan
                ),
                "same_bar_ambiguous_trades": int(executed["same_bar_ambiguous"].astype(bool).sum()) if len(executed) else 0,
                "status_counts": json.dumps(
                    {str(k): int(v) for k, v in group["execution_status"].value_counts(dropna=False).items()},
                    sort_keys=True,
                ),
            }
        )
    return pd.DataFrame(rows)


def daily_frame(trades: pd.DataFrame) -> pd.DataFrame:
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    if executed.empty:
        return pd.DataFrame()
    grouped = executed.groupby(["exit_variant", "day"], sort=True).agg(
        trades=("trade_id", "size"),
        wins=("net_pnl_rupees", lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())),
        losses=("net_pnl_rupees", lambda x: int((pd.to_numeric(x, errors="coerce") < 0).sum())),
        target_hits=("target_hit", lambda x: int(pd.Series(x).astype(bool).sum())),
        runner_target_hits=("runner_target_hit", lambda x: int(pd.Series(x).astype(bool).sum())),
        stop_hits=("stop_hit", lambda x: int(pd.Series(x).astype(bool).sum())),
        entry_premium_outlay_rupees=("entry_premium_outlay_rupees", "sum"),
        gross_pnl_rupees=("gross_pnl_rupees", "sum"),
        cost_proxy_rupees=("cost_proxy_rupees", "sum"),
        net_pnl_rupees=("net_pnl_rupees", "sum"),
    )
    out = grouped.reset_index()
    out["period"] = out["day"].map(_period)
    out["average_trade_net_pnl_rupees"] = out["net_pnl_rupees"] / out["trades"]
    out["cumulative_net_pnl_rupees"] = out.groupby("exit_variant")["net_pnl_rupees"].cumsum()
    out["drawdown_rupees"] = out.groupby("exit_variant")["cumulative_net_pnl_rupees"].transform(
        lambda values: values - np.maximum.accumulate(np.r_[0.0, values.to_numpy(float)])[1:]
    )
    return out


def period_frame(trades: pd.DataFrame) -> pd.DataFrame:
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    if executed.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for (variant, period), group in executed.groupby(["exit_variant", "period"], sort=True):
        pnl = pd.to_numeric(group["net_pnl_rupees"], errors="coerce")
        trading_days = int(group["day"].nunique(dropna=True))
        rows.append(
            {
                "exit_variant": variant,
                "period": period,
                "executed_option_trades": int(len(group)),
                "trading_days_with_executed_trades": trading_days,
                "average_trades_per_trading_day": (
                    float(len(group) / trading_days) if trading_days else np.nan
                ),
                "wins": int((pnl > 0).sum()),
                "losses": int((pnl < 0).sum()),
                "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else np.nan,
                "target_hit_rate_pct": float(group["target_hit"].astype(bool).mean() * 100.0),
                "runner_target_hit_rate_pct": float(group["runner_target_hit"].astype(bool).mean() * 100.0),
                "net_pnl_rupees": float(pnl.sum()),
                "average_trade_net_pnl_rupees": float(pnl.mean()) if len(pnl) else np.nan,
                "profit_factor": profit_factor(pnl),
            }
        )
    return pd.DataFrame(rows)


def render_report(
    *,
    coverage: pd.DataFrame,
    trades: pd.DataFrame,
    summary: pd.DataFrame,
    daily: pd.DataFrame,
    periods: pd.DataFrame,
    cost_bps: float,
    option_lots: int,
    initial_stop_pct: float,
    first_target_pct: float,
    runner_target_pct: float,
) -> str:
    coverage_counts = coverage["coverage_state"].value_counts(dropna=False).to_dict()
    lines = [
        "# V13-V5 ATM Options Backtest Results",
        "",
        "## What Was Coded",
        "",
        (
            "This is a separate options replay file, not a mutation of the V13-V5 "
            "stock-signal backtester. It consumes the derivative market-data package "
            f"and replays {option_lots} lot(s) of the mapped same-expiry ATM CE/PE contract."
        ),
        "",
        "## Coverage",
        "",
        f"- Source V13-V5 filled trades: {len(coverage)}",
        f"- Coverage states: `{json.dumps({str(k): int(v) for k, v in coverage_counts.items()}, sort_keys=True)}`",
        "- Headline option-native rows use only `READY` coverage. August options remain missing and were not substituted.",
        f"- Native option stop/target: stop `-{initial_stop_pct:g}%`, full-lot target `+{first_target_pct:g}%`, runner target `+{runner_target_pct:g}%`.",
        f"- Option sizing: `{option_lots}` lot(s) per executed trade; `quantity = one_lot_quantity * option_lots`.",
        f"- Cost proxy: {cost_bps:g} bps of entry premium outlay per executed option trade.",
        "- Investment columns are premium outlay for the configured lot count. `sum_entry_premium_outlay_rupees` is turnover; `peak_concurrent_premium_outlay_rupees` is the maximum simultaneous capital needed.",
        "- `trading_days_with_executed_trades` counts unique dates with at least one executed option trade; `average_trades_per_trading_day` is executed trades divided by those dates.",
        "",
        "## Headline Summary",
        "",
        summary.to_markdown(index=False, floatfmt=".3f"),
        "",
        "## Period Summary",
        "",
        periods.to_markdown(index=False, floatfmt=".3f") if not periods.empty else "No executed rows.",
        "",
        "## Day-Wise Results",
        "",
        daily.to_markdown(index=False, floatfmt=".3f") if not daily.empty else "No executed rows.",
        "",
        "## Important Limits",
        "",
        "- This is gross option-premium OHLC replay with a simple flat cost proxy, not broker contract-note charges.",
        "- Historical bid/ask spread and market depth are unavailable, so fill quality and impact are not modeled.",
        "- The sample is only the September-mapped READY subset; it is not enough to validate an options strategy.",
        "- `CASH_EVENT_REPRICE_STOCK_EXIT` is a reference only; it follows the stock strategy's exit time instead of option-native targets/stops.",
        "",
        "## Output Files",
        "",
        f"- Trades: `{TRADES_PATH}`",
        f"- Daily: `{DAILY_PATH}`",
        f"- Summary: `{SUMMARY_PATH}`",
        f"- Periods: `{PERIOD_PATH}`",
        f"- Provenance: `{PROVENANCE_PATH}`",
    ]
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    parser.add_argument("--option-lots", type=int, default=DEFAULT_OPTION_LOTS)
    parser.add_argument("--initial-stop-pct", type=float, default=DEFAULT_INITIAL_STOP_PCT)
    parser.add_argument("--first-target-pct", type=float, default=DEFAULT_FIRST_TARGET_PCT)
    parser.add_argument("--runner-target-pct", type=float, default=DEFAULT_RUNNER_TARGET_PCT)
    parser.add_argument("--square-off-hhmm", default=DEFAULT_SQUARE_OFF_HHMM)
    parser.add_argument(
        "--square-off-execution-delay-minutes",
        type=int,
        default=DEFAULT_SQUARE_OFF_EXECUTION_DELAY_MINUTES,
    )
    parser.add_argument(
        "--max-square-off-delay-minutes",
        type=int,
        default=DEFAULT_MAX_SQUARE_OFF_DELAY_MINUTES,
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if float(args.initial_stop_pct) <= 0:
        raise ValueError("--initial-stop-pct must be positive.")
    if int(args.option_lots) <= 0:
        raise ValueError("--option-lots must be a positive integer.")
    if float(args.first_target_pct) <= 0:
        raise ValueError("--first-target-pct must be positive.")
    if float(args.runner_target_pct) <= 0:
        raise ValueError("--runner-target-pct must be positive.")
    if float(args.runner_target_pct) < float(args.first_target_pct):
        raise ValueError("--runner-target-pct must be greater than or equal to --first-target-pct.")
    started = time.monotonic()
    data_root = args.data_root.resolve()
    output_root = args.output_root.resolve()
    coverage_path = data_root / "audit" / "option_trade_coverage_and_capital.csv"
    raw_options_dir = data_root / "raw_options_1m"
    if not coverage_path.exists():
        raise FileNotFoundError(f"Missing option coverage file: {coverage_path}")
    coverage = pd.read_csv(coverage_path)
    coverage["day"] = pd.to_datetime(coverage["day"], errors="coerce").dt.date

    global RAW_OPTIONS_DIR, OUTPUT_ROOT, TRADES_PATH, DAILY_PATH, SUMMARY_PATH, PERIOD_PATH
    global PROVENANCE_PATH, REPORT_PATH, WORKSPACE_REPORT_PATH
    RAW_OPTIONS_DIR = raw_options_dir
    OUTPUT_ROOT = output_root
    TRADES_PATH = output_root / "fno_v13_v5_options_backtest_trades.csv"
    DAILY_PATH = output_root / "fno_v13_v5_options_backtest_daily.csv"
    SUMMARY_PATH = output_root / "fno_v13_v5_options_backtest_summary.csv"
    PERIOD_PATH = output_root / "fno_v13_v5_options_backtest_periods.csv"
    PROVENANCE_PATH = output_root / "fno_v13_v5_options_backtest_provenance.json"
    REPORT_PATH = output_root / "V13_V5_OPTIONS_BACKTEST_RESULTS.md"

    option_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for _, row in coverage.iterrows():
        symbol = str(row.get("option_tradingsymbol", "")).strip().upper()
        candles = load_option_candles(symbol, option_cache) if symbol else pd.DataFrame()
        for variant in OPTION_VARIANTS:
            rows.append(
                simulate_option_native_trade(
                    row,
                    candles,
                    variant,
                    cost_bps=float(args.cost_bps),
                    option_lots=int(args.option_lots),
                    initial_stop_pct=float(args.initial_stop_pct),
                    first_target_pct=float(args.first_target_pct),
                    runner_target_pct=float(args.runner_target_pct),
                    square_off_hhmm=args.square_off_hhmm,
                    square_off_execution_delay_minutes=int(args.square_off_execution_delay_minutes),
                    max_square_off_delay_minutes=int(args.max_square_off_delay_minutes),
                )
            )
        rows.append(
            cash_event_reference_trade(
                row,
                cost_bps=float(args.cost_bps),
                option_lots=int(args.option_lots),
            )
        )

    trades = add_excursions(pd.DataFrame(rows), option_cache)
    summary = summary_frame(trades, input_count=len(coverage))
    daily = daily_frame(trades)
    periods = period_frame(trades)

    output_root.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(trades, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    common.atomic_write_csv(summary, SUMMARY_PATH)
    common.atomic_write_csv(periods, PERIOD_PATH)
    report = render_report(
        coverage=coverage,
        trades=trades,
        summary=summary,
        daily=daily,
        periods=periods,
        cost_bps=float(args.cost_bps),
        option_lots=int(args.option_lots),
        initial_stop_pct=float(args.initial_stop_pct),
        first_target_pct=float(args.first_target_pct),
        runner_target_pct=float(args.runner_target_pct),
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "data_root": str(data_root),
            "coverage_path": str(coverage_path),
            "coverage_sha256": _sha256(coverage_path),
            "raw_options_dir": str(raw_options_dir),
            "cost_bps": float(args.cost_bps),
            "option_lots": int(args.option_lots),
            "initial_stop_pct": float(args.initial_stop_pct),
            "first_target_pct": float(args.first_target_pct),
            "runner_target_pct": float(args.runner_target_pct),
            "square_off_hhmm": args.square_off_hhmm,
            "square_off_execution_delay_minutes": int(args.square_off_execution_delay_minutes),
            "max_square_off_delay_minutes": int(args.max_square_off_delay_minutes),
            "variants": [variant.__dict__ for variant in OPTION_VARIANTS],
            "outputs": {
                "trades": str(TRADES_PATH),
                "daily": str(DAILY_PATH),
                "summary": str(SUMMARY_PATH),
                "periods": str(PERIOD_PATH),
                "report": str(REPORT_PATH),
            },
            "summary": summary.to_dict("records"),
            "warning": (
                "Only READY September option mappings are executed; missing August options "
                "are explicit and not substituted."
            ),
        },
    )

    for row in summary.to_dict("records"):
        print(
            f"[V13-v5 options] {row['exit_variant']}: "
            f"executed={row['executed_option_trades']} "
            f"days={row['trading_days_with_executed_trades']} "
            f"trades/day={row['average_trades_per_trading_day']:.3f} "
            f"WR={row['win_rate_pct']:.3f}% "
            f"target={row['target_hit_rate_pct']:.3f}% "
            f"PF={row['profit_factor']:.6f} "
            f"net={row['net_pnl_rupees']:+.2f} "
            f"avg={row['average_trade_net_pnl_rupees']:+.2f} "
            f"sum_outlay={row['sum_entry_premium_outlay_rupees']:.2f} "
            f"peak_outlay={row['peak_concurrent_premium_outlay_rupees']:.2f}",
            flush=True,
        )
    print(f"[V13-v5 options][REPORT] {REPORT_PATH}")
    print(f"[V13-v5 options][DONE] {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
