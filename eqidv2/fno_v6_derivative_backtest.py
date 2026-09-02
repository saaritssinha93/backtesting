"""Shared execution engine for the V6 futures and ATM-options backtests.

The original V6 strategy selects and manages trades from NSE cash-equity
prices; mapped NFO futures provide OI only.  This module deliberately leaves
that signal contract unchanged and reprices the resulting cash entry/exit
events in one exchange lot of a derivative:

* V6_F: LONG buys a future and SHORT sells a future.
* V6_O: LONG buys an ATM CE and SHORT buys an ATM PE.

No synthetic option prices, fallback lot sizes, or zero-volume fills are
allowed.  Results are written below an isolated research directory and never
to the protected canonical V6 output paths.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


SCHEMA_VERSION = "fno_v6_derivative_cash_event_execution_v1"
STRATEGY_VERSION = "FNO_V6_BEST_NET_CASH_EQUITY_20260811"
EXIT_POLICY = "PRESERVE_V6_CASH_ENTRY_AND_EXIT_EVENTS"
ATM_POLICY = "NEAREST_STRIKE_TO_CASH_ENTRY_PRICE_LOWER_STRIKE_ON_TIE"
LIQUIDITY_POLICY = "FIRST_POSITIVE_VOLUME_BAR_WITHIN_MAX_DELAY"

RESULT_ROOT = common.FNO_ROOT / "strategy_research" / "v6_derivative_backtests"
MARKET_CACHE_ROOT = RESULT_ROOT / "market_cache"
CANONICAL_AUDIT_PATH = (
    common.FNO_ROOT
    / "strategy_research"
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_trades.csv"
)
CANONICAL_DAILY_PATH = (
    common.FNO_ROOT
    / "strategy_research"
    / "ema_confirm_0925_0930_0935_0940_0945_v6_best_net_daily.csv"
)
PINNED_UNIVERSE_PATH = common.FNO_ROOT / "universe" / "near_month_2026-08-11.parquet"
FUTURES_1M_ROOT = common.FNO_ROOT / "raw_contracts_1m_hist"
PAPER_ORDER_ROOT = common.FNO_ROOT / "v6_live" / "orders" / "PAPER"

DEFAULT_HISTORY_FROM = date(2026, 5, 27)
DEFAULT_HISTORY_THROUGH = date(2026, 8, 10)
DEFAULT_PAPER_FROM = date(2026, 8, 27)


@dataclass(frozen=True)
class ChargeBreakdown:
    brokerage: float
    stt: float
    exchange_transaction: float
    sebi: float
    ipft: float
    stamp_duty: float
    gst: float
    total: float


def _as_ist(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(common.IST)
    return ts.tz_convert(common.IST)


def _parse_day(value: str | date | pd.Timestamp) -> date:
    return pd.Timestamp(value).date()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def option_type_for_side(side: str) -> str:
    normalized = str(side).strip().upper()
    if normalized == "LONG":
        return "CE"
    if normalized == "SHORT":
        return "PE"
    raise ValueError(f"Unsupported V6 side: {side!r}")


def estimate_zerodha_charges(
    *,
    instrument: str,
    side: str,
    entry_turnover: float,
    exit_turnover: float,
) -> ChargeBreakdown:
    """Estimate current NSE derivative charges for one normal-account trade.

    Rates reflect the post-2026-04-01 STT schedule.  Broker bills can differ
    slightly because taxes may be rounded/aggregated at contract-note level.
    """

    kind = str(instrument).strip().upper()
    direction = str(side).strip().upper()
    entry = float(entry_turnover)
    exit_ = float(exit_turnover)
    if min(entry, exit_) < 0 or not np.isfinite([entry, exit_]).all():
        raise ValueError("Turnover must be finite and non-negative.")
    turnover = entry + exit_

    if kind == "FUTURES":
        brokerage = min(20.0, entry * 0.0003) + min(20.0, exit_ * 0.0003)
        sell_turnover = exit_ if direction == "LONG" else entry
        stt = math.floor(sell_turnover * 0.0005 + 0.5)
        exchange_transaction = turnover * 0.0000183
        buy_turnover = entry if direction == "LONG" else exit_
        stamp_duty = buy_turnover * 0.00002
    elif kind == "OPTIONS":
        # V6_O always buys premium on entry and sells it on exit.
        brokerage = 40.0
        stt = math.floor(exit_ * 0.0015 + 0.5)
        exchange_transaction = turnover * 0.0003553
        stamp_duty = entry * 0.00003
    else:
        raise ValueError(f"Unsupported derivative instrument: {instrument!r}")

    sebi = turnover * 0.000001
    ipft = turnover * 0.000000001
    gst = (brokerage + exchange_transaction + sebi + ipft) * 0.18
    total = brokerage + stt + exchange_transaction + sebi + ipft + stamp_duty + gst
    return ChargeBreakdown(
        brokerage=float(brokerage),
        stt=float(stt),
        exchange_transaction=float(exchange_transaction),
        sebi=float(sebi),
        ipft=float(ipft),
        stamp_duty=float(stamp_duty),
        gst=float(gst),
        total=float(total),
    )


def normalize_candles(records: pd.DataFrame | Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    frame = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(list(records))
    if frame.empty:
        return pd.DataFrame(columns=["bar_start", "bar_end", "open", "high", "low", "close", "volume", "oi"])
    timestamp_column = "bar_start" if "bar_start" in frame.columns else "date"
    if timestamp_column not in frame.columns:
        timestamp_column = "ts" if "ts" in frame.columns else "timestamp"
    if timestamp_column not in frame.columns:
        raise ValueError("Derivative candles have no timestamp column.")
    for column in ("open", "high", "low", "close", "volume"):
        if column not in frame.columns:
            raise ValueError(f"Derivative candles missing {column!r}.")
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "oi" not in frame.columns:
        frame["oi"] = np.nan
    frame["oi"] = pd.to_numeric(frame["oi"], errors="coerce")
    parsed = pd.to_datetime(frame[timestamp_column], errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(common.IST)
    else:
        parsed = parsed.dt.tz_convert(common.IST)
    frame["bar_start"] = parsed
    frame["bar_end"] = parsed + pd.Timedelta(minutes=1)
    valid = frame["bar_start"].notna()
    for column in ("open", "high", "low", "close"):
        valid &= frame[column].gt(0) & np.isfinite(frame[column])
    frame = frame.loc[valid, ["bar_start", "bar_end", "open", "high", "low", "close", "volume", "oi"]]
    return frame.drop_duplicates("bar_start", keep="last").sort_values("bar_start").reset_index(drop=True)


def causal_execution_price(
    candles: pd.DataFrame,
    event_ts: Any,
    *,
    max_delay_minutes: int = 5,
    require_positive_volume: bool = True,
    allow_eod_close: bool = False,
) -> dict[str, Any] | None:
    """Return the first causally observable derivative price after an event.

    Cash historical timestamps are end-labelled.  A cash event at 09:42 may
    therefore use the derivative candle starting 09:42, never its preceding
    candle.  A second-level live event is rounded up to the next minute.  At
    the 15:30 exchange square-off only, the 15:29-15:30 close is allowed.
    """

    if candles.empty:
        return None
    event = _as_ist(event_ts)
    target_start = event.ceil("min")
    same_day = candles["bar_start"].dt.date.eq(event.date())
    # Exchange square-off is complete at 15:30.  Never drift into a 15:30+
    # post-market candle merely because the historical endpoint is inclusive.
    if allow_eod_close:
        prior = candles.loc[same_day & candles["bar_end"].eq(event)].copy()
        if require_positive_volume:
            prior = prior.loc[prior["volume"].gt(0)]
        if not prior.empty:
            row = prior.iloc[-1]
            return {
                "price": float(row["close"]),
                "execution_ts": row["bar_end"],
                "bar_volume": float(row["volume"]),
                "delay_minutes": 0.0,
                "price_field": "EOD_CLOSE",
            }
    candidates = candles.loc[
        same_day
        & candles["bar_start"].ge(target_start)
        & candles["bar_start"].le(target_start + pd.Timedelta(minutes=max_delay_minutes))
    ].copy()
    if require_positive_volume:
        candidates = candidates.loc[candidates["volume"].gt(0)]
    if not candidates.empty:
        row = candidates.iloc[0]
        return {
            "price": float(row["open"]),
            "execution_ts": row["bar_start"],
            "bar_volume": float(row["volume"]),
            "delay_minutes": float((row["bar_start"] - event).total_seconds() / 60.0),
            "price_field": "OPEN",
        }

    return None


def select_atm_option_contract(
    master: pd.DataFrame,
    *,
    underlying: str,
    session_date: date,
    cash_entry_price: float,
    side: str,
) -> pd.Series:
    option_type = option_type_for_side(side)
    required = {"name", "expiry", "strike", "instrument_type", "instrument_token", "tradingsymbol", "lot_size"}
    missing = required - set(master.columns)
    if missing:
        raise ValueError(f"Full NFO master missing columns: {sorted(missing)}")
    rows = master.loc[
        master["name"].astype(str).str.upper().str.strip().eq(str(underlying).upper().strip())
        & master["instrument_type"].astype(str).str.upper().eq(option_type)
        & pd.to_datetime(master["expiry"], errors="coerce").dt.date.ge(session_date)
    ].copy()
    rows["strike"] = pd.to_numeric(rows["strike"], errors="coerce")
    rows["lot_size"] = pd.to_numeric(rows["lot_size"], errors="coerce")
    rows = rows.loc[rows["strike"].gt(0) & rows["lot_size"].gt(0)]
    if rows.empty:
        raise LookupError(f"No active {option_type} contract for {underlying} on {session_date}.")
    nearest_expiry = pd.to_datetime(rows["expiry"]).min()
    rows = rows.loc[pd.to_datetime(rows["expiry"]).eq(nearest_expiry)].copy()
    rows["atm_distance"] = (rows["strike"] - float(cash_entry_price)).abs()
    rows = rows.sort_values(["atm_distance", "strike", "tradingsymbol"], kind="stable")
    selected = rows.iloc[0].copy()
    tied = rows.loc[np.isclose(rows["atm_distance"], float(selected["atm_distance"]), rtol=0.0, atol=1e-9)]
    selected["atm_tie"] = bool(len(tied) > 1)
    selected["atm_tie_candidate_strikes"] = ",".join(
        f"{float(value):g}" for value in sorted(tied["strike"].astype(float).unique())
    )
    return selected


def _load_cash_day(symbol: str, session_date: date, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    key = str(symbol).upper()
    if key not in cache:
        resolved = hybrid.resolve_backtest_equity_symbol(key)
        cache[key] = hybrid.load_equity_one_minute(resolved)
    frame = cache[key]
    return frame.loc[frame["ts"].dt.date.eq(session_date)].copy()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def reconstruct_v6_cash_events(audit: pd.DataFrame, *, cost_bps: float = 5.0) -> pd.DataFrame:
    """Recreate exact V6 cash fill and exit timestamps without changing signals."""

    cache: dict[str, pd.DataFrame] = {}
    records: list[dict[str, Any]] = []
    for trade in audit.to_dict("records"):
        record = dict(trade)
        record["input_filled"] = _truthy(trade.get("filled"))
        record["cash_event_status"] = "UNFILLED"
        if not record["input_filled"]:
            records.append(record)
            continue
        session_date = _parse_day(trade["day"])
        confirmation = _as_ist(trade["confirmation_ts"])
        minute = _load_cash_day(str(trade["tradingsymbol"]), session_date, cache)
        path = minute.loc[
            minute["ts"].gt(confirmation)
            & minute["ts"].le(confirmation.normalize() + pd.Timedelta(hours=15, minutes=30))
        ].reset_index(drop=True)
        if path.empty:
            record["cash_event_status"] = "MISSING_CASH_PATH"
            records.append(record)
            continue

        side = str(trade["side"]).upper()
        trigger = float(trade["trigger"])
        touched = np.flatnonzero(path["high"].to_numpy(float) >= trigger) if side == "LONG" else np.flatnonzero(path["low"].to_numpy(float) <= trigger)
        if touched.size == 0:
            record["cash_event_status"] = "RECONSTRUCTION_NO_FILL"
            records.append(record)
            continue
        entry_index = int(touched[0])
        stop_pct = float(trade["stop_pct"])
        target_pct = float(trade["target_pct"])
        if side == "LONG":
            stop_price = trigger * (1.0 - stop_pct / 100.0)
            target_price = trigger * (1.0 + target_pct / 100.0)
            stop_hits = np.flatnonzero(path["low"].to_numpy(float)[entry_index:] <= stop_price)
            target_hits = np.flatnonzero(path["high"].to_numpy(float)[entry_index:] >= target_price)
        else:
            stop_price = trigger * (1.0 + stop_pct / 100.0)
            target_price = trigger * (1.0 - target_pct / 100.0)
            stop_hits = np.flatnonzero(path["high"].to_numpy(float)[entry_index:] >= stop_price)
            target_hits = np.flatnonzero(path["low"].to_numpy(float)[entry_index:] <= target_price)
        missing_index = np.iinfo(np.int32).max
        stop_offset = int(stop_hits[0]) if stop_hits.size else missing_index
        target_offset = int(target_hits[0]) if target_hits.size else missing_index
        if stop_offset == target_offset == missing_index:
            exit_index = len(path) - 1
            exit_price = float(path.iloc[exit_index]["close"])
            exit_reason = "SQUARE_OFF"
        elif stop_offset <= target_offset:
            exit_index = entry_index + stop_offset
            exit_price = stop_price
            exit_reason = "STOP"
        else:
            exit_index = entry_index + target_offset
            exit_price = target_price
            exit_reason = "TARGET"
        gross_return = (exit_price / trigger - 1.0) if side == "LONG" else (1.0 - exit_price / trigger)
        reconstructed_net_pct = (gross_return - float(cost_bps) / 10000.0) * 100.0
        expected_net_pct = float(trade["net_return_pct"])
        record.update(
            {
                "cash_event_status": "READY",
                "cash_entry_event_ts": path.iloc[entry_index]["ts"],
                "cash_exit_event_ts": path.iloc[exit_index]["ts"],
                "cash_entry_price": trigger,
                "cash_exit_price": exit_price,
                "cash_exit_reason": exit_reason,
                "cash_path_quality": (
                    "EARLY_SOURCE_PATH_SQUARE_OFF"
                    if exit_reason == "SQUARE_OFF"
                    and not (path.iloc[exit_index]["ts"].hour == 15 and path.iloc[exit_index]["ts"].minute == 30)
                    else "COMPLETE_TO_EVENT"
                ),
                "cash_reconstructed_net_return_pct": reconstructed_net_pct,
                "cash_reconstruction_abs_error_pct": abs(reconstructed_net_pct - expected_net_pct),
            }
        )
        records.append(record)
    out = pd.DataFrame(records)
    ready = out.loc[out["cash_event_status"].eq("READY")]
    if not ready.empty and float(ready["cash_reconstruction_abs_error_pct"].max()) > 1e-7:
        raise AssertionError("Cash event reconstruction diverged from canonical V6 returns.")
    return out


def _load_archived_future(symbol: str, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    key = str(symbol).upper()
    if key not in cache:
        path = FUTURES_1M_ROOT / f"{common.safe_contract_stem(key)}_1minute.parquet"
        if not path.exists():
            cache[key] = pd.DataFrame()
        else:
            cache[key] = normalize_candles(pd.read_parquet(path))
    return cache[key]


def _execute_derivative_row(
    base: Mapping[str, Any],
    *,
    instrument: str,
    derivative_symbol: str,
    derivative_token: int,
    expiry: Any,
    strike: float | None,
    option_type: str | None,
    lot_size: int,
    tick_size: float,
    candles: pd.DataFrame,
    max_delay_minutes: int,
    require_positive_volume: bool = True,
) -> dict[str, Any]:
    row = dict(base)
    # PAPER order JSON uses these unqualified names for its cash execution.
    # Preserve them explicitly, then reserve the short names for derivatives
    # so a failed derivative mapping cannot masquerade as an executed fill.
    for field in (
        "entry_price",
        "exit_price",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "quantity",
        "lot_size",
        "tick_size",
    ):
        if field in row:
            row[f"source_cash_order_{field}"] = row[field]
    for field in (
        "entry_ts",
        "exit_ts",
        "entry_price",
        "exit_price",
        "entry_volume",
        "exit_volume",
        "entry_delay_minutes",
        "exit_delay_minutes",
        "gross_pnl_rs",
        "comparable_5bps_cost_rs",
        "comparable_5bps_net_pnl_rs",
        "estimated_net_pnl_rs",
        "entry_notional_or_premium_rs",
        "exit_notional_or_premium_rs",
    ):
        row[field] = np.nan
    row.update(
        {
            "schema_version": SCHEMA_VERSION,
            "execution_instrument": instrument,
            "derivative_tradingsymbol": derivative_symbol,
            "derivative_instrument_token": int(derivative_token),
            "derivative_expiry": pd.Timestamp(expiry).date().isoformat(),
            "derivative_strike": strike,
            "derivative_option_type": option_type,
            "lot_size": int(lot_size),
            "quantity": int(lot_size),
            "tick_size": float(tick_size),
            "exit_policy": EXIT_POLICY,
            "liquidity_policy": LIQUIDITY_POLICY if require_positive_volume else "OBSERVED_KITE_BAR_WITH_VOLUME_DISCLOSED",
            "execution_status": "MISSING_ENTRY_CANDLE",
        }
    )
    entry = causal_execution_price(
        candles,
        base["cash_entry_event_ts"],
        max_delay_minutes=max_delay_minutes,
        require_positive_volume=require_positive_volume,
    )
    if entry is None:
        return row
    exit_event = _as_ist(base["cash_exit_event_ts"])
    exit_fill = causal_execution_price(
        candles,
        exit_event,
        max_delay_minutes=max_delay_minutes,
        require_positive_volume=require_positive_volume,
        allow_eod_close=(exit_event.hour == 15 and exit_event.minute >= 30),
    )
    if exit_fill is None:
        row["execution_status"] = "MISSING_EXIT_CANDLE"
        return row

    entry_price = float(entry["price"])
    exit_price = float(exit_fill["price"])
    entry_turnover = entry_price * int(lot_size)
    exit_turnover = exit_price * int(lot_size)
    side = str(base["side"]).upper()
    if instrument == "FUTURES":
        gross_pnl = (exit_price - entry_price) * lot_size if side == "LONG" else (entry_price - exit_price) * lot_size
    else:
        gross_pnl = (exit_price - entry_price) * lot_size
    charges = estimate_zerodha_charges(
        instrument=instrument,
        side=side,
        entry_turnover=entry_turnover,
        exit_turnover=exit_turnover,
    )
    # Canonical V6 subtracts five basis points from entry-value return.
    comparable_cost = entry_turnover * 0.0005
    slippage_10bps_each_leg = (entry_turnover + exit_turnover) * 0.001
    row.update(
        {
            "execution_status": "EXECUTED",
            "entry_ts": entry["execution_ts"],
            "exit_ts": exit_fill["execution_ts"],
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_volume": entry["bar_volume"],
            "exit_volume": exit_fill["bar_volume"],
            "entry_delay_minutes": entry["delay_minutes"],
            "exit_delay_minutes": exit_fill["delay_minutes"],
            "entry_price_field": entry["price_field"],
            "exit_price_field": exit_fill["price_field"],
            "positive_volume_both_legs": bool(entry["bar_volume"] > 0 and exit_fill["bar_volume"] > 0),
            "entry_notional_or_premium_rs": entry_turnover,
            "exit_notional_or_premium_rs": exit_turnover,
            "gross_pnl_rs": gross_pnl,
            "comparable_5bps_cost_rs": comparable_cost,
            "comparable_5bps_net_pnl_rs": gross_pnl - comparable_cost,
            **{f"charge_{key}_rs": value for key, value in asdict(charges).items()},
            "estimated_net_pnl_rs": gross_pnl - charges.total,
            "net_after_10bps_each_leg_slippage_rs": gross_pnl - charges.total - slippage_10bps_each_leg,
            "return_on_entry_notional_or_premium_pct": (gross_pnl - charges.total) / entry_turnover * 100.0 if entry_turnover else np.nan,
        }
    )
    return row


def peak_concurrent_capital(trades: pd.DataFrame, capital_column: str) -> float:
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    if executed.empty:
        return 0.0
    events: list[tuple[pd.Timestamp, int, float]] = []
    for row in executed.to_dict("records"):
        capital = float(row[capital_column])
        events.append((_as_ist(row["entry_ts"]), 1, capital))
        events.append((_as_ist(row["exit_ts"]), 0, -capital))
    current = 0.0
    peak = 0.0
    for _, _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current += delta
        peak = max(peak, current)
    return float(peak)


def _profit_factor(values: np.ndarray) -> float:
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    return profit / loss if loss > 0 else (float("inf") if profit > 0 else float("nan"))


def summarize_derivative(trades: pd.DataFrame, *, instrument: str, sessions: int) -> dict[str, Any]:
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    pnl = pd.to_numeric(executed.get("estimated_net_pnl_rs"), errors="coerce").dropna().to_numpy(float)
    peak = peak_concurrent_capital(trades, "entry_notional_or_premium_rs")
    positive_both = executed.loc[executed.get("positive_volume_both_legs", False).eq(True)] if not executed.empty else executed
    positive_pnl = pd.to_numeric(positive_both.get("estimated_net_pnl_rs"), errors="coerce").dropna().to_numpy(float)
    early_path = (
        executed.loc[executed["cash_path_quality"].eq("EARLY_SOURCE_PATH_SQUARE_OFF")]
        if "cash_path_quality" in executed.columns
        else executed.iloc[0:0]
    )
    full_path = executed.drop(index=early_path.index)
    policies = sorted(set(executed.get("liquidity_policy", pd.Series(dtype=str)).dropna().astype(str)))
    if not executed.empty:
        position_frame = executed.copy()
        position_frame["_one_position"] = 1.0
        peak_positions = int(round(peak_concurrent_capital(position_frame, "_one_position")))
        daily_net = (
            executed.assign(_day=pd.to_datetime(executed["day"], errors="coerce").dt.date)
            .groupby("_day", sort=True)["estimated_net_pnl_rs"]
            .sum()
        )
        cumulative = daily_net.cumsum()
        running_peak = cumulative.cummax().clip(lower=0.0)
        max_daily_drawdown = float((running_peak - cumulative).max()) if len(cumulative) else 0.0
        positive_days = int((daily_net > 0).sum())
        negative_days = int((daily_net < 0).sum())
        zero_active_days = int((daily_net == 0).sum())
    else:
        peak_positions = 0
        max_daily_drawdown = 0.0
        positive_days = negative_days = zero_active_days = 0
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy_version": STRATEGY_VERSION,
        "instrument": instrument,
        "exit_policy": EXIT_POLICY,
        "liquidity_policy": policies[0] if len(policies) == 1 else policies,
        "sessions": int(sessions),
        "input_trades": int(len(trades)),
        "executed_trades": int(len(executed)),
        "coverage_pct": float(len(executed) / len(trades) * 100.0) if len(trades) else 0.0,
        "positive_volume_both_legs_trades": int(len(positive_both)),
        "positive_volume_both_legs_pct_of_executed": float(len(positive_both) / len(executed) * 100.0) if len(executed) else 0.0,
        "positive_volume_both_legs_net_pnl_rs": float(positive_pnl.sum()) if positive_pnl.size else 0.0,
        "early_source_path_squareoff_trades": int(len(early_path)),
        "full_path_sensitivity_trades": int(len(full_path)),
        "full_path_sensitivity_net_pnl_rs": float(pd.to_numeric(full_path.get("estimated_net_pnl_rs"), errors="coerce").sum()),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "profit_factor": _profit_factor(pnl),
        "gross_pnl_rs": float(pd.to_numeric(executed.get("gross_pnl_rs"), errors="coerce").sum()),
        "estimated_charges_rs": float(pd.to_numeric(executed.get("charge_total_rs"), errors="coerce").sum()),
        "estimated_net_pnl_rs": float(pnl.sum()) if pnl.size else 0.0,
        "comparable_5bps_net_pnl_rs": float(pd.to_numeric(executed.get("comparable_5bps_net_pnl_rs"), errors="coerce").sum()),
        "net_after_10bps_each_leg_slippage_rs": float(pd.to_numeric(executed.get("net_after_10bps_each_leg_slippage_rs"), errors="coerce").sum()),
        "sum_entry_notional_or_premium_rs": float(pd.to_numeric(executed.get("entry_notional_or_premium_rs"), errors="coerce").sum()),
        "peak_concurrent_notional_or_premium_rs": peak,
        "net_return_on_peak_pct": float(pnl.sum() / peak * 100.0) if peak > 0 else np.nan,
        "maximum_concurrent_positions": peak_positions,
        "positive_days": positive_days,
        "negative_days": negative_days,
        "flat_days": int(max(0, sessions - positive_days - negative_days - zero_active_days) + zero_active_days),
        "maximum_daily_equity_drawdown_rs": max_daily_drawdown,
        "status_counts": {str(k): int(v) for k, v in trades["execution_status"].value_counts(dropna=False).items()},
    }


def _normalise_master(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    required = ["instrument_token", "tradingsymbol", "name", "expiry", "strike", "tick_size", "lot_size", "instrument_type", "segment", "exchange"]
    for column in required:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame = frame[required].copy()
    for column in ("tradingsymbol", "name", "instrument_type", "segment", "exchange"):
        frame[column] = frame[column].astype("string").fillna("").str.upper().str.strip()
    frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce").dt.normalize()
    for column in ("instrument_token", "strike", "tick_size", "lot_size"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[
        frame["instrument_token"].gt(0)
        & frame["tradingsymbol"].ne("")
        & frame["expiry"].notna()
        & frame["segment"].isin(["NFO-FUT", "NFO-OPT"])
    ].copy()
    frame["instrument_token"] = frame["instrument_token"].astype("int64")
    return frame.drop_duplicates("tradingsymbol", keep="last").sort_values("tradingsymbol").reset_index(drop=True)


def load_or_fetch_full_nfo_master(*, fetch_missing: bool) -> tuple[pd.DataFrame, Path, Any | None]:
    today = common.now_ist().date()
    path = MARKET_CACHE_ROOT / f"full_nfo_master_{today.isoformat()}.parquet"
    client = None
    if path.exists():
        return _normalise_master(pd.read_parquet(path).to_dict("records")), path, client
    if not fetch_missing:
        raise FileNotFoundError(f"Full NFO master cache is missing: {path}")
    credential = common.discover_kite_credentials(max_apps=1)[0]
    client = common.make_kite_client(credential, timeout_sec=15.0)
    master = _normalise_master(client.instruments("NFO"))
    if master.loc[master["instrument_type"].isin(["CE", "PE"])].empty:
        raise RuntimeError("Kite full NFO master contained no options.")
    common.atomic_write_parquet(master, path)
    return master, path, client


def _fetch_candles_with_retry(client: Any, token: int, session_date: date) -> pd.DataFrame:
    start = datetime.combine(session_date, datetime_time(9, 15), tzinfo=common.IST)
    end = datetime.combine(session_date, datetime_time(15, 30), tzinfo=common.IST)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            records = client.historical_data(
                int(token), start, end, "minute", continuous=False, oi=True
            )
            return normalize_candles(records)
        except Exception as exc:  # Kite exceptions differ across client versions.
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    assert last_error is not None
    raise RuntimeError(f"Kite minute-candle request failed for token {token}: {last_error}") from last_error


def load_or_fetch_contract_candles(
    *,
    symbol: str,
    token: int,
    session_date: date,
    fetch_missing: bool,
    client: Any | None,
) -> tuple[pd.DataFrame, Path, Any | None]:
    path = MARKET_CACHE_ROOT / session_date.isoformat() / f"{common.safe_contract_stem(symbol)}_1minute.parquet"
    if path.exists():
        return normalize_candles(pd.read_parquet(path)), path, client
    if not fetch_missing:
        return pd.DataFrame(), path, client
    if client is None:
        credential = common.discover_kite_credentials(max_apps=1)[0]
        client = common.make_kite_client(credential, timeout_sec=15.0)
    frame = _fetch_candles_with_retry(client, token, session_date)
    if not frame.empty:
        persisted = frame.copy()
        persisted["date"] = persisted["bar_start"]
        common.atomic_write_parquet(persisted, path)
    time.sleep(0.36)
    return frame, path, client


def load_paper_orders(*, from_day: date, through_day: date) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for path in sorted(PAPER_ORDER_ROOT.glob("*/*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        try:
            session_date = _parse_day(payload.get("session_date"))
        except Exception:
            continue
        if not (from_day <= session_date <= through_day):
            continue
        if str(payload.get("status", "")).upper() != "CLOSED":
            continue
        if not payload.get("entry_at_ist") or not payload.get("exit_at_ist"):
            continue
        record = dict(payload)
        record.update(
            {
                "day": session_date.isoformat(),
                "cash_entry_event_ts": _as_ist(payload["entry_at_ist"]),
                "cash_exit_event_ts": _as_ist(payload["exit_at_ist"]),
                "cash_entry_price": float(payload["entry_price"]),
                "cash_exit_price": float(payload["exit_price"]),
                "cash_exit_reason": payload.get("exit_reason", ""),
                "source_order_path": str(path),
            }
        )
        records.append(record)
    return pd.DataFrame(records).sort_values(["day", "cash_entry_event_ts", "tradingsymbol"]).reset_index(drop=True) if records else pd.DataFrame()


def run_paper_derivative(
    *,
    instrument: str,
    from_day: date,
    through_day: date,
    fetch_missing: bool,
    max_delay_minutes: int = 5,
) -> tuple[pd.DataFrame, dict[str, Any], Path]:
    kind = str(instrument).upper()
    if kind not in {"FUTURES", "OPTIONS"}:
        raise ValueError("instrument must be FUTURES or OPTIONS")
    orders = load_paper_orders(from_day=from_day, through_day=through_day)
    if orders.empty:
        raise RuntimeError("No closed V6 PAPER trades were found in the requested window.")
    master, master_path, client = load_or_fetch_full_nfo_master(fetch_missing=fetch_missing)
    records: list[dict[str, Any]] = []
    cache_paths: list[str] = []
    for base in orders.to_dict("records"):
        session_date = _parse_day(base["day"])
        if kind == "FUTURES":
            symbol = str(base.get("futures_tradingsymbol", "")).upper()
            match = master.loc[master["tradingsymbol"].eq(symbol) & master["instrument_type"].eq("FUT")]
            if match.empty:
                failed = dict(base)
                failed.update({"execution_status": "MISSING_CONTRACT", "execution_instrument": kind})
                records.append(failed)
                continue
            contract = match.iloc[0]
            strike = None
            option_type = None
        else:
            try:
                contract = select_atm_option_contract(
                    master,
                    underlying=str(base["tradingsymbol"]),
                    session_date=session_date,
                    cash_entry_price=float(base["cash_entry_price"]),
                    side=str(base["side"]),
                )
            except LookupError:
                failed = dict(base)
                failed.update({"execution_status": "MISSING_CONTRACT", "execution_instrument": kind})
                records.append(failed)
                continue
            symbol = str(contract["tradingsymbol"])
            strike = float(contract["strike"])
            option_type = str(contract["instrument_type"])
        lot_size = int(contract["lot_size"])
        if lot_size <= 0:
            failed = dict(base)
            failed.update({"execution_status": "INVALID_LOT_SIZE", "execution_instrument": kind})
            records.append(failed)
            continue
        tick_size = float(contract["tick_size"])
        if not np.isfinite(tick_size) or tick_size <= 0:
            failed = dict(base)
            failed.update({"execution_status": "INVALID_TICK_SIZE", "execution_instrument": kind})
            records.append(failed)
            continue
        candles, cache_path, client = load_or_fetch_contract_candles(
            symbol=symbol,
            token=int(contract["instrument_token"]),
            session_date=session_date,
            fetch_missing=fetch_missing,
            client=client,
        )
        cache_paths.append(str(cache_path))
        executed_record = _execute_derivative_row(
            base,
            instrument=kind,
            derivative_symbol=symbol,
            derivative_token=int(contract["instrument_token"]),
            expiry=contract["expiry"],
            strike=strike,
            option_type=option_type,
            lot_size=lot_size,
            tick_size=tick_size,
            candles=candles,
            max_delay_minutes=max_delay_minutes,
            require_positive_volume=True,
        )
        if kind == "OPTIONS":
            executed_record["atm_reference_cash_entry_price"] = float(base["cash_entry_price"])
            executed_record["atm_tie"] = bool(contract.get("atm_tie", False))
            executed_record["atm_tie_candidate_strikes"] = str(contract.get("atm_tie_candidate_strikes", ""))
        records.append(executed_record)
    trades = pd.DataFrame(records)
    sessions = int(orders["day"].nunique())
    summary = summarize_derivative(trades, instrument=kind, sessions=sessions)
    summary.update(
        {
            "source": "CLOSED_V6_PAPER_ORDERS_REPRICED_FROM_ACTUAL_EVENT_TIMES",
            "from_day": from_day.isoformat(),
            "through_day": through_day.isoformat(),
            "master_path": str(master_path),
            "master_sha256": _sha256(master_path),
            "atm_policy": ATM_POLICY if kind == "OPTIONS" else None,
            "market_cache_paths": sorted(set(cache_paths)),
            "historical_master_limitation": "Current full-NFO master used retrospectively; contracts require actual session candles.",
        }
    )
    out_dir = RESULT_ROOT / f"paper_{from_day.strftime('%Y%m%d')}_{through_day.strftime('%Y%m%d')}"
    _write_run_artifacts(trades, summary, out_dir=out_dir, stem="v6_o" if kind == "OPTIONS" else "v6_f")
    return trades, summary, out_dir


def run_canonical_futures(
    *,
    from_day: date = DEFAULT_HISTORY_FROM,
    through_day: date = DEFAULT_HISTORY_THROUGH,
    max_delay_minutes: int = 5,
    require_positive_volume: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], Path]:
    if not CANONICAL_AUDIT_PATH.exists() or not PINNED_UNIVERSE_PATH.exists():
        raise FileNotFoundError("Canonical V6 audit or pinned universe is missing.")
    audit = pd.read_csv(CANONICAL_AUDIT_PATH)
    days = pd.to_datetime(audit["day"], errors="coerce").dt.date
    audit = audit.loc[days.between(from_day, through_day)].copy()
    events = reconstruct_v6_cash_events(audit)
    universe = pd.read_parquet(PINNED_UNIVERSE_PATH)
    by_symbol = universe.drop_duplicates("futures_tradingsymbol", keep="last").set_index("futures_tradingsymbol")
    candle_cache: dict[str, pd.DataFrame] = {}
    records: list[dict[str, Any]] = []
    for base in events.to_dict("records"):
        if base.get("cash_event_status") != "READY":
            failed = dict(base)
            failed.update({"execution_status": base.get("cash_event_status", "NOT_READY"), "execution_instrument": "FUTURES"})
            records.append(failed)
            continue
        symbol = str(base["futures_tradingsymbol"])
        if symbol not in by_symbol.index:
            failed = dict(base)
            failed.update({"execution_status": "MISSING_CONTRACT", "execution_instrument": "FUTURES"})
            records.append(failed)
            continue
        contract = by_symbol.loc[symbol]
        candles = _load_archived_future(symbol, candle_cache)
        records.append(
            _execute_derivative_row(
                base,
                instrument="FUTURES",
                derivative_symbol=symbol,
                derivative_token=int(contract["futures_instrument_token"]),
                expiry=contract["expiry"],
                strike=None,
                option_type=None,
                lot_size=int(contract["futures_lot_size"]),
                tick_size=float(contract["futures_tick_size"]),
                candles=candles,
                max_delay_minutes=max_delay_minutes,
                require_positive_volume=require_positive_volume,
            )
        )
    trades = pd.DataFrame(records)
    if CANONICAL_DAILY_PATH.exists():
        daily = pd.read_csv(CANONICAL_DAILY_PATH)
        daily_days = pd.to_datetime(daily["day"], errors="coerce").dt.date
        sessions = int(daily_days.between(from_day, through_day).sum())
    else:
        sessions = int(pd.bdate_range(from_day, through_day).size)
    summary = summarize_derivative(trades, instrument="FUTURES", sessions=sessions)
    summary.update(
        {
            "source": "CANONICAL_V6_CURRENT_SOURCE_AUDIT_PLUS_PINNED_26AUG_1M_FUTURES",
            "positive_volume_required": bool(require_positive_volume),
            "from_day": from_day.isoformat(),
            "through_day": through_day.isoformat(),
            "canonical_audit_path": str(CANONICAL_AUDIT_PATH),
            "canonical_audit_sha256": _sha256(CANONICAL_AUDIT_PATH),
            "pinned_universe_path": str(PINNED_UNIVERSE_PATH),
            "pinned_universe_sha256": _sha256(PINNED_UNIVERSE_PATH),
            "capital_limitation": "Historical SPAN/exposure margin is unavailable; reported capital is full futures notional, not broker margin.",
        }
    )
    ready = events.loc[events["cash_event_status"].eq("READY")].copy()
    ready["cash_quantity"] = np.floor(50000.0 / ready["cash_entry_price"].astype(float)).astype(int)
    ready["cash_entry_exposure_rs"] = ready["cash_quantity"] * ready["cash_entry_price"].astype(float)
    ready["cash_exit_value_rs"] = ready["cash_quantity"] * ready["cash_exit_price"].astype(float)
    ready["cash_gross_pnl_rs"] = np.where(
        ready["side"].astype(str).str.upper().eq("LONG"),
        ready["cash_exit_value_rs"] - ready["cash_entry_exposure_rs"],
        ready["cash_entry_exposure_rs"] - ready["cash_exit_value_rs"],
    )
    ready["cash_5bps_cost_rs"] = ready["cash_entry_exposure_rs"] * 0.0005
    ready["cash_net_pnl_rs"] = ready["cash_entry_exposure_rs"] * ready["net_return_pct"].astype(float) / 100.0
    capital_events = ready.copy()
    capital_events["execution_status"] = "EXECUTED"
    capital_events["entry_ts"] = capital_events["cash_entry_event_ts"]
    capital_events["exit_ts"] = capital_events["cash_exit_event_ts"]
    capital_events["capital_per_trade_rs"] = 10000.0
    peak_cash_capital = peak_concurrent_capital(capital_events, "capital_per_trade_rs")
    peak_cash_exposure = peak_concurrent_capital(capital_events, "cash_entry_exposure_rs")
    baseline_values = ready["net_return_pct"].to_numpy(float)
    cash_daily_net = ready.groupby("day", sort=True)["cash_net_pnl_rs"].sum()
    cash_cumulative = cash_daily_net.cumsum()
    cash_running_peak = cash_cumulative.cummax().clip(lower=0.0)
    cash_max_drawdown = float((cash_running_peak - cash_cumulative).max()) if len(cash_cumulative) else 0.0
    cash_positive_days = int((cash_daily_net > 0).sum())
    cash_negative_days = int((cash_daily_net < 0).sum())
    baseline_summary = {
        "strategy": "V6_CASH_BASELINE",
        "sessions": sessions,
        "orders": int(len(events)),
        "fills": int(len(ready)),
        "wins": int((baseline_values > 0).sum()),
        "losses": int((baseline_values < 0).sum()),
        "profit_factor": _profit_factor(baseline_values),
        "net_return_sum_pct_points": float(baseline_values.sum()),
        "gross_pnl_rs": float(ready["cash_gross_pnl_rs"].sum()),
        "comparable_5bps_cost_rs": float(ready["cash_5bps_cost_rs"].sum()),
        "cash_50k_exposure_proxy_net_pnl_rs": float(ready["cash_net_pnl_rs"].sum()),
        "sum_cash_entry_exposure_rs": float(ready["cash_entry_exposure_rs"].sum()),
        "capital_per_trade_rs": 10000.0,
        "capital_event_sum_rs": float(len(ready) * 10000.0),
        "peak_concurrent_capital_rs": peak_cash_capital,
        "maximum_concurrent_positions": int(round(peak_cash_capital / 10000.0)),
        "peak_concurrent_cash_exposure_rs": peak_cash_exposure,
        "net_return_on_peak_capital_pct": float(ready["cash_net_pnl_rs"].sum() / peak_cash_capital * 100.0) if peak_cash_capital else np.nan,
        "positive_days": cash_positive_days,
        "negative_days": cash_negative_days,
        "flat_days": int(max(0, sessions - cash_positive_days - cash_negative_days)),
        "maximum_daily_equity_drawdown_rs": cash_max_drawdown,
    }
    out_dir = RESULT_ROOT / f"canonical_{from_day.strftime('%Y%m%d')}_{through_day.strftime('%Y%m%d')}"
    stem = "v6_f_liquidity_checked" if require_positive_volume else "v6_f"
    _write_run_artifacts(trades, summary, out_dir=out_dir, stem=stem)
    common.atomic_write_json(out_dir / "v6_cash_baseline_summary.json", baseline_summary)
    return trades, summary, baseline_summary, out_dir


def historical_options_availability(*, from_day: date, through_day: date) -> dict[str, Any]:
    masters = sorted((common.FNO_ROOT / "instrument_master").glob("instrument_master_*.parquet"))
    option_rows = 0
    for path in masters:
        frame = pd.read_parquet(path, columns=["instrument_type"])
        option_rows += int(frame["instrument_type"].astype(str).str.upper().isin(["CE", "PE"]).sum())
    option_files = [
        path
        for path in FUTURES_1M_ROOT.glob("*_1minute.parquet")
        if re.search(r"(?:CE|PE)_1minute\.parquet$", path.name, flags=re.IGNORECASE)
    ]
    return {
        "status": "BLOCKED_MISSING_EXPIRED_OPTION_HISTORY" if not option_rows or not option_files else "AVAILABLE",
        "from_day": from_day.isoformat(),
        "through_day": through_day.isoformat(),
        "archived_master_files": len(masters),
        "archived_option_master_rows": option_rows,
        "archived_option_candle_files": len(option_files),
        "required_data": [
            "dated full NFO option master with token/expiry/strike/type/lot",
            "one-minute CE/PE OHLCV for each mapped expired contract",
        ],
        "synthetic_prices_used": False,
    }


def summarize_paper_cash_baseline(orders: pd.DataFrame) -> dict[str, Any]:
    if orders.empty:
        return {}
    net = pd.to_numeric(orders["net_pnl_rs"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(orders["gross_pnl_rs"], errors="coerce").fillna(0.0)
    # Each V6 PAPER order reserves the recorded capital, not its leveraged exposure.
    events = orders.copy()
    events["execution_status"] = "EXECUTED"
    events["entry_ts"] = events["cash_entry_event_ts"]
    events["exit_ts"] = events["cash_exit_event_ts"]
    events["capital_rs_for_peak"] = pd.to_numeric(events["capital_rs"], errors="coerce").fillna(0.0)
    peak = peak_concurrent_capital(events, "capital_rs_for_peak")
    pnl = net.to_numpy(float)
    return {
        "strategy": "V6_CASH_PAPER_BASELINE",
        "sessions": int(orders["day"].nunique()),
        "trades": int(len(orders)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "profit_factor": _profit_factor(pnl),
        "gross_pnl_rs": float(gross.sum()),
        "estimated_cost_rs": float(pd.to_numeric(orders["estimated_cost_rs"], errors="coerce").fillna(0.0).sum()),
        "net_pnl_rs": float(net.sum()),
        "sum_capital_events_rs": float(pd.to_numeric(orders["capital_rs"], errors="coerce").fillna(0.0).sum()),
        "peak_concurrent_capital_rs": peak,
        "net_return_on_peak_pct": float(net.sum() / peak * 100.0) if peak > 0 else np.nan,
    }


def _write_run_artifacts(trades: pd.DataFrame, summary: Mapping[str, Any], *, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(trades, out_dir / f"{stem}_trades.csv")
    common.atomic_write_json(out_dir / f"{stem}_summary.json", dict(summary))
    lines = [
        f"# {stem.upper()} Backtest",
        "",
        f"- Status: completed with {summary.get('executed_trades', 0)}/{summary.get('input_trades', 0)} quality-approved executions",
        f"- Net P&L after estimated charges: Rs {float(summary.get('estimated_net_pnl_rs', 0.0)):,.2f}",
        f"- Peak concurrent notional/premium: Rs {float(summary.get('peak_concurrent_notional_or_premium_rs', 0.0)):,.2f}",
        f"- Coverage: {float(summary.get('coverage_pct', 0.0)):.2f}%",
        f"- Exit policy: {EXIT_POLICY}",
        f"- Liquidity policy: {LIQUIDITY_POLICY}",
        "",
        "Missing or zero-volume candles are excluded, not filled from stale OHLC. Bid/ask history is unavailable; the summary also reports a 10-bps-per-leg adverse slippage sensitivity.",
        "",
    ]
    common.atomic_write_text(out_dir / f"{stem}_report.md", "\n".join(lines))


def write_comparison_report(
    *,
    orders: pd.DataFrame,
    futures_summary: Mapping[str, Any],
    options_summary: Mapping[str, Any],
    out_dir: Path,
) -> Path:
    futures_path = out_dir / "v6_f_trades.csv"
    options_path = out_dir / "v6_o_trades.csv"
    futures_trades = pd.read_csv(futures_path)
    options_trades = pd.read_csv(options_path)
    common_ids = set(
        options_trades.loc[
            options_trades["execution_status"].eq("EXECUTED"), "signal_id"
        ].astype(str)
    )
    common_ids &= set(
        futures_trades.loc[
            futures_trades["execution_status"].eq("EXECUTED"), "signal_id"
        ].astype(str)
    )
    common_orders = orders.loc[orders["signal_id"].astype(str).isin(common_ids)].copy()
    common_futures = futures_trades.loc[futures_trades["signal_id"].astype(str).isin(common_ids)].copy()
    common_options = options_trades.loc[options_trades["signal_id"].astype(str).isin(common_ids)].copy()
    cash = summarize_paper_cash_baseline(common_orders)
    common_futures_summary = summarize_derivative(
        common_futures, instrument="FUTURES", sessions=int(common_orders["day"].nunique())
    )
    common_options_summary = summarize_derivative(
        common_options, instrument="OPTIONS", sessions=int(common_orders["day"].nunique())
    )
    rows = [
        {
            "strategy": "V6 cash PAPER",
            "trades": cash.get("trades", 0),
            "quality_coverage_pct": 100.0,
            "gross_pnl_rs": cash.get("gross_pnl_rs", 0.0),
            "cost_or_charges_rs": cash.get("estimated_cost_rs", 0.0),
            "net_pnl_rs": cash.get("net_pnl_rs", 0.0),
            "peak_capital_basis_rs": cash.get("peak_concurrent_capital_rs", 0.0),
            "capital_basis": "recorded PAPER capital",
            "net_return_on_peak_pct": cash.get("net_return_on_peak_pct", np.nan),
        },
        {
            "strategy": "V6_F one lot",
            "trades": common_futures_summary.get("executed_trades", 0),
            "quality_coverage_pct": common_futures_summary.get("coverage_pct", 0.0),
            "gross_pnl_rs": common_futures_summary.get("gross_pnl_rs", 0.0),
            "cost_or_charges_rs": common_futures_summary.get("estimated_charges_rs", 0.0),
            "net_pnl_rs": common_futures_summary.get("estimated_net_pnl_rs", 0.0),
            "peak_capital_basis_rs": common_futures_summary.get("peak_concurrent_notional_or_premium_rs", 0.0),
            "capital_basis": "full futures notional; margin unavailable",
            "net_return_on_peak_pct": common_futures_summary.get("net_return_on_peak_pct", np.nan),
        },
        {
            "strategy": "V6_O one ATM lot",
            "trades": common_options_summary.get("executed_trades", 0),
            "quality_coverage_pct": common_options_summary.get("coverage_pct", 0.0),
            "gross_pnl_rs": common_options_summary.get("gross_pnl_rs", 0.0),
            "cost_or_charges_rs": common_options_summary.get("estimated_charges_rs", 0.0),
            "net_pnl_rs": common_options_summary.get("estimated_net_pnl_rs", 0.0),
            "peak_capital_basis_rs": common_options_summary.get("peak_concurrent_notional_or_premium_rs", 0.0),
            "capital_basis": "premium paid",
            "net_return_on_peak_pct": common_options_summary.get("net_return_on_peak_pct", np.nan),
        },
    ]
    comparison = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "v6_cash_futures_options_comparison.csv"
    common.atomic_write_csv(comparison, csv_path)
    lines = [
        "# V6 Cash vs V6_F vs V6_O",
        "",
        f"All rows use the same {len(common_ids)} quality-covered closed V6 PAPER trades. LONG buys CE and SHORT buys PE in V6_O.",
        "",
        comparison.to_markdown(index=False, floatfmt=",.2f"),
        "",
        f"Coverage before common-trade filtering: V6_F {futures_summary.get('executed_trades', 0)}/{futures_summary.get('input_trades', 0)}; V6_O {options_summary.get('executed_trades', 0)}/{options_summary.get('input_trades', 0)}.",
        "",
        "Futures notional is not broker margin. Option capital is premium paid. Raw rupee P&L is not risk-normalized because one exchange lot has different exposure in each instrument.",
        "",
    ]
    common.atomic_write_text(out_dir / "v6_cash_futures_options_comparison.md", "\n".join(lines))
    common.atomic_write_json(out_dir / "v6_cash_baseline_summary.json", cash)
    common.atomic_write_json(
        out_dir / "v6_common_trade_comparison_summary.json",
        {
            "common_signal_ids": sorted(common_ids),
            "cash": cash,
            "futures": common_futures_summary,
            "options": common_options_summary,
            "all_signal_futures": dict(futures_summary),
            "all_signal_options": dict(options_summary),
        },
    )
    return csv_path
