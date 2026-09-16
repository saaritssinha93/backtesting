"""Causal five-minute, three-lot long-option execution for V13-v10-G.

``timestamp`` is the START of the candle in Asia/Kolkata. The caller must
normalize source timestamps and select the contract without future knowledge.
``stop_pct`` and ``target_pct`` are fractions, for example .15 and .30.

Only the immediately preceding completed candle can screen pretrade volume.
The entry/exit candle volume is an ex-post capacity audit, not a size selector.
An entry with insufficient total printed volume is explicitly ex-post unfilled;
an impossible exit remains unresolved. Within that physical total-volume bound,
modeled fills retain all three lots even if the participation limit is breached.

Unknown held bars produce UNRESOLVED trades, retaining the purchase cash debit
and fees. Their net_pnl is NaN, never an invented zero; net_pnl_lower_bound
charges the full premium plus entry fees as a conservative loss scenario.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import math
from typing import Any

import pandas as pd


IST = "Asia/Kolkata"
FIVE_MINUTES = pd.Timedelta(minutes=5)
COST_SOURCE = "https://zerodha.com/charges"


def _positive(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{label} must be a finite positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{label} must be a finite positive number")
    return number


def _ist(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("timestamp cannot be missing")
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(IST)
    return timestamp.tz_convert(IST)


def _tick(value: float, tick: float, *, up: bool) -> float:
    unit = Decimal(str(tick))
    quotient = Decimal(str(value)) / unit
    nearest = quotient.to_integral_value()
    # Decimal conversion should not turn floating point 110.00000000000001
    # into an additional whole tick. This tolerance is a billionth of a tick.
    if abs(quotient - nearest) < Decimal("0.000000001"):
        quotient = nearest
    count = quotient.to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR
    )
    return float(count * unit)


def option_order_costs(price: float, quantity: int, side: str) -> dict[str, float]:
    """NSE equity-option premium charges in rupees, unrounded component model.

    Brokerage Rs20/order; STT .15% sell; exchange .03553%; SEBI .0001%;
    stamp .003% buy; IPFT .0000001%; GST 18% on brokerage/exchange/SEBI/IPFT.
    Rates are explicit research assumptions, not a historical rate calendar.
    """
    price = _positive(price, "price")
    size = _positive(quantity, "quantity")
    if not size.is_integer():
        raise ValueError("quantity must be a positive integer")
    side = str(side).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    turnover = price * int(size)
    brokerage = 20.0
    stt = turnover * .0015 if side == "SELL" else 0.0
    exchange = turnover * .0003553
    sebi = turnover * .000001
    stamp = turnover * .00003 if side == "BUY" else 0.0
    ipft = turnover * .000000001
    gst = .18 * (brokerage + exchange + sebi + ipft)
    total = brokerage + stt + exchange + sebi + stamp + ipft + gst
    return {
        "turnover": turnover, "brokerage": brokerage, "stt": stt,
        "exchange": exchange, "sebi": sebi, "stamp": stamp,
        "ipft": ipft, "gst": gst, "total": total,
    }


def _volume(bar: dict[str, Any] | None) -> float:
    if bar is None:
        return math.nan
    try:
        volume = float(bar.get("volume", math.nan))
    except (ValueError, TypeError):
        return math.nan
    return volume if math.isfinite(volume) and volume >= 0 else math.nan


def _ohlc_error(bar: dict[str, Any]) -> str | None:
    try:
        o, h, l, c = (_positive(bar.get(key), key) for key in ("open", "high", "low", "close"))
    except ValueError:
        return "NONPOSITIVE_OR_NONFINITE_OHLC"
    if l > min(o, c) or h < max(o, c) or l > h:
        return "INVALID_OHLC_GEOMETRY"
    return None


def simulate_trade(
    row: dict[str, Any],
    candles: pd.DataFrame,
    stop_pct: float,
    target_pct: float,
    *,
    slippage_bps: float = 10,
    participation: float = .1,
    lots: int = 3,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Simulate an explicitly timed purchase and return trade plus bar audit.

    Required row keys: trade_id, day, entry_ts, lot_size, tick_size. Optional
    row['check_previous_volume'] defaults True. Candles require timestamp and
    OHLC columns, with optional volume. Naive timestamps are interpreted IST.

    All three lots fill together. A candle needs total volume >= quantity to
    support a modeled fill, or >= 2*quantity for same-candle entry and exit.
    Insufficient entry volume means ex-post unfilled; insufficient exit volume
    means UNRESOLVED. Buy slippage rounds up to the price tick;
    fixed stop/target are rounded up from the slipped entry. Market sells
    receive adverse slippage rounded down; resting target limits never fill
    below their limit. At each open: stop gap, target gap, then 15:15 time exit.
    Otherwise stop is checked before target, conservatively resolving a bar
    touching both. The entry candle itself is included in stop/target checks.

    CLOSED has known realized net_pnl. SKIPPED has no purchase and zero cash
    flow. UNRESOLVED has a purchase, unknown realized net_pnl, and a retained
    negative net_cash_flow; its full-premium-loss lower bound is also supplied.
    """
    if lots != 3:
        raise ValueError("V13-v10-G options execution requires exactly 3 lots")
    lot_size_number = _positive(row["lot_size"], "lot_size")
    if not lot_size_number.is_integer():
        raise ValueError("historical lot_size must be a positive integer")
    lot_size = int(lot_size_number)
    quantity = 3 * lot_size
    tick_size = _positive(row["tick_size"], "tick_size")
    stop_pct = _positive(stop_pct, "stop_pct")
    target_pct = _positive(target_pct, "target_pct")
    if stop_pct >= 1:
        raise ValueError("stop_pct must be a fraction strictly between 0 and 1")
    if not math.isfinite(float(slippage_bps)) or not 0 <= slippage_bps < 10000:
        raise ValueError("slippage_bps must be in [0, 10000)")
    if not math.isfinite(float(participation)) or not 0 < participation <= 1:
        raise ValueError("participation must be in (0, 1]")
    slippage = float(slippage_bps) / 10000
    entry_ts = _ist(row["entry_ts"])
    day = _ist(row["day"]).date()
    if entry_ts.date() != day:
        raise ValueError("entry_ts must be on row['day'] in IST")
    session_start = entry_ts.normalize() + pd.Timedelta(hours=9, minutes=15)
    time_exit = entry_ts.normalize() + pd.Timedelta(hours=15, minutes=15)
    if entry_ts.minute % 5 or entry_ts.second or entry_ts.microsecond or entry_ts.nanosecond:
        raise ValueError("entry_ts must be aligned to a five-minute candle start")
    if not session_start <= entry_ts < time_exit:
        raise ValueError("entry_ts must be between 09:15 inclusive and 15:15 exclusive")
    for column in ("timestamp", "open", "high", "low", "close"):
        if column not in candles.columns:
            raise ValueError(f"candles missing required column: {column}")

    result: dict[str, Any] = dict(row)
    result.update({
        "entry_ts": entry_ts, "day": str(day), "lot_size": lot_size,
        "lots": 3, "option_lots": 3, "quantity": quantity,
        "tick_size": tick_size, "stop_pct": stop_pct, "target_pct": target_pct,
        "slippage_bps": slippage_bps, "participation": participation,
        "status": "SKIPPED", "execution_status": "SKIPPED", "entered": False,
        "reason": "", "exit_reason": "", "entry_raw_price": math.nan,
        "entry_price": math.nan, "stop_price": math.nan, "target_price": math.nan,
        "exit_raw_price": math.nan, "exit_price": math.nan, "exit_ts": pd.NaT,
        "exit_observed_ts": pd.NaT,
        "entry_costs": 0.0, "exit_costs": 0.0, "total_costs": 0.0,
        "entry_premium_outlay": 0.0, "gross_pnl": 0.0, "net_pnl": 0.0,
        "net_pnl_lower_bound": 0.0, "net_cash_flow": 0.0,
        "entry_capacity_breach": False, "exit_capacity_breach": False,
        "entry_volume_unknown": False, "exit_volume_unknown": False,
        "entry_fill_outside_ohlc": False, "exit_fill_outside_ohlc": False,
        "ambiguous_stop_target": False, "bars_held": 0,
        "previous_volume_check_enabled": bool(row.get("check_previous_volume", True)),
        "previous_bar_volume": math.nan, "previous_bar_capacity": math.nan,
        "entry_bar_volume": math.nan, "exit_bar_volume": math.nan,
        "entry_bar_capacity": math.nan, "exit_bar_capacity": math.nan,
        "capacity_breach": False, "cost_source": COST_SOURCE,
        "entry_physical_volume_insufficient": False,
        "exit_physical_volume_insufficient": False,
        "last_observed_ts": pd.NaT, "last_observed_close": math.nan,
        "last_observed_mark_pnl": math.nan,
    })
    audit: list[dict[str, Any]] = []
    by_time: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    for bar in candles.to_dict("records"):
        stamp = _ist(bar["timestamp"])
        if entry_ts - FIVE_MINUTES <= stamp <= time_exit:
            by_time.setdefault(stamp, []).append(bar)

    def record(stamp: pd.Timestamp, bar: dict[str, Any] | None, state: str, **extra: Any) -> dict[str, Any]:
        volume = _volume(bar)
        capacity = volume * participation
        item = {
            "trade_id": row["trade_id"], "day": str(day), "timestamp": stamp,
            "state": state, "open": math.nan, "high": math.nan,
            "low": math.nan, "close": math.nan, "volume": volume,
            "quantity": quantity, "participation": participation,
            "volume_capacity": capacity, "volume_unknown": math.isnan(volume),
            "quantity_exceeds_bar_capacity": quantity > capacity if not math.isnan(capacity) else False,
            "stop_price": result["stop_price"], "target_price": result["target_price"],
            "entry_price": result["entry_price"], "fill_price": math.nan,
            "exit_reason": "", "ambiguous_stop_target": False,
            "event": state, "event_observed_ts": stamp,
        }
        if bar is not None:
            item.update({key: bar.get(key, math.nan) for key in ("open", "high", "low", "close")})
        item.update(extra)
        audit.append(item)
        return item

    def skip(reason: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        result["reason"] = reason
        return result, audit

    def unresolved(stamp: pd.Timestamp, reason: str, bar: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        result.update({"status": "UNRESOLVED", "execution_status": "UNRESOLVED",
                       "reason": reason, "exit_reason": "UNRESOLVED", "gross_pnl": math.nan,
                       "net_pnl": math.nan, "unresolved_ts": stamp})
        record(stamp, bar, "UNRESOLVED", exit_reason=reason)
        return result, audit

    previous_ts = entry_ts - FIVE_MINUTES
    previous_bars = by_time.get(previous_ts, [])
    previous_bar = previous_bars[0] if len(previous_bars) == 1 else None
    previous_volume = _volume(previous_bar)
    result["previous_bar_volume"] = previous_volume
    result["previous_bar_capacity"] = previous_volume * participation
    record(previous_ts, previous_bar, "PRE_ENTRY_SCREEN")
    if result["previous_volume_check_enabled"]:
        if len(previous_bars) != 1:
            return skip("PREVIOUS_BAR_MISSING" if not previous_bars else "PREVIOUS_BAR_DUPLICATE")
        error = _ohlc_error(previous_bar)
        if error:
            return skip("PREVIOUS_BAR_" + error)
        if math.isnan(previous_volume):
            return skip("PREVIOUS_VOLUME_UNKNOWN")
        if quantity > previous_volume * participation:
            return skip("PREVIOUS_VOLUME_INSUFFICIENT")

    entry_bars = by_time.get(entry_ts, [])
    if len(entry_bars) != 1:
        record(entry_ts, None, "SKIPPED")
        return skip("ENTRY_BAR_MISSING" if not entry_bars else "ENTRY_BAR_DUPLICATE")
    entry_bar = entry_bars[0]
    error = _ohlc_error(entry_bar)
    if error:
        record(entry_ts, entry_bar, "SKIPPED")
        return skip("ENTRY_BAR_" + error)
    entry_volume = _volume(entry_bar)
    result["entry_bar_volume"] = entry_volume
    result["entry_bar_capacity"] = entry_volume * participation
    result["entry_volume_unknown"] = math.isnan(entry_volume)
    if math.isnan(entry_volume) or entry_volume < quantity:
        reason = ("ENTRY_VOLUME_UNKNOWN_UNFILLED" if math.isnan(entry_volume) else
                  "ENTRY_ZERO_VOLUME_UNFILLED" if entry_volume == 0 else
                  "EX_POST_INSUFFICIENT_TOTAL_VOLUME")
        result["entry_capacity_breach"] = not math.isnan(entry_volume)
        result["capacity_breach"] = result["entry_capacity_breach"]
        result["entry_physical_volume_insufficient"] = True
        record(entry_ts, entry_bar, "EX_POST_UNFILLED", exit_reason=reason,
               required_execution_volume=quantity)
        return skip(reason)

    entry_raw = float(entry_bar["open"])
    entry_price = _tick(entry_raw * (1 + slippage), tick_size, up=True)
    stop_price = _tick(entry_price * (1 - stop_pct), tick_size, up=True)
    target_price = _tick(entry_price * (1 + target_pct), tick_size, up=True)
    if not 0 < stop_price < entry_price < target_price:
        raise ValueError("tick-rounded stop and target must lie below/above the entry price")
    entry_fees = option_order_costs(entry_price, quantity, "BUY")
    result.update({
        "entered": True, "entry_raw_price": entry_raw, "entry_price": entry_price,
        "stop_price": stop_price, "target_price": target_price,
        "entry_costs": entry_fees["total"], "total_costs": entry_fees["total"],
        "entry_premium_outlay": entry_price * quantity,
        "net_cash_flow": -entry_price * quantity - entry_fees["total"],
        "net_pnl_lower_bound": -entry_price * quantity - entry_fees["total"],
        "entry_capacity_breach": quantity > entry_volume * participation if not math.isnan(entry_volume) else False,
        "entry_fill_outside_ohlc": not float(entry_bar["low"]) <= entry_price <= float(entry_bar["high"]),
    })
    result["capacity_breach"] = result["entry_capacity_breach"]
    for name, value in entry_fees.items():
        result["entry_cost_" + name] = value

    stamp = entry_ts
    while stamp <= time_exit:
        candidates = by_time.get(stamp, [])
        if len(candidates) != 1:
            reason = "HELD_BAR_DUPLICATE" if candidates else ("TIME_EXIT_BAR_MISSING" if stamp == time_exit else "HELD_BAR_MISSING")
            return unresolved(stamp, reason)
        bar = candidates[0]
        error = _ohlc_error(bar)
        if error:
            return unresolved(stamp, "HELD_BAR_" + error, bar)
        o, h, l, c = (float(bar[key]) for key in ("open", "high", "low", "close"))
        result["bars_held"] += 1
        reason = ""
        raw_exit = math.nan
        is_limit = False
        ambiguous = False
        # The resting bracket is considered at the open, before that candle's
        # high/low. An opening target gap cannot be retroactively stopped out.
        if o <= stop_price:
            reason, raw_exit = "STOP_GAP", o
        elif o >= target_price:
            reason, raw_exit, is_limit = "TARGET_GAP", target_price, True
        elif stamp == time_exit:
            reason, raw_exit = "TIME_EXIT_1515", o
        elif l <= stop_price:
            reason, raw_exit = "STOP", stop_price
            ambiguous = h >= target_price
        elif h >= target_price:
            reason, raw_exit, is_limit = "TARGET", target_price, True
        if reason:
            volume = _volume(bar)
            required_volume = quantity * (2 if stamp == entry_ts else 1)
            if math.isnan(volume) or volume < required_volume:
                failure = ("EXIT_VOLUME_UNKNOWN" if math.isnan(volume) else
                           "EXIT_ZERO_VOLUME" if volume == 0 else
                           "SAME_BAR_EXIT_INSUFFICIENT_TOTAL_VOLUME" if stamp == entry_ts else
                           "EXIT_INSUFFICIENT_TOTAL_VOLUME")
                result.update({
                    "exit_bar_volume": volume, "exit_bar_capacity": volume * participation,
                    "exit_capacity_breach": quantity > volume * participation if not math.isnan(volume) else False,
                    "exit_volume_unknown": math.isnan(volume),
                    "exit_physical_volume_insufficient": True,
                    "attempted_exit_reason": reason, "attempted_exit_ts": stamp,
                    "attempted_exit_raw_price": raw_exit,
                })
                result["capacity_breach"] = result["entry_capacity_breach"] or result["exit_capacity_breach"]
                failed = unresolved(stamp, failure, bar)
                audit[-1].update({"event": failure, "attempted_exit_reason": reason,
                                  "required_execution_volume": required_volume,
                                  "event_observed_ts": stamp + FIVE_MINUTES,
                                  "is_entry_bar": stamp == entry_ts})
                return failed
        audit_bar = record(stamp, bar, "EXIT" if reason else ("ENTRY_HELD" if stamp == entry_ts else "HELD"),
                           exit_reason=reason, ambiguous_stop_target=ambiguous,
                           is_entry_bar=stamp == entry_ts,
                           required_execution_volume=quantity * ((1 if stamp == entry_ts else 0) + (1 if reason else 0)),
                           event=reason or "CANDLE_COMPLETED",
                           event_observed_ts=stamp if reason in {"STOP_GAP", "TARGET_GAP", "TIME_EXIT_1515"} else stamp + FIVE_MINUTES)
        if not reason:
            result["last_observed_ts"] = stamp
            result["last_observed_close"] = c
            result["last_observed_mark_pnl"] = (c - entry_price) * quantity - entry_fees["total"]
            stamp += FIVE_MINUTES
            continue

        exit_price = raw_exit if is_limit else max(tick_size, _tick(raw_exit * (1 - slippage), tick_size, up=False))
        exit_fees = option_order_costs(exit_price, quantity, "SELL")
        volume = _volume(bar)
        exit_breach = quantity > volume * participation if not math.isnan(volume) else False
        gross = (exit_price - entry_price) * quantity
        total_costs = entry_fees["total"] + exit_fees["total"]
        result.update({
            "status": "CLOSED", "execution_status": "EXECUTED", "reason": reason,
            "exit_reason": reason, "exit_ts": stamp, "exit_raw_price": raw_exit,
            "exit_observed_ts": audit_bar["event_observed_ts"],
            "exit_price": exit_price, "exit_costs": exit_fees["total"],
            "total_costs": total_costs, "gross_pnl": gross, "net_pnl": gross - total_costs,
            "net_pnl_lower_bound": gross - total_costs, "net_cash_flow": gross - total_costs,
            "exit_bar_volume": volume, "exit_bar_capacity": volume * participation,
            "exit_capacity_breach": exit_breach, "exit_volume_unknown": math.isnan(volume),
            "capacity_breach": result["entry_capacity_breach"] or exit_breach,
            "exit_fill_outside_ohlc": not l <= exit_price <= h,
            "ambiguous_stop_target": ambiguous,
        })
        for name, value in exit_fees.items():
            result["exit_cost_" + name] = value
        audit_bar["fill_price"] = exit_price
        audit_bar["fill_is_limit"] = is_limit
        return result, audit

    raise AssertionError("time-exit boundary must either close or mark unresolved")
