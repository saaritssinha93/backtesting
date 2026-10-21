"""Causal, advisory G3 position review shared by monitoring and research.

The refresh interval is not a candle interval. Only completed one-minute bars
and explicitly timestamped prices enter these rules; no order API is imported.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import math
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
RULES_VERSION = "G3-TRADE-MONITOR-RULES-V1"
MONITOR_CONFIG = dict(refresh_seconds=15, max_price_age_seconds=120,
    minimum_bars=6, adverse_closes=3, trend_momentum_5m_pct=-.15,
    trend_entry_move_pct=-.30, profit_activation_pct=.50, profit_giveback_pct=.30)


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _time(value):
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return stamp.astimezone(IST) if stamp.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def assess_trade(trade, bars, *, now, mark_price=None, mark_timestamp=None, config=None):
    """Return serializable observations, never an instruction to submit orders.

    Bars are timestamped at candle end. A bar containing an intraminute entry
    is excluded in full. Future/current unfinished bars and future quotes are
    excluded. Research evaluates at a completed candle then fills, if any, on
    a later candle; this function never supplies an execution price.
    """
    settings = {**MONITOR_CONFIG, **(config or {})}
    result = dict(rules_version=RULES_VERSION, direction="NO_DATA", reasons=[],
        advisory_flags=[], metrics={}, freshness={}, execution_authority=False,
        bar_resolution_seconds=60, refresh_seconds=settings["refresh_seconds"])
    clock, entry_at = _time(now), _time(trade.get("entry_at_ist"))
    entry, price = _number(trade.get("entry_price")), _number(mark_price)
    quote_at, side = _time(mark_timestamp), trade.get("side")
    if clock is None or entry_at is None or entry is None or entry <= 0 or side not in {"LONG", "SHORT"} or entry_at > clock:
        result["reasons"].append("Valid side, entry price and timezone-aware entry/current times are required.")
        return result
    sign = 1 if side == "LONG" else -1
    rows = bars.to_dict("records") if hasattr(bars, "to_dict") else list(bars or [])
    accepted, quality = {}, []
    for row in rows:
        timestamp = _time(row.get("timestamp"))
        if timestamp is None or timestamp > clock or timestamp - timedelta(minutes=1) < entry_at or timestamp.date() != entry_at.date():
            continue
        values = {key: _number(row.get(key)) for key in ("open", "high", "low", "close")}
        if (any(value is None or value <= 0 for value in values.values())
                or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]):
            quality.append("Invalid post-entry OHLC bar.")
            continue
        if timestamp in accepted and accepted[timestamp] != values:
            quality.append("Conflicting post-entry bars at one timestamp.")
            continue
        accepted[timestamp] = values
    completed = sorted(accepted.items())
    result["metrics"]["completed_post_entry_bars"] = len(completed)
    result["freshness"]["last_bar_at_ist"] = completed[-1][0].isoformat() if completed else None
    result["freshness"]["bar_age_seconds"] = (clock - completed[-1][0]).total_seconds() if completed else None
    valid_quote = (price is not None and price > 0 and quote_at is not None
        and entry_at <= quote_at <= clock and quote_at.date() == clock.date())
    if not valid_quote:
        result["reasons"].append("No valid timestamped price at or after entry; updated order time is not quote time.")
        result["reasons"].extend(sorted(set(quality)))
        return result
    age = (clock - quote_at).total_seconds()
    result["freshness"].update(price_at_ist=quote_at.isoformat(), price_age_seconds=age,
        price_state="FRESH" if age <= settings["max_price_age_seconds"] else "STALE")
    move = sign * (price / entry - 1) * 100
    # A newer bar than the supplied mark would mix information times. Restrict
    # the path metrics and momentum to information available at the mark.
    observed = [(stamp, row) for stamp, row in completed if stamp <= quote_at]
    favorable = [move, 0.] + [sign * (row["high" if sign == 1 else "low"] / entry - 1) * 100 for _, row in observed]
    adverse = [move, 0.] + [sign * (row["low" if sign == 1 else "high"] / entry - 1) * 100 for _, row in observed]
    mfe, mae = max(favorable), min(adverse)
    metrics = result["metrics"]
    metrics.update(mark_price=price, signed_move_pct=move, mfe_pct=mfe, mae_pct=mae,
        giveback_pct=max(0., mfe - move), held_minutes=(clock - entry_at).total_seconds() / 60,
        momentum_1m_pct=None, momentum_5m_pct=None, consecutive_adverse_closes=0,
        stop_distance_pct=None, target_distance_pct=None, target_progress_pct=None,
        gross_unrealized_pnl_rs=None)
    quantity = _number(trade.get("quantity"))
    if quantity is not None and quantity > 0:
        metrics["gross_unrealized_pnl_rs"] = sign * (price - entry) * quantity
    stop, target = _number(trade.get("stop_price")), _number(trade.get("target_price"))
    if stop is not None and stop > 0:
        metrics["stop_distance_pct"] = sign * (price - stop) / entry * 100
    if target is not None and target > 0:
        metrics["target_distance_pct"] = sign * (target - price) / entry * 100
        target_move = sign * (target / entry - 1) * 100
        metrics["target_progress_pct"] = move / target_move * 100 if target_move > 0 else None
    if age > settings["max_price_age_seconds"]:
        result["direction"] = "STALE"
        result["reasons"].append("Price is stale; no directional or exit-rule signal is issued.")
        return result
    count = int(settings["minimum_bars"])
    recent = observed[-max(6, count):]
    contiguous = all((recent[i][0] - recent[i - 1][0]).total_seconds() == 60 for i in range(1, len(recent)))
    last_age = (clock - recent[-1][0]).total_seconds() if recent else None
    if quality or len(recent) < max(6, count) or not contiguous or last_age > settings["max_price_age_seconds"]:
        result["direction"] = "WARMING_UP"
        result["reasons"].extend(sorted(set(quality)))
        result["reasons"].append("Six recent contiguous completed post-entry one-minute bars are required; no exit-rule signal is issued.")
        return result
    closes = [row["close"] for _, row in recent]
    changes = [sign * (closes[i] / closes[i - 1] - 1) * 100 for i in range(1, len(closes))]
    streak = 0
    for change in reversed(changes):
        if change >= 0:
            break
        streak += 1
    momentum = sign * (closes[-1] / closes[-6] - 1) * 100
    metrics.update(momentum_1m_pct=changes[-1], momentum_5m_pct=momentum,
        consecutive_adverse_closes=streak)
    if move >= 0 and momentum >= .05:
        result["direction"] = "FAVORABLE"
        result["reasons"].append("Price is at/above entry in the trade direction and five-minute momentum supports it.")
    elif move <= -.15 and momentum <= -.05:
        result["direction"] = "ADVERSE"
        result["reasons"].append("Price is adverse to entry and five-minute momentum is against the trade.")
    else:
        result["direction"] = "MIXED"
        result["reasons"].append("Entry-relative price and recent momentum do not agree strongly.")
    if streak >= settings["adverse_closes"]:
        if momentum <= settings["trend_momentum_5m_pct"] and move <= settings["trend_entry_move_pct"]:
            result["advisory_flags"].append("TREND_FAILURE")
            result["reasons"].append("Three adverse closes, five-minute move <= -0.15%, and entry move <= -0.30%: review directional failure.")
        if mfe >= settings["profit_activation_pct"] and mfe - move >= settings["profit_giveback_pct"]:
            result["advisory_flags"].append("PROFIT_GIVEBACK")
            result["reasons"].append("Observed favorable move >= 0.50% has given back >= 0.30 percentage points with three adverse closes.")
    return result
