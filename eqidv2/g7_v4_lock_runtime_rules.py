"""Pure, causal G7 rule primitives; not a feed or broker execution adapter.

These functions do not grant trading authority. Native candidate generation,
ranking, post-quota filters and execution must still be integrated and tested
before the named G7 runtime can leave NOT_SUPPORTED status.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import pandas as pd

IST = "Asia/Kolkata"


def _stamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return stamp.tz_convert(IST)


def _positive(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("Finite positive price required")
    return result


def selected_slot_allowed(setup_id):
    from strategy_slot_selection import slot_allowed
    return slot_allowed("G7_V4_LOCK2", setup_id)


def low_efficiency_veto(minute_history, candidate, *, historical_inventory_complete):
    """Reuse exact research feature formulas with completed bars only.

    A missing reference feed is different from genuinely insufficient warmup.
    The caller must attest its history inventory before the rule is evaluated.
    No outcome columns are consumed. Invalid present-session prefixes raise.
    """
    if historical_inventory_complete is not True:
        raise ValueError("Historical reference inventory is not complete")
    from research.g7_v4_extremes import feature_row
    from research.g7_v4_indicator_iteration import indicator_row, decisions
    row = dict(candidate)
    stamp = _stamp(row["confirmation_ts"])
    if stamp != stamp.floor("min"):
        raise ValueError("Completed-minute boundary required")
    frame = minute_history.copy()
    column = next((c for c in ("ts", "date", "timestamp") if c in frame), None)
    if column is None:
        raise ValueError("Missing minute timestamps")
    frame["ts"] = frame[column].map(_stamp)
    frame = frame.loc[frame.ts.le(stamp)].copy()
    days = {str(day): part for day, part in frame.groupby(frame.ts.dt.date, sort=True)}
    if str(stamp.date()) not in days:
        raise ValueError("Missing confirmation session")
    features = feature_row(days, row)
    features.update(indicator_row(days[str(stamp.date())], row))
    data = pd.DataFrame([features]).set_index("sid", drop=False)
    veto = bool(decisions(data)["low_efficiency"].iloc[0])
    return dict(veto=veto, confirmation_ts=stamp.isoformat(),
        reason="LOW_EFFICIENCY_EXTREME_RSI" if veto else "PERMITTED_BY_EXTREME_RULE",
        features=features)


def nifty_permission(nifty_bars, confirmation_ts, side):
    """Exact completed dated-Nifty directions; invalid/missing bars raise."""
    from research.g4_nifty_direction_backtest import direction_at
    from research.g6_nifty_direction import allowed
    stamp = _stamp(confirmation_ts)
    if side not in {"LONG", "SHORT"}:
        raise ValueError("Invalid direction")
    frame = nifty_bars.copy()
    frame["ts"] = frame.ts.map(_stamp)
    frame = frame.loc[frame.ts.le(stamp) & frame.ts.dt.date.eq(stamp.date())].copy()
    values = direction_at(frame, stamp)
    if not all(math.isfinite(float(values[key])) for key in ("slot_candle", "session_open")):
        raise ValueError("Missing or invalid exact Nifty directions")
    return dict(allowed=bool(allowed(side, values["slot_candle"], values["session_open"])), **values)


def admission(states, *, setup_id, symbol, prospective_entry_ts):
    """Check slot, open symbol and two realized net losses as of admission.

    This pure function does not reserve capital or submit entries. Only CLOSED
    rows with known net P&L and an exit strictly before admission count; ties
    remain eligible as in the backtest. Existing OPEN rows are never changed.
    """
    now = _stamp(prospective_entry_ts)
    if not selected_slot_allowed(setup_id):
        return dict(allowed=False, reason="SLOT_WIN_RATE_POLICY_DISABLED")
    losses, open_symbols = [], set()
    seen = set()
    for row in states:
        if not isinstance(row, Mapping):
            raise ValueError("Invalid order state")
        if str(row.get("session_date", "")) != str(now.date()):
            continue
        identity = str(row.get("signal_id", ""))
        if not identity or identity in seen:
            raise ValueError("Missing or duplicate state identity")
        seen.add(identity)
        status = str(row.get("status", ""))
        if status in {"OPEN", "SQUARE_OFF_PENDING"}:
            entered = _stamp(row["entry_at_ist"])
            if entered <= now:
                open_symbols.add(str(row["tradingsymbol"]).upper())
        elif status == "CLOSED":
            exited = _stamp(row["exit_at_ist"])
            if exited >= now:
                continue
            net = float(row["net_pnl_rs"])
            if not math.isfinite(net):
                raise ValueError("Realized net P&L is unavailable")
            if net < -1e-8:
                losses.append(exited)
        elif status not in {"PENDING_ENTRY", "CANCELLED", "NO_FILL", "REJECTED",
                            "BLOCKED_SIZING", "BLOCKED_PORTFOLIO"}:
            raise ValueError("Unknown order status")
    if len(losses) >= 2:
        return dict(allowed=False, reason="DAILY_NET_LOSS_COUNT_LIMIT",
            realized_net_losing_exits=len(losses), lock_ts=sorted(losses)[1].isoformat())
    if str(symbol).upper() in open_symbols:
        return dict(allowed=False, reason="ONE_ACTIVE_POSITION_PER_SYMBOL")
    return dict(allowed=True, reason="ADMISSION_RULES_PASSED", realized_net_losing_exits=len(losses))


def observe_completed_close(state, *, close, bar_end, observed_at):
    """Propose +0.1% stop only after a complete post-entry close reaches +0.6%.

    Timestamps label candle ends. The following minute opens at bar_end; its
    close is bar_end+1 minute. Calling this function does not modify a stop.
    """
    result = dict(state)
    if result.get("status") != "OPEN":
        return result
    end, observed = _stamp(bar_end), _stamp(observed_at)
    if end != end.floor("min") or end > observed:
        raise ValueError("Uncompleted or invalid minute close")
    entered = _stamp(result["entry_at_ist"])
    entry_bar_end = _stamp(result.get("entry_bar_end", entered.ceil("min")))
    if end <= entry_bar_end:
        return result
    last = result.get("last_protection_observation_bar_end")
    if last and end < _stamp(last):
        raise ValueError("Protection observations must be chronological")
    side = result.get("side")
    if side not in {"LONG", "SHORT"}:
        raise ValueError("Invalid direction")
    sign = 1 if side == "LONG" else -1
    price, entry = _positive(close), _positive(result["entry_price"])
    result["last_protection_observation_bar_end"] = end.isoformat()
    if sign * (price - entry) + 1e-12 >= entry * .006:
        stop = entry * (1 + sign * .001)
        existing = _positive(result["stop_price"])
        proposal = max(existing, stop) if side == "LONG" else min(existing, stop)
        result.setdefault("protection_armed_at", end.isoformat())
        result.setdefault("protection_effective_from", end.isoformat())
        result["protection_proposed_stop"] = proposal
    return result


def protected_stop_for_next_bar(state, *, bar_start):
    """Return a stop proposal without mutating broker or position state."""
    existing = _positive(state["stop_price"])
    if state.get("side") not in {"LONG", "SHORT"}:
        raise ValueError("Invalid direction")
    if not state.get("protection_effective_from"):
        return existing
    if _stamp(bar_start) < _stamp(state["protection_effective_from"]):
        return existing
    proposal = _positive(state["protection_proposed_stop"])
    return max(existing, proposal) if state["side"] == "LONG" else min(existing, proposal)
