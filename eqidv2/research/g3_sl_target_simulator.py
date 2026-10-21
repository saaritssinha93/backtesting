"""Causal, research-only exits for frozen G3 entries and genuine minute paths.

Times label candle ends. Entry timing uses the native conservative entry-bar
end convention. A protection rule observes full post-entry candle CLOSES only;
it cannot change that candle's stop. No portfolio sizing or costs are applied
here. Native ambiguous intrabar touches use stop-first ordering.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd


MINUTE_NS = 60_000_000_000
IST = "Asia/Kolkata"


def _finite(value, name):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def simulate_exit(
    path: Mapping,
    entry_index: int,
    entry_price: float,
    is_long: bool,
    stop_pct: float,
    target_pct: float,
    protect_after_close_pct: float | None = None,
    protected_profit_pct: float = 0.0,
    *,
    tighten_after_minutes: float | None = None,
    tightened_stop_pct: float | None = None,
) -> dict:
    """Replay one entry with a fixed target and stops which can only tighten.

    ``active_stop_pct_at_exit`` is signed adverse distance from entry: +1.2
    means a loss stop at 1.2%; -0.1 means a stop protecting a gross 0.1% gain.
    Profit protection arms when a completed full post-entry candle's signed
    close return reaches ``protect_after_close_pct``. Its stop becomes active
    at the following candle open. Optional scheduled tightening similarly
    activates at the first bar open at/after entry-bar end plus its delay.

    Stop gaps after the entry candle fill at the observed open. Targets retain
    native conservative limit-price fills, and an intrabar stop/target tie is
    a stop. The original entry candle's open cannot fill an exit because it
    may predate the intrabar entry. At 15:15, native stops/targets retain
    intrabar precedence before square-off at that completed candle's close.

    Excursions retain native OHLC diagnostic conventions for ambiguous entry
    and intrabar exit candles, but exclude an OPEN exit candle's later range.
    Only the open fill itself is then included. These diagnostics never arm
    protection. Surviving a path without its 15:15 candle raises an error.
    """
    entry = _finite(entry_price, "entry_price")
    initial = _finite(stop_pct, "stop_pct")
    target_pct = _finite(target_pct, "target_pct")
    protected_profit = _finite(protected_profit_pct, "protected_profit_pct")
    if entry <= 0 or not 0 < initial < 100 or not 0 < target_pct < 100:
        raise ValueError("Entry must be positive; stop and target must be between 0 and 100 percent")
    if not isinstance(is_long, (bool, np.bool_)):
        raise ValueError("is_long must be a boolean")
    if not isinstance(entry_index, (int, np.integer)) or isinstance(entry_index, (bool, np.bool_)):
        raise ValueError("entry_index must be an integer")
    if protected_profit < 0:
        raise ValueError("protected_profit_pct must be nonnegative")
    activation_pct = None
    if protect_after_close_pct is not None:
        activation_pct = _finite(protect_after_close_pct, "protect_after_close_pct")
        if activation_pct <= 0 or protected_profit >= activation_pct:
            raise ValueError("Protection activation must exceed the nonnegative protected profit")
    elif protected_profit != 0:
        raise ValueError("Protected profit requires a close activation threshold")
    if (tighten_after_minutes is None) != (tightened_stop_pct is None):
        raise ValueError("Scheduled tightening needs both its delay and stop percentage")
    delay, timed_stop = None, None
    if tighten_after_minutes is not None:
        delay = _finite(tighten_after_minutes, "tighten_after_minutes")
        timed_stop = _finite(tightened_stop_pct, "tightened_stop_pct")
        if delay <= 0 or not 0 < timed_stop <= initial:
            raise ValueError("Scheduled tightening needs a positive delay and cannot loosen the initial stop")

    try:
        stamps = np.asarray(path["timestamp_ns"])
        values = {name: np.asarray(path[name], dtype=float) for name in ("open", "high", "low", "close")}
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Path requires timestamp_ns and numeric one-minute OHLC arrays") from exc
    if (stamps.ndim != 1 or not np.issubdtype(stamps.dtype, np.integer)
            or not len(stamps) or any(a.ndim != 1 or len(a) != len(stamps) for a in values.values())):
        raise ValueError("Path arrays must be equally sized, nonempty vectors with integer nanosecond times")
    if not 0 <= entry_index < len(stamps):
        raise ValueError("entry_index is outside the path")
    if np.any(np.diff(stamps) <= 0):
        raise ValueError("Path candle-end timestamps must increase strictly")
    if any(not np.isfinite(a).all() or np.any(a <= 0) for a in values.values()):
        raise ValueError("Path prices must be finite and positive")
    if (np.any(values["low"] > np.minimum(values["open"], values["close"]))
            or np.any(values["high"] < np.maximum(values["open"], values["close"]))):
        raise ValueError("Invalid OHLC candle")
    entry_end_ns = int(stamps[entry_index])
    entry_end = pd.Timestamp(entry_end_ns, tz="UTC").tz_convert(IST)
    squareoff = entry_end.normalize() + pd.Timedelta(hours=15, minutes=15)
    squareoff_ns = int(squareoff.value)
    if entry_end_ns > squareoff_ns:
        raise ValueError("Entry is later than native 15:15 square-off")
    finish = int(np.searchsorted(stamps, squareoff_ns, side="right"))
    if np.any(np.diff(stamps[entry_index:finish]) != MINUTE_NS):
        raise ValueError("Post-entry replay requires contiguous one-minute candles")
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    activation_price = entry * (1 + sign * activation_pct / 100) if activation_pct is not None else None
    active_stop = initial
    timer_ns = entry_end_ns + round(delay * MINUTE_NS) if delay is not None else None
    protection_armed_index = None
    protection_effective_index = None
    protection_applied = False

    def result(index, price, reason, event, *, ambiguous=False):
        end = pd.Timestamp(int(stamps[index]), tz="UTC").tz_convert(IST)
        executed = end - pd.Timedelta(minutes=1) if event == "OPEN" else end
        gross = sign * (price / entry - 1) * 100
        last = index - 1 if event == "OPEN" else index
        if last >= entry_index:
            favorable = values["high" if is_long else "low"][entry_index:last+1]
            adverse = values["low" if is_long else "high"][entry_index:last+1]
            mfe = max(0., gross, float(np.max(sign * (favorable / entry - 1) * 100)))
            mae = min(0., gross, float(np.min(sign * (adverse / entry - 1) * 100)))
        else:
            mfe, mae = max(0., gross), min(0., gross)
        stopped = reason in {"STOP", "TIGHTENED_STOP", "PROTECTED_STOP"}
        stop_level = entry * (1 - sign * active_stop / 100)
        gap = bool(stopped and event == "OPEN" and abs(price-stop_level) > 1e-8)
        return dict(
            exit_path_index=int(index), exit_price=float(price), exit_reason=reason,
            exit_event=event, exit_ts=executed, exit_execution_ts=str(executed),
            exit_bar_end_ts=str(end), active_stop_pct_at_exit=float(active_stop),
            active_stop_price=float(stop_level), initial_stop_pct=initial,
            gross_return_pct=float(gross), mfe_pct=float(mfe), mae_pct=float(mae),
            holding_minutes=(executed-entry_end).total_seconds()/60,
            stop_hit=stopped, target_hit=reason == "TARGET",
            first_target_hit=reason == "TARGET", runner_target_hit=reason == "TARGET",
            same_bar_ambiguous=bool(ambiguous), exit_gap_through=gap,
            exit_gap_bps=abs(price/stop_level-1)*10000 if gap else 0.,
            protection_armed_path_index=protection_armed_index,
            protection_effective_path_index=protection_effective_index,
            protection_applied=protection_applied,
        )

    for index in range(entry_index, finish):
        open_ns = int(stamps[index]) - MINUTE_NS
        if timer_ns is not None and open_ns >= timer_ns:
            active_stop = min(active_stop, timed_stop)
        if protection_effective_index is not None and index >= protection_effective_index:
            active_stop = min(active_stop, -protected_profit)
            protection_applied = True
        stop = entry * (1 - sign * active_stop / 100)
        op, hi, lo, close = (float(values[name][index]) for name in ("open", "high", "low", "close"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            at_open = index > entry_index and stop_open
            reason = ("PROTECTED_STOP" if protection_applied else
                      "TIGHTENED_STOP" if active_stop < initial else "STOP")
            return result(index, op if at_open else stop, reason,
                          "OPEN" if at_open else "INTRABAR", ambiguous=not at_open and target_hit)
        if target_hit:
            return result(index, target, "TARGET", "INTRABAR")
        if int(stamps[index]) == squareoff_ns:
            return result(index, close, "TIME_EXIT_1515", "CLOSE")
        # The entry candle contains an unknown intraminute entry and is never
        # a full post-entry observation. Only a later completed close can arm.
        if (activation_pct is not None and protection_armed_index is None
                and index > entry_index and open_ns >= entry_end_ns
                and (close >= activation_price if is_long else close <= activation_price)):
            protection_armed_index = index
            protection_effective_index = index + 1
    raise ValueError("Position survives a path missing its native 15:15 square-off candle")
