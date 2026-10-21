"""Causal percentage-of-price trailing exit for offline G4 research only."""
from __future__ import annotations

import json
import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import IST, MINUTE_NS


def simulate_percentage_trailing_exit(
    path: Mapping, entry_index: int, entry_price: float, is_long: bool,
    target_pct: float, config: Mapping,
) -> dict:
    """Trail the best completed post-entry close by a percentage of that close.

    A completed candle may arm or ratchet only the following candle's stop.
    Stop/target ties are stop-first; gaps through a resting stop fill at open.
    """
    entry = float(entry_price)
    target_pct = float(target_pct)
    initial = float(config["initial_stop_pct"])
    timed = float(config["tightened_stop_pct"])
    delay = float(config["tighten_after_minutes"])
    activation = max(float(config["activation_floor_pct"]),
                     float(config["activation_target_fraction"]) * target_pct)
    distance = float(config["trail_distance_pct"])
    protected = config["protected_profit_pct"]
    protected = None if protected is None else float(protected)
    if (not all(math.isfinite(x) for x in (entry, target_pct, initial, timed, delay,
                                          activation, distance))
            or not entry > 0 or not 0 < target_pct < 100 or not 0 < timed <= initial < 100
            or not 0 < distance < 100 or delay <= 0 or activation <= 0
            or (protected is not None and (not math.isfinite(protected)
                                           or not 0 <= protected < activation))):
        raise ValueError("Invalid percentage trailing parameters")
    if not isinstance(is_long, (bool, np.bool_)):
        raise ValueError("is_long must be boolean")
    if not isinstance(entry_index, (int, np.integer)) or isinstance(entry_index, (bool, np.bool_)):
        raise ValueError("entry_index must be an integer")
    stamps = np.asarray(path["timestamp_ns"])
    values = {name: np.asarray(path[name], dtype=float)
              for name in ("open", "high", "low", "close")}
    if (stamps.ndim != 1 or not np.issubdtype(stamps.dtype, np.integer)
            or any(a.ndim != 1 or len(a) != len(stamps) for a in values.values())
            or not 0 <= entry_index < len(stamps)):
        raise ValueError("Invalid one-minute execution path")
    if (np.any(np.diff(stamps) <= 0) or
            any(not np.isfinite(a).all() or np.any(a <= 0) for a in values.values()) or
            np.any(values["low"] > np.minimum(values["open"], values["close"])) or
            np.any(values["high"] < np.maximum(values["open"], values["close"]))):
        raise ValueError("Invalid OHLC execution path")
    entry_end = pd.Timestamp(int(stamps[entry_index]), tz="UTC").tz_convert(IST)
    squareoff_ns = int((entry_end.normalize() + pd.Timedelta(hours=15, minutes=15)).value)
    finish = int(np.searchsorted(stamps, squareoff_ns, side="right"))
    if entry_end.value > squareoff_ns or finish <= entry_index or int(stamps[finish - 1]) != squareoff_ns:
        raise ValueError("Execution path must contain the native 15:15 square-off candle")
    if np.any(np.diff(stamps[entry_index:finish]) != MINUTE_NS):
        raise ValueError("Percentage replay requires contiguous one-minute candles")

    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    timer_ns = int(stamps[entry_index]) + round(delay * MINUTE_NS)
    stop = entry * (1 - sign * initial / 100)
    stop_reason = "STOP"
    pending = None
    armed_index = None
    effective_index = None
    best_close = None
    updates = []

    def tighter(current: float, candidate: float) -> float:
        return max(current, candidate) if is_long else min(current, candidate)

    def result(index: int, price: float, reason: str, event: str, *, ambiguous=False) -> dict:
        end = pd.Timestamp(int(stamps[index]), tz="UTC").tz_convert(IST)
        executed = end - pd.Timedelta(minutes=1) if event == "OPEN" else end
        gross = sign * (price / entry - 1) * 100
        last = index - 1 if event == "OPEN" else index
        if last >= entry_index:
            favorable = values["high" if is_long else "low"][entry_index:last + 1]
            adverse = values["low" if is_long else "high"][entry_index:last + 1]
            mfe = max(0., gross, float(np.max(sign * (favorable / entry - 1) * 100)))
            mae = min(0., gross, float(np.min(sign * (adverse / entry - 1) * 100)))
        else:
            mfe, mae = max(0., gross), min(0., gross)
        stopped = reason in {"STOP", "TIGHTENED_STOP", "PERCENT_TRAILING_STOP"}
        gap = bool(stopped and event == "OPEN" and abs(price - stop) > 1e-8)
        return dict(
            exit_path_index=int(index), exit_price=float(price), exit_reason=reason,
            exit_event=event, exit_ts=executed, exit_execution_ts=str(executed),
            exit_bar_end_ts=str(end), active_stop_pct_at_exit=float(sign * (1 - stop / entry) * 100),
            active_stop_price=float(stop), initial_stop_pct=initial,
            gross_return_pct=float(gross), mfe_pct=float(mfe), mae_pct=float(mae),
            holding_minutes=(executed - entry_end).total_seconds() / 60,
            stop_hit=stopped, target_hit=reason == "TARGET",
            first_target_hit=reason == "TARGET", runner_target_hit=reason == "TARGET",
            same_bar_ambiguous=bool(ambiguous), exit_gap_through=gap,
            exit_gap_bps=abs(price / stop - 1) * 10000 if gap else 0.,
            protection_armed_path_index=armed_index,
            protection_effective_path_index=effective_index,
            protection_applied=effective_index is not None,
            stop_updates_json=json.dumps(updates),
        )

    for index in range(entry_index, finish):
        if int(stamps[index]) - MINUTE_NS >= timer_ns:
            revised = tighter(stop, entry * (1 - sign * timed / 100))
            if sign * (revised - stop) > 1e-10:
                stop, stop_reason = revised, "TIGHTENED_STOP"
        if pending is not None:
            revised = tighter(stop, pending)
            if sign * (revised - stop) > 1e-10:
                stop, stop_reason = revised, "PERCENT_TRAILING_STOP"
                if effective_index is None:
                    effective_index = index
            pending = None
        op, high, low, close = (float(values[name][index])
                                for name in ("open", "high", "low", "close"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = low <= stop if is_long else high >= stop
        target_hit = high >= target if is_long else low <= target
        if stop_hit:
            at_open = index > entry_index and stop_open
            return result(index, op if at_open else stop, stop_reason,
                          "OPEN" if at_open else "INTRABAR",
                          ambiguous=not at_open and target_hit)
        if target_hit:
            return result(index, target, "TARGET", "INTRABAR")
        if int(stamps[index]) == squareoff_ns:
            return result(index, close, "TIME_EXIT_1515", "CLOSE")
        if index <= entry_index:
            continue
        best_close = close if best_close is None else tighter(best_close, close)
        trigger = entry * (1 + sign * activation / 100)
        if armed_index is None and (close >= trigger if is_long else close <= trigger):
            armed_index = index
        if armed_index is not None:
            candidate = best_close * (1 - sign * distance / 100)
            if protected is not None:
                candidate = tighter(candidate, entry * (1 + sign * protected / 100))
            pending = tighter(stop, candidate)
            updates.append(dict(decision_path_index=index, effective_path_index=index + 1,
                best_completed_close=best_close, trail_distance_pct=distance,
                proposed_stop=candidate, ratcheted_stop=pending))
    raise ValueError("Position survives without an exit")
