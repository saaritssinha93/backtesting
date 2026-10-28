"""Bounded, causal G7 V4 exit research. No sizing, allocation, or live wiring.

Minute timestamps label candle ends. New decisions use only fully post-entry
completed candles and become effective at the next minute open. Native stops
and targets precede a decision made at the same candle's close. At an already
scheduled market exit, a stop breached at the open takes precedence, followed
by a target already marketable at that open (filled at its conservative limit),
then the scheduled market exit. That candle's later range cannot change an
open exit. Without a scheduled market exit, native stop-first intrabar ordering
is preserved, as are the timed tightening and 15:15 square-off.

``policy=None`` delegates to the native simulator and returns its exact schema.
The three research policies add audit fields and never relax a protective stop.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import IST, MINUTE_NS, _finite
from research.g3_sl_target_simulator import simulate_exit as native_simulate_exit


POLICIES = ("confirmation_failure", "stalled_30m", "confirmed_trail")


def simulate_exit(
    path: Mapping,
    entry_index: int,
    entry_price: float,
    is_long: bool,
    stop_pct: float,
    target_pct: float,
    *,
    policy: str | None = None,
    confirmation_low: float | None = None,
    confirmation_high: float | None = None,
    tighten_after_minutes: float | None = 120,
    tightened_stop_pct: float | None = 1.0,
) -> dict:
    """Replay fixed entries under one of three predeclared exit hypotheses.

    confirmation_failure: two consecutive full post-entry closes strictly past
    the confirmation candle's adverse edge; exit at the following open.

    stalled_30m: evaluate exactly once, at the first full close at elapsed >=30
    minutes. Exit next open if the best full post-entry close return is below
    one quarter of initial risk and the current close return is negative.

    confirmed_trail: once at least three full post-entry bars exist, a completed
    close reaching half the native target arms a three-bar low (long) / high
    (short) stop. Reconsider after every later close; apply only tighter levels
    at the following open. Entry-bar observations never enter this window.
    """
    if policy is None:
        return native_simulate_exit(
            path, entry_index, entry_price, is_long, stop_pct, target_pct,
            tighten_after_minutes=tighten_after_minutes,
            tightened_stop_pct=tightened_stop_pct,
        )
    if policy not in POLICIES:
        raise ValueError(f"Unknown research exit policy: {policy!r}")

    entry = _finite(entry_price, "entry_price")
    initial = _finite(stop_pct, "stop_pct")
    target_pct = _finite(target_pct, "target_pct")
    if entry <= 0 or not 0 < initial < 100 or not 0 < target_pct < 100:
        raise ValueError("Entry must be positive; stop and target must be between 0 and 100 percent")
    if not isinstance(is_long, (bool, np.bool_)):
        raise ValueError("is_long must be a boolean")
    if not isinstance(entry_index, (int, np.integer)) or isinstance(entry_index, (bool, np.bool_)):
        raise ValueError("entry_index must be an integer")
    if (tighten_after_minutes is None) != (tightened_stop_pct is None):
        raise ValueError("Scheduled tightening needs both its delay and stop percentage")
    delay, timed_stop = None, None
    if tighten_after_minutes is not None:
        delay = _finite(tighten_after_minutes, "tighten_after_minutes")
        timed_stop = _finite(tightened_stop_pct, "tightened_stop_pct")
        if delay <= 0 or not 0 < timed_stop <= initial:
            raise ValueError("Scheduled tightening needs a positive delay and cannot loosen the initial stop")
    edge = None
    if policy == "confirmation_failure":
        low = _finite(confirmation_low, "confirmation_low")
        high = _finite(confirmation_high, "confirmation_high")
        if low <= 0 or high < low:
            raise ValueError("Confirmation candle edges must be positive and ordered")
        edge = low if is_long else high

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
    squareoff_ns = int((entry_end.normalize() + pd.Timedelta(hours=15, minutes=15)).value)
    if entry_end_ns > squareoff_ns:
        raise ValueError("Entry is later than native 15:15 square-off")
    finish = int(np.searchsorted(stamps, squareoff_ns, side="right"))
    if np.any(np.diff(stamps[entry_index:finish]) != MINUTE_NS):
        raise ValueError("Post-entry replay requires contiguous one-minute candles")

    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    trail_activation = entry * (1 + sign * 0.5 * target_pct / 100)
    timer_ns = entry_end_ns + round(delay * MINUTE_NS) if delay is not None else None
    active_stop = initial
    stop_source = "STOP"
    pending_market = None
    pending_trail = None
    decision_index = None
    decision_reason = None
    consecutive_failures = 0
    stall_evaluation = None
    best_close_return = None
    best_close_price = None
    trail_armed = None
    trail_last_update = None
    trail_effective = None
    trail_applied = False

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
            protection_armed_path_index=trail_armed,
            protection_effective_path_index=None if trail_armed is None else trail_armed+1,
            protection_applied=trail_applied,
            research_exit_policy=policy,
            decision_path_index=decision_index, decision_reason=decision_reason,
            decision_effective_path_index=None if decision_index is None else decision_index+1,
            decision_ts=(None if decision_index is None else
                         str(pd.Timestamp(int(stamps[decision_index]), tz="UTC").tz_convert(IST))),
            # The following minute's open is the observed close's boundary.
            decision_effective_ts=(None if decision_index is None else
                                   str(pd.Timestamp(int(stamps[decision_index]), tz="UTC").tz_convert(IST))),
            confirmation_failure_count=consecutive_failures,
            stall_evaluation_path_index=stall_evaluation,
            best_completed_close_return_pct=best_close_return,
            trail_armed_path_index=trail_armed,
            trail_last_update_path_index=trail_last_update,
            trail_effective_path_index=trail_effective,
        )

    for index in range(entry_index, finish):
        open_ns = int(stamps[index]) - MINUTE_NS
        if timer_ns is not None and open_ns >= timer_ns and timed_stop < active_stop:
            active_stop, stop_source = timed_stop, "TIGHTENED_STOP"
        if pending_trail is not None:
            if pending_trail < active_stop:
                active_stop, stop_source = pending_trail, "PROTECTED_STOP"
                trail_effective, trail_applied = index, True
            pending_trail = None
        stop = entry * (1 - sign * active_stop / 100)
        op, hi, lo, close = (float(values[name][index]) for name in ("open", "high", "low", "close"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if index > entry_index and stop_open:
            return result(index, op, stop_source, "OPEN")
        if pending_market is not None:
            target_open = op >= target if is_long else op <= target
            if target_open:
                return result(index, target, "TARGET", "OPEN")
            return result(index, op, pending_market, "OPEN")
        if stop_hit:
            return result(index, stop, stop_source, "INTRABAR", ambiguous=target_hit)
        if target_hit:
            return result(index, target, "TARGET", "INTRABAR")
        if int(stamps[index]) == squareoff_ns:
            return result(index, close, "TIME_EXIT_1515", "CLOSE")
        if index <= entry_index or open_ns < entry_end_ns:
            continue

        close_return = sign * (close/entry - 1) * 100
        best_close_return = close_return if best_close_return is None else max(best_close_return, close_return)
        best_close_price = (close if best_close_price is None else
                            max(best_close_price, close) if is_long else min(best_close_price, close))
        if policy == "confirmation_failure":
            failed = close < edge if is_long else close > edge
            consecutive_failures = consecutive_failures+1 if failed else 0
            if consecutive_failures == 2:
                pending_market = decision_reason = "CONFIRMATION_FAILURE"
                decision_index = index
        elif policy == "stalled_30m":
            elapsed = (int(stamps[index])-entry_end_ns)/MINUTE_NS
            if stall_evaluation is None and elapsed >= 30:
                stall_evaluation = index
                # Compare prices so an exact threshold close is not rounded
                # just below the strictly-less-than return boundary.
                hurdle = entry*(1 + sign*0.25*initial/100)
                below_hurdle = best_close_price < hurdle if is_long else best_close_price > hurdle
                currently_losing = close < entry if is_long else close > entry
                if below_hurdle and currently_losing:
                    pending_market = decision_reason = "STALLED_30M"
                    decision_index = index
        elif index-entry_index >= 3:
            reached = close >= trail_activation if is_long else close <= trail_activation
            if trail_armed is None and reached:
                trail_armed = decision_index = index
                decision_reason = "CONFIRMED_TRAIL_ARM"
            if trail_armed is not None:
                window = values["low" if is_long else "high"][index-2:index+1]
                level = float(np.min(window) if is_long else np.max(window))
                candidate_stop = sign*(1-level/entry)*100
                if candidate_stop < active_stop:
                    pending_trail = candidate_stop
                    trail_last_update = index
    raise ValueError("Position survives a path missing its native 15:15 square-off candle")
