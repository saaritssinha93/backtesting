"""Research-only confirmation-boundary invalidation for frozen G4 entries.

The first completed full post-entry minute closing strictly through the adverse
confirmation boundary requests an exit at the following minute's open. SHORT
is the primary experiment; LONG is an explicitly enabled symmetric experiment.
Native stop, target, timed tightening and 15:15 square-off remain in force.
There are no parameter searches or changes to entry selection in this module.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import IST, MINUTE_NS, simulate_exit
from research.g4_partial_day_backtest import partial_exit


RULE = "CONFIRMATION_BOUNDARY_CLOSE_NEXT_OPEN"


def _boundary(value, name):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{name} must be a finite positive price")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a finite positive price") from error
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be a finite positive price")
    return result


def _excursions(path, entry_index, last_index, entry_price, is_long, price):
    sign = 1 if is_long else -1
    gross = sign * (price / entry_price - 1) * 100
    favorable = np.asarray(path["high" if is_long else "low"], float)[entry_index:last_index + 1]
    adverse = np.asarray(path["low" if is_long else "high"], float)[entry_index:last_index + 1]
    mfe = max(0., gross, float(np.max(sign * (favorable / entry_price - 1) * 100)))
    mae = min(0., gross, float(np.min(sign * (adverse / entry_price - 1) * 100)))
    return gross, mfe, mae


def _open_result(native, path, entry_index, entry_price, is_long, index, price, reason):
    """Use only earlier candle ranges and the executable OPEN itself."""
    stamps = path["timestamp_ns"]
    end = pd.Timestamp(int(stamps[index]), tz="UTC").tz_convert(IST)
    executed = end - pd.Timedelta(minutes=1)
    first = pd.Timestamp(int(stamps[entry_index]), tz="UTC").tz_convert(IST)
    active = 1. if int(stamps[index]) - MINUTE_NS >= int(stamps[entry_index]) + 120 * MINUTE_NS else 1.2
    sign = 1 if is_long else -1
    gross, mfe, mae = _excursions(path, entry_index, index - 1, entry_price, is_long, price)
    result = dict(native)
    result.update(
        position_open=False, mark_price=np.nan, mark_ts=pd.NaT,
        exit_path_index=int(index), exit_price=float(price), exit_reason=reason,
        exit_event="OPEN", exit_ts=executed, exit_execution_ts=str(executed),
        exit_bar_end_ts=str(end), active_stop_pct_at_exit=active,
        active_stop_price=entry_price * (1 - sign * active / 100), initial_stop_pct=1.2,
        gross_return_pct=gross, mfe_pct=mfe, mae_pct=mae,
        holding_minutes=(executed - first).total_seconds() / 60,
        stop_hit=False, target_hit=reason == "TARGET",
        first_target_hit=reason == "TARGET", runner_target_hit=reason == "TARGET",
        same_bar_ambiguous=False, exit_gap_through=False, exit_gap_bps=0.,
        protection_armed_path_index=None, protection_effective_path_index=None,
        protection_applied=False,
    )
    return result


def simulate_failed_breakout_exit(
    path: Mapping,
    entry_index: int,
    entry_price: float,
    is_long: bool,
    target_pct: float,
    confirmation_high: float,
    confirmation_low: float,
    *,
    partial: bool = False,
    enable_long: bool = False,
) -> dict:
    """Replay an already-filled entry; the caller retains unfilled orders.

    The native 1.20% stop tightens to 1.00% at the first bar OPEN at/after the
    entry-bar end plus 120 minutes. Times label candle ends. An invalidating
    close at index ``i > entry_index`` can first execute at ``i + 1`` OPEN,
    whose timestamp is ``timestamp_ns[i + 1] - MINUTE_NS``. No later range of
    that execution candle contributes to MFE/MAE.

    Native intrabar exits through the signal candle have priority. At the next
    open, a native stop gap or already-marketable target also has priority over
    the requested discretionary exit. A marketable target preserves the native
    candle treatment exactly, including its conservative stop-first ambiguity
    and end timestamp. This experiment changes only the discretionary exit.
    Otherwise, later intrabar touches cannot defeat an exit filled at OPEN.
    A surviving partial path requires ``partial=True`` and remains open when
    the signal has no observed following bar. A full path must contain native
    square-off evidence unless the native position closes earlier.
    """
    high = _boundary(confirmation_high, "confirmation_high")
    low = _boundary(confirmation_low, "confirmation_low")
    if low > high:
        raise ValueError("confirmation_low cannot exceed confirmation_high")
    if not isinstance(partial, (bool, np.bool_)) or not isinstance(enable_long, (bool, np.bool_)):
        raise ValueError("partial and enable_long must be booleans")
    if partial:
        native = partial_exit(path, entry_index, entry_price, is_long, target_pct)
    else:
        native = simulate_exit(path, entry_index, entry_price, is_long, 1.2, target_pct,
                               tighten_after_minutes=120, tightened_stop_pct=1.)
        native.update(position_open=False, mark_price=np.nan, mark_ts=pd.NaT)
    stamps = np.asarray(path["timestamp_ns"])
    entry_price = float(entry_price)
    if native["position_open"]:
        gross, mfe, mae = _excursions(path, entry_index, len(stamps) - 1, entry_price,
                                     is_long, float(native["mark_price"]))
        native.update(gross_return_pct=gross, mfe_pct=mfe, mae_pct=mae,
                      holding_minutes=(int(stamps[-1]) - int(stamps[entry_index])) / MINUTE_NS,
                      exit_execution_ts=None, exit_bar_end_ts=None,
                      exit_gap_through=False, exit_gap_bps=0.,
                      first_target_hit=False, runner_target_hit=False,
                      protection_armed_path_index=None, protection_effective_path_index=None,
                      protection_applied=False)
    audit = dict(
        failed_breakout_rule=RULE, failed_breakout_rule_enabled=bool(not is_long or enable_long),
        failed_breakout_exit_applied=False, failed_breakout_audit_reason="NO_INVALIDATING_CLOSE",
        invalidation_signal_path_index=None, invalidation_signal_ts=None,
        invalidation_signal_close=None, invalidation_boundary=low if is_long else high,
        invalidation_pending_next_open=False,
        native_exit_reason=native["exit_reason"], native_exit_ts=native["exit_ts"],
    )

    def finish(result, why):
        return {**result, **audit, "failed_breakout_audit_reason": why}

    if is_long and not enable_long:
        return finish(native, "LONG_RULE_DISABLED")
    squareoff = pd.Timestamp(int(stamps[entry_index]), tz="UTC").tz_convert(IST).normalize() + pd.Timedelta(hours=15, minutes=15)
    native_index = native["exit_path_index"]
    for index in range(entry_index + 1, len(stamps)):
        # A completed close cannot be observed by a position which already
        # stopped/targeted intrabar, including this same signal candle.
        if native_index is not None and index >= native_index:
            return finish(native, "NATIVE_EXIT_BEFORE_INVALIDATION_CLOSE")
        if int(stamps[index]) >= int(squareoff.value):
            return finish(native, "NATIVE_SQUAREOFF_PRECEDENCE")
        close = float(path["close"][index])
        breached = close < low if is_long else close > high
        if not breached:
            continue
        audit.update(invalidation_signal_path_index=index,
                     invalidation_signal_ts=str(pd.Timestamp(int(stamps[index]), tz="UTC").tz_convert(IST)),
                     invalidation_signal_close=close)
        following = index + 1
        if following >= len(stamps):
            audit["invalidation_pending_next_open"] = True
            return finish(native, "PENDING_NEXT_OPEN_AT_CUTOFF")
        opening = float(path["open"][following])
        sign = 1 if is_long else -1
        active = 1. if int(stamps[following]) - MINUTE_NS >= int(stamps[entry_index]) + 120 * MINUTE_NS else 1.2
        stop = entry_price * (1 - sign * active / 100)
        target = entry_price * (1 + sign * float(target_pct) / 100)
        if opening <= stop if is_long else opening >= stop:
            # The native simulator already uses observed-open stop gaps.
            return finish(native, "NATIVE_STOP_AT_NEXT_OPEN")
        if opening >= target if is_long else opening <= target:
            return finish(native, "NATIVE_TARGET_OPEN_PRECEDENCE")
        audit["failed_breakout_exit_applied"] = True
        return finish(_open_result(native, path, entry_index, entry_price, is_long,
                                   following, opening, "FAILED_BREAKOUT_EXIT"), "FAILED_BREAKOUT_EXIT")
    return finish(native, "NO_INVALIDATING_CLOSE")
