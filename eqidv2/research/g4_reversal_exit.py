"""Causal, next-open reversal exits for sealed G4 backtest research only."""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import IST, simulate_exit
from research.g4_failed_breakout_exit import _open_result


def simulate_reversal_exit(path: Mapping, entry_index: int, entry_price: float,
                           is_long: bool, target_pct: float, policy: Mapping) -> dict:
    """Exit at the next observed open after a completed close confirms reversal.

    Native stop, target, timed tightening and 15:15 square-off take precedence.
    An adverse-persistence signal needs consecutive closes beyond a loss
    threshold. A profitable-pullback signal requires a favorable peak, a
    retracement and a still-profitable completed close. Neither uses future bars.
    """
    kind = policy["kind"]
    if kind not in {"adverse_persistence", "profitable_pullback"}:
        raise ValueError("Unknown reversal policy kind")
    if not isinstance(is_long, (bool, np.bool_)):
        raise ValueError("is_long must be boolean")
    entry = float(entry_price)
    if not math.isfinite(entry) or entry <= 0:
        raise ValueError("Entry price must be positive and finite")
    if kind == "adverse_persistence":
        threshold = float(policy["loss_threshold_pct"])
        consecutive = policy["consecutive_closes"]
        if (not math.isfinite(threshold) or not 0 < threshold < 1.2
                or not isinstance(consecutive, int) or isinstance(consecutive, bool)
                or consecutive < 2):
            raise ValueError("Invalid adverse-persistence parameters")
    else:
        activation = float(policy["activation_profit_pct"])
        pullback = float(policy["pullback_pct"])
        minimum = float(policy["min_signal_profit_pct"])
        if (not all(math.isfinite(x) for x in (activation, pullback, minimum))
                or not 0 < activation < 2 or not 0 < pullback < 2
                or not 0 < minimum < activation):
            raise ValueError("Invalid profitable-pullback parameters")
    native = simulate_exit(path, entry_index, entry, is_long, 1.2, target_pct,
                           tighten_after_minutes=120, tightened_stop_pct=1.)
    stamps = np.asarray(path["timestamp_ns"])
    closes = np.asarray(path["close"], dtype=float)
    sign = 1 if is_long else -1
    native_index = int(native["exit_path_index"])
    squareoff_ns = int((pd.Timestamp(int(stamps[entry_index]), tz="UTC")
                        .tz_convert(IST).normalize() + pd.Timedelta(hours=15, minutes=15)).value)
    best_close = None
    adverse_count = 0
    audit = dict(reversal_rule=kind, reversal_exit_applied=False,
                 reversal_signal_path_index=None, reversal_signal_close=None,
                 reversal_signal_strength_pct=None)
    for index in range(entry_index + 1, min(native_index + 1, len(stamps))):
        # The native intrabar exit happens before this bar's completed close.
        if index >= native_index or int(stamps[index]) >= squareoff_ns:
            break
        close = float(closes[index])
        signed_pct = sign * (close / entry - 1) * 100
        best_close = close if best_close is None else (
            max(best_close, close) if is_long else min(best_close, close))
        if kind == "adverse_persistence":
            adverse_count = adverse_count + 1 if signed_pct <= -threshold else 0
            triggered = adverse_count >= consecutive
            strength = -signed_pct
        else:
            best_gain = sign * (best_close / entry - 1) * 100
            retrace = sign * (best_close - close) / best_close * 100
            triggered = (best_gain >= activation and retrace >= pullback
                         and signed_pct >= minimum)
            strength = retrace
        if not triggered:
            continue
        following = index + 1
        if following >= len(stamps):
            break
        opening = float(path["open"][following])
        timed = (int(stamps[following]) - 60_000_000_000 >=
                 int(stamps[entry_index]) + 120 * 60_000_000_000)
        stop = entry * (1 - sign * (1. if timed else 1.2) / 100)
        target = entry * (1 + sign * float(target_pct) / 100)
        # Resting native stop or marketable native target wins at the open.
        if (opening <= stop if is_long else opening >= stop):
            break
        if (opening >= target if is_long else opening <= target):
            break
        audit.update(reversal_exit_applied=True, reversal_signal_path_index=index,
                     reversal_signal_close=close, reversal_signal_strength_pct=strength)
        return {**_open_result(native, path, entry_index, entry, is_long,
                               following, opening, "REVERSAL_EXIT"), **audit}
    return {**native, **audit}
