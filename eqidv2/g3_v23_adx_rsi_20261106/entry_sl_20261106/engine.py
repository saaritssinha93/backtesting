"""Entry-price stop replay with causal, optional close-confirmed profit locks.

The initial distance must be computed before entry using causal source inputs.
One-minute OHLC cannot reveal the entry-bar path: this engine preserves the
native conservative full-bar, stop-first assumption on that bar. Its open is
never used for a stop fill because it can precede an intrabar entry. Thereafter
an adverse gap through an active stop fills at the actual open. Target and
15:15 execution conventions are inherited from the native replay.
"""
from __future__ import annotations

import numpy as np

MINUTE_NS = 60_000_000_000


def replay(path, entry_index, entry, is_long, target_pct, initial_stop_pct=1.25,
           breakeven_trigger_pct=None, lock_return_pct=.05, trigger_on='close'):
    """Return the native-shaped exit dictionary for one unchanged entry.

    LONG initial SL = entry * (1 - initial_stop_pct / 100); SHORT reverses
    the sign. The native 120-minute tightening can reduce the risk to 1%,
    but never widens a tighter initial stop. Optional progress locks observe
    only completed post-entry closes and become active on the following bar.
    A +0.05% price lock offsets the inherited model's 5bps round-trip fee; it
    is not guaranteed breakeven when gaps or additional slippage occur.

    The caller supplies a verified minute path already ending at 15:15 and
    preserves entry identity, quantity and costs outside this price engine.
    """
    if trigger_on != 'close':
        raise ValueError('Only completed-close progress triggers are supported')
    if (not np.isfinite(entry) or entry <= 0 or not np.isfinite(target_pct) or target_pct <= 0
            or not np.isfinite(initial_stop_pct) or not 0 < initial_stop_pct <= 1.25):
        raise ValueError('Entry, target and initial stop must be finite; stop must be in (0,1.25]')
    if not np.isfinite(lock_return_pct) or lock_return_pct < 0:
        raise ValueError('Progress lock return must be finite and nonnegative')
    if breakeven_trigger_pct is not None:
        if (not np.isfinite(breakeven_trigger_pct) or breakeven_trigger_pct <= 0
                or lock_return_pct >= breakeven_trigger_pct):
            raise ValueError('Progress trigger must be finite, positive and above its lock')
    count = len(path['close'])
    if not isinstance(entry_index, (int, np.integer)) or not 0 <= entry_index < count:
        raise ValueError('Invalid native entry index')
    if any(len(path[key]) != count for key in ('timestamp_ns', 'open', 'high', 'low')):
        raise ValueError('Execution path arrays have different lengths')
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    entry_end = int(path['timestamp_ns'][entry_index])
    lock_active = False
    for j in range(entry_index, count):
        at = int(path['timestamp_ns'][j])
        native_stop = 1.0 if at - MINUTE_NS >= entry_end + 120 * MINUTE_NS else 1.25
        distance = min(float(initial_stop_pct), native_stop)
        stop_return = -distance
        if native_stop < initial_stop_pct:
            reason = 'TIGHTENED_STOP'
        elif initial_stop_pct < 1.25:
            reason = 'ENTRY_RELATIVE_STOP'
        else:
            reason = 'STOP'
        if lock_active:
            stop_return, reason = float(lock_return_pct), 'ENTRY_PROGRESS_LOCK'
        stop = entry * (1 + sign * stop_return / 100)
        op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return dict(index=j, price=op if open_fill else stop, reason=reason,
                        event='OPEN' if open_fill else 'INTRABAR',
                        ambiguous=bool(target_hit and not open_fill))
        if target_hit:
            return dict(index=j, price=target, reason='TARGET', event='INTRABAR', ambiguous=False)
        if breakeven_trigger_pct is not None and j > entry_index:
            observed_return = sign * (close / entry - 1) * 100
            if observed_return + 1e-10 >= breakeven_trigger_pct:
                # Set after evaluating this bar; only the next bar sees it.
                lock_active = True
    return dict(index=count - 1, price=float(path['close'][-1]), reason='TIME_EXIT_1515',
                event='CLOSE', ambiguous=False)
