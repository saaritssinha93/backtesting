"""Four fixed entry-stop policies with identical native entries and targets.

The constant-1.50% policy explicitly permits a larger loss on an individual
trade. It is a research comparison, not a guarantee that aggregate losses fall.
The caller supplies hash-verified one-minute paths ending at 15:15; a policy
can hold beyond the old native stop, but never beyond its supplied session path.
"""
from __future__ import annotations

import numpy as np

MINUTE_NS = 60_000_000_000
BASELINE = 'baseline_v21_g7'
CANDIDATES = {
    BASELINE: dict(initial_stop_pct=1.25, tighten_after_minutes=120, tightened_stop_pct=1.0),
    'constant_1p25': dict(initial_stop_pct=1.25, tighten_after_minutes=None, tightened_stop_pct=1.0),
    'delay_tighten240': dict(initial_stop_pct=1.25, tighten_after_minutes=240, tightened_stop_pct=1.0),
    'constant_1p50': dict(initial_stop_pct=1.5, tighten_after_minutes=None, tightened_stop_pct=1.0),
}


def _settings(settings):
    if not isinstance(settings, dict) or not set(settings).issubset(
            {'initial_stop_pct','tighten_after_minutes','tightened_stop_pct'}):
        raise ValueError('Unknown fixed stop-policy settings')
    initial = settings.get('initial_stop_pct',1.25)
    after = settings.get('tighten_after_minutes',120)
    tightened = settings.get('tightened_stop_pct',1.0)
    if not np.isfinite(initial) or initial not in (1.25,1.5):
        raise ValueError('Initial distance must be one of the declared 1.25% or 1.50% values')
    if after not in (None,120,240):
        raise ValueError('Tightening must be absent or use the declared 120/240-minute interval')
    if not np.isfinite(tightened) or tightened != 1.0 or tightened > initial:
        raise ValueError('The only declared tightened distance is 1%, without risk widening')
    return float(initial),after,float(tightened)


def replay(path, entry_index, entry, is_long, target_pct, settings):
    """Return a native-shaped exit dictionary for one unchanged entry.

    Stops are measured from actual entry, with the sign reversed for SHORT.
    Tightening activates only on a bar whose start is at least the configured
    interval after the entry bar's completed timestamp, matching native timing.
    Conservative entry-bar full-range/stop-first handling is inherited: its
    open cannot fill a stop because it may precede an intrabar entry. Later
    adverse gaps fill at the actual open. No indicator, label or outcome is
    used to decide which individual trade receives a policy.
    """
    initial,after,tightened = _settings(settings)
    if not np.isfinite(entry) or entry <= 0 or not np.isfinite(target_pct) or target_pct <= 0:
        raise ValueError('Entry and target must be finite and positive')
    count = len(path['close'])
    if not isinstance(entry_index,(int,np.integer)) or not 0 <= entry_index < count:
        raise ValueError('Invalid native entry index')
    if any(len(path[key]) != count for key in ('timestamp_ns','open','high','low')):
        raise ValueError('Execution path array lengths differ')
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    entry_end = int(path['timestamp_ns'][entry_index])
    for j in range(entry_index,count):
        at = int(path['timestamp_ns'][j])
        active_tightening = after is not None and at - MINUTE_NS >= entry_end + after * MINUTE_NS
        distance = min(initial,tightened) if active_tightening else initial
        reason = 'TIGHTENED_STOP' if active_tightening and tightened < initial else 'STOP'
        stop = entry * (1 - sign * distance / 100)
        op,hi,lo,close = [float(path[key][j]) for key in ('open','high','low','close')]
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return dict(index=j,price=op if open_fill else stop,reason=reason,
                        event='OPEN' if open_fill else 'INTRABAR',
                        ambiguous=bool(target_hit and not open_fill))
        if target_hit:
            return dict(index=j,price=target,reason='TARGET',event='INTRABAR',ambiguous=False)
    return dict(index=count-1,price=float(path['close'][-1]),reason='TIME_EXIT_1515',
                event='CLOSE',ambiguous=False)
