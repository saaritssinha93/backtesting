"""Causal confirmation stretch and pending-breakout invalidation checks.

These optional research vetoes run after the existing stock ranking. A veto
skips the selected order; it never substitutes the next-ranked stock.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from g3_one_minute_extension import last_features, extension_atr


def confirmation_features(raw, row):
    stamp = pd.Timestamp(row['confirmation_ts'])
    if stamp.tzinfo is None:
        raise ValueError('Confirmation must have a timezone')
    observed = raw.loc[raw.ts.eq(stamp)]
    if len(observed) != 1:
        raise ValueError('Exact confirmation candle required')
    bar = observed.iloc[0]
    for name in ('open', 'high', 'low', 'close'):
        if not np.isclose(float(bar[name]), float(row['confirmation_' + name]), atol=.001, rtol=0):
            raise ValueError('Confirmation OHLC differs from the selected signal')
    prior = raw.loc[raw.ts.lt(stamp)].drop_duplicates('ts', keep='last')
    ema, atr = last_features(prior, bar.to_dict())
    extension = extension_atr(bar.close, ema, atr, row['side'])
    if not np.isfinite(extension):
        raise ValueError('Invalid confirmation extension evidence')
    return dict(entry_veto_ema20=ema, entry_veto_atr14=atr,
                entry_veto_extension_atr=extension)


def pending_invalidation(row, path, expiry_minutes=10):
    """Cancel only after a completed pre-fill close breaks the opposite edge.

    A trigger touch takes precedence over the close in the SAME minute: the
    order has already filled before that close can be observed. No candle at
    or after the first native trigger touch can cancel an existing fill.
    """
    if row['side'] not in ('LONG', 'SHORT') or expiry_minutes <= 0:
        raise ValueError('Invalid side or entry expiry')
    is_long = row['side'] == 'LONG'
    trigger = float(row['trigger'])
    opposite = float(row['confirmation_low' if is_long else 'confirmation_high'])
    names = ('timestamp_ns', 'open', 'high', 'low', 'close')
    values = {name: np.asarray(path[name]) for name in names}
    n = len(values['close'])
    if n < expiry_minutes or any(len(a) != n for a in values.values()):
        raise ValueError('Complete pending-entry path required')
    stamp = pd.Timestamp(row['confirmation_ts'])
    expected = stamp.value + np.arange(1, expiry_minutes + 1) * 60_000_000_000
    if not np.array_equal(values['timestamp_ns'][:expiry_minutes], expected):
        raise ValueError('Pending-entry candles must be consecutive and end-labelled')
    if not np.isfinite([trigger, opposite]).all() or min(trigger, opposite) <= 0:
        raise ValueError('Invalid entry levels')
    for index in range(expiry_minutes):
        op, hi, lo, close = [float(values[k][index]) for k in ('open', 'high', 'low', 'close')]
        if (not np.isfinite([op, hi, lo, close]).all() or min(op, lo, close) <= 0
                or lo > min(op, close) or hi < max(op, close)):
            raise ValueError('Invalid pending-entry OHLC')
        hit = hi >= trigger if is_long else lo <= trigger
        if hit:
            return dict(pending_cancelled=False, pending_cancel_ts=None,
                        pending_cancel_close=None, pending_fill_index=index)
        invalid = close < opposite if is_long else close > opposite
        if invalid:
            end = pd.Timestamp(int(values['timestamp_ns'][index]), tz='UTC').tz_convert(stamp.tz)
            return dict(pending_cancelled=True, pending_cancel_ts=str(end),
                        pending_cancel_close=close, pending_fill_index=None)
    return dict(pending_cancelled=False, pending_cancel_ts=None,
                pending_cancel_close=None, pending_fill_index=None)
