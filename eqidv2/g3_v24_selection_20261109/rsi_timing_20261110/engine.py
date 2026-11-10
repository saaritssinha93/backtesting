"""Frozen, causal RSI reset/recovery entries with the adopted V23 exits.

Only completed one-minute observations can create an entry trigger. A trigger
can first fill on the following bar and expires with the original G3 order.
The two policies are fixed research hypotheses, never trading authority.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from g3_v24_selection_20261109.engine import SETTINGS, exits, exit_timestamp


MINUTE_NS = 60_000_000_000
EXPIRY_MINUTES = 10
POLICIES = ('rsi_reset_recovery', 'rsi_reset_recovery_keep_trigger')
EXPOSURE_RS = 500_000.
COST_RS = 250.
IDENTITY = ('sid', 'day', 'setup_id', 'tradingsymbol', 'side')
TRADE_COLUMNS = [*IDENTITY, 'entry_ts', 'exit_ts', 'entry_price', 'exit_price',
                 'exit_reason', 'portfolio_net_profit_rupees', 'policy']


def configuration():
    return dict(version='G3-V24-RSI-RESET-TIMING-20261110', scope='BACKTEST_ONLY',
        execution_authority=False, policies=list(POLICIES), expiry_minutes=EXPIRY_MINUTES,
        observe_from='original completed confirmation inclusive',
        observe_through='confirmation + 9 minutes; fills allowed through +10 minutes',
        reset_zone_inclusive=[45., 55.],
        reset_price_rule='LONG close < previous close; SHORT close > previous close',
        recovery_rule='first later LONG RSI previous<=55,current>55; SHORT previous>=45,current<45',
        reset_before_confirmation_allowed=False, reset_opposite_side_invalidation=False,
        trigger_rule='first recovery candle high for LONG or low for SHORT',
        control_trigger_rule='LONG max(original,cross high); SHORT min(original,cross low)',
        trigger_frozen_until_original_expiry=True, same_candle_entry=False,
        entry_gap_fill='worse bar open', entry_timestamp='completed entry-bar timestamp, matching native baseline',
        missing_observation='cancel only the affected order when the unavailable closed bar is observed',
        post_entry_missing_observation='does not retroactively cancel an existing entry',
        exit_policy='breakeven_eighty_target', exit_settings=SETTINGS,
        exposure_rs=EXPOSURE_RS, cost_rs=COST_RS, thresholds_optimized=False)


def _value(row, key):
    return row[key] if isinstance(row, Mapping) else getattr(row, key)


def _timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError('RSI timing confirmation must be timezone aware')
    return stamp.tz_convert('Asia/Kolkata')


def _clock(ns):
    return str(pd.Timestamp(int(ns), tz='UTC').tz_convert('Asia/Kolkata'))


def find_entry(order, path, window, policy='rsi_reset_recovery'):
    """Return one timing audit; each entry decision reads only its causal prefix.

``window[0]`` is the completed original confirmation. ``path[0]`` is the
following bar. During each bar an already armed trigger is checked before
that bar's closing RSI is observed. This ordering prevents future missing
RSI observations or a same-candle high from deciding an earlier entry.
"""
    if policy not in POLICIES:
        raise ValueError('Unknown RSI timing policy')
    sid = int(_value(order, 'sid'))
    side = _value(order, 'side')
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Invalid timing side')
    is_long = side == 'LONG'
    stamp = _timestamp(_value(order, 'confirmation_ts'))
    start_ns = stamp.tz_convert('UTC').value
    original_trigger = float(_value(order, 'trigger'))
    if not np.isfinite(original_trigger) or original_trigger <= 0:
        raise ValueError('Invalid original G3 trigger')
    result = dict(sid=sid, policy=policy, filled=False, status='NO_RESET_WITHIN_WINDOW',
        confirmation_ts=str(stamp), expiry_ts=str(stamp+pd.Timedelta(minutes=10)),
        original_trigger=original_trigger, reset_index=None, reset_ts=None, reset_rsi=None,
        cross_index=None, cross_ts=None, cross_rsi=None, trigger_used=None,
        entry_path_index=None, entry_price=None, entry_ts=None, entry_gap_through=None,
        entry_delay_minutes=None, invalid_observation_index=None,
        invalid_observation_status=None, observed_bars=0)
    if window is None:
        result['status'] = 'MISSING_FEATURE_WINDOW'
        return result
    if int(window.get('sid', sid)) != sid:
        raise ValueError('RSI feature SID mismatch')
    keys = ('timestamp_ns', 'open', 'high', 'low', 'close', 'rsi14', 'prior_close', 'valid', 'bar_status')
    if not all(key in window for key in keys):
        raise ValueError('Incomplete RSI feature window schema')
    lengths = {len(window[key]) for key in keys}
    if len(lengths) != 1:
        raise ValueError('RSI feature arrays have different lengths')
    count = len(path['timestamp_ns'])
    if any(len(path[key]) != count for key in ('open', 'high', 'low', 'close')):
        raise ValueError('Execution arrays have different lengths')
    pending = None
    previous_rsi = None

    def observe(q):
        nonlocal pending, previous_rsi
        if q >= len(window['timestamp_ns']):
            result.update(status='MISSING_OBSERVATION_BEFORE_ENTRY',
                invalid_observation_index=q, invalid_observation_status='MISSING_WINDOW_ROW')
            return False
        if int(window['timestamp_ns'][q]) != start_ns+q*MINUTE_NS:
            raise ValueError('RSI observation clock differs from the original window')
        result['observed_bars'] += 1
        if not bool(window['valid'][q] == True):
            result.update(status='MISSING_OBSERVATION_BEFORE_ENTRY',
                invalid_observation_index=q,
                invalid_observation_status=str(window['bar_status'][q]))
            return False
        rsi, close, prior = (float(window[key][q]) for key in ('rsi14', 'close', 'prior_close'))
        if not np.isfinite([rsi, close, prior]).all() or not 0 <= rsi <= 100 or min(close, prior) <= 0:
            raise ValueError('A READY RSI observation contains invalid values')
        if result['reset_index'] is None:
            adverse_close = close < prior if is_long else close > prior
            if 45. <= rsi <= 55. and adverse_close:
                result.update(reset_index=q, reset_ts=_clock(start_ns+q*MINUTE_NS), reset_rsi=rsi,
                              status='RESET_WITHOUT_RECOVERY')
        elif result['cross_index'] is None and q > result['reset_index']:
            crossed = (previous_rsi <= 55. < rsi if is_long else previous_rsi >= 45. > rsi)
            if crossed:
                extreme = float(window['high' if is_long else 'low'][q])
                if not np.isfinite(extreme) or extreme <= 0:
                    raise ValueError('Invalid completed RSI-cross candle extreme')
                pending = extreme
                if policy == 'rsi_reset_recovery_keep_trigger':
                    pending = max(pending, original_trigger) if is_long else min(pending, original_trigger)
                result.update(cross_index=q, cross_ts=_clock(start_ns+q*MINUTE_NS),
                              cross_rsi=rsi, trigger_used=pending, status='RECOVERY_WITHOUT_BREAK')
        previous_rsi = rsi
        return True

    if not observe(0):
        return result
    for j in range(min(EXPIRY_MINUTES, count)):
        if int(path['timestamp_ns'][j]) != start_ns+(j+1)*MINUTE_NS:
            raise ValueError('Execution entry window must have exact consecutive minutes')
        if pending is not None:
            op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
            if (not np.isfinite([op, hi, lo, close]).all() or min(op, hi, lo, close) <= 0
                    or hi < max(op, lo, close) or lo > min(op, hi, close)):
                raise ValueError('Invalid entry-window execution prices')
            touched = hi >= pending if is_long else lo <= pending
            if touched:
                gap = op > pending if is_long else op < pending
                result.update(filled=True, status='FILLED', entry_path_index=j,
                    entry_price=op if gap else pending, entry_ts=_clock(path['timestamp_ns'][j]),
                    entry_gap_through=gap, entry_delay_minutes=j+1)
                return result
        # The +10 close cannot produce a trigger that can fill before expiry.
        if j+1 < EXPIRY_MINUTES and not observe(j+1):
            return result
    if count < EXPIRY_MINUTES:
        result['status'] = 'INCOMPLETE_EXECUTION_WINDOW'
    return result


def simulate(orders, paths, windows, policy='rsi_reset_recovery'):
    """Replay all admitted identities; return filled trades and every timing decision."""
    if policy not in POLICIES:
        raise ValueError('Unknown RSI timing policy')
    if orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError('Timing order identities must be unique')
    trades, audits = [], []
    for order in orders.itertuples(index=False):
        sid = int(order.sid)
        if sid not in paths:
            raise ValueError('Missing execution path for admitted order: '+str(sid))
        path = paths[sid]
        decision = find_entry(order, path, windows.get(sid), policy)
        identity = {key: getattr(order, key) for key in IDENTITY}
        audits.append(dict(identity, **{key: value for key, value in decision.items() if key != 'sid'}))
        if not decision['filled']:
            continue
        if (_clock(path['timestamp_ns'][-1])[11:16] != '15:15'):
            raise ValueError('Execution path lacks the exact 15:15 square-off bar')
        entry = float(decision['entry_price'])
        target = float(order.native_target_pct)
        # This frozen cushion policy does not consume ATR. Its shared API
        # still requires a positive number, exactly as in the V24 baseline.
        outcome = exits.replay(path, int(decision['entry_path_index']), entry,
            order.side == 'LONG', target, SETTINGS, 1.)
        pnl = (1 if order.side == 'LONG' else -1)*(outcome['price']/entry-1)*EXPOSURE_RS-COST_RS
        if abs(pnl) < 1e-7:
            pnl = 0.
        trades.append(dict(identity, policy=policy, entry_ts=decision['entry_ts'],
            entry_price=entry, entry_path_index=decision['entry_path_index'],
            entry_delay_minutes=decision['entry_delay_minutes'],
            entry_gap_through=decision['entry_gap_through'], trigger_used=decision['trigger_used'],
            reset_ts=decision['reset_ts'], cross_ts=decision['cross_ts'],
            exit_ts=str(exit_timestamp(path, outcome)), exit_price=outcome['price'],
            exit_reason=outcome['reason'], portfolio_net_profit_rupees=pnl,
            native_target_pct=target, initial_stop_pct=1.5,
            stop_update_count=outcome['stop_update_count'], same_bar_ambiguous=outcome['ambiguous'],
            exposure_rs=EXPOSURE_RS, cost_rs=COST_RS))
    return (pd.DataFrame(trades) if trades else pd.DataFrame(columns=TRADE_COLUMNS),
            pd.DataFrame(audits))
