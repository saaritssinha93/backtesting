"""Efficient, causal Wilder RSI windows from the existing sealed G3 sources.

Only completed bars are exposed. Each order receives the confirmation bar
and ten subsequent minute closes; an engine must inspect those sequentially.
Future missing evidence never changes an earlier bar's readiness.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import features as ranking_features
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import stock_features

VERSION = 'G3_V24_RSI_TIMING_CONTINUOUS_WILDER14_V1'
IST = 'Asia/Kolkata'
MINUTE_NS = 60_000_000_000
PERIOD = 14
OHLCV = ('open', 'high', 'low', 'close', 'volume')
FLAGS = ('gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic')


def rsi_wilder(close, period=PERIOD):
    """Arithmetic mean of the first period deltas, then Wilder recurrence.

The first valid RSI requires period+1 prices. A constant sequence is 50;
gain-only/loss-only sequences are 100/0. Invalid price prefixes stay NaN.
"""
    if not isinstance(period, int) or period < 1:
        raise ValueError('RSI period must be a positive integer')
    values = np.asarray(close, dtype=float)
    if values.ndim != 1:
        raise ValueError('RSI prices must be one-dimensional')
    out = np.full(len(values), np.nan)
    if len(values) <= period:
        return out
    good = np.isfinite(values) & (values > 0)
    # A finite placeholder permits vectorized computation; prefix masking
    # prevents this placeholder from ever becoming observable evidence.
    safe = np.where(good, values, 1.)
    delta = np.diff(safe)
    gain, loss = np.maximum(delta, 0.), np.maximum(-delta, 0.)
    avg_gain = pd.Series(np.r_[gain[:period].mean(), gain[period:]]).ewm(
        alpha=1./period, adjust=False).mean().to_numpy()
    avg_loss = pd.Series(np.r_[loss[:period].mean(), loss[period:]]).ewm(
        alpha=1./period, adjust=False).mean().to_numpy()
    total = avg_gain + avg_loss
    result = np.divide(100.*avg_gain, total, out=np.full(len(total), 50.), where=total > 0)
    out[period:] = result
    out[~np.logical_and.accumulate(good)] = np.nan
    return out


def _stamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError('Confirmation time must be timezone aware')
    return stamp.tz_convert(IST)


def _empty_window(row, expiry_minutes, status):
    t0 = _stamp(row['confirmation_ts'])
    if str(t0.date()) != str(row['day'])[:10] or row['side'] not in ('LONG', 'SHORT'):
        raise ValueError('Invalid order identity')
    if not isinstance(expiry_minutes, int) or expiry_minutes < 1:
        raise ValueError('Entry expiry must be a positive integer')
    times = t0.value + np.arange(expiry_minutes+1, dtype=np.int64)*MINUTE_NS
    value = dict(sid=int(row['sid']), confirmation_ts=t0.isoformat(),
        status=status, available=False, feature_version=VERSION, timestamp_ns=times,
        valid=np.zeros(len(times), dtype=bool),
        bar_status=np.full(len(times), status, dtype=object))
    value.update({key:np.full(len(times), np.nan) for key in (*OHLCV, 'rsi14', 'prior_close')})
    return value


def _prepared_values(frame):
    """Forward arrays once per source; avoid expensive date-string grouping."""
    column = next((key for key in ('ts', 'date', 'timestamp') if key in frame), None)
    if column is None or not set(OHLCV).issubset(frame):
        raise ValueError('Minute history requires timestamps and OHLCV')
    times = pd.DatetimeIndex(pd.to_datetime(frame[column], utc=True)).tz_convert(IST).as_unit('ns')
    order = np.argsort(times.asi8, kind='stable')
    times = times[order]
    if times.hasnans or times.has_duplicates:
        raise ValueError('Missing or duplicate minute chronology')
    numeric = frame.loc[:, list(OHLCV)].apply(pd.to_numeric, errors='coerce').to_numpy(float)[order]
    op, hi, lo, close, vol = numeric.T
    invalid = (~np.isfinite(numeric).all(axis=1) | (numeric[:, :4] <= 0).any(axis=1)
        | (vol < 0) | (hi < np.maximum.reduce([op, lo, close]))
        | (lo > np.minimum.reduce([op, hi, close])))
    flagged = np.zeros(len(frame), dtype=bool)
    for name in FLAGS:
        if name in frame:
            flag = frame[name]
            if flag.dtype == object or isinstance(flag.dtype, pd.StringDtype):
                truthy = [item for item in flag.dropna().unique()
                          if str(item).strip().lower() in ('true', '1', '1.0', 'yes')]
                flag = flag.isin(truthy)
            else:
                flag = flag.fillna(False).astype(bool)
            flagged |= flag.to_numpy(bool)[order]
    nanos = times.asi8
    # Overnight/weekend gaps are intentional for continuous RSI. In-session
    # missing minutes invalidate the affected recent window without imputing.
    local_day = times.tz_localize(None).asi8 // (24*60*MINUTE_NS)
    same_day = np.r_[False, local_day[1:] == local_day[:-1]]
    gap = np.r_[False, np.diff(nanos) != MINUTE_NS] & same_day
    recent_bad = pd.Series(invalid | flagged | gap).rolling(PERIOD+1, min_periods=1).max().to_numpy(bool)
    rsi = rsi_wilder(close)
    valid = np.isfinite(rsi) & ~recent_bad
    status = np.where(np.isfinite(rsi), 'READY', 'INSUFFICIENT_OR_INVALID_RSI_WARMUP').astype(object)
    status[recent_bad] = 'INVALID_OR_FLAGGED_RECENT_INPUT'
    previous = np.r_[np.nan, close[:-1]]
    previous[gap] = np.nan
    return dict(timestamp_ns=nanos, **dict(zip(OHLCV, numeric.T)), rsi14=rsi,
                prior_close=previous, valid=valid, bar_status=status)


def _window(prepared, row, expiry_minutes=10):
    result = _empty_window(row, expiry_minutes, 'MISSING_MINUTE')
    times = prepared['timestamp_ns']
    at = np.searchsorted(times, result['timestamp_ns'])
    observed = at < len(times)
    observed[observed] &= times[at[observed]] == result['timestamp_ns'][observed]
    if observed[0]:
        for key in OHLCV:
            expected = float(row['confirmation_'+key])
            if not np.isfinite(expected) or not np.isclose(prepared[key][at[0]], expected, rtol=0, atol=.001):
                raise ValueError(f'RSI source/confirmation {key} mismatch: {row["sid"]}')
    for key in (*OHLCV, 'rsi14', 'prior_close', 'valid', 'bar_status'):
        result[key][observed] = prepared[key][at[observed]]
    # If a literal clock row is missing, subsequent decision bars within this
    # ten-minute window have broken recent chronology too.
    missing_before = np.maximum.accumulate(~observed)
    result['valid'][missing_before] = False
    result['bar_status'][observed & missing_before] = 'INCOMPLETE_DECISION_CHRONOLOGY'
    result['available'] = bool(result['valid'][0])
    result['status'] = str(result['bar_status'][0])
    return result


def window_from_frame(history, row, expiry_minutes=10):
    """Pure API: values after this order's expiry cannot affect its window."""
    column = next((key for key in ('ts', 'date', 'timestamp') if key in history), None)
    if column is None:
        raise ValueError('Minute history requires timestamps')
    cutoff = _stamp(row['confirmation_ts']) + pd.Timedelta(minutes=expiry_minutes)
    times = pd.to_datetime(history[column], utc=True)
    prefix = history.loc[times.le(cutoff.tz_convert('UTC'))]
    if prefix.empty:
        return _empty_window(row, expiry_minutes, 'MISSING_MINUTE')
    return _window(_prepared_values(prefix), row, expiry_minutes)


def build_rsi_windows(orders, expiry_minutes=10):
    """Return SID windows, inventory-preserving audit, and sealed proofs.

`available` only describes confirmation-time readiness. Engines must inspect
`valid` sequentially and may not prefilter on all future window bars.
"""
    if orders.empty:
        return {}, pd.DataFrame(columns=['sid', 'available', 'status']), []
    grouped, source_proofs, missing_rows, missing_proofs = ranking_features._inventory_with_missing(orders)
    by_sid = orders.set_index('sid').to_dict('index')
    windows, proofs, audit = {}, [], []
    for missing in missing_rows:
        sid = int(missing['sid'])
        windows[sid] = _empty_window(dict(by_sid[sid], sid=sid), expiry_minutes, 'MISSING_SEALED_SOURCE')
    for proof in missing_proofs:
        proofs.append(dict(proof, rsi_feature_version=VERSION, policy='No invented RSI evidence; report unavailable source'))
    for path, rows in grouped.items():
        # Reuse the established Oct28 warmup stitching. The shifted timestamps
        # control loading only, never replace actual order/confirmation times.
        load_rows = [dict(row, confirmation_ts=_stamp(row['confirmation_ts']) + pd.Timedelta(minutes=expiry_minutes)) for row in rows]
        frame, proof = stock_features._prepared_frame(path, load_rows, source_proofs[path])
        prepared = _prepared_values(frame)
        for row in rows:
            sid = int(row['sid'])
            windows[sid] = _window(prepared, row, expiry_minutes)
            windows[sid]['stock_source_sha256'] = proof['stock_sha256']
            windows[sid]['stock_source_path'] = str(path)
        proofs.append(dict(proof, rsi_feature_version=VERSION, order_rows=len(rows),
            order_days=sorted({str(row['day'])[:10] for row in rows}), source_rows=len(frame),
            loading_cutoff=str(max(_stamp(row['confirmation_ts']) for row in load_rows)),
            definition='Continuous Wilder RSI14; arithmetic first14-delta seed; each value available at its own completed minute'))
    if set(windows) != set(orders.sid.astype(int)) or len(windows) != len(orders):
        raise ValueError('RSI window builder changed order inventory')
    for row in orders.to_dict('records'):
        value = windows[int(row['sid'])]
        audit.append(dict(sid=int(row['sid']), day=str(row['day'])[:10], setup_id=row['setup_id'],
            tradingsymbol=row['tradingsymbol'], side=row['side'], confirmation_ts=value['confirmation_ts'],
            available=value['available'], status=value['status'], valid_bars=int(value['valid'].sum()),
            window_bars=len(value['timestamp_ns']), rsi_at_confirmation=float(value['rsi14'][0]),
            prior_close_at_confirmation=float(value['prior_close'][0]),
            stock_source_sha256=value.get('stock_source_sha256'), feature_version=VERSION))
    return windows, pd.DataFrame(audit).sort_values('sid').reset_index(drop=True), proofs
