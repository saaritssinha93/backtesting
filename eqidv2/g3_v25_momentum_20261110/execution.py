"""Causal next-open entries with the frozen V23 target-relative stop policy.

New signals trade at the next minute's open, subject to a known-at-open chase
limit. All source timestamps are completed-bar labels. The public entry_ts
retains that baseline convention; entry_execution_ts records the actual open.
Source defects after entry are replay errors, never retrospective entry vetoes.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.target_trailing_20261106 import engine as exits
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study.run import exit_timestamp

IST = 'Asia/Kolkata'
MINUTE_NS = 60_000_000_000
EXPOSURE_RS = 500_000.
COST_RS = 250.
TARGET_PCT = 1.50
MAX_ADVERSE_GAP_PCT = .15
SETTINGS = exits.CANDIDATES['breakeven_eighty_target']
IDENTITY = ('sid', 'day', 'setup_id', 'tradingsymbol', 'side')
FLAGS = ('gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic')
PRICE_KEYS = ('open', 'high', 'low', 'close')
TRADE_COLUMNS = [*IDENTITY, 'entry_ts', 'entry_execution_ts', 'entry_bar_end_ts',
    'exit_ts', 'exit_bar_end_ts', 'entry_price', 'exit_price', 'exit_reason',
    'portfolio_net_profit_rupees', 'native_target_pct', 'initial_stop_pct',
    'stop_update_count', 'same_bar_ambiguous', 'exposure_rs', 'cost_rs',
    'slippage_bps_per_side']


def configuration():
    return dict(version='G3-V25-NEXT-OPEN-EXECUTION-V1', scope='BACKTEST_ONLY',
        execution_authority=False, target_pct=TARGET_PCT, initial_stop_pct=1.5,
        exit_policy='breakeven_eighty_target', exit_settings=SETTINGS.copy(),
        entry='first minute open strictly after completed confirmation',
        entry_ts='end-label of entry minute, matching baseline convention',
        entry_execution_ts='entry minute end-label minus one minute',
        max_adverse_gap_pct=MAX_ADVERSE_GAP_PCT,
        gap_basis='raw next open versus confirmation close, checked at that open',
        rejected_gap='permanent cancellation; no retry',
        protection='completed close strictly after entry bar; next-bar effective',
        same_bar_ambiguity='stop first', target_open_gap='target price, no windfall',
        square_off='15:15 completed minute close', exposure_rs=EXPOSURE_RS,
        round_trip_cost_rs=COST_RS, unlimited_concurrent_capital=True,
        slippage='adverse multiplicative entry and exit adjustment, basis points per side',
        replay_data_failure='raise rather than retrospectively remove entered trades')


def _get(row, key):
    return row[key] if isinstance(row, Mapping) else getattr(row, key)


def _stamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError('Execution timestamps must be timezone aware')
    stamp = stamp.tz_convert(IST)
    if stamp != stamp.floor('min'):
        raise ValueError('Execution timestamps must have exact minute precision')
    return stamp


def _clock(ns):
    return str(pd.Timestamp(int(ns), tz='UTC').tz_convert(IST))


def _slippage(value):
    value = float(value)
    if not np.isfinite(value) or not 0 <= value <= 100:
        raise ValueError('Slippage must be finite and between zero and 100 bps per side')
    return value / 10_000


def _identity(row):
    result = {key: _get(row, key) for key in IDENTITY}
    result['sid'] = int(result['sid'])
    result['day'] = str(result['day'])[:10]
    if result['side'] not in ('LONG', 'SHORT'):
        raise ValueError('Invalid execution side')
    return result


def find_entry(order, next_bar, *, slippage_bps=0.):
    """Decide using confirmation and the exact next bar's open only.

next_bar is None or a mapping containing ts/open. High, low, close and future
observations cannot affect this decision. Missing next-minute data is recorded
as unfillable evidence; it is never replaced with a later observed open.
"""
    identity = _identity(order)
    stamp = _stamp(_get(order, 'confirmation_ts'))
    if str(stamp.date()) != identity['day']:
        raise ValueError('Order day differs from confirmation day')
    close = float(_get(order, 'confirmation_close'))
    if not np.isfinite(close) or close <= 0:
        raise ValueError('Confirmation close must be finite and positive')
    if stamp >= pd.Timestamp(identity['day']+' 15:15', tz=IST):
        raise ValueError('Confirmation leaves no time before square-off')
    slip = _slippage(slippage_bps)
    sign = 1 if identity['side'] == 'LONG' else -1
    result = dict(identity, confirmation_ts=str(stamp), filled=False,
        status='MISSING_NEXT_MINUTE', entry_ts=None, entry_execution_ts=None,
        raw_entry_open=None, entry_price=None, adverse_gap_pct=None,
        slippage_bps_per_side=float(slippage_bps))
    if next_bar is None:
        return result
    next_stamp = _stamp(next_bar['ts'])
    if next_stamp != stamp + pd.Timedelta(minutes=1):
        return result
    opening = float(next_bar['open'])
    if not np.isfinite(opening) or opening <= 0:
        raise ValueError('Next-minute open must be finite and positive')
    gap = sign * (opening / close - 1) * 100
    result.update(raw_entry_open=opening, adverse_gap_pct=gap)
    if gap > MAX_ADVERSE_GAP_PCT + 1e-10:
        result['status'] = 'CANCELLED_ADVERSE_OPEN_GAP'
        return result
    result.update(filled=True, status='FILLED', entry_ts=str(next_stamp),
        entry_execution_ts=str(next_stamp-pd.Timedelta(minutes=1)),
        entry_price=opening*(1+sign*slip))
    return result


def _minute_groups(minutes, symbols):
    required = {'tradingsymbol', 'ts', *PRICE_KEYS}
    if not required.issubset(minutes):
        raise ValueError('Execution minutes lack required fields')
    # Only executed/admitted symbols need replay arrays. Selecting this subset
    # uses order identities, never their subsequent prices or outcomes.
    work = minutes.loc[minutes.tradingsymbol.isin(symbols)].copy()
    work['ts'] = pd.to_datetime(work.ts, utc=True).dt.tz_convert(IST).dt.as_unit('ns')
    if work.ts.isna().any() or work.ts.ne(work.ts.dt.floor('min')).any():
        raise ValueError('Invalid execution-minute chronology')
    work['_day'] = work.ts.dt.strftime('%Y-%m-%d')
    if work.duplicated(['tradingsymbol', 'ts']).any():
        raise ValueError('Duplicate execution minute')
    return {(day, symbol): part.sort_values('ts', kind='stable').reset_index(drop=True)
            for (day, symbol), part in work.groupby(['_day', 'tradingsymbol'], sort=False)}


def _path(part, entry_bar, day):
    """Validate required entered-trade evidence, without returning a veto."""
    end = pd.Timestamp(day+' 15:15', tz=IST)
    used = part.loc[part.ts.between(entry_bar, end)].copy()
    expected = pd.date_range(entry_bar, end, freq='min').asi8
    if not np.array_equal(used.ts.astype('int64').to_numpy(), expected):
        raise ValueError('Entered trade lacks complete consecutive execution path through 15:15')
    for flag in FLAGS:
        if flag in used and used[flag].astype(str).str.lower().isin(['true', '1', '1.0', 'yes']).any():
            raise ValueError('Entered trade has flagged execution evidence: '+flag)
    values = used.loc[:, list(PRICE_KEYS)].apply(pd.to_numeric, errors='coerce').to_numpy(float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError('Entered trade has invalid execution prices')
    op, hi, lo, close = values.T
    if (hi < np.maximum.reduce([op, lo, close])).any() or (lo > np.minimum.reduce([op, hi, close])).any():
        raise ValueError('Entered trade has inconsistent OHLC evidence')
    return dict(timestamp_ns=used.ts.astype('int64').to_numpy(),
                **dict(zip(PRICE_KEYS, values.T)))


def _trade(identity, path, entry, target_pct, slippage_bps, *, entry_execution_ts=None,
           entry_origin='NEXT_OPEN', raw_entry_open=None):
    slip = _slippage(slippage_bps)
    sign = 1 if identity['side'] == 'LONG' else -1
    outcome = exits.replay(path, 0, entry, sign == 1, target_pct, SETTINGS, 1.)
    raw_exit = float(outcome['price'])
    exit_price = raw_exit*(1-sign*slip)
    pnl = sign*(exit_price/entry-1)*EXPOSURE_RS-COST_RS
    if abs(pnl) < 1e-7:
        pnl = 0.
    return dict(identity, entry_ts=_clock(path['timestamp_ns'][0]),
        entry_bar_end_ts=_clock(path['timestamp_ns'][0]),
        entry_execution_ts=entry_execution_ts,
        entry_price=float(entry), raw_entry_open=raw_entry_open, entry_origin=entry_origin,
        exit_ts=str(exit_timestamp(path, outcome)),
        exit_bar_end_ts=_clock(path['timestamp_ns'][outcome['index']]),
        exit_price=float(exit_price), raw_exit_price=raw_exit, exit_reason=outcome['reason'],
        exit_event=outcome['event'], portfolio_net_profit_rupees=float(pnl),
        native_target_pct=float(target_pct), initial_stop_pct=1.5,
        stop_update_count=outcome['stop_update_count'],
        same_bar_ambiguous=outcome['ambiguous'], exposure_rs=EXPOSURE_RS, cost_rs=COST_RS,
        slippage_bps_per_side=float(slippage_bps), execution_authority=False)


def _validate_orders(orders):
    if not set(IDENTITY).issubset(orders):
        raise ValueError('Orders lack identity columns')
    if orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError('Order SID must be present and unique')


def simulate(orders, minutes, *, target_pct=TARGET_PCT, slippage_bps=0.):
    """Return filled trades and every admitted order's entry decision."""
    if not np.isfinite(target_pct) or target_pct <= 0:
        raise ValueError('Target must be finite and positive')
    _slippage(slippage_bps)
    _validate_orders(orders)
    if orders.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS), pd.DataFrame(columns=[*IDENTITY, 'filled', 'status'])
    groups = _minute_groups(minutes, set(orders.tradingsymbol))
    trades, decisions = [], []
    for order in orders.itertuples(index=False):
        identity = _identity(order)
        stamp = _stamp(order.confirmation_ts)
        part = groups.get((identity['day'], identity['tradingsymbol']))
        next_bar = None
        if part is not None:
            following = part.loc[part.ts.gt(stamp)]
            if not following.empty:
                next_bar = following.iloc[0]
        decision = find_entry(order, next_bar, slippage_bps=slippage_bps)
        decisions.append(decision)
        if not decision['filled']:
            continue
        path = _path(part, _stamp(decision['entry_ts']), identity['day'])
        trade = _trade(identity, path, float(decision['entry_price']), target_pct, slippage_bps,
            entry_execution_ts=decision['entry_execution_ts'], raw_entry_open=decision['raw_entry_open'])
        trade['confirmation_ts'] = str(stamp)
        trade['confirmation_close'] = float(order.confirmation_close)
        trade['adverse_gap_pct'] = decision['adverse_gap_pct']
        trades.append(trade)
    return (pd.DataFrame(trades) if trades else pd.DataFrame(columns=TRADE_COLUMNS), pd.DataFrame(decisions))


def counterfactual_exit_replay(baseline_trades, minutes, *, target_pct=TARGET_PCT, slippage_bps=0.):
    """Match the new exit target while keeping published baseline entry fills.

Baseline entry_ts is an end-label, and its exact within-bar fill time is not
known. Reusing all entry-bar extrema follows the original conservative replay.
No new gap veto applies to these already-filled baseline identities.
"""
    if not np.isfinite(target_pct) or target_pct <= 0:
        raise ValueError('Target must be finite and positive')
    slip = _slippage(slippage_bps)
    _validate_orders(baseline_trades)
    if baseline_trades.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS)
    groups = _minute_groups(minutes, set(baseline_trades.tradingsymbol))
    result = []
    for row in baseline_trades.itertuples(index=False):
        identity = _identity(row)
        entry_bar = _stamp(row.entry_ts)
        if str(entry_bar.date()) != identity['day']:
            raise ValueError('Baseline entry date mismatch')
        part = groups.get((identity['day'], identity['tradingsymbol']))
        if part is None:
            raise ValueError('Baseline filled stock has no execution evidence')
        path = _path(part, entry_bar, identity['day'])
        raw_entry = float(row.entry_price)
        if not np.isfinite(raw_entry) or raw_entry <= 0:
            raise ValueError('Invalid original baseline entry price')
        sign = 1 if identity['side'] == 'LONG' else -1
        trade = _trade(identity, path, raw_entry*(1+sign*slip), target_pct, slippage_bps,
            entry_origin='PRESERVED_BASELINE_FILL')
        trade['original_entry_price'] = raw_entry
        trade['original_native_target_pct'] = getattr(row, 'native_target_pct', np.nan)
        result.append(trade)
    return pd.DataFrame(result)
