"""H's shared cash-equity execution sensitivity model; no broker connectivity.

Minute bars cannot establish intrabar ordering, queue priority or market impact.
Ticks and flat round-trip fees are declared assumptions, not broker calibration.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ExecutionModel:
    cost_bps: float = 5.0
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    delay_minutes: int = 0
    entry_expiry_minutes: int = 10
    tick_size: float = 0.05
    portfolio_capital: float = 1_000_000.0
    entry_capital: float = 100_000.0
    leverage: float = 5.0

    def validate(self):
        for name in ('cost_bps', 'entry_slippage_bps', 'exit_slippage_bps'):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0 <= value < 1000:
                raise ValueError(f'Invalid {name}')
        for name in ('tick_size', 'portfolio_capital', 'entry_capital', 'leverage'):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f'Invalid {name}')
        if type(self.delay_minutes) is not int or self.delay_minutes < 0:
            raise ValueError('delay_minutes must be a nonnegative integer')
        if self.entry_expiry_minutes != 10:
            raise ValueError('H retains the absolute confirmation-plus-10-minute expiry')


def tick_round(price: float, tick: float, up: bool) -> float:
    if not math.isfinite(price) or price <= 0 or not math.isfinite(tick) or tick <= 0:
        raise ValueError('Invalid price/tick')
    step = Decimal(str(tick))
    units = Decimal(str(price)) / step
    nearest = units.to_integral_value()
    # Arithmetic round-off in a computed bracket must not add a whole tick.
    if abs(units-nearest) < Decimal('0.000000001'):
        units = nearest
    return float(units.to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR) * step)


def _utc(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None or pd.isna(stamp):
        raise ValueError('Execution timestamps must be valid and timezone-aware')
    return stamp.tz_convert('UTC')


def observed_prices(values, tick: float):
    """Remove float32 storage noise at a tick, not genuine off-grid prices.

Historical OHLC was stored as float32 (e.g. 118.90 -> 118.9000015).
Blind directional rounding would invent an additional tick and false non-fills.
Only snap within half a float32 ULP, capped well below half a tick.
"""
    prices = np.asarray(values, dtype=float)
    nearest = np.round(prices / tick) * tick
    tolerance = np.minimum(tick / 10, np.maximum(1e-9, np.abs(np.spacing(prices.astype(np.float32))) * .51))
    return np.where(np.abs(prices-nearest) <= tolerance, np.round(nearest, 10), prices)


def simulate_order(row: dict, path: dict, model: ExecutionModel, risk_rupees: float | None) -> dict:
    """One whole-share bracket. Risk sizing never reads future prices/outcomes."""
    model.validate()
    if risk_rupees is not None and (not math.isfinite(risk_rupees) or risk_rupees <= 0):
        raise ValueError('Risk budget must be positive and finite')
    side = row['side']
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Invalid side')
    sign = 1 if side == 'LONG' else -1
    confirmation = _utc(row['confirmation_ts'])
    stamp = pd.DatetimeIndex(pd.to_datetime(path['timestamp_ns'], utc=True))
    if not len(stamp) or stamp.has_duplicates or not stamp.is_monotonic_increasing:
        raise ValueError('Invalid path clock')
    if stamp[0] != confirmation + pd.Timedelta(minutes=1) or (len(stamp) > 1 and
            not np.all(np.diff(stamp.asi8) == pd.Timedelta(minutes=1).value)):
        raise ValueError('Missing or mistimed forward minute')
    arrays = {key: np.asarray(path[key], dtype=float) for key in ('open', 'high', 'low', 'close')}
    if any(len(x) != len(stamp) or not np.isfinite(x).all() or (x <= 0).any()
           for x in arrays.values()):
        raise ValueError('Invalid path prices')
    if ((arrays['high'] < np.maximum(arrays['open'], arrays['close'])) |
        (arrays['low'] > np.minimum(arrays['open'], arrays['close']))).any():
        raise ValueError('Inconsistent OHLC')
    # Normalize observed prices before directional rounding or touch comparisons.
    arrays = {key: observed_prices(value, model.tick_size) for key, value in arrays.items()}
    raw_trigger = float(observed_prices([row['trigger']], model.tick_size)[0])
    trigger = tick_round(raw_trigger, model.tick_size, sign == 1)
    stop_pct, target_pct = float(row['native_stop_pct']), float(row['native_target_pct'])
    if not all(math.isfinite(v) and 0 < v < 100 for v in (stop_pct, target_pct)):
        raise ValueError('Invalid bracket')
    deadline = confirmation + pd.Timedelta(minutes=model.entry_expiry_minutes)
    activation = confirmation + pd.Timedelta(minutes=model.delay_minutes)
    within = (stamp > activation) & (stamp <= deadline)
    touched = arrays['high'] >= trigger if sign == 1 else arrays['low'] <= trigger
    hits = np.flatnonzero(within & touched)
    result = {key: row[key] for key in ('sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'confirmation_ts')}
    result.update(status='UNFILLED_TRIGGER', quantity=0, net_profit_rupees=0.0,
                  gross_profit_rupees=0.0, cost_rupees=0.0, capital_rupees=0.0,
                  exposure_rupees=0.0, planned_risk_rupees=0.0, same_bar_ambiguous=False,
                  entry_ts=pd.NaT, exit_ts=pd.NaT, entry_price=np.nan, exit_price=np.nan,
                  entry_index=-1, exit_index=-1, deadline=deadline, trigger=trigger)
    if not hits.size:
        return result
    start = int(hits[0])
    entry_rule = row.get('research_entry_rule', 'breakout')
    if entry_rule == 'retest':
        # A completed candle must retest the broken level and reject it. The
        # order enters at the following minute's open, never that candle's close.
        retest = next((i for i in range(start + 1, len(stamp) - 1)
                       if stamp[i + 1] <= deadline and
                       ((arrays['low'][i] <= trigger and arrays['close'][i] > trigger)
                        if sign == 1 else
                        (arrays['high'][i] >= trigger and arrays['close'][i] < trigger))), None)
        if retest is None:
            result['status'] = 'UNFILLED_RETEST'
            return result
        start = retest + 1
        raw_entry = arrays['open'][start]
    elif entry_rule == 'breakout':
        raw_entry = max(trigger, arrays['open'][start]) if sign == 1 else min(trigger, arrays['open'][start])
    else:
        raise ValueError('Unknown research entry rule')
    entry = tick_round(raw_entry * (1 + sign * model.entry_slippage_bps / 10000), model.tick_size, sign == 1)
    stop_distance = row.get('research_stop_distance')
    stop_reference = row.get('research_stop_reference')
    if stop_distance is not None and pd.isna(stop_distance):
        stop_distance = None
    if stop_reference is not None and pd.isna(stop_reference):
        stop_reference = None
    if stop_distance is not None and stop_reference is not None:
        raise ValueError('Only one research stop definition is allowed')
    if stop_distance is not None:
        if not math.isfinite(float(stop_distance)) or float(stop_distance) <= 0:
            raise ValueError('Invalid research stop distance')
        raw_stop = entry - sign * float(stop_distance)
    elif stop_reference is not None:
        if not math.isfinite(float(stop_reference)) or float(stop_reference) <= 0:
            raise ValueError('Invalid research stop reference')
        raw_stop = float(stop_reference)
    else:
        raw_stop = entry * (1 - sign * stop_pct / 100)
    if sign * (entry - raw_stop) <= 0:
        result['status'] = 'INVALID_RESEARCH_STOP'
        return result
    stop = tick_round(raw_stop, model.tick_size, sign == -1)
    target = tick_round(entry * (1 + sign * target_pct / 100), model.tick_size, sign == 1)
    stop_fill = tick_round(stop * (1 - sign * model.exit_slippage_bps / 10000), model.tick_size, sign == -1)
    risk_per_share = abs(entry - stop_fill) + entry * model.cost_bps / 10000
    quantity = math.floor(model.entry_capital * model.leverage / entry)
    if risk_rupees is not None:
        quantity = min(quantity, math.floor(risk_rupees / risk_per_share))
    if quantity <= 0:
        result['status'] = 'SIZE_BELOW_ONE_SHARE'
        return result
    exit_i, raw_exit, reason, ambiguous = len(stamp) - 1, arrays['close'][-1], 'TIME_EXIT_1515', False
    invalidation = row.get('research_invalidation_reference')
    pending_invalidation = False
    for i in range(start, len(stamp)):
        if pending_invalidation:
            # Decision used the preceding completed minute; execute at this open.
            raw_exit = arrays['open'][i]
            if (sign == 1 and raw_exit <= stop) or (sign == -1 and raw_exit >= stop):
                reason = 'STOP'
                raw_exit = min(stop, raw_exit) if sign == 1 else max(stop, raw_exit)
            else:
                reason = 'FAILED_BREAKDOWN_EXIT'
            exit_i = i
            break
        stop_hit = arrays['low'][i] <= stop if sign == 1 else arrays['high'][i] >= stop
        target_hit = arrays['high'][i] >= target if sign == 1 else arrays['low'][i] <= target
        if stop_hit:
            raw_exit = stop
            if i > start:
                raw_exit = min(stop, arrays['open'][i]) if sign == 1 else max(stop, arrays['open'][i])
            exit_i, reason, ambiguous = i, 'STOP', bool(target_hit)
            break
        if target_hit:
            exit_i, raw_exit, reason = i, target, 'TARGET'
            break
        if invalidation is not None and i + 1 < len(stamp):
            if not math.isfinite(float(invalidation)) or float(invalidation) <= 0:
                raise ValueError('Invalid invalidation reference')
            pending_invalidation = (arrays['close'][i] <= float(invalidation)
                                    if sign == 1 else arrays['close'][i] >= float(invalidation))
    exit_price = tick_round(raw_exit * (1 - sign * model.exit_slippage_bps / 10000), model.tick_size, sign == -1)
    gross = quantity * sign * (exit_price - entry)
    cost = quantity * entry * model.cost_bps / 10000
    result.update(status='FILLED_PENDING_CAPITAL', quantity=quantity,
                  entry_ts=stamp[start], exit_ts=stamp[exit_i], entry_price=entry, exit_price=exit_price,
                  entry_index=start, exit_index=exit_i, stop_price=stop, target_price=target,
                  exit_reason=reason, same_bar_ambiguous=ambiguous,
                  gross_profit_rupees=gross, cost_rupees=cost, net_profit_rupees=gross-cost,
                  exposure_rupees=quantity*entry, capital_rupees=quantity*entry/model.leverage,
                  planned_risk_rupees=quantity*risk_per_share)
    return result


def simulate_portfolio(orders: pd.DataFrame, paths: dict, model: ExecutionModel,
                       risk_rupees: float | None = None) -> pd.DataFrame:
    """No compounding; same-minute capital release is conservatively disallowed."""
    model.validate()
    if orders.duplicated(['sid', 'setup_id']).any():
        raise ValueError('Duplicate order identities')
    records = [simulate_order(row, paths[int(row['sid'])], model, risk_rupees)
               for row in orders.to_dict('records')]
    ledger = pd.DataFrame(records)
    if ledger.empty:
        return pd.DataFrame(columns=['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'status',
                                     'net_profit_rupees', 'gross_profit_rupees', 'cost_rupees', 'executed'])
    active = []
    pending = ledger.loc[ledger.status.eq('FILLED_PENDING_CAPITAL')].sort_values(
        ['entry_ts', 'confirmation_ts', 'setup_id', 'tradingsymbol', 'sid'], kind='stable')
    for index, row in pending.iterrows():
        active = [p for p in active if p['exit_ts'] >= row.entry_ts]
        reserved = sum(p['capital_rupees'] for p in active)
        ledger.loc[index, 'reserved_before_rupees'] = reserved
        if reserved + row.capital_rupees > model.portfolio_capital + 1e-8:
            ledger.loc[index, 'status'] = 'CAPITAL_REJECTED'
            ledger.loc[index, ['net_profit_rupees', 'gross_profit_rupees', 'cost_rupees']] = 0.0
        else:
            ledger.loc[index, 'status'] = 'EXECUTED'
            active.append(row.to_dict())
    ledger['executed'] = ledger.status.eq('EXECUTED')
    return ledger


def minute_equity(ledger: pd.DataFrame, paths: dict) -> pd.DataFrame:
    """Close-marked equity with half the flat cost at entry and half at exit.

This is minute-close MTM, not the unobservable worst intraminute drawdown.
"""
    accepted = ledger.loc[ledger.status.eq('EXECUTED')]
    if accepted.empty:
        return pd.DataFrame(columns=['timestamp', 'equity_pnl_rupees', 'drawdown_rupees', 'exposure_rupees'])
    clocks = sorted({int(ns) for row in accepted.itertuples()
                     for ns in paths[int(row.sid)]['timestamp_ns']})
    clock = pd.DatetimeIndex(pd.to_datetime(clocks, utc=True))
    pnl = np.zeros(len(clock))
    exposure = np.zeros(len(clock))
    for row in accepted.itertuples():
        path = paths[int(row.sid)]
        stamps = pd.to_datetime(path['timestamp_ns'], utc=True)
        close = pd.Series(path['close'], index=stamps).reindex(clock)
        opened = (clock >= row.entry_ts) & (clock < row.exit_ts)
        if close.loc[opened].isna().any():
            raise ValueError('Missing minute marks for an open position')
        sign = 1 if row.side == 'LONG' else -1
        pnl[opened] += row.quantity * sign * (close.loc[opened].to_numpy() - row.entry_price) - row.cost_rupees/2
        pnl[clock >= row.exit_ts] += row.net_profit_rupees
        exposure[opened] += row.quantity * close.loc[opened].to_numpy()
    peak = np.maximum.accumulate(np.r_[0., pnl])[1:]
    return pd.DataFrame({'timestamp': clock, 'equity_pnl_rupees': pnl,
                         'drawdown_rupees': peak-pnl, 'exposure_rupees': exposure})


def daily_metrics(ledger: pd.DataFrame, days: list[str]) -> pd.DataFrame:
    records = []
    for day in days:
        rows = ledger.loc[ledger.day.astype(str).eq(day)]
        executed = rows.loc[rows.status.eq('EXECUTED')]
        net = executed.net_profit_rupees
        records.append(dict(day=day, selected=len(rows), trades=len(executed),
                            wins=int(net.gt(0).sum()), losses=int(net.lt(0).sum()),
                            net_profit_rupees=float(net.sum()), cost_rupees=float(executed.cost_rupees.sum())))
    return pd.DataFrame(records)


def metrics(ledger: pd.DataFrame, daily: pd.DataFrame, marks: pd.DataFrame) -> dict:
    pnl = ledger.loc[ledger.status.eq('EXECUTED'), 'net_profit_rupees']
    loss = float(-pnl[pnl < 0].sum())
    curve = daily.net_profit_rupees.cumsum().to_numpy()
    return dict(selected_orders=len(ledger), executed_trades=len(pnl), wins=int(pnl.gt(0).sum()),
                losses=int(pnl.lt(0).sum()), win_rate_pct=float(pnl.gt(0).mean()*100) if len(pnl) else None,
                net_profit_rupees=float(pnl.sum()), profit_factor=float(pnl[pnl > 0].sum()/loss) if loss else None,
                cost_rupees=float(ledger.cost_rupees.sum()),
                daily_close_drawdown_rupees=float((np.maximum.accumulate(np.r_[0., curve])[1:]-curve).max()) if len(curve) else 0.,
                minute_close_mtm_drawdown_rupees=float(marks.drawdown_rupees.max()) if len(marks) else 0.,
                peak_minute_close_exposure_rupees=float(marks.exposure_rupees.max()) if len(marks) else 0.,
                worst_day_rupees=float(daily.net_profit_rupees.min()) if len(daily) else None,
                status_counts={str(k): int(v) for k, v in ledger.status.value_counts().items()})
