"""G10 pending-order price-drift check. No future outcome selects an entry."""
from __future__ import annotations
from types import SimpleNamespace
import numpy as np
import pandas as pd
from research.g8_exit_engine import _flags
from fno_v13_corrected_v5_backtest import _entry


def reward_risk_at_open(trigger, observed_open, stop_pct, target_pct, side, cost_bps):
    """Compare executable price to levels fixed at the original signal trigger.

    These levels are a quality benchmark only. Accepted trades retain the
    actual-fill-anchored G7 exits; no claim that this ratio is realized RR.
    """
    values = (trigger, observed_open, stop_pct, target_pct, cost_bps)
    if (any(isinstance(x, (bool, np.bool_)) for x in values) or
            not all(np.isfinite(x) for x in values) or min(values[:4]) <= 0 or cost_bps < 0 or
            stop_pct >= 100 or target_pct >= 100):
        raise ValueError('Invalid reference risk inputs')
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Invalid side')
    direction = 1 if side == 'LONG' else -1
    fill = max(trigger, observed_open) if direction == 1 else min(trigger, observed_open)
    stop = trigger * (1 - direction * stop_pct / 100)
    target = trigger * (1 + direction * target_pct / 100)
    risk = direction * (fill - stop) + fill * cost_bps / 10000
    reward = direction * (target - fill) - fill * cost_bps / 10000
    baseline_risk = trigger * (stop_pct / 100 + cost_bps / 10000)
    baseline_reward = trigger * (target_pct / 100 - cost_bps / 10000)
    ratio = reward / risk if risk > 0 else float('-inf')
    baseline = baseline_reward / baseline_risk
    return dict(reference_stop=stop, reference_target=target, prospective_entry=fill,
                reference_rr=baseline, current_rr=ratio,
                retention=ratio / baseline if baseline > 0 else float('-inf'))


def cancel_overextended_orders(native, paths, retention=.8, cost_bps=5., expiry_minutes=10):
    if isinstance(retention, (bool, np.bool_)) or not np.isfinite(retention) or not 0 < retention <= 1:
        raise ValueError('RR retention must be in (0,1]')
    if expiry_minutes != 10:
        raise ValueError('This protocol preserves the original ten-minute lifetime')
    required = {'sid', 'filled', 'side', 'confirmation_ts', 'entry_path_index', 'entry_ts',
                'entry_price', 'trigger', 'native_stop_pct', 'native_target_pct'}
    if not required.issubset(native):
        raise ValueError('Missing native order columns: ' + ', '.join(sorted(required.difference(native))))
    if native.sid.isna().any() or native.sid.duplicated().any() or not native.index.is_unique:
        raise ValueError('Native SID and row index must be unique and nonmissing')
    records = []
    filled_flags = _flags(native.filled, 'native filled')
    if 'g8_baseline_filled' in native and not filled_flags.equals(_flags(native.g8_baseline_filled, 'baseline filled')):
        raise ValueError('Native filled and baseline filled flags disagree')
    for index, source in native.iterrows():
        row = source.to_dict()
        path = paths[int(row['sid'])]
        confirmation = pd.Timestamp(row['confirmation_ts'])
        if confirmation.tzinfo is None:
            confirmation = confirmation.tz_localize('Asia/Kolkata')
        stamps = pd.date_range(confirmation + pd.Timedelta(minutes=1), periods=expiry_minutes, freq='min')
        if not np.array_equal(path['timestamp_ns'][:expiry_minutes], stamps.tz_convert('UTC').asi8):
            raise ValueError('Incomplete pending window')
        a = np.column_stack([path[k][:expiry_minutes] for k in ('open', 'high', 'low', 'close')])
        if (not np.isfinite(a).all() or (a <= 0).any() or
                (a[:, 1] < a.max(axis=1)).any() or (a[:, 2] > a.min(axis=1)).any()):
            raise ValueError('Invalid pending OHLC')
        filled = bool(filled_flags.loc[index])
        fill_index = None
        if filled:
            raw_index = float(row['entry_path_index'])
            if not np.isfinite(raw_index) or raw_index != int(raw_index):
                raise ValueError('Recorded entry_path_index must be an integer')
            fill_index = int(raw_index)
        trigger = float(row['trigger'])
        strict = _entry(SimpleNamespace(**row), path, delay_bars=0, trigger_buffer_pct=0.,
                        worse_fill_bps=0., max_entry_delay_minutes=expiry_minutes)
        if filled:
            if not 0 <= fill_index < expiry_minutes:
                raise ValueError('Recorded fill outside lifetime')
            entry_ts = pd.Timestamp(row['entry_ts'])
            if pd.isna(entry_ts):
                raise ValueError('Recorded entry timestamp is missing')
            if entry_ts.tzinfo is None:
                entry_ts = entry_ts.tz_localize('Asia/Kolkata')
            if entry_ts.value != int(path['timestamp_ns'][fill_index]):
                raise ValueError('Recorded entry timestamp differs from path bar')
            long = row['side'] == 'LONG'
            tolerance = 8 * np.finfo(float).eps * max(1., abs(trigger))
            touch = (path['high'][fill_index] >= trigger - tolerance if long else
                     path['low'][fill_index] <= trigger + tolerance)
            price = max(path['open'][fill_index], trigger) if long else min(path['open'][fill_index], trigger)
            if not touch or not np.isclose(price, row['entry_price'], rtol=0, atol=1e-6):
                raise ValueError('Recorded entry unsupported by minute path')
            if strict is not None and strict[0] < fill_index:
                raise ValueError('An earlier native fill exists')
        elif strict is not None:
            raise ValueError('Unfilled order has a native fill')
        canceled = None
        last = None
        for i in range(fill_index + 1 if filled else expiry_minutes):
            last = reward_risk_at_open(trigger, float(path['open'][i]),
                float(row['native_stop_pct']), float(row['native_target_pct']), row['side'], cost_bps)
            if last['reference_rr'] <= 0 or last['retention'] < retention:
                canceled = i
                break
        row.update(g10_original_native_filled=filled,
                   g10_entry_check='CANCELED_REFERENCE_RR' if canceled is not None else 'RETAINED',
                   g10_cancel_path_index=canceled,
                   g10_decision_ts=(pd.Timestamp(int(path['timestamp_ns'][canceled]), tz='UTC') - pd.Timedelta(minutes=1)
                                    if canceled is not None else pd.NaT),
                   g10_reference_rr=last['reference_rr'], g10_reference_rr_retention=last['retention'])
        if canceled is not None:
            row.update(filled=False, g8_baseline_filled=False, entry_ts=pd.NaT, entry_price=np.nan,
                entry_path_index=np.nan, exit_ts=pd.NaT, exit_price=np.nan, exit_path_index=np.nan,
                gross_return_pct=np.nan, net_return_pct=np.nan, exit_reason='UNFILLED')
        records.append(row)
    return pd.DataFrame(records, index=native.index) if records else native.copy(deep=True)
