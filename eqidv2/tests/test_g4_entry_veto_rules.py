import numpy as np
import pandas as pd
import pytest

from research.g4_entry_veto_rules import confirmation_features, pending_invalidation


def case(side='LONG'):
    stamp = pd.Timestamp('2026-10-15 10:01', tz='Asia/Kolkata')
    row = dict(side=side, trigger=101. if side == 'LONG' else 99.,
               confirmation_ts=stamp, confirmation_high=101., confirmation_low=99.)
    path = dict(timestamp_ns=pd.date_range(stamp + pd.Timedelta(minutes=1), periods=10, freq='min').asi8,
                open=np.full(10,100.), high=np.full(10,100.5), low=np.full(10,99.5), close=np.full(10,100.))
    return row, path


@pytest.mark.parametrize('side', ['LONG', 'SHORT'])
def test_cancel_only_after_opposite_close_before_fill(side):
    row, path = case(side)
    path['close'][0] = 98.9 if side == 'LONG' else 101.1
    path['low' if side == 'LONG' else 'high'][0] = path['close'][0]
    path['high' if side == 'LONG' else 'low'][1] = row['trigger']
    result = pending_invalidation(row, path)
    assert result['pending_cancelled']
    assert result['pending_cancel_ts'].startswith('2026-10-15 10:02')


@pytest.mark.parametrize('side', ['LONG', 'SHORT'])
def test_same_bar_trigger_cannot_be_cancelled_using_its_future_close(side):
    row, path = case(side)
    path['high'][0], path['low'][0] = 101.2, 98.8
    path['close'][0] = 98.9 if side == 'LONG' else 101.1
    assert pending_invalidation(row, path)['pending_fill_index'] == 0
    assert not pending_invalidation(row, path)['pending_cancelled']


def test_wick_or_boundary_touch_does_not_cancel():
    row, path = case()
    path['low'][0] = 98.
    path['close'][0] = 99.
    path['high'][1] = 101.
    assert pending_invalidation(row, path)['pending_fill_index'] == 1


def test_invalid_timestamp_window_is_rejected():
    row, path = case()
    path['timestamp_ns'][1] += 1
    with pytest.raises(ValueError, match='consecutive'):
        pending_invalidation(row, path)


def test_confirmation_extension_ignores_all_future_bars():
    stamp = pd.Timestamp('2026-10-15 10:01', tz='Asia/Kolkata')
    raw = pd.DataFrame(dict(ts=pd.date_range(stamp-pd.Timedelta(minutes=29), periods=40, freq='min'),
                            open=100., high=101., low=99., close=100.))
    row = dict(confirmation_ts=stamp, side='LONG', confirmation_open=100.,
               confirmation_high=101., confirmation_low=99., confirmation_close=100.)
    before = confirmation_features(raw,row)
    raw.loc[raw.ts.gt(stamp), ['open','high','low','close']] = 10000.
    assert confirmation_features(raw,row) == before
