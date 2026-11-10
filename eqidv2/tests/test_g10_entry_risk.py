"""Path-based checks for the G10 pending-open RR gate, independent of outcomes."""
import numpy as np
import pandas as pd
import pytest

from research import g10_entry_risk as gate


def stamp(clock):
    return pd.Timestamp('2026-10-01 '+clock, tz='Asia/Kolkata')


def example(side='LONG', fill_index=3, gap_pct=0., size=12):
    times = pd.date_range(stamp('10:01'), periods=size, freq='min')
    p = dict(timestamp_ns=times.as_unit('ns').asi8, open=np.full(size, 99.8),
             high=np.full(size, 99.9), low=np.full(size, 99.7), close=np.full(size, 99.8))
    if fill_index is not None:
        opening = 100.+gap_pct
        p['open'][fill_index] = opening
        p['high'][fill_index] = max(100.2, opening)
        p['low'][fill_index] = min(99.9, opening)
        p['close'][fill_index] = 100.
    if side == 'SHORT':
        p = {**p, 'open':200.-p['open'], 'high':200.-p['low'],
             'low':200.-p['high'], 'close':200.-p['close']}
    filled = fill_index is not None and fill_index < 10
    row = dict(sid=1, side=side, day='2026-10-01', tradingsymbol='EXAMPLE', setup_id='1000_'+side,
               confirmation_ts=stamp('10:00'), trigger=100., filled=filled, g8_baseline_filled=filled,
               entry_path_index=fill_index if filled else np.nan,
               entry_ts=times[fill_index] if filled else pd.NaT,
               entry_price=(100.+(1 if side=='LONG' else -1)*max(gap_pct, 0.)) if filled else np.nan,
               native_stop_pct=1.2, native_target_pct=2., exit_price=101., exit_ts=stamp('10:12'),
               exit_path_index=11, gross_return_pct=1., net_return_pct=.95, exit_reason='TARGET')
    return pd.DataFrame([row]), {1:p}


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_no_adverse_drift_preserves_native_fill_fields_and_input(side):
    native, paths = example(side)
    before = native.copy(deep=True)
    r = gate.cancel_overextended_orders(native, paths)
    pd.testing.assert_frame_equal(r[native.columns], before)
    pd.testing.assert_frame_equal(native, before)
    assert r.g10_entry_check.tolist() == ['RETAINED']
    assert r.g10_original_native_filled.tolist() == [True]
    assert r.g10_reference_rr_retention.iloc[0] == pytest.approx(1.)


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_adverse_open_cancels_permanently_even_if_later_recovers(side):
    native, paths = example(side, fill_index=0, gap_pct=.5)
    # The first bar closes back through the trigger; later opens also recover.
    r = gate.cancel_overextended_orders(native, paths)
    assert not r.filled.iloc[0]
    assert not r.g8_baseline_filled.iloc[0]
    assert r.g10_original_native_filled.iloc[0]
    assert r.g10_entry_check.iloc[0] == 'CANCELED_REFERENCE_RR'
    assert r.g10_cancel_path_index.iloc[0] == 0
    assert r.g10_decision_ts.iloc[0] == stamp('10:00')
    assert r.g10_reference_rr_retention.iloc[0] < .8
    for c in ('entry_ts','entry_price','entry_path_index','exit_ts','exit_price','exit_path_index',
              'gross_return_pct','net_return_pct'):
        assert pd.isna(r[c].iloc[0])


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_later_prices_and_outcomes_cannot_cancel_an_already_retained_entry(side):
    native, paths = example(side, fill_index=1)
    a = gate.cancel_overextended_orders(native, paths)
    altered = {1:{k:v.copy() for k,v in paths[1].items()}}
    values = (500.,501.,499.,500.) if side=='LONG' else (10.,11.,9.,10.)
    for k,v in zip(('open','high','low','close'), values):
        altered[1][k][2:] = v
    changed_native = native.copy(deep=True)
    changed_native['gross_return_pct'] = -99.
    changed_native['exit_reason'] = 'STOP'
    b = gate.cancel_overextended_orders(changed_native, altered)
    for c in ('filled','entry_ts','entry_price','g10_entry_check','g10_cancel_path_index',
              'g10_decision_ts','g10_reference_rr_retention'):
        pd.testing.assert_series_equal(a[c], b[c])


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_reference_rr_is_side_symmetric_without_fees_and_fees_are_deducted(side):
    opening = 100.5 if side=='LONG' else 99.5
    a = gate.reward_risk_at_open(100., opening, 1.2, 2., side, 0.)
    b = gate.reward_risk_at_open(100., opening, 1.2, 2., side, 5.)
    assert a['current_rr'] == pytest.approx(1.5/1.7)
    assert a['retention'] == pytest.approx((1.5/1.7)/(2./1.2))
    assert b['current_rr'] < a['current_rr']


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_exact_computed_retention_is_accepted_and_lower_value_is_rejected(side):
    native, paths = example(side, fill_index=0, gap_pct=.2)
    score = gate.reward_risk_at_open(100., paths[1]['open'][0], 1.2, 2., side, 5.)['retention']
    assert gate.cancel_overextended_orders(native, paths, retention=score).filled.iloc[0]
    assert not gate.cancel_overextended_orders(native, paths, retention=np.nextafter(score, 1.)).filled.iloc[0]


@pytest.mark.parametrize('field', ['timestamp_ns','open','high','low','close'])
def test_invalid_bar_anywhere_in_entire_pending_window_fails_closed(field):
    native, paths = example(fill_index=0)
    if field=='timestamp_ns':
        paths[1][field][9] += 60_000_000_000
    else:
        paths[1][field][9] = np.nan
    with pytest.raises(ValueError, match='pending'):
        gate.cancel_overextended_orders(native, paths)


def test_short_pending_window_is_not_accepted_even_if_native_fill_is_early():
    native, paths = example(fill_index=0, size=9)
    with pytest.raises(ValueError, match='pending window'):
        gate.cancel_overextended_orders(native, paths)


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_touch_after_ten_minute_lifetime_does_not_make_an_entry(side):
    native, paths = example(side, fill_index=10)
    r = gate.cancel_overextended_orders(native, paths)
    assert not r.filled.iloc[0]
    assert not r.g10_original_native_filled.iloc[0]
    assert r.g10_entry_check.iloc[0] == 'RETAINED'


def test_claimed_unfilled_native_with_real_touch_is_rejected():
    native, paths = example()
    native[['filled','g8_baseline_filled']] = False
    with pytest.raises(ValueError, match='Unfilled order'):
        gate.cancel_overextended_orders(native, paths)


@pytest.mark.parametrize('side', ['LONG','SHORT'])
def test_an_earlier_unambiguous_touch_cannot_be_hidden_by_recorded_fill(side):
    native, paths = example(side, fill_index=5)
    paths[1]['high' if side=='LONG' else 'low'][2] = 100.
    with pytest.raises(ValueError, match='earlier native fill'):
        gate.cancel_overextended_orders(native, paths)


@pytest.mark.parametrize('column,value,message', [
    ('entry_path_index',3.5,'integer'), ('entry_ts',stamp('10:05'),'timestamp'),
    ('entry_price',123.,'unsupported'), ('g8_baseline_filled',False,'flags disagree')])
def test_inconsistent_source_fill_metadata_fails_closed(column, value, message):
    native, paths = example()
    native[column] = value
    with pytest.raises(ValueError, match=message):
        gate.cancel_overextended_orders(native, paths)


def test_full_inventory_index_and_original_fill_audit_are_preserved():
    a, ap = example(fill_index=0, gap_pct=.5)
    b, bp = example('SHORT', fill_index=2)
    c, cp = example(fill_index=None)
    b['sid'], c['sid'] = 2, 3
    native = pd.concat([a,b,c], ignore_index=True)
    native.index = [9,2,7]
    r = gate.cancel_overextended_orders(native, {1:ap[1], 2:bp[1], 3:cp[1]})
    assert r.index.tolist() == [9,2,7]
    assert r.sid.tolist() == [1,2,3]
    assert r.g10_original_native_filled.tolist() == [True,True,False]
    assert r.filled.tolist() == [False,True,False]


def test_duplicate_identity_is_rejected():
    native, paths = example()
    native = pd.concat([native,native], ignore_index=True)
    with pytest.raises(ValueError, match='unique'):
        gate.cancel_overextended_orders(native, paths)
