"""The user-selected V4 rule preserves causal execution and portfolio constraints."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from research import g7_v4_selected_backtest as selected


SETTINGS = dict(
    variant_id='V13-V10-G-7-V4-PROFIT-PROTECTION',
    scope='BACKTEST_ONLY', execution_authority=False, selection_basis='USER_REQUEST',
    max_positions_per_symbol=1,
    exit_policy=dict(protect_after_close_pct=.6, protected_profit_pct=.1,
                     activation='COMPLETED_FULL_POST_ENTRY_CLOSE',
                     effective='NEXT_MINUTE_OPEN'),
)
CONFIG = dict(initial_stop_pct=1.2, tightened_stop_pct=1., tighten_after_minutes=120,
              capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.,
              model_cost_bps_round_trip=5.)


def example(short=False):
    ts = pd.date_range('2026-10-26 10:00', '2026-10-26 15:15', freq='min', tz='Asia/Kolkata')
    p = dict(timestamp_ns=ts.asi8, open=np.full(len(ts), 100.2),
             high=np.full(len(ts), 100.3), low=np.full(len(ts), 100.15), close=np.full(len(ts), 100.2))
    for i, bar in enumerate([(100., 100.8, 99.9, 100.7),
                             (100.2, 100.8, 99.8, 100.7),
                             (100.5, 100.6, 100.05, 100.2)]):
        for name, value in zip(('open', 'high', 'low', 'close'), bar):
            p[name][i] = value
    row = dict(sid=1, day='2026-10-26', setup_id='1000_LONG', tradingsymbol='TEST',
               side='LONG', filled=True, entry_ts=ts[0], entry_price=100., entry_path_index=0,
               native_stop_pct=1.2, native_target_pct=2., picker='max_liquidity', traded_value=1e6)
    if short:
        p = {**p, 'open': 200-p['open'], 'high': 200-p['low'],
             'low': 200-p['high'], 'close': 200-p['close']}
        row.update(side='SHORT', setup_id='1000_SHORT')
    return p, row


@pytest.mark.parametrize('short', [False, True])
def test_completed_close_arms_next_bar_and_preserves_entry_and_target(short):
    p, row = example(short)
    raw = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS)
    r = raw.iloc[0]
    assert r.entry_price == row['entry_price'] and r.entry_ts == row['entry_ts']
    assert r.native_target_pct == row['native_target_pct']
    assert r.protection_armed_path_index == 1
    assert r.protection_effective_path_index == 2
    assert r.exit_path_index == 2 and r.exit_reason == 'PROTECTED_STOP'
    assert r.exit_price == pytest.approx(99.9 if short else 100.1)


@pytest.mark.parametrize('short', [False, True])
def test_entry_bar_close_alone_cannot_arm_protection(short):
    p, row = example(short)
    p['close'][1] = 99.8 if short else 100.2
    r = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS).iloc[0]
    assert r.exit_reason == 'TIME_EXIT_1515'
    assert not r.protection_applied


@pytest.mark.parametrize('short', [False, True])
def test_trigger_is_absolute_point_six_not_half_the_initial_stop(short):
    p, row = example(short)
    row['native_stop_pct'] = 1.4
    p['close'][1] = 99.35 if short else 100.65
    r = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS).iloc[0]
    assert r.initial_stop_pct == pytest.approx(1.4)
    assert r.protection_armed_path_index == 1
    assert r.exit_reason == 'PROTECTED_STOP'


@pytest.mark.parametrize('short', [False, True])
def test_adverse_open_gap_fills_at_observed_open(short):
    p, row = example(short)
    values = (100.2, 100.3, 99.4, 99.8) if short else (99.8, 100.6, 99.7, 100.2)
    for field, value in zip(('open', 'high', 'low', 'close'), values):
        p[field][2] = value
    b = selected.make_book(pd.DataFrame([row]), {1: p}, pd.Series({1: True}), CONFIG, SETTINGS)
    assert b.iloc[0].exit_event == 'OPEN'
    assert b.iloc[0].exit_price == values[0]
    assert b.iloc[0].portfolio_net_profit_rupees == pytest.approx(-1250.)


@pytest.mark.parametrize('cost,expected_net', [(5., 250.), (15., -250.)])
def test_configured_cost_is_charged_and_backtest_metadata_is_v4(cost, expected_net):
    p, row = example()
    cfg = dict(CONFIG, model_cost_bps_round_trip=cost)
    b = selected.make_book(pd.DataFrame([row]), {1: p}, pd.Series({1: True}), cfg, SETTINGS)
    r = b.iloc[0]
    assert r.portfolio_gross_profit_rupees == pytest.approx(500.)
    assert r.portfolio_cost_rupees == pytest.approx(500000.*cost/10000)
    assert r.portfolio_net_profit_rupees == pytest.approx(expected_net)
    assert r.strategy == 'V13-V10-G-7'
    assert r.strategy_version == SETTINGS['variant_id']
    assert r.scope == 'BACKTEST_ONLY' and not r.execution_authority


def test_unknown_future_does_not_change_already_exited_trade():
    p, row = example()
    original = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS)
    for field in ('open', 'high', 'low', 'close'):
        p[field][3:] = 50.
    altered = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS)
    for field in ('entry_price', 'entry_ts', 'exit_price', 'exit_ts', 'gross_return_pct'):
        pd.testing.assert_series_equal(original[field], altered[field])


def test_existing_target_still_executes_before_close_can_arm_protection():
    p, row = example()
    row['native_target_pct'] = .6
    r = selected.replay(pd.DataFrame([row]), {1: p}, CONFIG, SETTINGS).iloc[0]
    assert r.exit_reason == 'TARGET'
    assert r.exit_price == pytest.approx(100.6)
    assert r.exit_path_index == 0
    assert not r.protection_applied


def test_same_symbol_overlap_is_blocked_but_exit_releases_later_entry():
    p, row = example()
    overlap = dict(row, sid=2, setup_id='1001_LONG', entry_path_index=1,
                   entry_ts=pd.Timestamp(p['timestamp_ns'][1], tz='UTC'), entry_price=100.2)
    later = dict(row, sid=3, setup_id='1003_LONG', entry_path_index=3,
                 entry_ts=pd.Timestamp(p['timestamp_ns'][3], tz='UTC'), entry_price=100.2)
    native = pd.DataFrame([row, overlap, later])
    b = selected.make_book(native, {1: p, 2: p, 3: p}, pd.Series({1: True, 2: True, 3: True}), CONFIG, SETTINGS)
    assert b.portfolio_executed.tolist() == [True, False, True]


def test_unfilled_inventory_and_nifty_veto_are_preserved():
    p, row = example()
    unfilled = dict(row, sid=2, setup_id='1001_LONG', filled=False,
                    entry_price=np.nan, entry_path_index=np.nan)
    b = selected.make_book(pd.DataFrame([row, unfilled]), {1: p},
                           pd.Series({2: True, 1: False}), CONFIG, SETTINGS)
    assert b.sid.tolist() == [1, 2]
    assert b.g8_baseline_filled.tolist() == [True, False]
    assert not b.portfolio_executed.any()


@pytest.mark.parametrize('field,value', [
    ('scope', 'LIVE'), ('execution_authority', True), ('execution_authority', 'false'),
    ('max_positions_per_symbol', 2), ('max_positions_per_symbol', None),
])
def test_settings_reject_execution_authority_and_weaker_symbol_constraint(field, value):
    settings = deepcopy(SETTINGS)
    settings[field] = value
    with pytest.raises(ValueError):
        selected.validate_settings(settings)


@pytest.mark.parametrize('field,value', [
    ('protect_after_close_pct', -.6), ('protect_after_close_pct', float('nan')),
    ('protected_profit_pct', -.1), ('protected_profit_pct', .6),
    ('activation', 'INTRABAR_HIGH'), ('effective', 'SAME_BAR'),
])
def test_settings_reject_invalid_protection_threshold_or_timing(field, value):
    settings = deepcopy(SETTINGS)
    settings['exit_policy'][field] = value
    with pytest.raises(ValueError):
        selected.validate_settings(settings)
