"""Integration checks for causal protection and complete portfolio reallocation."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_profit_protection as study
from research.g8_exit_engine import allocate_variant


CONFIG = dict(initial_stop_pct=1.2, tightened_stop_pct=1., tighten_after_minutes=120,
              capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.,
              max_positions_per_symbol=1, model_cost_bps_round_trip=5.)


def example(short=False):
    ts = pd.date_range('2026-10-26 10:00', '2026-10-26 15:15', freq='min', tz='Asia/Kolkata')
    p = dict(timestamp_ns=ts.asi8, open=np.full(len(ts), 100.2),
             high=np.full(len(ts), 100.3), low=np.full(len(ts), 100.15), close=np.full(len(ts), 100.2))
    for i, bar in enumerate([(100., 100.8, 99.9, 100.7), (100.2, 100.8, 99.8, 100.7), (100.5, 100.6, 100.05, 100.2)]):
        for field, value in zip(('open', 'high', 'low', 'close'), bar):
            p[field][i] = value
    row = dict(sid=1, day='2026-10-26', setup_id='1000_LONG', tradingsymbol='TEST',
               side='LONG', filled=True, entry_ts=ts[0], entry_price=100., entry_path_index=0,
               native_stop_pct=1.2, native_target_pct=2., picker='max_liquidity', traded_value=1e6)
    if short:
        p = {**p, 'open': 200-p['open'], 'high': 200-p['low'], 'low': 200-p['high'], 'close': 200-p['close']}
        row.update(side='SHORT', setup_id='1000_SHORT')
    return p, row


@pytest.mark.parametrize('short', [False, True])
def test_completed_close_protection_preserves_entry_and_charges_cost(short):
    p, row = example(short)
    raw = study.replay(pd.DataFrame([row]), {1: p}, CONFIG, 'cushion_half_r')
    r = raw.iloc[0]
    assert r.entry_price == row['entry_price'] and r.entry_ts == row['entry_ts']
    assert r.protection_armed_path_index == 1  # Never the entry candle.
    assert r.protection_effective_path_index == 2
    assert r.exit_path_index == 2 and r.exit_reason == 'PROTECTED_STOP'
    book = allocate_variant(raw, pd.Series({1: True}), CONFIG, 'test')
    assert book.iloc[0].portfolio_net_profit_rupees == pytest.approx(250.)
    assert book.iloc[0].portfolio_cost_rupees == pytest.approx(250.)


@pytest.mark.parametrize('short', [False, True])
def test_protected_stop_cannot_ignore_adverse_gap(short):
    p, row = example(short)
    values = [99.8, 100.6, 99.7, 100.2]
    if short:
        values = [100.2, 100.3, 99.4, 99.8]
    for k, value in zip(('open', 'high', 'low', 'close'), values):
        p[k][2] = value
    raw = study.replay(pd.DataFrame([row]), {1: p}, CONFIG, 'cushion_half_r')
    book = allocate_variant(raw, pd.Series({1: True}), CONFIG, 'test')
    assert book.iloc[0].exit_event == 'OPEN'
    assert book.iloc[0].portfolio_net_profit_rupees == pytest.approx(-1250.)


def test_stop_at_entry_still_loses_transaction_cost():
    p, row = example()
    p['low'][2] = 99.9
    raw = study.replay(pd.DataFrame([row]), {1: p}, CONFIG, 'breakeven_half_r')
    book = allocate_variant(raw, pd.Series({1: True}), CONFIG, 'test')
    assert book.iloc[0].portfolio_net_profit_rupees == pytest.approx(-250.)


def test_future_candles_cannot_change_completed_protective_exit():
    p, row = example()
    old = study.replay(pd.DataFrame([row]), {1: p}, CONFIG, 'cushion_half_r')
    for name in ('open', 'high', 'low', 'close'):
        p[name][3:] = 50.
    new = study.replay(pd.DataFrame([row]), {1: p}, CONFIG, 'cushion_half_r')
    for name in ('entry_price', 'entry_ts', 'exit_price', 'exit_ts', 'gross_return_pct'):
        pd.testing.assert_series_equal(old[name], new[name])


def test_unfilled_inventory_and_nifty_permission_survive_replay():
    p, row = example()
    unfilled = dict(row, sid=2, setup_id='1001_LONG', filled=False, entry_price=np.nan, entry_path_index=np.nan)
    raw = study.replay(pd.DataFrame([row, unfilled]), {1: p}, CONFIG, 'cushion_half_r')
    book = allocate_variant(raw, pd.Series({2: True, 1: False}), CONFIG, 'test')
    assert book.sid.tolist() == [1, 2]
    assert not book.portfolio_executed.any()
    assert raw.g8_baseline_filled.tolist() == [True, False]
    with pytest.raises(ValueError):
        study.replay(pd.DataFrame([dict(row, filled='unknown')]), {1: p}, CONFIG, 'cushion_half_r')


def test_earlier_exit_reallocates_later_same_symbol_entry():
    p, row = example()
    second = dict(row, sid=2, setup_id='1003_LONG', entry_path_index=3,
                  entry_ts=pd.Timestamp(p['timestamp_ns'][3], tz='UTC'), entry_price=100.2)
    native = pd.DataFrame([row, second])
    books = []
    for policy in (None, 'cushion_half_r'):
        raw = study.replay(native, {1: p, 2: p}, CONFIG, policy)
        books.append(allocate_variant(raw, pd.Series({1: True, 2: True}), CONFIG, policy or 'control'))
    assert books[0].portfolio_executed.tolist() == [True, False]
    assert books[1].portfolio_executed.tolist() == [True, True]
    delta = study.differences(books[1], books[0])
    assert delta.changed_execution.tolist() == [True, True]
    assert delta.new_allocation.tolist() == [False, True]


def test_giveback_diagnostic_excludes_unknown_entry_and_exit_bar_closes():
    p, row = example()
    book = pd.DataFrame([dict(row, portfolio_executed=True, exit_path_index=2,
                             exit_ts=pd.Timestamp(p['timestamp_ns'][2], tz='UTC'),
                             exit_reason='STOP', portfolio_net_profit_rupees=-6250.)])
    p['close'][0], p['close'][1], p['close'][2] = 102., 100.3, 103.
    r = study.giveback_audit(book, {1: p}).iloc[0]
    assert r.best_completed_close_pct == pytest.approx(.3)
    assert not r.losing_after_half_r


def selection_table():
    return pd.DataFrame([dict(reference=ref, policy=policy, period=period,
        net_pnl_rs=10000., negative_trade_sum_abs_rs=900. if policy in study.POLICIES else 1000.,
        trades=100, wins=80, daily_close_drawdown_rs=100., changed_executions=10, changed_days=4,
        loss_amount_reduction_rs=100. if policy in study.POLICIES else 0.)
        for ref in study.REFERENCES for policy in [*study.REFERENCES, *study.POLICIES]
        for period in ('development', 'validation', 'assessment')])


def test_selection_ignores_later_results_and_requires_both_controls():
    table = selection_table()
    expected = study.select(table, list(study.POLICIES))
    assert expected[0] is not None
    table.loc[~table.period.eq('development'), 'net_pnl_rs'] = -1e20
    assert study.select(table, list(study.POLICIES)) == expected
    mask = table.period.eq('development') & table.reference.eq('v4_symbol_guard') & table.policy.isin(study.POLICIES)
    table.loc[mask, 'negative_trade_sum_abs_rs'] = 960.
    assert study.select(table, list(study.POLICIES))[0] is None
