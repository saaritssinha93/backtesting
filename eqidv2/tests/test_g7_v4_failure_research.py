"""Stronger user-objective gates and complete portfolio exit integration."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_failure_research as study
from research.g8_exit_engine import allocate_variant


def table():
    rows = []
    for period in ('development', 'validation', 'assessment'):
        for name in ['g7_v2', 'v4_symbol_guard', *study.POLICIES]:
            candidate = name in study.POLICIES
            rows.append(dict(policy=name, reference='v4_symbol_guard', period=period,
                net_pnl_rs=10500. if candidate or name == 'g7_v2' else 10000.,
                wins=101 if candidate else 100, losses=19 if candidate else 20,
                win_rate_pct=101/120*100 if candidate else 100/120*100,
                negative_trade_sum_abs_rs=900. if candidate else 1000.,
                daily_close_drawdown_rs=100., trades=120, changed_executions=6, changed_days=3))
    return pd.DataFrame(rows)


def test_selection_never_uses_later_results_or_selects_validation_replacement():
    t = table()
    expected = study.select(t, list(study.POLICIES))
    assert expected[0] in study.POLICIES
    t.loc[~t.period.eq('development'), 'net_pnl_rs'] = -1e20
    assert study.select(t, list(study.POLICIES)) == expected
    assert study.select(t, [expected[0]], 'validation')[0] is None


@pytest.mark.parametrize('field,value,reason', [
    ('net_pnl_rs', 9999., 'net profit below current V4'),
    ('wins', 100, 'winning trade count not increased'),
    ('losses', 20, 'losing trade count not reduced'),
    ('win_rate_pct', 80., 'win rate reduced'),
    ('negative_trade_sum_abs_rs', 951., 'losing amount reduction below5%'),
    ('daily_close_drawdown_rs', 101., 'larger daily-close drawdown'),
    ('trades', 107, 'trade retention below90%'),
    ('changed_executions', 4, 'insufficient changed executions/days'),
    ('changed_days', 2, 'insufficient changed executions/days'),
])
def test_partial_metric_improvement_cannot_pass_full_objective(field, value, reason):
    t = table()
    mask = t.policy.eq('confirmed_trail') & t.period.eq('development')
    t.loc[mask, field] = value
    chosen, audit = study.select(t, ['confirmed_trail'])
    assert chosen is None
    assert reason in audit[0]['failure_reasons']


def test_original_v2_net_floor_still_applies_separately():
    t = table()
    t.loc[t.policy.eq('g7_v2'), 'net_pnl_rs'] = 12000.
    selected, audit = study.select(t, ['confirmed_trail'])
    assert selected is None
    assert 'net profit below95% of original V2' in audit[0]['failure_reasons']


CONFIG = dict(initial_stop_pct=1.2, tightened_stop_pct=1., tighten_after_minutes=120,
              capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.,
              max_positions_per_symbol=1, model_cost_bps_round_trip=5.)


def opportunities():
    ts = pd.date_range('2026-10-26 10:00', '2026-10-26 15:15', freq='min', tz='Asia/Kolkata')
    p = dict(timestamp_ns=ts.asi8, open=np.full(len(ts), 100.2),
        high=np.full(len(ts), 100.3), low=np.full(len(ts), 100.1), close=np.full(len(ts), 100.2))
    for i, bar in enumerate([(100., 100.1, 99.9, 100.), (100., 100.1, 99.6, 99.7),
                              (99.7, 99.8, 99.5, 99.6), (99.6, 100.3, 99.5, 100.2)]):
        for field, value in zip(('open', 'high', 'low', 'close'), bar):
            p[field][i] = value
    r = dict(sid=1, day='2026-10-26', setup_id='1000_LONG', tradingsymbol='TEST',
        side='LONG', filled=True, entry_ts=ts[0], entry_price=100., entry_path_index=0,
        native_stop_pct=1.2, native_target_pct=2., picker='max_liquidity', traded_value=1e6,
        confirmation_low=99.8, confirmation_high=100.)
    second = dict(r, sid=2, setup_id='1004_LONG', entry_ts=ts[4], entry_price=100.2, entry_path_index=4,
                  confirmation_low=99.5, confirmation_high=100.2)
    missing = dict(r, sid=3, setup_id='1005_LONG', filled=False, entry_ts=None,
                   entry_price=np.nan, entry_path_index=np.nan)
    return pd.DataFrame([r, second, missing]), {1: p, 2: p}


@pytest.mark.parametrize('policy', [None, *study.POLICIES])
def test_policy_keeps_entries_targets_and_unfilled_opportunities(policy):
    native, paths = opportunities()
    raw = study.replay(native, paths, CONFIG, policy)
    assert raw.sid.tolist() == [1, 2, 3]
    assert raw.g8_baseline_filled.tolist() == [True, True, False]
    np.testing.assert_allclose(raw.entry_price, native.entry_price, equal_nan=True)
    np.testing.assert_allclose(raw.native_target_pct, native.native_target_pct)
    book = allocate_variant(raw, pd.Series({3: True, 2: True, 1: False}), CONFIG, policy or 'baseline')
    assert book.portfolio_executed.tolist() == [False, True, False]


def test_market_exit_releases_symbol_for_later_entry_and_recalculates_all_costs():
    native, paths = opportunities()
    books = []
    for policy in (None, 'confirmation_failure'):
        raw = study.replay(native, paths, CONFIG, policy)
        books.append(allocate_variant(raw, pd.Series({1: True, 2: True, 3: True}), CONFIG, policy or 'control'))
    original, changed = books
    assert original.portfolio_executed.tolist() == [True, False, False]
    assert changed.portfolio_executed.tolist() == [True, True, False]
    assert changed.iloc[0].exit_event == 'OPEN'
    assert changed.iloc[0].exit_price == pytest.approx(99.6)
    assert changed.iloc[0].portfolio_net_profit_rupees == pytest.approx(-2250.)
    assert changed.portfolio_cost_rupees.sum() == pytest.approx(500.)
    delta = study.prior.differences(changed, original)
    assert delta.changed_execution.tolist() == [True, True, False]
    assert delta.new_allocation.tolist() == [False, True, False]


def test_runtime_unknown_policy_fails_instead_of_falling_back():
    native, paths = opportunities()
    with pytest.raises(ValueError, match='Unknown fixed policy'):
        study.replay(native, paths, CONFIG, 'new_threshold')
