from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_a_backtest as e
import fno_v13_v10_a_research as r


def market(side='LONG'):
    confirmation = pd.Timestamp('2026-08-03 09:26', tz='Asia/Kolkata')
    stamps = pd.date_range(confirmation + pd.Timedelta(minutes=1),
                          confirmation.normalize() + pd.Timedelta(hours=15, minutes=15), freq='min')
    row = dict(sid=1, day=confirmation.date(), tradingsymbol='TEST', side=side, trigger=100.,
               confirmation_ts=confirmation, signal_ts=confirmation - pd.Timedelta(minutes=1),
               setup_id='0926_' + side, picker='max_liquidity', traded_value=1e9)
    rng = np.random.default_rng(10101)
    close = 100 + np.cumsum(rng.normal(0, .15, len(stamps)))
    opening = np.r_[100., close[:-1]]
    path = dict(timestamp_ns=stamps.asi8, open=opening, high=np.maximum(opening, close) + .1,
                low=np.minimum(opening, close) - .1, close=close)
    return dict(orders=pd.DataFrame([row]), paths={1: path}, days=[confirmation.date()],
                v9_config=e.v10.v9.V9Config())


@pytest.mark.parametrize('stop,target', [(1, 1.49), (1.5, 2), (.5, 2.01), (0, 1), (float('nan'), 1)])
def test_invalid_brackets_rejected(stop, target):
    with pytest.raises(ValueError):
        e.exit_config(dict(stop_pct=stop, target_pct=target))


def test_registered_grid_is_unique_and_meets_full_exit_constraints():
    pairs = r.grid()
    assert len(pairs) == len(np.unique(pairs, axis=0)) == 11594
    for stop, target in pairs:
        cfg = e.exit_config(dict(stop_pct=stop, target_pct=target))
        assert cfg.partial_pct == 1 and cfg.runner_stop == 'INITIAL'
        assert target + 1e-12 >= 1.5 * stop


@pytest.mark.parametrize('side', ['LONG', 'SHORT'])
def test_cache_matches_native_full_brackets_and_missing_slot_fallback(side):
    dataset = market(side)
    pairs = np.array([[.1, .15], [.4, .6], [.8, 1.8], [1.3, 2.]])
    cache = r.ReplayCache(dataset, pairs)
    for index in range(len(pairs)):
        ids = np.full(len(cache.setup_names), index)
        cfg = r.settings(cache, ids, index)
        cfg['setups'] = {}  # Unknown/missing configuration uses validated shared pair.
        trades, ledger, _ = e.evaluate(dataset, cfg)
        r.verify(cache, ids, ledger)
        assert trades.partial_pct.eq(1).all()
        assert not trades.exit_reason.str.contains('BREAKEVEN|T1_THEN').any()


@pytest.mark.parametrize('gap', [False, True])
def test_accelerator_stop_first_and_later_gap_fill(gap):
    dataset = market()
    p = dataset['paths'][1]
    for key, value in [('open', 100), ('high', 100.1), ('low', 99.9), ('close', 100)]:
        p[key][:] = value
    p['high'][1], p['low'][1] = 102, 98
    if gap:
        p['open'][1] = 98.5
    cache = r.ReplayCache(dataset, np.array([[1., 1.5]]))
    cfg = r.settings(cache, np.zeros(14, dtype=int), 0)
    trades, ledger, _ = e.evaluate(dataset, cfg)
    r.verify(cache, np.zeros(14, dtype=int), ledger)
    assert trades.iloc[0].exit_reason == 'FULL_STOP'
    assert trades.iloc[0].gross_return_pct == pytest.approx(-1.5 if gap else -1.)


def tiny_cache():
    cache = r.ReplayCache.__new__(r.ReplayCache)
    cache.rows = pd.DataFrame({'sid': range(5)})
    cache.setup_index = np.zeros(5, dtype=int)
    cache.entry = np.array([10, 11, 12, 13, 14])
    cache.exit = np.array([[20, 13], [20, 20], [20, 20], [20, 20], [20, 20]])
    cache.gross = np.ones((5, 2))
    cache.days = np.array(['2026-08-03'] * 3 + ['2026-09-01'] * 2)
    return cache


def test_capital_released_at_equal_timestamp_and_rejected_trade_reserves_nothing():
    cache = tiny_cache()
    _, accepted = cache.replay(np.array([[0], [1]]))
    np.testing.assert_array_equal(accepted[0], [True, True, True, False, False])
    np.testing.assert_array_equal(accepted[1], [True, True, True, True, False])


def test_later_outcomes_cannot_change_development_objective_or_ranking():
    cache = tiny_cache()
    ids = np.array([[0], [1]])
    before = cache.assess(ids)
    cache.gross[-2:] = -100
    after = cache.assess(ids)
    cols = ['deficit', 'objective_pf', 'objective_net', 'TRAIN_win', 'VALIDATION_win']
    pd.testing.assert_frame_equal(before[cols], after[cols])
    np.testing.assert_array_equal(r.ranking(before), r.ranking(after))


def test_unsupported_portfolio_fails_closed():
    dataset = market()
    dataset['v9_config'] = replace(dataset['v9_config'], max_positions_per_symbol=1)
    with pytest.raises(ValueError, match='frozen V10 portfolio'):
        r.ReplayCache(dataset, np.array([[1., 1.5]]))
