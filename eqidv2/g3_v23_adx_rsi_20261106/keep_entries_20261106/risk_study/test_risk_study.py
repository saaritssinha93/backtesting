"""Offline invariants for constant-entry ADX/RSI risk sizing."""
from pathlib import Path
import json

import numpy as np
import pandas as pd
import pytest

from g3_v23_adx_rsi_20261106.keep_entries_20261106.risk_study import run


def audit():
    return pd.DataFrame([
        dict(sid=1, day='2026-10-01', side='LONG', all_g7_allowed=True,
             adx_ready=True, adx_delta3_ready=True, rsi_ready=True,
             adx14=30., plus_di14=30., minus_di14=10., adx_delta3=1.,
             rsi14=60., signed_rsi_delta3=1., confirmation_ts='2026-10-01T10:31:00+05:30',
             adx_available_at='2026-10-01T10:31:00+05:30', feature_available_at='2026-10-01T10:31:00+05:30'),
        dict(sid=2, day='2026-10-02', side='SHORT', all_g7_allowed=True,
             adx_ready=True, adx_delta3_ready=True, rsi_ready=True,
             adx14=19., plus_di14=30., minus_di14=10., adx_delta3=-1.,
             rsi14=60., signed_rsi_delta3=-1., confirmation_ts='2026-10-02T10:31:00+05:30',
             adx_available_at='2026-10-02T10:31:00+05:30', feature_available_at='2026-10-02T10:31:00+05:30')])


def test_factors_use_no_pnl_and_never_zero_out_entries():
    x = audit()
    a = run.factors(x,'weak_50pct')
    x['portfolio_net_profit_rupees'] = [1e12,-1e12]
    pd.testing.assert_series_equal(a,run.factors(x,'weak_50pct'))
    assert a.tolist() == [1., .5] and a.gt(0).all()


def test_normalization_uses_only_earlier_orders_not_fills_or_later_data():
    x = audit()
    scale = run.normalization(x,'train_normalized_weak_50')
    assert np.isclose(scale,4/3)
    later = x.iloc[[1]].copy()
    later['sid'] = 3
    later['day'] = '2026-11-04'
    assert run.normalization(pd.concat([x,later]),'train_normalized_weak_50') == scale
    x['filled'] = [False,True]
    assert run.normalization(x,'train_normalized_weak_50') == scale


def test_native_outcomes_and_cost_records_are_preserved():
    x = audit()
    trades = pd.DataFrame(dict(sid=[1,2], day=x.day,
        tradingsymbol=['A','B'], entry_ts=['entry1','entry2'],exit_ts=['exit1','exit2'],
        portfolio_net_profit_rupees=[1000.,-1000.]))
    book = run.make_book(trades,x,'weak_50pct',1.)
    pd.testing.assert_frame_equal(book[trades.columns],trades)
    assert book.adjusted_net_pnl_rs.tolist() == [1000.,-500.]
    assert book.baseline_costs_rs.tolist() == [250.,250.]
    assert book.adjusted_costs_rs.tolist() == [250.,125.]
    assert np.array_equal(np.sign(book.adjusted_net_pnl_rs),np.sign(trades.portfolio_net_profit_rupees))


def test_later_outcomes_cannot_select_policy():
    base = dict(policy='baseline_fixed_size',period='early_59',net_pnl_rs=100.,
                gross_loss_rs=50.,daily_close_drawdown_rs=20.)
    candidate = {**base,'policy':'weak_75pct','net_pnl_rs':99.,'gross_loss_rs':30.,'daily_close_drawdown_rs':15.}
    later = {**candidate,'period':'later_6','net_pnl_rs':1e12}
    assert run.choose(pd.DataFrame([base,candidate,later])) == ('baseline_fixed_size','NO_ELIGIBLE_SIZING_CHANGE')
    candidate['net_pnl_rs'] = 100.
    assert run.choose(pd.DataFrame([base,candidate,later]))[0] == 'weak_75pct'


def test_saved_study_preserves_every_daily_trade_and_outcome_sign():
    folder = run.PACKAGE/'results_through_20261104'
    reference = pd.read_csv(run.SOURCE/'frozen_inputs/v21_g7_native_trades.csv')
    books = pd.read_csv(folder/'trade_comparison.csv')
    ref = reference.sort_values('sid').reset_index(drop=True)
    for name,book in books.groupby('policy'):
        book = book.sort_values('sid').reset_index(drop=True)
        assert len(book) == 771
        for field in reference.columns:
            if pd.api.types.is_numeric_dtype(reference[field]):
                assert np.allclose(book[field],ref[field])
            else:
                assert book[field].equals(ref[field])
        assert book.groupby('day').sid.apply(set).equals(ref.groupby('day').sid.apply(set))
        assert np.array_equal(np.sign(book.adjusted_net_pnl_rs),np.sign(ref.portfolio_net_profit_rupees))
    full = pd.read_csv(folder/'overall.csv').query("period == 'full_65'")
    assert full.trades.eq(771).all() and full.wins.eq(524).all() and full.losses.eq(247).all()
    assert np.allclose(full.win_rate_pct,100*524/771)
    config = json.loads((folder/'config.json').read_text())
    assert config['selection'] == 'NO_ELIGIBLE_SIZING_CHANGE'
    for name,digest in json.loads((folder/'manifest.json').read_text()).items():
        assert run.sha(folder/name) == digest


def test_runner_will_not_overwrite_existing_result():
    with pytest.raises(ValueError,match='fresh output'):
        run.run(run.PACKAGE/'results_through_20261104')
