"""Offline V23 admission and replay invariants; no API, scheduler, or LIVE use."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from g3_v23_adx_rsi_20261106 import policy, run


def row(**updates):
    value = dict(side='LONG', all_g7_allowed=True,
        adx_ready=True, adx_delta3_ready=True, rsi_ready=True,
        adx14=25., plus_di14=30., minus_di14=10., adx_delta3=1.,
        rsi14=55., signed_rsi_delta3=1.,
        confirmation_ts='2026-11-04T10:31:00+05:30',
        adx_available_at='2026-11-04T10:31:00+05:30',
        feature_available_at='2026-11-04T10:31:00+05:30')
    return {**value, **updates}


@pytest.mark.parametrize('adx,allowed', [(19.999, False), (20., True), (25., True)])
def test_selected_adx_threshold(adx, allowed):
    assert bool(policy.masks(pd.DataFrame([row(adx14=adx)]))['adx20_di_rsi'].iloc[0]) == allowed


@pytest.mark.parametrize('side,plus,minus,rsi_delta,allowed', [
    ('LONG', 30, 10, 0, True), ('LONG', 10, 30, 1, False),
    ('SHORT', 10, 30, 0, True), ('SHORT', 30, 10, 1, False),
    ('LONG', 30, 10, -.1, False), ('SHORT', 10, 30, -.1, False),
    ('LONG', 20, 20, 1, False)])
def test_di_and_signed_rsi_momentum(side, plus, minus, rsi_delta, allowed):
    x = pd.DataFrame([row(side=side, plus_di14=plus, minus_di14=minus, signed_rsi_delta3=rsi_delta)])
    assert bool(policy.masks(x)['adx20_di_rsi'].iloc[0]) == allowed


def test_unknown_warmup_preserves_existing_g7_without_changing_it():
    x = pd.DataFrame([row(adx_ready=False, adx_delta3_ready=False, rsi_ready=False,
                         adx14=np.nan, plus_di14=np.nan, minus_di14=np.nan,
                         adx_delta3=np.nan, rsi14=np.nan, signed_rsi_delta3=np.nan)])
    assert all(m.iloc[0] for m in policy.masks(x).values())
    x['all_g7_allowed'] = False
    assert not any(m.iloc[0] for m in policy.masks(x).values())


@pytest.mark.parametrize('change', [dict(adx14=np.nan), dict(rsi14=101), dict(side='OTHER'),
    dict(adx_ready='unknown'), dict(adx_ready=False, adx_delta3_ready=True),
    dict(adx_available_at='2026-11-04T10:32:00+05:30'),
    dict(feature_available_at='2026-11-04T10:32:00+05:30')])
def test_invalid_or_future_features_fail_closed(change):
    with pytest.raises(ValueError):
        policy.masks(pd.DataFrame([row(**change)]))


def test_outcomes_and_later_rows_cannot_change_admission():
    x = pd.DataFrame([row(), row(adx14=18)])
    before = policy.masks(x)
    x['portfolio_net_profit_rupees'] = [-1e12, 1e12]
    x['future_price'] = [0, 9999999]
    x.loc[2] = {**row(), 'portfolio_net_profit_rupees': 999999999, 'future_price': 1}
    after = policy.masks(x)
    for name in before:
        pd.testing.assert_series_equal(before[name], after[name].iloc[:2])


def test_later_results_do_not_choose_the_policy():
    base = dict(period='early_59', policy='baseline_v21_g7', trades=100,
        net_pnl_rs=1000, win_rate_pct=60, gross_loss_rs=500, daily_close_drawdown_rs=100)
    better = {**base, 'policy': 'adx20_di_rsi', 'trades': 80, 'net_pnl_rs': 800,
              'win_rate_pct': 65, 'gross_loss_rs': 300, 'daily_close_drawdown_rs': 90}
    late = {**base, 'period': 'later_6', 'policy': 'balanced_or', 'win_rate_pct': 100,
            'net_pnl_rs': 1e12}
    assert policy.select_policy(pd.DataFrame([base, better, late])) == 'adx20_di_rsi'
    late.update(win_rate_pct=0, net_pnl_rs=-1e12)
    assert policy.select_policy(pd.DataFrame([base, better, late])) == 'adx20_di_rsi'
    better['net_pnl_rs'] = 700
    assert policy.select_policy(pd.DataFrame([base, better, late])) == 'baseline_v21_g7'


def test_metrics_include_zero_days_and_initial_drawdown():
    fills = pd.DataFrame(dict(day=['2026-01-01','2026-01-02','2026-01-03'],
                              portfolio_net_profit_rupees=[-10.,20.,-5.]))
    m = run.metrics(fills, 4, ['2026-01-01','2026-01-02','2026-01-03','2026-01-04'])
    assert m['daily_close_drawdown_rs'] == 10
    assert m['gross_loss_rs'] == 15 and m['net_pnl_rs'] == 5
    assert m['negative_days'] == 2 and m['sessions'] == 4


def test_v23_reproduces_existing_adx_rsi_hypotheses():
    x = pd.read_csv(run.ADX_SOURCE / 'admission_audit.csv')
    masks = policy.masks(x)
    for new, existing in [('baseline_v21_g7', 'admit_all_g7'),
                          ('adx25_di_rsi50', 'admit_adx14_ge25_di_direction_rsi14_directional_50'),
                          ('rising_adx_di_rsi', 'admit_adx14_rising3_di_direction_rsi14_rising3')]:
        assert masks[new].equals(policy.flag(x[existing]))


def test_saved_results_reconcile_and_all_slots_remain_visible():
    folder = run.PACKAGE / 'results_through_20261104'
    config = json.loads((folder / 'config.json').read_text())
    manifest = json.loads((folder / 'manifest.json').read_text())
    for name, digest in manifest.items():
        assert run.sha(folder / name) == digest
    overall = pd.read_csv(folder / 'overall.csv')
    chosen = config['selected_policy']
    assert policy.select_policy(overall) == chosen
    slots = pd.read_csv(folder / 'slot_results.csv')
    assert slots.loc[slots.policy.eq(chosen), 'setup_id'].nunique() == 73
    trades = pd.read_csv(folder / 'v23_trades.csv')
    daily = pd.read_csv(folder / 'daily_results.csv')
    totals = overall.loc[overall.policy.eq(chosen) & overall.period.eq('full_65')].iloc[0]
    assert len(trades) == totals.trades
    assert np.isclose(trades.portfolio_net_profit_rupees.sum(), totals.net_pnl_rs)
    assert len(daily.loc[daily.policy.eq(chosen)]) == 65
    assert config['scope'] == 'BACKTEST_ONLY' and config['execution_authority'] is False


def test_existing_results_cannot_be_overwritten():
    with pytest.raises(ValueError, match='fresh output'):
        run.run(run.PACKAGE / 'results_through_20261104')
