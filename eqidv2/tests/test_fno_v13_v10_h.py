"""Offline H safety, execution, attribution and causal-context contracts."""
import copy
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_backtest as g
from ai_platform.research.v13h_execution import (
    ExecutionModel, daily_metrics, metrics, minute_equity, simulate_order,
    simulate_portfolio, tick_round, observed_prices,
)
from ai_platform.research.v13h_research import (
    context_features, coverage_audit, decision_audit, paired_validation, run, safe_child, load_source,
)


def order(sid=1, **changes):
    row = dict(sid=sid, day='2026-09-01', setup_id='0931_LONG', tradingsymbol=f'STOCK{sid}',
               side='LONG', confirmation_ts=pd.Timestamp('2026-09-01 09:31', tz='Asia/Kolkata'),
               trigger=100., native_stop_pct=1., native_target_pct=2.)
    row.update(changes)
    return row


def path(high=None, low=None, opening=None, close=None, periods=15):
    return dict(timestamp_ns=pd.date_range('2026-09-01 09:32', periods=periods, freq='min', tz='Asia/Kolkata').asi8,
                open=np.array(opening if opening is not None else [99.5]*periods, dtype=float),
                high=np.array(high if high is not None else [100.5]*periods, dtype=float),
                low=np.array(low if low is not None else [99.4]*periods, dtype=float),
                close=np.array(close if close is not None else [100.]*periods, dtype=float))


@pytest.mark.parametrize('delay', [0, 1, 9, 10, 20])
def test_expiry_is_absolute_not_reset_by_activation_delay(delay):
    high = [99.9]*10 + [101.]*5
    result = simulate_order(order(), path(high=high, close=[99.7]*15), ExecutionModel(delay_minutes=delay), None)
    assert result['status'] == 'UNFILLED_TRIGGER'


def test_last_entry_minute_is_inclusive_and_delay_skips_early_touch():
    high = [99.9]*9 + [100.5] + [99.9]*5
    p = path(high=high, close=[99.7]*15)
    result = simulate_order(order(), p, ExecutionModel(delay_minutes=9), None)
    assert result['entry_index'] == 9
    assert result['entry_ts'] == result['deadline']
    assert simulate_order(order(), p, ExecutionModel(delay_minutes=10), None)['quantity'] == 0


def test_entry_gap_and_stop_first_same_bar():
    p = path(opening=[101.]+[100.]*14, high=[104.]+[100.5]*14, low=[99.]*15)
    result = simulate_order(order(), p, ExecutionModel(), None)
    assert result['entry_price'] == 101.
    assert result['exit_reason'] == 'STOP'
    assert result['same_bar_ambiguous']
    assert result['exit_price'] <= 99.99


def test_later_stop_gap_uses_adverse_open():
    p = path(opening=[100., 95.]+[100.]*13, high=[100.5, 96.]+[100.5]*13,
             low=[99.5, 94.]+[99.5]*13, close=[100., 95.]+[100.]*13)
    result = simulate_order(order(), p, ExecutionModel(), None)
    assert result['exit_price'] == 95.
    assert result['exit_index'] == 1


@pytest.mark.parametrize('side', ['LONG', 'SHORT'])
def test_integer_quantity_exposure_and_planned_risk_caps(side):
    r = simulate_order(order(side=side), path(), ExecutionModel(), 3000.)
    assert isinstance(r['quantity'], int)
    assert r['exposure_rupees'] <= 500000.
    assert r['planned_risk_rupees'] <= 3000.
    assert r['cost_rupees'] == pytest.approx(r['quantity'] * r['entry_price'] * .0005)
    assert r['net_profit_rupees'] == pytest.approx(r['gross_profit_rupees']-r['cost_rupees'])


def test_worse_costs_reduce_fixed_exposure_result():
    r = simulate_order(order(), path(), ExecutionModel(), None)
    worse = simulate_order(order(), path(), ExecutionModel(cost_bps=10), None)
    assert worse['net_profit_rupees'] < r['net_profit_rupees']


def test_tick_rounding_is_directional():
    assert tick_round(100.021, .05, True) == 100.05
    assert tick_round(100.021, .05, False) == 100.
    assert tick_round(100.05, .05, True) == 100.05
    assert tick_round(100.05, .05, False) == 100.05
    assert tick_round(100.05-1e-14, .05, False) == 100.05
    assert tick_round(100.05+1e-14, .05, True) == 100.05


def test_float32_storage_noise_does_not_add_a_trigger_tick_or_remove_fill():
    trigger = float(np.float32(118.9))
    result = simulate_order(order(trigger=trigger),
                            path(opening=[118.8]*15, high=[trigger]*15, low=[118.7]*15, close=[118.8]*15),
                            ExecutionModel(), None)
    assert result['trigger'] == 118.9
    assert result['quantity'] > 0
    assert observed_prices([118.91], .05)[0] == 118.91
    assert observed_prices([float(np.float32(592.05))], .05)[0] == 592.05


@pytest.mark.parametrize('change', [dict(cost_bps=float('nan')), dict(tick_size=0),
                                  dict(delay_minutes=-1), dict(delay_minutes=1.5),
                                  dict(entry_expiry_minutes=11), dict(leverage=0)])
def test_invalid_model_rejected(change):
    with pytest.raises(ValueError):
        ExecutionModel(**change).validate()


def test_inputs_unchanged_and_control_equality():
    rows = pd.DataFrame([order()])
    before = rows.copy(deep=True)
    paths = {1: path()}
    snapshot = copy.deepcopy(paths)
    control = simulate_portfolio(rows, paths, ExecutionModel())
    same = simulate_portfolio(rows, paths, ExecutionModel(), None)
    pd.testing.assert_frame_equal(control, same)
    pd.testing.assert_frame_equal(rows, before)
    for key in snapshot[1]:
        np.testing.assert_array_equal(paths[1][key], snapshot[1][key])


def test_no_orders_has_explicit_zero_days_and_no_invented_win_rate():
    rows = pd.DataFrame(columns=['sid', 'setup_id'])
    ledger = simulate_portfolio(rows, {}, ExecutionModel())
    daily = daily_metrics(ledger, ['2026-09-01'])
    marks = minute_equity(ledger, {})
    stats = metrics(ledger, daily, marks)
    assert stats['selected_orders'] == stats['executed_trades'] == 0
    assert stats['profit_factor'] is None
    assert stats['win_rate_pct'] is None
    assert daily.net_profit_rupees.tolist() == [0.]


def test_missing_forward_minute_is_not_silently_skipped():
    p = path()
    p['timestamp_ns'][4:] += pd.Timedelta(minutes=1).value
    with pytest.raises(ValueError, match='minute'):
        simulate_order(order(), p, ExecutionModel(), None)


def test_portfolio_capital_constraint_and_minute_equity():
    rows = pd.DataFrame([order(1), order(2)])
    paths = {1: path(), 2: path()}
    ledger = simulate_portfolio(rows, paths, ExecutionModel(portfolio_capital=100000.))
    assert ledger.status.tolist() == ['EXECUTED', 'CAPITAL_REJECTED']
    assert ledger.net_profit_rupees.iloc[1] == 0
    marks = minute_equity(ledger, paths)
    assert marks.equity_pnl_rupees.iloc[-1] == pytest.approx(ledger.net_profit_rupees.sum())
    days = daily_metrics(ledger, ['2026-09-01', '2026-09-02'])
    assert days.trades.tolist() == [1, 0]
    assert days.net_profit_rupees.iloc[1] == 0
    stats = metrics(ledger, days, marks)
    assert stats['executed_trades'] == 1
    assert stats['daily_close_drawdown_rupees'] >= 0


def test_same_minute_capital_not_recycled_using_unknown_intrabar_order():
    rows = pd.DataFrame([order(1), order(2)])
    p = path(high=[103.]*15)
    ledger = simulate_portfolio(rows, {1: p, 2: p}, ExecutionModel(portfolio_capital=100000.))
    assert ledger.status.tolist() == ['EXECUTED', 'CAPITAL_REJECTED']


def bars():
    days = pd.date_range('2026-08-01', periods=25)
    stamps = pd.DatetimeIndex([pd.Timestamp(str(day.date())+' 09:25', tz='Asia/Kolkata') for day in days])
    return pd.DataFrame(dict(day=days.date, signal_ts=stamps, tradingsymbol=['TEST']*25,
                             contract_month=['26AUG']*25, hhmm_int=[925]*25, open=[100.]*25,
                             high=np.arange(101., 126.), low=[99.]*25, close=np.arange(100.,125.),
                             prev_close=np.arange(99.,124.), volume=np.arange(10.,35.),
                             oi_change_pct=[.1]*25, v9_5m_vwap=[100.]*25,
                             v9_5m_feature_ts=stamps, price_change_pct=[.1]*25))


def test_context_is_causal_same_clock_excludes_current_and_missing_is_null():
    frame = bars()
    actual = context_features(frame)
    assert pd.isna(actual.same_clock_rvol_prior20_min5.iloc[4])
    assert actual.same_clock_rvol_prior20_min5.iloc[5] == pytest.approx(15/12)
    assert actual.india_vix.isna().all()
    assert actual.india_vix_status.eq('UNAVAILABLE').all()
    changed = frame.copy()
    changed.loc[24, ['high', 'volume', 'oi_change_pct']] = [1000., 10000., 50.]
    pd.testing.assert_frame_equal(actual.iloc[:24], context_features(changed).iloc[:24])
    assert actual.oi_acceleration_5m.isna().all()  # overnight gaps are not 5m acceleration


def test_future_context_timestamp_refused():
    frame = bars()
    frame.loc[0, 'v9_5m_feature_ts'] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match='Noncausal'):
        context_features(frame)


def audit_rows():
    stamp = pd.Timestamp('2026-09-01 09:30', tz='Asia/Kolkata')
    rows = []
    for sid, volume in [(1, 1.3), (2, 1.0), (3, np.nan), (4, 1.3)]:
        rows.append(dict(sid=sid, day='2026-09-01', hhmm_int=930, setup_id='0931_SHORT', side='SHORT',
                         tradingsymbol=f'STOCK{sid}', price_change_pct=-.3, oi_change_pct=.13,
                         volume_ratio=2., body_ratio=.6, wick_ratio=.2, traded_value=1000000.,
                         v9_1m_volume_ratio=volume, signal_ts=stamp, confirmation_ts=stamp+pd.Timedelta(minutes=1),
                         v9_5m_feature_ts=stamp, v9_1m_feature_ts=stamp+pd.Timedelta(minutes=1),
                         v9_feature_available_ts=stamp+pd.Timedelta(minutes=1), oi=100., prev_oi=99.,
                         signal_close=100., confirmation_open=100., confirmation_close=99.,
                         confirmation_high=100.1, confirmation_low=98.9,
                         v9_5m_ema9=99., v9_5m_ema20=100., v9_5m_ema50=101., check_5m_ema_stack=True))
    return pd.DataFrame(rows)


def test_audit_retains_missing_low_volume_and_ranked_out_without_selection_change():
    raw = audit_rows()
    settings = {'selection_change': dict(price_multiplier=.65, expansion_side='SHORT')}
    selection = g.selection_audit(raw.drop(columns='setup_id'), g.v9.V9Config(), g.SelectionChange(**settings['selection_change']))
    result = decision_audit(raw, selection, settings).set_index('sid')
    assert result.h_state.to_dict() == {1: 'SELECTED', 2: 'GATE_FAILED', 3: 'DATA_MISSING', 4: 'RANKED_OUT'}
    assert result.loc[2, 'h_margin_volume_1m'] == pytest.approx(-.2)
    assert result.loc[1, 'h_margin_price'] == pytest.approx(.17)
    shuffled = raw.sample(frac=1, random_state=17)
    tied = g.select_orders(shuffled.drop(columns='setup_id'), g.v9.V9Config(), g.SelectionChange(**settings['selection_change']))
    assert tied.sid.tolist() == [1]


def test_coverage_preserves_excluded_and_zero_trade_sessions():
    audit = audit_rows()
    audit['h_state'] = 'GATE_FAILED'
    eligibility = pd.DataFrame([dict(day='2026-09-01', universe_size=5, eligible=True),
                                dict(day='2026-09-02', universe_size=5, eligible=False)])
    result = coverage_audit(audit, eligibility, ['2026-09-01'])
    assert result.missing_expected_count.tolist() == [1, 5]
    assert result.included_in_comparison.tolist() == [True, False]


def test_chronological_folds_use_whole_sessions_no_false_holdout():
    days = pd.date_range('2026-08-01', periods=38).strftime('%Y-%m-%d').tolist()
    result = paired_validation(pd.DataFrame(dict(day=days, g_net_rupees=[1.]*38,
                                                h_net_rupees=[2.]*38, delta_rupees=[1.]*38)))
    assert len(result['folds']) == 4
    assert all(x['train_through'] < x['test_from'] for x in result['folds'])
    assert result['prospective_sessions'] == 0
    assert not result['promotion_eligible']
    assert result['exploratory_day_block_bootstrap_mean_delta_95pct'] == [1., 1.]


def test_output_guards_and_unrecognized_experiment(tmp_path):
    with pytest.raises(ValueError, match='inside'):
        run(tmp_path, tmp_path/'outputs')
    with pytest.raises(ValueError, match='preregistered'):
        run(tmp_path/'source', tmp_path/'output', experiment='optimize')
    with pytest.raises(ValueError, match='run ID'):
        run(tmp_path/'source', tmp_path/'output', run_id='../bad')
    with pytest.raises(ValueError, match='Both'):
        run(tmp_path/'source', tmp_path/'output', holdout_start='2099-01-01')
    with pytest.raises(ValueError, match='future'):
        run(tmp_path/'source', tmp_path/'output', holdout_start='2001-01-01', holdout_end='2001-02-01')
    with pytest.raises(ValueError, match='outside'):
        safe_child(tmp_path, '../escape')
    (tmp_path/'output/runs/existing').mkdir(parents=True)
    with pytest.raises(FileExistsError):
        run(tmp_path/'source', tmp_path/'output', run_id='existing')


def test_source_bundle_requires_explicit_complete_state_and_inventory(tmp_path):
    import json
    manifest = tmp_path/'bundle_manifest.json'
    manifest.write_text(json.dumps({'state': 'RUNNING'}))
    with pytest.raises(ValueError, match='COMPLETE'):
        load_source(tmp_path)
    manifest.write_text(json.dumps({'state': 'COMPLETE', 'artifacts': {}}))
    with pytest.raises(ValueError, match='required artifacts'):
        load_source(tmp_path)


def test_source_checksum_mismatch_blocks_before_any_simulation(tmp_path):
    import json
    names = ['dataset/signals.parquet', 'dataset/paths.npz', 'dataset/dataset_manifest.json',
             'dataset/setup_audit.parquet', 'dataset/all_5m_features.parquet', 'dataset/eligibility.parquet',
             'dataset/path_quality.parquet', 'g_backtest/portfolio_trades.csv',
             'g_backtest/summary.json', 'g_backtest/run_metadata.json']
    inventory = {name: {'sha256': '0'*64} for name in names}
    (tmp_path/'bundle_manifest.json').write_text(json.dumps({'state': 'COMPLETE', 'artifacts': inventory}))
    with pytest.raises(ValueError, match='checksum mismatch'):
        load_source(tmp_path)
