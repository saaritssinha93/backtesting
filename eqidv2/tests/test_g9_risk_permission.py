import numpy as np
import pandas as pd
import pytest

from research.g9_risk_permission import permission_timeline, entry_permissions, allocate

SETTINGS = dict(recovery_observations=2, rv_ratio_threshold=1.5,
                vix_5m_abs_threshold_pct=1., quantity_scales_by_warning_count=[1., .5, .25])
SPEC = dict(id='test', mode='both', recovery=2)


def grid(rv, vix=None, ready=None):
    n = len(rv)
    return pd.DataFrame(dict(day=['2026-10-23']*n,
        available_at=pd.date_range('2026-10-23 09:20', periods=n, freq='5min', tz='Asia/Kolkata'),
        rv_ratio=rv, vix_change_5m_pct=vix or [0.]*n,
        features_ready=ready or [True]*n))


def test_thresholds_and_two_bar_recovery():
    m = grid([1.5, 1.6, 1.6, 1.4, 1.4], [0., 1., 0., 0., 0.])
    result = permission_timeline(m, SPEC, SETTINGS)
    assert result.quantity_scale.tolist() == [1., .25, .25, .5, 1.]


def test_reductions_are_immediate_and_missing_is_not_green():
    m = grid([1., 1.6, np.nan, 1., 1.], [0., -1., 0., 0., 0.], [True, True, False, True, True])
    assert permission_timeline(m, SPEC, SETTINGS).quantity_scale.tolist() == [1., .25, 0., 0., 1.]


def test_dropped_clock_restarts_recovery():
    m = grid([1., 1., 1., 1.]).drop(index=1)
    assert permission_timeline(m, SPEC, SETTINGS).quantity_scale.tolist() == [1., 0., 1.]


def test_first_observation_can_be_reduced():
    assert permission_timeline(grid([1.6], [1.]), SPEC, SETTINGS).quantity_scale.tolist() == [.25]


def test_future_rows_cannot_change_previous_permissions():
    m = grid([1.6, 1.6, 1., 1., 4.], [1., 0., 0., 0., 3.])
    expected = permission_timeline(m.iloc[:3], SPEC, SETTINGS).quantity_scale
    actual = permission_timeline(m, SPEC, SETTINGS).quantity_scale.iloc[:3]
    pd.testing.assert_series_equal(expected, actual)


def test_asof_before_entry_not_future_fill_and_staleness():
    m = permission_timeline(grid([1., 2., 2.]), SPEC, SETTINGS)
    b = pd.DataFrame(dict(sid=[1, 2], day=['2026-10-23']*2,
        confirmation_ts=['2026-10-23 09:21:00+05:30', '2026-10-23 09:35:00+05:30']))
    d = entry_permissions(b, m)
    assert d.loc[1, 'quantity_scale'] == 1.
    assert d.loc[2, 'quantity_scale'] == 0.
    assert d.loc[1, 'available_at'] <= d.loc[1, 'confirmation_ts']


def test_unknown_mode_and_duplicate_observations_fail():
    with pytest.raises(ValueError):
        permission_timeline(grid([1.]), dict(id='x', mode='unknown'), SETTINGS)
    m = grid([1., 1.])
    m.loc[1, 'available_at'] = m.loc[0, 'available_at']
    with pytest.raises(ValueError):
        permission_timeline(m, SPEC, SETTINGS)


def test_csv_false_is_not_truthy():
    m = grid([1.])
    m['features_ready'] = 'False'
    assert permission_timeline(m, SPEC, SETTINGS).quantity_scale.iloc[0] == 0.


CONFIG = dict(capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.,
              model_cost_bps_round_trip=5.)


def candidate_book():
    rows = []
    for sid in (10, 20):
        rows.append(dict(sid=sid, day='2026-10-23', setup_id='1001_LONG',
            tradingsymbol=f'STOCK{sid}', side='LONG', filled=True, g8_baseline_filled=True,
            g8_direction_allowed=True, g8_quantity_scale=1., portfolio_executed=(sid == 10),
            entry_price=100., entry_path_index=0, gross_return_pct=1., net_return_pct=.95,
            cost_pct=.05, initial_stop_pct=1.2, stop_pct=1.2, target_pct=1.,
            entry_ts=pd.Timestamp(f'2026-10-23 10:{1 if sid == 10 else 2:02d}', tz='Asia/Kolkata'),
            confirmation_ts=pd.Timestamp('2026-10-23 10:00', tz='Asia/Kolkata'),
            exit_ts=pd.Timestamp('2026-10-23 11:00', tz='Asia/Kolkata'),
            exit_reason='TARGET', exit_price=101., picker='max_liquidity', traded_value=100_000.,
            strategy_fingerprint='old-g8-fingerprint'))
    return pd.DataFrame(rows)


def permissions(scales):
    return pd.DataFrame(dict(quantity_scale=scales, permission_reason=['READY']*len(scales)),
                        index=pd.Index([10, 20][:len(scales)], name='sid'))


def test_money_and_cost_are_scaled_once_and_exits_are_unchanged():
    original = candidate_book()
    frozen = original.copy(deep=True)
    decision = permissions([.5, .25]).iloc[::-1]
    result = allocate(original, decision, CONFIG, 'scaled')
    assert result.portfolio_executed.tolist() == [True, True]
    assert result.exposure_per_entry_rupees.tolist() == pytest.approx([250_000., 125_000.])
    assert result.portfolio_gross_profit_rupees.tolist() == pytest.approx([2500., 1250.])
    assert result.portfolio_cost_rupees.tolist() == pytest.approx([125., 62.5])
    assert result.portfolio_net_profit_rupees.tolist() == pytest.approx([2375., 1187.5])
    assert result.g9_model_quantity.tolist() == pytest.approx([2500., 1250.])
    assert result.portfolio_trade_initial_risk_rupees.tolist() == pytest.approx([3125., 1562.5])
    for column in ('entry_ts', 'exit_ts'):
        pd.testing.assert_series_equal(pd.to_datetime(result[column], utc=True), pd.to_datetime(original[column], utc=True))
    for column in ('entry_price', 'exit_price', 'stop_pct', 'target_pct', 'gross_return_pct'):
        pd.testing.assert_series_equal(result[column], original[column])
    pd.testing.assert_frame_equal(original, frozen)


def test_capital_allocator_reconsiders_native_fills_rejected_in_source():
    book = candidate_book()
    small_portfolio = dict(CONFIG, portfolio_capital_rs=100_000.)
    full = allocate(book, permissions([1., 1.]), small_portfolio, 'full')
    half = allocate(book, permissions([.5, .5]), small_portfolio, 'half')
    assert full.portfolio_executed.tolist() == [True, False]
    assert half.portfolio_executed.tolist() == [True, True]
    assert half.g9_original_baseline_executed.tolist() == [True, False]
    assert half.g8_baseline_filled.tolist() == [True, True]
    assert half.portfolio_net_profit_rupees.sum() == pytest.approx(full.portfolio_net_profit_rupees.sum())


def test_zero_permission_blocks_without_losing_original_native_fill():
    result = allocate(candidate_book(), permissions([0., .25]), CONFIG, 'zero')
    assert result.g8_baseline_filled.tolist() == [True, True]
    assert result.g9_original_direction_allowed.tolist() == [True, True]
    assert result.portfolio_executed.tolist() == [False, True]
    assert result.loc[0, 'portfolio_status'] == 'G9_RISK_PERMISSION_BLOCKED'
    assert result.loc[0, 'portfolio_net_profit_rupees'] == 0.
    assert result.loc[0, 'portfolio_cost_rupees'] == 0.
    assert result.loc[0, 'g9_model_quantity'] == 0.


def test_sizing_cannot_override_g8_gate_or_create_unfilled_trade():
    book = candidate_book()
    book['g8_direction_allowed'] = ['False', 'True']
    book['g8_baseline_filled'] = ['True', 'False']
    book['filled'] = ['False', 'False']
    result = allocate(book, permissions([1., 1.]), CONFIG, 'permission')
    assert result.portfolio_executed.tolist() == [False, False]
    assert result.g9_original_direction_allowed.tolist() == [False, True]
    assert result.g8_baseline_filled.tolist() == [True, False]
    assert result.portfolio_net_profit_rupees.sum() == 0.


def test_whole_share_rounding_never_increases_allowed_quantity():
    book = candidate_book()
    book['entry_price'] = [333., 600_000.]
    decisions = permissions([.25, 1.])
    fractional = allocate(book, decisions, CONFIG, 'fractional')
    whole = allocate(book, decisions, CONFIG, 'whole', whole_shares=True)
    assert whole.g9_model_quantity.tolist() == pytest.approx([375., 0.])
    assert (whole.g9_effective_quantity_scale <= whole.g9_requested_quantity_scale).all()
    assert (whole.g9_model_quantity <= fractional.g9_model_quantity).all()
    assert whole.portfolio_executed.tolist() == [True, False]
    assert whole.loc[0, 'exposure_per_entry_rupees'] == pytest.approx(375.*333.)
    assert whole.loc[0, 'portfolio_cost_rupees'] == pytest.approx(375.*333.*5./10_000.)


def test_higher_cost_sensitivity_changes_only_cost_and_net_accounting():
    book = candidate_book()
    decisions = permissions([.5, .25])
    five = allocate(book, decisions, CONFIG, 'sensitivity', cost_bps=5.)
    ten = allocate(book, decisions, CONFIG, 'sensitivity', cost_bps=10.)
    assert ten.portfolio_gross_profit_rupees.tolist() == pytest.approx(five.portfolio_gross_profit_rupees.tolist())
    assert ten.portfolio_cost_rupees.tolist() == pytest.approx([250., 125.])
    assert (five.portfolio_net_profit_rupees-ten.portfolio_net_profit_rupees).tolist() == pytest.approx([125., 62.5])
    assert not five.strategy_fingerprint.equals(ten.strategy_fingerprint)


def test_g9_identity_retains_source_and_has_no_execution_authority():
    result = allocate(candidate_book(), permissions([1., 1.]), CONFIG, 'identity')
    assert result.strategy.eq('V13-V10-G-9').all()
    assert result.strategy_version.eq('V13-V10-G-9-G8-RISK-PERMISSION-V1').all()
    assert result.scope.eq('BACKTEST_ONLY').all()
    assert result.execution_authority.eq(False).all()
    assert result.source_strategy_fingerprint.eq('old-g8-fingerprint').all()
    assert result.strategy_fingerprint.str.len().eq(64).all()


@pytest.mark.parametrize('bad', [-.1, np.nan, np.inf, 1.01])
def test_invalid_quantity_permission_fails(bad):
    with pytest.raises(ValueError):
        allocate(candidate_book(), permissions([bad, .5]), CONFIG, 'invalid')


def test_whole_share_flag_is_not_a_truthy_string():
    with pytest.raises(ValueError):
        allocate(candidate_book(), permissions([1., 1.]), CONFIG, 'invalid', whole_shares='False')
