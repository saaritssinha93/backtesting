"""G9 reporting controls: attribution, available-day metrics, isolation."""
import pandas as pd
import pytest

from research import g9_backtest as backtest


def ledger(rows):
    return pd.DataFrame([dict(sid=sid, day=day, portfolio_executed=executed,
        portfolio_net_profit_rupees=pnl, portfolio_gross_profit_rupees=pnl+cost if executed else 0.,
        portfolio_cost_rupees=cost if executed else 0., exposure_per_entry_rupees=exposure,
        g9_effective_quantity_scale=scale, filled=executed, g8_baseline_filled=True,
        exit_reason='TIME_EXIT') for sid,day,executed,pnl,cost,exposure,scale in rows])


def test_attribution_separates_removed_winners_losers_size_and_new_entries():
    day = '2026-10-23'
    # Remove a winner and loser; halve another winner and loser; add a new loser.
    base = ledger([(1,day,True,100.,5.,1000.,1.),(2,day,True,-80.,5.,1000.,1.),
                   (3,day,True,60.,5.,1000.,1.),(4,day,True,-40.,5.,1000.,1.),
                   (5,day,False,0.,0.,1000.,1.)])
    new = ledger([(1,day,False,0.,0.,1000.,1.),(2,day,False,0.,0.,1000.,1.),
                  (3,day,True,30.,2.5,500.,.5),(4,day,True,-20.,2.5,500.,.5),
                  (5,day,True,-10.,2.5,500.,.5)])
    result = backtest.attribution(base,new,[day])
    assert result['delta_net_rs'] == -40.
    assert result['removed_trades'] == 2
    assert result['removed_winners'] == result['removed_losers'] == 1
    assert result['loss_value_reduction_rs'] == 100.
    assert result['winner_profit_surrendered_rs'] == 130.
    assert result['retained_trade_pnl_change_rs'] == -10.
    assert result['newly_allocated_trades'] == 1
    assert result['newly_allocated_pnl_rs'] == -10.
    assert result['reconciliation_error_rs'] == 0.
    assert (result['loss_value_reduction_rs']-result['winner_profit_surrendered_rs']
            +result['newly_allocated_pnl_rs']) == result['delta_net_rs']


def test_month_restriction_does_not_leak_other_periods_into_attribution():
    base = ledger([(1,'2026-09-30',True,100.,5.,1000.,1.),
                   (2,'2026-10-23',True,-50.,5.,1000.,1.)])
    new = ledger([(1,'2026-09-30',False,0.,0.,1000.,1.),
                  (2,'2026-10-23',True,-25.,2.5,500.,.5)])
    result = backtest.attribution(base,new,['2026-10-23'])
    assert result['removed_trades'] == 0
    assert result['winner_profit_surrendered_rs'] == 0.
    assert result['loss_value_reduction_rs'] == result['delta_net_rs'] == 25.


def test_no_trade_sessions_count_as_flat_not_positive_and_exposure_is_executed_only():
    book = ledger([(1,'2026-10-22',True,1000.,25.,50_000.,.5),
                   (2,'2026-10-22',False,0.,0.,100_000.,1.)])
    result = backtest.enriched_metrics(book,['2026-10-22','2026-10-23'])
    assert result['sessions'] == 2
    assert result['trades'] == 1
    assert result['flat_days'] == 1
    assert result['profitable_days'] == 1
    assert result['total_traded_exposure_rs'] == 50_000.
    assert result['net_pnl_per_lakh_exposure_rs'] == 2000.
    assert result['net_return_on_fixed_10lakh_pct'] == .1
    assert result['mean_executed_quantity_scale'] == .5
    assert result['daily_close_drawdown_rs'] == 0.


def test_zero_trade_metrics_are_finite_and_report_zero_profit():
    book = ledger([(1,'2026-10-23',False,0.,0.,100_000.,0.)])
    result = backtest.enriched_metrics(book,['2026-10-23'])
    for column in ('trades','wins','losses','net_pnl_rs','total_traded_exposure_rs',
                   'net_pnl_per_lakh_exposure_rs','mean_executed_quantity_scale'):
        assert result[column] == 0.
    assert result['flat_days'] == 1
    assert result['losing_days'] == 0


def test_descriptor_is_backtest_only_and_contains_explicit_rollback():
    value = backtest.definition()
    assert value['scope'] == 'BACKTEST_ONLY'
    assert value['execution_authority'] is False
    assert value['rollback_policy'] == 'g8_baseline'
    assert value['output_root'].endswith('backtesting_result_v13_v10_g_9')
    assert len(value['experiments']) == 13
    assert value['risk_permission']['quantity_scales_by_warning_count'] == [1.,.5,.25]
    assert value['risk_permission']['rv_min_history_sessions'] == 20


def test_invalid_scope_descriptor_is_rejected(monkeypatch):
    descriptor = backtest.definition()
    descriptor['execution_authority'] = True
    monkeypatch.setattr(backtest,'read',lambda path:descriptor)
    with pytest.raises(ValueError,match='backtest-only'):
        backtest.definition()


def test_duplicate_experiments_are_rejected(monkeypatch):
    descriptor = backtest.definition()
    descriptor['experiments'].append(descriptor['experiments'][0].copy())
    monkeypatch.setattr(backtest,'read',lambda path:descriptor)
    with pytest.raises(ValueError,match='Duplicate experiment'):
        backtest.definition()
