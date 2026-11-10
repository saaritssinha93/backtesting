"""Report arithmetic and decision-schema checks independent of market results."""
import pandas as pd
import pytest

from research import g10_backtest as report


def ledger(pnls, days=None, executed=None):
    days = days or [f'2026-10-{i+1:02}' for i in range(len(pnls))]
    executed = executed or [True]*len(pnls)
    return pd.DataFrame([
        dict(sid=i+1, day=day, filled=True, g8_baseline_filled=True,
             portfolio_executed=take, portfolio_net_profit_rupees=pnl if take else 0.,
             portfolio_gross_profit_rupees=pnl+10 if take else 0.,
             portfolio_cost_rupees=10. if take else 0.,
             portfolio_trade_initial_risk_rupees=100., portfolio_trade_exposure_rupees=1000.,
             exit_reason='TARGET' if pnl>0 else 'STOP')
        for i,(day,pnl,take) in enumerate(zip(days,pnls,executed))])


def test_zero_start_drawdown_includes_the_first_loss_in_both_schema_fields():
    book = ledger([-100.,40.,-25.])
    result = report.stats(book, ['2026-10-01','2026-10-02','2026-10-03','2026-10-04'])
    assert result['net_pnl_rs'] == -85.
    assert result['daily_close_drawdown_rs'] == result['max_drawdown_rs'] == 100.
    assert result['sessions'] == 4
    assert (result['trades'],result['wins'],result['losses'],result['flat_days']) == (3,1,2,1)
    assert result['gross_pnl_rs']-result['cost_rs'] == result['net_pnl_rs']
    assert result['negative_trade_sum_abs_rs'] == 125.
    assert result['expectancy_rs'] == pytest.approx(-85/3)


def test_single_losing_day_is_not_zero_drawdown():
    result = report.stats(ledger([-600.]), ['2026-10-01'])
    assert result['daily_close_drawdown_rs'] == result['max_drawdown_rs'] == 600.


def test_zero_trade_day_keeps_calendar_without_counting_rejected_pnl():
    result = report.stats(ledger([-999.], executed=[False]), ['2026-10-01'])
    assert (result['sessions'],result['trades'],result['flat_days']) == (1,0,1)
    assert result['net_pnl_rs'] == result['daily_close_drawdown_rs'] == 0.
    assert result['total_initial_risk_rs'] == result['total_exposure_rs'] == 0.
    assert result['profit_factor'] is None
    assert result['net_per_initial_risk'] is None


def test_missed_winners_and_new_allocations_are_sid_aligned():
    control = ledger([100.,-50.,20.], executed=[True,True,False])
    candidate = ledger([100.,-50.,20.], executed=[False,False,True]).iloc[::-1]
    result = report.compare_rows(candidate, control, ['2026-10-01','2026-10-02','2026-10-03'])
    assert result == dict(changed_days=3, changed_trades=3, avoided_baseline_losers=1,
                          missed_baseline_winners=1, new_allocations=1)


def test_acceptance_rejects_lower_losses_bought_by_losing_profit_and_winners():
    base = dict(net_pnl_rs=1000.,negative_trade_sum_abs_rs=100.,daily_close_drawdown_rs=50.,
                trades=100,wins=80)
    candidate = dict(net_pnl_rs=600.,negative_trade_sum_abs_rs=50.,daily_close_drawdown_rs=60.,
                     trades=60,wins=50)
    assert report.acceptance(candidate,base,10) == [
        'net profit decreased','daily-close drawdown increased','trade retention below 80%',
        'winner retention below 90%']


def test_numeric_acceptance_is_not_declared_independent_validation():
    config = report.read(report.CONFIG)
    assert config['primary_candidate'] == 'g10_combined'
    assert config['selection'] == 'FIXED_BEFORE_RUN_NO_OPTIMIZATION_NO_AUTOMATIC_PROMOTION'
    assert config['scope'] == 'BACKTEST_ONLY' and config['execution_authority'] is False
    assert 'REQUIRED_FOR_ANY_PROMOTION' in config['acceptance']['independent_validation']
