"""Risk reports must retain empty sessions and avoid simultaneous-fill peaks."""
import numpy as np
import pandas as pd
import pytest

from research.g3_trade_monitor_risk import breakdowns, risk_diagnostics


def ledger():
    return pd.DataFrame([
        dict(day="2026-08-03", portfolio_executed="True", portfolio_net_profit_rupees=100,
             exit_ts="2026-08-03 10:00:00+05:30", tradingsymbol="A", setup_id="one", side="LONG"),
        dict(day="2026-08-03", portfolio_executed="true", portfolio_net_profit_rupees=-100,
             exit_ts="2026-08-03 04:30:00Z", tradingsymbol="B", setup_id="two", side="SHORT"),
        dict(day="2026-08-04", portfolio_executed="False", portfolio_net_profit_rupees=np.nan,
             exit_ts=None, tradingsymbol="C", setup_id="one", side="LONG"),
        dict(day="2026-08-05", portfolio_executed=1, portfolio_net_profit_rupees=-30,
             exit_ts="2026-08-05 10:00:00+05:30", tradingsymbol="A", setup_id="one", side="LONG"),
        dict(day="2026-08-06", portfolio_executed=True, portfolio_net_profit_rupees=50,
             exit_ts="2026-08-06 10:00:00+05:30", tradingsymbol="A", setup_id="one", side="LONG"),
    ])


def test_tied_exits_settle_atomically_and_row_order_does_not_change_risk():
    expected = risk_diagnostics(ledger())
    assert expected == risk_diagnostics(ledger().sample(frac=1, random_state=17))
    assert expected["realized_exit_drawdown_rs"] == 30
    assert expected["daily_close_drawdown_rs"] == 30
    assert expected["total_days"] == 4
    assert expected["positive_days"] == 1
    assert expected["executed_trades"] == 4
    assert expected["wins"] == 2
    assert expected["losses"] == 2
    assert expected["top5_day_profit_share_pct"] == 250
    assert expected["top5_symbol_profit_share_pct"] == 600
    assert expected["mark_to_market_available"] is False


def test_realized_drawdown_differs_from_daily_drawdown():
    data = ledger().iloc[:2].copy()
    data.loc[1, "exit_ts"] = "2026-08-03 11:00:00+05:30"
    actual = risk_diagnostics(data)
    assert actual["daily_close_drawdown_rs"] == 0
    assert actual["realized_exit_drawdown_rs"] == 100
    assert actual["top5_day_profit_share_pct"] is None


def test_realized_curve_uses_actual_fills_instead_of_capital_reservation_times():
    data = ledger().iloc[:2].copy()
    data.loc[1, "exit_ts"] = "2026-08-03 10:01:00+05:30"
    data["exit_execution_ts"] = "2026-08-03 10:00:00+05:30"
    assert risk_diagnostics(data)["realized_exit_drawdown_rs"] == 0


def test_invalid_explicit_fill_timestamp_does_not_fall_back_silently():
    data = ledger()
    data["exit_execution_ts"] = None
    data.loc[0, "exit_execution_ts"] = "invalid"
    with pytest.raises(ValueError):
        risk_diagnostics(data)


@pytest.mark.parametrize("column,value", [
    ("portfolio_executed", "maybe"), ("portfolio_executed", None),
    ("portfolio_net_profit_rupees", np.inf), ("portfolio_net_profit_rupees", np.nan),
    ("exit_ts", "invalid"), ("day", None), ("day", "NaT"),
])
def test_invalid_executed_data_fails_instead_of_omitting_risk(column, value):
    data = ledger()
    data.loc[0, column] = value
    with pytest.raises(ValueError):
        risk_diagnostics(data)


def test_empty_ledger_has_no_observed_days_or_positive_profit_share():
    actual = risk_diagnostics(pd.DataFrame())
    assert actual["total_days"] == actual["executed_trades"] == 0
    assert actual["daily_close_drawdown_rs"] == actual["realized_exit_drawdown_rs"] == 0
    assert actual["top5_symbol_profit_share_pct"] is None
    assert breakdowns(pd.DataFrame())["period"].executed_trades.iloc[0] == 0


def test_breakdowns_reconcile_to_the_portfolio_and_do_not_mutate_input():
    data = ledger()
    original = data.copy(deep=True)
    result = breakdowns(data)
    for key in ("period", "monthly", "setup", "symbol", "side"):
        assert result[key].net_pnl_rs.sum() == 20
        assert result[key].executed_trades.sum() == 4
    assert result["monthly"].total_days.iloc[0] == 4
    pd.testing.assert_frame_equal(data, original)
