from __future__ import annotations

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v13_v5_options_backtest as opt


def _coverage_row(**overrides):
    base = {
        "trade_id": "2026-09-01|1|ABC|2026-09-01T09:30:00+05:30",
        "sid": 1,
        "day": "2026-09-01",
        "profile": "higher_frequency",
        "strategy_version": "V13V5",
        "equity_symbol": "ABC",
        "equity_side": "LONG",
        "option_tradingsymbol": "ABC26SEP100CE",
        "required_option_type": "CE",
        "option_strike": 100.0,
        "lot_size": 10,
        "quantity": 10,
        "coverage_state": "READY",
        "mapping_status": "MAPPED_ATM",
        "reference_entry_bar": pd.Timestamp("2026-09-01 09:30", tz=common.IST),
        "reference_entry_premium": 100.0,
        "entry_bar_volume": 100,
        "reference_exit_bar": pd.Timestamp("2026-09-01 10:00", tz=common.IST),
        "reference_exit_premium": 110.0,
    }
    base.update(overrides)
    return pd.Series(base)


def _candles(records):
    frame = pd.DataFrame(records)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return opt.normalize_option_candles(frame)


def test_full_lot_at_t1_exits_without_partial_quantity() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 123.00,
                "low": 99.90,
                "close": 122.5,
                "volume": 100,
            }
        ]
    )

    result = opt.simulate_option_native_trade(
        _coverage_row(),
        candles,
        opt.OPTION_VARIANTS[0],
        cost_bps=5.0,
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_reason"] == "FULL_LOT_T1"
    assert result["option_lots"] == 3
    assert result["one_lot_quantity"] == 10
    assert result["quantity"] == 30
    assert result["target_hit"]
    assert result["entry_premium_outlay_rupees"] == pytest.approx(3000.0)
    assert result["gross_pnl_rupees"] == pytest.approx(675.0)
    assert result["net_pnl_rupees"] == pytest.approx(673.5)


def test_t1_arm_be_runner_keeps_whole_lot_until_runner() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 123.00,
                "low": 100.01,
                "close": 122.5,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 122.5,
                "high": 126.00,
                "low": 100.50,
                "close": 125.0,
                "volume": 100,
            },
        ]
    )

    result = opt.simulate_option_native_trade(
        _coverage_row(),
        candles,
        opt.OPTION_VARIANTS[1],
        cost_bps=5.0,
        runner_target_pct=25.0,
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_reason"] == "RUNNER_TARGET"
    assert result["quantity"] == 30
    assert result["target_hit"]
    assert result["runner_target_hit"]
    assert result["gross_pnl_rupees"] == pytest.approx(750.0)
    assert result["net_pnl_rupees"] == pytest.approx(748.5)


def test_t1_arm_be_runner_uses_breakeven_after_t1() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 123.00,
                "low": 100.01,
                "close": 122.5,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 120.0,
                "high": 121.00,
                "low": 99.95,
                "close": 100.0,
                "volume": 100,
            },
        ]
    )

    result = opt.simulate_option_native_trade(
        _coverage_row(),
        candles,
        opt.OPTION_VARIANTS[1],
        cost_bps=5.0,
        runner_target_pct=25.0,
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_reason"] == "T1_THEN_BREAKEVEN"
    assert result["target_hit"]
    assert result["breakeven_exit"]
    assert result["gross_pnl_rupees"] == pytest.approx(0.0)
    assert result["net_pnl_rupees"] == pytest.approx(-1.5)


def test_non_ready_coverage_is_not_backtested() -> None:
    result = opt.simulate_option_native_trade(
        _coverage_row(coverage_state="EXPIRED_OPTION_NOT_IN_CURRENT_MASTER"),
        pd.DataFrame(),
        opt.OPTION_VARIANTS[0],
        cost_bps=5.0,
    )
    assert result["execution_status"] == "SKIPPED_NOT_READY_COVERAGE"
    assert result["skip_reason"] == "EXPIRED_OPTION_NOT_IN_CURRENT_MASTER"


def test_cash_event_reference_reprices_stock_exit() -> None:
    result = opt.cash_event_reference_trade(_coverage_row(), cost_bps=5.0)

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_reason"] == "STOCK_STRATEGY_EXIT_REPRICE"
    assert result["gross_pnl_rupees"] == pytest.approx(300.0)
    assert result["net_pnl_rupees"] == pytest.approx(298.5)


def test_summary_reports_trading_days_and_average_trades_per_day() -> None:
    rows = [
        opt.cash_event_reference_trade(_coverage_row(), cost_bps=5.0),
        opt.cash_event_reference_trade(
            _coverage_row(
                trade_id="2026-09-01|2|ABC|2026-09-01T10:30:00+05:30",
                reference_entry_bar=pd.Timestamp("2026-09-01 10:30", tz=common.IST),
                reference_exit_bar=pd.Timestamp("2026-09-01 11:00", tz=common.IST),
            ),
            cost_bps=5.0,
        ),
        opt.cash_event_reference_trade(
            _coverage_row(
                trade_id="2026-09-02|3|ABC|2026-09-02T09:30:00+05:30",
                day="2026-09-02",
                reference_entry_bar=pd.Timestamp("2026-09-02 09:30", tz=common.IST),
                reference_exit_bar=pd.Timestamp("2026-09-02 10:00", tz=common.IST),
            ),
            cost_bps=5.0,
        ),
    ]

    summary = opt.summary_frame(pd.DataFrame(rows), input_count=3).iloc[0]

    assert summary["trading_days_with_executed_trades"] == 2
    assert summary["average_trades_per_trading_day"] == pytest.approx(1.5)
