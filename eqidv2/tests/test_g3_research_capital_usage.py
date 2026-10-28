"""Position-margin accounting for the offline unlimited-capital comparison."""
import pandas as pd

from research.g3_early_no_capital_limit_20261023 import capital_usage


def position(entry, exit, *, executed=True, economic_open=None):
    row = dict(entry_ts=pd.Timestamp(f"2026-10-23 {entry}", tz="Asia/Kolkata"),
               exit_ts=pd.Timestamp(f"2026-10-23 {exit}", tz="Asia/Kolkata"),
               portfolio_executed=executed)
    if economic_open:
        row["entry_open_ts"] = pd.Timestamp(f"2026-10-23 {economic_open}", tz="Asia/Kolkata")
    return row


def test_reused_margin_is_peak_concurrency_not_sum_of_trade_allocations():
    result = capital_usage(pd.DataFrame([
        position("09:22", "09:30"), position("09:30", "09:40")]))
    assert result["peak_positions"] == 1
    assert result["same_minute_conservative_peak_positions"] == 2
    assert result["peak_margin_rupees"] == 100_000
    assert result["peak_gross_exposure_rupees"] == 500_000


def test_rejected_orders_do_not_consume_margin():
    result = capital_usage(pd.DataFrame([
        position("09:22", "10:00"), position("09:23", "10:00"),
        position("09:24", "10:00", executed=False)]))
    assert result["peak_positions"] == 2
    assert result["peak_margin_rupees"] == 200_000


def test_market_order_uses_economic_open_before_prior_position_exit():
    result = capital_usage(pd.DataFrame([
        position("09:20", "09:22"),
        position("09:22", "09:30", economic_open="09:21")]))
    assert result["peak_positions"] == 2
    assert result["peak_margin_rupees"] == 200_000


def test_same_candle_entry_exit_still_requires_margin():
    result = capital_usage(pd.DataFrame([position("09:22", "09:22")]))
    assert result["peak_positions"] == 1
    assert result["peak_margin_rupees"] == 100_000


def test_no_executions_require_zero_margin():
    result = capital_usage(pd.DataFrame([position("09:22", "10:00", executed=False)]))
    assert set(result.values()) == {0}
