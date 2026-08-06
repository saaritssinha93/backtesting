import pandas as pd

import research_v12_ml_long_entry_backtest as replay


def test_apply_daily_cap_is_chronological_and_limited() -> None:
    rows = []
    for index in range(18):
        rows.append({
            "ticker": f"T{index:02d}",
            "v7_signal_entry_time_ist": f"2026-06-04 10:{index:02d}:00+05:30",
            "selection_rank": 300 - index,
        })
    accepted, rejected = replay.apply_daily_cap(pd.DataFrame(rows))
    assert len(accepted) == replay.DAILY_CAP
    assert len(rejected) == 3
    assert accepted["daily_sequence"].max() == replay.DAILY_CAP
    assert rejected["reject_reason"].eq("portfolio_daily_cap_15").all()


def test_apply_daily_cap_uses_rank_then_ticker_for_same_entry_time() -> None:
    frame = pd.DataFrame([
        {"ticker": "BBB", "v7_signal_entry_time_ist": "2026-06-04 10:01:00+05:30", "selection_rank": 205},
        {"ticker": "AAA", "v7_signal_entry_time_ist": "2026-06-04 10:01:00+05:30", "selection_rank": 205},
        {"ticker": "CCC", "v7_signal_entry_time_ist": "2026-06-04 10:01:00+05:30", "selection_rank": 204},
    ])
    accepted, _ = replay.apply_daily_cap(frame)
    assert accepted["ticker"].tolist() == ["CCC", "AAA", "BBB"]


def test_metrics_include_zero_trade_sessions_and_costs() -> None:
    trades = pd.DataFrame([
        {"trade_date": "2026-06-04", "ticker": "AAA", "gross_pnl_rs": 100.0, "cost_rs": 10.0, "net_pnl_rs": 90.0},
        {"trade_date": "2026-06-06", "ticker": "BBB", "gross_pnl_rs": -50.0, "cost_rs": 10.0, "net_pnl_rs": -60.0},
    ])
    result = replay.metrics(trades, ["2026-06-04", "2026-06-05", "2026-06-06"])
    assert result["sessions"] == 3
    assert result["zero_trade_sessions"] == 1
    assert result["gross_pnl_rs"] == 50.0
    assert result["cost_rs"] == 20.0
    assert result["net_pnl_rs"] == 30.0
    assert result["profit_factor"] == 1.5


def test_research_contract_cannot_be_production_approved() -> None:
    assert replay.PRODUCTION_APPROVED is False
    assert replay.PRIMARY_SL_PCT == 0.90
    assert replay.PRIMARY_TARGET_PCT == 1.50
    assert replay.SETUP not in replay.v12.ENTRY_SHADOW_SETUPS
