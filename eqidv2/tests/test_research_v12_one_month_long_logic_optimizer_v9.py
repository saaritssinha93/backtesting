from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as research


def _config(**overrides):
    base = research.RuleConfig(
        config_id="TEST",
        family="MOMENTUM_CONTINUATION",
        rank_min=200,
        rank_max=300,
        signal_minute_min=570,
        signal_minute_max=855,
        atr_pct_min=0.35,
        session_return_min=0.0,
        vwap_dist_atr_min=-0.5,
        close_position_min=0.35,
    )
    return replace(base, **overrides)


def _frame() -> pd.DataFrame:
    rows = []
    sessions = [f"2026-07-{day:02d}" for day in range(1, 4)]
    for day_index, day in enumerate(sessions):
        for ticker_index, ticker in enumerate(("AAA", "BBB")):
            for minute_index, minute in enumerate((600, 605)):
                rows.append({
                    "trade_date": day,
                    "ticker": ticker,
                    "selection_rank": 200 + ticker_index,
                    "signal_minute": minute,
                    "atr_pct": 1.0,
                    "session_return_so_far_pct": 1.0,
                    "vwap_dist_atr": 0.5,
                    "close_position_in_bar": 0.8,
                    "range_pct": 0.5,
                    "ret_5m_pct": 0.2,
                    "ret_15m_pct": 0.3,
                    "ret_30m_pct": 0.4,
                    "ret_60m_pct": 0.5,
                    "return_acceleration_5_vs_15": 0.1,
                    "ADX": 30.0,
                    "RSI": 60.0,
                    "volume_ratio20": 1.0,
                    "upper_wick_pct": 0.1,
                    "distance_from_running_session_high_atr": -0.2,
                    "ema20_dist_atr": 0.5,
                    "ema50_dist_atr": 0.4,
                    "score_margin": 0.1,
                    "previous_ret_5m_pct": -0.1,
                    "previous_vwap_dist_atr": -0.1,
                    "contiguous_previous": bool(minute_index),
                    "bullish_reversal": bool(minute_index),
                    "vwap_reclaim": bool(minute_index),
                    "net_pnl_rs": 500.0 if (day_index + ticker_index) % 2 == 0 else -500.0,
                    "gross_pnl_rs": 550.0 if (day_index + ticker_index) % 2 == 0 else -450.0,
                    "cost_rs": 50.0,
                    "entry_time_ist": pd.Timestamp(day, tz="Asia/Kolkata")
                    + pd.Timedelta(minutes=minute),
                    "exit_time_ist": pd.Timestamp(day, tz="Asia/Kolkata")
                    + pd.Timedelta(minutes=minute + 30),
                })
    return pd.DataFrame(rows)


def test_generated_configurations_are_deterministic_and_unique():
    first = research.generate_configurations(100)
    second = research.generate_configurations(100)
    assert first == second
    hashes = [research.config_hash(item) for item in first]
    assert len(set(hashes)) == len(hashes)


def test_selection_uses_first_passing_signal_and_one_ticker_per_day():
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03"]
    arrays = research.SearchArrays(_frame(), sessions)
    selected = arrays.selected_indices(_config())
    chosen = arrays.frame.iloc[selected]
    assert len(chosen) == 6
    assert chosen["signal_minute"].eq(600).all()
    assert not chosen.duplicated(["trade_date", "ticker"]).any()


def test_contiguous_reversal_requirement_moves_entry_to_second_bar():
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03"]
    arrays = research.SearchArrays(_frame(), sessions)
    selected = arrays.selected_indices(
        _config(require_contiguous_previous=True, require_bullish_reversal=True)
    )
    assert len(selected) == 6
    assert arrays.frame.iloc[selected]["signal_minute"].eq(605).all()


def test_performance_drawdown_is_nonpositive_and_cost_stress_reduces_net():
    sessions = ["2026-07-01", "2026-07-02", "2026-07-03"]
    arrays = research.SearchArrays(_frame(), sessions)
    selected = arrays.selected_indices(_config())
    base = research.performance_from_indices(arrays, selected, range(3))
    stressed = research.performance_from_indices(
        arrays, selected, range(3), cost_multiplier=1.5
    )
    assert base["max_drawdown_rs"] <= 0.0
    assert stressed["net_pnl_rs"] < base["net_pnl_rs"]


def test_drawdown_uses_realized_exit_order():
    frame = _frame().iloc[:3].copy()
    frame["net_pnl_rs"] = [-100.0, 200.0, -50.0]
    frame["gross_pnl_rs"] = frame["net_pnl_rs"]
    frame["cost_rs"] = 0.0
    base = pd.Timestamp("2026-07-01 10:00", tz="Asia/Kolkata")
    frame["exit_time_ist"] = [
        base + pd.Timedelta(minutes=1),
        base,
        base + pd.Timedelta(minutes=2),
    ]
    arrays = research.SearchArrays(frame, ["2026-07-01"])
    result = research.performance_from_indices(arrays, np.arange(3), range(1))
    assert result["max_drawdown_rs"] == -150.0
    assert result["max_drawdown_basis"] == "realized_exit_order"


def test_daily_cap_is_applied_chronologically():
    frame = pd.concat([_frame().iloc[[0]].assign(ticker=f"T{index:02d}") for index in range(20)], ignore_index=True)
    arrays = research.SearchArrays(frame, ["2026-07-01"])
    selected = arrays.selected_indices(_config())
    assert len(selected) == research.DAILY_CAP
    assert arrays.frame.iloc[selected]["ticker"].tolist() == [f"T{index:02d}" for index in range(15)]


def test_written_configuration_remains_research_only(tmp_path):
    output = tmp_path / "candidate.py"
    research.write_config(output, _config())
    text = output.read_text(encoding="utf-8")
    assert "PRODUCTION_APPROVED = False" in text
    assert "PREFILTER_JOB_CHANGED = False" in text
    assert "STOP_LOSS_PCT = 1.0" in text
    assert "TARGET_PCT = 2.0" in text
    assert "PAPER_ENTRY_SLIPPAGE_BPS" in text
    assert "RISK_EQUITY_RS" in text
    assert 'ONE_MINUTE_GAP_POLICY = "CONSERVATIVE_5MIN_FALLBACK"' in text


def test_negative_minimum_threshold_perturbations_are_labeled_correctly():
    config = _config(ret_5m_min=-0.10)
    perturbed = dict(research.perturbation_configs(config))
    assert np.isclose(perturbed["RET_5M_MIN_LOOSER10"].ret_5m_min, -0.11)
    assert np.isclose(perturbed["RET_5M_MIN_TIGHTER10"].ret_5m_min, -0.09)
