from __future__ import annotations

import pandas as pd

import research_v12_six_month_frozen_balanced_long_replay_v11 as replay


def _feature_frame() -> pd.DataFrame:
    rows = []
    for ticker, previous_return in (("PASS", -0.20), ("FAIL", 0.10)):
        rows.append({
            "trade_date": "2026-02-05",
            "ticker": ticker,
            "signal_time_ist": pd.Timestamp("2026-02-05 10:00", tz="Asia/Kolkata"),
            "selection_rank": 210,
            "signal_minute": 600,
            "atr_pct": 0.50,
            "session_return_so_far_pct": 1.50,
            "vwap_dist_atr": 1.00,
            "close_position_in_bar": 0.80,
            "range_pct": 0.30,
            "ret_5m_pct": 0.20,
            "ret_15m_pct": 0.0,
            "ret_30m_pct": 0.0,
            "ret_60m_pct": 0.0,
            "return_acceleration_5_vs_15": 0.0,
            "ADX": 25.0,
            "RSI": 55.0,
            "volume_ratio20": 1.0,
            "upper_wick_pct": 0.1,
            "distance_from_running_session_high_atr": -0.2,
            "ema20_dist_atr": 1.20,
            "ema50_dist_atr": 0.5,
            "score_margin": 0.10,
            "previous_ret_5m_pct": previous_return,
            "previous_vwap_dist_atr": 0.0,
            "contiguous_previous": True,
            "bullish_reversal": False,
            "vwap_reclaim": False,
        })
    return pd.DataFrame(rows)


def test_frozen_configuration_id_and_hash_are_exact():
    config, payload = replay.load_frozen_config()
    assert config.config_id == replay.EXPECTED_CONFIG_ID
    assert replay.v9.config_hash(config) == replay.EXPECTED_CONFIG_SHA256
    assert payload["production_approved"] is False


def test_six_month_calendar_is_complete_and_bounded():
    calendar = replay.session_calendar()
    assert len(calendar) == 120
    assert calendar[0] == replay.START_DATE
    assert calendar[-1] == replay.END_DATE


def test_rule_mask_reuses_frozen_previous_bar_requirement():
    config, _ = replay.load_frozen_config()
    mask = replay.rule_mask(_feature_frame(), config, ["2026-02-05"])
    assert mask.tolist() == [True, False]


def test_daily_results_preserve_zero_trade_sessions_and_cumulative_pnl():
    trades = pd.DataFrame([
        {
            "trade_date": "2026-02-05",
            "ticker": "AAA",
            "gross_pnl_rs": 550.0,
            "cost_rs": 50.0,
            "net_pnl_rs": 500.0,
        }
    ])
    daily = replay.daily_results(
        trades, ["2026-02-05", "2026-02-06", "2026-02-09"]
    )
    assert daily["trades"].tolist() == [1, 0, 0]
    assert daily["cumulative_net_pnl_rs"].tolist() == [500.0, 500.0, 500.0]


def test_period_helpers_cover_every_session_once():
    sessions = [
        "2026-02-27",
        "2026-03-02",
        "2026-03-03",
        "2026-04-01",
        "2026-04-02",
    ]
    months = replay.month_periods(sessions)
    blocks = replay.block_periods(sessions, size=2)
    assert [day for values in months.values() for day in values] == sessions
    assert [day for values in blocks.values() for day in values] == sessions


def test_written_replay_configuration_remains_research_only(tmp_path):
    config, _ = replay.load_frozen_config()
    output = tmp_path / "conf.py"
    replay.write_replay_config(output, config)
    text = output.read_text(encoding="utf-8")
    assert "PRODUCTION_APPROVED = False" in text
    assert "BACKCAST_NOT_FORWARD_HOLDOUT = True" in text
    assert replay.EXPECTED_CONFIG_SHA256 in text
    assert "STOP_LOSS_PCT = 1.0" in text
    assert "TARGET_PCT = 2.0" in text


def test_grid_validation_rejects_missing_and_synthetic_bars():
    index = pd.date_range(
        pd.Timestamp("2026-02-05 10:05", tz="Asia/Kolkata"),
        periods=3,
        freq="5min",
    )
    bars = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "gap_filled": [False, True, False],
        },
        index=index,
    )
    reasons = replay._grid_validation_reasons(
        bars,
        index[0],
        index[-1],
        "5min",
        reject_gap_filled=True,
    )
    assert "SYNTHETIC_GAP_FILLED_BAR" in reasons
    missing = replay._grid_validation_reasons(
        bars.drop(index=index[1]),
        index[0],
        index[-1],
        "5min",
        reject_gap_filled=False,
    )
    assert "INCOMPLETE_TIMESTAMP_GRID" in missing


def test_runtime_contract_and_same_bar_stop_first_are_frozen():
    contract = replay.assert_frozen_runtime_contract()
    assert contract["intraday_leverage"] == 5.0
    assert contract["paper_entry_slippage_pct"] == 0.0005
    assert contract["same_bar_collision_policy"] == "STOP_FIRST"
