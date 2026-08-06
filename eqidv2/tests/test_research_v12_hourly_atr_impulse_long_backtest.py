from __future__ import annotations

import pandas as pd

import research_v12_hourly_atr_impulse_long_backtest as replay


def _state(**overrides: object) -> dict[str, object]:
    # Two 1% returns compound to 2.01%.  With close=100 and ATR=.804 this is
    # exactly 2.50 updated ATR percent (within floating-point precision).
    row: dict[str, object] = {
        "raw_two_bar_trigger": True,
        "common_gate_pass": True,
        "previous_return_5m_close_pct": 1.0,
        "return_5m_close_pct": 1.0,
        "signal_close": 100.0,
        "signal_atr": 0.804,
        "vwap_dist_atr": 0.0,
        "traded_value_rs": 5_000_000.0,
        # Deliberately hostile soft fields: none may become a hard gate.
        "signal_adx": 1.0,
        "stoch_k": 1.0,
        "stoch_d": 99.0,
        "volume_ratio20": 0.01,
        "context_score": 0,
    }
    row.update(overrides)
    return row


def test_locked_gate_passes_boundaries_and_ignores_soft_indicators() -> None:
    frame = pd.DataFrame([_state()])

    gated, funnel = replay.apply_preregistered_signal_gate(frame)

    assert bool(gated.loc[0, "preregistered_signal"])
    assert abs(float(gated.loc[0, "impulse_atr_ratio"]) - 2.5) < 1e-9
    assert funnel[-1]["after"] == 1


def test_locked_gate_rejects_each_independent_hard_failure() -> None:
    frame = pd.DataFrame(
        [
            _state(),
            _state(signal_atr=0.805),  # just below 2.50 ATR
            _state(vwap_dist_atr=-0.000001),
            _state(traded_value_rs=4_999_999.99),
            _state(previous_return_5m_close_pct=0.499999),
            _state(return_5m_close_pct=1.500001),
            _state(raw_two_bar_trigger=False),
            _state(common_gate_pass=False),
        ]
    )

    gated, _ = replay.apply_preregistered_signal_gate(frame)

    assert gated["preregistered_signal"].tolist() == [
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
    ]


def test_locked_context_freezes_data_gates_and_keeps_indicators_nonbinding() -> None:
    times = pd.date_range(
        pd.Timestamp("2026-08-03 10:00", tz="Asia/Kolkata"),
        periods=3,
        freq="5min",
    )
    frame = pd.DataFrame(
        {
            "ticker": ["ABC"] * 3,
            "trade_date": ["2026-08-03"] * 3,
            "signal_time_ist": times,
            "return_5m_close_pct": [0.8, 0.9, 1.0],
            "return_pair_exact": [True, True, True],
            "signal_close": [100.0, 100.0, 79.99],
            "signal_atr": [1.0, 1.0, 1.0],
            "traded_value_rs": [1_000_000.0] * 3,
            "range_atr": [3.5, 3.5, 3.5],
            "signal_adx": [1.0] * 3,
            "stoch_k": [1.0] * 3,
            "stoch_d": [99.0] * 3,
            "vwap_dist_atr": [0.0] * 3,
            "vwap_slope_3": [-1.0] * 3,
            "plus_di": [1.0] * 3,
            "minus_di": [99.0] * 3,
            "adx_rising_3": [False] * 3,
            "stoch_rising": [False] * 3,
            "volume_ratio20": [0.01] * 3,
            "close_location": [0.01] * 3,
            "nifty_aligned": [False] * 3,
        }
    )

    states, funnel = replay.add_locked_signal_context(frame)

    assert states["raw_two_bar_trigger"].tolist() == [False, True, False]
    assert states["common_gate_pass"].tolist() == [True, True, False]
    assert states["context_score"].tolist() == [1, 1, 1]
    assert funnel[-1]["after"] == 1


def test_discovery_dates_are_never_mixed_into_backward_validation() -> None:
    dates = pd.Series(
        [
            "2026-06-04",
            replay.DISCOVERY_START,
            replay.DISCOVERY_END,
            "2026-08-05",
        ]
    )

    assert replay.label_trade_windows(dates).tolist() == [
        "backward_pre_discovery",
        "discovery",
        "discovery",
        "post_discovery",
    ]


def _profitable_prior_trades(count: int = 100) -> tuple[pd.DataFrame, list[str]]:
    sessions = [f"2026-05-{day:02d}" for day in range(1, 11)]
    rows = []
    for index in range(count):
        # Every block of ten has six +200 wins and four -100 losses: PF 3.0.
        pnl = 200.0 if index % 10 < 6 else -100.0
        session_index = min(index // 10, len(sessions) - 1)
        rows.append(
            {
                "ticker": f"T{index}",
                "trade_date": sessions[session_index],
                "gross_pnl_rs": pnl,
                "cost_rs": 0.0,
                "net_pnl_rs": pnl,
                "net_r": pnl / 500.0,
            }
        )
    return pd.DataFrame(rows), sessions


def test_validation_gate_can_qualify_but_never_promotes() -> None:
    trades, sessions = _profitable_prior_trades()

    result = replay.evaluate_backward_validation_gates(trades, sessions)

    assert result["qualification_pass"] is True
    assert all(result["checks"].values())
    assert result["production_approved"] is False
    assert result["promotion_action"] == "NONE_RESEARCH_ONLY"


def test_validation_gate_fails_below_100_prior_trades() -> None:
    trades, sessions = _profitable_prior_trades(99)

    result = replay.evaluate_backward_validation_gates(trades, sessions)

    assert result["qualification_pass"] is False
    assert result["checks"]["at_least_100_prior_trades"] is False


def test_cli_accepts_arbitrary_data_roots_without_tuning_discovery_dates() -> None:
    args = replay.parse_args(
        [
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2026-08-04",
            "--prefilter",
            "X:/prefilter.csv",
            "--five-minute-dir",
            "X:/canonical5m",
            "--nifty-five-minute-dir",
            "X:/nifty5m",
            "--one-minute-dir",
            "X:/one1m",
            "--out",
            "X:/out",
        ]
    )

    assert args.start_date == "2025-01-01"
    assert args.end_date == "2026-08-04"
    assert str(args.five_minute_dir).replace("\\", "/") == "X:/canonical5m"
    assert replay.DISCOVERY_START == "2026-06-05"
    assert replay.DISCOVERY_END == "2026-08-04"
