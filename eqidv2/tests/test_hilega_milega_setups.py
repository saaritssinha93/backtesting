import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import all_setups_catalog as catalog
import avwap_5min_ID_v2_backtesting as v2
import hilega_milega_setups as hm
import run_hilega_milega_5m_research_backtest as research
import sweep_hilega_milega_research as sweep


def _indicator_input(length: int = 80) -> pd.DataFrame:
    dates = pd.date_range("2026-07-01 09:15", periods=length, freq="5min", tz="Asia/Kolkata")
    trend = np.linspace(100.0, 108.0, length)
    close = trend + 1.6 * np.sin(np.arange(length) / 2.7)
    open_price = np.r_[close[0], close[:-1]]
    return pd.DataFrame(
        {
            "date": dates,
            "open": open_price,
            "high": np.maximum(open_price, close) + 0.4,
            "low": np.minimum(open_price, close) - 0.4,
            "close": close,
            "volume": np.linspace(10_000.0, 25_000.0, length),
        }
    )


def _flag_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame[hm.BB_UPPER_COLUMN] = frame[hm.BB_MID_COLUMN] + 2.0
    frame[hm.BB_LOWER_COLUMN] = frame[hm.BB_MID_COLUMN] - 2.0
    return frame


def _scanner_frame() -> pd.DataFrame:
    dates = pd.date_range("2026-07-06 09:20", periods=23, freq="5min", tz="Asia/Kolkata")
    count = len(dates)
    open_price = np.full(count, 100.0)
    close = np.full(count, 100.0)
    high = np.full(count, 100.5)
    low = np.full(count, 99.5)
    signal_index = 20
    open_price[signal_index] = 99.5
    close[signal_index] = 101.0
    high[signal_index] = 101.5
    low[signal_index] = 99.0
    candle_range = high - low
    frame = pd.DataFrame(
        {
            "date": dates,
            "date_only": dates.date,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": 20_000.0,
            "ATR": 2.0,
            "VWAP": 99.0,
            "AVWAP": 99.0,
            "EMA_20": 99.5,
            "EMA_50": 99.0,
            "EMA_200": 98.0,
            "Upper_Band": 103.0,
            "Lower_Band": 97.0,
            "RSI": 55.0,
            "ADX": 22.0,
            "range": candle_range,
            "body_pct": np.abs(close - open_price) / candle_range,
            "close_loc": (close - low) / candle_range,
            "vol_ratio": 2.0,
            "atr_pct": 2.0 / close,
            "vwap_dist_atr": (close - 99.0) / 2.0,
            "avwap_dist_atr": (close - 99.0) / 2.0,
            "traded_value_rs": close * 20_000.0,
            "day_value_so_far_rs": 50_000_000.0,
        }
    )
    for flag_column in hm.SETUP_FLAG_COLUMNS.values():
        frame[flag_column] = False
    frame.loc[signal_index, hm.SETUP_FLAG_COLUMNS[hm.LONG_RSI50_REVERSAL]] = True
    return frame


def test_weighted_moving_average_uses_time_weights() -> None:
    values = pd.Series([1.0, 2.0, 3.0, 4.0])

    result = hm.weighted_moving_average(values, 3)

    assert np.isnan(result.iloc[1])
    assert result.iloc[2] == (1.0 + 4.0 + 9.0) / 6.0
    assert result.iloc[3] == (2.0 + 6.0 + 12.0) / 6.0


def test_full_history_does_not_change_prefix_features() -> None:
    source = _indicator_input()
    prefix_length = 60

    full = hm.add_hilega_milega_features(source)
    prefix = hm.add_hilega_milega_features(source.iloc[:prefix_length].copy())

    columns = [
        hm.RSI_COLUMN,
        hm.EMA_COLUMN,
        hm.WMA_COLUMN,
        hm.BB_MID_COLUMN,
        hm.BB_UPPER_COLUMN,
        hm.BB_LOWER_COLUMN,
        "HM_LINE_DISTANCE",
        *hm.SETUP_FLAG_COLUMNS.values(),
        "HM_EXIT_LONG_WMA_CROSS",
        "HM_EXIT_SHORT_WMA_CROSS",
    ]
    pd.testing.assert_frame_equal(
        full.loc[: prefix_length - 1, columns],
        prefix.loc[:, columns],
    )


def test_confirmed_reversal_flags_are_directional_and_close_confirmed() -> None:
    long_frame = _flag_frame(
        [
            {"open": 99.0, "high": 100.0, "low": 98.5, "close": 99.5,
             hm.RSI_COLUMN: 49.0, hm.EMA_COLUMN: 49.5, hm.WMA_COLUMN: 49.7,
             hm.BB_MID_COLUMN: 99.0},
            {"open": 99.5, "high": 101.5, "low": 99.2, "close": 101.0,
             hm.RSI_COLUMN: 52.0, hm.EMA_COLUMN: 50.0, hm.WMA_COLUMN: 49.8,
             hm.BB_MID_COLUMN: 100.0},
        ]
    )
    short_frame = _flag_frame(
        [
            {"open": 101.0, "high": 101.5, "low": 100.0, "close": 100.5,
             hm.RSI_COLUMN: 51.0, hm.EMA_COLUMN: 50.5, hm.WMA_COLUMN: 50.3,
             hm.BB_MID_COLUMN: 101.0},
            {"open": 100.2, "high": 100.4, "low": 98.0, "close": 99.0,
             hm.RSI_COLUMN: 48.0, hm.EMA_COLUMN: 50.0, hm.WMA_COLUMN: 50.1,
             hm.BB_MID_COLUMN: 100.0},
        ]
    )

    long_flags = hm.add_hilega_milega_setup_flags(long_frame)
    short_flags = hm.add_hilega_milega_setup_flags(short_frame)

    assert long_flags.iloc[-1][hm.SETUP_FLAG_COLUMNS[hm.LONG_RSI50_REVERSAL]]
    assert not long_flags.iloc[-1][hm.SETUP_FLAG_COLUMNS[hm.SHORT_RSI50_REVERSAL]]
    assert short_flags.iloc[-1][hm.SETUP_FLAG_COLUMNS[hm.SHORT_RSI50_REVERSAL]]
    assert not short_flags.iloc[-1][hm.SETUP_FLAG_COLUMNS[hm.LONG_RSI50_REVERSAL]]


def test_pullback_and_wma_trailing_exit_flags() -> None:
    frame = _flag_frame(
        [
            {"open": 101.0, "high": 102.0, "low": 100.5, "close": 101.5,
             hm.RSI_COLUMN: 55.0, hm.EMA_COLUMN: 52.0, hm.WMA_COLUMN: 51.0,
             hm.BB_MID_COLUMN: 100.0},
            {"open": 100.5, "high": 101.5, "low": 99.8, "close": 101.0,
             hm.RSI_COLUMN: 56.0, hm.EMA_COLUMN: 53.0, hm.WMA_COLUMN: 52.0,
             hm.BB_MID_COLUMN: 100.0},
            {"open": 101.0, "high": 101.2, "low": 99.8, "close": 100.2,
             hm.RSI_COLUMN: 54.0, hm.EMA_COLUMN: 53.5, hm.WMA_COLUMN: 55.0,
             hm.BB_MID_COLUMN: 100.0},
        ]
    )

    result = hm.add_hilega_milega_setup_flags(frame)

    assert result.iloc[1][hm.SETUP_FLAG_COLUMNS[hm.LONG_BB20_PULLBACK]]
    assert result.iloc[2]["HM_EXIT_LONG_WMA_CROSS"]


def test_v2_research_adapter_emits_setup_only_when_explicitly_enabled() -> None:
    frame = _scanner_frame()

    with patch.object(v2, "ENABLE_HILEGA_MILEGA_RESEARCH", False):
        disabled = v2._scan_day(frame, "TEST", {})
    with patch.object(v2, "ENABLE_HILEGA_MILEGA_RESEARCH", True):
        enabled = v2._scan_day(frame, "TEST", {})

    assert hm.LONG_RSI50_REVERSAL not in {candidate.setup for candidate in disabled}
    assert hm.LONG_RSI50_REVERSAL in {candidate.setup for candidate in enabled}


def test_catalog_marks_only_approved_hilega_setup_active() -> None:
    for setup in hm.ALL_SETUPS:
        item = catalog.find(setup)
        assert item is not None
        assert item.source == catalog.SRC_HILEGA_MILEGA
        expected = catalog.ACTIVE if setup == hm.SHORT_RSI50_REVERSAL else catalog.RAW_ONLY
        assert catalog.status_of(setup) == expected


def test_research_runner_excludes_opening_snapshot_before_aggregation() -> None:
    dates = pd.to_datetime(
        [
            "2026-08-03 09:15",
            "2026-08-03 09:20",
            "2026-08-03 09:25",
            "2026-08-03 09:30",
        ]
    ).tz_localize("Asia/Kolkata")
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": [90.0, 100.0, 101.0, 102.0],
            "high": [110.0, 101.0, 102.0, 103.0],
            "low": [80.0, 99.0, 100.0, 101.0],
            "close": [105.0, 100.5, 101.5, 102.5],
        }
    )

    completed = research._drop_opening_snapshots(frame)
    bars_15m = research._aggregate_signal_bars(completed, 15)

    assert completed["date"].dt.strftime("%H:%M").tolist() == ["09:20", "09:25", "09:30"]
    assert len(bars_15m) == 1
    assert bars_15m.iloc[0]["open"] == 100.0
    assert bars_15m.iloc[0]["high"] == 103.0
    assert bars_15m.iloc[0]["close"] == 102.5


def test_research_runner_resolves_same_bar_collision_as_stop() -> None:
    dates = pd.date_range("2026-08-03 09:20", periods=2, freq="5min", tz="Asia/Kolkata")
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": [100.0, 100.0],
            "high": [100.5, 102.0],
            "low": [99.5, 98.0],
            "close": [100.0, 101.0],
        }
    )

    outcome, exit_price, _, bars_held = research._resolve_exit_levels(
        frame,
        entry_index=1,
        side="LONG",
        stop_price=99.0,
        target_price=101.0,
        max_bars=0,
    )

    assert outcome == "SL_BOTH_HIT_CONSERVATIVE"
    assert exit_price == 99.0
    assert bars_held == 1


def test_research_runner_cost_model_is_round_trip_and_nonzero() -> None:
    cost = research._equity_intraday_cost(
        side="LONG",
        entry_price=100.0,
        exit_price=101.0,
        notional=50_000.0,
    )

    assert 50.0 < cost < 56.0


def test_research_adx_does_not_change_with_future_rows() -> None:
    source = _indicator_input(100)
    prefix_length = 70

    full = research._add_adx(source)
    prefix = research._add_adx(source.iloc[:prefix_length].copy())

    pd.testing.assert_series_equal(
        full.loc[: prefix_length - 1, "HM_ADX_14"],
        prefix["HM_ADX_14"],
    )


def test_sweep_variant_names_are_unique() -> None:
    for round_name in ("quick", "context", "reward", "fine_reward", "full"):
        variants = sweep.build_variants(round_name)
        names = [variant["variant"] for variant in variants]
        assert len(names) == len(set(names))
