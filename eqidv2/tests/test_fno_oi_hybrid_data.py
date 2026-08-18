from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


class FnoOiHybridDataTests(unittest.TestCase):
    def test_stock_futures_map_to_exact_cash_equities_and_indexes_are_excluded(self) -> None:
        universe = pd.DataFrame(
            [
                {
                    "underlying": "IDEA",
                    "tradingsymbol": "IDEA26AUGFUT",
                    "instrument_token": 1,
                    "lot_size": 1000,
                    "tick_size": 0.05,
                },
                {
                    "underlying": "LTM",
                    "tradingsymbol": "LTM26AUGFUT",
                    "instrument_token": 2,
                    "lot_size": 100,
                    "tick_size": 0.05,
                },
                {
                    "underlying": "NIFTY",
                    "tradingsymbol": "NIFTY26AUGFUT",
                    "instrument_token": 3,
                    "lot_size": 75,
                    "tick_size": 0.05,
                },
                {
                    "underlying": "NIFTYFPI",
                    "tradingsymbol": "NIFTYFPI26AUGFUT",
                    "instrument_token": 4,
                    "lot_size": 1100,
                    "tick_size": 0.05,
                },
            ]
        )
        nse = pd.DataFrame(
            [
                {
                    "tradingsymbol": "IDEA",
                    "instrument_token": 101,
                    "instrument_type": "EQ",
                    "exchange": "NSE",
                    "tick_size": 0.05,
                },
                {
                    "tradingsymbol": "LTIM",
                    "instrument_token": 102,
                    "instrument_type": "EQ",
                    "exchange": "NSE",
                    "tick_size": 0.05,
                },
            ]
        )

        mapped, excluded = hybrid.attach_equity_instruments(universe, nse)

        self.assertEqual(set(mapped["equity_symbol"]), {"IDEA", "LTIM"})
        self.assertEqual(
            dict(zip(mapped["equity_symbol"], mapped["equity_instrument_token"])),
            {"IDEA": 101, "LTIM": 102},
        )
        self.assertEqual(
            set(excluded["underlying"]),
            {"NIFTY", "NIFTYFPI"},
        )
        self.assertEqual(
            set(excluded["reason"]),
            {"INDEX_FUTURE_HAS_NO_CASH_EQUITY"},
        )

    def test_hybrid_join_uses_cash_ohlcv_and_only_futures_oi(self) -> None:
        timestamps = pd.date_range(
            "2026-08-10 09:20", periods=3, freq="5min", tz=common.IST
        )
        equity = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": [99.0, 100.0, 101.0],
                "high": [100.0, 101.0, 102.0],
                "low": [98.0, 99.0, 100.0],
                "close": [100.0, 101.0, 102.0],
                "volume": [10.0, 20.0, 30.0],
            }
        )
        future = pd.DataFrame(
            {
                "ts": timestamps,
                "open": [500.0, 400.0, 300.0],
                "high": [501.0, 401.0, 301.0],
                "low": [499.0, 399.0, 299.0],
                "close": [500.0, 400.0, 300.0],
                "volume": [999.0, 999.0, 999.0],
                "oi": [1000.0, 1100.0, 1210.0],
            }
        )

        merged = hybrid.join_equity_price_with_futures_oi(equity, future)

        self.assertEqual(merged["close"].tolist(), [100.0, 101.0, 102.0])
        self.assertEqual(merged["volume"].tolist(), [10.0, 20.0, 30.0])
        self.assertEqual(merged["oi"].tolist(), [1000.0, 1100.0, 1210.0])
        self.assertTrue(np.isnan(merged.iloc[0]["prev_oi"]))
        self.assertEqual(merged["prev_oi"].iloc[1:].tolist(), [1000.0, 1100.0])
        self.assertAlmostEqual(merged.iloc[1]["price_change_pct"], 1.0)
        self.assertAlmostEqual(merged.iloc[1]["oi_change_pct"], 10.0)
        self.assertEqual(set(merged["price_source"]), {"NSE_EQUITY"})
        self.assertEqual(set(merged["oi_source"]), {"NFO_FUTURE"})
        self.assertFalse(
            {
                "futures_open",
                "futures_high",
                "futures_low",
                "futures_close",
                "futures_volume",
            }
            & set(merged.columns)
        )

    def test_five_minute_quality_filter_rejects_non_real_bars(self) -> None:
        timestamps = pd.date_range(
            "2026-08-10 09:15", periods=6, freq="5min", tz=common.IST
        )
        frame = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": range(6),
                "high": range(1, 7),
                "low": range(6),
                "close": range(1, 7),
                "volume": [10.0] * 6,
                "opening_snapshot": [1, 0, 0, 0, 0, 0],
                "gap_filled": [0, 0, 1, 0, 0, 0],
                "provisional_stale": [0, 0, 0, 1, 0, 0],
                "source_1m_count": [None, 5, 5, 5, 4, 5],
            }
        )

        completed = hybrid.completed_real_equity_five_minute_bars(frame)

        self.assertEqual(
            completed["ts"].dt.strftime("%H:%M").tolist(),
            ["09:20", "09:40"],
        )

    def test_one_minute_aggregation_is_causal_and_requires_all_five_rows(self) -> None:
        timestamps = pd.date_range(
            "2026-08-10 09:16", periods=10, freq="1min", tz=common.IST
        )
        minute = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": np.arange(10, dtype=float) + 100.0,
                "high": np.arange(10, dtype=float) + 101.0,
                "low": np.arange(10, dtype=float) + 99.0,
                "close": np.arange(10, dtype=float) + 100.5,
                "volume": np.arange(10, dtype=float) + 1.0,
                "gap_filled": [0] * 8 + [1, 0],
            }
        )

        five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)

        self.assertEqual(five["ts"].dt.strftime("%H:%M").tolist(), ["09:20"])
        self.assertEqual(five.iloc[0]["open"], 100.0)
        self.assertEqual(five.iloc[0]["high"], 105.0)
        self.assertEqual(five.iloc[0]["low"], 99.0)
        self.assertEqual(five.iloc[0]["close"], 104.5)
        self.assertEqual(five.iloc[0]["volume"], 15.0)
        self.assertEqual(five.iloc[0]["source_1m_count"], 5)

    def test_file_bar_reader_rejects_exact_adjacent_ohlcv_copy(self) -> None:
        timestamps = pd.date_range(
            "2026-08-10 09:20", periods=3, freq="5min", tz=common.IST
        )
        frame = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": [100.0, 100.0, 101.0],
                "high": [101.0, 101.0, 102.0],
                "low": [99.0, 99.0, 100.0],
                "close": [100.5, 100.5, 101.5],
                "volume": [1000.0, 1000.0, 800.0],
            }
        )

        cleaned = hybrid.reject_exact_adjacent_ohlcv_copies(frame)

        self.assertEqual(cleaned["ts"].dt.strftime("%H:%M").tolist(), ["09:20", "09:30"])

    def test_file_bar_reader_keeps_exact_copy_with_verified_one_minute_lineage(self) -> None:
        timestamps = pd.date_range(
            "2026-08-10 09:20", periods=3, freq="5min", tz=common.IST
        )
        frame = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": [100.0, 100.0, 101.0],
                "high": [101.0, 101.0, 102.0],
                "low": [99.0, 99.0, 100.0],
                "close": [100.5, 100.5, 101.5],
                "volume": [1000.0, 1000.0, 800.0],
                "source_1m_count": [5, 5, 5],
            }
        )

        cleaned = hybrid.reject_exact_adjacent_ohlcv_copies(frame)

        self.assertEqual(
            cleaned["ts"].dt.strftime("%H:%M").tolist(),
            ["09:20", "09:25", "09:30"],
        )


if __name__ == "__main__":
    unittest.main()
