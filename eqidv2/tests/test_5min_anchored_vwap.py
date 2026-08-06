from datetime import datetime
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_5minonly as historical_5m
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as live_5m
import research_v12_build_canonical_5m_from_1m as canonical_5m
import avwap_5min_ID_v2_backtesting as shared_scanner
import eqidv2_late_bb10_compression as late_bb10


def _stamp(day: int, hour: int, minute: int) -> pd.Timestamp:
    return pd.Timestamp(live_5m.IST_TZ.localize(datetime(2026, 8, day, hour, minute)))


def _frame() -> pd.DataFrame:
    prices = [500.0, 100.0, 900.0, 110.0, 700.0, 200.0]
    return pd.DataFrame(
        {
            "date": [
                _stamp(5, 9, 15),
                _stamp(5, 9, 20),
                _stamp(5, 9, 25),
                _stamp(5, 9, 30),
                _stamp(6, 9, 15),
                _stamp(6, 9, 20),
            ],
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "volume": [1_000.0, 10.0, 2_000.0, 30.0, 1_000.0, 20.0],
            "gap_filled": [0, 0, 1, 0, 0, 0],
        }
    )


class AnchoredVwap5MinTests(unittest.TestCase):
    def test_both_producers_share_causal_completed_bar_contract(self):
        expected = np.array([np.nan, 100.0, np.nan, 107.5, np.nan, 200.0])

        calculators = (
            (live_5m.__name__, live_5m.calculate_anchored_vwap_5min),
            (historical_5m.__name__, historical_5m.calculate_anchored_vwap_5min),
            (shared_scanner.__name__, shared_scanner._calc_session_open_avwap),
        )
        for name, calculator in calculators:
            with self.subTest(calculator=name):
                actual = calculator(_frame()).to_numpy()
                np.testing.assert_allclose(actual, expected, equal_nan=True)

    def test_future_bar_mutation_cannot_change_earlier_avwap(self):
        original = _frame().iloc[:4].copy()
        mutated = original.copy()
        mutated.loc[3, ["high", "low", "close"]] = 50_000.0
        mutated.loc[3, "volume"] = 9_000_000.0

        for calculator in (
            live_5m.calculate_anchored_vwap_5min,
            historical_5m.calculate_anchored_vwap_5min,
            shared_scanner._calc_session_open_avwap,
        ):
            before = calculator(original)
            after = calculator(mutated)
            np.testing.assert_allclose(
                before.iloc[:3].to_numpy(),
                after.iloc[:3].to_numpy(),
                equal_nan=True,
            )

    def test_partial_source_bucket_is_excluded_without_breaking_later_cumulative_value(self):
        frame = _frame().iloc[:4].copy()
        frame["gap_filled"] = 0
        frame["source_1m_count"] = [5, 5, 4, 5]

        actual = live_5m.calculate_anchored_vwap_5min(frame).to_numpy()

        np.testing.assert_allclose(
            actual,
            np.array([np.nan, 100.0, np.nan, 107.5]),
            equal_nan=True,
        )

    def test_non_finite_bar_is_excluded_and_later_bar_resumes(self):
        frame = _frame().iloc[:4].copy()
        frame["gap_filled"] = 0
        frame.loc[2, ["high", "low", "close"]] = np.inf
        expected = np.array([np.nan, 100.0, np.nan, 107.5])

        for calculator in (
            live_5m.calculate_anchored_vwap_5min,
            historical_5m.calculate_anchored_vwap_5min,
            shared_scanner._calc_session_open_avwap,
        ):
            np.testing.assert_allclose(
                calculator(frame).to_numpy(), expected, equal_nan=True
            )

    def test_late_bb10_uses_exact_shared_avwap_series(self):
        frame = _frame().iloc[:4].copy()
        frame["source_1m_count"] = [5, 5, 5, 5]
        expected = shared_scanner._calc_session_open_avwap(frame)

        featured = late_bb10.add_features(frame)

        np.testing.assert_allclose(
            featured["avwap"].to_numpy(),
            expected.to_numpy(),
            equal_nan=True,
        )
        self.assertTrue(np.isnan(featured.iloc[0]["avwap"]))
        self.assertTrue(np.isnan(featured.iloc[2]["avwap"]))
        self.assertEqual(featured.iloc[3]["avwap"], 107.5)

    def test_late_bb10_breadth_uses_shared_avwap_contract(self):
        slot = _stamp(5, 9, 30)

        alignment = late_bb10.market_alignment_for_slots(
            ["TEST"],
            [slot],
            lambda ticker: _frame().iloc[:4].copy() if ticker == "TEST" else None,
        )

        # Ignoring the 500-price opening snapshot and the 900-price gap bar
        # leaves AVWAP 107.5, so the 110 close is correctly above it.
        self.assertEqual(alignment[slot.isoformat()]["market_breadth"], 1.0)

    def test_live_five_minute_feature_pipeline_persists_avwap_only_for_5min(self):
        five_minute = live_5m._compute_common_features(_frame(), "5min")
        fifteen_minute = live_5m._compute_common_features(_frame(), "15min")

        self.assertIn("AVWAP", five_minute.columns)
        self.assertNotIn("AVWAP", fifteen_minute.columns)
        self.assertTrue(np.isnan(five_minute.loc[0, "AVWAP"]))
        self.assertEqual(five_minute.loc[1, "AVWAP"], 100.0)

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "TEST_stocks_indicators_5min.parquet"
            live_5m._finalize_and_save(five_minute, str(output))
            persisted = pd.read_parquet(output, columns=["date", "AVWAP"])

        self.assertEqual(len(persisted), len(five_minute))
        np.testing.assert_allclose(
            persisted["AVWAP"].to_numpy(),
            five_minute["AVWAP"].to_numpy(),
            equal_nan=True,
        )

    def test_canonical_v12_builder_excludes_snapshot_and_partial_bucket(self):
        day = "2026-08-05"
        rows = []
        for end_minute, price, volume, count in (
            (20, 100.0, 2.0, 5),
            (25, 110.0, 3.0, 5),
            (30, 900.0, 100.0, 4),
            (35, 120.0, 5.0, 5),
        ):
            first_minute = end_minute - count + 1
            for minute in range(first_minute, end_minute + 1):
                rows.append(
                    {
                        "date": pd.Timestamp(
                            f"{day} 09:{minute:02d}", tz=canonical_5m.IST
                        ),
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": volume,
                    }
                )
        minute = pd.DataFrame(rows)
        market_days = [day]
        start = pd.Timestamp(day, tz=canonical_5m.IST)
        end = start + pd.Timedelta(days=1)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "one_minute"
            output_dir = root / "five_minute"
            source_dir.mkdir()
            minute.to_parquet(
                source_dir / "TEST_stocks_indicators_1min.parquet", index=False
            )
            canonical_5m._canonical_ticker(
                "TEST",
                source_dir,
                output_dir,
                start,
                end,
                canonical_5m._expected_grid(market_days),
                market_days,
            )
            result = pd.read_parquet(
                output_dir / "TEST_stocks_indicators_5min.parquet"
            ).set_index("date")

        def at(clock: str) -> pd.Series:
            return result.loc[pd.Timestamp(f"{day} {clock}", tz=canonical_5m.IST)]

        self.assertTrue(np.isnan(at("09:15")["AVWAP"]))
        self.assertEqual(at("09:20")["AVWAP"], 100.0)
        self.assertEqual(at("09:25")["AVWAP"], 106.0)
        self.assertTrue(np.isnan(at("09:30")["AVWAP"]))
        self.assertEqual(at("09:35")["AVWAP"], 113.0)


if __name__ == "__main__":
    unittest.main()
