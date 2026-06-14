from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as core


class _Logger:
    def warning(self, *args, **kwargs):
        pass


def _ist(hour: int, minute: int) -> pd.Timestamp:
    return pd.Timestamp(
        core.IST_TZ.localize(datetime(2026, 6, 11, hour, minute))
    )


class Live5MinFetchWindowTests(unittest.TestCase):
    def test_finalize_and_save_stamps_opening_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "TEST_stocks_indicators_5min.parquet"
            core._finalize_and_save(
                pd.DataFrame(
                    {
                        "date": [_ist(9, 15), _ist(9, 20), _ist(15, 30)],
                        "open": [100.0, 101.0, 102.0],
                        "high": [100.5, 101.5, 102.5],
                        "low": [99.5, 100.5, 101.5],
                        "close": [100.2, 101.2, 102.2],
                        "volume": [10, 20, 30],
                    }
                ),
                str(out_path),
            )

            saved = pd.read_parquet(out_path)

        self.assertIn("opening_snapshot", saved.columns)
        self.assertEqual(saved["opening_snapshot"].tolist(), [True, False, False])

    def test_latest_missing_stamp_fetches_only_one_raw_bar(self):
        calls = []

        def fake_fetch(kite, token, start_dt, end_dt, logger, intraday_ts):
            calls.append((pd.Timestamp(start_dt), pd.Timestamp(end_dt), intraday_ts))
            return pd.DataFrame(
                {
                    "date": [_ist(13, 0)],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [10],
                }
            )

        with patch.object(core, "fetch_historical_5min_df", fake_fetch):
            result = core._fetch_missing_5min_session_rows(
                "TEST",
                object(),
                123,
                _ist(13, 0),
                [_ist(13, 0)],
                _Logger(),
            )

        self.assertEqual(calls, [(_ist(12, 55), _ist(13, 0), "end")])
        self.assertEqual(result["date"].tolist(), [_ist(13, 0)])

    def test_disjoint_missing_ranges_are_fetched_separately(self):
        calls = []

        def fake_fetch(kite, token, start_dt, end_dt, logger, intraday_ts):
            calls.append((pd.Timestamp(start_dt), pd.Timestamp(end_dt), intraday_ts))
            end_stamp = pd.Timestamp(end_dt)
            return pd.DataFrame(
                {
                    "date": [end_stamp],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [10],
                }
            )

        with patch.object(core, "fetch_historical_5min_df", fake_fetch):
            result = core._fetch_missing_5min_session_rows(
                "TEST",
                object(),
                123,
                _ist(13, 0),
                [_ist(10, 0), _ist(13, 0)],
                _Logger(),
            )

        self.assertEqual(
            calls,
            [
                (_ist(9, 55), _ist(10, 0), "end"),
                (_ist(12, 55), _ist(13, 0), "end"),
            ],
        )
        self.assertEqual(result["date"].tolist(), [_ist(10, 0), _ist(13, 0)])


if __name__ == "__main__":
    unittest.main()
