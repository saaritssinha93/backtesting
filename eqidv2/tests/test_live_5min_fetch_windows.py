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

    def info(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
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

    def test_empty_exchange_backfill_uses_synthetic_zero_volume_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "TEST_stocks_indicators_5min.parquet"
            current_stamps = pd.date_range(
                start=_ist(9, 15),
                end=_ist(11, 55),
                freq="5min",
            )
            # A prior end-stamped session prevents the legacy one-time
            # start-to-end migration heuristic from shifting today's rows.
            prior_stamp = pd.Timestamp(
                core.IST_TZ.localize(datetime(2026, 6, 10, 9, 20))
            )
            stamps = pd.DatetimeIndex([prior_stamp]).append(current_stamps)
            pd.DataFrame(
                {
                    "date": stamps,
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 0.0,
                    "gap_filled": 0,
                }
            ).to_parquet(out_path, index=False)

            with (
                patch.dict(core.DIRS["5min"], {"out": tmp}, clear=False),
                patch.object(
                    core,
                    "expected_last_stamp",
                    return_value={
                        "kind": "ts",
                        "value": _ist(12, 5).to_pydatetime(),
                        "step_min": 5,
                    },
                ),
                patch.object(
                    core,
                    "_fetch_missing_5min_session_rows",
                    return_value=pd.DataFrame(),
                ),
            ):
                report = core.process_ticker(
                    "5min",
                    "TEST",
                    123,
                    object(),
                    _ist(9, 15).to_pydatetime(),
                    _ist(12, 5).to_pydatetime(),
                    _Logger(),
                    set(),
                    False,
                    "end",
                    tmp,
                    False,
                    5,
                )

            saved = pd.read_parquet(out_path)
            saved_dates = pd.to_datetime(saved["date"])
            tail = saved.loc[
                saved_dates.isin([_ist(12, 0), _ist(12, 5)]),
                ["date", "volume", "gap_filled"],
            ]

        self.assertEqual(report.status, "updated")
        self.assertEqual(report.new_rows_count, 2)
        self.assertEqual(tail["date"].tolist(), [_ist(12, 0), _ist(12, 5)])
        self.assertEqual(tail["volume"].tolist(), [0.0, 0.0])
        self.assertEqual(tail["gap_filled"].tolist(), [1, 1])


if __name__ == "__main__":
    unittest.main()
