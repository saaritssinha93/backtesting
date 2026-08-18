from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

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
    def test_exact_fno_minute_aggregation_requires_all_five_rows(self):
        timestamps = pd.date_range(_ist(9, 16), periods=10, freq="1min")
        minute = pd.DataFrame(
            {
                "date": timestamps,
                "open": range(10),
                "high": [value + 2 for value in range(10)],
                "low": [value - 1 for value in range(10)],
                "close": [value + 1 for value in range(10)],
                "volume": range(1, 11),
            }
        ).drop(index=7)

        exact = core._aggregate_exact_minute_targets(
            minute,
            [_ist(9, 20), _ist(9, 25)],
        )

        self.assertEqual(exact["date"].tolist(), [_ist(9, 20)])
        self.assertEqual(exact.iloc[0]["volume"], 15.0)
        self.assertEqual(exact.iloc[0]["source_1m_count"], 5)

        zoneinfo_target = pd.Timestamp(
            datetime(2026, 6, 11, 9, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
        )
        mixed_timezone = core._aggregate_exact_minute_targets(
            minute,
            [zoneinfo_target],
        )
        self.assertEqual(len(mixed_timezone), 1)

    def test_exact_fno_fetch_builds_opening_snapshot_and_real_bar(self):
        timestamps = pd.date_range(_ist(9, 16), periods=5, freq="1min")
        minute = pd.DataFrame(
            {
                "date": timestamps,
                "open": [100.0, 101.0, 102.0, 103.0, 104.0],
                "high": [101.0, 102.0, 103.0, 104.0, 105.0],
                "low": [99.0, 100.0, 101.0, 102.0, 103.0],
                "close": [100.5, 101.5, 102.5, 103.5, 104.5],
                "volume": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )

        with patch.object(core, "fetch_historical_generic", return_value=minute):
            exact = core._fetch_exact_fno_5min_rows(
                "TEST",
                object(),
                123,
                [_ist(9, 15), _ist(9, 20)],
                _Logger(),
            )

        self.assertEqual(exact["date"].tolist(), [_ist(9, 15), _ist(9, 20)])
        self.assertEqual(exact["source_1m_count"].tolist(), [0, 5])
        self.assertEqual(exact["opening_snapshot"].tolist(), [True, False])
        self.assertEqual(exact.iloc[1]["open"], 100.0)
        self.assertEqual(exact.iloc[1]["close"], 104.5)
        self.assertEqual(exact.iloc[1]["volume"], 150.0)

    def test_exact_fno_fetch_retries_an_incomplete_minute_bucket(self):
        timestamps = pd.date_range(_ist(9, 16), periods=5, freq="1min")
        complete = pd.DataFrame(
            {
                "date": timestamps,
                "open": [100.0] * 5,
                "high": [101.0] * 5,
                "low": [99.0] * 5,
                "close": [100.5] * 5,
                "volume": [10.0] * 5,
            }
        )

        with (
            patch.object(
                core,
                "fetch_historical_generic",
                side_effect=[complete.iloc[:4], complete],
            ) as fetch,
            patch.object(core, "DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS", 2),
            patch.object(core, "DEFAULT_5M_PROVISIONAL_RETRY_INTERVAL_SEC", 0.0),
        ):
            exact = core._fetch_exact_fno_5min_rows(
                "TEST",
                object(),
                123,
                [_ist(9, 20)],
                _Logger(),
            )

        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(len(exact), 1)
        self.assertEqual(exact.iloc[0]["source_1m_count"], 5)

    def test_existing_loader_preserves_one_minute_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "TEST_stocks_indicators_5min.parquet"
            pd.DataFrame(
                {
                    "date": [_ist(9, 15), _ist(9, 20)],
                    "open": [100.0, 100.0],
                    "high": [101.0, 101.0],
                    "low": [99.0, 99.0],
                    "close": [100.5, 100.5],
                    "volume": [150.0, 150.0],
                    "gap_filled": [0, 0],
                    "opening_snapshot": [1, 0],
                    "provisional_stale": [0, 0],
                    "source_1m_count": [0, 5],
                }
            ).to_parquet(out_path, index=False)

            loaded = core._load_existing_ohlc(str(out_path), "end", "5min")

        self.assertEqual(loaded["source_1m_count"].tolist(), [0, 5])

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

    def test_opening_snapshot_does_not_trigger_legacy_timestamp_shift(self):
        frame = pd.DataFrame(
            {
                "date": [_ist(9, 15), _ist(9, 20)],
                "opening_snapshot": [True, False],
            }
        )

        converted = core._maybe_convert_existing_intraday_to_end(frame, 5)

        self.assertEqual(converted["date"].tolist(), frame["date"].tolist())

    def test_provisional_duplicate_is_refetched_and_replaced(self):
        existing = pd.DataFrame(
            {
                "date": [_ist(9, 20)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
            }
        )
        fetched = existing.copy()
        fetched["date"] = [_ist(9, 25)]
        corrected = pd.DataFrame(
            {
                "date": [_ist(9, 25)],
                "open": [100.5],
                "high": [102.0],
                "low": [100.0],
                "close": [101.5],
                "volume": [800.0],
            }
        )

        with (
            patch.object(core, "DEFAULT_5M_PROVISIONAL_SETTLE_SEC", 0.0),
            patch.object(core, "DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS", 2),
            patch.object(core, "DEFAULT_5M_PROVISIONAL_RETRY_INTERVAL_SEC", 0.0),
            patch.object(core, "fetch_historical_5min_df", return_value=corrected),
        ):
            result = core._revalidate_provisional_5min_target(
                "TEST",
                object(),
                123,
                fetched,
                existing,
                _ist(9, 25),
                _Logger(),
            )

        self.assertEqual(result.iloc[-1]["close"], 101.5)
        self.assertEqual(result.iloc[-1]["provisional_stale"], 0)

    def test_unresolved_provisional_duplicate_blocks_session_completeness(self):
        existing = pd.DataFrame(
            {
                "date": [_ist(9, 20)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
            }
        )
        fetched = existing.copy()
        fetched["date"] = [_ist(9, 25)]

        with (
            patch.object(core, "DEFAULT_5M_PROVISIONAL_SETTLE_SEC", 0.0),
            patch.object(core, "DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS", 1),
            patch.object(core, "fetch_historical_5min_df", return_value=fetched),
        ):
            result = core._revalidate_provisional_5min_target(
                "TEST",
                object(),
                123,
                fetched,
                existing,
                _ist(9, 25),
                _Logger(),
            )

        combined = pd.concat([existing, result], ignore_index=True)
        self.assertEqual(result.iloc[-1]["provisional_stale"], 1)
        self.assertEqual(
            core._missing_5min_session_stamps_from_df(combined, _ist(9, 25)),
            [_ist(9, 15), _ist(9, 25)],
        )

    def test_verified_one_minute_aggregate_duplicate_is_complete(self):
        frame = pd.DataFrame(
            {
                "date": [_ist(9, 15), _ist(9, 20), _ist(9, 25)],
                "open": [100.0, 100.0, 100.0],
                "high": [101.0, 101.0, 101.0],
                "low": [99.0, 99.0, 99.0],
                "close": [100.5, 100.5, 100.5],
                "volume": [1000.0, 1000.0, 1000.0],
                "opening_snapshot": [1, 0, 0],
                "source_1m_count": [0, 5, 5],
            }
        )

        self.assertEqual(
            core._missing_5min_session_stamps_from_df(frame, _ist(9, 25)),
            [],
        )

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
