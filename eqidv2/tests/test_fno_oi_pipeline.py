from __future__ import annotations

import re
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from unittest import mock

import pandas as pd

import fno_oi_common as common
import fno_oi_eod_qc as eod_qc
import fno_oi_feature_ranker as ranker
import fno_oi_fetch_5min as fetcher
import log_dashboard_server as dashboard
import preopen_session_autofix as preopen_autofix
import preopen_session_healthcheck as preopen


def _instrument(
    symbol: str,
    underlying: str,
    expiry: str,
    token: int,
    *,
    segment: str = "NFO-FUT",
    instrument_type: str = "FUT",
) -> dict[str, object]:
    return {
        "instrument_token": token,
        "exchange_token": token + 1000,
        "tradingsymbol": symbol,
        "name": underlying,
        "last_price": 0,
        "expiry": expiry,
        "strike": 0,
        "tick_size": 0.05,
        "lot_size": 250,
        "instrument_type": instrument_type,
        "segment": segment,
        "exchange": "NFO",
    }


def _contract(symbol: str = "RELIANCE26AUGFUT", token: int = 101) -> pd.Series:
    return pd.Series(
        {
            "underlying": "RELIANCE",
            "tradingsymbol": symbol,
            "instrument_token": token,
            "exchange_token": token + 1000,
            "expiry": pd.Timestamp("2026-08-27"),
            "lot_size": 500,
            "tick_size": 0.05,
            "is_index_future": False,
        }
    )


def _fetch_universe(
    stock_underlyings: list[str],
    index_underlyings: list[str] | None = None,
) -> pd.DataFrame:
    rows = []
    all_underlyings = [
        *((underlying, False) for underlying in stock_underlyings),
        *((underlying, True) for underlying in (index_underlyings or [])),
    ]
    for index, (underlying, is_index) in enumerate(all_underlyings, start=1):
        rows.append(
            {
                "exchange": "NFO",
                "tradingsymbol": f"{underlying}26AUGFUT",
                "underlying": underlying,
                "instrument_token": 30_000 + index,
                "exchange_token": 40_000 + index,
                "expiry": pd.Timestamp("2026-08-27"),
                "lot_size": 100,
                "tick_size": 0.05,
                "is_index_future": is_index,
            }
        )
    return pd.DataFrame(rows)


def _raw_row(timestamp: pd.Timestamp, close: float, oi: float, volume: float = 100.0) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "underlying": "RELIANCE",
        "tradingsymbol": "RELIANCE26AUGFUT",
        "instrument_token": 101,
        "expiry": pd.Timestamp("2026-08-27"),
        "open": close,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": volume,
        "oi": oi,
        "quality_state": "VALID",
    }


class FnOOIPipelineTests(unittest.TestCase):
    def test_universe_filters_futures_and_selects_nearest_expiry(self) -> None:
        records = [
            _instrument("RELIANCE26AUGFUT", "RELIANCE", "2026-08-27", 101),
            _instrument("RELIANCE26SEPFUT", "RELIANCE", "2026-09-24", 102),
            _instrument("NIFTY26AUGFUT", "NIFTY", "2026-08-27", 103),
            _instrument("NIFTYFPI26AUGFUT", "NIFTYFPI", "2026-08-27", 106),
            _instrument(
                "RELIANCE26AUG3000CE",
                "RELIANCE",
                "2026-08-27",
                104,
                segment="NFO-OPT",
                instrument_type="CE",
            ),
            _instrument("OLD26JULFUT", "OLD", "2026-07-30", 105),
        ]
        master = common.normalize_futures_master(records, session_date=date(2026, 8, 10))
        universe = common.select_near_month(master)

        self.assertEqual(set(master["tradingsymbol"]), {
            "RELIANCE26AUGFUT",
            "RELIANCE26SEPFUT",
            "NIFTY26AUGFUT",
            "NIFTYFPI26AUGFUT",
        })
        self.assertEqual(set(universe["tradingsymbol"]), {
            "RELIANCE26AUGFUT",
            "NIFTY26AUGFUT",
            "NIFTYFPI26AUGFUT",
        })
        self.assertEqual(master.set_index("tradingsymbol").loc["RELIANCE26AUGFUT", "underlying"], "RELIANCE")
        self.assertEqual(master.set_index("tradingsymbol").loc["RELIANCE26AUGFUT", "contract_month"], "2026-08")
        self.assertTrue(
            master.set_index("tradingsymbol").loc[
                "NIFTYFPI26AUGFUT", "is_index_future"
            ]
        )

    def test_contract_registry_preserves_expired_contract_history(self) -> None:
        august = date(2026, 8, 10)
        initial = common.normalize_futures_master(
            [
                _instrument("RELIANCE26AUGFUT", "RELIANCE", "2026-08-27", 101),
                _instrument("RELIANCE26SEPFUT", "RELIANCE", "2026-09-24", 102),
            ],
            session_date=august,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "contract_registry.parquet"
            with mock.patch.object(common, "CONTRACT_REGISTRY_PATH", registry_path):
                common.update_contract_registry(
                    initial,
                    common.select_near_month(initial),
                    session_date=august,
                )
                september = date(2026, 9, 1)
                current = common.normalize_futures_master(
                    [_instrument("RELIANCE26SEPFUT", "RELIANCE", "2026-09-24", 102)],
                    session_date=september,
                )
                registry = common.update_contract_registry(
                    current,
                    common.select_near_month(current),
                    session_date=september,
                ).set_index("tradingsymbol")

        self.assertEqual(registry.loc["RELIANCE26AUGFUT", "status"], "EXPIRED")
        self.assertEqual(registry.loc["RELIANCE26SEPFUT", "status"], "ACTIVE")
        self.assertEqual(registry.loc["RELIANCE26AUGFUT", "first_seen"].date(), august)
        self.assertEqual(registry.loc["RELIANCE26AUGFUT", "last_seen"].date(), august)

    def test_historical_candles_are_end_stamped_and_keep_real_oi(self) -> None:
        records = [
            {
                "date": "2026-08-10T09:15:00+05:30",
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "volume": 1200,
                "oi": 456789,
            },
            {
                "date": "2026-08-10T09:20:00+05:30",
                "open": 101,
                "high": 103,
                "low": 100,
                "close": 102,
                "volume": 1300,
                "oi": 457000,
            },
        ]
        slot = datetime(2026, 8, 10, 9, 20, tzinfo=common.IST)
        rows = common.normalize_historical_candles(records, _contract(), slot_end=slot)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows.iloc[0]["timestamp"], pd.Timestamp(slot))
        self.assertEqual(rows.iloc[0]["candle_start"].strftime("%H:%M"), "09:15")
        self.assertEqual(rows.iloc[0]["oi"], 456789)
        self.assertEqual(rows.iloc[0]["quality_state"], "VALID")
        self.assertEqual(rows.iloc[0]["contract_month"], "2026-08")

    def test_cash_marker_state_requires_complete_live_feed(self) -> None:
        slot = datetime(2026, 8, 10, 9, 25, tzinfo=common.IST)
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            common, "CASH_SLOT_DIR", Path(temp_dir)
        ):
            marker_path = common.cash_slot_path(slot)
            common.atomic_write_json(
                marker_path,
                {
                    "slot_ist": slot.isoformat(),
                    "source": "final",
                    "complete": False,
                },
            )
            self.assertEqual(
                fetcher._cash_marker_state(slot),
                (False, "final_incomplete"),
            )

            common.atomic_write_json(
                marker_path,
                {
                    "slot_ist": slot.isoformat(),
                    "source": "final",
                    "complete": True,
                },
            )
            self.assertEqual(fetcher._cash_marker_state(slot), (True, "complete"))

    def test_failed_fetcher_auto_refresh_marks_universe_heartbeat_crashed(self) -> None:
        session = date(2026, 8, 12)
        args = mock.Mock(timeout_sec=8.0, max_apps=8)
        with (
            mock.patch.object(
                common,
                "load_near_month_universe",
                side_effect=ValueError("stale universe"),
            ),
            mock.patch.object(
                fetcher.fno_oi_universe,
                "refresh_universe",
                side_effect=RuntimeError("mapping incomplete"),
            ),
            mock.patch.object(common, "publish_status") as publish,
        ):
            with self.assertRaisesRegex(RuntimeError, "mapping incomplete"):
                fetcher.ensure_universe(session, args)

        publish.assert_called_once()
        self.assertEqual(publish.call_args.args[:2], ("fno_oi_universe", "FAILED"))
        self.assertEqual(publish.call_args.kwargs["heartbeat_state"], "CRASHED")
        self.assertEqual(publish.call_args.kwargs["phase"], "AUTO_REFRESH_FAILED")

    def test_fno_fetch_starts_before_cash_feed_completion(self) -> None:
        slot = datetime(2026, 8, 10, 9, 25, tzinfo=common.IST)
        universe = pd.DataFrame([{"tradingsymbol": "RELIANCE26AUGFUT"}])
        runtime = mock.Mock(app_name="app1")
        args = mock.Mock(
            max_retries=1,
            slot_retry_attempts=0,
            slot_retry_delay_sec=0.0,
            min_coverage=1.0,
        )
        events: list[str] = []

        def cash_state(_: datetime) -> tuple[bool, str]:
            events.append("cash_check")
            return (False, "missing") if len(events) == 1 else (True, "complete")

        def fetch_contracts(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
            events.append("fno_fetch")
            return [{"tradingsymbol": "RELIANCE26AUGFUT", "state": "WRITTEN"}]

        with (
            mock.patch.object(fetcher, "_cash_marker_state", side_effect=cash_state),
            mock.patch.object(fetcher, "fetch_contracts", side_effect=fetch_contracts),
            mock.patch.object(common, "atomic_write_json"),
            mock.patch.object(common, "atomic_write_text"),
            mock.patch.object(common, "publish_status"),
            mock.patch.object(common, "universe_sha256", return_value="test-sha"),
        ):
            marker = fetcher.run_slot(slot, universe, [runtime], args)

        self.assertEqual(events, ["cash_check", "fno_fetch", "cash_check"])
        self.assertEqual(marker["cash_marker_state_at_start"], "missing")
        self.assertEqual(marker["cash_marker_state"], "complete")
        self.assertTrue(marker["complete"])

    def test_fetch_marker_allows_verified_sail_skip_and_excludes_index_coverage(self) -> None:
        slot = datetime(2026, 8, 17, 9, 25, tzinfo=common.IST)
        stocks = [f"STOCK{index:03d}" for index in range(207)] + ["SAIL"]
        indexes = sorted(common.INDEX_UNDERLYINGS)
        universe = _fetch_universe(stocks, indexes)
        no_candle = {"SAIL26AUGFUT", "NIFTYFPI26AUGFUT"}
        runtime = mock.Mock(app_name="app1")
        args = mock.Mock(
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=0.0,
            min_coverage=0.99,
        )

        def outcomes(frame: pd.DataFrame, *_args: object, **_kwargs: object) -> list[dict]:
            return [
                {
                    "tradingsymbol": symbol,
                    "state": "NO_CANDLE" if symbol in no_candle else "WRITTEN",
                }
                for symbol in frame["tradingsymbol"].astype(str)
            ]

        with (
            mock.patch.object(fetcher, "_cash_marker_state", return_value=(True, "complete")),
            mock.patch.object(fetcher, "fetch_contracts", side_effect=outcomes) as fetch,
            mock.patch.object(fetcher.time, "sleep"),
            mock.patch.object(common, "atomic_write_json"),
            mock.patch.object(common, "atomic_write_text"),
            mock.patch.object(common, "publish_status"),
        ):
            marker = fetcher.run_slot(slot, universe, [runtime], args)

        self.assertEqual(fetch.call_count, 3)
        self.assertEqual(
            set(fetch.call_args_list[1].args[0]["tradingsymbol"]), no_candle
        )
        self.assertEqual(
            set(fetch.call_args_list[2].args[0]["tradingsymbol"]), no_candle
        )
        self.assertEqual(marker["schema_version"], "fno_oi_fetch_slot_v2")
        self.assertTrue(marker["complete"])
        self.assertEqual(marker["state"], "SUCCESS")
        self.assertEqual(marker["contracts_written"], 212)
        self.assertEqual(marker["contracts_expected"], 214)
        self.assertEqual(marker["no_candle_symbols"], sorted(no_candle))
        self.assertEqual(marker["stock_contracts_expected"], 208)
        self.assertEqual(marker["stock_contracts_written"], 207)
        self.assertAlmostEqual(marker["stock_coverage_ratio"], 207 / 208)
        self.assertEqual(
            marker["stock_verified_no_candle_symbols"], ["SAIL26AUGFUT"]
        )
        self.assertEqual(
            marker["index_no_candle_symbols"], ["NIFTYFPI26AUGFUT"]
        )
        self.assertEqual(marker["no_candle_observations"]["SAIL26AUGFUT"], 3)
        self.assertEqual(
            marker["no_candle_observations"]["NIFTYFPI26AUGFUT"], 3
        )

    def test_no_candle_verification_counts_clean_empty_observations_only(self) -> None:
        slot = datetime(2026, 8, 17, 9, 25, tzinfo=common.IST)
        universe = _fetch_universe(["SAIL"])
        runtime = mock.Mock(app_name="app1")
        args = mock.Mock(
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=0.0,
            min_coverage=0.99,
        )
        states = iter(("FAILED", "NO_CANDLE", "NO_CANDLE"))

        def outcomes(*_args: object, **_kwargs: object) -> list[dict]:
            return [{"tradingsymbol": "SAIL26AUGFUT", "state": next(states)}]

        with (
            mock.patch.object(fetcher, "_cash_marker_state", return_value=(True, "complete")),
            mock.patch.object(fetcher, "fetch_contracts", side_effect=outcomes),
            mock.patch.object(fetcher.time, "sleep"),
            mock.patch.object(common, "atomic_write_json"),
            mock.patch.object(common, "atomic_write_text"),
            mock.patch.object(common, "publish_status"),
        ):
            marker = fetcher.run_slot(slot, universe, [runtime], args)

        self.assertFalse(marker["complete"])
        self.assertEqual(marker["state"], "PARTIAL")
        self.assertEqual(marker["verified_no_candle_symbols"], [])
        self.assertEqual(marker["unverified_no_candle_symbols"], ["SAIL26AUGFUT"])
        self.assertEqual(marker["no_candle_fetch_attempts"]["SAIL26AUGFUT"], 3)
        self.assertEqual(marker["no_candle_observations"]["SAIL26AUGFUT"], 2)

    def test_api_and_invalid_outcomes_remain_fail_closed(self) -> None:
        slot = datetime(2026, 8, 17, 9, 25, tzinfo=common.IST)
        universe = _fetch_universe(["SAIL"])
        runtime = mock.Mock(app_name="app1")
        args = mock.Mock(
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=0.0,
            min_coverage=0.99,
        )
        for outcome_state, count_field in (
            ("FAILED", "failed_count"),
            ("INVALID_DATA", "invalid_data_count"),
        ):
            with self.subTest(state=outcome_state):
                with (
                    mock.patch.object(
                        fetcher,
                        "_cash_marker_state",
                        return_value=(True, "complete"),
                    ),
                    mock.patch.object(
                        fetcher,
                        "fetch_contracts",
                        return_value=[
                            {
                                "tradingsymbol": "SAIL26AUGFUT",
                                "state": outcome_state,
                            }
                        ],
                    ),
                    mock.patch.object(fetcher.time, "sleep"),
                    mock.patch.object(common, "atomic_write_json"),
                    mock.patch.object(common, "atomic_write_text"),
                    mock.patch.object(common, "publish_status"),
                ):
                    marker = fetcher.run_slot(slot, universe, [runtime], args)
                self.assertFalse(marker["complete"])
                self.assertEqual(marker["state"], "PARTIAL")
                self.assertEqual(marker[count_field], 1)

    def test_verified_skip_cap_and_coverage_floor_cannot_be_weakened(self) -> None:
        slot = datetime(2026, 8, 17, 9, 25, tzinfo=common.IST)
        runtime = mock.Mock(app_name="app1")

        def run_case(universe: pd.DataFrame, missing: set[str], min_coverage: float) -> dict:
            args = mock.Mock(
                max_retries=1,
                slot_retry_attempts=2,
                slot_retry_delay_sec=0.0,
                min_coverage=min_coverage,
            )

            def outcomes(frame: pd.DataFrame, *_args: object, **_kwargs: object) -> list[dict]:
                return [
                    {
                        "tradingsymbol": symbol,
                        "state": "NO_CANDLE" if symbol in missing else "WRITTEN",
                    }
                    for symbol in frame["tradingsymbol"].astype(str)
                ]

            with (
                mock.patch.object(fetcher, "_cash_marker_state", return_value=(True, "complete")),
                mock.patch.object(fetcher, "fetch_contracts", side_effect=outcomes),
                mock.patch.object(fetcher.time, "sleep"),
                mock.patch.object(common, "atomic_write_json"),
                mock.patch.object(common, "atomic_write_text"),
                mock.patch.object(common, "publish_status"),
            ):
                return fetcher.run_slot(slot, universe, [runtime], args)

        large = _fetch_universe([f"STOCK{index:04d}" for index in range(1000)])
        three_missing = {
            "STOCK0997 26AUGFUT".replace(" ", ""),
            "STOCK0998 26AUGFUT".replace(" ", ""),
            "STOCK0999 26AUGFUT".replace(" ", ""),
        }
        capped = run_case(large, three_missing, 0.99)
        self.assertAlmostEqual(capped["stock_coverage_ratio"], 0.997)
        self.assertFalse(capped["stock_complete"])
        self.assertFalse(capped["complete"])

        small = _fetch_universe([f"SMALL{index:02d}" for index in range(50)])
        floored = run_case(small, {"SMALL49 26AUGFUT".replace(" ", "")}, 0.80)
        self.assertAlmostEqual(floored["stock_coverage_ratio"], 0.98)
        self.assertEqual(floored["minimum_stock_coverage"], 0.99)
        self.assertFalse(floored["complete"])
        self.assertEqual(fetcher.build_parser().get_default("min_coverage"), 0.99)
        runner = (
            Path(__file__).resolve().parents[1]
            / "bat"
            / "run_fno_oi_fetch_5min.bat"
        ).read_text(encoding="utf-8")
        self.assertIn('"--min-coverage","0.99"', runner)
        self.assertIn('"--slot-retry-attempts","2"', runner)

    def test_fetch_marker_rejects_ghost_result_and_never_fabricates_empty_bar(self) -> None:
        slot = datetime(2026, 8, 17, 9, 25, tzinfo=common.IST)
        universe = _fetch_universe(["SAIL"])
        runtime = mock.Mock(app_name="app1")
        args = mock.Mock(
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=0.0,
            min_coverage=0.99,
        )
        calls = 0

        def outcomes(*_args: object, **_kwargs: object) -> list[dict]:
            nonlocal calls
            calls += 1
            symbol = "GHOST26AUGFUT" if calls == 1 else "SAIL26AUGFUT"
            return [{"tradingsymbol": symbol, "state": "WRITTEN"}]

        with (
            mock.patch.object(fetcher, "_cash_marker_state", return_value=(True, "complete")),
            mock.patch.object(fetcher, "fetch_contracts", side_effect=outcomes),
            mock.patch.object(fetcher.time, "sleep"),
            mock.patch.object(common, "atomic_write_json"),
            mock.patch.object(common, "atomic_write_text"),
            mock.patch.object(common, "publish_status"),
        ):
            marker = fetcher.run_slot(slot, universe, [runtime], args)
        self.assertFalse(marker["complete"])
        self.assertEqual(marker["unexpected_outcome_symbols"], ["GHOST26AUGFUT"])

        with (
            mock.patch.object(fetcher, "_historical_call", return_value=[]),
            mock.patch.object(common, "append_contract_rows") as append_rows,
        ):
            empty = fetcher.fetch_one_contract(
                runtime,
                universe.iloc[0],
                slot - pd.Timedelta(minutes=5),
                slot,
                slot_end=slot,
                max_retries=1,
            )
        self.assertEqual(empty["state"], "NO_CANDLE")
        self.assertEqual(empty["rows"], 0)
        append_rows.assert_not_called()

    def test_processed_slots_require_v2_evidence_for_legacy_omissions(self) -> None:
        session = date(2026, 8, 17)
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            common, "FETCH_SLOT_DIR", Path(temp_dir)
        ):
            markers = {
                "0925": {
                    "source": "final",
                    "complete": True,
                    "schema_version": "fno_oi_fetch_slot_v1",
                    "no_candle_count": 1,
                },
                "0930": {
                    "source": "final",
                    "complete": True,
                    "schema_version": "fno_oi_fetch_slot_v1",
                    "no_candle_count": 0,
                },
                "0935": {
                    "source": "final",
                    "complete": True,
                    "schema_version": common.FNO_FETCH_SLOT_SCHEMA_VERSION,
                    "readiness_policy": common.VERIFIED_NO_CANDLE_POLICY_VERSION,
                    "stock_complete": True,
                },
            }
            for hhmm, marker in markers.items():
                common.atomic_write_json(
                    Path(temp_dir) / f"slot_20260817_{hhmm}.json", marker
                )

            self.assertEqual(fetcher._today_processed_slots(session), {"0930", "0935"})

    def test_missing_oi_is_flagged_and_never_synthesized(self) -> None:
        rows = common.normalize_historical_candles(
            [{
                "date": "2026-08-10T09:15:00+05:30",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
            }],
            _contract(),
        )
        self.assertEqual(rows.iloc[0]["quality_state"], "MISSING_OI")
        self.assertTrue(pd.isna(rows.iloc[0]["oi"]))

    def test_raw_append_is_idempotent_by_token_and_timestamp(self) -> None:
        slot = datetime(2026, 8, 10, 9, 20, tzinfo=common.IST)
        first = common.normalize_historical_candles(
            [{
                "date": "2026-08-10T09:15:00+05:30",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
                "oi": 1000,
            }],
            _contract(),
            slot_end=slot,
        )
        corrected = first.copy()
        corrected["close"] = corrected["close"].astype("float64")
        corrected.loc[:, "close"] = 100.5
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "contract.parquet"
            common.append_contract_rows(path, first)
            combined = common.append_contract_rows(path, corrected)
        self.assertEqual(len(combined), 1)
        self.assertEqual(combined.iloc[0]["close"], 100.5)
        self.assertEqual(combined.iloc[0]["oi"], 1000)

    def test_slot_convention_has_75_completed_end_timestamps(self) -> None:
        slots = common.expected_slot_ends(date(2026, 8, 10))
        self.assertEqual(len(slots), 75)
        self.assertEqual(slots[0].strftime("%H:%M"), "09:20")
        self.assertEqual(slots[-1].strftime("%H:%M"), "15:30")
        self.assertEqual(
            fetcher.latest_completed_slot(
                datetime(2026, 8, 10, 9, 23, tzinfo=common.IST), set()
            ).strftime("%H:%M"),
            "09:20",
        )

    def test_bootstrap_fetches_incrementally_for_existing_contracts(self) -> None:
        universe = pd.DataFrame(
            [
                _contract("RELIANCE26AUGFUT", 101),
                _contract("HDFCBANK26AUGFUT", 102),
            ]
        )
        target = datetime(2026, 8, 10, 12, 10, tzinfo=common.IST)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            existing_path = root / "RELIANCE26AUGFUT.parquet"
            pd.DataFrame(
                {"timestamp": [pd.Timestamp("2026-08-10 12:05", tz=common.IST)]}
            ).to_parquet(existing_path, index=False)

            def path_for(symbol: str) -> Path:
                return root / f"{symbol}.parquet"

            with (
                mock.patch.object(common, "raw_contract_path", side_effect=path_for),
                mock.patch.object(common, "publish_heartbeat") as publish_heartbeat,
            ):
                planned = fetcher._bootstrap_required(
                    universe, target, bootstrap_days=60
                ).set_index("tradingsymbol")

        self.assertEqual(
            planned.loc["RELIANCE26AUGFUT", "_fetch_from"],
            pd.Timestamp("2026-08-10 12:05", tz=common.IST),
        )
        self.assertEqual(
            planned.loc["HDFCBANK26AUGFUT", "_fetch_from"],
            pd.Timestamp(target - pd.Timedelta(days=60)),
        )
        self.assertGreaterEqual(publish_heartbeat.call_count, 1)
        self.assertEqual(
            publish_heartbeat.call_args_list[0].kwargs["phase"],
            "BOOTSTRAP_AUDIT",
        )

    def test_status_write_falls_back_when_windows_replace_is_denied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "session.status"
            with (
                mock.patch.object(common, "ATOMIC_REPLACE_ATTEMPTS", 1),
                mock.patch.object(common.os, "replace", side_effect=PermissionError("sharing")),
            ):
                written = common.atomic_write_kv(path, {"status": "RUNNING", "slot": "12:10"})
            text = path.read_text(encoding="utf-8")
        self.assertTrue(written)
        self.assertIn("status=RUNNING", text)

    def test_exact_lags_day_baseline_and_material_classification(self) -> None:
        rows = []
        for minute in (10, 15, 20, 25, 30):
            rows.append(
                _raw_row(pd.Timestamp(f"2026-08-07 15:{minute}:00", tz=common.IST), 100.0, 1000.0)
            )
        current_values = [
            ("09:20", 100.00, 1000.0),
            ("09:25", 100.05, 1001.0),
            ("09:30", 100.30, 1005.0),
            ("09:35", 99.80, 1010.0),
            ("09:40", 100.20, 1000.0),
            ("09:45", 99.90, 990.0),
        ]
        for clock, close, oi in current_values:
            rows.append(_raw_row(pd.Timestamp(f"2026-08-10 {clock}:00", tz=common.IST), close, oi))

        features = ranker.build_contract_features(pd.DataFrame(rows)).set_index("timestamp")
        at_0925 = features.loc[pd.Timestamp("2026-08-10 09:25", tz=common.IST)]
        at_0930 = features.loc[pd.Timestamp("2026-08-10 09:30", tz=common.IST)]
        at_0935 = features.loc[pd.Timestamp("2026-08-10 09:35", tz=common.IST)]
        at_0940 = features.loc[pd.Timestamp("2026-08-10 09:40", tz=common.IST)]
        at_0945 = features.loc[pd.Timestamp("2026-08-10 09:45", tz=common.IST)]

        self.assertEqual(at_0925["classification"], "NEUTRAL")
        self.assertFalse(bool(at_0925["classification_threshold_pass"]))
        self.assertEqual(at_0930["classification"], "LONG_BUILDUP")
        self.assertEqual(at_0935["classification"], "SHORT_BUILDUP")
        self.assertEqual(at_0940["classification"], "SHORT_COVERING")
        self.assertEqual(at_0945["classification"], "LONG_UNWINDING")
        self.assertAlmostEqual(at_0935["oi_change_pct_15m"], 1.0, places=8)
        self.assertEqual(at_0935["prev_day_close_oi"], 1000.0)

    def test_rank_one_is_strongest_and_has_highest_percentile(self) -> None:
        snapshot = pd.DataFrame(
            {
                "tradingsymbol": ["AAA26AUGFUT", "BBB26AUGFUT", "CCC26AUGFUT"],
                "eligible_for_rank": [True, True, True],
                "oi_change_pct_5m": [3.0, 1.0, -2.0],
                "oi_change_pct_15m": [4.0, 2.0, -3.0],
                "oi_change_pct_30m": [5.0, 3.0, -4.0],
                "oi_change_pct_60m": [6.0, 4.0, -5.0],
                "oi_change_pct_day": [7.0, 5.0, -6.0],
                "oi_change_pct_from_open": [3.0, 1.0, -2.0],
                "volume_ratio": [2.0, 1.5, 1.0],
                "price_change_pct_5m": [1.0, 0.5, -0.5],
                "oi_acceleration": [2.0, 1.0, -0.5],
                "volume_acceleration": [1.0, 0.5, -0.5],
                "oi_zscore_20": [2.5, 1.0, -2.0],
            }
        )
        prior = pd.DataFrame(
            {"tradingsymbol": snapshot["tradingsymbol"], "oi_rank_5m": [3.0, 1.0, 2.0]}
        )
        with mock.patch.object(ranker, "_load_prior_ranks", return_value=prior):
            ranked = ranker.rank_feature_snapshot(
                snapshot, datetime(2026, 8, 10, 10, 30, tzinfo=common.IST)
            ).set_index("tradingsymbol")

        self.assertEqual(ranked.loc["AAA26AUGFUT", "oi_rank_5m"], 1.0)
        self.assertEqual(ranked.loc["AAA26AUGFUT", "oi_percentile_5m"], 100.0)
        self.assertEqual(ranked.loc["CCC26AUGFUT", "oi_rank_5m"], 3.0)
        self.assertEqual(ranked.loc["AAA26AUGFUT", "oi_rank_change_5m"], 2.0)

    def test_eod_qc_detects_complete_and_missing_candles(self) -> None:
        session_date = date(2026, 8, 10)
        rows = pd.DataFrame(
            [
                {
                    "timestamp": stamp,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "oi": 5000,
                }
                for stamp in common.expected_slot_ends(session_date)
            ]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "raw.parquet"
            rows.to_parquet(path, index=False)
            with mock.patch.object(common, "raw_contract_path", return_value=path):
                complete = eod_qc.inspect_contract(_contract(), session_date)
                rows.iloc[:-1].to_parquet(path, index=False)
                partial = eod_qc.inspect_contract(_contract(), session_date)

        self.assertEqual(complete["state"], "COMPLETE")
        self.assertEqual(complete["observed_candles"], 75)
        self.assertEqual(partial["state"], "PARTIAL")
        self.assertEqual(partial["missing_candles"], 1)

    def test_dashboard_places_fno_immediately_after_live_market_data(self) -> None:
        source = Path(dashboard.__file__).read_text(encoding="utf-8", errors="replace")
        groups = re.findall(r'key:\s*"([^"]+)"', source[source.index("const ACTIVE_GROUPS"):])
        self.assertGreaterEqual(len(groups), 2)
        self.assertEqual(groups[:2], ["market", "fno"])
        self.assertIn('title: "Live Market Data"', source)
        self.assertIn('title: "FnO"', source)
        for card in (
            "fno_oi_universe",
            "fno_oi_fetch_5min",
            "fno_oi_feature_ranker",
            "fno_oi_eod_qc",
        ):
            self.assertIn(card, dashboard.LOG_FILES)
            self.assertIn(card, dashboard.CARD_TASK_NAMES)
        self.assertIn("fno_oi_fetch_5min", dashboard.RESTARTABLE_CARDS)
        self.assertIn("fno_oi_feature_ranker", dashboard.RESTARTABLE_CARDS)

    def test_fno_tasks_are_preopen_visible_and_installer_defines_all_jobs(self) -> None:
        expected = {
            "EQIDV2_fno_oi_universe_0850",
            "EQIDV2_fno_oi_fetch_5min_0905",
            "EQIDV2_fno_oi_feature_ranker_0915",
            "EQIDV2_fno_v6_scanner_5min_0918",
            "EQIDV2_fno_v6_equity_1min_feed_0919",
            "EQIDV2_fno_v6_confirmation_1min_0919",
            "EQIDV2_fno_v6_live_long_0920",
            "EQIDV2_fno_v6_live_short_0920",
            "EQIDV2_fno_v6_trade_logger_0920",
            "EQIDV2_fno_v6_net_result_0920",
            "EQIDV2_fno_oi_eod_qc_1540",
        }
        self.assertTrue(expected.issubset(set(preopen.DASHBOARD_SESSION_TASKS)))
        installer = (
            Path(__file__).resolve().parents[1] / "bat" / "schedule_fno_oi_weekday.ps1"
        ).read_text(encoding="utf-8", errors="replace")
        for task in expected:
            self.assertIn(task, installer)

        start_at_0915 = expected - {
            "EQIDV2_fno_oi_universe_0850",
            "EQIDV2_fno_oi_fetch_5min_0905",
            "EQIDV2_fno_oi_eod_qc_1540",
        }
        for task in start_at_0915:
            self.assertRegex(
                installer,
                rf'Name\s*=\s*"{re.escape(task)}";\s*Time\s*=\s*"09:15"',
            )

    def test_preopen_uses_actual_trigger_before_legacy_task_name_suffix(self) -> None:
        query = "\n".join(
            (
                "TaskName: EQIDV2_fno_v6_live_long_0920",
                "Start Time: 09:15:00",
            )
        )
        with mock.patch.object(preopen, "_run_schtasks_query", return_value=query):
            self.assertEqual(
                preopen._task_scheduled_time("EQIDV2_fno_v6_live_long_0920"),
                time(9, 15),
            )

    def test_durable_confirmation_feed_task_is_required_and_autofixable(self) -> None:
        task = "EQIDV2_fno_v6_equity_1min_feed_0919"
        self.assertIn(task, preopen.REQUIRED_DASHBOARD_SESSION_TASKS)
        self.assertEqual(
            preopen_autofix.TASK_TO_BAT[task].name,
            "run_fno_v6_equity_1min_feed.bat",
        )

        for name, query in (
            ("missing", ""),
            (
                "disabled",
                "\n".join(
                    (
                        f"TaskName: {task}",
                        "Scheduled Task State: Disabled",
                        "Status: Disabled",
                    )
                ),
            ),
        ):
            with self.subTest(name=name), mock.patch.object(
                preopen, "_run_schtasks_query", return_value=query
            ):
                result = preopen.check_task_enabled_state(
                    task,
                    f"task_{task}",
                    require_run_today=False,
                    inactive_ok=task not in preopen.REQUIRED_DASHBOARD_SESSION_TASKS,
                    inactive_detail="session not enabled",
                )
            self.assertEqual(result.status, "FAIL")


if __name__ == "__main__":
    unittest.main()
