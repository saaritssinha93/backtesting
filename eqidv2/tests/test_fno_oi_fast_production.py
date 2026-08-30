from __future__ import annotations

import argparse
import threading
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min_fast_production as producer


def _contract(symbol: str, token: int, underlying: str) -> dict[str, object]:
    return {
        "exchange": "NFO",
        "exchange_token": token + 1000,
        "tradingsymbol": symbol,
        "underlying": underlying,
        "instrument_token": token,
        "expiry": pd.Timestamp("2026-09-24"),
        "lot_size": 1,
        "tick_size": 0.05,
        "is_index_future": underlying == "NIFTY",
    }


def _slot_frame(contract: dict[str, object], slot: datetime) -> pd.DataFrame:
    return common.normalize_historical_candles(
        [
            {
                "date": slot - timedelta(minutes=5),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000,
                "oi": 5000,
            }
        ],
        contract,
        fetch_timestamp=slot + timedelta(seconds=3),
        slot_end=slot,
    )


class FastProductionTests(unittest.TestCase):
    def test_parser_keeps_legacy_defaults_and_adds_fast_worker_controls(self) -> None:
        parser = producer.build_parser()
        self.assertEqual(parser.get_default("boundary_buffer_sec"), 3.0)
        self.assertEqual(parser.get_default("request_interval_sec"), 0.36)
        self.assertEqual(parser.get_default("min_coverage"), 0.99)
        self.assertEqual(
            parser.get_default("workers_per_app"),
            producer.DEFAULT_WORKERS_PER_APP,
        )
        self.assertEqual(
            parser.get_default("writer_workers"),
            producer.DEFAULT_WRITER_WORKERS,
        )

    def test_fast_fetch_uses_full_scope_exact_window_and_canonical_archives(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        contracts = [
            _contract("RELIANCE26SEPFUT", 101, "RELIANCE"),
            _contract("NIFTY26SEPFUT", 202, "NIFTY"),
        ]
        universe = pd.DataFrame(contracts)
        outcomes = [
            {
                "tradingsymbol": item["tradingsymbol"],
                "underlying": item["underlying"],
                "app": "app1",
                "state": "WRITTEN",
                "rows": 1,
                "valid_rows": 1,
                "elapsed_sec": 0.1,
                "error": "",
                "_frame": _slot_frame(item, slot),
            }
            for item in contracts
        ]

        def append(_path: object, frame: pd.DataFrame) -> pd.DataFrame:
            return frame.copy()

        outcome_by_symbol = {
            str(item["tradingsymbol"]): item for item in outcomes
        }

        def fetch_one(
            _lane: object,
            _client: object,
            contract: dict[str, object],
            _slot: datetime,
            **_kwargs: object,
        ) -> dict[str, object]:
            return outcome_by_symbol[str(contract["tradingsymbol"])]

        lane = producer.fast_core.AppLane("app1", [object(), object()], 0.36)

        with (
            mock.patch.object(
                producer.fast_core,
                "fetch_one_contract",
                side_effect=fetch_one,
            ) as fetch,
            mock.patch.object(common, "append_contract_rows", side_effect=append) as persist,
            mock.patch.object(common, "publish_heartbeat"),
        ):
            result = producer.fetch_contracts_fast(
                universe,
                [lane],
                slot - timedelta(minutes=5),
                slot,
                slot_end=slot,
                max_retries=3,
                phase="FETCH_SLOT",
                writer_workers=2,
            )

        self.assertEqual({item["state"] for item in result}, {"WRITTEN"})
        self.assertEqual(persist.call_count, 2)
        fetched_contracts = [call.args[2] for call in fetch.call_args_list]
        self.assertEqual(
            {str(item["tradingsymbol"]) for item in fetched_contracts},
            {"RELIANCE26SEPFUT", "NIFTY26SEPFUT"},
        )
        self.assertTrue(
            all(call.kwargs["lookback_minutes"] == 5 for call in fetch.call_args_list)
        )
        self.assertTrue(
            all(
                call.kwargs["require_oi_pair"] is False
                for call in fetch.call_args_list
            )
        )
        persisted_names = {
            call.args[0].name for call in persist.call_args_list
        }
        self.assertEqual(
            persisted_names,
            {"RELIANCE26SEPFUT_5minute.parquet", "NIFTY26SEPFUT_5minute.parquet"},
        )

    def test_archive_failure_is_fail_closed_for_marker_accounting(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        contract = _contract("RELIANCE26SEPFUT", 101, "RELIANCE")
        outcome = {
            "tradingsymbol": contract["tradingsymbol"],
            "underlying": contract["underlying"],
            "app": "app1",
            "state": "WRITTEN",
            "rows": 1,
            "valid_rows": 1,
            "elapsed_sec": 0.1,
            "error": "",
            "_frame": _slot_frame(contract, slot),
        }
        with mock.patch.object(
            common,
            "append_contract_rows",
            side_effect=OSError("disk unavailable"),
        ):
            result = producer._persist_outcome(outcome, pd.Timestamp(slot))

        self.assertEqual(result["state"], "FAILED")
        self.assertEqual(result["rows"], 0)
        self.assertIn("ArchivePersistenceError", result["error"])
        self.assertIn("disk unavailable", result["error"])

    def test_warm_cache_uses_canonical_merge_and_advances_after_atomic_success(self) -> None:
        symbol = "RELIANCE26SEPFUT"
        contract = _contract(symbol, 101, "RELIANCE")
        first_slot = datetime(2026, 9, 1, 9, 20, tzinfo=common.IST)
        next_slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        existing = _slot_frame(contract, first_slot)
        incoming = _slot_frame(contract, next_slot)
        expected = common.merge_contract_rows(existing, incoming)
        cache = producer.CanonicalArchiveCache(frames={symbol: existing.copy()})

        with mock.patch.object(common, "atomic_write_parquet") as atomic_write:
            combined = cache.append(symbol, incoming, pd.Timestamp(next_slot))

        pd.testing.assert_frame_equal(combined, expected)
        pd.testing.assert_frame_equal(cache.frames[symbol], expected)
        atomic_write.assert_called_once()
        self.assertEqual(
            atomic_write.call_args.args[0]["timestamp"].tolist(),
            expected["timestamp"].tolist(),
        )

    def test_warm_cache_rolls_back_memory_when_atomic_write_fails(self) -> None:
        symbol = "RELIANCE26SEPFUT"
        contract = _contract(symbol, 101, "RELIANCE")
        first_slot = datetime(2026, 9, 1, 9, 20, tzinfo=common.IST)
        next_slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        existing = _slot_frame(contract, first_slot)
        before = existing.copy(deep=True)
        cache = producer.CanonicalArchiveCache(frames={symbol: existing})

        with mock.patch.object(
            common,
            "atomic_write_parquet",
            side_effect=OSError("atomic replace failed"),
        ):
            with self.assertRaisesRegex(OSError, "atomic replace failed"):
                cache.append(
                    symbol,
                    _slot_frame(contract, next_slot),
                    pd.Timestamp(next_slot),
                )

        pd.testing.assert_frame_equal(cache.frames[symbol], before)

    def test_fast_path_rejects_noncanonical_or_bootstrap_windows(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        with self.assertRaisesRegex(ValueError, "canonical"):
            producer._validate_live_window(
                slot - timedelta(minutes=10), slot, slot, ""
            )
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            producer._validate_live_window(
                slot - timedelta(minutes=5), slot, slot, "_fetch_from"
            )
        with self.assertRaisesRegex(ValueError, "exact live slot"):
            producer._validate_live_window(
                slot - timedelta(minutes=5), slot, None, ""
            )

    def test_retry_is_assigned_to_a_different_healthy_app(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        contract = _contract("RELIANCE26SEPFUT", 101, "RELIANCE")
        universe = pd.DataFrame([contract])
        lanes = [
            producer.fast_core.AppLane("app1", [object()], 0.36),
            producer.fast_core.AppLane("app2", [object()], 0.36),
        ]
        no_candle = {
            "tradingsymbol": contract["tradingsymbol"],
            "underlying": contract["underlying"],
            "app": "app1",
            "state": "NO_CANDLE",
            "rows": 0,
            "valid_rows": 0,
            "elapsed_sec": 0.1,
            "error": "",
        }
        written = {
            **no_candle,
            "app": "app2",
            "state": "WRITTEN",
            "rows": 1,
            "valid_rows": 1,
            "_frame": _slot_frame(contract, slot),
        }
        fetcher = producer.FastSlotFetcher(writer_workers=1)

        def fetch_one(
            lane: producer.fast_core.AppLane,
            _client: object,
            _contract_row: dict[str, object],
            _slot: datetime,
            **_kwargs: object,
        ) -> dict[str, object]:
            return no_candle if lane.app_name == "app1" else written

        with (
            mock.patch.object(
                producer.fast_core,
                "fetch_one_contract",
                side_effect=fetch_one,
            ) as fetch,
            mock.patch.object(
                common,
                "append_contract_rows",
                side_effect=lambda _path, frame: frame.copy(),
            ),
            mock.patch.object(common, "publish_heartbeat"),
        ):
            first = fetcher(
                universe,
                [lanes[0]],
                slot - timedelta(minutes=5),
                slot,
                slot_end=slot,
                max_retries=1,
                phase="FETCH_SLOT",
            )
            second = fetcher(
                universe,
                lanes[1:] + lanes[:1],
                slot - timedelta(minutes=5),
                slot,
                slot_end=slot,
                max_retries=1,
                phase="FETCH_SLOT_RETRY_1",
            )

        self.assertEqual(first[0]["state"], "NO_CANDLE")
        self.assertEqual(second[0]["state"], "WRITTEN")
        self.assertEqual(
            [call.args[0].app_name for call in fetch.call_args_list],
            ["app1", "app2"],
        )
        self.assertEqual(
            fetcher.attempted_apps_by_symbol["RELIANCE26SEPFUT"],
            ["app1", "app2"],
        )

    def test_archive_persistence_overlaps_remaining_network_fetches(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        contracts = [
            _contract("AAA26SEPFUT", 101, "AAA"),
            _contract("BBB26SEPFUT", 202, "BBB"),
        ]
        universe = pd.DataFrame(contracts)
        lane = producer.fast_core.AppLane("app1", [object(), object()], 0.36)
        call_lock = threading.Lock()
        fetch_calls = 0
        second_fetch_waiting = threading.Event()
        persistence_started = threading.Event()

        def fetch_one(
            lane_arg: producer.fast_core.AppLane,
            _client: object,
            contract: dict[str, object],
            _slot: datetime,
            **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal fetch_calls
            with call_lock:
                fetch_calls += 1
                ordinal = fetch_calls
            if ordinal == 2:
                second_fetch_waiting.set()
                if not persistence_started.wait(timeout=2.0):
                    raise AssertionError(
                        "persistence did not start while another fetch remained active"
                    )
            return {
                "tradingsymbol": contract["tradingsymbol"],
                "underlying": contract["underlying"],
                "app": lane_arg.app_name,
                "state": "WRITTEN",
                "rows": 1,
                "valid_rows": 1,
                "elapsed_sec": 0.1,
                "error": "",
                "_frame": _slot_frame(contract, slot),
            }

        def append(_path: object, frame: pd.DataFrame) -> pd.DataFrame:
            persistence_started.set()
            return frame.copy()

        with (
            mock.patch.object(
                producer.fast_core,
                "fetch_one_contract",
                side_effect=fetch_one,
            ),
            mock.patch.object(common, "append_contract_rows", side_effect=append),
            mock.patch.object(common, "publish_heartbeat"),
        ):
            outcomes = producer.fetch_contracts_fast(
                universe,
                [lane],
                slot - timedelta(minutes=5),
                slot,
                slot_end=slot,
                max_retries=1,
                phase="FETCH_SLOT",
                writer_workers=1,
            )

        self.assertTrue(second_fetch_waiting.is_set())
        self.assertTrue(persistence_started.is_set())
        self.assertEqual({item["state"] for item in outcomes}, {"WRITTEN"})

    def test_canonical_marker_is_written_only_after_cached_archive_success(self) -> None:
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        contract = _contract("RELIANCE26SEPFUT", 101, "RELIANCE")
        universe = pd.DataFrame([contract])
        lane = producer.fast_core.AppLane("app1", [object()], 0.36)
        cache = producer.CanonicalArchiveCache(
            frames={
                "RELIANCE26SEPFUT": pd.DataFrame(columns=list(common.RAW_COLUMNS))
            }
        )
        args = argparse.Namespace(
            writer_workers=1,
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=0.0,
            min_coverage=0.99,
        )
        outcome = {
            "tradingsymbol": contract["tradingsymbol"],
            "underlying": contract["underlying"],
            "app": "app1",
            "state": "WRITTEN",
            "rows": 1,
            "valid_rows": 1,
            "elapsed_sec": 0.1,
            "error": "",
            "_frame": _slot_frame(contract, slot),
        }
        events: list[str] = []
        with (
            mock.patch.object(
                producer.fast_core,
                "fetch_one_contract",
                return_value=outcome,
            ),
            mock.patch.object(
                common,
                "atomic_write_parquet",
                side_effect=lambda *_args, **_kwargs: events.append("archive"),
            ),
            mock.patch.object(
                common,
                "atomic_write_json",
                side_effect=lambda *_args, **_kwargs: events.append("marker"),
            ),
            mock.patch.object(common, "atomic_write_text"),
            mock.patch.object(common, "publish_status"),
            mock.patch.object(common, "publish_heartbeat"),
            mock.patch.object(
                producer.legacy,
                "_cash_marker_state",
                return_value=(True, "complete"),
            ),
        ):
            marker = producer.run_fast_slot(
                slot,
                universe,
                [lane],
                args,
                archive_cache=cache,
            )

        self.assertTrue(marker["complete"])
        self.assertEqual(events, ["archive", "marker"])

    def test_slot_delegates_to_canonical_marker_builder_with_fast_session(self) -> None:
        args = argparse.Namespace(writer_workers=8)
        universe = pd.DataFrame([_contract("RELIANCE26SEPFUT", 101, "RELIANCE")])
        slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        lanes = [mock.Mock(app_name="app1")]
        with mock.patch.object(
            producer.legacy,
            "run_slot",
            return_value={"complete": True},
        ) as run_slot, mock.patch.object(common, "atomic_write_text"):
            marker = producer.run_fast_slot(slot, universe, lanes, args)

        self.assertTrue(marker["complete"])
        self.assertEqual(run_slot.call_args.kwargs["session"], producer.SESSION)
        fetch_impl = run_slot.call_args.kwargs["fetch_contracts_impl"]
        self.assertIsInstance(fetch_impl, producer.FastSlotFetcher)
        self.assertEqual(fetch_impl.writer_workers, 8)


if __name__ == "__main__":
    unittest.main()
