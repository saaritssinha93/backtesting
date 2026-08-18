"""Safety, parity, and SLA contracts for the accelerated V7 live pipeline.

These tests are deliberately local and deterministic: they exercise the
critical file/publication and scanner contracts without touching live data,
Kite, or ``C:\\TradingData``.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import avwap_5min_ID_v7_live_scan as live_scan
import avwap_5min_ID_v7_candidate_scan as active_candidate_scan
import eqidv2_eod_scheduler_for_5mins_data_live_minimal as fetch_scheduler
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as fetch_core


def _one_bar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-07-31 10:15:00+05:30")],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1_000.0],
        }
    )


class AtomicFetchWriteTests(unittest.TestCase):
    def test_failure_preserves_last_complete_snapshot(self) -> None:
        """A failed encoder must not corrupt the snapshot visible to the scanner."""

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            target = tmp_path / "TEST_stocks_indicators_5min.parquet"
            previous_bytes = b"previous-complete-snapshot"
            target.write_bytes(previous_bytes)

            def partial_write_then_fail(self, path, *args, **kwargs):
                Path(path).write_bytes(b"partial-new-snapshot")
                raise RuntimeError("simulated parquet encoder failure")

            with (
                patch.object(pd.DataFrame, "to_parquet", partial_write_then_fail),
                self.assertRaisesRegex(RuntimeError, "simulated parquet encoder failure"),
            ):
                fetch_core._finalize_and_save(_one_bar_frame(), str(target))

            self.assertEqual(target.read_bytes(), previous_bytes)
            self.assertEqual(
                list(tmp_path.iterdir()),
                [target],
                "failed temp output was not cleaned",
            )

    def test_success_replaces_snapshot_and_is_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            target = tmp_path / "TEST_stocks_indicators_5min.parquet"
            target.write_bytes(b"old-snapshot")

            fetch_core._finalize_and_save(_one_bar_frame(), str(target))

            saved = pd.read_parquet(target)
            self.assertEqual(saved["close"].tolist(), [100.5])
            self.assertEqual(saved["opening_snapshot"].tolist(), [False])
            self.assertEqual(list(tmp_path.iterdir()), [target])


class AuthoritativeFetchAccountingTests(unittest.TestCase):
    def test_outcome_precedence_and_unknown_symbols_fail_closed(self) -> None:
        summary = fetch_core._symbol_outcome_summary(
            ["a", "B", "C", "D", "E", "A"],
            current_symbols={"A", "B", "D"},
            previous_slot_symbols={"B", "C"},
            failed_symbols={"D"},
            token_missing_symbols={"E"},
            written_symbols={"A", "B", "C", "D"},
            noop_symbols={"E"},
        )

        self.assertEqual(summary["universe_symbols"], ["A", "B", "C", "D", "E"])
        self.assertEqual(summary["current_symbols"], ["A"])
        self.assertEqual(summary["previous_slot_symbols"], ["B", "C"])
        self.assertEqual(summary["failed_symbols"], ["D"])
        self.assertEqual(summary["unresolved_symbols"], ["E"])
        self.assertEqual(summary["complete_symbols"], ["A", "B", "C"])
        self.assertEqual(
            summary["outcome_counts"],
            {"current": 1, "previous_slot": 2, "failed": 1, "unresolved": 1},
        )
        self.assertEqual(
            summary["universe_count"],
            sum(summary["outcome_counts"].values()),
        )

    def test_unmentioned_assigned_symbol_is_never_counted_complete(self) -> None:
        summary = fetch_core._symbol_outcome_summary(
            ["KNOWN", "UNREPORTED"],
            current_symbols={"KNOWN"},
        )

        self.assertEqual(summary["complete_symbols"], ["KNOWN"])
        self.assertEqual(summary["unresolved_symbols"], ["UNREPORTED"])
        self.assertEqual(summary["complete_count"], 1)
        self.assertEqual(summary["unresolved_count"], 1)

    def test_missing_spec_reads_last_timestamp_once(self) -> None:
        """The freshness scan must not reopen each Parquet just to classify it."""

        now_ist = pd.Timestamp("2026-07-31 10:15:10+05:30").to_pydatetime()
        expected = pd.Timestamp("2026-07-31 10:15:00+05:30").to_pydatetime()
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "TEST_stocks_indicators_5min.parquet"
            target.touch()
            with (
                patch.object(
                    fetch_core,
                    "expected_last_stamp",
                    return_value={"kind": "ts", "value": expected, "step_min": 5},
                ),
                patch.object(
                    fetch_core,
                    "_read_last_ts_from_store",
                    return_value=pd.Timestamp(expected),
                ) as read_last,
                patch.object(
                    fetch_core,
                    "DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS",
                    False,
                ),
            ):
                result = fetch_core.missing_spec(
                    "5min",
                    str(target),
                    now_ist,
                    set(),
                    "end",
                )

        self.assertEqual(result["kind"], "fresh")
        read_last.assert_called_once_with(str(target))


class ExactScannerCacheParityTests(unittest.TestCase):
    @staticmethod
    def _bars(stamps: list[str]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(stamps),
                "open": [100.0 + i for i in range(len(stamps))],
                "high": [101.0 + i for i in range(len(stamps))],
                "low": [99.0 + i for i in range(len(stamps))],
                "close": [100.5 + i for i in range(len(stamps))],
                "volume": [1_000.0 + i for i in range(len(stamps))],
            }
        )

    @staticmethod
    def _signature(candidates) -> list[tuple[str, str, str]]:
        return [
            (
                str(candidate.setup),
                pd.Timestamp(candidate.signal_ts).isoformat(),
                pd.Timestamp(candidate.entry_ts).isoformat(),
            )
            for candidate in candidates
        ]

    def test_cache_reuses_exact_prepared_frame_and_invalidates_on_file_change(self) -> None:
        first_bars = self._bars(
            [
                "2026-07-31 10:05:00+05:30",
                "2026-07-31 10:10:00+05:30",
                "2026-07-31 10:15:00+05:30",
            ]
        )
        next_bars = self._bars(
            [
                "2026-07-31 10:05:00+05:30",
                "2026-07-31 10:10:00+05:30",
                "2026-07-31 10:15:00+05:30",
                "2026-07-31 10:20:00+05:30",
            ]
        )
        prepare_calls: list[int] = []

        def fake_prepare(frame: pd.DataFrame) -> pd.DataFrame:
            prepare_calls.append(len(frame))
            prepared = frame.copy()
            prepared["date_only"] = prepared["date"].dt.date
            return prepared

        def fake_scan_day(day_df: pd.DataFrame, ticker: str, market_ctx):
            return [
                SimpleNamespace(
                    ticker=ticker,
                    setup="C_OR_BREAKOUT",
                    signal_ts=day_df["date"].iloc[-2],
                    entry_ts=day_df["date"].iloc[-1],
                )
            ]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            live_path = tmp_path / "TEST_stocks_indicators_5min.parquet"
            first_bars.to_parquet(live_path, index=False)
            live_scan.reset_scan_caches(shutdown_pool_workers=False)
            try:
                with (
                    patch.object(live_scan, "LIVE_5M_DIR", tmp_path),
                    patch.object(live_scan.v2, "_prepare_5m", side_effect=fake_prepare),
                    patch.object(live_scan.v2, "_scan_day", side_effect=fake_scan_day),
                ):
                    cold_telemetry = live_scan._new_ticker_telemetry()
                    cold = live_scan.scan_ticker_live(
                        "TEST",
                        "2026-07-31 10:15:00+05:30",
                        {},
                        telemetry=cold_telemetry,
                    )
                    warm_telemetry = live_scan._new_ticker_telemetry()
                    warm = live_scan.scan_ticker_live(
                        "TEST",
                        "2026-07-31 10:15:00+05:30",
                        {},
                        telemetry=warm_telemetry,
                    )

                    self.assertEqual(self._signature(warm), self._signature(cold))
                    self.assertEqual(prepare_calls, [3])
                    self.assertEqual(cold_telemetry["raw_cache_misses"], 1)
                    self.assertEqual(cold_telemetry["prepared_cache_misses"], 1)
                    self.assertEqual(warm_telemetry["raw_cache_hits"], 1)
                    self.assertEqual(warm_telemetry["prepared_cache_hits"], 1)

                    fetch_core._finalize_and_save(next_bars, str(live_path))
                    changed_telemetry = live_scan._new_ticker_telemetry()
                    changed = live_scan.scan_ticker_live(
                        "TEST",
                        "2026-07-31 10:20:00+05:30",
                        {},
                        telemetry=changed_telemetry,
                    )

                self.assertEqual(prepare_calls, [3, 4])
                self.assertEqual(changed_telemetry["raw_cache_misses"], 1)
                self.assertEqual(changed_telemetry["prepared_cache_misses"], 1)
                self.assertNotEqual(self._signature(changed), self._signature(warm))
            finally:
                live_scan.reset_scan_caches(shutdown_pool_workers=False)

    def test_scan_slot_honors_explicit_chunksize_and_records_telemetry(self) -> None:
        observed: dict[str, object] = {}

        class FakePool:
            def map(self, function, payloads, *, chunksize):
                payloads = list(payloads)
                observed["payloads"] = payloads
                observed["chunksize"] = chunksize
                return [
                    ([], live_scan._new_ticker_telemetry())
                    for _ in payloads
                ]

        with patch.object(live_scan, "_get_scan_pool", return_value=FakePool()):
            short_df, long_df = live_scan.scan_slot(
                "2026-07-31 10:15:00+05:30",
                ["a", "b", "c"],
                max_workers=2,
                chunksize=7,
            )

        self.assertTrue(short_df.empty)
        self.assertTrue(long_df.empty)
        self.assertEqual(observed["chunksize"], 7)
        self.assertEqual(
            [ticker for ticker, _ in observed["payloads"]],
            ["A", "B", "C"],
        )
        telemetry = live_scan.get_last_scan_telemetry()
        self.assertEqual(telemetry["chunksize"], 7)
        self.assertEqual(telemetry["ticker_count"], 3)
        self.assertEqual(telemetry["ticker_errors"], 0)

    def test_active_candidate_scanner_unpacks_worker_telemetry_and_chunksize(self) -> None:
        """The module imported by persistent V7 must use the optimized protocol."""

        observed: dict[str, object] = {}

        class FakePool:
            def map(self, function, payloads, *, chunksize):
                payloads = list(payloads)
                observed["payloads"] = payloads
                observed["chunksize"] = chunksize
                return [
                    ([], active_candidate_scan._new_ticker_telemetry())
                    for _ in payloads
                ]

        with (
            patch.object(active_candidate_scan, "_get_scan_pool", return_value=FakePool()),
            patch.object(active_candidate_scan, "DEFAULT_SCAN_CHUNKSIZE", 7),
        ):
            frame = active_candidate_scan.scan_slot_candidates(
                "2026-07-31 10:15:00+05:30",
                ["a", "b", "c"],
                max_workers=2,
            )

        self.assertTrue(frame.empty)
        self.assertEqual(observed["chunksize"], 7)
        self.assertEqual(
            [ticker for ticker, *_ in observed["payloads"]],
            ["A", "B", "C"],
        )
        telemetry = active_candidate_scan.get_last_scan_telemetry()
        self.assertEqual(telemetry["chunksize"], 7)
        self.assertEqual(telemetry["ticker_count"], 3)
        self.assertEqual(telemetry["ticker_errors"], 0)

    def test_active_candidate_cache_preserves_results_and_invalidates_exactly(self) -> None:
        first_bars = self._bars(
            [
                "2026-07-31 10:05:00+05:30",
                "2026-07-31 10:10:00+05:30",
                "2026-07-31 10:15:00+05:30",
            ]
        )
        next_bars = self._bars(
            [
                "2026-07-31 10:05:00+05:30",
                "2026-07-31 10:10:00+05:30",
                "2026-07-31 10:15:00+05:30",
                "2026-07-31 10:20:00+05:30",
            ]
        )
        prepare_calls: list[int] = []

        def fake_prepare(frame: pd.DataFrame) -> pd.DataFrame:
            prepare_calls.append(len(frame))
            prepared = frame.copy()
            prepared["date_only"] = prepared["date"].dt.date
            return prepared

        def fake_scan_day(scan_df: pd.DataFrame, ticker: str, market_ctx):
            signal_ts = scan_df["date"].iloc[-2]
            return [
                SimpleNamespace(
                    ticker=ticker,
                    setup="C_OR_BREAKOUT",
                    signal_ts=signal_ts,
                )
            ]

        def signature(found) -> list[tuple[str, str, float]]:
            return [
                (
                    str(candidate.setup),
                    pd.Timestamp(candidate.signal_ts).isoformat(),
                    float(signal_row["close"]),
                )
                for candidate, signal_row in found
            ]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            live_path = tmp_path / "TEST_stocks_indicators_5min.parquet"
            first_bars.to_parquet(live_path, index=False)
            active_candidate_scan.reset_scan_caches(shutdown_pool_workers=False)
            try:
                with (
                    patch.object(active_candidate_scan, "LIVE_5M_DIR", tmp_path),
                    patch.object(
                        active_candidate_scan.v2,
                        "_prepare_5m",
                        side_effect=fake_prepare,
                    ),
                    patch.object(
                        active_candidate_scan.v2,
                        "_scan_day",
                        side_effect=fake_scan_day,
                    ),
                    patch.object(
                        active_candidate_scan,
                        "_scan_early_slot_candidates",
                        return_value=[],
                    ),
                    patch.object(
                        active_candidate_scan,
                        "_scan_late_bb10_signal",
                        return_value=None,
                    ),
                ):
                    cold_telemetry = active_candidate_scan._new_ticker_telemetry()
                    cold = active_candidate_scan.scan_ticker_signal_candle(
                        "TEST",
                        "2026-07-31 10:15:00+05:30",
                        {},
                        telemetry=cold_telemetry,
                    )
                    warm_telemetry = active_candidate_scan._new_ticker_telemetry()
                    warm = active_candidate_scan.scan_ticker_signal_candle(
                        "TEST",
                        "2026-07-31 10:15:00+05:30",
                        {},
                        telemetry=warm_telemetry,
                    )

                    self.assertEqual(signature(warm), signature(cold))
                    self.assertEqual(prepare_calls, [3])
                    self.assertEqual(warm_telemetry["raw_cache_hits"], 1)
                    self.assertEqual(warm_telemetry["prepared_cache_hits"], 1)

                    fetch_core._finalize_and_save(next_bars, str(live_path))
                    changed_telemetry = active_candidate_scan._new_ticker_telemetry()
                    changed = active_candidate_scan.scan_ticker_signal_candle(
                        "TEST",
                        "2026-07-31 10:20:00+05:30",
                        {},
                        telemetry=changed_telemetry,
                    )

                self.assertEqual(prepare_calls, [3, 4])
                self.assertEqual(changed_telemetry["raw_cache_misses"], 1)
                self.assertEqual(changed_telemetry["prepared_cache_misses"], 1)
                self.assertNotEqual(signature(changed), signature(warm))
            finally:
                active_candidate_scan.reset_scan_caches(shutdown_pool_workers=False)


class CanonicalUniverseContractTests(unittest.TestCase):
    def test_feed_manifest_is_canonical_hashed_and_atomically_published(self) -> None:
        slot = fetch_scheduler.IST.localize(datetime(2026, 7, 31, 10, 15))
        expected_symbols = ["HDFCBANK", "INFY", "RELIANCE"]
        expected_hash = hashlib.sha256(
            "\n".join(expected_symbols).encode("utf-8")
        ).hexdigest()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "feed_universe_5m.json"
            payload = fetch_scheduler._write_feed_universe_manifest(
                slot,
                [" reliance ", "INFY", "hdfcbank", "RELIANCE", ""],
                path=path,
            )
            persisted = json.loads(path.read_text(encoding="utf-8"))

            self.assertEqual(list(Path(tmp).iterdir()), [path])

        self.assertEqual(payload["symbols"], expected_symbols)
        self.assertEqual(payload["universe_count"], 3)
        self.assertEqual(payload["universe_sha256"], expected_hash)
        self.assertEqual(persisted, payload)


class SchedulerAuthoritativeCompletionTests(unittest.TestCase):
    class _FakeProcess:
        exitcode = 0

    def test_scheduler_private_freshness_patch_rejects_previous_slot(self) -> None:
        expected = fetch_scheduler.IST.localize(datetime(2026, 7, 31, 12, 10))
        previous = expected - pd.Timedelta(minutes=5)

        with (
            patch.object(
                fetch_scheduler.core,
                "expected_last_stamp",
                return_value={"kind": "ts", "value": expected, "step_min": 5},
            ),
            patch.object(
                fetch_scheduler.core,
                "DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS",
                False,
            ),
        ):
            self.assertFalse(
                fetch_scheduler._ticker_is_fresh_from_last_ts_strict(
                    "5min",
                    "unused.parquet",
                    previous,
                    expected,
                    set(),
                    "end",
                )
            )
            self.assertTrue(
                fetch_scheduler._ticker_is_fresh_from_last_ts_strict(
                    "5min",
                    "unused.parquet",
                    expected,
                    expected,
                    set(),
                    "end",
                )
            )

        self.assertIs(
            fetch_scheduler.core._ticker_is_fresh_from_last_ts,
            fetch_scheduler._ticker_is_fresh_from_last_ts_strict,
        )

    def _run_incomplete_persistent_slot(self):
        slot = fetch_scheduler.IST.localize(datetime(2026, 7, 31, 10, 15))
        partition_summary = {
            "universe_symbols": ["A", "B"],
            "current_symbols": ["A"],
            "previous_slot_symbols": [],
            "complete_symbols": ["A"],
            "failed_symbols": [],
            "unresolved_symbols": ["B"],
            "token_missing_symbols": [],
            "written_symbols": ["A"],
            "noop_symbols": ["B"],
            "universe_count": 2,
            "current_count": 1,
            "previous_slot_count": 0,
            "complete_count": 1,
            "failed_count": 0,
            "unresolved_count": 1,
            "token_missing_count": 0,
        }
        persistent_runner = MagicMock(
            return_value=(
                [("app1", self._FakeProcess())],
                {
                    "app1": (
                        True,
                        "worker_returned_but_one_symbol_is_unresolved",
                        0.01,
                        2,
                        0,
                        [],
                        partition_summary,
                    )
                },
                {},
                {"app1": 0},
            )
        )
        marker = MagicMock()
        status = MagicMock()
        manifest = MagicMock(
            return_value={
                "universe_count": 2,
                "universe_sha256": fetch_scheduler._universe_sha256(["A", "B"]),
                "symbols": ["A", "B"],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch.object(
                    fetch_scheduler.core,
                    "load_stocks_universe",
                    return_value=(["A", "B"], {"A": 1, "B": 2}),
                ),
                patch.object(
                    fetch_scheduler,
                    "_mapped_fno_equities",
                    return_value=pd.DataFrame(
                        {
                            "equity_symbol": ["A", "B"],
                            "equity_instrument_token": [1, 2],
                        }
                    ),
                ),
                patch.object(
                    fetch_scheduler,
                    "_build_working_app_partitions",
                    return_value=(
                        [("app1", ["A", "B"], {"A": 1, "B": 2}, "test-user")],
                        [],
                    ),
                ),
                patch.object(
                    fetch_scheduler,
                    "DEFAULT_PERSISTENT_PARTITION_WORKERS",
                    True,
                ),
                patch.object(
                    fetch_scheduler,
                    "_run_persistent_partition_jobs",
                    persistent_runner,
                ),
                patch.object(fetch_scheduler, "_publish_slot_completion_marker", marker),
                patch.object(fetch_scheduler, "_write_slot_status", status),
                patch.object(fetch_scheduler, "_write_feed_universe_manifest", manifest),
            ):
                summary = fetch_scheduler.run_update_5m_once(
                    max_workers=8,
                    max_workers_per_app=8,
                    report_dir=tmp,
                    buffer_sec=0,
                    refresh_tokens=False,
                    slot_end=slot,
                    ready_marker_enabled=False,
                )

        return {
            "slot": slot,
            "summary": summary,
            "persistent_runner": persistent_runner,
            "marker": marker,
            "status": status,
            "manifest": manifest,
        }

    def test_persistent_worker_enabled_path_dispatches_without_spawn_fallback(self) -> None:
        result = self._run_incomplete_persistent_slot()

        result["persistent_runner"].assert_called_once()

    def test_persistent_partition_reuses_kite_session_while_auth_is_unchanged(self) -> None:
        setup_calls: list[object] = []
        sessions_seen: list[object] = []
        sink_payloads: list[tuple] = []
        shared_session_cache: dict = {}
        expected = fetch_scheduler.IST.localize(datetime(2026, 7, 31, 10, 15))

        def base_setup():
            session = object()
            setup_calls.append(session)
            return session

        def fake_run_partition(
            mode,
            partition_name,
            partition_tickers,
            partition_token_map,
            setup_kite_fn,
            **kwargs,
        ):
            sessions_seen.append(setup_kite_fn())
            return {
                "verify_failed_count": 0,
                "verify_failed_sample": [],
                "universe_symbols": ["A"],
                "current_symbols": ["A"],
                "complete_symbols": ["A"],
                "universe_count": 1,
                "current_count": 1,
                "complete_count": 1,
                "failed_count": 0,
                "unresolved_count": 0,
                "token_missing_count": 0,
            }

        class Sink:
            def put(self, payload):
                sink_payloads.append(payload)

        with (
            patch.object(fetch_scheduler, "_setup_fn_map", return_value={"app1": base_setup}),
            patch.object(fetch_scheduler, "_run_partition", side_effect=fake_run_partition),
        ):
            for _ in range(2):
                fetch_scheduler._run_partition_worker(
                    "5min",
                    "app1",
                    ["A"],
                    {"A": 1},
                    "app1",
                    max_workers=1,
                    report_dir="unused",
                    holidays=set(),
                    refresh_tokens=False,
                    intraday_ts="end",
                    skip_if_fresh=True,
                    expected_ts_ist=expected,
                    result_queue=Sink(),
                    session_cache=shared_session_cache,
                )

        self.assertEqual(len(setup_calls), 1)
        self.assertEqual(len(sessions_seen), 2)
        self.assertIs(sessions_seen[0], sessions_seen[1])
        self.assertEqual(len(sink_payloads), 2)
        self.assertTrue(all(payload[1] is True for payload in sink_payloads))

    def test_completion_marker_uses_confirmed_complete_not_assigned_count(self) -> None:
        result = self._run_incomplete_persistent_slot()
        result["marker"].assert_called_once()
        marker_kwargs = result["marker"].call_args.kwargs

        self.assertEqual(marker_kwargs["tickers_expected"], 2)
        self.assertEqual(marker_kwargs["tickers_written"], 1)
        self.assertTrue(
            marker_kwargs["failures"],
            "an unresolved authoritative symbol must force an incomplete slot",
        )

        result["status"].assert_called_once()
        status_kwargs = result["status"].call_args.kwargs
        self.assertEqual(status_kwargs["universe_count"], 2)
        self.assertEqual(status_kwargs["current_symbol_count"], 1)
        self.assertEqual(status_kwargs["unresolved_symbol_count"], 1)

    def test_full_feed_run_publishes_the_canonical_manifest(self) -> None:
        result = self._run_incomplete_persistent_slot()

        result["manifest"].assert_called_once()
        manifest_args = result["manifest"].call_args.args
        self.assertEqual(manifest_args[0], result["slot"])
        self.assertEqual(manifest_args[1], ["A", "B"])
        self.assertEqual(result["summary"]["universe_count"], 2)
        self.assertEqual(
            result["summary"]["universe_sha256"],
            fetch_scheduler._universe_sha256(["A", "B"]),
        )


class LauncherFastPathConfigTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parents[1]

    @staticmethod
    def _batch_value(text: str, name: str) -> str | None:
        match = re.search(
            rf'(?im)^\s*set\s+"?{re.escape(name)}=([^"\r\n]*)"?\s*$',
            text,
        )
        return match.group(1).strip() if match else None

    def test_fetch_launcher_enables_persistent_bounded_workers(self) -> None:
        text = (
            self.ROOT / "bat" / "run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat"
        ).read_text(encoding="utf-8", errors="replace")

        self.assertEqual(
            self._batch_value(text, "EQIDV2_5M_PERSISTENT_PARTITION_WORKERS"),
            "1",
        )
        self.assertEqual(
            self._batch_value(text, "EQIDV2_FNO_5M_FROM_1M"),
            "1",
        )
        per_app = self._batch_value(text, "MAX_WORKERS_PER_APP")
        total = self._batch_value(text, "MAX_WORKERS")
        self.assertIsNotNone(per_app)
        self.assertIsNotNone(total)
        self.assertLessEqual(int(per_app), 20)
        self.assertLessEqual(int(total), 8 * int(per_app))

    def test_scanner_launcher_starts_immediately_and_uses_small_chunks(self) -> None:
        text = (
            self.ROOT / "bat" / "run_eqidv2_signal_discovery_v7_5min_id_persistent.bat"
        ).read_text(encoding="utf-8", errors="replace")

        post_slot_delay = self._batch_value(
            text,
            "EQIDV2_SIGNAL_DISCOVERY_V7_POST_SLOT_DELAY_SEC",
        )
        chunksize = self._batch_value(
            text,
            "EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_CHUNKSIZE",
        )
        self.assertIsNotNone(post_slot_delay)
        self.assertIsNotNone(chunksize)
        self.assertLessEqual(int(post_slot_delay), 5)
        self.assertLessEqual(int(chunksize), 12)
        self.assertEqual(
            self._batch_value(
                text,
                "EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_CACHE_ENABLED",
            ),
            "1",
        )

    def test_persistent_scanner_wires_process_pool_prewarm(self) -> None:
        source = (
            self.ROOT / "eqidv2_signal_discovery_v7_5min_id_persistent.py"
        ).read_text(encoding="utf-8", errors="replace")

        self.assertIn(
            "candidate_scan.prewarm_scan_pool(",
            source,
            "the active candidate pool must start before the first critical slot",
        )


if __name__ == "__main__":
    unittest.main()
