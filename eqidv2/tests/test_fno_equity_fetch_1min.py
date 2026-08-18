from __future__ import annotations

import io
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import fno_equity_fetch_1min as feed
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v5_live as live
import fno_v5_live_config as config


SESSION = date(2026, 8, 10)
SIGNAL_END = "09:25"


def _candidate(symbol: str, token: int, *, side: str = "LONG") -> dict:
    return {
        "tradingsymbol": symbol,
        "instrument_token": token,
        "futures_tradingsymbol": f"{symbol}26AUGFUT",
        "signal_timestamp": config.slot_datetime(SESSION, SIGNAL_END).isoformat(),
        "signal_end": SIGNAL_END,
        "side": side,
        "signal_close": 100.0,
        "tick_size": 0.05,
        "lot_size": 1,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
    }


def _snapshot(candidates: list[dict], *, state: str = "SUCCESS") -> dict:
    return {
        "schema_version": "fno_v5_scanner_5m_hybrid_v3",
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": SESSION.isoformat(),
        "signal_end": SIGNAL_END,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[SIGNAL_END],
        "state": state,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "candidates": candidates,
    }


def _persisted_bar(candidate: dict, confirmation_end, *, close: float = 100.0) -> dict:
    return {
        "timestamp": confirmation_end.isoformat(),
        "candle_start": (confirmation_end - timedelta(minutes=1)).isoformat(),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": close,
        "volume": 1000,
        "tradingsymbol": candidate["tradingsymbol"],
        "instrument_token": candidate["instrument_token"],
        "exchange": "NSE",
        "source": "KITE_HISTORICAL_COMPLETED_1M",
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "fetched_at_ist": confirmation_end.isoformat(),
    }


class ScriptedHistoricalClient:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict] = []

    def historical_data(
        self,
        instrument_token,
        from_date,
        to_date,
        interval,
        *,
        continuous,
        oi,
    ):
        self.calls.append(
            {
                "instrument_token": instrument_token,
                "from_date": from_date,
                "to_date": to_date,
                "interval": interval,
                "continuous": continuous,
                "oi": oi,
            }
        )
        if self.error is not None:
            raise self.error
        return list(self.response or [])


class ForbiddenBrokerPool:
    def historical_data(self, *args, **kwargs):  # pragma: no cover - safety trap
        raise AssertionError("confirmation consumer attempted direct API polling")


class DurableEquityOneMinuteFeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.raw_patch = patch.object(common, "EQUITY_1M_RAW_DIR", root / "raw")
        self.slot_patch = patch.object(common, "EQUITY_1M_SLOT_DIR", root / "markers")
        self.raw_patch.start()
        self.slot_patch.start()

    def tearDown(self) -> None:
        self.slot_patch.stop()
        self.raw_patch.stop()
        self.temp_dir.cleanup()

    @property
    def signal_start(self):
        return config.slot_datetime(SESSION, SIGNAL_END)

    @property
    def confirmation_end(self):
        return config.slot_datetime(
            SESSION, config.SIGNAL_TO_CONFIRMATION[SIGNAL_END]
        )

    def _runtime(self, client) -> feed.AppRuntime:
        return feed.AppRuntime("test-app", client, pace_seconds=0.0)

    def test_exact_completed_bar_is_persisted_and_final_marker_is_idempotent(self) -> None:
        candidate = _candidate("ONE", 101)
        snapshot = _snapshot([candidate])
        client = ScriptedHistoricalClient(
            [
                {
                    "date": self.signal_start,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.5,
                    "close": 100.5,
                    "volume": 1000,
                }
            ]
        )
        now = self.confirmation_end + timedelta(seconds=10)

        with patch.object(common, "now_ist", return_value=now):
            first = feed.produce_slot(
                snapshot,
                "v5",
                SESSION,
                SIGNAL_END,
                [self._runtime(client)],
            )
            second = feed.produce_slot(
                snapshot, "v5", SESSION, SIGNAL_END, []
            )

        self.assertEqual(first, second)
        self.assertEqual(first["state"], "SUCCESS")
        self.assertTrue(first["complete"])
        self.assertEqual(first["written_symbols"], ["ONE"])
        self.assertEqual(first["verified_no_candle_symbols"], [])
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["from_date"], self.signal_start)
        self.assertEqual(client.calls[0]["interval"], "minute")
        self.assertFalse(client.calls[0]["continuous"])
        self.assertFalse(client.calls[0]["oi"])
        self.assertTrue(common.equity_1m_path(SESSION, "ONE").exists())
        self.assertTrue(Path(first["slot_data_path"]).exists())
        self.assertEqual(feed._sha256_file(Path(first["slot_data_path"])), first["slot_data_sha256"])

    def test_persist_bar_normalizes_mixed_naive_and_aware_existing_timestamps(self) -> None:
        candidate = _candidate("MIXEDTZ", 109)
        path = common.equity_1m_path(SESSION, candidate["tradingsymbol"])
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "timestamp": "2026-08-10 09:20:00",
                    "candle_start": "2026-08-10 09:19:00",
                    "tradingsymbol": "MIXEDTZ",
                },
                {
                    "timestamp": "2026-08-10T09:21:00+05:30",
                    "candle_start": "2026-08-10T09:20:00+05:30",
                    "tradingsymbol": "MIXEDTZ",
                },
            ]
        ).to_parquet(path, index=False)
        bar = {
            "timestamp": self.confirmation_end.isoformat(),
            "candle_start": self.signal_start.isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000,
        }

        persisted = feed._persist_bar(
            candidate, bar, SESSION, self.confirmation_end
        )

        self.assertEqual(persisted["tradingsymbol"], "MIXEDTZ")
        stored = pd.read_parquet(path)
        timestamps = feed._coerce_ist_timestamps(stored["timestamp"])
        self.assertEqual(str(timestamps.dt.tz), "Asia/Kolkata")
        self.assertIn(pd.Timestamp(self.confirmation_end), set(timestamps))

    def test_three_early_empty_responses_cannot_become_verified_no_candle(self) -> None:
        candidate = _candidate("EARLY", 102)
        snapshot = _snapshot([candidate])
        early = self.confirmation_end + timedelta(seconds=14)
        response = {
            "tradingsymbol": "EARLY",
            "state": "NO_CANDLE",
            "error": "",
            "observed_at_ist": early.isoformat(),
        }

        with (
            patch.object(common, "now_ist", side_effect=[early, early, early]),
            patch.object(feed, "fetch_candidates", return_value=[response]) as fetch,
        ):
            marker = feed.produce_slot(
                snapshot,
                "v5",
                SESSION,
                SIGNAL_END,
                [SimpleNamespace()],
                observations=3,
                retry_delay_sec=0.0,
            )

        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(marker["source"], "provisional")
        self.assertEqual(marker["verified_no_candle_symbols"], [])
        self.assertEqual(marker["unverified_no_candle_symbols"], ["EARLY"])
        self.assertFalse(marker["complete"])

    def test_verified_no_candle_candidate_is_ineligible_while_written_candidate_proceeds(self) -> None:
        written = _candidate("WRITTEN", 201)
        idle = _candidate("IDLE", 202)
        snapshot = _snapshot([written, idle])
        starts = [
            self.confirmation_end + timedelta(seconds=15),
            self.confirmation_end + timedelta(seconds=17),
            self.confirmation_end + timedelta(seconds=19),
        ]
        written_bar = _persisted_bar(written, self.confirmation_end)
        fetch_results = [
            [
                {
                    "tradingsymbol": "WRITTEN",
                    "state": "WRITTEN",
                    "error": "",
                    "bar": written_bar,
                    "observed_at_ist": starts[0].isoformat(),
                },
                {
                    "tradingsymbol": "IDLE",
                    "state": "NO_CANDLE",
                    "error": "",
                    "observed_at_ist": starts[0].isoformat(),
                },
            ],
            [
                {
                    "tradingsymbol": "IDLE",
                    "state": "NO_CANDLE",
                    "error": "",
                    "observed_at_ist": starts[1].isoformat(),
                }
            ],
            [
                {
                    "tradingsymbol": "IDLE",
                    "state": "NO_CANDLE",
                    "error": "",
                    "observed_at_ist": starts[2].isoformat(),
                }
            ],
        ]

        with (
            patch.object(
                common, "now_ist", side_effect=[starts[0], starts[2], starts[2]]
            ),
            patch.object(feed, "fetch_candidates", side_effect=fetch_results),
            patch.object(feed.time, "sleep"),
        ):
            marker = feed.produce_slot(
                snapshot,
                "v5",
                SESSION,
                SIGNAL_END,
                [SimpleNamespace()],
            )

        self.assertEqual(marker["state"], "SUCCESS")
        self.assertEqual(marker["written_symbols"], ["WRITTEN"])
        self.assertEqual(marker["no_candle_symbols"], ["IDLE"])
        self.assertEqual(marker["verified_no_candle_symbols"], ["IDLE"])
        self.assertEqual(marker["unverified_no_candle_symbols"], [])
        self.assertEqual(marker["resolved_symbols"], ["IDLE", "WRITTEN"])
        self.assertEqual(marker["candidate_resolution_policy"], "ALL_WRITTEN_OR_VERIFIED_NO_CANDLE")
        self.assertIsNone(marker["verified_no_candle_cap"])
        self.assertIsNone(marker["written_bar_minimum_ratio"])
        self.assertEqual(marker["no_candle_observations"]["IDLE"], 3)

        args = SimpleNamespace(
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with patch.object(live, "_archive_json_evidence"):
            confirmation = live.process_confirmation_slot(
                snapshot,
                SESSION,
                SIGNAL_END,
                ForbiddenBrokerPool(),
                args,
            )

        self.assertEqual(confirmation["state"], "SUCCESS")
        self.assertEqual(confirmation["confirmation_bars"], 1)
        self.assertEqual(confirmation["ineligible_no_candle_symbols"], ["IDLE"])
        self.assertEqual(
            confirmation["candidate_rejections"],
            {"IDLE": "INELIGIBLE_NO_CANDLE"},
        )
        self.assertFalse(hasattr(live, "fetch_confirmation_bars"))

    def test_api_invalid_unverified_and_unexpected_outcomes_fail_closed(self) -> None:
        candidate = _candidate("BROKEN", 301)
        snapshot = _snapshot([candidate])
        now = self.confirmation_end + timedelta(seconds=20)
        cases = {
            "api": (
                [
                    {
                        "tradingsymbol": "BROKEN",
                        "state": "FAILED",
                        "error": "TimeoutError: test",
                        "observed_at_ist": now.isoformat(),
                    }
                ],
                "api_failed_symbols",
            ),
            "invalid": (
                [
                    {
                        "tradingsymbol": "BROKEN",
                        "state": "INVALID_DATA",
                        "error": "invalid_ohlcv_geometry",
                        "observed_at_ist": now.isoformat(),
                    }
                ],
                "invalid_symbols",
            ),
            "unverified_no_candle": (
                [
                    {
                        "tradingsymbol": "BROKEN",
                        "state": "NO_CANDLE",
                        "error": "",
                        "observed_at_ist": now.isoformat(),
                    }
                ],
                "unverified_no_candle_symbols",
            ),
            "unexpected": ([], "unexpected_missing_symbols"),
        }
        for name, (first_result, failure_field) in cases.items():
            with self.subTest(name=name):
                with tempfile.TemporaryDirectory() as case_dir:
                    with (
                        patch.object(common, "EQUITY_1M_RAW_DIR", Path(case_dir) / "raw"),
                        patch.object(common, "EQUITY_1M_SLOT_DIR", Path(case_dir) / "markers"),
                        patch.object(common, "now_ist", side_effect=[now, now, now]),
                        patch.object(
                            feed,
                            "fetch_candidates",
                            side_effect=[first_result, [], []],
                        ),
                        patch.object(feed.time, "sleep"),
                    ):
                        marker = feed.produce_slot(
                            snapshot,
                            "v5",
                            SESSION,
                            SIGNAL_END,
                            [SimpleNamespace()],
                            finalize_incomplete=True,
                        )
                self.assertEqual(marker["source"], "final")
                self.assertEqual(marker["state"], "BLOCKED_INCOMPLETE_DATA")
                self.assertFalse(marker["complete"])
                self.assertEqual(marker[failure_field], ["BROKEN"])

    def test_wrong_slot_and_malformed_ohlc_are_invalid(self) -> None:
        expected = self.confirmation_end
        wrong = {
            "timestamp": (expected + timedelta(minutes=1)).isoformat(),
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 1,
        }
        malformed = {
            "timestamp": expected.isoformat(),
            "open": 100,
            "high": 99,
            "low": 101,
            "close": 100,
            "volume": 1,
        }
        self.assertEqual(feed._validate_bar(wrong, expected), "wrong_candle_end")
        self.assertEqual(
            feed._validate_bar(malformed, expected), "invalid_ohlcv_geometry"
        )

    def test_zero_negative_and_nonfinite_prices_are_invalid(self) -> None:
        expected = self.confirmation_end
        base = {
            "timestamp": expected.isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 0,
        }
        cases = {
            "zero": ({**base, "open": 0.0, "low": 0.0}, "non_positive_ohlc"),
            "negative": (
                {**base, "open": -2.0, "high": -1.0, "low": -3.0, "close": -2.0},
                "non_positive_ohlc",
            ),
            "nan": ({**base, "close": float("nan")}, "non_finite_ohlcv"),
            "infinity": ({**base, "high": float("inf")}, "non_finite_ohlcv"),
            "negative_volume": (
                {**base, "volume": -1},
                "invalid_ohlcv_geometry",
            ),
        }
        for name, (bar, expected_error) in cases.items():
            with self.subTest(name=name):
                self.assertEqual(feed._validate_bar(bar, expected), expected_error)

    def test_marker_and_scanner_candidate_hash_tampering_are_rejected(self) -> None:
        candidate = _candidate("HASHED", 401)
        snapshot = _snapshot([candidate])
        client = ScriptedHistoricalClient(
            [
                {
                    "date": self.signal_start,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 100,
                }
            ]
        )
        now = self.confirmation_end + timedelta(seconds=10)
        with patch.object(common, "now_ist", return_value=now):
            marker = feed.produce_slot(
                snapshot, "v5", SESSION, SIGNAL_END, [self._runtime(client)]
            )
        marker_path = feed._marker_path("v5", self.confirmation_end, snapshot)

        tampered = dict(marker)
        tampered["candidate_contract_sha256"] = "0" * 64
        common.atomic_write_json(marker_path, tampered)
        with patch.object(live, "_archive_json_evidence"):
            _, errors, _ = live._load_completed_confirmation_feed(
                snapshot, SESSION, SIGNAL_END
            )
        self.assertEqual(
            errors["_feed"],
            "durable_confirmation_marker_candidate_contract_sha256_mismatch",
        )

        common.atomic_write_json(marker_path, marker)
        embedded_tamper = dict(marker)
        embedded_tamper["scanner_snapshot"] = {
            **marker["scanner_snapshot"],
            "state": "PARTIAL",
        }
        common.atomic_write_json(marker_path, embedded_tamper)
        with patch.object(live, "_archive_json_evidence"):
            _, errors, _ = live._load_completed_confirmation_feed(
                snapshot, SESSION, SIGNAL_END
            )
        self.assertEqual(
            errors["_feed"],
            "durable_confirmation_marker_scanner_snapshot_tampered",
        )

        changed_snapshot = {
            **snapshot,
            "candidates": [{**candidate, "side": "SHORT"}],
        }
        with patch.object(live, "_archive_json_evidence"):
            _, errors, _ = live._load_completed_confirmation_feed(
                changed_snapshot, SESSION, SIGNAL_END
            )
        self.assertEqual(errors["_feed"], "durable_confirmation_marker_missing")

    def test_existing_invalid_final_marker_is_never_overwritten(self) -> None:
        candidate = _candidate("IMMUTABLE", 425)
        snapshot = _snapshot([candidate])
        marker_path = feed._marker_path("v5", self.confirmation_end, snapshot)
        invalid = {
            "schema_version": common.EQUITY_1M_SLOT_SCHEMA_VERSION,
            "feed_policy": feed.FEED_POLICY_VERSION,
            "source": "final",
            "scanner_snapshot_sha256": feed.scanner_snapshot_sha256(snapshot),
            "candidate_contract_sha256": "0" * 64,
        }
        common.atomic_write_json(marker_path, invalid)
        before = marker_path.read_bytes()

        with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
            feed.produce_slot(snapshot, "v5", SESSION, SIGNAL_END, [])

        self.assertEqual(marker_path.read_bytes(), before)

    def test_nonidentical_slot_data_writer_cannot_replace_first_snapshot(self) -> None:
        path = common.EQUITY_1M_SLOT_DIR / "immutable.parquet"
        first = pd.DataFrame([{"tradingsymbol": "FIRST", "close": 100.0}])
        rival = pd.DataFrame([{"tradingsymbol": "RIVAL", "close": 200.0}])

        first_hash = feed._publish_slot_data_once(path, first)
        before = path.read_bytes()
        self.assertEqual(feed._publish_slot_data_once(path, first), first_hash)
        with self.assertRaisesRegex(RuntimeError, "data collision"):
            feed._publish_slot_data_once(path, rival)

        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(feed._sha256_file(path), first_hash)

    def test_consumer_hashes_and_parses_the_same_slot_bytes(self) -> None:
        candidate = _candidate("BYTELOCK", 426)
        snapshot = _snapshot([candidate])
        client = ScriptedHistoricalClient(
            [
                {
                    "date": self.signal_start,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.25,
                    "volume": 100,
                }
            ]
        )
        now = self.confirmation_end + timedelta(seconds=10)
        with patch.object(common, "now_ist", return_value=now):
            marker = feed.produce_slot(
                snapshot, "v5", SESSION, SIGNAL_END, [self._runtime(client)]
            )
        data_path = Path(marker["slot_data_path"])
        real_read_parquet = pd.read_parquet
        replacement = real_read_parquet(data_path)
        replacement.loc[:, "close"] = 100.75
        observed_sources: list[object] = []

        def swap_path_after_byte_read(source, *args, **kwargs):
            observed_sources.append(source)
            common.atomic_write_parquet(replacement, data_path)
            return real_read_parquet(source, *args, **kwargs)

        with (
            patch.object(live, "_archive_json_evidence"),
            patch.object(live.pd, "read_parquet", side_effect=swap_path_after_byte_read),
        ):
            bars, errors, _ = live._load_completed_confirmation_feed(
                snapshot, SESSION, SIGNAL_END
            )

        self.assertEqual(errors, {})
        self.assertEqual(len(observed_sources), 1)
        self.assertIsInstance(observed_sources[0], io.BytesIO)
        self.assertEqual(float(bars["BYTELOCK"]["close"]), 100.25)

    def test_future_dated_no_candle_evidence_is_rejected(self) -> None:
        candidate = _candidate("FUTUREPROOF", 450)
        snapshot = _snapshot([candidate])
        observed = [
            self.confirmation_end + timedelta(seconds=15),
            self.confirmation_end + timedelta(seconds=17),
            self.confirmation_end + timedelta(seconds=19),
        ]
        results = [
            [
                {
                    "tradingsymbol": "FUTUREPROOF",
                    "state": "NO_CANDLE",
                    "error": "",
                    "observed_at_ist": stamp.isoformat(),
                }
            ]
            for stamp in observed
        ]
        with (
            patch.object(
                common,
                "now_ist",
                side_effect=[observed[0], observed[-1], observed[-1]],
            ),
            patch.object(feed, "fetch_candidates", side_effect=results),
            patch.object(feed.time, "sleep"),
        ):
            marker = feed.produce_slot(
                snapshot,
                "v5",
                SESSION,
                SIGNAL_END,
                [SimpleNamespace()],
            )
        self.assertEqual(marker["state"], "SUCCESS")

        marker["observation_history"]["FUTUREPROOF"][-1][
            "observed_at_ist"
        ] = (observed[-1] + timedelta(minutes=1)).isoformat()
        marker_path = feed._marker_path("v5", self.confirmation_end, snapshot)
        common.atomic_write_json(marker_path, marker)
        with patch.object(live, "_archive_json_evidence"):
            _, errors, _ = live._load_completed_confirmation_feed(
                snapshot, SESSION, SIGNAL_END
            )
        self.assertEqual(
            errors["_feed"], "durable_confirmation_marker_evidence_invalid"
        )

    def test_deadline_equality_succeeds_and_one_microsecond_late_is_rejected(self) -> None:
        for name, delta, expected_state in (
            ("equal", timedelta(seconds=90), "SUCCESS"),
            ("late", timedelta(seconds=90, microseconds=1), "LATE_COMPLETE"),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as case_dir:
                candidate = _candidate(name.upper(), 500 if name == "equal" else 501)
                snapshot = _snapshot([candidate])
                bar = _persisted_bar(candidate, self.confirmation_end)
                published = self.confirmation_end + delta
                outcome = {
                    "tradingsymbol": candidate["tradingsymbol"],
                    "state": "WRITTEN",
                    "error": "",
                    "bar": bar,
                    "observed_at_ist": published.isoformat(),
                }
                with (
                    patch.object(common, "EQUITY_1M_RAW_DIR", Path(case_dir) / "raw"),
                    patch.object(common, "EQUITY_1M_SLOT_DIR", Path(case_dir) / "markers"),
                    patch.object(
                        common,
                        "now_ist",
                        side_effect=[published, published, published],
                    ),
                    patch.object(feed, "fetch_candidates", return_value=[outcome]),
                ):
                    marker = feed.produce_slot(
                        snapshot,
                        "v5",
                        SESSION,
                        SIGNAL_END,
                        [SimpleNamespace()],
                    )
                self.assertEqual(marker["state"], expected_state)
                self.assertEqual(marker["within_deadline"], name == "equal")

    def test_publication_after_persistence_controls_deadline_state(self) -> None:
        candidate = _candidate("IOEDGE", 550)
        snapshot = _snapshot([candidate])
        deadline = self.confirmation_end + timedelta(seconds=90)
        published = deadline + timedelta(microseconds=1)
        outcome = {
            "tradingsymbol": candidate["tradingsymbol"],
            "state": "WRITTEN",
            "error": "",
            "bar": _persisted_bar(candidate, self.confirmation_end),
            "observed_at_ist": deadline.isoformat(),
        }
        with (
            patch.object(
                common,
                "now_ist",
                side_effect=[deadline, deadline, published],
            ),
            patch.object(feed, "fetch_candidates", return_value=[outcome]),
        ):
            marker = feed.produce_slot(
                snapshot,
                "v5",
                SESSION,
                SIGNAL_END,
                [SimpleNamespace()],
            )

        self.assertEqual(marker["state"], "LATE_COMPLETE")
        self.assertFalse(marker["within_deadline"])
        self.assertEqual(marker["published_at_ist"], published.isoformat())

    def test_once_mode_before_completed_boundary_returns_without_api_or_marker(self) -> None:
        snapshot = _snapshot([_candidate("EARLYRUN", 601)])
        before_due = self.confirmation_end + timedelta(seconds=2)
        args = SimpleNamespace(
            generation="v5",
            session_date=SESSION.isoformat(),
            allow_non_trading_day=True,
            slot=SIGNAL_END,
            once=True,
            boundary_buffer_sec=3.0,
            max_apps=1,
            timeout_sec=1.0,
            request_interval_sec=0.0,
            observations=3,
            retry_delay_sec=2.0,
            poll_sec=0.1,
        )
        with (
            patch.object(common, "now_ist", return_value=before_due),
            patch.object(feed, "_load_scanner", return_value=snapshot) as load_scanner,
            patch.object(feed, "_build_runtimes") as build_runtimes,
            patch.object(feed, "produce_slot") as produce,
            patch.object(common, "publish_heartbeat"),
        ):
            result = feed.run(args)

        self.assertEqual(result, 2)
        load_scanner.assert_not_called()
        build_runtimes.assert_not_called()
        produce.assert_not_called()

    def test_v6_rejects_completed_boundary_buffer_drift_before_io(self) -> None:
        args = SimpleNamespace(generation="v6", boundary_buffer_sec=2.0)
        with (
            patch.object(feed, "_load_scanner") as load_scanner,
            patch.object(feed, "_build_runtimes") as build_runtimes,
            patch.object(feed, "produce_slot") as produce,
        ):
            with self.assertRaisesRegex(ValueError, "fingerprint-locked to 3.0 seconds"):
                feed.run(args)

        load_scanner.assert_not_called()
        build_runtimes.assert_not_called()
        produce.assert_not_called()

    def test_scheduler_prewarms_after_scanner_and_before_completed_boundary(self) -> None:
        snapshot = _snapshot([_candidate("PREWARM", 701)])
        before_due = self.confirmation_end + timedelta(seconds=2)
        due = self.confirmation_end + timedelta(seconds=3)
        runtime = self._runtime(ScriptedHistoricalClient())
        events: list[str] = []
        args = SimpleNamespace(
            generation="v5",
            session_date=SESSION.isoformat(),
            allow_non_trading_day=True,
            slot=SIGNAL_END,
            once=False,
            boundary_buffer_sec=3.0,
            max_apps=8,
            timeout_sec=8.0,
            request_interval_sec=0.36,
            observations=3,
            retry_delay_sec=2.0,
            poll_sec=0.1,
        )

        def build(_args):
            events.append("authenticate")
            return [runtime]

        def produce(*_args, **_kwargs):
            events.append("fetch")
            return {
                "source": "final",
                "state": "SUCCESS",
                "written_count": 1,
                "candidate_count": 1,
            }

        with (
            patch.object(common, "now_ist", side_effect=[before_due, due, due]),
            patch.object(feed, "_load_scanner", return_value=snapshot) as load_scanner,
            patch.object(feed, "_build_runtimes", side_effect=build) as build_runtimes,
            patch.object(feed, "produce_slot", side_effect=produce) as produce_mock,
            patch.object(feed, "_render_report", return_value=""),
            patch.object(feed.time, "sleep"),
            patch.object(common, "publish_status"),
            patch.object(common, "publish_heartbeat"),
            patch.object(common, "atomic_write_text"),
        ):
            result = feed.run(args)

        self.assertEqual(result, 0)
        self.assertGreaterEqual(load_scanner.call_count, 2)
        build_runtimes.assert_called_once_with(args)
        produce_mock.assert_called_once()
        self.assertEqual(events, ["authenticate", "fetch"])

    def test_scheduler_retries_failed_prewarm_without_crashing(self) -> None:
        snapshot = _snapshot([_candidate("RECOVER", 702)])
        before_due = self.confirmation_end + timedelta(seconds=2)
        due = self.confirmation_end + timedelta(seconds=3)
        runtime = self._runtime(ScriptedHistoricalClient())
        args = SimpleNamespace(
            generation="v5",
            session_date=SESSION.isoformat(),
            allow_non_trading_day=True,
            slot=SIGNAL_END,
            once=False,
            boundary_buffer_sec=3.0,
            max_apps=8,
            timeout_sec=2.0,
            request_interval_sec=0.36,
            observations=3,
            retry_delay_sec=2.0,
            poll_sec=0.1,
        )
        marker = {
            "source": "final",
            "state": "SUCCESS",
            "written_count": 1,
            "candidate_count": 1,
        }

        with (
            patch.object(common, "now_ist", side_effect=[before_due, due, due]),
            patch.object(feed, "_load_scanner", return_value=snapshot),
            patch.object(
                feed,
                "_build_runtimes",
                side_effect=[RuntimeError("temporary auth outage"), [runtime]],
            ) as build_runtimes,
            patch.object(feed, "produce_slot", return_value=marker) as produce_mock,
            patch.object(feed, "_render_report", return_value=""),
            patch.object(feed.time, "monotonic", side_effect=[0.0, 0.0, 3.0]),
            patch.object(feed.time, "sleep"),
            patch.object(common, "publish_status") as publish_status,
            patch.object(common, "publish_heartbeat"),
            patch.object(common, "atomic_write_text"),
        ):
            result = feed.run(args)

        self.assertEqual(result, 0)
        self.assertEqual(build_runtimes.call_count, 2)
        produce_mock.assert_called_once()
        self.assertTrue(
            any(
                call.args[1] == "DEGRADED"
                and call.kwargs.get("phase") == "KITE_RUNTIME_PREWARM_FAILED"
                for call in publish_status.call_args_list
            )
        )


if __name__ == "__main__":
    unittest.main()
