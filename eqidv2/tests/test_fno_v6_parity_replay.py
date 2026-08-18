from __future__ import annotations

import copy
import io
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import fno_equity_fetch_1min as equity_feed
import fno_live_evidence as evidence
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v5_live as shared_live
import fno_v6_live_config as config
import fno_v6_parity_replay as replay


def _mapped_frame(count: int = 1) -> pd.DataFrame:
    rows = []
    for index in range(count):
        underlying = "TEST" if count == 1 else f"STOCK{index:03d}"
        future = f"{underlying}26AUGFUT"
        rows.append(
            {
                "exchange": "NFO",
                "tradingsymbol": future,
                "instrument_token": 10_000 + index,
                "expiry": pd.Timestamp("2026-08-27"),
                "futures_tradingsymbol": future,
                "equity_symbol": underlying,
            }
        )
    return pd.DataFrame(rows)


def _universe_payload(
    frame: pd.DataFrame, session_date: date, signal_end: str
) -> dict:
    futures = sorted(frame["futures_tradingsymbol"].str.upper())
    equities = sorted(frame["equity_symbol"].str.upper())
    return {
        "schema_version": "fno_mapped_stock_universe_evidence_v1",
        "generation": "v6",
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "contract_count": len(futures),
        "futures_symbols": futures,
        "futures_symbol_set_sha256": common.symbol_set_sha256(futures),
        "futures_universe_sha256": common.universe_sha256(frame),
        "equity_symbols": equities,
        "equity_symbol_set_sha256": common.symbol_set_sha256(equities),
        "equity_universe_sha256": common.symbol_set_sha256(equities),
    }


def _fno_marker(
    frame: pd.DataFrame,
    session_date: date,
    signal_end: str,
    skipped: set[str] | None = None,
) -> dict:
    expected_slot = config.slot_datetime(session_date, signal_end)
    expected = set(frame["futures_tradingsymbol"].str.upper())
    verified = {str(symbol).upper() for symbol in (skipped or set())}
    written = expected - verified
    coverage = len(written) / len(expected)
    return {
        "schema_version": config.FNO_FETCH_SLOT_SCHEMA_VERSION,
        "readiness_policy": config.FNO_READINESS_POLICY,
        "minimum_stock_coverage": config.MIN_STOCK_FUTURES_COVERAGE,
        "minimum_coverage": config.MIN_STOCK_FUTURES_COVERAGE,
        "maximum_verified_no_candle_stocks": config.MAX_VERIFIED_NO_CANDLE_STOCKS,
        "minimum_no_candle_fetch_attempts": config.MIN_NO_CANDLE_FETCH_ATTEMPTS,
        "slot_ist": expected_slot.isoformat(),
        "source": "final",
        "state": "SUCCESS",
        "complete": True,
        "stock_complete": True,
        "outcome_symbol_set_complete": True,
        "stock_outcome_symbol_set_complete": True,
        "failed_count": 0,
        "invalid_data_count": 0,
        "stock_failed_count": 0,
        "stock_invalid_data_count": 0,
        "contracts_expected": len(expected),
        "contracts_written": len(written),
        "no_candle_count": len(verified),
        "no_candle_symbols": sorted(verified),
        "stock_contracts_expected": len(expected),
        "stock_contracts_written": len(written),
        "stock_written_symbols": sorted(written),
        "stock_no_candle_count": len(verified),
        "stock_no_candle_symbols": sorted(verified),
        "stock_verified_no_candle_count": len(verified),
        "stock_verified_no_candle_symbols": sorted(verified),
        "stock_unverified_no_candle_symbols": [],
        "stock_coverage_ratio": coverage,
        "stock_symbol_set_sha256": common.symbol_set_sha256(expected),
        "stock_universe_sha256": common.universe_sha256(frame),
        "no_candle_observations": {
            symbol: config.MIN_NO_CANDLE_FETCH_ATTEMPTS for symbol in verified
        },
        "no_candle_fetch_attempts": {
            symbol: config.MIN_NO_CANDLE_FETCH_ATTEMPTS for symbol in verified
        },
    }


def _cash_marker(
    frame: pd.DataFrame, session_date: date, signal_end: str
) -> dict:
    count = len(frame)
    equities = set(frame["equity_symbol"].str.upper())
    return {
        "slot_ist": config.slot_datetime(session_date, signal_end).isoformat(),
        "source": "final",
        "complete": True,
        "tickers_expected": count,
        "tickers_written": count,
        "tickers_complete": count,
        "tickers_failed": 0,
        "fno_equity_expected": count,
        "fno_equity_ready": count,
        "fno_equity_failed": 0,
        "fno_equity_quality_complete": True,
        "fno_equity_universe_sha256": common.symbol_set_sha256(equities),
    }


def _scanner_snapshot(session_date: date, signal_end: str) -> dict:
    return {
        "schema_version": "fno_v6_equity_scanner_5m_v3",
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "published_at_ist": (
            config.slot_datetime(session_date, signal_end) + timedelta(seconds=5)
        ).isoformat(timespec="microseconds"),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "state": "SUCCESS",
        "contracts_skipped_no_candle": 0,
        "contracts_unexpected_missing": 0,
        "candidates": [
            {
                "tradingsymbol": "TEST",
                "instrument_token": 123,
                "futures_tradingsymbol": "TEST26AUGFUT",
                "signal_timestamp": config.slot_datetime(
                    session_date, signal_end
                ).isoformat(),
                "side": "LONG",
            }
        ],
    }


def _feed_marker(
    snapshot: dict,
    session_date: date,
    signal_end: str,
    *,
    published_at: datetime | None = None,
) -> dict:
    confirmation_hhmm = config.SIGNAL_TO_CONFIRMATION[signal_end]
    confirmation_end = config.slot_datetime(session_date, confirmation_hhmm)
    deadline = config.activation_deadline(session_date, confirmation_hhmm)
    scanner_hash = equity_feed.scanner_snapshot_sha256(snapshot)
    data_path = common.equity_1m_slot_data_path(
        confirmation_end, generation="v6", scanner_sha256=scanner_hash
    )
    empty = pd.DataFrame(
        columns=(
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "tradingsymbol",
            "instrument_token",
        )
    )
    common.atomic_write_parquet(empty, data_path)
    history = [
        {
            "state": "NO_CANDLE",
            "observed_at_ist": (confirmation_end + timedelta(seconds=offset)).isoformat(),
        }
        for offset in (15, 17, 19)
    ]
    return {
        "schema_version": config.CONFIRMATION_FEED_SCHEMA_VERSION,
        "feed_policy": config.CONFIRMATION_FEED_POLICY,
        "source": "final",
        "state": "SUCCESS",
        "complete": True,
        "within_deadline": True,
        "generation": "v6",
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": confirmation_hhmm,
        "slot_ist": confirmation_end.isoformat(),
        "deadline_ist": deadline.isoformat(),
        "minimum_no_candle_verification_ist": (
            confirmation_end
            + timedelta(seconds=config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC)
        ).isoformat(),
        "published_at_ist": (
            published_at
            or confirmation_end
            + timedelta(seconds=19)
        ).isoformat(timespec="microseconds"),
        "scanner_snapshot_sha256": scanner_hash,
        "scanner_snapshot": snapshot,
        "candidate_contract_sha256": equity_feed.candidate_contract_sha256(snapshot),
        "candidate_symbol_set_sha256": common.symbol_set_sha256(["TEST"]),
        "candidate_resolution_policy": equity_feed.NO_CANDLE_RESOLUTION_POLICY,
        "minimum_no_candle_observations": config.CONFIRMATION_NO_CANDLE_OBSERVATIONS,
        "minimum_no_candle_verification_age_sec": config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
        "minimum_no_candle_observation_spacing_sec": config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
        "configured_no_candle_observation_spacing_sec": config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
        "verified_no_candle_cap": None,
        "written_bar_minimum_ratio": None,
        "candidate_count": 1,
        "candidate_symbols": ["TEST"],
        "written_count": 0,
        "written_symbols": [],
        "no_candle_symbols": ["TEST"],
        "verified_no_candle_count": 1,
        "verified_no_candle_symbols": ["TEST"],
        "unverified_no_candle_symbols": [],
        "resolved_count": 1,
        "resolved_symbols": ["TEST"],
        "invalid_symbols": [],
        "api_failed_symbols": [],
        "unexpected_missing_symbols": [],
        "attempts_by_symbol": {"TEST": 3},
        "no_candle_observations": {"TEST": 3},
        "observation_history": {"TEST": history},
        "errors": {},
        "slot_data_path": str(data_path),
        "slot_data_sha256": equity_feed._sha256_file(data_path),
    }


def _archive(
    root: Path,
    session_date: date,
    signal_end: str,
    kind: str,
    payload: dict,
    observed_at: datetime,
) -> Path:
    return evidence.archive_json_evidence(
        root,
        generation="v6",
        session_date=session_date,
        slot=signal_end,
        artifact_kind=kind,
        payload=payload,
        observed_at=observed_at,
    )


def _archive_complete_bundle(
    root: Path,
    session_date: date,
    signal_end: str,
    *,
    observation_date: date | None = None,
    recorded_state: str = "SUCCESS",
    observation_overrides: dict[str, datetime] | None = None,
    include_feed: bool = True,
) -> dict[str, dict]:
    frame = _mapped_frame()
    universe = _universe_payload(frame, session_date, signal_end)
    fno = _fno_marker(frame, session_date, signal_end)
    cash = _cash_marker(frame, session_date, signal_end)
    scanner = _scanner_snapshot(session_date, signal_end)
    feed = _feed_marker(scanner, session_date, signal_end)
    confirmation = {
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "state": recorded_state,
    }
    day = observation_date or session_date
    observations = {
        "mapped_universe": datetime(day.year, day.month, day.day, 9, 20, tzinfo=common.IST),
        "fno_fetch_marker": datetime(day.year, day.month, day.day, 9, 25, 1, tzinfo=common.IST),
        "cash_5m_marker": datetime(day.year, day.month, day.day, 9, 25, 2, tzinfo=common.IST),
        "scanner_snapshot": datetime(day.year, day.month, day.day, 9, 25, 5, tzinfo=common.IST),
        "confirmation_feed_marker": datetime(day.year, day.month, day.day, 9, 26, 20, tzinfo=common.IST),
        "confirmation_snapshot": datetime(day.year, day.month, day.day, 9, 26, 21, tzinfo=common.IST),
    }
    observations.update(observation_overrides or {})
    payloads = {
        "mapped_universe": universe,
        "fno_fetch_marker": fno,
        "cash_5m_marker": cash,
        "scanner_snapshot": scanner,
        "confirmation_feed_marker": feed,
        "confirmation_snapshot": confirmation,
    }
    if not include_feed:
        payloads.pop("confirmation_feed_marker")
    for kind, payload in payloads.items():
        _archive(root, session_date, signal_end, kind, payload, observations[kind])
    return payloads


class FnoV6ParityReplayTests(unittest.TestCase):
    def test_archive_is_content_addressed_and_selects_earliest_or_latest(self) -> None:
        session = date(2026, 8, 18)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = _archive(
                root,
                session,
                "09:25",
                "scanner_snapshot",
                {"state": "PARTIAL"},
                config.slot_datetime(session, "09:25"),
            )
            duplicate = _archive(
                root,
                session,
                "09:25",
                "scanner_snapshot",
                {"state": "PARTIAL"},
                config.slot_datetime(session, "09:26"),
            )
            latest = _archive(
                root,
                session,
                "09:25",
                "scanner_snapshot",
                {"state": "SUCCESS"},
                config.slot_datetime(session, "09:26"),
            )
            self.assertEqual(first, duplicate)
            self.assertNotEqual(first, latest)
            self.assertEqual(
                evidence.select_revision(
                    root,
                    generation="v6",
                    session_date=session,
                    slot="09:25",
                    artifact_kind="scanner_snapshot",
                    mode="observed",
                    strict=True,
                ).payload["state"],
                "PARTIAL",
            )
            self.assertEqual(
                evidence.select_revision(
                    root,
                    generation="v6",
                    session_date=session,
                    slot="09:25",
                    artifact_kind="scanner_snapshot",
                    mode="counterfactual",
                    strict=True,
                ).payload["state"],
                "SUCCESS",
            )

    def test_strict_missing_and_tampered_evidence_fail_closed(self) -> None:
        session = date(2026, 8, 18)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self.assertRaises(evidence.EvidenceMissingError):
                evidence.select_revision(
                    root,
                    generation="v6",
                    session_date=session,
                    slot="09:25",
                    artifact_kind="cash_5m_marker",
                    mode="observed",
                    strict=True,
                )
            path = _archive(
                root,
                session,
                "09:25",
                "cash_5m_marker",
                {"complete": True},
                config.slot_datetime(session, "09:25"),
            )
            envelope = json.loads(path.read_text(encoding="utf-8"))
            envelope["payload"]["complete"] = False
            path.write_text(json.dumps(envelope), encoding="utf-8")
            with self.assertRaises(evidence.EvidenceIntegrityError):
                evidence.list_revisions(
                    root,
                    generation="v6",
                    session_date=session,
                    slot="09:25",
                    artifact_kind="cash_5m_marker",
                    strict=True,
                )
            with self.assertRaises(evidence.EvidenceIntegrityError):
                _archive(
                    root,
                    session,
                    "09:25",
                    "cash_5m_marker",
                    {"complete": True},
                    config.slot_datetime(session, "09:26"),
                )

    def test_deadline_equality_is_accepted_and_one_microsecond_is_stale(self) -> None:
        session = date(2026, 8, 18)
        deadline = config.activation_deadline(session, "09:26")
        self.assertEqual(config.ENTRY_ACTIVATION_GRACE_SEC, 90)
        self.assertEqual(
            config.strategy_payload()["confirmation_feed"][
                "activation_deadline_sec"
            ],
            config.ENTRY_ACTIVATION_GRACE_SEC,
        )
        self.assertEqual(replay.deadline_state(deadline, deadline), "IN_WINDOW")
        self.assertEqual(
            replay.deadline_state(deadline + timedelta(microseconds=1), deadline),
            "BLOCKED_STALE_ACTIVATION",
        )

    def test_v6_rejects_runtime_confirmation_deadline_override(self) -> None:
        args = shared_live.build_parser().parse_args(
            [
                "--role",
                "confirmation-1m",
                "--confirmation-max-wait-sec",
                "91",
            ]
        )
        with (
            patch.object(shared_live, "LIVE_GENERATION", "v6"),
            patch.object(shared_live, "config", config),
        ):
            with self.assertRaisesRegex(ValueError, "fingerprint-locked"):
                shared_live.run(args)

    def test_fno_replay_validator_matches_live_v2_tamper_gates(self) -> None:
        session = date(2026, 8, 18)
        frame = _mapped_frame(100)
        universe = _universe_payload(frame, session, "09:25")
        skipped = {str(frame.iloc[-1]["futures_tradingsymbol"]).upper()}
        marker = _fno_marker(frame, session, "09:25", skipped)
        slot = config.slot_datetime(session, "09:25")
        self.assertEqual(shared_live._validate_v2_fno_marker(marker, frame), "")
        self.assertEqual(replay._fno_marker_ready(marker, universe, slot), (True, "ready"))
        cases = {
            "minimum alias": (
                {"minimum_coverage": 0.5},
                "fno_fetch_marker_minimum_coverage_alias_mismatch",
            ),
            "cap policy": (
                {"maximum_verified_no_candle_stocks": 99},
                "fno_fetch_marker_no_candle_cap_mismatch",
            ),
            "attempt policy": (
                {"minimum_no_candle_fetch_attempts": 1},
                "fno_fetch_marker_no_candle_attempt_policy_mismatch",
            ),
            "outcome set": (
                {"outcome_symbol_set_complete": False},
                "fno_fetch_marker_outcome_symbol_set_incomplete",
            ),
            "total partition": (
                {"contracts_expected": 101},
                "fno_fetch_marker_incomplete_coverage",
            ),
        }
        for name, (change, reason) in cases.items():
            with self.subTest(name=name):
                tampered = {**marker, **change}
                self.assertEqual(shared_live._validate_v2_fno_marker(tampered, frame), reason)
                self.assertEqual(
                    replay._fno_marker_ready(tampered, universe, slot),
                    (False, reason),
                )
        weak = copy.deepcopy(marker)
        weak["no_candle_observations"][next(iter(skipped))] = 2
        reason = "fno_fetch_marker_no_candle_not_repeatedly_verified"
        self.assertEqual(shared_live._validate_v2_fno_marker(weak, frame), reason)
        self.assertEqual(replay._fno_marker_ready(weak, universe, slot), (False, reason))

    def test_cash_and_confirmation_tamper_gates(self) -> None:
        session = date(2026, 8, 18)
        frame = _mapped_frame()
        universe = _universe_payload(frame, session, "09:25")
        cash = _cash_marker(frame, session, "09:25")
        slot = config.slot_datetime(session, "09:25")
        self.assertEqual(replay._cash_marker_ready(cash, universe, slot), (True, "ready"))
        for key, value, reason in (
            ("tickers_written", 0, "cash_5m_marker_incomplete_coverage"),
            ("fno_equity_quality_complete", False, "cash_5m_marker_fno_equity_quality_incomplete"),
            ("fno_equity_universe_sha256", "bad", "cash_5m_marker_fno_equity_universe_mismatch"),
        ):
            with self.subTest(cash_field=key):
                self.assertEqual(
                    replay._cash_marker_ready({**cash, key: value}, universe, slot),
                    (False, reason),
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(common, "EQUITY_1M_SLOT_DIR", Path(temp_dir)):
                scanner = _scanner_snapshot(session, "09:25")
                marker = _feed_marker(scanner, session, "09:25")
                self.assertEqual(
                    replay._confirmation_feed_ready(
                        marker, scanner, session, "09:25"
                    ),
                    (True, "ready"),
                )
                deadline = config.activation_deadline(session, "09:26")
                cases = {
                    "scanner snapshot": (
                        {"scanner_snapshot": {**scanner, "state": "PARTIAL"}},
                        "confirmation_feed_scanner_snapshot_tampered",
                    ),
                    "candidate contract": (
                        {"candidate_contract_sha256": "bad"},
                        "confirmation_feed_candidate_contract_sha256_mismatch",
                    ),
                    "candidate symbols": (
                        {"candidate_symbol_set_sha256": "bad"},
                        "confirmation_feed_candidate_symbol_set_sha256_mismatch",
                    ),
                    "deadline": (
                        {"published_at_ist": (deadline + timedelta(microseconds=1)).isoformat()},
                        "confirmation_feed_late",
                    ),
                    "skip policy": (
                        {"candidate_resolution_policy": "WEAK_POLICY"},
                        "confirmation_feed_candidate_resolution_policy_mismatch",
                    ),
                    "data hash": (
                        {"slot_data_sha256": "bad"},
                        "confirmation_feed_data_hash_mismatch",
                    ),
                }
                for name, (change, reason) in cases.items():
                    with self.subTest(feed_field=name):
                        self.assertEqual(
                            replay._confirmation_feed_ready(
                                {**marker, **change}, scanner, session, "09:25"
                            ),
                            (False, reason),
                        )

                data_path = Path(marker["slot_data_path"])
                real_read_parquet = pd.read_parquet
                replacement = pd.DataFrame(
                    [
                        {
                            "timestamp": config.slot_datetime(session, "09:26").isoformat(),
                            "open": 100.0,
                            "high": 101.0,
                            "low": 99.0,
                            "close": 100.5,
                            "volume": 1,
                            "tradingsymbol": "TEST",
                            "instrument_token": 123,
                        }
                    ]
                )
                observed_sources: list[object] = []

                def swap_path_after_byte_read(source, *args, **kwargs):
                    observed_sources.append(source)
                    common.atomic_write_parquet(replacement, data_path)
                    return real_read_parquet(source, *args, **kwargs)

                with patch.object(
                    replay.pd,
                    "read_parquet",
                    side_effect=swap_path_after_byte_read,
                ):
                    self.assertEqual(
                        replay._confirmation_feed_ready(
                            marker, scanner, session, "09:25"
                        ),
                        (True, "ready"),
                    )
                self.assertEqual(len(observed_sources), 1)
                self.assertIsInstance(observed_sources[0], io.BytesIO)

    def test_observed_complete_parity_and_cli_all(self) -> None:
        session = date(2026, 8, 18)
        self.assertEqual(replay.normalize_slots(["all"]), list(config.SIGNAL_TO_CONFIRMATION))
        self.assertEqual(replay.normalize_slots(["0925", "09:30"]), ["09:25", "09:30"])
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence_root = root / "evidence"
            with patch.object(common, "EQUITY_1M_SLOT_DIR", root / "bars"):
                _archive_complete_bundle(evidence_root, session, "09:25")
                result = replay.replay_session(
                    session,
                    evidence_root=evidence_root,
                    mode="observed",
                    strict=True,
                    slots=["09:25"],
                )
            self.assertEqual(result["state"], "PARITY_MATCH")
            self.assertTrue(result["live_parity_claimed"])
            self.assertEqual(result["slots"][0]["classification"], "OBSERVED_IMMUTABLE")

    def test_same_session_late_scanner_blocks_stale_without_feed_evidence(self) -> None:
        session = date(2026, 8, 18)
        deadline = config.activation_deadline(session, "09:26")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch.object(common, "EQUITY_1M_SLOT_DIR", root / "bars"):
                payloads = _archive_complete_bundle(
                    root / "evidence",
                    session,
                    "09:25",
                    recorded_state="BLOCKED_STALE_ACTIVATION",
                    include_feed=False,
                    observation_overrides={
                        "scanner_snapshot": deadline + timedelta(microseconds=1),
                        "confirmation_snapshot": deadline + timedelta(seconds=1),
                    },
                )
                result = replay.replay_session(
                    session,
                    evidence_root=root / "evidence",
                    mode="observed",
                    strict=True,
                    slots=["09:25"],
                )
            row = result["slots"][0]
            self.assertLessEqual(
                datetime.fromisoformat(
                    payloads["scanner_snapshot"]["published_at_ist"]
                ),
                deadline,
            )
            self.assertEqual(
                datetime.fromisoformat(row["upstream_ready_at_ist"]),
                deadline + timedelta(microseconds=1),
            )
            self.assertEqual(row["upstream_deadline_state"], "BLOCKED_STALE_ACTIVATION")
            self.assertEqual(row["replayed_confirmation_state"], "BLOCKED_STALE_ACTIVATION")
            self.assertEqual(row["evidence_state"], "COMPLETE")
            self.assertNotIn("confirmation_feed_marker", row["evidence_issues"])
            self.assertTrue(row["parity_match"])

    def test_same_session_late_feed_is_replayed_as_stale(self) -> None:
        session = date(2026, 8, 18)
        deadline = config.activation_deadline(session, "09:26")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch.object(common, "EQUITY_1M_SLOT_DIR", root / "bars"):
                payloads = _archive_complete_bundle(
                    root / "evidence",
                    session,
                    "09:25",
                    recorded_state="BLOCKED_STALE_ACTIVATION",
                    observation_overrides={
                        "confirmation_feed_marker": deadline
                        + timedelta(microseconds=1),
                        "confirmation_snapshot": deadline + timedelta(seconds=1),
                    },
                )
                result = replay.replay_session(
                    session,
                    evidence_root=root / "evidence",
                    mode="observed",
                    strict=True,
                    slots=["09:25"],
                )
            row = result["slots"][0]
            self.assertLessEqual(
                pd.Timestamp(
                    payloads["confirmation_feed_marker"]["published_at_ist"]
                ).to_pydatetime(),
                deadline,
            )
            self.assertEqual(
                datetime.fromisoformat(row["confirmation_feed_available_at_ist"]),
                deadline + timedelta(microseconds=1),
            )
            self.assertTrue(row["confirmation_feed_ready"])
            self.assertEqual(
                row["confirmation_feed_deadline_state"],
                "BLOCKED_STALE_ACTIVATION",
            )
            self.assertEqual(row["replayed_confirmation_state"], "BLOCKED_STALE_ACTIVATION")
            self.assertTrue(row["parity_match"])

    def test_late_historical_archive_is_incomplete_observed_and_counterfactual(self) -> None:
        session = date(2026, 8, 17)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence_root = root / "evidence"
            with patch.object(common, "EQUITY_1M_SLOT_DIR", root / "bars"):
                _archive_complete_bundle(
                    evidence_root,
                    session,
                    "09:25",
                    observation_date=date(2026, 8, 18),
                )
                observed = replay.replay_session(
                    session,
                    evidence_root=evidence_root,
                    mode="observed",
                    strict=False,
                    slots=["09:25"],
                )
                counterfactual = replay.replay_session(
                    session,
                    evidence_root=evidence_root,
                    mode="counterfactual",
                    strict=True,
                    slots=["09:25"],
                )
                with self.assertRaises(evidence.EvidenceMissingError):
                    replay.replay_session(
                        session,
                        evidence_root=evidence_root,
                        mode="observed",
                        strict=True,
                        slots=["09:25"],
                    )
            row = observed["slots"][0]
            self.assertEqual(observed["state"], "INCOMPLETE_EVIDENCE")
            self.assertEqual(row["classification"], "OBSERVED_INCOMPLETE_EVIDENCE")
            self.assertTrue(
                row["evidence_issues"]["mapped_universe"].startswith(
                    "OUTSIDE_LIVE_WINDOW:"
                )
            )
            self.assertEqual(
                counterfactual["slots"][0]["classification"],
                "HISTORICAL_REPAIR_COUNTERFACTUAL",
            )
            self.assertTrue(counterfactual["historical_repair_counterfactual"])
            self.assertFalse(counterfactual["live_parity_claimed"])

    def test_cli_exit_codes_for_missing_evidence_and_mismatch(self) -> None:
        session = date(2026, 8, 18)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            missing_code = replay.main(
                [
                    "--session-date",
                    session.isoformat(),
                    "--slot",
                    "0925",
                    "--evidence-root",
                    str(root / "missing"),
                    "--output-root",
                    str(root / "missing-output"),
                ]
            )
            self.assertEqual(missing_code, 2)
            with patch.object(common, "EQUITY_1M_SLOT_DIR", root / "bars"):
                _archive_complete_bundle(
                    root / "mismatch",
                    session,
                    "09:25",
                    recorded_state="BLOCKED_INCOMPLETE_DATA",
                )
                mismatch_code = replay.main(
                    [
                        "--session-date",
                        session.isoformat(),
                        "--slot",
                        "09:25",
                        "--strict",
                        "--evidence-root",
                        str(root / "mismatch"),
                        "--output-root",
                        str(root / "mismatch-output"),
                    ]
                )
            self.assertEqual(mismatch_code, 3)

    def test_v6_archive_failure_prevents_canonical_decision_snapshot(self) -> None:
        session = date(2026, 8, 18)
        with tempfile.TemporaryDirectory() as temp_dir:
            canonical = Path(temp_dir) / "scanner.json"
            with (
                patch.object(shared_live, "LIVE_GENERATION", "v6"),
                patch.object(shared_live, "scanner_slot_path", return_value=canonical),
                patch.object(
                    shared_live.live_evidence,
                    "archive_json_evidence",
                    side_effect=OSError("disk full"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "archive failed"):
                    shared_live._write_scanner_snapshot(
                        session, "09:25", {"state": "SUCCESS"}
                    )
            self.assertFalse(canonical.exists())

    def test_tampered_existing_evidence_blocks_v6_live_dedupe(self) -> None:
        session = date(2026, 8, 18)
        payload = {"state": "SUCCESS", "complete": True}
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with (
                patch.object(shared_live, "LIVE_GENERATION", "v6"),
                patch.object(shared_live, "EVIDENCE_ROOT", root),
            ):
                path = shared_live._archive_json_evidence(
                    "scanner_snapshot", session, "09:25", payload
                )
                self.assertIsNotNone(path)
                envelope = json.loads(path.read_text(encoding="utf-8"))
                envelope["payload"]["complete"] = False
                path.write_text(json.dumps(envelope), encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "archive failed"):
                    shared_live._archive_json_evidence(
                        "scanner_snapshot", session, "09:25", payload
                    )

    def test_signal_write_failure_never_commits_success_confirmation(self) -> None:
        session = date(2026, 8, 18)
        snapshot = {"state": "SUCCESS", "selected_signal_ids": ["signal-1"]}
        with (
            patch.object(
                shared_live, "_write_entry_signal", side_effect=OSError("disk full")
            ),
            patch.object(shared_live, "_write_confirmation_snapshot") as write_snapshot,
        ):
            with self.assertRaises(OSError):
                shared_live._commit_confirmation_decision(
                    session,
                    "09:25",
                    snapshot,
                    [{"signal_id": "signal-1"}],
                )
        write_snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
