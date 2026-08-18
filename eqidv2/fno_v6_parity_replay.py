"""Replay FNO V6 live completeness and activation-deadline decisions.

Observed mode is deliberately strict about causality: it selects the earliest
immutable observation for each artifact and never substitutes today's mutable
canonical pointer.  Counterfactual mode selects the latest available revision
and may, when ``--strict`` is not used, fall back to a canonical repaired
artifact.  Such fallback is explicitly labelled and is not live parity.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_equity_fetch_1min as equity_feed
import fno_live_evidence as evidence
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v6_live_config as config


REPLAY_SCHEMA_VERSION = "fno_v6_live_parity_replay_v1"
PIPELINE_DEADLINE = dtime(9, 50)
HISTORICAL_REPAIR_DATE = date(2026, 8, 17)
ARTIFACT_KINDS = (
    "mapped_universe",
    "fno_fetch_marker",
    "cash_5m_marker",
    "scanner_snapshot",
    "confirmation_feed_marker",
    "confirmation_snapshot",
)


def _to_ist(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def deadline_state(available_at: datetime, deadline: datetime) -> str:
    """The production policy accepts an artifact exactly on the deadline."""

    return (
        "IN_WINDOW"
        if _to_ist(available_at) <= _to_ist(deadline)
        else "BLOCKED_STALE_ACTIVATION"
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _payload_time(payload: dict[str, Any], path: Path) -> datetime:
    candidates = [datetime.fromtimestamp(path.stat().st_mtime, tz=common.IST)]
    for key in (
        "published_at_ist",
        "completed_at_ist",
        "observed_at_ist",
        "written_at_ist",
    ):
        try:
            candidates.append(_to_ist(payload[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return max(candidates)


def _causal_available_at(
    revision: evidence.EvidenceRevision | None,
) -> datetime | None:
    """Return when an artifact could first causally affect the consumer."""

    if revision is None:
        return None
    candidates = [revision.observed_at_ist]
    for key in ("published_at_ist", "completed_at_ist", "written_at_ist"):
        try:
            candidates.append(_to_ist(revision.payload[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return max(candidates)


def _synthetic_revision(
    path: Path,
    *,
    artifact_kind: str,
    session_date: date,
    slot: str,
    payload: dict[str, Any],
) -> evidence.EvidenceRevision:
    return evidence.EvidenceRevision(
        path=path,
        artifact_kind=artifact_kind,
        generation="v6",
        session_date=session_date,
        slot=slot.replace(":", ""),
        observed_at_ist=_payload_time(payload, path),
        payload_sha256=common.canonical_json_sha256(payload),
        payload=payload,
        immutable=False,
        source_kind="canonical_fallback",
    )


def _mapped_universe_payload(session_date: date, signal_end: str) -> tuple[Path, dict[str, Any]]:
    path = common.universe_paths(session_date)[0]
    if not path.exists():
        return path, {}
    try:
        universe = pd.read_parquet(path)
        mapped, excluded = hybrid.ensure_equity_mapping(universe)
        unexpected = (
            excluded.loc[excluded["reason"].ne("INDEX_FUTURE_HAS_NO_CASH_EQUITY")]
            if not excluded.empty
            else excluded
        )
        if mapped.empty or not unexpected.empty:
            return path, {}
        futures_symbols = sorted(
            set(mapped["futures_tradingsymbol"].astype(str).str.strip().str.upper())
        )
        equity_symbols = sorted(
            set(mapped["equity_symbol"].astype(str).str.strip().str.upper())
        )
        payload = {
            "schema_version": "fno_mapped_stock_universe_evidence_v1",
            "generation": "v6",
            "session_date": session_date.isoformat(),
            "signal_end": signal_end,
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "contract_count": len(futures_symbols),
            "futures_symbols": futures_symbols,
            "futures_symbol_set_sha256": common.symbol_set_sha256(futures_symbols),
            "futures_universe_sha256": common.universe_sha256(mapped),
            "equity_symbols": equity_symbols,
            "equity_symbol_set_sha256": common.symbol_set_sha256(equity_symbols),
            "equity_universe_sha256": common.symbol_set_sha256(equity_symbols),
        }
        return path, payload
    except Exception:
        return path, {}


def _canonical_artifact(
    artifact_kind: str,
    session_date: date,
    signal_end: str,
    scanner: evidence.EvidenceRevision | None = None,
) -> evidence.EvidenceRevision | None:
    slot_dt = config.slot_datetime(session_date, signal_end)
    live_root = common.FNO_ROOT / "v6_live"
    if artifact_kind == "mapped_universe":
        path, payload = _mapped_universe_payload(session_date, signal_end)
    elif artifact_kind == "fno_fetch_marker":
        path = common.fetch_slot_path(slot_dt)
        payload = _read_json(path)
    elif artifact_kind == "cash_5m_marker":
        path = common.cash_slot_path(slot_dt)
        payload = _read_json(path)
    elif artifact_kind == "scanner_snapshot":
        path = live_root / "scanner_5m" / session_date.isoformat() / (
            f"slot_{signal_end.replace(':', '')}.json"
        )
        payload = _read_json(path)
    elif artifact_kind == "confirmation_snapshot":
        confirmation_end = config.SIGNAL_TO_CONFIRMATION[signal_end]
        path = live_root / "confirmation_1m" / session_date.isoformat() / (
            f"slot_{confirmation_end.replace(':', '')}.json"
        )
        payload = _read_json(path)
    elif artifact_kind == "confirmation_feed_marker":
        confirmation_end = config.SIGNAL_TO_CONFIRMATION[signal_end].replace(":", "")
        directory = common.EQUITY_1M_SLOT_DIR / "v6" / session_date.isoformat()
        paths = sorted(
            directory.glob(f"slot_{confirmation_end}_*.json"),
            key=lambda candidate: candidate.stat().st_mtime,
        ) if directory.exists() else []
        if scanner is not None:
            scanner_hash = common.canonical_json_sha256(scanner.payload)[:16]
            exact = [path for path in paths if path.stem.endswith(scanner_hash)]
            paths = exact or paths
        path = paths[-1] if paths else directory / f"slot_{confirmation_end}_missing.json"
        payload = _read_json(path)
    else:
        raise ValueError(f"Unsupported artifact kind: {artifact_kind}")
    if not payload or not path.exists():
        return None
    return _synthetic_revision(
        path,
        artifact_kind=artifact_kind,
        session_date=session_date,
        slot=signal_end,
        payload=payload,
    )


def _select_artifact(
    evidence_root: Path,
    *,
    artifact_kind: str,
    session_date: date,
    signal_end: str,
    mode: str,
    strict: bool,
    required: bool = True,
    scanner: evidence.EvidenceRevision | None = None,
    selection_issues: dict[str, str] | None = None,
) -> evidence.EvidenceRevision | None:
    revisions = evidence.list_revisions(
        evidence_root,
        session_date=session_date,
        slot=signal_end,
        artifact_kind=artifact_kind,
        generation="v6",
        # Integrity and metadata failures are never safe to ignore.  The CLI
        # strict flag controls missing-artifact/canonical fallback policy.
        strict=True,
    )
    immutable = (
        (revisions[0] if mode == "observed" else revisions[-1])
        if revisions
        else None
    )
    if strict and required and immutable is None:
        raise evidence.EvidenceMissingError(
            f"Missing {artifact_kind} evidence for {session_date} slot {signal_end}"
        )
    if mode == "observed":
        if immutable is None:
            if required and selection_issues is not None:
                selection_issues[artifact_kind] = "MISSING_IMMUTABLE_EVIDENCE"
            return None
        live_window_start = datetime.combine(
            session_date, dtime.min, tzinfo=common.IST
        )
        live_window_end = datetime.combine(
            session_date, PIPELINE_DEADLINE, tzinfo=common.IST
        )
        if not (
            live_window_start
            <= immutable.observed_at_ist
            <= live_window_end
        ):
            message = (
                f"{artifact_kind} earliest immutable evidence was observed at "
                f"{immutable.observed_at_ist.isoformat()} outside the live "
                f"window ending {live_window_end.isoformat()}"
            )
            if strict and required:
                raise evidence.EvidenceMissingError(message)
            if required and selection_issues is not None:
                selection_issues[artifact_kind] = (
                    "OUTSIDE_LIVE_WINDOW:" + immutable.observed_at_ist.isoformat()
                )
            return None
        return immutable
    if strict:
        return immutable
    canonical = _canonical_artifact(
        artifact_kind, session_date, signal_end, scanner=scanner
    )
    if immutable is None:
        if canonical is None and required and selection_issues is not None:
            selection_issues[artifact_kind] = "MISSING_IMMUTABLE_AND_CANONICAL_EVIDENCE"
        return canonical
    if canonical is None or canonical.payload_sha256 == immutable.payload_sha256:
        return immutable
    return max(
        (immutable, canonical),
        key=lambda revision: (revision.observed_at_ist, revision.payload_sha256),
    )


def _marker_slot_matches(payload: dict[str, Any], expected: datetime) -> bool:
    try:
        return _to_ist(payload["slot_ist"]) == expected
    except (KeyError, TypeError, ValueError):
        return False


def _symbol_list(payload: dict[str, Any], key: str) -> set[str] | None:
    values = payload.get(key)
    if not isinstance(values, list):
        return None
    normalized = [str(value).strip().upper() for value in values]
    if any(not value for value in normalized) or len(set(normalized)) != len(values):
        return None
    return set(normalized)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mapped_universe_ready(
    universe: dict[str, Any], session_date: date, signal_end: str
) -> tuple[bool, str]:
    if not universe:
        return False, "mapped_universe_missing"
    expected_fields = {
        "schema_version": "fno_mapped_stock_universe_evidence_v1",
        "generation": "v6",
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
    }
    for key, expected in expected_fields.items():
        if universe.get(key) != expected:
            return False, f"mapped_universe_{key}_mismatch"
    futures = _symbol_list(universe, "futures_symbols")
    equities = _symbol_list(universe, "equity_symbols")
    if futures is None or equities is None or not futures or not equities:
        return False, "mapped_universe_symbol_list_invalid"
    if _safe_int(universe.get("contract_count"), -1) != len(futures):
        return False, "mapped_universe_contract_count_mismatch"
    if len(equities) != len(futures):
        return False, "mapped_universe_equity_count_mismatch"
    if universe.get("futures_symbol_set_sha256") != common.symbol_set_sha256(
        futures
    ):
        return False, "mapped_universe_futures_symbol_set_mismatch"
    if universe.get("equity_symbol_set_sha256") != common.symbol_set_sha256(
        equities
    ):
        return False, "mapped_universe_equity_symbol_set_mismatch"
    if universe.get("equity_universe_sha256") != common.symbol_set_sha256(
        equities
    ):
        return False, "mapped_universe_equity_universe_mismatch"
    full_hash = str(universe.get("futures_universe_sha256", ""))
    if len(full_hash) != 64 or any(character not in "0123456789abcdef" for character in full_hash):
        return False, "mapped_universe_futures_universe_hash_invalid"
    return True, "ready"


def _fno_marker_ready(
    marker: dict[str, Any],
    universe: dict[str, Any],
    expected_slot: datetime,
) -> tuple[bool, str]:
    if not marker:
        return False, "fno_fetch_marker_missing"
    if str(marker.get("source", "")).lower() != "final":
        return False, "fno_fetch_marker_not_final"
    if not _marker_slot_matches(marker, expected_slot):
        return False, "fno_fetch_marker_wrong_slot"
    if marker.get("schema_version") != config.FNO_FETCH_SLOT_SCHEMA_VERSION:
        return False, "fno_fetch_marker_schema_unsupported"
    if marker.get("readiness_policy") != config.FNO_READINESS_POLICY:
        return False, "fno_fetch_marker_readiness_policy_mismatch"
    declared_minimum_coverage = _safe_float(
        marker.get("minimum_stock_coverage"), -1.0
    )
    if not (
        float(config.MIN_STOCK_FUTURES_COVERAGE)
        <= declared_minimum_coverage
        <= 1.0
    ):
        return False, "fno_fetch_marker_minimum_coverage_mismatch"
    if abs(
        _safe_float(marker.get("minimum_coverage"), -1.0)
        - declared_minimum_coverage
    ) > 1e-12:
        return False, "fno_fetch_marker_minimum_coverage_alias_mismatch"
    if _safe_int(marker.get("maximum_verified_no_candle_stocks"), -1) != int(
        config.MAX_VERIFIED_NO_CANDLE_STOCKS
    ):
        return False, "fno_fetch_marker_no_candle_cap_mismatch"
    if _safe_int(marker.get("minimum_no_candle_fetch_attempts"), -1) != int(
        config.MIN_NO_CANDLE_FETCH_ATTEMPTS
    ):
        return False, "fno_fetch_marker_no_candle_attempt_policy_mismatch"
    if not marker.get("complete") or not marker.get("stock_complete"):
        return False, f"fno_fetch_marker_{marker.get('state', 'partial')}"
    if str(marker.get("state", "")).upper() != "SUCCESS":
        return False, "fno_fetch_marker_state_mismatch"
    if not bool(marker.get("outcome_symbol_set_complete")):
        return False, "fno_fetch_marker_outcome_symbol_set_incomplete"
    if not bool(marker.get("stock_outcome_symbol_set_complete")):
        return False, "fno_fetch_marker_stock_symbol_set_incomplete"
    if _safe_int(marker.get("failed_count")) != 0:
        return False, "fno_fetch_marker_api_failure"
    if _safe_int(marker.get("invalid_data_count")) != 0:
        return False, "fno_fetch_marker_invalid_data"
    if _safe_int(marker.get("stock_failed_count")) != 0:
        return False, "fno_fetch_marker_stock_api_failure"
    if _safe_int(marker.get("stock_invalid_data_count")) != 0:
        return False, "fno_fetch_marker_stock_invalid_data"
    all_no_candle = _symbol_list(marker, "no_candle_symbols")
    written = _symbol_list(marker, "stock_written_symbols")
    no_candle = _symbol_list(marker, "stock_no_candle_symbols")
    verified = _symbol_list(marker, "stock_verified_no_candle_symbols")
    unverified = _symbol_list(marker, "stock_unverified_no_candle_symbols")
    if any(
        value is None
        for value in (all_no_candle, written, no_candle, verified, unverified)
    ):
        return False, "fno_fetch_marker_symbol_list_invalid"
    assert all_no_candle is not None
    assert written is not None and no_candle is not None
    assert verified is not None and unverified is not None
    expected_symbols = _symbol_list(universe, "futures_symbols") or set()
    if not expected_symbols or len(expected_symbols) != _safe_int(
        universe.get("contract_count"), -1
    ):
        return False, "fno_fetch_marker_stock_universe_invalid"
    if _safe_int(marker.get("stock_contracts_expected")) != len(expected_symbols):
        return False, "fno_fetch_marker_stock_count_mismatch"
    if marker.get("stock_symbol_set_sha256") != common.symbol_set_sha256(
        expected_symbols
    ):
        return False, "fno_fetch_marker_stock_symbol_set_mismatch"
    expected_full_hash = str(universe.get("futures_universe_sha256", ""))
    if not expected_full_hash:
        return False, "fno_fetch_marker_stock_universe_unattestable"
    if marker.get("stock_universe_sha256") != expected_full_hash:
        return False, "fno_fetch_marker_stock_universe_mismatch"
    if no_candle - expected_symbols:
        return False, "fno_fetch_marker_foreign_no_candle_symbol"
    if no_candle - all_no_candle:
        return False, "fno_fetch_marker_no_candle_list_mismatch"
    if verified != no_candle or unverified:
        return False, "fno_fetch_marker_unverified_stock_no_candle"
    if len(verified) > int(config.MAX_VERIFIED_NO_CANDLE_STOCKS):
        return False, "fno_fetch_marker_no_candle_cap_exceeded"
    if written != expected_symbols - verified:
        return False, "fno_fetch_marker_stock_partition_mismatch"

    stock_expected = len(expected_symbols)
    stock_written = len(written)
    if _safe_int(marker.get("stock_contracts_written")) != stock_written:
        return False, "fno_fetch_marker_stock_written_count_mismatch"
    if _safe_int(marker.get("stock_no_candle_count")) != len(no_candle):
        return False, "fno_fetch_marker_stock_no_candle_count_mismatch"
    if _safe_int(marker.get("stock_verified_no_candle_count")) != len(verified):
        return False, "fno_fetch_marker_verified_no_candle_count_mismatch"
    if stock_written + len(verified) != stock_expected:
        return False, "fno_fetch_marker_stock_incomplete_coverage"
    coverage = stock_written / stock_expected
    if coverage < declared_minimum_coverage:
        return False, "fno_fetch_marker_stock_incomplete_coverage"
    if abs(_safe_float(marker.get("stock_coverage_ratio"), -1.0) - coverage) > 1e-12:
        return False, "fno_fetch_marker_stock_coverage_ratio_mismatch"

    total_expected = _safe_int(marker.get("contracts_expected"))
    total_written = _safe_int(marker.get("contracts_written"))
    total_no_candle = _safe_int(marker.get("no_candle_count"))
    if total_expected <= 0 or total_no_candle != len(all_no_candle):
        return False, "fno_fetch_marker_total_count_mismatch"
    if total_written + total_no_candle != total_expected:
        return False, "fno_fetch_marker_incomplete_coverage"

    observations = marker.get("no_candle_observations")
    attempts = marker.get("no_candle_fetch_attempts")
    if not isinstance(observations, dict) or not isinstance(attempts, dict):
        return False, "fno_fetch_marker_no_candle_evidence_missing"
    normalized_observations = {
        str(symbol).strip().upper(): _safe_int(count, -1)
        for symbol, count in observations.items()
        if str(symbol).strip()
    }
    normalized_attempts = {
        str(symbol).strip().upper(): _safe_int(count, -1)
        for symbol, count in attempts.items()
        if str(symbol).strip()
    }
    required = int(config.MIN_NO_CANDLE_FETCH_ATTEMPTS)
    for symbol in verified:
        if normalized_observations.get(symbol, -1) < required:
            return False, "fno_fetch_marker_no_candle_not_repeatedly_verified"
        if normalized_attempts.get(symbol, -1) < required:
            return False, "fno_fetch_marker_no_candle_attempts_insufficient"
    return True, "ready"


def _cash_marker_ready(
    marker: dict[str, Any],
    universe: dict[str, Any],
    expected_slot: datetime,
) -> tuple[bool, str]:
    if not marker:
        return False, "cash_5m_marker_missing"
    if str(marker.get("source", "")).lower() != "final":
        return False, "cash_5m_marker_not_final"
    if not _marker_slot_matches(marker, expected_slot):
        return False, "cash_5m_marker_wrong_slot"
    if not bool(marker.get("complete")):
        return False, "cash_5m_marker_incomplete"
    cash_expected = _safe_int(marker.get("tickers_expected"))
    if (
        cash_expected <= 0
        or _safe_int(marker.get("tickers_written")) != cash_expected
        or _safe_int(marker.get("tickers_complete")) != cash_expected
        or _safe_int(marker.get("tickers_failed")) != 0
    ):
        return False, "cash_5m_marker_incomplete_coverage"
    if not bool(marker.get("fno_equity_quality_complete")):
        return False, "cash_5m_marker_fno_equity_quality_incomplete"
    expected = _safe_int(marker.get("fno_equity_expected"))
    universe_count = _safe_int(universe.get("contract_count"))
    if (
        expected <= 0
        or expected != universe_count
        or _safe_int(marker.get("fno_equity_ready")) != expected
        or _safe_int(marker.get("fno_equity_failed")) != 0
    ):
        return False, "cash_5m_marker_fno_equity_incomplete_coverage"
    if marker.get("fno_equity_universe_sha256") != universe.get(
        "equity_universe_sha256"
    ):
        return False, "cash_5m_marker_fno_equity_universe_mismatch"
    return True, "ready"


def _scanner_snapshot_ready(
    snapshot: dict[str, Any], session_date: date, signal_end: str
) -> tuple[bool, str]:
    if not snapshot:
        return False, "scanner_snapshot_missing"
    expected_fields = {
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
    }
    for key, expected in expected_fields.items():
        if snapshot.get(key) != expected:
            return False, f"scanner_snapshot_{key}_mismatch"
    if str(snapshot.get("state", "")) != "SUCCESS":
        return False, f"scanner_snapshot_{snapshot.get('state', 'incomplete')}"
    try:
        equity_feed.validate_scanner_snapshot(
            snapshot, "v6", session_date, signal_end
        )
    except (KeyError, TypeError, ValueError) as exc:
        return False, f"scanner_snapshot_candidate_contract_invalid:{exc}"
    return True, "ready"


def _confirmation_feed_ready(
    marker: dict[str, Any],
    scanner_snapshot: dict[str, Any],
    session_date: date,
    signal_end: str,
) -> tuple[bool, str]:
    if not marker:
        return False, "confirmation_feed_marker_missing"
    confirmation_hhmm = config.SIGNAL_TO_CONFIRMATION[signal_end]
    confirmation_end = config.slot_datetime(session_date, confirmation_hhmm)
    expected_symbols = {
        str(candidate.get("tradingsymbol", "")).strip().upper()
        for candidate in list(scanner_snapshot.get("candidates") or [])
    }
    scanner_sha256 = equity_feed.scanner_snapshot_sha256(scanner_snapshot)
    expected_fields = {
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
        "scanner_snapshot_sha256": scanner_sha256,
        "candidate_contract_sha256": equity_feed.candidate_contract_sha256(
            scanner_snapshot
        ),
        "candidate_symbol_set_sha256": common.symbol_set_sha256(
            expected_symbols
        ),
        "candidate_resolution_policy": equity_feed.NO_CANDLE_RESOLUTION_POLICY,
        "minimum_no_candle_observations": config.CONFIRMATION_NO_CANDLE_OBSERVATIONS,
        "minimum_no_candle_verification_age_sec": config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
        "minimum_no_candle_observation_spacing_sec": config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
        "verified_no_candle_cap": None,
        "written_bar_minimum_ratio": None,
    }
    for key, expected in expected_fields.items():
        if marker.get(key) != expected:
            return False, f"confirmation_feed_{key}_mismatch"
    if common.canonical_json_sha256(marker.get("scanner_snapshot")) != scanner_sha256:
        return False, "confirmation_feed_scanner_snapshot_tampered"
    try:
        published_at = _to_ist(marker["published_at_ist"])
        marker_deadline = _to_ist(marker["deadline_ist"])
        no_candle_verification_at = _to_ist(
            marker["minimum_no_candle_verification_ist"]
        )
    except (KeyError, TypeError, ValueError):
        return False, "confirmation_feed_time_invalid"
    expected_deadline = config.activation_deadline(
        session_date, confirmation_hhmm
    )
    expected_no_candle_verification_at = confirmation_end + timedelta(
        seconds=config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC
    )
    if (
        marker_deadline != expected_deadline
        or no_candle_verification_at != expected_no_candle_verification_at
        or published_at < confirmation_end
        or published_at > expected_deadline
    ):
        return False, "confirmation_feed_late"

    list_fields = (
        "candidate_symbols",
        "written_symbols",
        "no_candle_symbols",
        "verified_no_candle_symbols",
        "unverified_no_candle_symbols",
        "resolved_symbols",
        "invalid_symbols",
        "api_failed_symbols",
        "unexpected_missing_symbols",
    )
    normalized = {field: _symbol_list(marker, field) for field in list_fields}
    if any(value is None for value in normalized.values()):
        return False, "confirmation_feed_symbols_invalid"
    candidates = normalized["candidate_symbols"] or set()
    written = normalized["written_symbols"] or set()
    no_candle = normalized["no_candle_symbols"] or set()
    verified = normalized["verified_no_candle_symbols"] or set()
    unverified = normalized["unverified_no_candle_symbols"] or set()
    resolved = normalized["resolved_symbols"] or set()
    if (
        candidates != expected_symbols
        or written & verified
        or written | verified != expected_symbols
        or no_candle != verified
        or unverified
        or resolved != expected_symbols
        or _safe_int(marker.get("candidate_count"), -1) != len(expected_symbols)
        or _safe_int(marker.get("written_count"), -1) != len(written)
        or _safe_int(marker.get("verified_no_candle_count"), -1) != len(verified)
        or _safe_int(marker.get("resolved_count"), -1) != len(expected_symbols)
    ):
        return False, "confirmation_feed_coverage_mismatch"
    if any(
        marker.get(field)
        for field in (
            "unverified_no_candle_symbols",
            "invalid_symbols",
            "api_failed_symbols",
            "unexpected_missing_symbols",
            "errors",
        )
    ):
        return False, "confirmation_feed_failure_list_nonempty"

    attempts = marker.get("attempts_by_symbol")
    no_candle_observations = marker.get("no_candle_observations")
    observation_history = marker.get("observation_history")
    if (
        not isinstance(attempts, dict)
        or not isinstance(no_candle_observations, dict)
        or not isinstance(observation_history, dict)
        or set(attempts) != expected_symbols
        or set(no_candle_observations) != expected_symbols
        or set(observation_history) != expected_symbols
    ):
        return False, "confirmation_feed_evidence_invalid"
    try:
        configured_spacing = float(
            marker["configured_no_candle_observation_spacing_sec"]
        )
    except (KeyError, TypeError, ValueError):
        return False, "confirmation_feed_evidence_invalid"
    if configured_spacing < config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC:
        return False, "confirmation_feed_evidence_invalid"
    for symbol in expected_symbols:
        history = observation_history.get(symbol)
        try:
            attempt_count = int(attempts[symbol])
            no_candle_count = int(no_candle_observations[symbol])
        except (TypeError, ValueError):
            return False, "confirmation_feed_evidence_invalid"
        if (
            not isinstance(history, list)
            or attempt_count < 0
            or no_candle_count < 0
            or no_candle_count > attempt_count
            or len(history) != attempt_count
            or any(
                not isinstance(item, dict)
                or str(item.get("state", ""))
                not in {"WRITTEN", "NO_CANDLE", "INVALID_DATA", "FAILED"}
                for item in history
            )
            or sum(
                str(item.get("state", "")) == "NO_CANDLE"
                for item in history
            )
            != no_candle_count
        ):
            return False, "confirmation_feed_evidence_invalid"
    for symbol in verified:
        history = observation_history.get(symbol)
        try:
            attempt_count = int(attempts[symbol])
            no_candle_count = int(no_candle_observations[symbol])
        except (TypeError, ValueError):
            return False, "confirmation_feed_evidence_invalid"
        if (
            not isinstance(history, list)
            or attempt_count != no_candle_count
            or attempt_count != len(history)
            or no_candle_count < config.CONFIRMATION_NO_CANDLE_OBSERVATIONS
            or not equity_feed._clean_no_candle_history(
                history,
                required_observations=config.CONFIRMATION_NO_CANDLE_OBSERVATIONS,
                minimum_spacing_sec=config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
                not_before=expected_no_candle_verification_at,
                not_after=published_at,
            )
            or published_at < expected_no_candle_verification_at
        ):
            return False, "confirmation_feed_evidence_invalid"

    expected_data_path = common.equity_1m_slot_data_path(
        confirmation_end,
        generation="v6",
        scanner_sha256=scanner_sha256,
    )
    if str(marker.get("slot_data_path", "")) != str(expected_data_path):
        return False, "confirmation_feed_data_path_mismatch"
    if not expected_data_path.exists():
        return False, "confirmation_feed_data_missing"
    try:
        slot_data_bytes = expected_data_path.read_bytes()
    except OSError as exc:
        return False, f"confirmation_feed_data_unreadable:{exc}"
    if hashlib.sha256(slot_data_bytes).hexdigest() != str(
        marker.get("slot_data_sha256", "")
    ):
        return False, "confirmation_feed_data_hash_mismatch"
    try:
        frame = pd.read_parquet(io.BytesIO(slot_data_bytes))
    except Exception as exc:
        return False, f"confirmation_feed_data_unreadable:{exc}"
    if "tradingsymbol" not in frame.columns or len(frame) != len(written):
        return False, "confirmation_feed_data_row_count_mismatch"
    frame["tradingsymbol"] = (
        frame["tradingsymbol"].astype(str).str.strip().str.upper()
    )
    if set(frame["tradingsymbol"]) != written or frame["tradingsymbol"].duplicated().any():
        return False, "confirmation_feed_data_symbol_mismatch"
    candidates_by_symbol = {
        str(candidate.get("tradingsymbol", "")).strip().upper(): candidate
        for candidate in list(scanner_snapshot.get("candidates") or [])
    }
    for row in frame.to_dict("records"):
        symbol = str(row["tradingsymbol"]).strip().upper()
        if _safe_int(row.get("instrument_token")) != _safe_int(
            candidates_by_symbol[symbol].get("instrument_token")
        ):
            return False, "confirmation_feed_data_token_mismatch"
        error = equity_feed._validate_bar(row, confirmation_end)
        if error:
            return False, f"confirmation_feed_data_{error}"
    return True, "ready"


def _revision_summary(revision: evidence.EvidenceRevision | None) -> dict[str, Any]:
    if revision is None:
        return {}
    return {
        "path": str(revision.path),
        "payload_sha256": revision.payload_sha256,
        "observed_at_ist": revision.observed_at_ist.isoformat(timespec="microseconds"),
        "immutable": revision.immutable,
        "source_kind": revision.source_kind,
    }


def replay_slot(
    session_date: date,
    signal_end: str,
    *,
    evidence_root: Path,
    mode: str,
    strict: bool,
) -> dict[str, Any]:
    selected: dict[str, evidence.EvidenceRevision | None] = {}
    selection_issues: dict[str, str] = {}
    universe_revision = _select_artifact(
        evidence_root,
        artifact_kind="mapped_universe",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict,
        selection_issues=selection_issues,
    )
    selected["mapped_universe"] = universe_revision
    universe = universe_revision.payload if universe_revision else {}
    expected_slot = config.slot_datetime(session_date, signal_end)
    universe_ready, universe_reason = _mapped_universe_ready(
        universe, session_date, signal_end
    )
    fno_required = universe_revision is not None
    fno_revision = _select_artifact(
        evidence_root,
        artifact_kind="fno_fetch_marker",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict and fno_required,
        required=fno_required,
        selection_issues=selection_issues,
    )
    selected["fno_fetch_marker"] = fno_revision
    fno_ready, fno_reason = _fno_marker_ready(
        fno_revision.payload if fno_revision else {}, universe, expected_slot
    )
    cash_required = universe_ready and fno_ready
    cash_revision = _select_artifact(
        evidence_root,
        artifact_kind="cash_5m_marker",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict and cash_required,
        required=cash_required,
        selection_issues=selection_issues,
    )
    selected["cash_5m_marker"] = cash_revision
    cash_ready, cash_reason = _cash_marker_ready(
        cash_revision.payload if cash_revision else {}, universe, expected_slot
    )
    marker_ready_at = max(
        (
            available_at
            for revision in (fno_revision, cash_revision)
            if (available_at := _causal_available_at(revision)) is not None
        ),
        default=None,
    )
    pipeline_deadline = datetime.combine(
        session_date, PIPELINE_DEADLINE, tzinfo=common.IST
    )
    five_minute_gate = "READY"
    five_minute_reason = "ready"
    if not universe_ready:
        five_minute_gate, five_minute_reason = "BLOCKED", universe_reason
    elif not fno_ready:
        five_minute_gate, five_minute_reason = "BLOCKED", fno_reason
    elif not cash_ready:
        five_minute_gate, five_minute_reason = "BLOCKED", cash_reason
    elif marker_ready_at is None or marker_ready_at > pipeline_deadline:
        five_minute_gate, five_minute_reason = "BLOCKED", "markers_after_pipeline_deadline"

    scanner_revision = _select_artifact(
        evidence_root,
        artifact_kind="scanner_snapshot",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict and five_minute_gate == "READY",
        required=five_minute_gate == "READY",
        selection_issues=selection_issues,
    )
    selected["scanner_snapshot"] = scanner_revision
    scanner_payload = scanner_revision.payload if scanner_revision else {}
    scanner_ready, scanner_reason = _scanner_snapshot_ready(
        scanner_payload, session_date, signal_end
    )
    scanner_state = str(scanner_payload.get("state", "MISSING"))
    candidate_count = len(scanner_payload.get("candidates") or [])
    confirmation_end = config.slot_datetime(
        session_date, config.SIGNAL_TO_CONFIRMATION[signal_end]
    )
    activation_deadline = confirmation_end + timedelta(
        seconds=config.ENTRY_ACTIVATION_GRACE_SEC
    )
    upstream_ready_at = max(
        (
            available_at
            for revision in (
                universe_revision,
                fno_revision,
                cash_revision,
                scanner_revision,
            )
            if (available_at := _causal_available_at(revision)) is not None
        ),
        default=None,
    )
    upstream_deadline_state = (
        deadline_state(upstream_ready_at, activation_deadline)
        if upstream_ready_at is not None
        else "MISSING"
    )
    feed_required = bool(
        five_minute_gate == "READY"
        and scanner_ready
        and upstream_deadline_state == "IN_WINDOW"
    )

    feed_revision = _select_artifact(
        evidence_root,
        artifact_kind="confirmation_feed_marker",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict and feed_required,
        required=feed_required,
        scanner=scanner_revision,
        selection_issues=selection_issues,
    )
    selected["confirmation_feed_marker"] = feed_revision
    feed_ready, feed_reason = _confirmation_feed_ready(
        feed_revision.payload if feed_revision else {},
        scanner_payload,
        session_date,
        signal_end,
    )
    feed_available_at = _causal_available_at(feed_revision)
    feed_deadline_state = (
        deadline_state(feed_available_at, activation_deadline)
        if feed_available_at is not None
        else "MISSING"
    )

    if five_minute_gate != "READY" or not scanner_ready:
        replayed_confirmation_state = "BLOCKED_INCOMPLETE_DATA"
        confirmation_reason = (
            five_minute_reason
            if five_minute_gate != "READY"
            else scanner_reason
        )
    elif upstream_deadline_state == "BLOCKED_STALE_ACTIVATION":
        replayed_confirmation_state = "BLOCKED_STALE_ACTIVATION"
        confirmation_reason = "scanner_or_upstream_observed_after_activation_deadline"
    elif feed_ready and feed_deadline_state == "IN_WINDOW":
        replayed_confirmation_state = "SUCCESS"
        confirmation_reason = feed_reason
    elif feed_deadline_state == "BLOCKED_STALE_ACTIVATION":
        replayed_confirmation_state = "BLOCKED_STALE_ACTIVATION"
        confirmation_reason = "confirmation_feed_observed_after_activation_deadline"
    else:
        replayed_confirmation_state = "BLOCKED_INCOMPLETE_DATA"
        confirmation_reason = feed_reason

    confirmation_revision = _select_artifact(
        evidence_root,
        artifact_kind="confirmation_snapshot",
        session_date=session_date,
        signal_end=signal_end,
        mode=mode,
        strict=strict,
        scanner=scanner_revision,
        selection_issues=selection_issues,
    )
    selected["confirmation_snapshot"] = confirmation_revision
    recorded_confirmation_state = (
        str(confirmation_revision.payload.get("state", ""))
        if confirmation_revision
        else "MISSING"
    )
    observed_mode = mode == "observed"
    parity_match: bool | None = (
        recorded_confirmation_state == replayed_confirmation_state
        if observed_mode and confirmation_revision is not None
        else None
    )
    canonical_fallbacks = sorted(
        kind
        for kind, revision in selected.items()
        if revision is not None and not revision.immutable
    )
    if mode == "counterfactual" and session_date == HISTORICAL_REPAIR_DATE:
        classification = "HISTORICAL_REPAIR_COUNTERFACTUAL"
    elif canonical_fallbacks:
        classification = "COUNTERFACTUAL_CANONICAL_FALLBACK"
    elif mode == "counterfactual":
        classification = "COUNTERFACTUAL_LATEST_IMMUTABLE"
    elif selection_issues:
        classification = "OBSERVED_INCOMPLETE_EVIDENCE"
    else:
        classification = "OBSERVED_IMMUTABLE"
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "mode": mode,
        "classification": classification,
        "live_parity_claimed": (
            observed_mode
            and classification == "OBSERVED_IMMUTABLE"
            and parity_match is True
        ),
        "evidence_state": (
            "INCOMPLETE_EVIDENCE"
            if selection_issues
            else "COMPLETE"
        ),
        "evidence_issues": selection_issues,
        "canonical_fallback_artifacts": canonical_fallbacks,
        "pipeline_deadline_ist": pipeline_deadline.isoformat(),
        "activation_deadline_ist": activation_deadline.isoformat(),
        "marker_ready_at_ist": marker_ready_at.isoformat() if marker_ready_at else "",
        "upstream_ready_at_ist": upstream_ready_at.isoformat() if upstream_ready_at else "",
        "upstream_deadline_state": upstream_deadline_state,
        "mapped_universe_ready": universe_ready,
        "mapped_universe_reason": universe_reason,
        "fno_marker_ready": fno_ready,
        "fno_marker_reason": fno_reason,
        "cash_marker_ready": cash_ready,
        "cash_marker_reason": cash_reason,
        "five_minute_gate": five_minute_gate,
        "five_minute_reason": five_minute_reason,
        "scanner_state": scanner_state,
        "scanner_ready": scanner_ready,
        "scanner_reason": scanner_reason,
        "scanner_candidate_count": candidate_count,
        "scanner_skipped_no_candle": int(
            scanner_payload.get("contracts_skipped_no_candle", 0) or 0
        ),
        "scanner_unexpected_missing": int(
            scanner_payload.get(
                "contracts_unexpected_missing",
                scanner_payload.get("contracts_missing_slot", 0),
            )
            or 0
        ),
        "confirmation_feed_ready": feed_ready,
        "confirmation_feed_reason": feed_reason,
        "confirmation_feed_available_at_ist": (
            feed_available_at.isoformat() if feed_available_at else ""
        ),
        "confirmation_feed_deadline_state": feed_deadline_state,
        "replayed_confirmation_state": replayed_confirmation_state,
        "confirmation_reason": confirmation_reason,
        "recorded_confirmation_state": recorded_confirmation_state,
        "parity_match": parity_match,
        "evidence": {
            kind: _revision_summary(revision) for kind, revision in selected.items()
        },
    }


def replay_session(
    session_date: date,
    *,
    evidence_root: Path,
    mode: str,
    strict: bool,
    slots: list[str] | None = None,
) -> dict[str, Any]:
    wanted = slots or list(config.SIGNAL_TO_CONFIRMATION)
    rows = [
        replay_slot(
            session_date,
            signal_end,
            evidence_root=evidence_root,
            mode=mode,
            strict=strict,
        )
        for signal_end in wanted
    ]
    historical_repair = mode == "counterfactual" and session_date == HISTORICAL_REPAIR_DATE
    mismatch_count = sum(row["parity_match"] is False for row in rows)
    evidence_complete = all(row["evidence_state"] == "COMPLETE" for row in rows)
    live_parity_claimed = (
        mode == "observed"
        and evidence_complete
        and mismatch_count == 0
        and all(row["live_parity_claimed"] for row in rows)
    )
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "mode": mode,
        "strict": strict,
        "evidence_root": str(Path(evidence_root)),
        "historical_repair_counterfactual": historical_repair,
        "evidence_complete": evidence_complete,
        "live_parity_claimed": live_parity_claimed,
        "state": (
            "PARITY_MISMATCH"
            if mismatch_count
            else "INCOMPLETE_EVIDENCE"
            if mode == "observed" and not evidence_complete
            else "PARITY_MATCH"
            if live_parity_claimed
            else "COUNTERFACTUAL"
        ),
        "parity_mismatches": mismatch_count,
        "slots": rows,
    }


def _render_report(result: dict[str, Any]) -> str:
    warning = ""
    if result["historical_repair_counterfactual"]:
        warning = (
            "\n> **HISTORICAL REPAIR COUNTERFACTUAL:** 2026-08-17 canonical "
            "artifacts include after-hours repairs. This run does not claim what "
            "the live process knew at the deadline.\n"
        )
    elif not result["live_parity_claimed"]:
        warning = (
            "\n> This run is counterfactual or has incomplete immutable evidence; "
            "it does not claim observed-live parity.\n"
        )
    lines = [
        "# FNO V6 Completeness and Deadline Parity Replay",
        "",
        f"- Session: `{result['session_date']}`",
        f"- Mode: `{result['mode']}`",
        f"- Strict evidence: `{result['strict']}`",
        f"- State: `{result['state']}`",
        f"- Evidence complete: `{result['evidence_complete']}`",
        f"- Live parity claimed: `{result['live_parity_claimed']}`",
        f"- Strategy fingerprint: `{result['strategy_fingerprint']}`",
        warning.rstrip(),
        "",
        "Signal | Class | 5m gate | Scanner | Candidates | 1m feed | Deadline | Replayed | Recorded | Parity",
        "--- | --- | --- | --- | ---: | --- | --- | --- | --- | ---",
    ]
    for row in result["slots"]:
        parity = "N/A" if row["parity_match"] is None else str(row["parity_match"])
        lines.append(
            f"{row['signal_end']} | {row['classification']} | "
            f"{row['five_minute_gate']} ({row['five_minute_reason']}) | "
            f"{row['scanner_state']} | {row['scanner_candidate_count']} | "
            f"{row['confirmation_feed_ready']} ({row['confirmation_feed_reason']}) | "
            f"{row['confirmation_feed_deadline_state']} | "
            f"{row['replayed_confirmation_state']} | "
            f"{row['recorded_confirmation_state']} | {parity}"
        )
    return "\n".join(lines).strip() + "\n"


def _write_outputs(result: dict[str, Any], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    common.atomic_write_json(output_root / "manifest.json", result)
    rows = pd.DataFrame(
        [
            {key: value for key, value in row.items() if key != "evidence"}
            for row in result["slots"]
        ]
    )
    common.atomic_write_csv(rows, output_root / "slot_timeline.csv")
    for row in result["slots"]:
        common.atomic_write_json(
            output_root / f"slot_{row['signal_end'].replace(':', '')}.json", row
        )
    common.atomic_write_text(output_root / "parity_report.md", _render_report(result))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--mode", choices=("observed", "counterfactual"), default="observed")
    parser.add_argument("--slot", action="append", default=[])
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--evidence-root", default="")
    parser.add_argument("--output-root", default="")
    return parser


def normalize_slots(values: list[str]) -> list[str]:
    """Normalize repeatable CLI slot values; literal ``all`` selects all."""

    if not values or any(str(value).strip().lower() == "all" for value in values):
        return list(config.SIGNAL_TO_CONFIRMATION)
    normalized: list[str] = []
    for value in values:
        digits = str(value).strip().replace(":", "")
        if len(digits) != 4 or not digits.isdigit():
            raise ValueError(f"Invalid V6 replay slot: {value!r}")
        slot = f"{digits[:2]}:{digits[2:]}"
        if slot not in normalized:
            normalized.append(slot)
    unsupported = sorted(set(normalized) - set(config.SIGNAL_TO_CONFIRMATION))
    if unsupported:
        raise ValueError(f"Unsupported V6 replay slots: {unsupported}")
    return normalized


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    session_date = date.fromisoformat(args.session_date)
    slots = normalize_slots(args.slot)
    evidence_root = Path(args.evidence_root) if args.evidence_root else (
        common.FNO_ROOT / "v6_live" / "evidence"
    )
    output_root = Path(args.output_root) if args.output_root else (
        common.FNO_ROOT
        / "v6_live"
        / "parity_replay"
        / session_date.isoformat()
        / args.mode
    )
    try:
        result = replay_session(
            session_date,
            evidence_root=evidence_root,
            mode=args.mode,
            strict=args.strict,
            slots=slots,
        )
    except evidence.EvidenceError as exc:
        failure = {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "session_date": session_date.isoformat(),
            "mode": args.mode,
            "strict": args.strict,
            "state": "BLOCKED_MISSING_OR_INVALID_EVIDENCE",
            "reason": str(exc),
        }
        output_root.mkdir(parents=True, exist_ok=True)
        common.atomic_write_json(output_root / "manifest.json", failure)
        common.atomic_write_text(
            output_root / "parity_report.md",
            "# FNO V6 Parity Replay\n\n"
            f"State: **BLOCKED_MISSING_OR_INVALID_EVIDENCE**\n\n{exc}\n",
        )
        print(f"[BLOCKED] {exc}", flush=True)
        return 2
    _write_outputs(result, output_root)
    print(
        f"[PARITY] {session_date} mode={args.mode} slots={len(result['slots'])} "
        f"mismatches={result['parity_mismatches']} output={output_root}",
        flush=True,
    )
    if result["parity_mismatches"]:
        return 3
    if not result["evidence_complete"]:
        return 2
    if args.mode == "observed" and not result["live_parity_claimed"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
