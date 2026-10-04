"""Generic read-only health/readiness status inspection."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from ai_platform.schemas import ArtifactState, SourceMetadata, StatusSnapshot

from .common import file_sha256, now_ist, parse_timestamp, source_metadata


_COMPLETE_STATES = {"COMPLETE", "DONE", "PASS", "READY", "SUCCESS"}


def inspect_status(
    path: Path | str,
    *,
    expected_schema: str | None = None,
    expected_strategy_version: str | None = None,
    expected_strategy_fingerprint: str | None = None,
    required_fields: Iterable[str] = (),
    timestamp_fields: tuple[str, ...] = (
        "updated_at_ist",
        "heartbeat_ist",
        "observed_at_ist",
    ),
    max_age: timedelta | None = None,
    now: datetime | None = None,
) -> StatusSnapshot:
    source = Path(path)
    if not source.is_file():
        return StatusSnapshot(ArtifactState.UNAVAILABLE, None, None, None, f"Missing {source}")
    read_at = now or now_ist()
    metadata = source_metadata(source, file_sha256(source), "runtime_status")
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return StatusSnapshot(ArtifactState.SCHEMA_ERROR, metadata, None, None, str(exc))
    if not isinstance(payload, dict):
        return StatusSnapshot(
            ArtifactState.SCHEMA_ERROR,
            metadata,
            None,
            None,
            "Expected a JSON object",
        )
    missing = [field for field in required_fields if field not in payload]
    if missing or (expected_schema is not None and payload.get("schema_version") != expected_schema):
        detail = f"Missing fields: {', '.join(missing)}" if missing else "Unsupported schema"
        return StatusSnapshot(ArtifactState.SCHEMA_ERROR, metadata, None, payload, detail)
    mismatches = []
    for field, expected in (
        ("strategy_version", expected_strategy_version),
        ("strategy_fingerprint", expected_strategy_fingerprint),
    ):
        if expected is not None and payload.get(field) != expected:
            mismatches.append(field)
    if mismatches:
        return StatusSnapshot(
            ArtifactState.INTEGRITY_ERROR,
            metadata,
            None,
            payload,
            f"Identity mismatch: {', '.join(mismatches)}",
        )
    observed = None
    for field in timestamp_fields:
        if payload.get(field):
            try:
                observed = parse_timestamp(payload[field], field)
            except ValueError as exc:
                return StatusSnapshot(ArtifactState.SCHEMA_ERROR, metadata, None, payload, str(exc))
            break
    if max_age is not None:
        if observed is None:
            return StatusSnapshot(
                ArtifactState.SCHEMA_ERROR,
                metadata,
                None,
                payload,
                "Freshness requested but no supported timestamp exists",
            )
        if read_at.astimezone(observed.tzinfo) - observed > max_age:
            return StatusSnapshot(ArtifactState.STALE, metadata, observed, payload, "Age exceeds limit")
    declared = str(payload.get("state", payload.get("status", ""))).upper()
    state = ArtifactState.COMPLETE if declared in _COMPLETE_STATES else ArtifactState.PARTIAL
    return StatusSnapshot(state, metadata, observed, payload)
