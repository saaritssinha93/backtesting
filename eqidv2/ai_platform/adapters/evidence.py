"""Read and select immutable evidence without importing producer modules."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from ai_platform.schemas import EvidenceRecord

from .common import (
    ArtifactError,
    canonical_json_sha256,
    parse_timestamp,
    read_json,
    require_equal,
    source_metadata,
)


EVIDENCE_SCHEMA = "fno_live_evidence_v1"


def list_evidence(
    evidence_root: Path | str,
    *,
    session_date: date,
    slot: str,
    artifact_kind: str,
    generation: str | None = None,
    strict: bool = False,
) -> list[EvidenceRecord]:
    root = Path(evidence_root) / session_date.isoformat() / f"slot_{slot}" / artifact_kind
    if not root.exists():
        if strict:
            raise ArtifactError(f"Evidence directory does not exist: {root}")
        return []
    records: list[EvidenceRecord] = []
    errors: list[str] = []
    for path in sorted(root.rglob("*.json")):
        try:
            envelope, source, sha = read_json(path)
            require_equal(envelope.get("schema_version"), EVIDENCE_SCHEMA, "schema_version", source)
            payload = envelope.get("payload")
            if not isinstance(payload, dict):
                raise ArtifactError(f"Evidence payload is not an object: {source}")
            observed_hash = canonical_json_sha256(payload)
            require_equal(envelope.get("payload_sha256"), observed_hash, "payload_sha256", source)
            observed = parse_timestamp(envelope.get("observed_at_ist"), "observed_at_ist")
            observed_session = date.fromisoformat(str(envelope.get("session_date", "")))
            require_equal(observed_session, session_date, "session_date", source)
            require_equal(envelope.get("slot"), slot, "slot", source)
            require_equal(envelope.get("artifact_kind"), artifact_kind, "artifact_kind", source)
            if generation is not None:
                require_equal(envelope.get("generation"), generation, "generation", source)
            records.append(
                EvidenceRecord(
                    artifact_kind=artifact_kind,
                    generation=str(envelope.get("generation", "")),
                    session_date=session_date,
                    slot=slot,
                    observed_at_ist=observed,
                    payload_sha256=observed_hash,
                    payload=payload,
                    source=source_metadata(source, sha, "immutable_evidence"),
                )
            )
        except (ArtifactError, ValueError) as exc:
            errors.append(str(exc))
    if strict and errors:
        raise ArtifactError("; ".join(errors))
    return sorted(records, key=lambda item: (item.observed_at_ist, item.payload_sha256, str(item.source.path)))


def select_evidence(
    records: list[EvidenceRecord],
    *,
    mode: str = "observed",
    as_of_ist: datetime | None = None,
) -> EvidenceRecord | None:
    eligible = records
    if as_of_ist is not None:
        if as_of_ist.tzinfo is None:
            raise ArtifactError("as_of_ist must include a timezone")
        eligible = [item for item in records if item.observed_at_ist <= as_of_ist]
    if not eligible:
        return None
    normalized = mode.strip().lower()
    if normalized == "observed":
        return eligible[0]
    if normalized == "counterfactual":
        return eligible[-1]
    raise ArtifactError(f"Unsupported evidence selection mode: {mode}")
