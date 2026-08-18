"""Immutable evidence envelopes for FNO live/replay decisions.

Canonical live markers are intentionally convenient mutable pointers.  They are
not, by themselves, sufficient to reproduce what a consumer knew at a given
deadline.  This module stores every distinct payload observed by a live role in
an append-only, generation-scoped evidence tree.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

import fno_oi_common as common


EVIDENCE_SCHEMA_VERSION = "fno_live_evidence_v1"


class EvidenceError(RuntimeError):
    """Base class for evidence selection and integrity failures."""


class EvidenceMissingError(EvidenceError):
    """Raised when strict replay cannot find required immutable evidence."""


class EvidenceIntegrityError(EvidenceError):
    """Raised when an evidence envelope no longer matches its payload hash."""


@dataclass(frozen=True)
class EvidenceRevision:
    path: Path
    artifact_kind: str
    generation: str
    session_date: date
    slot: str
    observed_at_ist: datetime
    payload_sha256: str
    payload: dict[str, Any]
    immutable: bool = True
    source_kind: str = "immutable_evidence"


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]+", "_", str(value).strip().lower()).strip("_")
    if not normalized:
        raise ValueError("Evidence path component cannot be empty.")
    return normalized


def _normalize_slot(slot: str) -> str:
    normalized = str(slot).strip().replace(":", "")
    if not re.fullmatch(r"\d{4}", normalized):
        raise ValueError(f"Invalid evidence slot: {slot!r}")
    return normalized


def _to_ist(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def evidence_kind_dir(
    root: Path,
    *,
    session_date: date,
    slot: str,
    artifact_kind: str,
) -> Path:
    return (
        Path(root)
        / session_date.isoformat()
        / f"slot_{_normalize_slot(slot)}"
        / _safe_component(artifact_kind)
    )


def archive_json_evidence(
    root: Path,
    *,
    generation: str,
    session_date: date,
    slot: str,
    artifact_kind: str,
    payload: Mapping[str, Any],
    observed_at: datetime | None = None,
) -> Path:
    """Archive one observation without replacing an earlier observation.

    Repeated observations of identical content are deduplicated.  A changed
    marker necessarily has a new content hash and therefore creates a new,
    immutable revision.
    """

    # Normalize exactly as the JSON writer will, so dedupe comparisons are
    # stable for numpy/pandas scalar values as well as ordinary JSON values.
    body = json.loads(json.dumps(dict(payload), ensure_ascii=True, default=str))
    digest = common.canonical_json_sha256(body)
    directory = evidence_kind_dir(
        root,
        session_date=session_date,
        slot=slot,
        artifact_kind=artifact_kind,
    )
    directory.mkdir(parents=True, exist_ok=True)
    existing = sorted(directory.glob(f"*_{digest}.json"))
    if existing:
        for candidate in existing:
            revision = _load_revision(candidate)
            if (
                revision.artifact_kind != _safe_component(artifact_kind)
                or revision.generation != _safe_component(generation)
                or revision.session_date != session_date
                or revision.slot != _normalize_slot(slot)
                or revision.payload_sha256 != digest
                or revision.payload != body
            ):
                raise EvidenceIntegrityError(
                    f"Existing evidence does not match requested observation: "
                    f"{candidate}"
                )
        return existing[0]

    observed = _to_ist(observed_at or common.now_ist())
    stamp = observed.strftime("%Y%m%dT%H%M%S%f%z")
    path = directory / f"{stamp}_{digest}.json"
    envelope = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "artifact_kind": _safe_component(artifact_kind),
        "generation": _safe_component(generation),
        "session_date": session_date.isoformat(),
        "slot": _normalize_slot(slot),
        "observed_at_ist": observed.isoformat(timespec="microseconds"),
        "payload_sha256": digest,
        "payload": body,
    }
    if path.exists():
        # The only legitimate collision is the same observation being retried.
        current = json.loads(path.read_text(encoding="utf-8"))
        if current != envelope:
            raise EvidenceIntegrityError(f"Evidence path collision: {path}")
        return path
    common.atomic_write_json(path, envelope)
    return path


def _load_revision(path: Path) -> EvidenceRevision:
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError) as exc:
        raise EvidenceIntegrityError(f"Unreadable evidence envelope {path}: {exc}") from exc
    if envelope.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise EvidenceIntegrityError(f"Unsupported evidence schema in {path}")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise EvidenceIntegrityError(f"Evidence payload is not an object: {path}")
    digest = common.canonical_json_sha256(payload)
    if digest != str(envelope.get("payload_sha256", "")):
        raise EvidenceIntegrityError(f"Evidence payload hash mismatch: {path}")
    try:
        observed = _to_ist(envelope["observed_at_ist"])
        session = date.fromisoformat(str(envelope["session_date"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceIntegrityError(f"Invalid evidence metadata in {path}: {exc}") from exc
    return EvidenceRevision(
        path=path,
        artifact_kind=str(envelope.get("artifact_kind", "")),
        generation=str(envelope.get("generation", "")),
        session_date=session,
        slot=str(envelope.get("slot", "")),
        observed_at_ist=observed,
        payload_sha256=digest,
        payload=payload,
    )


def list_revisions(
    root: Path,
    *,
    session_date: date,
    slot: str,
    artifact_kind: str,
    generation: str | None = None,
    strict: bool = False,
) -> list[EvidenceRevision]:
    directory = evidence_kind_dir(
        root,
        session_date=session_date,
        slot=slot,
        artifact_kind=artifact_kind,
    )
    revisions: list[EvidenceRevision] = []
    errors: list[str] = []
    for path in sorted(directory.glob("*.json")) if directory.exists() else []:
        try:
            revision = _load_revision(path)
        except EvidenceIntegrityError as exc:
            errors.append(str(exc))
            continue
        if (
            revision.session_date != session_date
            or revision.slot != _normalize_slot(slot)
            or revision.artifact_kind != _safe_component(artifact_kind)
            or (
                generation is not None
                and revision.generation != _safe_component(generation)
            )
        ):
            errors.append(f"Evidence metadata/path mismatch: {path}")
            continue
        revisions.append(revision)
    if strict and errors:
        raise EvidenceIntegrityError("; ".join(errors))
    return sorted(
        revisions,
        key=lambda item: (item.observed_at_ist, item.payload_sha256, str(item.path)),
    )


def select_revision(
    root: Path,
    *,
    session_date: date,
    slot: str,
    artifact_kind: str,
    mode: str,
    generation: str | None = None,
    strict: bool = False,
) -> EvidenceRevision | None:
    """Select the first live observation or latest counterfactual revision."""

    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in {"observed", "counterfactual"}:
        raise ValueError(f"Unsupported replay evidence mode: {mode}")
    revisions = list_revisions(
        root,
        session_date=session_date,
        slot=slot,
        artifact_kind=artifact_kind,
        generation=generation,
        strict=strict,
    )
    if not revisions:
        if strict:
            raise EvidenceMissingError(
                f"Missing {artifact_kind} evidence for {session_date} slot {_normalize_slot(slot)}"
            )
        return None
    return revisions[0] if normalized_mode == "observed" else revisions[-1]
