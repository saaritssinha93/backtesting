"""Bounded-retry coherent snapshots across mutable JSON pointer files."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping

from ai_platform.schemas import CoherentSnapshot

from .common import ArtifactError, canonical_json_sha256, file_sha256, source_metadata


def read_coherent_snapshot(
    sources: Mapping[str, Path | str],
    *,
    equal_fields: Iterable[str] = (
        "session_date",
        "strategy_version",
        "strategy_fingerprint",
    ),
    max_attempts: int = 3,
) -> CoherentSnapshot:
    if not sources:
        raise ArtifactError("A coherent snapshot needs at least one source")
    if max_attempts < 1 or max_attempts > 10:
        raise ValueError("max_attempts must be between 1 and 10")
    paths = {name: Path(path) for name, path in sources.items()}
    last_change = ""
    for _ in range(max_attempts):
        payloads: dict[str, dict] = {}
        hashes: dict[str, str] = {}
        for name, path in paths.items():
            if not path.is_file():
                raise ArtifactError(f"Snapshot source does not exist: {path}")
            raw = path.read_bytes()
            hashes[name] = hashlib.sha256(raw).hexdigest()
            try:
                payload = json.loads(raw)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ArtifactError(f"Invalid JSON snapshot source {path}: {exc}") from exc
            if not isinstance(payload, dict):
                raise ArtifactError(f"Snapshot source is not an object: {path}")
            payloads[name] = payload
        changed = [name for name, path in paths.items() if file_sha256(path) != hashes[name]]
        if changed:
            last_change = ", ".join(changed)
            continue
        for field in equal_fields:
            values = {payload[field] for payload in payloads.values() if field in payload}
            if len(values) > 1:
                raise ArtifactError(f"Conflicting {field} values in coherent snapshot: {values!r}")
        metadata = {
            name: source_metadata(paths[name], hashes[name], name) for name in paths
        }
        identity = canonical_json_sha256(
            {name: {"path": str(paths[name]), "sha256": hashes[name]} for name in sorted(paths)}
        )
        return CoherentSnapshot(identity, payloads, metadata)
    raise ArtifactError(
        f"Snapshot sources changed during all {max_attempts} attempts: {last_change}"
    )
