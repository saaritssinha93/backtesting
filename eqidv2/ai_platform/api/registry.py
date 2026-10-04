"""Registered-source resolution with an enforced runtime-root boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ai_platform.adapters.common import ArtifactError, read_json


class SourceRegistry:
    def __init__(self, registry_path: Path, runtime_root: Path):
        payload, _, _ = read_json(registry_path)
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            raise ArtifactError("Source registry has no sources object")
        self.registry_path = registry_path.resolve()
        self.runtime_root = runtime_root.resolve()
        self.sources: dict[str, dict[str, Any]] = sources

    def record(self, source_id: str) -> dict[str, Any]:
        record = self.sources.get(source_id)
        if not isinstance(record, dict):
            raise KeyError(source_id)
        return record

    def path(self, source_id: str) -> Path:
        record = self.record(source_id)
        relative = Path(str(record.get("relative_path", "")))
        if not str(relative) or relative.is_absolute():
            raise ArtifactError(f"Registered source path must be relative: {source_id}")
        resolved = (self.runtime_root / relative).resolve()
        try:
            resolved.relative_to(self.runtime_root)
        except ValueError as exc:
            raise ArtifactError(f"Registered source leaves runtime root: {source_id}") from exc
        return resolved
