"""Read the pinned strategy contract without importing live strategy modules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .common import ArtifactError, file_sha256, read_json


@dataclass(frozen=True)
class StrategyContract:
    label: str
    strategy_version: str
    strategy_fingerprint: str
    live_generation: str
    frozen_config_path: Path
    frozen_config_sha256: str
    settings: Mapping[str, Any]


def read_strategy_contract(
    registry_path: Path | str,
    *,
    profile: str = "V13_V10_G",
) -> StrategyContract:
    registry, source, _ = read_json(registry_path)
    profiles = registry.get("profiles")
    if not isinstance(profiles, dict) or profile not in profiles:
        raise ArtifactError(f"Unknown strategy profile {profile!r} in {source}")
    record = profiles[profile]
    if not isinstance(record, dict):
        raise ArtifactError(f"Invalid strategy profile {profile!r} in {source}")
    config_path = Path(str(record.get("frozen_config_path", "")))
    expected_sha = str(record.get("frozen_config_sha256", "")).lower()
    observed_sha = file_sha256(config_path)
    if observed_sha != expected_sha:
        raise ArtifactError(
            f"Frozen config hash mismatch: expected {expected_sha}, got {observed_sha}"
        )
    settings, _, _ = read_json(config_path)
    if settings.get("morning_slots", False) or settings.get("two_bar_continuation", False):
        raise ArtifactError("Rejected strategy expansions are enabled in frozen config")
    return StrategyContract(
        label=str(record["label"]),
        strategy_version=str(record["strategy_version"]),
        strategy_fingerprint=str(record["strategy_fingerprint"]),
        live_generation=str(record["live_generation"]),
        frozen_config_path=config_path,
        frozen_config_sha256=observed_sha,
        settings=settings,
    )
