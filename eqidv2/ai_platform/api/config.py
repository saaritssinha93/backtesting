"""Explicit API configuration with no credential-file reads."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ApiSettings:
    runtime_root: Path
    source_registry_path: Path
    profile_registry_path: Path
    api_token: str
    principal: str = "local-operator"
    capabilities: frozenset[str] = field(
        default_factory=lambda: frozenset({"read:platform", "read:evidence"})
    )
    max_response_bytes: int = 256 * 1024
    max_request_bytes: int = 64 * 1024
    cache_ttl_seconds: float = 2.0
    heartbeat_max_age_seconds: float = 120.0
    observability_root: Path | None = None

    def __post_init__(self) -> None:
        if len(self.api_token) < 32:
            raise ValueError("AI_PLATFORM_API_TOKEN must contain at least 32 characters")
        if self.max_response_bytes < 1024 or self.max_request_bytes < 1024:
            raise ValueError("API byte limits must be at least 1024")
        if self.cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds cannot be negative")
        if self.heartbeat_max_age_seconds <= 0:
            raise ValueError("heartbeat_max_age_seconds must be positive")
        if self.observability_root is None:
            object.__setattr__(
                self, "observability_root", (self.runtime_root / "observability").resolve()
            )

    @classmethod
    def from_env(cls, workspace_root: Path | None = None) -> "ApiSettings":
        workspace = workspace_root or Path(__file__).resolve().parents[2]
        token = os.environ.get("AI_PLATFORM_API_TOKEN", "")
        if not token:
            raise RuntimeError(
                "AI_PLATFORM_API_TOKEN is required; provide it through the process environment"
            )
        return cls(
            runtime_root=Path(
                os.environ.get("EQIDV2_RUNTIME_ROOT", "C:/TradingData/eqidv2")
            ),
            source_registry_path=workspace / "docs/ai_platform/source_registry.json",
            profile_registry_path=workspace / "docs/ai_platform/profile_registry.json",
            api_token=token,
            heartbeat_max_age_seconds=float(
                os.environ.get("AI_PLATFORM_HEARTBEAT_MAX_AGE_SECONDS", "120")
            ),
            observability_root=Path(
                os.environ.get(
                    "EQIDV2_OBSERVABILITY_ROOT",
                    str(
                        Path(os.environ.get("EQIDV2_RUNTIME_ROOT", "C:/TradingData/eqidv2"))
                        / "observability"
                    ),
                )
            ),
        )
