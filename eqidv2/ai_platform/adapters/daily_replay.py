"""Adapter for the daily V13-V10-G replay status/result envelope."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_platform.schemas import DailyReplaySnapshot

from .common import ArtifactError, read_json, require_equal, source_metadata


DAILY_REPLAY_SCHEMA = "fno_v13_v10_g_daily_replay_v1"


def read_daily_replay(
    path: Path | str,
    *,
    expected_strategy_version: str | None = None,
    expected_strategy_fingerprint: str | None = None,
) -> DailyReplaySnapshot:
    envelope, source, sha = read_json(path)
    try:
        session = date.fromisoformat(str(envelope["session_date"]))
    except (KeyError, ValueError) as exc:
        raise ArtifactError(f"Invalid session_date in {source}") from exc
    version = str(envelope.get("strategy_version", ""))
    fingerprint = str(envelope.get("strategy_fingerprint", ""))
    require_equal(version, expected_strategy_version, "strategy_version", source)
    require_equal(
        fingerprint,
        expected_strategy_fingerprint,
        "strategy_fingerprint",
        source,
    )
    status = str(envelope.get("status", "UNKNOWN")).upper()
    result = envelope.get("result")
    complete = False
    metrics = coverage = verification = None
    if status == "SUCCESS":
        if not isinstance(result, dict):
            raise ArtifactError(f"SUCCESS envelope has no result object: {source}")
        require_equal(result.get("schema_version"), DAILY_REPLAY_SCHEMA, "schema_version", source)
        require_equal(result.get("strategy_version"), version, "result.strategy_version", source)
        require_equal(result.get("session_date"), session.isoformat(), "result.session_date", source)
        if result.get("complete") is not True or str(result.get("state", "")).upper() != "SUCCESS":
            raise ArtifactError(f"SUCCESS envelope contains an incomplete result: {source}")
        if not isinstance(result.get("metrics"), dict):
            raise ArtifactError(f"Completed result has no metrics object: {source}")
        complete = True
        metrics = result["metrics"]
        coverage = result.get("coverage") if isinstance(result.get("coverage"), dict) else None
        verification = (
            result.get("data_verification")
            if isinstance(result.get("data_verification"), dict)
            else None
        )
    return DailyReplaySnapshot(
        session_date=session,
        status=status,
        complete=complete,
        strategy_version=version,
        strategy_fingerprint=fingerprint,
        metrics=metrics,
        coverage=coverage,
        data_verification=verification,
        source=source_metadata(source, sha, "daily_replay_latest_json"),
        reason=str(envelope.get("reason", "")),
    )
