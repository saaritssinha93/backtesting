"""Adapter for the daily V13-V10-G replay status/result envelope."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_platform.schemas import DailyReplaySnapshot

from .common import ArtifactError, file_sha256, read_json, require_equal, source_metadata


DAILY_REPLAY_SCHEMA = "fno_v13_v10_g_daily_replay_v1"
G3_DAILY_REPLAY_SCHEMA = "fno_g3_production_run_v1"


def _require_bound(actual, expected, field: str, source: Path) -> None:
    if actual is None or expected is None or actual != expected:
        raise ArtifactError(f"Missing or conflicting G3 {field} in {source}")


def verified_g3_result(path: Path | str) -> dict:
    """Read the committed production generation, never a presentation fallback."""
    source = Path(path).resolve()
    envelope, _, _ = read_json(source)
    if source.parent.name == "latest":
        root = source.parent.parent
        pointer, _, _ = read_json(source.parent / "current_run.json")
        run = (root / str(pointer.get("run_dir", ""))).resolve()
        if not run.is_relative_to(root / "production" / "runs"):
            raise ArtifactError("G3 current pointer leaves production runs")
        manifest_path = run / "run_manifest.json"
        _require_bound(file_sha256(manifest_path), pointer.get("manifest_sha256"), "manifest_sha256", source)
        _require_bound(str(manifest_path), str(Path(str(envelope.get("run_manifest", ""))).resolve()), "run_manifest", source)
        _require_bound(envelope.get("manifest_sha256"), pointer.get("manifest_sha256"), "summary.manifest_sha256", source)
    else:
        run = source.parent
        manifest_path = run / "run_manifest.json"
        pointer = envelope
    manifest, _, _ = read_json(manifest_path)
    for key in ("strategy_version", "strategy_fingerprint", "session_date", "role"):
        _require_bound(manifest.get(key), envelope.get(key), key, source)
        _require_bound(manifest.get(key), pointer.get(key), "pointer." + key, source)
    for key, expected in (("schema_version", G3_DAILY_REPLAY_SCHEMA), ("role", "production_daily"),
                          ("execution_authority", False), ("complete", True), ("state", "SUCCESS")):
        _require_bound(manifest.get(key), expected, key, source)
    if not isinstance(manifest.get("metrics"), dict) or not isinstance(manifest.get("artifacts"), dict):
        raise ArtifactError("G3 production manifest requires metrics and artifacts")
    if "portfolio_trades" not in manifest["artifacts"]:
        raise ArtifactError("G3 production manifest has no portfolio ledger")
    for artifact in manifest["artifacts"].values():
        if not isinstance(artifact, dict) or not artifact.get("path") or not artifact.get("sha256"):
            raise ArtifactError("Invalid G3 artifact record")
        artifact_path = (run / str(artifact["path"])).resolve()
        if not artifact_path.is_relative_to(run):
            raise ArtifactError("G3 artifact leaves its generation directory")
        require_equal(file_sha256(artifact_path), artifact["sha256"], "artifact.sha256", source)
    return manifest


def read_daily_replay(
    path: Path | str,
    *,
    expected_strategy_version: str | None = None,
    expected_strategy_fingerprint: str | None = None,
) -> DailyReplaySnapshot:
    envelope, source, sha = read_json(path)
    g3 = envelope.get("schema_version") == G3_DAILY_REPLAY_SCHEMA
    if str(expected_strategy_version or envelope.get("strategy_version", "")).startswith("V13-V10-G-3-PRODUCTION-") and not g3:
        raise ArtifactError("G3 daily result has no production-run schema")
    if g3:
        envelope = verified_g3_result(source)
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
    status = str(envelope.get("state" if g3 else "status", "UNKNOWN")).upper()
    result = envelope if g3 else envelope.get("result")
    complete = False
    metrics = coverage = verification = None
    if status == "SUCCESS":
        if not isinstance(result, dict):
            raise ArtifactError(f"SUCCESS envelope has no result object: {source}")
        require_equal(result.get("schema_version"), G3_DAILY_REPLAY_SCHEMA if g3 else DAILY_REPLAY_SCHEMA, "schema_version", source)
        require_equal(result.get("strategy_version"), version, "result.strategy_version", source)
        require_equal(result.get("session_date"), session.isoformat(), "result.session_date", source)
        if g3:
            require_equal(result.get("strategy_fingerprint"), fingerprint, "result.strategy_fingerprint", source)
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
