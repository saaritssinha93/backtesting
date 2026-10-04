"""Hash-verified, no-authority prospective shadow session capture.

The lifecycle is deliberately split into three irreversible evidence steps:

* ``prepare`` freezes dataset/model/strategy inputs before the session opens;
* ``seal_decisions`` freezes decisions before the session closes;
* ``finalize`` joins finalized outcomes after the close and appends one journal row.

This module never imports a live worker or broker client and cannot place,
modify, or cancel an order.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
SESSION_SCHEMA = "eqidv2.v13_v10_g.prospective_shadow_session.v1"
PREPARED_SCHEMA = "eqidv2.v13_v10_g.prospective_shadow_prepared.v1"
DECISIONS_SCHEMA = "eqidv2.v13_v10_g.shadow_decisions.v1"
OUTCOMES_SCHEMA = "eqidv2.v13_v10_g.shadow_outcomes.v1"


@dataclass(frozen=True)
class ShadowEvidence:
    session_date: date
    state: str
    path: Path
    sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_date": self.session_date.isoformat(),
            "state": self.state,
            "path": str(self.path),
            "sha256": self.sha256,
            "mode": "PROSPECTIVE_SHADOW",
            "execution_authority": False,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def _exclusive_write(path: Path, payload: bytes) -> None:
    """Create immutable evidence or prove an existing object is identical."""

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise FileExistsError(f"conflicting immutable shadow evidence: {path}")
        return
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _copy_immutable(source: Path, destination: Path) -> dict[str, Any]:
    source = source.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"shadow input is not a file: {source}")
    before = sha256_file(source)
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != before or sha256_file(source) != before:
        raise RuntimeError(f"shadow input changed during capture: {source}")
    _exclusive_write(destination, raw)
    observed = sha256_file(destination)
    if observed != before:
        raise RuntimeError(f"shadow input copy verification failed: {destination}")
    return {
        "source_path": str(source),
        "captured_path": str(destination.resolve()),
        "bytes": len(raw),
        "sha256": before,
    }


def _now(value: datetime | None) -> datetime:
    stamp = value or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return stamp.astimezone(IST)


def _session_root(output_root: Path, session_date: date) -> Path:
    return output_root.resolve() / "shadow_sessions" / session_date.isoformat()


def _before_open(session_date: date, now: datetime) -> None:
    if now.date() != session_date or now.timetz().replace(tzinfo=None) >= time(9, 15):
        raise ValueError("shadow inputs must be prepared on the session date before 09:15 IST")


def _before_close(session_date: date, now: datetime) -> None:
    if now.date() != session_date or now.timetz().replace(tzinfo=None) >= time(15, 30):
        raise ValueError("shadow decisions must be sealed on the session date before 15:30 IST")


def _after_close(session_date: date, now: datetime) -> None:
    if now.date() < session_date or (
        now.date() == session_date and now.timetz().replace(tzinfo=None) < time(15, 30)
    ):
        raise ValueError("shadow outcomes cannot be finalized before 15:30 IST")


def prepare_shadow_session(
    *,
    session_date: date,
    dataset: Path,
    model: Path,
    strategy: Path,
    output_root: Path,
    now: datetime | None = None,
) -> ShadowEvidence:
    """Freeze declared inputs before the market session; never execute them."""

    stamp = _now(now)
    _before_open(session_date, stamp)
    root = _session_root(output_root, session_date)
    inputs = root / "inputs"
    artifacts = {
        "dataset": _copy_immutable(dataset, inputs / "dataset.snapshot"),
        "model": _copy_immutable(model, inputs / "model.snapshot"),
        "strategy": _copy_immutable(strategy, inputs / "strategy.snapshot"),
    }
    payload: dict[str, Any] = {
        "schema_version": PREPARED_SCHEMA,
        "mode": "PROSPECTIVE_SHADOW",
        "session_date": session_date.isoformat(),
        "prepared_at_ist": stamp.isoformat(timespec="seconds"),
        "execution_authority": False,
        "broker_access": False,
        "completed": False,
        "dataset_sha256": artifacts["dataset"]["sha256"],
        "model_sha256": artifacts["model"]["sha256"],
        "strategy_fingerprint": artifacts["strategy"]["sha256"],
        "artifacts": artifacts,
    }
    path = root / "prepared_manifest.json"
    raw = _canonical_bytes(payload)
    _exclusive_write(path, raw)
    return ShadowEvidence(session_date, "PREPARED", path, hashlib.sha256(raw).hexdigest())


def _parse_stamp(value: Any, *, field: str, session_date: date) -> datetime:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid {field}") from exc
    if stamp.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    stamp = stamp.astimezone(IST)
    if stamp.date() != session_date:
        raise ValueError(f"{field} must fall on {session_date}")
    return stamp


def _validate_prepared(root: Path, session_date: date) -> dict[str, Any]:
    prepared_path = root / "prepared_manifest.json"
    prepared = _json(prepared_path)
    if (
        prepared.get("schema_version") != PREPARED_SCHEMA
        or prepared.get("mode") != "PROSPECTIVE_SHADOW"
        or prepared.get("execution_authority") is not False
        or prepared.get("session_date") != session_date.isoformat()
    ):
        raise ValueError("invalid prepared shadow manifest")
    for name, expected_field in (
        ("dataset", "dataset_sha256"),
        ("model", "model_sha256"),
        ("strategy", "strategy_fingerprint"),
    ):
        record = prepared.get("artifacts", {}).get(name, {})
        captured = Path(str(record.get("captured_path", ""))).resolve()
        if root.resolve() not in captured.parents or not captured.is_file():
            raise ValueError(f"prepared {name} snapshot is missing or outside its session")
        observed = sha256_file(captured)
        if observed != record.get("sha256") or observed != prepared.get(expected_field):
            raise ValueError(f"prepared {name} snapshot hash mismatch")
    return prepared


def seal_shadow_decisions(
    *,
    session_date: date,
    decisions: Path,
    output_root: Path,
    now: datetime | None = None,
) -> ShadowEvidence:
    """Seal a complete decision bundle before outcomes are available."""

    stamp = _now(now)
    _before_close(session_date, stamp)
    root = _session_root(output_root, session_date)
    prepared = _validate_prepared(root, session_date)
    bundle = _json(decisions.resolve())
    if (
        bundle.get("schema_version") != DECISIONS_SCHEMA
        or bundle.get("session_date") != session_date.isoformat()
        or bundle.get("execution_authority") is not False
        or bundle.get("complete") is not True
        or bundle.get("dataset_sha256") != prepared["dataset_sha256"]
        or bundle.get("model_sha256") != prepared["model_sha256"]
        or bundle.get("strategy_fingerprint") != prepared["strategy_fingerprint"]
    ):
        raise ValueError("decision bundle does not match the prepared shadow inputs")
    rows = bundle.get("rows")
    if not isinstance(rows, list):
        raise ValueError("decision bundle rows must be a list")
    identifiers: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("signal_id", "")).strip():
            raise ValueError("each shadow decision requires a signal_id")
        signal_id = str(row["signal_id"])
        if signal_id in identifiers:
            raise ValueError(f"duplicate shadow decision signal_id: {signal_id}")
        identifiers.add(signal_id)
        decision_at = _parse_stamp(
            row.get("decision_at_ist"), field="decision_at_ist", session_date=session_date
        )
        if decision_at > stamp:
            raise ValueError("a shadow decision timestamp is later than seal time")
    source_hash = sha256_file(decisions.resolve())
    captured = root / "sealed_decisions.json"
    record = _copy_immutable(decisions, captured)
    if record["sha256"] != source_hash:
        raise RuntimeError("decision bundle changed during capture")
    seal: dict[str, Any] = {
        "schema_version": DECISIONS_SCHEMA,
        "mode": "PROSPECTIVE_SHADOW",
        "session_date": session_date.isoformat(),
        "sealed_at_ist": stamp.isoformat(timespec="seconds"),
        "execution_authority": False,
        "complete": True,
        "dataset_sha256": prepared["dataset_sha256"],
        "model_sha256": prepared["model_sha256"],
        "strategy_fingerprint": prepared["strategy_fingerprint"],
        "decision_rows": len(rows),
        "signal_ids_sha256": hashlib.sha256(
            "\n".join(sorted(identifiers)).encode("utf-8")
        ).hexdigest(),
        "bundle": record,
    }
    path = root / "decision_seal.json"
    raw = _canonical_bytes(seal)
    _exclusive_write(path, raw)
    return ShadowEvidence(session_date, "DECISIONS_SEALED", path, hashlib.sha256(raw).hexdigest())


def _append_once(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_name(f".{path.name}.lock")
    try:
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise RuntimeError(f"shadow journal is locked: {lock}") from exc
    os.close(descriptor)
    try:
        existing: list[dict[str, Any]] = []
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError("shadow journal contains a non-object row")
                    existing.append(value)
        same_day = [item for item in existing if item.get("session_date") == row["session_date"]]
        if same_day:
            if len(same_day) == 1 and same_day[0] == dict(row):
                return
            raise ValueError(f"conflicting shadow journal row for {row['session_date']}")
        with path.open("ab") as handle:
            handle.write(_canonical_bytes(dict(row)))
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        try:
            lock.unlink()
        except OSError:
            pass


def finalize_shadow_session(
    *,
    session_date: date,
    outcomes: Path,
    output_root: Path,
    now: datetime | None = None,
) -> ShadowEvidence:
    """Join sealed decisions to finalized outcomes and publish journal evidence."""

    stamp = _now(now)
    _after_close(session_date, stamp)
    root = _session_root(output_root, session_date)
    prepared = _validate_prepared(root, session_date)
    seal = _json(root / "decision_seal.json")
    if (
        seal.get("schema_version") != DECISIONS_SCHEMA
        or seal.get("execution_authority") is not False
        or seal.get("complete") is not True
        or seal.get("session_date") != session_date.isoformat()
    ):
        raise ValueError("invalid decision seal")
    decision_bundle_path = Path(str(seal.get("bundle", {}).get("captured_path", ""))).resolve()
    if root.resolve() not in decision_bundle_path.parents or not decision_bundle_path.is_file():
        raise ValueError("sealed decision bundle is missing or outside its session")
    if sha256_file(decision_bundle_path) != seal.get("bundle", {}).get("sha256"):
        raise ValueError("sealed decision bundle hash mismatch")
    decisions = _json(decision_bundle_path)
    decision_rows = decisions.get("rows", [])
    decision_by_id = {str(row["signal_id"]): row for row in decision_rows}

    bundle = _json(outcomes.resolve())
    if (
        bundle.get("schema_version") != OUTCOMES_SCHEMA
        or bundle.get("session_date") != session_date.isoformat()
        or bundle.get("execution_authority") is not False
        or bundle.get("complete") is not True
    ):
        raise ValueError("outcome bundle is not a complete no-authority session")
    rows = bundle.get("rows")
    if not isinstance(rows, list):
        raise ValueError("outcome bundle rows must be a list")
    outcome_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("signal_id", "")).strip():
            raise ValueError("each shadow outcome requires a signal_id")
        signal_id = str(row["signal_id"])
        if signal_id in outcome_by_id:
            raise ValueError(f"duplicate shadow outcome signal_id: {signal_id}")
        outcome_at = _parse_stamp(
            row.get("outcome_at_ist"), field="outcome_at_ist", session_date=session_date
        )
        decision = decision_by_id.get(signal_id)
        if decision is None:
            raise ValueError(f"outcome has no sealed decision: {signal_id}")
        decision_at = _parse_stamp(
            decision.get("decision_at_ist"), field="decision_at_ist", session_date=session_date
        )
        if outcome_at < decision_at:
            raise ValueError(f"outcome precedes decision: {signal_id}")
        outcome_by_id[signal_id] = row
    if set(decision_by_id) != set(outcome_by_id):
        raise ValueError("outcomes must join exactly one-to-one with sealed decisions")
    outcome_record = _copy_immutable(outcomes, root / "sealed_outcomes.json")

    manifest: dict[str, Any] = {
        "schema_version": SESSION_SCHEMA,
        "mode": "PROSPECTIVE_SHADOW",
        "session_date": session_date.isoformat(),
        "prepared_at_ist": prepared["prepared_at_ist"],
        "decisions_sealed_at_ist": seal["sealed_at_ist"],
        "completed_at_ist": stamp.isoformat(timespec="seconds"),
        "completed": True,
        "execution_authority": False,
        "broker_access": False,
        "outcome_join_state": "COMPLETE",
        "strategy_fingerprint": prepared["strategy_fingerprint"],
        "dataset_sha256": prepared["dataset_sha256"],
        "model_sha256": prepared["model_sha256"],
        "decision_rows": len(decision_by_id),
        "outcome_rows": len(outcome_by_id),
        "artifacts": {
            "prepared_manifest": {
                "path": str((root / "prepared_manifest.json").resolve()),
                "sha256": sha256_file(root / "prepared_manifest.json"),
            },
            "decision_seal": {
                "path": str((root / "decision_seal.json").resolve()),
                "sha256": sha256_file(root / "decision_seal.json"),
            },
            "decision_bundle": seal["bundle"],
            "outcome_bundle": outcome_record,
        },
    }
    manifest_path = root / "manifest.json"
    raw = _canonical_bytes(manifest)
    _exclusive_write(manifest_path, raw)
    manifest_sha = hashlib.sha256(raw).hexdigest()
    journal_row = {
        "schema_version": SESSION_SCHEMA,
        "mode": "PROSPECTIVE_SHADOW",
        "completed": True,
        "execution_authority": False,
        "outcome_join_state": "COMPLETE",
        "strategy_fingerprint": prepared["strategy_fingerprint"],
        "dataset_sha256": prepared["dataset_sha256"],
        "model_sha256": prepared["model_sha256"],
        "session_date": session_date.isoformat(),
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha,
    }
    _append_once(output_root.resolve() / "shadow_observations.jsonl", journal_row)
    return ShadowEvidence(session_date, "COMPLETE", manifest_path, manifest_sha)


def verify_shadow_session(
    *, session_date: date, output_root: Path
) -> dict[str, Any]:
    """Verify the final manifest, every captured artifact, and journal link."""

    root = _session_root(output_root, session_date)
    manifest_path = root / "manifest.json"
    manifest = _json(manifest_path)
    errors: list[str] = []
    if (
        manifest.get("schema_version") != SESSION_SCHEMA
        or manifest.get("execution_authority") is not False
        or manifest.get("completed") is not True
        or manifest.get("outcome_join_state") != "COMPLETE"
        or manifest.get("session_date") != session_date.isoformat()
    ):
        errors.append("manifest_contract")
    for name, record in manifest.get("artifacts", {}).items():
        try:
            path = Path(str(record["path"] if "path" in record else record["captured_path"])).resolve()
            if root.resolve() not in path.parents or sha256_file(path) != record["sha256"]:
                errors.append(f"artifact:{name}")
        except (KeyError, OSError, ValueError):
            errors.append(f"artifact:{name}")
    manifest_sha = sha256_file(manifest_path)
    journal_path = output_root.resolve() / "shadow_observations.jsonl"
    linked = False
    if journal_path.is_file():
        for line in journal_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(row, dict)
                and row.get("session_date") == session_date.isoformat()
                and row.get("manifest_sha256") == manifest_sha
                and Path(str(row.get("manifest_path", ""))).resolve() == manifest_path.resolve()
            ):
                linked = True
    if not linked:
        errors.append("journal_link")
    return {
        "session_date": session_date.isoformat(),
        "state": "VERIFIED" if not errors else "INVALID",
        "verified": not errors,
        "errors": errors,
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha,
        "execution_authority": False,
    }


__all__ = [
    "DECISIONS_SCHEMA",
    "OUTCOMES_SCHEMA",
    "PREPARED_SCHEMA",
    "SESSION_SCHEMA",
    "ShadowEvidence",
    "finalize_shadow_session",
    "prepare_shadow_session",
    "seal_shadow_decisions",
    "sha256_file",
    "verify_shadow_session",
]
