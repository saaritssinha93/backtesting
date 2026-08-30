"""Fail-closed activation control for the FNO V8-Combined PAPER session.

Two independent conditions are required before credential discovery:

1. ``activation.json`` must point at one immutable, hash-matching permit for
   the current session and the exact current strategy/runtime bundle.
2. ``kill_switch.json`` must be explicitly disengaged and bound to that same
   permit hash.

Approval never disarms the kill switch, starts a process, or changes Task
Scheduler.  There is intentionally no LIVE mode or task-enabling command.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import tempfile
import time as time_module
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar

import fno_v8_combined_paper_config as config
from eqidv2_runtime_paths import RUNTIME_STATUS_DIR


ACTIVATION_POINTER_SCHEMA_VERSION = "fno_v8_combined_paper_activation_pointer_v1"
ACTIVATION_PERMIT_SCHEMA_VERSION = "fno_v8_combined_paper_activation_permit_v1"
KILL_SWITCH_SCHEMA_VERSION = "fno_v8_combined_paper_kill_switch_v1"
CONTROL_EVENT_SCHEMA_VERSION = "fno_v8_combined_paper_control_event_v1"
DASHBOARD_RUNTIME_IDENTITY_SCHEMA_VERSION = "eqidv2_log_dashboard_runtime_identity_v1"
DASHBOARD_RUNTIME_IDENTITY_PATH = (
    RUNTIME_STATUS_DIR / "log_dashboard_server.runtime.json"
)

APPROVAL_PHRASE = "I APPROVE ONE SESSION OF FNO V8-COMBINED PAPER ONLY"
APPROVAL_PHRASE_SHA256 = hashlib.sha256(APPROVAL_PHRASE.encode("utf-8")).hexdigest()

RUNTIME_BUNDLE_FILENAMES = (
    "fno_v8_combined_paper_config.py",
    "fno_v8_combined_paper_control.py",
    "fno_v8_combined_paper_engine.py",
    "fno_v8_combined_paper_market_data.py",
    "fno_v8_combined_paper_session.py",
    # Direct executable dependencies used for runtime paths, credential/client
    # construction, exact timestamps, atomic writes and five-minute source
    # parsing.  A one-session approval must not survive drift in any of them.
    "eqidv2_runtime_paths.py",
    "fno_oi_common.py",
    "fno_oi_hybrid_data.py",
    # V8 independently evaluates the full mapped universe, while reconciling
    # the finalized V6 scanner snapshot as a required diagnostic dependency.
    # The producer implementation/runner must not drift after approval even
    # though its candidate rows are not authoritative V8 admission inputs.
    "fno_v5_live.py",
    "fno_v6_live.py",
    "fno_v6_live_config.py",
    "fno_oi_ema_confirm_backtest.py",
    "bat/run_fno_v6_scanner_5min.bat",
    # Mutual-exclusion and lifecycle controls are part of the approval
    # boundary: same-day autofix must honor the V8 scheduler mode, and the
    # dashboard must refuse restart/PID actions for disabled V6 tasks.  These
    # controls must not drift between permit approval and use.
    "preopen_session_healthcheck.py",
    "preopen_session_autofix.py",
    "log_dashboard_server.py",
    "bat/switch_fno_v6_1m_to_v8_paper_after_approval.ps1",
    "bat/restore_fno_v6_1m_after_v8_paper.ps1",
    # Task Scheduler executes this wrapper, so its environment/path bindings
    # are part of the approved runtime rather than untrusted deployment glue.
    "bat/run_fno_v8_combined_paper_session.bat",
)
_HEX_DIGITS = frozenset("0123456789abcdef")
_T = TypeVar("_T")


@dataclass(frozen=True)
class ControlPaths:
    activation_path: Path = config.ACTIVATION_PATH
    kill_switch_path: Path = config.KILL_SWITCH_PATH
    permit_archive_root: Path = config.PERMIT_ARCHIVE_ROOT
    event_archive_root: Path = config.CONTROL_ROOT / "events"


DEFAULT_CONTROL_PATHS = ControlPaths()


@dataclass(frozen=True)
class ActivationDecision:
    allowed: bool
    reason: str
    session_date: str
    permit_id: str = ""
    permit_sha256: str = ""
    strategy_fingerprint: str = ""
    runtime_bundle_sha256: str = ""
    permit: Mapping[str, Any] | None = None


class ActivationBlockedError(RuntimeError):
    """Raised before credentials when the two-key PAPER gate is not valid."""

    def __init__(self, reason: str) -> None:
        self.reason = str(reason)
        super().__init__(f"FNO V8-Combined PAPER activation blocked: {self.reason}")


class ControlCommandError(ValueError):
    """Raised for invalid or unsafe approve/revoke/kill/disarm commands."""


def _now_ist() -> datetime:
    return datetime.now(config.IST)


def _normalize_day(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.astimezone(config.IST).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ControlCommandError(f"invalid session date: {value!r}") from exc


def _normalize_now(value: datetime | None) -> datetime:
    observed = value or _now_ist()
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ControlCommandError("control timestamps must be timezone-aware")
    return observed.astimezone(config.IST)


def _parse_timestamp(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ActivationBlockedError(f"INVALID_{field.upper()}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ActivationBlockedError(f"NAIVE_{field.upper()}")
    return parsed.astimezone(config.IST)


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and set(text) <= _HEX_DIGITS


def _require_text(value: Any, field: str) -> str:
    normalized = " ".join(str(value or "").split())
    if not normalized:
        raise ControlCommandError(f"{field} cannot be empty")
    return normalized


def _json_file_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(_json_file_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        last_error: OSError | None = None
        for attempt in range(8):
            try:
                os.replace(temp_path, destination)
                temp_path = None
                return
            except OSError as exc:
                last_error = exc
                if attempt < 7:
                    time_module.sleep(0.025 * (attempt + 1))
        assert last_error is not None
        raise last_error
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> bytes:
    """Write one archive exactly once and return its exact stored bytes."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    content = _json_file_bytes(payload)
    try:
        with destination.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        existing = destination.read_bytes()
        if existing != content:
            raise ControlCommandError(f"immutable archive collision: {destination}")
        return existing
    return content


def _read_json_object(path: Path, missing_reason: str, invalid_reason: str) -> dict[str, Any]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ActivationBlockedError(missing_reason) from exc
    except OSError as exc:
        raise ActivationBlockedError(invalid_reason) from exc
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ActivationBlockedError(invalid_reason) from exc
    if not isinstance(payload, dict):
        raise ActivationBlockedError(invalid_reason)
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def runtime_bundle_records(
    source_paths: Iterable[Path | str] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Hash the exact available PAPER runtime sources, without importing them."""

    if source_paths is None:
        base_dir = Path(__file__).resolve().parent
        entries = [
            (name.replace("\\", "/"), (base_dir / name).resolve())
            for name in RUNTIME_BUNDLE_FILENAMES
        ]
        # A permit is never minted for a partial deployment: config, control,
        # engine, market-data adapter, and session orchestrator are all part of
        # the mandatory runtime identity.
    else:
        paths = [Path(path).resolve() for path in source_paths]
        entries = [(path.name, path) for path in paths]

    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for relative_path, path in entries:
        name = path.name
        key = relative_path.casefold()
        if key in seen:
            raise ControlCommandError(
                f"duplicate runtime source identity: {relative_path}"
            )
        seen.add(key)
        if not path.is_file():
            raise ControlCommandError(f"missing runtime source: {path}")
        records.append(
            {
                "name": name,
                "relative_path": relative_path,
                "size_bytes": int(path.stat().st_size),
                "sha256": _sha256_file(path),
            }
        )
    if not records:
        raise ControlCommandError("runtime bundle has no source files")
    return tuple(
        sorted(records, key=lambda item: str(item["relative_path"]).casefold())
    )


def runtime_bundle_sha256(
    source_paths: Iterable[Path | str] | None = None,
) -> str:
    return config.canonical_json_sha256(runtime_bundle_records(source_paths))


def require_dashboard_runtime_identity(
    *,
    identity_path: Path = DASHBOARD_RUNTIME_IDENTITY_PATH,
    observed_now: datetime | None = None,
    max_heartbeat_age_seconds: float = 15.0,
) -> dict[str, Any]:
    """Require the running dashboard to have loaded the reviewed source bytes.

    The runtime bundle proves the on-disk dashboard source.  This identity also
    binds those bytes to a fresh heartbeat emitted by the loaded dashboard
    process, so a long-running pre-change or stopped dashboard cannot retain
    unsafe restart behavior across a V6-to-V8 cutover.
    """

    payload = _read_json_object(
        Path(identity_path),
        "DASHBOARD_RUNTIME_IDENTITY_MISSING",
        "DASHBOARD_RUNTIME_IDENTITY_INVALID",
    )
    if payload.get("schema_version") != DASHBOARD_RUNTIME_IDENTITY_SCHEMA_VERSION:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_IDENTITY_SCHEMA_MISMATCH")
    if str(payload.get("host", "")).strip() != "127.0.0.1":
        raise ActivationBlockedError("DASHBOARD_RUNTIME_HOST_MISMATCH")
    raw_port = payload.get("port")
    if isinstance(raw_port, bool):
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PORT_MISMATCH")
    try:
        observed_port = int(raw_port)
    except (TypeError, ValueError) as exc:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PORT_MISMATCH") from exc
    if observed_port != 8787:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PORT_MISMATCH")

    expected_source = Path(__file__).resolve().parent / "log_dashboard_server.py"
    try:
        observed_source = Path(str(payload.get("source_path", ""))).resolve()
    except (OSError, ValueError, TypeError) as exc:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_SOURCE_PATH_INVALID") from exc
    if observed_source != expected_source.resolve():
        raise ActivationBlockedError("DASHBOARD_RUNTIME_SOURCE_PATH_MISMATCH")

    expected_sha256 = _sha256_file(expected_source)
    observed_sha256 = str(payload.get("source_sha256", "")).strip().lower()
    if not _is_sha256(observed_sha256) or not hmac.compare_digest(
        observed_sha256, expected_sha256
    ):
        raise ActivationBlockedError("DASHBOARD_RUNTIME_SOURCE_SHA256_MISMATCH")

    raw_pid = payload.get("pid")
    if isinstance(raw_pid, bool):
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PID_INVALID")
    try:
        pid = int(raw_pid)
    except (TypeError, ValueError) as exc:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PID_INVALID") from exc
    if pid <= 0:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_PID_INVALID")

    identity_started = _parse_timestamp(
        payload.get("started_at_utc"), "dashboard_runtime_started_at_utc"
    )
    heartbeat_at = _parse_timestamp(
        payload.get("heartbeat_at_utc"), "dashboard_runtime_heartbeat_at_utc"
    )
    if heartbeat_at < identity_started:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_HEARTBEAT_BEFORE_START")
    observed = _normalize_now(observed_now)
    heartbeat_age = (observed - heartbeat_at.astimezone(config.IST)).total_seconds()
    if heartbeat_age < -5.0:
        raise ActivationBlockedError("DASHBOARD_RUNTIME_HEARTBEAT_IN_FUTURE")
    if heartbeat_age > float(max_heartbeat_age_seconds):
        raise ActivationBlockedError("DASHBOARD_RUNTIME_HEARTBEAT_STALE")

    return {
        "schema_version": DASHBOARD_RUNTIME_IDENTITY_SCHEMA_VERSION,
        "pid": pid,
        "source_path": str(expected_source.resolve()),
        "source_sha256": expected_sha256,
        "started_at_utc": str(payload.get("started_at_utc")),
        "heartbeat_at_utc": str(payload.get("heartbeat_at_utc")),
        "heartbeat_age_seconds": max(0.0, heartbeat_age),
    }


def _archive_control_event(
    paths: ControlPaths,
    *,
    action: str,
    actor: str,
    session_date: date,
    now: datetime,
    details: Mapping[str, Any],
) -> Path:
    event = {
        "schema_version": CONTROL_EVENT_SCHEMA_VERSION,
        "event_id": uuid.uuid4().hex,
        "action": str(action).upper(),
        "actor": actor,
        "session_date": session_date.isoformat(),
        "created_at_ist": now.isoformat(timespec="microseconds"),
        "details": dict(details),
    }
    digest = config.canonical_json_sha256(event)
    filename = (
        f"{now.strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{str(action).lower()}_{digest}.json"
    )
    target = paths.event_archive_root / session_date.isoformat() / filename
    _write_immutable_json(target, event)
    return target


def _session_boundary(session_date: date, hhmm: str) -> datetime:
    parsed = time.fromisoformat(hhmm)
    return datetime.combine(session_date, parsed, tzinfo=config.IST)


def _load_bound_permit(
    session_date: date,
    *,
    paths: ControlPaths,
    expected_bundle_sha256: str,
    now: datetime | None,
    enforce_current_window: bool,
) -> tuple[dict[str, Any], str]:
    pointer = _read_json_object(
        paths.activation_path,
        "ACTIVATION_POINTER_MISSING",
        "ACTIVATION_POINTER_INVALID",
    )
    if pointer.get("schema_version") != ACTIVATION_POINTER_SCHEMA_VERSION:
        raise ActivationBlockedError("ACTIVATION_POINTER_SCHEMA_MISMATCH")
    if pointer.get("enabled") is not True:
        raise ActivationBlockedError("ACTIVATION_DISABLED_OR_REVOKED")
    if pointer.get("mode") != config.MODE:
        raise ActivationBlockedError("ACTIVATION_MODE_MISMATCH")
    if pointer.get("session_date") != session_date.isoformat():
        raise ActivationBlockedError("ACTIVATION_SESSION_DATE_MISMATCH")

    archive_name = str(pointer.get("permit_archive_name", "")).strip()
    if (
        not archive_name
        or Path(archive_name).name != archive_name
        or "/" in archive_name
        or "\\" in archive_name
    ):
        raise ActivationBlockedError("ACTIVATION_ARCHIVE_NAME_INVALID")
    permit_sha256 = str(pointer.get("permit_sha256", "")).strip().lower()
    if not _is_sha256(permit_sha256):
        raise ActivationBlockedError("ACTIVATION_PERMIT_HASH_INVALID")
    archive_path = paths.permit_archive_root / archive_name
    try:
        archive_bytes = archive_path.read_bytes()
    except FileNotFoundError as exc:
        raise ActivationBlockedError("ACTIVATION_PERMIT_ARCHIVE_MISSING") from exc
    except OSError as exc:
        raise ActivationBlockedError("ACTIVATION_PERMIT_ARCHIVE_UNREADABLE") from exc
    if not hmac.compare_digest(hashlib.sha256(archive_bytes).hexdigest(), permit_sha256):
        raise ActivationBlockedError("ACTIVATION_PERMIT_ARCHIVE_HASH_MISMATCH")
    try:
        permit = json.loads(archive_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ActivationBlockedError("ACTIVATION_PERMIT_ARCHIVE_INVALID") from exc
    if not isinstance(permit, dict):
        raise ActivationBlockedError("ACTIVATION_PERMIT_ARCHIVE_INVALID")

    expected_strategy = config.strategy_fingerprint()
    checks = (
        (permit.get("schema_version") == ACTIVATION_PERMIT_SCHEMA_VERSION, "PERMIT_SCHEMA_MISMATCH"),
        (permit.get("enabled") is True, "PERMIT_DISABLED"),
        (permit.get("mode") == config.MODE, "PERMIT_MODE_MISMATCH"),
        (permit.get("paper_only") is True, "PERMIT_NOT_PAPER_ONLY"),
        (permit.get("session_date") == session_date.isoformat(), "PERMIT_SESSION_DATE_MISMATCH"),
        (permit.get("permit_id") == pointer.get("permit_id"), "PERMIT_ID_MISMATCH"),
        (
            permit.get("setup_book_sha256") == config.COMBINED_SETUP_BOOK_SHA256,
            "PERMIT_SETUP_BOOK_MISMATCH",
        ),
        (
            permit.get("strategy_fingerprint") == expected_strategy,
            "PERMIT_STRATEGY_FINGERPRINT_MISMATCH",
        ),
        (
            permit.get("runtime_bundle_sha256") == expected_bundle_sha256,
            "PERMIT_RUNTIME_BUNDLE_MISMATCH",
        ),
        (
            permit.get("approval_phrase_sha256") == APPROVAL_PHRASE_SHA256,
            "PERMIT_APPROVAL_PHRASE_MISMATCH",
        ),
    )
    for passed, reason in checks:
        if not passed:
            raise ActivationBlockedError(reason)
    if not _is_sha256(permit.get("runtime_bundle_sha256")):
        raise ActivationBlockedError("PERMIT_RUNTIME_BUNDLE_INVALID")
    _require_permit_audit_text(permit, "approved_by")
    _require_permit_audit_text(permit, "reason")

    approved_at = _parse_timestamp(permit.get("approved_at_ist"), "approved_at_ist")
    valid_from = _parse_timestamp(permit.get("valid_from_ist"), "valid_from_ist")
    expires_at = _parse_timestamp(permit.get("expires_at_ist"), "expires_at_ist")
    expected_valid_from = _session_boundary(session_date, "00:00")
    expected_expiry = _session_boundary(session_date, config.CONTROL_EXPIRY)
    if valid_from != expected_valid_from or expires_at != expected_expiry:
        raise ActivationBlockedError("PERMIT_VALIDITY_BOUNDARY_MISMATCH")
    if approved_at >= expires_at:
        raise ActivationBlockedError("PERMIT_APPROVED_AFTER_EXPIRY")

    if enforce_current_window:
        observed = _normalize_now(now)
        if observed.date() != session_date:
            raise ActivationBlockedError("CURRENT_SESSION_DATE_MISMATCH")
        if observed < valid_from:
            raise ActivationBlockedError("PERMIT_NOT_YET_VALID")
        if observed >= expires_at:
            raise ActivationBlockedError("PERMIT_EXPIRED")
        if approved_at > observed:
            raise ActivationBlockedError("PERMIT_APPROVED_IN_FUTURE")
    return permit, permit_sha256


def _require_permit_audit_text(permit: Mapping[str, Any], field: str) -> None:
    if not " ".join(str(permit.get(field, "")).split()):
        raise ActivationBlockedError(f"PERMIT_{field.upper()}_MISSING")


def _validate_disengaged_kill_switch(
    session_date: date,
    *,
    paths: ControlPaths,
    permit: Mapping[str, Any],
    permit_sha256: str,
    expected_bundle_sha256: str,
    now: datetime | None,
) -> None:
    state = _read_json_object(
        paths.kill_switch_path,
        "KILL_SWITCH_MISSING_FAIL_CLOSED",
        "KILL_SWITCH_INVALID_FAIL_CLOSED",
    )
    if state.get("schema_version") != KILL_SWITCH_SCHEMA_VERSION:
        raise ActivationBlockedError("KILL_SWITCH_SCHEMA_MISMATCH")
    if state.get("engaged") is not False:
        raise ActivationBlockedError("KILL_SWITCH_ENGAGED")
    checks = (
        (state.get("mode") == config.MODE, "KILL_SWITCH_MODE_MISMATCH"),
        (state.get("session_date") == session_date.isoformat(), "KILL_SWITCH_DATE_MISMATCH"),
        (state.get("permit_id") == permit.get("permit_id"), "KILL_SWITCH_PERMIT_ID_MISMATCH"),
        (state.get("permit_sha256") == permit_sha256, "KILL_SWITCH_PERMIT_HASH_MISMATCH"),
        (
            state.get("setup_book_sha256") == config.COMBINED_SETUP_BOOK_SHA256,
            "KILL_SWITCH_SETUP_BOOK_MISMATCH",
        ),
        (
            state.get("strategy_fingerprint") == config.strategy_fingerprint(),
            "KILL_SWITCH_STRATEGY_MISMATCH",
        ),
        (
            state.get("runtime_bundle_sha256") == expected_bundle_sha256,
            "KILL_SWITCH_RUNTIME_BUNDLE_MISMATCH",
        ),
    )
    for passed, reason in checks:
        if not passed:
            raise ActivationBlockedError(reason)
    updated_at = _parse_timestamp(state.get("updated_at_ist"), "kill_updated_at_ist")
    if now is not None and updated_at > _normalize_now(now):
        raise ActivationBlockedError("KILL_SWITCH_UPDATED_IN_FUTURE")


def evaluate_activation(
    session_date: date | str,
    *,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    expected_runtime_bundle_sha256: str | None = None,
) -> ActivationDecision:
    """Evaluate both keys and return a reason instead of ever failing open."""

    try:
        wanted_day = _normalize_day(session_date)
    except Exception as exc:
        return ActivationDecision(False, f"INVALID_SESSION_DATE:{exc}", str(session_date))
    try:
        bundle_sha256 = (
            str(expected_runtime_bundle_sha256).strip().lower()
            if expected_runtime_bundle_sha256 is not None
            else runtime_bundle_sha256()
        )
        if not _is_sha256(bundle_sha256):
            raise ActivationBlockedError("EXPECTED_RUNTIME_BUNDLE_INVALID")
        permit, permit_sha256 = _load_bound_permit(
            wanted_day,
            paths=paths,
            expected_bundle_sha256=bundle_sha256,
            now=now,
            enforce_current_window=True,
        )
        _validate_disengaged_kill_switch(
            wanted_day,
            paths=paths,
            permit=permit,
            permit_sha256=permit_sha256,
            expected_bundle_sha256=bundle_sha256,
            now=now,
        )
        return ActivationDecision(
            True,
            "PAPER_ACTIVATION_VALID",
            wanted_day.isoformat(),
            permit_id=str(permit["permit_id"]),
            permit_sha256=permit_sha256,
            strategy_fingerprint=str(permit["strategy_fingerprint"]),
            runtime_bundle_sha256=bundle_sha256,
            permit=permit,
        )
    except ActivationBlockedError as exc:
        return ActivationDecision(
            False,
            exc.reason,
            wanted_day.isoformat(),
            strategy_fingerprint=config.strategy_fingerprint(),
        )
    except Exception as exc:  # fail closed on unexpected I/O or hash errors
        return ActivationDecision(
            False,
            f"CONTROL_VALIDATION_ERROR:{type(exc).__name__}",
            wanted_day.isoformat(),
            strategy_fingerprint=config.strategy_fingerprint(),
        )


def require_activation(
    session_date: date | str,
    *,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    expected_runtime_bundle_sha256: str | None = None,
) -> ActivationDecision:
    decision = evaluate_activation(
        session_date,
        now=now,
        paths=paths,
        expected_runtime_bundle_sha256=expected_runtime_bundle_sha256,
    )
    if not decision.allowed:
        raise ActivationBlockedError(decision.reason)
    return decision


def discover_credentials_after_activation(
    discoverer: Callable[[], Sequence[_T]],
    session_date: date | str,
    *,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    expected_runtime_bundle_sha256: str | None = None,
) -> tuple[ActivationDecision, tuple[_T, ...]]:
    """Run the gate first, then require exactly eight discovered Kite apps."""

    decision = require_activation(
        session_date,
        now=now,
        paths=paths,
        expected_runtime_bundle_sha256=expected_runtime_bundle_sha256,
    )
    credentials = tuple(discoverer())
    if len(credentials) != config.REQUIRED_KITE_APPS:
        raise ActivationBlockedError(
            f"KITE_APP_COUNT_MISMATCH:{len(credentials)}/{config.REQUIRED_KITE_APPS}"
        )
    return decision, credentials


def _kill_state_payload(
    *,
    engaged: bool,
    session_date: date,
    permit_id: str,
    permit_sha256: str,
    runtime_bundle_digest: str,
    actor: str,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": KILL_SWITCH_SCHEMA_VERSION,
        "command_id": uuid.uuid4().hex,
        "engaged": bool(engaged),
        "mode": config.MODE,
        "paper_only": True,
        "session_date": session_date.isoformat(),
        "permit_id": permit_id,
        "permit_sha256": permit_sha256,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": runtime_bundle_digest,
        "actor": actor,
        "reason": reason,
        "updated_at_ist": now.isoformat(timespec="microseconds"),
    }


def approve_session(
    session_date: date | str,
    *,
    approved_by: str,
    reason: str,
    approval_phrase: str,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    runtime_bundle_digest: str | None = None,
) -> dict[str, Any]:
    """Create one immutable permit; leave the kill switch engaged."""

    if not hmac.compare_digest(str(approval_phrase), APPROVAL_PHRASE):
        raise ControlCommandError("exact PAPER approval phrase is required")
    day = _normalize_day(session_date)
    observed = _normalize_now(now)
    actor = _require_text(approved_by, "approved_by")
    approval_reason = _require_text(reason, "reason")
    expiry = _session_boundary(day, config.CONTROL_EXPIRY)
    if day < observed.date() or observed >= expiry:
        raise ControlCommandError("cannot approve an expired/past PAPER session")
    bundle_sha256 = (
        str(runtime_bundle_digest).strip().lower()
        if runtime_bundle_digest is not None
        else runtime_bundle_sha256()
    )
    if not _is_sha256(bundle_sha256):
        raise ControlCommandError("runtime_bundle_digest must be a SHA-256")

    permit_id = uuid.uuid4().hex
    permit = {
        "schema_version": ACTIVATION_PERMIT_SCHEMA_VERSION,
        "permit_id": permit_id,
        "enabled": True,
        "mode": config.MODE,
        "paper_only": True,
        "session_date": day.isoformat(),
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": bundle_sha256,
        "approval_phrase_sha256": APPROVAL_PHRASE_SHA256,
        "approved_by": actor,
        "reason": approval_reason,
        "approved_at_ist": observed.isoformat(timespec="microseconds"),
        "valid_from_ist": _session_boundary(day, "00:00").isoformat(),
        "expires_at_ist": expiry.isoformat(),
    }
    archive_content = _json_file_bytes(permit)
    permit_sha256 = hashlib.sha256(archive_content).hexdigest()
    archive_name = f"{day.isoformat()}_{permit_id}_{permit_sha256}.json"
    archive_path = paths.permit_archive_root / archive_name
    stored_content = _write_immutable_json(archive_path, permit)
    if hashlib.sha256(stored_content).hexdigest() != permit_sha256:
        raise ControlCommandError("stored permit hash does not match approval")

    pointer = {
        "schema_version": ACTIVATION_POINTER_SCHEMA_VERSION,
        "enabled": True,
        "mode": config.MODE,
        "paper_only": True,
        "session_date": day.isoformat(),
        "permit_id": permit_id,
        "permit_sha256": permit_sha256,
        "permit_archive_name": archive_name,
        "updated_at_ist": observed.isoformat(timespec="microseconds"),
    }
    _atomic_write_json(paths.activation_path, pointer)

    # Approval is not arming.  Binding a fresh engaged switch to the new permit
    # also prevents an old session's disengaged state from being reused.
    kill_state = _kill_state_payload(
        engaged=True,
        session_date=day,
        permit_id=permit_id,
        permit_sha256=permit_sha256,
        runtime_bundle_digest=bundle_sha256,
        actor=actor,
        reason="NEW_APPROVAL_REQUIRES_SEPARATE_DISARM",
        now=observed,
    )
    _atomic_write_json(paths.kill_switch_path, kill_state)
    event_path = _archive_control_event(
        paths,
        action="APPROVE",
        actor=actor,
        session_date=day,
        now=observed,
        details={
            "permit_id": permit_id,
            "permit_sha256": permit_sha256,
            "permit_archive_name": archive_name,
            "kill_switch_engaged": True,
            "does_not_start_or_enable_tasks": True,
        },
    )
    return {
        "permit": permit,
        "permit_sha256": permit_sha256,
        "permit_archive_path": archive_path,
        "activation_pointer": pointer,
        "kill_switch": kill_state,
        "control_event_path": event_path,
    }


def disarm_kill_switch(
    session_date: date | str,
    *,
    permit_id: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    runtime_bundle_digest: str | None = None,
) -> dict[str, Any]:
    """Disengage only for the exact currently archived permit."""

    day = _normalize_day(session_date)
    observed = _normalize_now(now)
    command_actor = _require_text(actor, "actor")
    command_reason = _require_text(reason, "reason")
    bundle_sha256 = (
        str(runtime_bundle_digest).strip().lower()
        if runtime_bundle_digest is not None
        else runtime_bundle_sha256()
    )
    if not _is_sha256(bundle_sha256):
        raise ControlCommandError("runtime_bundle_digest must be a SHA-256")
    permit, permit_sha256 = _load_bound_permit(
        day,
        paths=paths,
        expected_bundle_sha256=bundle_sha256,
        now=None,
        enforce_current_window=False,
    )
    if not hmac.compare_digest(str(permit_id), str(permit["permit_id"])):
        raise ControlCommandError("permit_id does not match the active archived permit")
    state = _kill_state_payload(
        engaged=False,
        session_date=day,
        permit_id=str(permit["permit_id"]),
        permit_sha256=permit_sha256,
        runtime_bundle_digest=bundle_sha256,
        actor=command_actor,
        reason=command_reason,
        now=observed,
    )
    event_path = _archive_control_event(
        paths,
        action="DISARM",
        actor=command_actor,
        session_date=day,
        now=observed,
        details={
            "permit_id": permit["permit_id"],
            "permit_sha256": permit_sha256,
            "kill_switch_engaged": False,
            "does_not_start_or_enable_tasks": True,
        },
    )
    # The immutable audit event must exist before the only state-changing
    # operation that can permit credential discovery.  If event archival fails,
    # the previously engaged switch remains fail-closed.
    _atomic_write_json(paths.kill_switch_path, state)
    return {"kill_switch": state, "control_event_path": event_path}


def engage_kill_switch(
    session_date: date | str | None = None,
    *,
    actor: str,
    reason: str,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    runtime_bundle_digest: str | None = None,
) -> dict[str, Any]:
    """Always make the control state safer, even if activation is corrupted."""

    observed = _normalize_now(now)
    command_actor = _require_text(actor, "actor")
    command_reason = _require_text(reason, "reason")
    pointer: dict[str, Any] = {}
    try:
        pointer = _read_json_object(
            paths.activation_path,
            "ACTIVATION_POINTER_MISSING",
            "ACTIVATION_POINTER_INVALID",
        )
    except ActivationBlockedError:
        pointer = {}
    inferred_day = session_date or pointer.get("session_date") or observed.date()
    day = _normalize_day(inferred_day)
    if runtime_bundle_digest is not None:
        bundle_sha256 = str(runtime_bundle_digest).strip().lower()
        if not _is_sha256(bundle_sha256):
            raise ControlCommandError("runtime_bundle_digest must be a SHA-256")
    else:
        try:
            bundle_sha256 = runtime_bundle_sha256()
        except Exception:
            # An emergency kill must still be writable if a runtime source is
            # missing, locked, or mid-deployment.  Since engaged=True blocks
            # before any hash can authorize credentials, an all-zero sentinel
            # is strictly fail-closed and is surfaced in the audit payload.
            bundle_sha256 = "0" * 64
    state = _kill_state_payload(
        engaged=True,
        session_date=day,
        permit_id=str(pointer.get("permit_id", "")),
        permit_sha256=str(pointer.get("permit_sha256", "")),
        runtime_bundle_digest=bundle_sha256,
        actor=command_actor,
        reason=command_reason,
        now=observed,
    )
    _atomic_write_json(paths.kill_switch_path, state)
    event_path = _archive_control_event(
        paths,
        action="KILL",
        actor=command_actor,
        session_date=day,
        now=observed,
        details={
            "permit_id": state["permit_id"],
            "permit_sha256": state["permit_sha256"],
            "kill_switch_engaged": True,
        },
    )
    return {"kill_switch": state, "control_event_path": event_path}


def revoke_session(
    session_date: date | str | None = None,
    *,
    actor: str,
    reason: str,
    now: datetime | None = None,
    paths: ControlPaths = DEFAULT_CONTROL_PATHS,
    runtime_bundle_digest: str | None = None,
) -> dict[str, Any]:
    """Engage kill first, then replace only the mutable pointer with revoked."""

    observed = _normalize_now(now)
    command_actor = _require_text(actor, "actor")
    command_reason = _require_text(reason, "reason")
    killed = engage_kill_switch(
        session_date,
        actor=command_actor,
        reason=f"REVOKE:{command_reason}",
        now=observed,
        paths=paths,
        runtime_bundle_digest=runtime_bundle_digest,
    )
    state = dict(killed["kill_switch"])
    day = _normalize_day(state["session_date"])
    pointer = {
        "schema_version": ACTIVATION_POINTER_SCHEMA_VERSION,
        "enabled": False,
        "mode": config.MODE,
        "paper_only": True,
        "session_date": day.isoformat(),
        "permit_id": state.get("permit_id", ""),
        "permit_sha256": state.get("permit_sha256", ""),
        "revoked_by": command_actor,
        "reason": command_reason,
        "revoked_at_ist": observed.isoformat(timespec="microseconds"),
        "updated_at_ist": observed.isoformat(timespec="microseconds"),
    }
    _atomic_write_json(paths.activation_path, pointer)
    event_path = _archive_control_event(
        paths,
        action="REVOKE",
        actor=command_actor,
        session_date=day,
        now=observed,
        details={
            "permit_id": pointer["permit_id"],
            "permit_sha256": pointer["permit_sha256"],
            "kill_switch_engaged": True,
            "activation_enabled": False,
        },
    )
    return {
        "activation_pointer": pointer,
        "kill_switch": state,
        "control_event_path": event_path,
    }


# Short aliases form the public control API without shadowing builtins.
approve = approve_session
revoke = revoke_session
kill = engage_kill_switch
disarm = disarm_kill_switch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Control one PAPER-only FNO V8-Combined session."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    approve_parser = subparsers.add_parser("approve")
    approve_parser.add_argument("--session-date", required=True)
    approve_parser.add_argument("--approved-by", required=True)
    approve_parser.add_argument("--reason", required=True)
    approve_parser.add_argument("--phrase", required=True)

    for command in ("revoke", "kill"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--session-date")
        command_parser.add_argument("--actor", required=True)
        command_parser.add_argument("--reason", required=True)

    disarm_parser = subparsers.add_parser("disarm")
    disarm_parser.add_argument("--session-date", required=True)
    disarm_parser.add_argument("--permit-id", required=True)
    disarm_parser.add_argument("--actor", required=True)
    disarm_parser.add_argument("--reason", required=True)
    return parser


def _print_result(payload: Mapping[str, Any]) -> None:
    printable = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in payload.items()
    }
    print(json.dumps(printable, indent=2, sort_keys=True, default=str))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "approve":
            result = approve_session(
                args.session_date,
                approved_by=args.approved_by,
                reason=args.reason,
                approval_phrase=args.phrase,
            )
        elif args.command == "disarm":
            result = disarm_kill_switch(
                args.session_date,
                permit_id=args.permit_id,
                actor=args.actor,
                reason=args.reason,
            )
        elif args.command == "kill":
            result = engage_kill_switch(
                args.session_date,
                actor=args.actor,
                reason=args.reason,
            )
        else:
            result = revoke_session(
                args.session_date,
                actor=args.actor,
                reason=args.reason,
            )
    except (ControlCommandError, ActivationBlockedError) as exc:
        print(f"[BLOCKED] {exc}", file=sys.stderr)
        return 2
    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
