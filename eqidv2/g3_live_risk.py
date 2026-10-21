"""Fail-closed, durable portfolio admission for G3 equity LIVE entries.

The caller must hold the global LIVE entry-owner lock while calling
``admit_live_entry`` and must persist an admitted state before contacting the
broker.  This module performs no broker calls and grants no execution authority.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Iterable

ADMISSION_SCHEMA_VERSION = "g3_live_risk_admission_v1"
ACTIVE_STATES = frozenset({"PENDING_ENTRY", "OPEN", "SQUARE_OFF_PENDING"})


class LiveRiskError(RuntimeError):
    """Base class for G3 LIVE admission failures."""


class LiveRiskLimitError(LiveRiskError):
    """Expected terminal rejection because a configured portfolio cap was hit."""


class LiveRiskIntegrityError(LiveRiskError):
    """Untrusted or ambiguous risk-book evidence; never overwrite it."""


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise LiveRiskIntegrityError(f"{label} is not a non-negative integer")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveRiskIntegrityError(f"{label} is not a non-negative integer") from exc
    if not number.is_finite() or number != number.to_integral_value() or number < 0:
        raise LiveRiskIntegrityError(f"{label} is not a non-negative integer")
    return int(number)


def _positive_decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool):
        raise LiveRiskIntegrityError(f"{label} is not finite and positive")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveRiskIntegrityError(f"{label} is not finite and positive") from exc
    if not number.is_finite() or number <= 0:
        raise LiveRiskIntegrityError(f"{label} is not finite and positive")
    return number


def _strict_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LiveRiskIntegrityError(f"Unreadable G3 LIVE risk state: {path}") from exc
    if not isinstance(value, dict):
        raise LiveRiskIntegrityError(f"G3 LIVE risk state is not an object: {path}")
    return value


def _current_day_paths(book_root: Path, session_date: str) -> list[Path]:
    if not book_root.exists():
        return []
    try:
        paths = [
            path
            for path in book_root.rglob("*.json")
            if path.parent.name == session_date
        ]
    except OSError as exc:
        raise LiveRiskIntegrityError(f"Unable to enumerate G3 LIVE risk book: {book_root}") from exc
    return sorted(paths, key=lambda item: str(item).casefold())


def _parse_admitted_at(value: Any, session_date: str, path: Path) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LiveRiskIntegrityError(f"Missing G3 LIVE admission timestamp: {path}")
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError as exc:
        raise LiveRiskIntegrityError(f"Invalid G3 LIVE admission timestamp: {path}") from exc
    if stamp.tzinfo is None or stamp.date().isoformat() != session_date:
        raise LiveRiskIntegrityError(f"G3 LIVE admission timestamp is outside its session: {path}")
    return stamp


def _has_broker_evidence(row: dict[str, Any]) -> bool:
    attempts = _integer(row.get("entry_submission_attempt_count", 0), "entry submission attempts")
    uncertain = row.get("entry_submission_uncertain", False)
    if not isinstance(uncertain, bool):
        raise LiveRiskIntegrityError("entry_submission_uncertain is not boolean")
    entry_price = row.get("entry_price", 0)
    if isinstance(entry_price, bool):
        raise LiveRiskIntegrityError("entry_price is malformed")
    try:
        entry_price_number = Decimal(str(entry_price or 0))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiveRiskIntegrityError("entry_price is malformed") from exc
    if not entry_price_number.is_finite() or entry_price_number < 0:
        raise LiveRiskIntegrityError("entry_price is malformed")
    return bool(
        attempts
        or uncertain
        or str(row.get("entry_order_id", "")).strip()
        or str(row.get("entry_at_ist", "")).strip()
        or entry_price_number > 0
        or str(row.get("status", "")).upper()
        in {"OPEN", "SQUARE_OFF_PENDING", "CLOSED", "ENTRY_REJECTED"}
    )


def _validate_row(
    row: dict[str, Any],
    path: Path,
    *,
    session_date: str,
    strategy_version: str,
    strategy_fingerprint: str,
    policy: Any,
    terminal_states: frozenset[str],
) -> tuple[str, bool, Decimal | None]:
    signal_id = str(row.get("signal_id", "")).strip()
    if not signal_id or path.stem != signal_id:
        raise LiveRiskIntegrityError(f"G3 LIVE filename/signal identity mismatch: {path}")
    if str(row.get("mode", "")).upper() != "LIVE" or row.get("session_date") != session_date:
        raise LiveRiskIntegrityError(f"G3 LIVE mode/session identity mismatch: {path}")
    if (row.get("strategy_version") != strategy_version
            or row.get("strategy_fingerprint") != strategy_fingerprint):
        raise LiveRiskIntegrityError(f"G3 LIVE strategy identity mismatch: {path}")
    try:
        recognized_policy_fingerprint = policy.validate_existing_order_sizing(row)
    except (TypeError, ValueError) as exc:
        raise LiveRiskIntegrityError(
            f"G3 LIVE execution policy/sizing mismatch: {path}"
        ) from exc

    status = str(row.get("status", "")).strip().upper()
    known_states = terminal_states | ACTIVE_STATES
    if status not in known_states:
        raise LiveRiskIntegrityError(f"Unknown G3 LIVE order status {status!r}: {path}")
    uncertain = row.get("entry_submission_uncertain", False)
    if not isinstance(uncertain, bool):
        raise LiveRiskIntegrityError(f"Malformed uncertain-submission flag: {path}")

    admitted_value = row.get("live_risk_admitted_at_ist")
    admitted = admitted_value not in (None, "")
    if not admitted:
        if status in ACTIVE_STATES and status != "PENDING_ENTRY":
            raise LiveRiskIntegrityError(f"Active G3 LIVE state has no risk admission: {path}")
        if _has_broker_evidence(row):
            raise LiveRiskIntegrityError(f"G3 LIVE broker evidence has no risk admission: {path}")
        return signal_id, False, None

    if row.get("live_risk_admission_schema_version") != ADMISSION_SCHEMA_VERSION:
        raise LiveRiskIntegrityError(f"G3 LIVE admission schema mismatch: {path}")
    if row.get("live_risk_decision") != "ADMITTED":
        raise LiveRiskIntegrityError(f"G3 LIVE admitted state has an invalid decision: {path}")
    if row.get("live_risk_policy_fingerprint") != recognized_policy_fingerprint:
        raise LiveRiskIntegrityError(f"G3 LIVE admission policy fingerprint mismatch: {path}")
    _parse_admitted_at(admitted_value, session_date, path)
    reserved = _positive_decimal(
        row.get("live_risk_reserved_exposure_rs"), "reserved gross exposure"
    )
    expected_reserved = Decimal(
        str(
            float(
                Decimal(_integer(row.get("requested_quantity"), "requested quantity"))
                * _positive_decimal(
                    row.get("sizing_reference_price"), "sizing reference price"
                )
            )
        )
    )
    if reserved != expected_reserved:
        raise LiveRiskIntegrityError(
            f"G3 LIVE reserved exposure does not match admitted sizing: {path}"
        )
    if reserved > Decimal(str(policy.TARGET_EXPOSURE_RS)):
        raise LiveRiskIntegrityError(f"G3 LIVE reserved exposure exceeds per-trade policy: {path}")
    return signal_id, True, reserved


def admit_live_entry(
    state: dict[str, Any],
    *,
    book_root: Path | str,
    state_path: Path | str,
    now: datetime,
    policy: Any,
    terminal_states: Iterable[str],
) -> dict[str, Any]:
    """Mutate ``state`` to ADMITTED or BLOCKED after a strict shared-book scan.

    The returned state is not written here.  The caller owns both the global
    entry lock and the required atomic write-before-broker ordering.
    """

    if now.tzinfo is None:
        raise LiveRiskIntegrityError("G3 LIVE admission clock must be timezone-aware")
    if str(state.get("mode", "")).upper() != "LIVE" or state.get("status") != "PENDING_ENTRY":
        raise LiveRiskIntegrityError("Only a pending G3 LIVE entry can be admitted")
    session_date = str(state.get("session_date", ""))
    if not session_date or now.date().isoformat() != session_date:
        raise LiveRiskIntegrityError("G3 LIVE admission date does not match the active session")
    try:
        policy.validate_order_sizing(state, require_pending_quantity=True)
    except (TypeError, ValueError) as exc:
        raise LiveRiskIntegrityError("G3 LIVE candidate sizing is not trusted") from exc

    terminal = frozenset(str(value).upper() for value in terminal_states)
    root = Path(book_root)
    candidate_path = Path(state_path)
    try:
        candidate_resolved = candidate_path.resolve()
        candidate_resolved.relative_to(root.resolve())
    except OSError as exc:
        raise LiveRiskIntegrityError("Unable to resolve the G3 LIVE candidate path") from exc
    except ValueError as exc:
        raise LiveRiskIntegrityError("G3 LIVE candidate path is outside its shared risk book") from exc
    candidate_id = str(state.get("signal_id", "")).strip()
    if not candidate_id or candidate_path.stem != candidate_id:
        raise LiveRiskIntegrityError("G3 LIVE candidate filename/signal identity mismatch")

    rows: list[tuple[Path, dict[str, Any], bool, Decimal | None]] = []
    seen_ids: dict[str, Path] = {}
    persisted_candidate: dict[str, Any] | None = None
    unadmitted_active_paths: list[Path] = []
    for path in _current_day_paths(root, session_date):
        row = _strict_json(path)
        signal_id, admitted, reserved = _validate_row(
            row,
            path,
            session_date=session_date,
            strategy_version=str(state.get("strategy_version", "")),
            strategy_fingerprint=str(state.get("strategy_fingerprint", "")),
            policy=policy,
            terminal_states=terminal,
        )
        if signal_id in seen_ids:
            raise LiveRiskIntegrityError(
                f"Duplicate G3 LIVE signal identity: {seen_ids[signal_id]} and {path}"
            )
        seen_ids[signal_id] = path
        try:
            is_candidate = path.resolve() == candidate_resolved
        except OSError as exc:
            raise LiveRiskIntegrityError(f"Unable to resolve G3 LIVE risk state: {path}") from exc
        if is_candidate:
            persisted_candidate = row
        elif not admitted and str(row.get("status", "")).upper() in ACTIVE_STATES:
            unadmitted_active_paths.append(path)
        rows.append((path, row, admitted, reserved))

    if persisted_candidate is not None and str(
        persisted_candidate.get("signal_id", "")
    ) != candidate_id:
        raise LiveRiskIntegrityError(
            "G3 LIVE candidate path resolves to a different signal identity"
        )
    if candidate_id in seen_ids and persisted_candidate is None:
        raise LiveRiskIntegrityError(
            "Duplicate G3 LIVE signal identity: "
            f"{seen_ids[candidate_id]} and {candidate_path}"
        )

    if unadmitted_active_paths:
        raise LiveRiskIntegrityError(
            "Another active G3 LIVE state has no risk admission: "
            + ", ".join(str(path) for path in unadmitted_active_paths)
        )

    state_admitted = state.get("live_risk_admitted_at_ist") not in (None, "")
    persisted_admitted = bool(
        persisted_candidate
        and persisted_candidate.get("live_risk_admitted_at_ist") not in (None, "")
    )
    if state_admitted != persisted_admitted:
        raise LiveRiskIntegrityError("G3 LIVE in-memory and durable admission evidence disagree")

    daily_entries = 0
    active_entries = 0
    reserved_gross = Decimal("0")
    for _path, row, admitted, reserved in rows:
        if not admitted:
            continue
        daily_entries += 1
        status = str(row.get("status", "")).strip().upper()
        remains_reserved = status not in terminal or row.get("entry_submission_uncertain") is True
        if remains_reserved:
            active_entries += 1
            reserved_gross += reserved or Decimal("0")

    if state_admitted:
        immutable = (
            "live_risk_admission_schema_version",
            "live_risk_policy_fingerprint",
            "live_risk_admitted_at_ist",
            "live_risk_reserved_exposure_rs",
            "live_risk_decision",
        )
        if any(state.get(key) != persisted_candidate.get(key) for key in immutable):
            raise LiveRiskIntegrityError("G3 LIVE durable admission fields changed in memory")
        return state

    quantity = _integer(state.get("quantity"), "candidate quantity")
    if quantity < 1:
        raise LiveRiskIntegrityError("G3 LIVE candidate quantity must be positive")
    reference_price = _positive_decimal(
        state.get("sizing_reference_price"), "candidate sizing reference price"
    )
    candidate_exposure = Decimal(quantity) * reference_price
    if candidate_exposure > Decimal(str(policy.TARGET_EXPOSURE_RS)):
        raise LiveRiskIntegrityError("G3 LIVE candidate exceeds its per-trade exposure")

    violations: list[str] = []
    if (policy.MAX_DAILY_ENTRIES is not None
            and daily_entries + 1 > int(policy.MAX_DAILY_ENTRIES)):
        violations.append("MAX_DAILY_ENTRIES")
    if (policy.MAX_RESERVED_OPEN_POSITIONS is not None
            and active_entries + 1 > int(policy.MAX_RESERVED_OPEN_POSITIONS)):
        violations.append("MAX_RESERVED_OPEN_POSITIONS")
    projected_gross = reserved_gross + candidate_exposure
    if (policy.MAX_RESERVED_GROSS_EXPOSURE_RS is not None
            and projected_gross > Decimal(str(policy.MAX_RESERVED_GROSS_EXPOSURE_RS))):
        violations.append("MAX_RESERVED_GROSS_EXPOSURE")

    observed = {
        "live_risk_observed_daily_entries": daily_entries,
        "live_risk_observed_reserved_open_positions": active_entries,
        "live_risk_observed_reserved_gross_exposure_rs": float(reserved_gross),
        "live_risk_candidate_exposure_rs": float(candidate_exposure),
    }
    stamp = now.isoformat(timespec="seconds")
    if violations:
        reason = "LIVE_RISK_LIMIT_BLOCKED:" + ",".join(violations)
        state.update(
            **observed,
            status="BLOCKED_PORTFOLIO",
            status_reason=reason,
            entry_terminal_cause="LIVE_RISK_LIMIT",
            live_risk_decision="BLOCKED",
            live_risk_blocked_at_ist=stamp,
            live_risk_block_reason=reason,
            updated_at_ist=stamp,
        )
        raise LiveRiskLimitError(reason)

    state.update(
        **observed,
        live_risk_admission_schema_version=ADMISSION_SCHEMA_VERSION,
        live_risk_policy_fingerprint=policy.policy_fingerprint(),
        live_risk_book_scope="ALL_G3_LIVE_EXECUTION_NAMESPACES",
        live_risk_admitted_at_ist=stamp,
        live_risk_reserved_exposure_rs=float(candidate_exposure),
        live_risk_decision="ADMITTED",
        live_risk_projected_daily_entries=daily_entries + 1,
        live_risk_projected_reserved_open_positions=active_entries + 1,
        live_risk_projected_reserved_gross_exposure_rs=float(projected_gross),
    )
    return state
