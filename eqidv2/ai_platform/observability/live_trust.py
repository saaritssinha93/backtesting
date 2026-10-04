"""Fail-closed, read-only trust checks for the V13-V10-G LIVE session.

This module never talks to the broker, starts a process, or changes a safety
control.  It verifies the coordinator's current-session status and the
digest-verified broker/local position and active-order reconciliation that
belongs to the same run.  Missing,
stale, malformed, incomplete, or mismatched evidence is a failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


IST = timezone(timedelta(hours=5, minutes=30))
REPORT_SCHEMA_VERSION = "v13_v10_g_live_trust_v2"
STATUS_SCHEMA_VERSION = "fno_v6_live_kite_qty1_status_v1"
RECONCILIATION_SCHEMA_VERSION = "v13_v10_g_broker_position_reconciliation_v2"
EXPECTED_EXECUTION_PROFILE = "live_kite_qty1"
EXPECTED_QUANTITY_POLICY = "FIXED_ONE_SHARE"
EXPECTED_RECONCILIATION_SCOPE = "nse_mis_strategy_tagged_symbols_and_active_orders"
EXPECTED_SCANNER_SLOTS = (
    "09:25",
    "09:30",
    "09:35",
    "09:40",
    "09:45",
    "09:50",
    "09:55",
    "10:00",
    "11:20",
)
SCANNER_SLOT_COMPLETION_GRACE_SECONDS = 180.0
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
MAX_MATCHING_MARKERS = 512
MAX_FUTURE_SKEW_SECONDS = 5.0
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class TrustCheck:
    name: str
    state: str
    detail: str


@dataclass(frozen=True)
class LiveTrustReport:
    observed_at_ist: str
    session_date: str
    state: str
    status_path: str
    reconciliation_path: str
    run_id: str | None
    checks: tuple[TrustCheck, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            **asdict(self),
            "checks": [asdict(check) for check in self.checks],
            "summary": {
                "passed": sum(check.state == "PASS" for check in self.checks),
                "failed": sum(check.state == "FAIL" for check in self.checks),
            },
        }


def _read_json_object(path: Path) -> tuple[dict[str, Any] | None, str]:
    try:
        stat = path.stat()
        if not path.is_file():
            return None, "path is not a regular file"
        if stat.st_size > MAX_ARTIFACT_BYTES:
            return None, f"artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "artifact is missing"
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, f"artifact is unreadable or invalid JSON: {type(exc).__name__}"
    if not isinstance(value, dict):
        return None, "artifact must contain one JSON object"
    return value, "valid JSON object"


def _read_key_value_status(path: Path) -> tuple[dict[str, str] | None, str]:
    try:
        stat = path.stat()
        if not path.is_file():
            return None, "path is not a regular file"
        if stat.st_size > MAX_ARTIFACT_BYTES:
            return None, f"artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None, "artifact is missing"
    except (OSError, UnicodeError) as exc:
        return None, f"artifact is unreadable: {type(exc).__name__}"
    payload: dict[str, str] = {}
    for line in lines:
        if not line.strip():
            continue
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not key or key in payload:
            return None, "artifact has a malformed or duplicate field"
        payload[key] = value.strip()
    if not payload:
        return None, "artifact has no fields"
    return payload, "valid key/value status"


def _latest_session_marker(
    directory: Path, *, prefix: str, session_date: date
) -> tuple[Path | None, str]:
    day_token = session_date.strftime("%Y%m%d")
    pattern = re.compile(
        rf"^{re.escape(prefix)}{day_token}_(?P<hh>[0-2][0-9])(?P<mm>[0-5][0-9])\.json$"
    )
    try:
        candidates: list[Path] = []
        for path in directory.glob(f"{prefix}{day_token}_*.json"):
            if path.is_file() and pattern.fullmatch(path.name):
                candidates.append(path)
                if len(candidates) > MAX_MATCHING_MARKERS:
                    return None, (
                        f"session has more than {MAX_MATCHING_MARKERS} matching markers"
                    )
    except FileNotFoundError:
        return None, "marker directory is missing"
    except OSError as exc:
        return None, f"marker directory is unreadable: {type(exc).__name__}"
    if not candidates:
        return None, f"no marker exists for {session_date.isoformat()}"
    latest = max(candidates, key=lambda candidate: candidate.name)
    return latest, f"latest marker={latest.name}"


def _parse_aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(IST)


def _canonical_sha256(payload: Mapping[str, Any]) -> str | None:
    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        return None
    return hashlib.sha256(encoded).hexdigest()


def _freshness_detail(
    timestamp: datetime | None,
    *,
    now: datetime,
    maximum_age_seconds: float,
) -> tuple[bool, str]:
    if timestamp is None:
        return False, "timestamp is missing, invalid, or timezone-naive"
    age = (now - timestamp).total_seconds()
    if age < -MAX_FUTURE_SKEW_SECONDS:
        return False, f"timestamp is {-age:.1f}s in the future"
    if age > maximum_age_seconds:
        return False, f"evidence is stale: age={age:.1f}s limit={maximum_age_seconds:.1f}s"
    return True, f"fresh: age={max(0.0, age):.1f}s limit={maximum_age_seconds:.1f}s"


def _scanner_schedule_detail(
    heartbeat: Mapping[str, str], *, now: datetime, session_date: date
) -> tuple[bool, str] | None:
    """Evaluate the slot-driven scanner without inventing continuous heartbeats.

    The scanner deliberately sleeps between the frozen strategy's signal slots,
    so its last status may be older than a generic heartbeat threshold.  A
    completed latest-due slot is current until the next configured slot.  Once a
    new slot is due, a short bounded completion grace applies.
    """

    heartbeat_at = _parse_aware_datetime(heartbeat.get("ts"))
    if heartbeat_at is None or heartbeat_at.date() != session_date:
        return False, "scanner heartbeat is not timestamped for the requested session"
    if (heartbeat_at - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        return False, "scanner heartbeat timestamp is in the future"

    slot_datetimes = [
        datetime.combine(session_date, day_time.fromisoformat(value), tzinfo=IST)
        for value in EXPECTED_SCANNER_SLOTS
    ]
    due = [value for value in slot_datetimes if value <= now]
    if not due:
        return None
    latest_due = due[-1]
    latest_label = latest_due.strftime("%H:%M")
    completed_label = str(heartbeat.get("slot", ""))
    state = str(heartbeat.get("state", "")).upper()
    completed_state = state in {"SUCCESS", "DONE"}
    completed_phase = str(heartbeat.get("phase", ""))
    if (
        latest_label == EXPECTED_SCANNER_SLOTS[-1]
        and state == "DONE"
        and completed_phase == "ALL_V6_WINDOWS_DONE"
    ):
        return True, "all frozen scanner slots are complete"
    if (
        completed_label == latest_label
        and completed_state
        and completed_phase in {"SLOT_DONE", "ALL_V6_WINDOWS_DONE"}
    ):
        return True, f"latest due scanner slot {latest_label} is complete"
    grace_deadline = latest_due + timedelta(seconds=SCANNER_SLOT_COMPLETION_GRACE_SECONDS)
    if now <= grace_deadline:
        return True, (
            f"scanner slot {latest_label} is inside its "
            f"{SCANNER_SLOT_COMPLETION_GRACE_SECONDS:.0f}s completion grace"
        )
    return False, (
        f"latest due scanner slot {latest_label} is not complete; "
        f"observed slot={completed_label!r} state={heartbeat.get('state')!r} "
        f"phase={completed_phase!r}"
    )


def _confirmation_completion_detail(
    heartbeat: Mapping[str, str], *, now: datetime, session_date: date
) -> tuple[bool, str] | None:
    """Accept a stopped confirmation worker only with complete terminal proof."""

    if str(heartbeat.get("state", "")).upper() != "DONE":
        return None
    heartbeat_at = _parse_aware_datetime(heartbeat.get("ts"))
    if heartbeat_at is None or heartbeat_at.date() != session_date:
        return False, "confirmation completion is not timestamped for the requested session"
    if (heartbeat_at - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        return False, "confirmation completion timestamp is in the future"
    try:
        processed_slots = int(str(heartbeat.get("processed_slots", "")))
    except ValueError:
        processed_slots = -1
    complete = (
        heartbeat.get("phase") == "ALL_V6_WINDOWS_DONE"
        and processed_slots == len(EXPECTED_SCANNER_SLOTS)
    )
    if complete:
        return True, "all frozen confirmation slots are complete"
    return False, (
        "confirmation DONE lacks terminal proof; "
        f"phase={heartbeat.get('phase')!r} processed_slots={processed_slots!r} "
        f"expected={len(EXPECTED_SCANNER_SLOTS)}"
    )


def _equity_feed_completion_detail(
    heartbeat: Mapping[str, str], *, now: datetime, session_date: date
) -> tuple[bool, str] | None:
    """Accept the bounded equity feed after every frozen slot is processed."""

    if str(heartbeat.get("state", "")).upper() != "DONE":
        return None
    heartbeat_at = _parse_aware_datetime(heartbeat.get("ts"))
    if heartbeat_at is None or heartbeat_at.date() != session_date:
        return False, "equity-feed completion is not timestamped for the requested session"
    if (heartbeat_at - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        return False, "equity-feed completion timestamp is in the future"
    try:
        processed_slots = int(str(heartbeat.get("processed_slots", "")))
    except ValueError:
        processed_slots = -1
    if processed_slots == len(EXPECTED_SCANNER_SLOTS):
        return True, "all frozen equity-feed slots are complete"
    return False, (
        "equity feed DONE lacks terminal proof; "
        f"processed_slots={processed_slots!r} expected={len(EXPECTED_SCANNER_SLOTS)}"
    )


def evaluate_live_trust(
    *,
    runtime_root: Path,
    status_path: Path,
    reconciliation_path: Path,
    expected_session_date: date,
    expected_strategy_version: str,
    expected_strategy_fingerprint: str,
    observed_at: datetime | None = None,
    status_max_age_seconds: float = 30.0,
    reconciliation_max_age_seconds: float = 120.0,
    pipeline_max_age_seconds: float = 120.0,
    market_data_max_age_seconds: float = 420.0,
) -> LiveTrustReport:
    """Evaluate current LIVE evidence without changing any runtime state."""

    if any(
        value <= 0
        for value in (
            status_max_age_seconds,
            reconciliation_max_age_seconds,
            pipeline_max_age_seconds,
            market_data_max_age_seconds,
        )
    ):
        raise ValueError("freshness limits must be positive")
    now = (observed_at or datetime.now(IST)).astimezone(IST)
    day_text = expected_session_date.isoformat()
    checks: list[TrustCheck] = []

    def record(name: str, passed: bool, detail: str) -> None:
        checks.append(TrustCheck(name, "PASS" if passed else "FAIL", detail))

    status, status_detail = _read_json_object(status_path)
    record("coordinator_artifact", status is not None, status_detail)
    status = status or {}

    record(
        "coordinator_contract",
        status.get("schema_version") == STATUS_SCHEMA_VERSION,
        f"schema={status.get('schema_version')!r}",
    )
    record(
        "coordinator_session",
        status.get("session_date") == day_text,
        f"expected={day_text} observed={status.get('session_date')!r}",
    )
    strategy_matches = (
        status.get("strategy_version") == expected_strategy_version
        and status.get("strategy_fingerprint") == expected_strategy_fingerprint
    )
    record(
        "strategy_identity",
        strategy_matches,
        "version and frozen fingerprint match" if strategy_matches else "version or frozen fingerprint differs",
    )
    profile_matches = (
        status.get("execution_mode") == "LIVE"
        and status.get("execution_profile") == EXPECTED_EXECUTION_PROFILE
        and status.get("quantity") == 1
        and status.get("quantity_policy") == EXPECTED_QUANTITY_POLICY
    )
    record(
        "live_quantity_one_profile",
        profile_matches,
        (
            f"mode={status.get('execution_mode')!r} "
            f"profile={status.get('execution_profile')!r} "
            f"quantity={status.get('quantity')!r} policy={status.get('quantity_policy')!r}"
        ),
    )
    record(
        "coordinator_state",
        status.get("state") == "RUNNING",
        f"state={status.get('state')!r}",
    )
    safety_controls_ok = (
        status.get("armed") is True
        and status.get("arm_reason") == "LIVE_ARMED"
        and status.get("acknowledgement_valid") is True
        and status.get("arm_enabled") is True
        and status.get("arm_date_matches") is True
        and status.get("arm_strategy_matches") is True
        and status.get("kill_switch_enabled") is False
    )
    record(
        "live_safety_controls",
        safety_controls_ok,
        (
            f"armed={status.get('armed')!r} reason={status.get('arm_reason')!r} "
            f"kill_switch={status.get('kill_switch_enabled')!r}"
        ),
    )
    status_fresh, status_fresh_detail = _freshness_detail(
        _parse_aware_datetime(status.get("updated_at_ist")),
        now=now,
        maximum_age_seconds=status_max_age_seconds,
    )
    record("coordinator_freshness", status_fresh, status_fresh_detail)

    status_run_id = status.get("run_id")
    valid_status_run_id = isinstance(status_run_id, str) and bool(status_run_id.strip())
    record(
        "coordinator_run_id",
        valid_status_run_id,
        "non-empty run correlation is present" if valid_status_run_id else "run_id is missing",
    )

    children = status.get("children")
    children = children if isinstance(children, dict) else {}
    order_children_ok = True
    order_child_details: list[str] = []
    for side in ("long", "short"):
        child = children.get(side)
        child = child if isinstance(child, dict) else {}
        expected_child_session = f"fno_v13_v10_g_live_kite_qty1_{side}"
        child_fresh, child_fresh_detail = _freshness_detail(
            _parse_aware_datetime(child.get("updated_at_ist")),
            now=now,
            maximum_age_seconds=status_max_age_seconds,
        )
        child_ok = (
            child.get("session_id") == expected_child_session
            and type(child.get("pid")) is int
            and child.get("pid", 0) > 0
            and child.get("return_code") is None
            and child.get("state") == "RUNNING"
            and child_fresh
        )
        order_children_ok = order_children_ok and child_ok
        order_child_details.append(
            f"{side}:state={child.get('state')!r},pid={child.get('pid')!r},"
            f"return_code={child.get('return_code')!r},{child_fresh_detail}"
        )
    record(
        "order_manager_children",
        order_children_ok,
        "; ".join(order_child_details),
    )

    broker_child = children.get("broker_reconciliation")
    broker_child = broker_child if isinstance(broker_child, dict) else {}
    broker_child_ok = (
        broker_child.get("session_id")
        == "fno_v13_v10_g_live_kite_qty1_broker_reconciliation"
        and type(broker_child.get("pid")) is int
        and broker_child.get("pid", 0) > 0
        and broker_child.get("return_code") is None
        and broker_child.get("state") == "RUNNING"
        and broker_child.get("broker_truth_available") is True
        and broker_child.get("scope_complete") is True
        and broker_child.get("mismatch_count") == 0
        and broker_child.get("active_order_parity_complete") is True
        and broker_child.get("active_order_mismatch_count") == 0
    )
    record(
        "broker_reconciliation_child",
        broker_child_ok,
        (
            f"state={broker_child.get('state')!r} pid={broker_child.get('pid')!r} "
            f"return_code={broker_child.get('return_code')!r} "
            f"truth={broker_child.get('broker_truth_available')!r} "
            f"scope_complete={broker_child.get('scope_complete')!r} "
            f"mismatch_count={broker_child.get('mismatch_count')!r} "
            f"active_order_parity_complete="
            f"{broker_child.get('active_order_parity_complete')!r} "
            f"active_order_mismatch_count="
            f"{broker_child.get('active_order_mismatch_count')!r}"
        ),
    )
    broker_child_fresh, broker_child_fresh_detail = _freshness_detail(
        _parse_aware_datetime(broker_child.get("updated_at_ist")),
        now=now,
        maximum_age_seconds=reconciliation_max_age_seconds,
    )
    record(
        "broker_reconciliation_child_freshness",
        broker_child_fresh,
        broker_child_fresh_detail,
    )

    pipeline_roles = (
        ("scanner", "fno_v13_v10_g_scanner_5min", "scanner-5m"),
        ("confirmation", "fno_v13_v10_g_confirmation_1min", "confirmation-1m"),
        ("equity_1m_feed", "fno_v13_v10_g_equity_1min_feed", None),
    )
    runtime_status_root = runtime_root / "runtime_status"
    for label, session_name, role in pipeline_roles:
        heartbeat_path = runtime_status_root / f"{session_name}.heartbeat"
        heartbeat, heartbeat_detail = _read_key_value_status(heartbeat_path)
        record(f"{label}_heartbeat_artifact", heartbeat is not None, heartbeat_detail)
        heartbeat = heartbeat or {}
        terminal_completion = (
            _confirmation_completion_detail(
                heartbeat, now=now, session_date=expected_session_date
            )
            if label == "confirmation"
            else (
                _equity_feed_completion_detail(
                    heartbeat, now=now, session_date=expected_session_date
                )
                if label == "equity_1m_feed"
                else None
            )
        )
        state_ok = heartbeat.get("state") in {"RUNNING", "WAITING", "SUCCESS"} or (
            label == "scanner" and heartbeat.get("state") == "DONE"
        ) or (terminal_completion is not None and terminal_completion[0])
        identity_ok = (
            heartbeat.get("session") == session_name
            and heartbeat.get("strategy_version") == expected_strategy_version
            and heartbeat.get("strategy_fingerprint") == expected_strategy_fingerprint
            and (role is None or heartbeat.get("role") == role)
        )
        record(
            f"{label}_heartbeat_contract",
            state_ok and identity_ok,
            (
                f"state={heartbeat.get('state')!r} session={heartbeat.get('session')!r} "
                f"role={heartbeat.get('role')!r}"
            ),
        )
        heartbeat_fresh, heartbeat_fresh_detail = _freshness_detail(
            _parse_aware_datetime(heartbeat.get("ts")),
            now=now,
            maximum_age_seconds=pipeline_max_age_seconds,
        )
        if label == "scanner" and identity_ok:
            scheduled = _scanner_schedule_detail(
                heartbeat, now=now, session_date=expected_session_date
            )
            if scheduled is not None:
                heartbeat_fresh, heartbeat_fresh_detail = scheduled
        elif identity_ok and terminal_completion is not None:
            heartbeat_fresh, heartbeat_fresh_detail = terminal_completion
        record(f"{label}_heartbeat_freshness", heartbeat_fresh, heartbeat_fresh_detail)

    cash_path, cash_path_detail = _latest_session_marker(
        runtime_root / "slot_ready_5m",
        prefix="slot_",
        session_date=expected_session_date,
    )
    record("cash_5m_marker_artifact", cash_path is not None, cash_path_detail)
    cash, cash_detail = _read_json_object(cash_path) if cash_path is not None else (None, "artifact is missing")
    cash = cash or {}
    cash_slot = _parse_aware_datetime(cash.get("slot_ist"))
    cash_contract = (
        cash.get("source") == "final"
        and cash.get("complete") is True
        and cash.get("fno_equity_quality_complete") is True
        and type(cash.get("fno_equity_expected")) is int
        and cash.get("fno_equity_expected", 0) > 0
        and cash.get("fno_equity_ready") == cash.get("fno_equity_expected")
        and cash.get("fno_equity_failed") == 0
        and cash_slot is not None
        and cash_slot.date() == expected_session_date
    )
    record(
        "cash_5m_marker_contract",
        cash_contract,
        (
            f"{cash_detail}; complete={cash.get('complete')!r} "
            f"equity_ready={cash.get('fno_equity_ready')!r}/"
            f"{cash.get('fno_equity_expected')!r} failed={cash.get('fno_equity_failed')!r}"
        ),
    )
    cash_fresh, cash_fresh_detail = _freshness_detail(
        _parse_aware_datetime(cash.get("published_at_ist")),
        now=now,
        maximum_age_seconds=market_data_max_age_seconds,
    )
    record("cash_5m_marker_freshness", cash_fresh, cash_fresh_detail)

    futures_path, futures_path_detail = _latest_session_marker(
        runtime_root / "fno_oi" / "slot_ready",
        prefix="slot_",
        session_date=expected_session_date,
    )
    record("futures_oi_5m_marker_artifact", futures_path is not None, futures_path_detail)
    futures, futures_detail = (
        _read_json_object(futures_path)
        if futures_path is not None
        else (None, "artifact is missing")
    )
    futures = futures or {}
    futures_slot = _parse_aware_datetime(futures.get("slot_ist"))
    coverage = futures.get("stock_coverage_ratio")
    coverage_ok = (
        type(coverage) in {int, float}
        and not isinstance(coverage, bool)
        and float(coverage) >= 0.99
    )
    futures_contract = (
        futures.get("schema_version") == "fno_oi_fetch_slot_v2"
        and futures.get("source") == "final"
        and futures.get("state") == "SUCCESS"
        and futures.get("universe_date") == day_text
        and futures.get("stock_complete") is True
        and futures.get("stock_state") == "SUCCESS"
        and coverage_ok
        and futures.get("stock_failed_count") == 0
        and futures.get("stock_failed_symbols") == []
        and futures.get("unexpected_outcome_symbols") == []
        and futures_slot is not None
        and futures_slot.date() == expected_session_date
    )
    record(
        "futures_oi_5m_marker_contract",
        futures_contract,
        (
            f"{futures_detail}; state={futures.get('state')!r} "
            f"stock_complete={futures.get('stock_complete')!r} "
            f"coverage={coverage!r} failed={futures.get('stock_failed_count')!r}"
        ),
    )
    futures_fresh, futures_fresh_detail = _freshness_detail(
        _parse_aware_datetime(futures.get("published_at_ist")),
        now=now,
        maximum_age_seconds=market_data_max_age_seconds,
    )
    record("futures_oi_5m_marker_freshness", futures_fresh, futures_fresh_detail)

    reconciliation, reconciliation_detail = _read_json_object(reconciliation_path)
    record("broker_reconciliation_artifact", reconciliation is not None, reconciliation_detail)
    reconciliation = reconciliation or {}

    claimed_digest = reconciliation.get("report_sha256")
    unsigned = {
        key: value for key, value in reconciliation.items() if key != "report_sha256"
    }
    observed_digest = _canonical_sha256(unsigned)
    digest_valid = (
        isinstance(claimed_digest, str)
        and SHA256_PATTERN.fullmatch(claimed_digest) is not None
        and observed_digest == claimed_digest
    )
    record(
        "broker_reconciliation_digest",
        digest_valid,
        "canonical SHA-256 verified" if digest_valid else "canonical SHA-256 is missing or invalid",
    )
    record(
        "broker_reconciliation_contract",
        reconciliation.get("schema_version") == RECONCILIATION_SCHEMA_VERSION,
        f"schema={reconciliation.get('schema_version')!r}",
    )
    record(
        "broker_reconciliation_session",
        reconciliation.get("session_date") == day_text,
        f"expected={day_text} observed={reconciliation.get('session_date')!r}",
    )
    reconciliation_run_id = reconciliation.get("run_id")
    correlated = (
        valid_status_run_id
        and isinstance(reconciliation_run_id, str)
        and reconciliation_run_id == status_run_id
    )
    record(
        "run_correlation",
        correlated,
        "status and broker evidence share one run_id" if correlated else "status and broker run_id differ",
    )

    broker = reconciliation.get("position_reconciliation")
    broker = broker if isinstance(broker, dict) else {}
    broker_timestamp = None
    for candidate in (
        broker.get("observed_at_ist"),
        broker.get("generated_at_ist"),
        reconciliation.get("observed_at_ist"),
        reconciliation.get("generated_at_ist"),
    ):
        broker_timestamp = _parse_aware_datetime(candidate)
        if broker_timestamp is not None:
            break
    broker_fresh, broker_fresh_detail = _freshness_detail(
        broker_timestamp,
        now=now,
        maximum_age_seconds=reconciliation_max_age_seconds,
    )
    record("broker_reconciliation_freshness", broker_fresh, broker_fresh_detail)
    record(
        "broker_truth",
        broker.get("broker_truth_available") is True,
        f"broker_truth_available={broker.get('broker_truth_available')!r}",
    )
    scope_complete = broker.get("scope_complete") is True
    scope_matches = broker.get("scope") == EXPECTED_RECONCILIATION_SCOPE
    unscoped = broker.get("unscoped_nonzero_positions")
    complete_empty_scope = scope_complete and isinstance(unscoped, list) and not unscoped
    record(
        "broker_scope",
        scope_matches and complete_empty_scope,
        (
            f"scope={broker.get('scope')!r} complete={broker.get('scope_complete')!r} "
            f"unscoped_nonzero_count={len(unscoped) if isinstance(unscoped, list) else 'invalid'}"
        ),
    )
    mismatch_count = broker.get("mismatch_count")
    mismatches = broker.get("mismatches")
    parity = (
        type(mismatch_count) is int
        and mismatch_count == 0
        and isinstance(mismatches, list)
        and not mismatches
    )
    record(
        "broker_local_position_parity",
        parity,
        (
            f"mismatch_count={mismatch_count!r} "
            f"mismatch_rows={len(mismatches) if isinstance(mismatches, list) else 'invalid'}"
        ),
    )
    active_order_count = broker.get("active_order_mismatch_count")
    active_order_mismatches = broker.get("active_order_mismatches")
    local_active_count = broker.get("local_expected_active_order_count")
    broker_active_count = broker.get("broker_active_tagged_order_count")
    tagged_order_count = broker.get("tagged_order_count")
    local_active_ids = broker.get("local_expected_active_order_ids")
    broker_active_ids = broker.get("broker_active_tagged_order_ids")
    active_ids_are_complete = (
        type(local_active_count) is int
        and local_active_count >= 0
        and type(broker_active_count) is int
        and broker_active_count >= 0
        and type(tagged_order_count) is int
        and tagged_order_count >= broker_active_count
        and isinstance(local_active_ids, list)
        and isinstance(broker_active_ids, list)
        and all(isinstance(value, str) and value for value in local_active_ids)
        and all(isinstance(value, str) and value for value in broker_active_ids)
        and len(local_active_ids) == len(set(local_active_ids)) == local_active_count
        and len(broker_active_ids) == len(set(broker_active_ids)) == broker_active_count
        and local_active_ids == sorted(local_active_ids)
        and broker_active_ids == sorted(broker_active_ids)
    )
    active_order_parity = (
        broker.get("active_order_parity_complete") is True
        and type(active_order_count) is int
        and active_order_count == 0
        and isinstance(active_order_mismatches, list)
        and not active_order_mismatches
        and active_ids_are_complete
        and local_active_ids == broker_active_ids
    )
    record(
        "broker_local_active_order_parity",
        active_order_parity,
        (
            f"complete={broker.get('active_order_parity_complete')!r} "
            f"mismatch_count={active_order_count!r} "
            f"mismatch_rows="
            f"{len(active_order_mismatches) if isinstance(active_order_mismatches, list) else 'invalid'} "
            f"local_active_count={local_active_count!r} "
            f"broker_active_count={broker_active_count!r}"
        ),
    )

    state = "PASS" if checks and all(check.state == "PASS" for check in checks) else "FAIL"
    return LiveTrustReport(
        observed_at_ist=now.isoformat(timespec="seconds"),
        session_date=day_text,
        state=state,
        status_path=str(status_path),
        reconciliation_path=str(reconciliation_path),
        run_id=status_run_id if valid_status_run_id else None,
        checks=tuple(checks),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")),
    )
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--status-max-age-seconds", type=float, default=30.0)
    parser.add_argument("--reconciliation-max-age-seconds", type=float, default=120.0)
    parser.add_argument("--pipeline-max-age-seconds", type=float, default=120.0)
    parser.add_argument("--market-data-max-age-seconds", type=float, default=420.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from fno_v13_v10_g_live_config import (
        STRATEGY_VERSION,
        strategy_fingerprint,
        validate_strategy,
    )

    validate_strategy()
    observed_at = datetime.now(IST)
    session_date = args.session_date or observed_at.date()
    runtime_root = args.runtime_root.resolve()
    status_path = (
        runtime_root / "fno_oi" / "v13_v10_g_live" / "live_kite" / "status.json"
    )
    reconciliation_path = (
        runtime_root
        / "observability"
        / "reconciliation"
        / f"broker_positions_{session_date.isoformat()}.json"
    )
    report = evaluate_live_trust(
        runtime_root=runtime_root,
        status_path=status_path,
        reconciliation_path=reconciliation_path,
        expected_session_date=session_date,
        expected_strategy_version=STRATEGY_VERSION,
        expected_strategy_fingerprint=strategy_fingerprint(),
        observed_at=observed_at,
        status_max_age_seconds=args.status_max_age_seconds,
        reconciliation_max_age_seconds=args.reconciliation_max_age_seconds,
        pipeline_max_age_seconds=args.pipeline_max_age_seconds,
        market_data_max_age_seconds=args.market_data_max_age_seconds,
    )
    json.dump(report.as_dict(), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if report.state == "PASS" else 2


if __name__ == "__main__":  # pragma: no cover - exercised as an operator command
    raise SystemExit(main())
