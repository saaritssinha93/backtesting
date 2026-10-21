"""Pure, offline tests for the shared G3 LIVE portfolio admission book."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import fno_live_entry_guard as guard
import fno_v13_v10_g_3_live_execution_policy as policy
from g3_live_risk import (
    ADMISSION_SCHEMA_VERSION,
    LiveRiskIntegrityError,
    LiveRiskLimitError,
    admit_live_entry,
)
from tests.test_fno_v13_v10_g_3_runtime import (
    DAY as RUNTIME_DAY,
    candidate,
    runtime,
)

DAY = "2026-10-21"
NOW = datetime(2026, 10, 21, 9, 30, tzinfo=ZoneInfo("Asia/Kolkata"))
VERSION = "V13-V10-G-3-PRODUCTION-V11"
FINGERPRINT = "82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4"
TERMINAL = frozenset({
    "CLOSED", "NO_FILL", "ENTRY_REJECTED", "BLOCKED_SIZING",
    "BLOCKED_PORTFOLIO", "CANCELLED",
})


def order(signal_id: str, *, side: str = "LONG") -> dict:
    return {
        "signal_id": signal_id,
        "mode": "LIVE",
        "session_date": DAY,
        "strategy_version": VERSION,
        "strategy_fingerprint": FINGERPRINT,
        "status": "PENDING_ENTRY",
        "side": side,
        "trigger_price": 102.0,
        "sizing_reference_price": 102.0,
        "lot_size": 1,
        "strategy_sized_quantity": 490,
        "requested_quantity": 490,
        "quantity": 490,
        "execution_quantity_override": None,
        "capital_rs": 10000.0,
        "entry_submission_attempt_count": 0,
        "entry_submission_uncertain": False,
        "entry_order_id": "",
        "entry_at_ist": "",
        "entry_price": 0.0,
        **policy.execution_metadata(),
    }


def path_for(root: Path, signal_id: str, namespace: str = "live_kite_qty1") -> Path:
    return root / namespace / DAY / f"{signal_id}.json"


def write(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row), encoding="utf-8")


def admit(root: Path, row: dict, *, namespace: str = "live_kite_qty1") -> Path:
    path = path_for(root, row["signal_id"], namespace)
    admit_live_entry(
        row,
        book_root=root,
        state_path=path,
        now=NOW,
        policy=policy,
        terminal_states=TERMINAL,
    )
    write(path, row)
    return path


def test_shared_reservations_are_unlimited_by_default(tmp_path):
    first, second, third = order("LONG_ONE"), order("SHORT_TWO", side="SHORT"), order("LONG_THREE")
    admit(tmp_path, first, namespace="long_worker")
    admit(tmp_path, second, namespace="short_worker")
    admit(tmp_path, third, namespace="third_worker")
    assert first["live_risk_admission_schema_version"] == ADMISSION_SCHEMA_VERSION
    assert second["live_risk_projected_reserved_gross_exposure_rs"] == 99960.0
    assert third["live_risk_projected_reserved_open_positions"] == 3
    assert third["live_risk_projected_reserved_gross_exposure_rs"] == 149940.0
    assert third["status"] == "PENDING_ENTRY"
    assert third["entry_submission_attempt_count"] == 0
    assert third["live_risk_decision"] == "ADMITTED"


def test_terminal_states_are_observed_without_a_daily_admission_cap(tmp_path):
    for number in range(1, 5):
        row = order(f"DONE_{number}")
        path = admit(tmp_path, row)
        row.update(status="CLOSED", entry_order_id=f"ORDER_{number}")
        write(path, row)
    fifth = order("FIFTH")
    admit_live_entry(fifth, book_root=tmp_path,
        state_path=path_for(tmp_path, fifth["signal_id"]), now=NOW,
        policy=policy, terminal_states=TERMINAL)
    assert fifth["live_risk_observed_reserved_open_positions"] == 0
    assert fifth["live_risk_observed_daily_entries"] == 4
    assert fifth["live_risk_projected_daily_entries"] == 5


def test_uncertain_submissions_are_observed_without_an_active_position_cap(tmp_path):
    for number in range(2):
        row = order(f"UNCERTAIN_{number}")
        path = admit(tmp_path, row)
        row.update(status="CANCELLED", entry_submission_uncertain=True,
                   entry_submission_attempt_count=1)
        write(path, row)
    candidate = order("THREE")
    admit_live_entry(candidate, book_root=tmp_path,
        state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
        policy=policy, terminal_states=TERMINAL)
    assert candidate["live_risk_observed_reserved_open_positions"] == 2


def test_reserved_gross_cap_is_enforced_independently(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, "MAX_RESERVED_OPEN_POSITIONS", 3)
    monkeypatch.setattr(policy, "MAX_RESERVED_GROSS_EXPOSURE_RS", 90000.0)
    first = order("GROSS_ONE")
    admit(tmp_path, first)
    second = order("GROSS_TWO")
    with pytest.raises(LiveRiskLimitError, match="MAX_RESERVED_GROSS_EXPOSURE"):
        admit_live_entry(second, book_root=tmp_path,
            state_path=path_for(tmp_path, second["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)
    assert second["live_risk_observed_reserved_open_positions"] == 1


def test_previous_capped_admission_is_counted_during_current_policy_restart(tmp_path):
    previous = order("PREVIOUS_OPEN")
    previous.update(policy.previous_capped_execution_metadata())
    previous.update(
        status="OPEN",
        entry_submission_attempt_count=1,
        entry_order_id="OLD_ORDER",
        entry_at_ist=NOW.isoformat(),
        entry_price=102.0,
        live_risk_admission_schema_version=ADMISSION_SCHEMA_VERSION,
        live_risk_policy_fingerprint=policy.previous_capped_policy_fingerprint(),
        live_risk_admitted_at_ist=NOW.isoformat(),
        live_risk_reserved_exposure_rs=49980.0,
        live_risk_decision="ADMITTED",
    )
    write(path_for(tmp_path, previous["signal_id"]), previous)
    previous_closed = {**previous,
        "signal_id": "PREVIOUS_CLOSED", "status": "CLOSED",
        "entry_order_id": "OLD_CLOSED_ORDER"}
    write(path_for(tmp_path, previous_closed["signal_id"]), previous_closed)
    current = order("CURRENT_UNLIMITED")
    admit_live_entry(current, book_root=tmp_path,
        state_path=path_for(tmp_path, current["signal_id"]), now=NOW,
        policy=policy, terminal_states=TERMINAL)
    assert current["live_risk_observed_daily_entries"] == 2
    assert current["live_risk_observed_reserved_open_positions"] == 1
    assert current["live_risk_projected_reserved_open_positions"] == 2
    assert current["live_risk_projected_reserved_gross_exposure_rs"] == 99960.0


def test_runtime_recognizes_previous_policy_only_for_broker_or_terminal_management(
    runtime,
):
    row = candidate()
    row["data_contract"] = runtime.hybrid.DATA_CONTRACT_VERSION
    signal = runtime.select_entry_signals(
        [row], RUNTIME_DAY, "09:30", capital_rs=100000.0, leverage=5.0
    )[0]
    previous = runtime.create_order_state(signal, "LIVE")
    previous.update(policy.previous_capped_execution_metadata())

    pristine = deepcopy(previous)
    assert runtime._previous_capped_g3_protective_state(pristine) is False
    with pytest.raises((ValueError, RuntimeError)):
        runtime._validate_order_state(pristine, signal, "LIVE")

    submitted = deepcopy(previous)
    submitted.update(status="OPEN", entry_submission_attempt_count=1,
                     entry_order_id="OLD_ORDER", entry_price=102.0,
                     entry_at_ist=NOW.isoformat(), quantity=7)
    runtime._validate_order_state(submitted, signal, "LIVE")
    assert submitted["live_execution_policy_mismatch"] == (
        "PREVIOUS_CAPPED_POLICY_PROTECTIVE_MANAGEMENT_ONLY"
    )

    terminal = deepcopy(previous)
    terminal.update(status="BLOCKED_PORTFOLIO", status_reason="OLD_CAP")
    runtime._validate_order_state(terminal, signal, "LIVE")
    assert terminal["live_execution_policy_mismatch"] == (
        "PREVIOUS_CAPPED_POLICY_PROTECTIVE_MANAGEMENT_ONLY"
    )


def test_same_durable_reservation_is_idempotent(tmp_path):
    row = order("RETRY")
    path = admit(tmp_path, row)
    before = deepcopy(row)
    admit_live_entry(row, book_root=tmp_path, state_path=path, now=NOW,
                     policy=policy, terminal_states=TERMINAL)
    assert row == before


def test_current_clean_pending_snapshot_can_be_admitted(tmp_path):
    row = order("CURRENT")
    path = path_for(tmp_path, row["signal_id"])
    write(path, row)
    admit_live_entry(row, book_root=tmp_path, state_path=path, now=NOW,
                     policy=policy, terminal_states=TERMINAL)
    assert row["live_risk_decision"] == "ADMITTED"


def test_different_unadmitted_active_snapshot_fails_closed(tmp_path):
    waiting = order("WAITING")
    write(path_for(tmp_path, waiting["signal_id"]), waiting)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError, match="Another active"):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


@pytest.mark.parametrize("payload", ["{broken", "[]"])
def test_unreadable_or_non_object_evidence_fails_closed(tmp_path, payload):
    bad = tmp_path / "other" / DAY / "BAD.json"
    bad.parent.mkdir(parents=True)
    bad.write_text(payload, encoding="utf-8")
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


def test_json_directory_evidence_fails_closed(tmp_path):
    bad = tmp_path / "other" / DAY / "BAD.json"
    bad.mkdir(parents=True)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError, match="Unreadable"):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


@pytest.mark.parametrize("change", [
    {"status": "MYSTERY"},
    {"strategy_fingerprint": "stale"},
    {"entry_submission_uncertain": "yes"},
    {"status": "OPEN"},
])
def test_ambiguous_or_unadmitted_active_evidence_fails_closed(tmp_path, change):
    bad = {**order("BAD"), **change}
    write(path_for(tmp_path, bad["signal_id"], "other"), bad)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


@pytest.mark.parametrize("status", ["CLOSED", "ENTRY_REJECTED"])
def test_terminal_broker_status_without_admission_fails_closed(tmp_path, status):
    bad = {**order("BAD"), "status": status}
    write(path_for(tmp_path, bad["signal_id"], "other"), bad)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError, match="broker evidence"):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


def test_duplicate_signal_across_namespaces_fails_closed(tmp_path):
    row = order("DUPLICATE")
    write(path_for(tmp_path, row["signal_id"], "one"), row)
    write(path_for(tmp_path, row["signal_id"], "two"), row)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError, match="Duplicate"):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


def test_admitted_signal_cannot_be_replayed_in_a_new_namespace(tmp_path):
    existing = order("REPLAY")
    admit(tmp_path, existing, namespace="first")
    replay = order("REPLAY")
    with pytest.raises(LiveRiskIntegrityError, match="Duplicate"):
        admit_live_entry(replay, book_root=tmp_path,
            state_path=path_for(tmp_path, replay["signal_id"], "second"), now=NOW,
            policy=policy, terminal_states=TERMINAL)


def test_tampered_reserved_exposure_fails_closed(tmp_path):
    existing = order("TAMPERED")
    path = admit(tmp_path, existing)
    existing["live_risk_reserved_exposure_rs"] = 1.0
    write(path, existing)
    candidate = order("CANDIDATE")
    with pytest.raises(LiveRiskIntegrityError, match="reserved exposure"):
        admit_live_entry(candidate, book_root=tmp_path,
            state_path=path_for(tmp_path, candidate["signal_id"]), now=NOW,
            policy=policy, terminal_states=TERMINAL)


def runtime_state(runtime, suffix: str = "") -> dict:
    row = candidate()
    row["data_contract"] = runtime.hybrid.DATA_CONTRACT_VERSION
    signal = runtime.select_entry_signals(
        [row], RUNTIME_DAY, "09:30", capital_rs=100000.0, leverage=5.0
    )[0]
    state = runtime.create_order_state(signal, "LIVE")
    if suffix:
        state["signal_id"] += suffix
    return state


def approve_entry_owner(monkeypatch):
    monkeypatch.setattr(guard, "deployment_state", lambda: {
        "profile": "V13_V10_G_3",
        "deployment_stage": "LIVE_APPROVED",
        "live_activation_authorized": True,
    })


def test_runtime_writes_every_unlimited_reservation_before_broker(runtime, monkeypatch):
    approve_entry_owner(monkeypatch)

    class Broker:
        def __init__(self):
            self.calls = []
            self.expected_path = None

        def place_order(self, **payload):
            snapshot = json.loads(self.expected_path.read_text(encoding="utf-8"))
            assert snapshot["live_risk_decision"] == "ADMITTED"
            assert snapshot["entry_submission_attempt_count"] == 1
            self.calls.append(payload)
            return f"ORDER_{len(self.calls)}"

    broker = Broker()
    now = runtime.config.slot_datetime(RUNTIME_DAY, "09:33")
    first, second, third = (
        runtime_state(runtime, "_ONE"),
        runtime_state(runtime, "_TWO"),
        runtime_state(runtime, "_THREE"),
    )
    for state in (first, second, third):
        broker.expected_path = runtime._order_path(
            RUNTIME_DAY, "LIVE", state["signal_id"]
        )
        result = runtime._place_live_entry_order(
            state, broker, now, transaction_type="BUY", tag="TEST",
            order_type="MARKET", reason="TEST",
        )
        assert result.startswith("ORDER_")

    third_path = runtime._order_path(RUNTIME_DAY, "LIVE", third["signal_id"])
    admitted = json.loads(third_path.read_text(encoding="utf-8"))
    assert admitted["live_risk_decision"] == "ADMITTED"
    assert admitted["entry_submission_attempt_count"] == 1
    assert len(broker.calls) == 3


def test_reservation_write_failure_prevents_broker_submission(runtime, monkeypatch):
    approve_entry_owner(monkeypatch)
    state = runtime_state(runtime, "_WRITE_FAIL")
    broker_calls = []

    class Broker:
        def place_order(self, **payload):
            broker_calls.append(payload)
            return "UNEXPECTED"

    monkeypatch.setattr(
        runtime, "_write_order_state",
        lambda _state: (_ for _ in ()).throw(PermissionError("simulated write failure")),
    )
    with pytest.raises(PermissionError, match="simulated"):
        runtime._place_live_entry_order(
            state, Broker(), runtime.config.slot_datetime(RUNTIME_DAY, "09:33"),
            transaction_type="BUY", tag="TEST", order_type="MARKET", reason="TEST",
        )
    assert broker_calls == []


def test_paper_state_never_contains_live_risk_admission(runtime):
    state = runtime.create_order_state(
        runtime.select_entry_signals(
            [{**candidate(), "data_contract": runtime.hybrid.DATA_CONTRACT_VERSION}],
            RUNTIME_DAY, "09:30", capital_rs=100000.0, leverage=5.0,
        )[0],
        "PAPER",
    )
    assert not any(key.startswith("live_risk_") for key in state)
