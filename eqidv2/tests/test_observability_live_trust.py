from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ai_platform.observability.live_trust import evaluate_live_trust


IST = timezone(timedelta(hours=5, minutes=30))
DAY = date(2026, 9, 25)
NOW = datetime(2026, 9, 25, 10, 0, tzinfo=IST)
VERSION = "FNO_V13_V10_G_RETAINED_20260914"
FINGERPRINT = "a" * 64
RUN_ID = "run-20260925-a"


def _digest(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_text(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return path


def _status(*, updated_at: datetime = NOW - timedelta(seconds=5)) -> dict:
    return {
        "schema_version": "fno_v6_live_kite_qty1_status_v1",
        "run_id": RUN_ID,
        "session_date": DAY.isoformat(),
        "strategy_version": VERSION,
        "strategy_fingerprint": FINGERPRINT,
        "state": "RUNNING",
        "execution_mode": "LIVE",
        "execution_profile": "live_kite_qty1",
        "quantity": 1,
        "quantity_policy": "FIXED_ONE_SHARE",
        "updated_at_ist": updated_at.isoformat(),
        "armed": True,
        "arm_reason": "LIVE_ARMED",
        "acknowledgement_valid": True,
        "arm_enabled": True,
        "arm_date_matches": True,
        "arm_strategy_matches": True,
        "kill_switch_enabled": False,
        "children": {
            "long": {
                "session_id": "fno_v13_v10_g_live_kite_qty1_long",
                "pid": 1001,
                "return_code": None,
                "state": "RUNNING",
                "updated_at_ist": updated_at.isoformat(),
            },
            "short": {
                "session_id": "fno_v13_v10_g_live_kite_qty1_short",
                "pid": 1002,
                "return_code": None,
                "state": "RUNNING",
                "updated_at_ist": updated_at.isoformat(),
            },
            "broker_reconciliation": {
                "session_id": (
                    "fno_v13_v10_g_live_kite_qty1_broker_reconciliation"
                ),
                "pid": 1003,
                "return_code": None,
                "state": "RUNNING",
                "updated_at_ist": updated_at.isoformat(),
                "broker_truth_available": True,
                "scope_complete": True,
                "mismatch_count": 0,
                "active_order_parity_complete": True,
                "active_order_mismatch_count": 0,
            },
        },
    }


def _reconciliation(
    *, mismatch_count: int = 0, generated_at: datetime = NOW - timedelta(seconds=10)
) -> dict:
    broker = {
        "broker_truth_available": True,
        "scope": "nse_mis_strategy_tagged_symbols_and_active_orders",
        "scope_complete": True,
        "mismatch_count": mismatch_count,
        "mismatches": [] if mismatch_count == 0 else [{"tradingsymbol": "EXAMPLE"}],
        "unscoped_nonzero_positions": [],
        "local_state_count": 0,
        "tagged_symbol_count": 0,
        "tagged_order_count": 0,
        "local_expected_active_order_count": 0,
        "local_expected_active_order_ids": [],
        "broker_active_tagged_order_count": 0,
        "broker_active_tagged_order_ids": [],
        "active_order_parity_complete": True,
        "active_order_mismatch_count": 0,
        "active_order_mismatches": [],
    }
    payload = {
        "schema_version": "v13_v10_g_broker_position_reconciliation_v2",
        "session_date": DAY.isoformat(),
        "generated_at_ist": generated_at.isoformat(),
        "run_id": RUN_ID,
        "position_reconciliation": broker,
    }
    payload["report_sha256"] = _digest(payload)
    return payload


def _runtime_evidence(root: Path) -> None:
    heartbeat_template = (
        "state=WAITING\n"
        "session={session}\n"
        f"ts={NOW.isoformat()}\n"
        "run_id=pipeline-run\n"
        "strategy_version={version}\n"
        "strategy_fingerprint={fingerprint}\n"
        "{role}"
    )
    roles = (
        ("fno_v13_v10_g_scanner_5min", "role=scanner-5m\n"),
        ("fno_v13_v10_g_confirmation_1min", "role=confirmation-1m\n"),
        ("fno_v13_v10_g_equity_1min_feed", ""),
    )
    for session, role in roles:
        _write_text(
            root / "runtime_status" / f"{session}.heartbeat",
            heartbeat_template.format(
                session=session,
                version=VERSION,
                fingerprint=FINGERPRINT,
                role=role,
            ),
        )
    _write(
        root / "slot_ready_5m" / "slot_20260925_1000.json",
        {
            "slot_ist": NOW.isoformat(),
            "published_at_ist": (NOW - timedelta(seconds=10)).isoformat(),
            "source": "final",
            "complete": True,
            "fno_equity_quality_complete": True,
            "fno_equity_expected": 210,
            "fno_equity_ready": 210,
            "fno_equity_failed": 0,
        },
    )
    _write(
        root / "fno_oi" / "slot_ready" / "slot_20260925_1000.json",
        {
            "schema_version": "fno_oi_fetch_slot_v2",
            "source": "final",
            "state": "SUCCESS",
            "universe_date": DAY.isoformat(),
            "slot_ist": NOW.isoformat(),
            "published_at_ist": (NOW - timedelta(seconds=5)).isoformat(),
            "stock_complete": True,
            "stock_state": "SUCCESS",
            "stock_coverage_ratio": 1.0,
            "stock_failed_count": 0,
            "stock_failed_symbols": [],
            "unexpected_outcome_symbols": [],
        },
    )


def _evaluate(tmp_path: Path, status: dict, reconciliation: dict):
    _runtime_evidence(tmp_path)
    return evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=NOW,
    )


def test_live_trust_passes_only_for_fresh_correlated_broker_truth(tmp_path: Path) -> None:
    report = _evaluate(tmp_path, _status(), _reconciliation())

    assert report.state == "PASS"
    assert all(check.state == "PASS" for check in report.checks)
    assert report.as_dict()["summary"] == {"passed": len(report.checks), "failed": 0}


def test_live_trust_fails_on_tampered_reconciliation(tmp_path: Path) -> None:
    reconciliation = _reconciliation()
    reconciliation["position_reconciliation"]["tagged_symbol_count"] = 1

    report = _evaluate(tmp_path, _status(), reconciliation)
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["broker_reconciliation_digest"] == "FAIL"


def test_live_trust_fails_on_stale_status_and_position_mismatch(tmp_path: Path) -> None:
    report = _evaluate(
        tmp_path,
        _status(updated_at=NOW - timedelta(minutes=3)),
        _reconciliation(mismatch_count=1),
    )
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["coordinator_freshness"] == "FAIL"
    assert checks["broker_local_position_parity"] == "FAIL"


def test_live_trust_fails_on_active_tagged_order_mismatch(tmp_path: Path) -> None:
    reconciliation = _reconciliation()
    broker = reconciliation["position_reconciliation"]
    broker.update(
        tagged_order_count=1,
        broker_active_tagged_order_count=1,
        broker_active_tagged_order_ids=["ORPHAN-1"],
        active_order_parity_complete=False,
        active_order_mismatch_count=1,
        active_order_mismatches=[
            {
                "kind": "unexpected_broker_active_tagged_order",
                "order_id": "ORPHAN-1",
            }
        ],
    )
    reconciliation["report_sha256"] = _digest(
        {
            key: value
            for key, value in reconciliation.items()
            if key != "report_sha256"
        }
    )

    report = _evaluate(tmp_path, _status(), reconciliation)
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["broker_local_position_parity"] == "PASS"
    assert checks["broker_local_active_order_parity"] == "FAIL"


def test_live_trust_rejects_legacy_position_only_reconciliation(tmp_path: Path) -> None:
    reconciliation = _reconciliation()
    reconciliation["schema_version"] = (
        "v13_v10_g_broker_position_reconciliation_v1"
    )
    broker = reconciliation["position_reconciliation"]
    for key in (
        "tagged_order_count",
        "local_expected_active_order_count",
        "local_expected_active_order_ids",
        "broker_active_tagged_order_count",
        "broker_active_tagged_order_ids",
        "active_order_parity_complete",
        "active_order_mismatch_count",
        "active_order_mismatches",
    ):
        broker.pop(key)
    reconciliation["report_sha256"] = _digest(
        {
            key: value
            for key, value in reconciliation.items()
            if key != "report_sha256"
        }
    )

    report = _evaluate(tmp_path, _status(), reconciliation)
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["broker_reconciliation_digest"] == "PASS"
    assert checks["broker_reconciliation_contract"] == "FAIL"
    assert checks["broker_local_active_order_parity"] == "FAIL"


def test_live_trust_fails_when_reconciliation_child_is_degraded(tmp_path: Path) -> None:
    status = _status()
    status["state"] = "DEGRADED"
    status["children"]["broker_reconciliation"].update(
        state="DEGRADED",
        broker_truth_available=False,
        error_type="AuthenticationException",
    )

    report = _evaluate(tmp_path, status, _reconciliation())
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["coordinator_state"] == "FAIL"
    assert checks["broker_reconciliation_child"] == "FAIL"


def test_live_trust_fails_when_reconciliation_is_missing(tmp_path: Path) -> None:
    _runtime_evidence(tmp_path)
    status_path = _write(tmp_path / "status.json", _status())
    report = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=status_path,
        reconciliation_path=tmp_path / "missing.json",
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=NOW,
    )
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["broker_reconciliation_artifact"] == "FAIL"
    assert checks["broker_truth"] == "FAIL"


def test_live_trust_fails_when_an_order_manager_is_stale_or_not_running(
    tmp_path: Path,
) -> None:
    status = _status()
    status["children"]["long"]["updated_at_ist"] = (
        NOW - timedelta(minutes=2)
    ).isoformat()
    status["children"]["short"]["state"] = "DEGRADED"

    report = _evaluate(tmp_path, status, _reconciliation())
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["order_manager_children"] == "FAIL"


def test_live_trust_fails_on_stale_pipeline_or_incomplete_market_data(tmp_path: Path) -> None:
    _runtime_evidence(tmp_path)
    scanner = tmp_path / "runtime_status" / "fno_v13_v10_g_scanner_5min.heartbeat"
    scanner.write_text(
        scanner.read_text(encoding="utf-8").replace(
            NOW.isoformat(), (NOW - timedelta(minutes=8)).isoformat()
        ),
        encoding="utf-8",
    )
    cash_path = tmp_path / "slot_ready_5m" / "slot_20260925_1000.json"
    cash = json.loads(cash_path.read_text(encoding="utf-8"))
    cash["fno_equity_failed"] = 1
    cash_path.write_text(json.dumps(cash), encoding="utf-8")

    report = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", _status()),
        reconciliation_path=_write(tmp_path / "broker.json", _reconciliation()),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=NOW + timedelta(minutes=4),
    )
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["scanner_heartbeat_freshness"] == "FAIL"
    assert checks["cash_5m_marker_contract"] == "FAIL"


def test_slot_driven_scanner_accepts_latest_completed_slot_between_windows(
    tmp_path: Path,
) -> None:
    observed = datetime(2026, 9, 25, 10, 12, tzinfo=IST)
    _runtime_evidence(tmp_path)
    scanner = tmp_path / "runtime_status" / "fno_v13_v10_g_scanner_5min.heartbeat"
    scanner.write_text(
        scanner.read_text(encoding="utf-8")
        .replace(f"ts={NOW.isoformat()}", "ts=2026-09-25T10:01:20+05:30")
        .replace("role=scanner-5m\n", "role=scanner-5m\nphase=SLOT_DONE\nslot=10:00\n")
        .replace("state=WAITING", "state=SUCCESS"),
        encoding="utf-8",
    )
    status = _status(updated_at=observed - timedelta(seconds=2))
    reconciliation = _reconciliation(generated_at=observed - timedelta(seconds=5))
    status["children"]["broker_reconciliation"]["updated_at_ist"] = (
        observed - timedelta(seconds=5)
    ).isoformat()
    cash_path = tmp_path / "slot_ready_5m" / "slot_20260925_1000.json"
    cash = json.loads(cash_path.read_text(encoding="utf-8"))
    cash["published_at_ist"] = (observed - timedelta(seconds=10)).isoformat()
    cash_path.write_text(json.dumps(cash), encoding="utf-8")
    futures_path = tmp_path / "fno_oi" / "slot_ready" / "slot_20260925_1000.json"
    futures = json.loads(futures_path.read_text(encoding="utf-8"))
    futures["published_at_ist"] = (observed - timedelta(seconds=8)).isoformat()
    futures_path.write_text(json.dumps(futures), encoding="utf-8")
    for session in (
        "fno_v13_v10_g_confirmation_1min",
        "fno_v13_v10_g_equity_1min_feed",
    ):
        path = tmp_path / "runtime_status" / f"{session}.heartbeat"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                NOW.isoformat(), observed.isoformat()
            ),
            encoding="utf-8",
        )

    report = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    checks = {check.name: check for check in report.checks}

    assert checks["scanner_heartbeat_freshness"].state == "PASS"
    assert "latest due scanner slot 10:00 is complete" in checks[
        "scanner_heartbeat_freshness"
    ].detail


def test_continuous_pipeline_heartbeats_use_two_minute_limit(tmp_path: Path) -> None:
    _runtime_evidence(tmp_path)
    path = tmp_path / "runtime_status" / "fno_v13_v10_g_equity_1min_feed.heartbeat"
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            NOW.isoformat(), (NOW - timedelta(seconds=121)).isoformat()
        ),
        encoding="utf-8",
    )

    report = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", _status()),
        reconciliation_path=_write(tmp_path / "broker.json", _reconciliation()),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=NOW,
    )
    checks = {check.name: check.state for check in report.checks}

    assert report.state == "FAIL"
    assert checks["equity_1m_feed_heartbeat_freshness"] == "FAIL"


def test_scanner_done_state_is_valid_after_final_frozen_slot(tmp_path: Path) -> None:
    observed = datetime(2026, 9, 25, 11, 21, tzinfo=IST)
    _runtime_evidence(tmp_path)
    scanner = tmp_path / "runtime_status" / "fno_v13_v10_g_scanner_5min.heartbeat"
    scanner.write_text(
        scanner.read_text(encoding="utf-8")
        .replace(f"ts={NOW.isoformat()}", f"ts={observed.isoformat()}")
        .replace("role=scanner-5m\n", "role=scanner-5m\nphase=ALL_V6_WINDOWS_DONE\nslot=11:20\n")
        .replace("state=WAITING", "state=DONE"),
        encoding="utf-8",
    )
    status = _status(updated_at=observed - timedelta(seconds=2))
    reconciliation = _reconciliation(generated_at=observed - timedelta(seconds=5))
    status["children"]["broker_reconciliation"]["updated_at_ist"] = (
        observed - timedelta(seconds=5)
    ).isoformat()

    report = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    checks = {check.name: check for check in report.checks}

    assert checks["scanner_heartbeat_contract"].state == "PASS"
    assert checks["scanner_heartbeat_freshness"].state == "PASS"
    assert "all frozen scanner slots are complete" in checks[
        "scanner_heartbeat_freshness"
    ].detail


def test_confirmation_done_requires_complete_terminal_proof(tmp_path: Path) -> None:
    _runtime_evidence(tmp_path)
    path = tmp_path / "runtime_status" / "fno_v13_v10_g_confirmation_1min.heartbeat"
    terminal = (
        path.read_text(encoding="utf-8")
        .replace("state=WAITING", "state=DONE")
        .replace(
            "role=confirmation-1m\n",
            "role=confirmation-1m\nphase=ALL_V6_WINDOWS_DONE\nprocessed_slots=9\n",
        )
        .replace(f"ts={NOW.isoformat()}", "ts=2026-09-25T11:21:05+05:30")
    )
    path.write_text(terminal, encoding="utf-8")
    observed = datetime(2026, 9, 25, 12, 0, tzinfo=IST)
    status = _status(updated_at=observed - timedelta(seconds=2))
    reconciliation = _reconciliation(generated_at=observed - timedelta(seconds=5))
    status["children"]["broker_reconciliation"]["updated_at_ist"] = (
        observed - timedelta(seconds=5)
    ).isoformat()

    complete = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    complete_checks = {check.name: check for check in complete.checks}
    assert complete_checks["confirmation_heartbeat_contract"].state == "PASS"
    assert complete_checks["confirmation_heartbeat_freshness"].state == "PASS"
    assert "all frozen confirmation slots are complete" in complete_checks[
        "confirmation_heartbeat_freshness"
    ].detail

    path.write_text(terminal.replace("processed_slots=9", "processed_slots=8"), encoding="utf-8")
    incomplete = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    incomplete_checks = {check.name: check.state for check in incomplete.checks}
    assert incomplete_checks["confirmation_heartbeat_contract"] == "FAIL"
    assert incomplete_checks["confirmation_heartbeat_freshness"] == "FAIL"


def test_equity_feed_done_requires_all_frozen_slots(tmp_path: Path) -> None:
    _runtime_evidence(tmp_path)
    path = tmp_path / "runtime_status" / "fno_v13_v10_g_equity_1min_feed.heartbeat"
    terminal = (
        path.read_text(encoding="utf-8")
        .replace("state=WAITING", "state=DONE")
        .replace(f"ts={NOW.isoformat()}", "ts=2026-09-25T11:21:07+05:30")
        + "processed_slots=9\n"
    )
    path.write_text(terminal, encoding="utf-8")
    observed = datetime(2026, 9, 25, 15, 12, tzinfo=IST)
    status = _status(updated_at=observed - timedelta(seconds=2))
    reconciliation = _reconciliation(generated_at=observed - timedelta(seconds=5))
    status["children"]["broker_reconciliation"]["updated_at_ist"] = (
        observed - timedelta(seconds=5)
    ).isoformat()

    complete = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    complete_checks = {check.name: check for check in complete.checks}
    assert complete_checks["equity_1m_feed_heartbeat_contract"].state == "PASS"
    assert complete_checks["equity_1m_feed_heartbeat_freshness"].state == "PASS"
    assert "all frozen equity-feed slots are complete" in complete_checks[
        "equity_1m_feed_heartbeat_freshness"
    ].detail

    path.write_text(terminal.replace("processed_slots=9", "processed_slots=8"), encoding="utf-8")
    incomplete = evaluate_live_trust(
        runtime_root=tmp_path,
        status_path=_write(tmp_path / "status.json", status),
        reconciliation_path=_write(tmp_path / "broker.json", reconciliation),
        expected_session_date=DAY,
        expected_strategy_version=VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        observed_at=observed,
    )
    incomplete_checks = {check.name: check.state for check in incomplete.checks}
    assert incomplete_checks["equity_1m_feed_heartbeat_contract"] == "FAIL"
    assert incomplete_checks["equity_1m_feed_heartbeat_freshness"] == "FAIL"


def test_live_trust_rejects_naive_or_future_timestamps(tmp_path: Path) -> None:
    naive = _evaluate(
        tmp_path / "naive",
        _status(updated_at=NOW).copy() | {"updated_at_ist": "2026-09-25T10:00:00"},
        _reconciliation(),
    )
    future = _evaluate(
        tmp_path / "future",
        _status(updated_at=NOW + timedelta(seconds=30)),
        _reconciliation(),
    )

    naive_checks = {check.name: check.state for check in naive.checks}
    future_checks = {check.name: check.state for check in future.checks}
    assert naive_checks["coordinator_freshness"] == "FAIL"
    assert future_checks["coordinator_freshness"] == "FAIL"
