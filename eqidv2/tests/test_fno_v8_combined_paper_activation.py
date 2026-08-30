from __future__ import annotations

import ast
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

import fno_v8_combined_paper_config as config
import fno_v8_combined_paper_control as control


SESSION = date(2026, 8, 21)
APPROVED_AT = datetime(2026, 8, 21, 8, 30, tzinfo=config.IST)
RUN_AT = datetime(2026, 8, 21, 9, 15, tzinfo=config.IST)
BUNDLE = "a" * 64


def _paths(tmp_path: Path) -> control.ControlPaths:
    root = tmp_path / "v8-control"
    return control.ControlPaths(
        activation_path=root / "activation.json",
        kill_switch_path=root / "kill_switch.json",
        permit_archive_root=root / "permits",
        event_archive_root=root / "events",
    )


def _approve(
    tmp_path: Path,
    *,
    disarm: bool = False,
) -> tuple[control.ControlPaths, dict[str, object]]:
    paths = _paths(tmp_path)
    approved = control.approve_session(
        SESSION,
        approved_by="test-operator",
        reason="one prospective PAPER session",
        approval_phrase=control.APPROVAL_PHRASE,
        now=APPROVED_AT,
        paths=paths,
        runtime_bundle_digest=BUNDLE,
    )
    if disarm:
        permit = approved["permit"]
        assert isinstance(permit, dict)
        control.disarm_kill_switch(
            SESSION,
            permit_id=str(permit["permit_id"]),
            actor="test-risk-operator",
            reason="explicit second-key PAPER arm",
            now=APPROVED_AT + timedelta(minutes=1),
            paths=paths,
            runtime_bundle_digest=BUNDLE,
        )
    return paths, approved


def _decision(
    paths: control.ControlPaths,
    *,
    day: date = SESSION,
    now: datetime = RUN_AT,
    bundle: str = BUNDLE,
) -> control.ActivationDecision:
    return control.evaluate_activation(
        day,
        now=now,
        paths=paths,
        expected_runtime_bundle_sha256=bundle,
    )


def test_missing_control_files_fail_closed_before_credentials(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    called = False

    def discoverer() -> list[object]:
        nonlocal called
        called = True
        return [object()] * 8

    with pytest.raises(control.ActivationBlockedError, match="ACTIVATION_POINTER_MISSING"):
        control.discover_credentials_after_activation(
            discoverer,
            SESSION,
            now=RUN_AT,
            paths=paths,
            expected_runtime_bundle_sha256=BUNDLE,
        )
    assert called is False


def test_approval_requires_exact_phrase_and_writes_nothing_on_mismatch(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(control.ControlCommandError, match="exact PAPER approval phrase"):
        control.approve_session(
            SESSION,
            approved_by="operator",
            reason="test",
            approval_phrase=control.APPROVAL_PHRASE.lower(),
            now=APPROVED_AT,
            paths=paths,
            runtime_bundle_digest=BUNDLE,
        )
    assert not paths.activation_path.exists()
    assert not paths.kill_switch_path.exists()
    assert not paths.permit_archive_root.exists()


def test_approval_archives_hash_bound_permit_but_does_not_arm(tmp_path: Path) -> None:
    paths, result = _approve(tmp_path)
    pointer = json.loads(paths.activation_path.read_text(encoding="utf-8"))
    kill = json.loads(paths.kill_switch_path.read_text(encoding="utf-8"))
    archive = Path(result["permit_archive_path"])
    assert archive.is_file()
    assert pointer["permit_archive_name"] == archive.name
    assert pointer["permit_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert pointer["mode"] == "PAPER"
    assert kill["engaged"] is True
    assert kill["permit_sha256"] == pointer["permit_sha256"]
    assert _decision(paths).allowed is False
    assert _decision(paths).reason == "KILL_SWITCH_ENGAGED"
    assert len(list(paths.event_archive_root.rglob("*.json"))) == 1


def test_valid_one_session_permit_and_disarmed_kill_switch_allow_paper(
    tmp_path: Path,
) -> None:
    paths, approved = _approve(tmp_path, disarm=True)
    decision = _decision(paths)
    permit = approved["permit"]
    assert isinstance(permit, dict)
    assert decision.allowed is True
    assert decision.reason == "PAPER_ACTIVATION_VALID"
    assert decision.permit_id == permit["permit_id"]
    assert decision.runtime_bundle_sha256 == BUNDLE
    assert decision.strategy_fingerprint == config.strategy_fingerprint()


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        (lambda payload: payload.update(mode="LIVE"), "ACTIVATION_MODE_MISMATCH"),
        (lambda payload: payload.update(session_date="2026-08-22"), "ACTIVATION_SESSION_DATE_MISMATCH"),
        (lambda payload: payload.update(permit_sha256="0" * 64), "ACTIVATION_PERMIT_ARCHIVE_HASH_MISMATCH"),
        (lambda payload: payload.update(permit_archive_name="../permit.json"), "ACTIVATION_ARCHIVE_NAME_INVALID"),
        (lambda payload: payload.update(enabled=False), "ACTIVATION_DISABLED_OR_REVOKED"),
    ),
)
def test_activation_pointer_tampering_fails_closed(
    tmp_path: Path,
    mutation,
    expected_reason: str,
) -> None:
    paths, _ = _approve(tmp_path, disarm=True)
    pointer = json.loads(paths.activation_path.read_text(encoding="utf-8"))
    mutation(pointer)
    paths.activation_path.write_text(json.dumps(pointer), encoding="utf-8")
    decision = _decision(paths)
    assert decision.allowed is False
    assert decision.reason == expected_reason


def test_permit_archive_tampering_fails_closed(tmp_path: Path) -> None:
    paths, approved = _approve(tmp_path, disarm=True)
    archive = Path(approved["permit_archive_path"])
    payload = json.loads(archive.read_text(encoding="utf-8"))
    payload["strategy_fingerprint"] = "0" * 64
    archive.write_text(json.dumps(payload), encoding="utf-8")
    decision = _decision(paths)
    assert decision.allowed is False
    assert decision.reason == "ACTIVATION_PERMIT_ARCHIVE_HASH_MISMATCH"


def test_runtime_bundle_and_strategy_drift_invalidate_permit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _approve(tmp_path, disarm=True)
    wrong_bundle = _decision(paths, bundle="b" * 64)
    assert wrong_bundle.allowed is False
    assert wrong_bundle.reason == "PERMIT_RUNTIME_BUNDLE_MISMATCH"

    monkeypatch.setattr(config, "BAR_SOURCE_POLICY", "DRIFTED_AFTER_APPROVAL")
    strategy_drift = _decision(paths)
    assert strategy_drift.allowed is False
    assert strategy_drift.reason == "PERMIT_STRATEGY_FINGERPRINT_MISMATCH"


def test_missing_malformed_or_reengaged_kill_switch_fails_closed(
    tmp_path: Path,
) -> None:
    paths, _ = _approve(tmp_path, disarm=True)
    paths.kill_switch_path.unlink()
    assert _decision(paths).reason == "KILL_SWITCH_MISSING_FAIL_CLOSED"

    paths.kill_switch_path.write_text("not-json", encoding="utf-8")
    assert _decision(paths).reason == "KILL_SWITCH_INVALID_FAIL_CLOSED"

    control.engage_kill_switch(
        SESSION,
        actor="risk",
        reason="manual stop",
        now=RUN_AT,
        paths=paths,
        runtime_bundle_digest=BUNDLE,
    )
    assert _decision(paths).reason == "KILL_SWITCH_ENGAGED"


def test_emergency_kill_still_engages_when_runtime_bundle_is_unreadable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _approve(tmp_path, disarm=True)

    def fail_bundle(*args, **kwargs):
        raise OSError("runtime source locked")

    monkeypatch.setattr(control, "runtime_bundle_sha256", fail_bundle)
    result = control.engage_kill_switch(
        SESSION,
        actor="risk",
        reason="emergency stop",
        now=RUN_AT,
        paths=paths,
    )
    assert result["kill_switch"]["engaged"] is True
    assert result["kill_switch"]["runtime_bundle_sha256"] == "0" * 64
    assert _decision(paths).reason == "KILL_SWITCH_ENGAGED"


def test_permit_is_valid_for_exactly_one_session_and_expires(tmp_path: Path) -> None:
    paths, _ = _approve(tmp_path, disarm=True)
    wrong_day = _decision(
        paths,
        day=date(2026, 8, 22),
        now=datetime(2026, 8, 22, 9, 15, tzinfo=config.IST),
    )
    assert wrong_day.allowed is False
    assert wrong_day.reason == "ACTIVATION_SESSION_DATE_MISMATCH"

    expired = _decision(
        paths,
        now=datetime(2026, 8, 21, 15, 35, tzinfo=config.IST),
    )
    assert expired.allowed is False
    assert expired.reason == "PERMIT_EXPIRED"


def test_disarm_requires_exact_active_permit_id(tmp_path: Path) -> None:
    paths, _ = _approve(tmp_path)
    with pytest.raises(control.ControlCommandError, match="permit_id does not match"):
        control.disarm_kill_switch(
            SESSION,
            permit_id="wrong-permit",
            actor="risk",
            reason="test",
            now=RUN_AT,
            paths=paths,
            runtime_bundle_digest=BUNDLE,
        )
    assert _decision(paths).reason == "KILL_SWITCH_ENGAGED"


def test_disarm_audit_failure_leaves_kill_switch_engaged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, approved = _approve(tmp_path)
    permit = approved["permit"]
    assert isinstance(permit, dict)

    def fail_archive(*args, **kwargs):
        raise OSError("audit disk unavailable")

    monkeypatch.setattr(control, "_archive_control_event", fail_archive)
    with pytest.raises(OSError, match="audit disk unavailable"):
        control.disarm_kill_switch(
            SESSION,
            permit_id=str(permit["permit_id"]),
            actor="risk",
            reason="test",
            now=RUN_AT,
            paths=paths,
            runtime_bundle_digest=BUNDLE,
        )
    kill = json.loads(paths.kill_switch_path.read_text(encoding="utf-8"))
    assert kill["engaged"] is True
    assert _decision(paths).reason == "KILL_SWITCH_ENGAGED"


def test_revoke_engages_kill_first_blocks_and_preserves_permit_archive(
    tmp_path: Path,
) -> None:
    paths, approved = _approve(tmp_path, disarm=True)
    archive = Path(approved["permit_archive_path"])
    before = archive.read_bytes()
    result = control.revoke_session(
        SESSION,
        actor="risk",
        reason="rollback",
        now=RUN_AT,
        paths=paths,
        runtime_bundle_digest=BUNDLE,
    )
    assert result["kill_switch"]["engaged"] is True
    assert result["activation_pointer"]["enabled"] is False
    assert _decision(paths).reason == "ACTIVATION_DISABLED_OR_REVOKED"
    assert archive.read_bytes() == before
    assert len(list(paths.event_archive_root.rglob("*.json"))) == 4


def test_credentials_are_discovered_only_after_gate_and_exactly_eight_required(
    tmp_path: Path,
) -> None:
    paths, _ = _approve(tmp_path, disarm=True)
    decision, credentials = control.discover_credentials_after_activation(
        lambda: tuple(f"app-{index}" for index in range(8)),
        SESSION,
        now=RUN_AT,
        paths=paths,
        expected_runtime_bundle_sha256=BUNDLE,
    )
    assert decision.allowed is True
    assert len(credentials) == 8

    with pytest.raises(control.ActivationBlockedError, match="KITE_APP_COUNT_MISMATCH:7/8"):
        control.discover_credentials_after_activation(
            lambda: tuple(f"app-{index}" for index in range(7)),
            SESSION,
            now=RUN_AT,
            paths=paths,
            expected_runtime_bundle_sha256=BUNDLE,
        )


def test_control_source_has_no_task_start_enable_or_broker_order_capability() -> None:
    source_path = Path(control.__file__)
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert "subprocess" not in imports
    assert "kiteconnect" not in imports
    assert not any(
        name.startswith(
            (
                "fno_v5",
                "fno_v6",
                "fno_v7",
                "fno_oi_ema_confirm",
                "fno_v8_combined_best_per_leg_backtest",
                "fno_v8_windowed_1m_entry_backtest",
                "fno_v8_windowed_1m_entry_optimize",
                "fno_v8_setup_param_sweep",
            )
        )
        for name in imports
    )
    assert "schtasks" not in source.lower()
    assert "place_order" not in source
    assert "modify_order" not in source
    assert "cancel_order" not in source
    assert "enable-task" not in source.lower()
    parser = control.build_parser()
    help_text = parser.format_help().lower()
    assert "live" not in help_text


def test_runtime_bundle_contract_includes_engine_market_data_and_session() -> None:
    assert control.RUNTIME_BUNDLE_FILENAMES == (
        "fno_v8_combined_paper_config.py",
        "fno_v8_combined_paper_control.py",
        "fno_v8_combined_paper_engine.py",
        "fno_v8_combined_paper_market_data.py",
        "fno_v8_combined_paper_session.py",
        "eqidv2_runtime_paths.py",
        "fno_oi_common.py",
        "fno_oi_hybrid_data.py",
        "fno_v5_live.py",
        "fno_v6_live.py",
        "fno_v6_live_config.py",
        "fno_oi_ema_confirm_backtest.py",
        "bat/run_fno_v6_scanner_5min.bat",
        "preopen_session_healthcheck.py",
        "preopen_session_autofix.py",
        "log_dashboard_server.py",
        "bat/switch_fno_v6_1m_to_v8_paper_after_approval.ps1",
        "bat/restore_fno_v6_1m_after_v8_paper.ps1",
        "bat/run_fno_v8_combined_paper_session.bat",
    )
    records = control.runtime_bundle_records()
    runner = next(
        row
        for row in records
        if row["relative_path"] == "bat/run_fno_v8_combined_paper_session.bat"
    )
    assert runner["name"] == "run_fno_v8_combined_paper_session.bat"
    assert len(str(runner["sha256"])) == 64


def test_runtime_bundle_nested_runner_is_mandatory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        control,
        "RUNTIME_BUNDLE_FILENAMES",
        ("bat/definitely_missing_v8_runner.bat",),
    )
    with pytest.raises(control.ControlCommandError, match="missing runtime source"):
        control.runtime_bundle_records()


def test_dashboard_runtime_identity_binds_loaded_source_pid_and_start(
    tmp_path: Path,
) -> None:
    source_path = Path(control.__file__).resolve().parent / "log_dashboard_server.py"
    started = datetime(2026, 8, 21, 8, 55, 1, tzinfo=config.IST)
    identity_path = tmp_path / "log_dashboard_server.runtime.json"
    payload = {
        "schema_version": control.DASHBOARD_RUNTIME_IDENTITY_SCHEMA_VERSION,
        "pid": 4242,
        "source_path": str(source_path),
        "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "started_at_utc": started.astimezone().isoformat(),
        "started_at_ist": started.isoformat(),
        "heartbeat_at_utc": (started + timedelta(seconds=5)).isoformat(),
        "heartbeat_at_ist": (started + timedelta(seconds=5)).isoformat(),
        "host": "127.0.0.1",
        "port": 8787,
    }
    identity_path.write_text(json.dumps(payload), encoding="utf-8")

    observed = control.require_dashboard_runtime_identity(
        identity_path=identity_path,
        observed_now=started + timedelta(seconds=5),
    )
    assert observed["pid"] == 4242
    assert observed["source_sha256"] == payload["source_sha256"]
    assert observed["heartbeat_age_seconds"] == pytest.approx(0.0)

    payload["host"] = "0.0.0.0"
    identity_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        control.ActivationBlockedError,
        match="DASHBOARD_RUNTIME_HOST_MISMATCH",
    ):
        control.require_dashboard_runtime_identity(
            identity_path=identity_path,
            observed_now=started + timedelta(seconds=5),
        )

    payload["host"] = "127.0.0.1"
    payload["port"] = 8788
    identity_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        control.ActivationBlockedError,
        match="DASHBOARD_RUNTIME_PORT_MISMATCH",
    ):
        control.require_dashboard_runtime_identity(
            identity_path=identity_path,
            observed_now=started + timedelta(seconds=5),
        )

    payload["port"] = 8787
    payload["source_sha256"] = "0" * 64
    identity_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        control.ActivationBlockedError,
        match="DASHBOARD_RUNTIME_SOURCE_SHA256_MISMATCH",
    ):
        control.require_dashboard_runtime_identity(
            identity_path=identity_path,
            observed_now=started + timedelta(seconds=5),
        )


def test_dashboard_runtime_identity_rejects_stale_or_future_heartbeat(
    tmp_path: Path,
) -> None:
    source_path = Path(control.__file__).resolve().parent / "log_dashboard_server.py"
    started = datetime(2026, 8, 21, 8, 55, 1, tzinfo=config.IST)
    identity_path = tmp_path / "log_dashboard_server.runtime.json"
    identity_path.write_text(
        json.dumps(
            {
                "schema_version": control.DASHBOARD_RUNTIME_IDENTITY_SCHEMA_VERSION,
                "pid": 4242,
                "source_path": str(source_path),
                    "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
                    "started_at_utc": started.isoformat(),
                    "heartbeat_at_utc": (started + timedelta(seconds=5)).isoformat(),
                    "host": "127.0.0.1",
                    "port": 8787,
                }
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        control.ActivationBlockedError,
        match="DASHBOARD_RUNTIME_HEARTBEAT_STALE",
    ):
        control.require_dashboard_runtime_identity(
            identity_path=identity_path,
            observed_now=started + timedelta(minutes=2),
        )

    payload = json.loads(identity_path.read_text(encoding="utf-8"))
    payload["heartbeat_at_utc"] = (started + timedelta(minutes=2)).isoformat()
    identity_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(
        control.ActivationBlockedError,
        match="DASHBOARD_RUNTIME_HEARTBEAT_IN_FUTURE",
    ):
        control.require_dashboard_runtime_identity(
            identity_path=identity_path,
            observed_now=started,
        )
