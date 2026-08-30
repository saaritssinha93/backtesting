from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import log_dashboard_server as dashboard
import preopen_session_autofix as preopen_autofix
import preopen_session_healthcheck as preopen


ROOT = Path(__file__).resolve().parents[1]
CARD_ID = "fno_v8_combined_paper"
TASK_NAME = "EQIDV2_fno_v8_combined_paper_0915"
RUNNER_NAME = "run_fno_v8_combined_paper_session.bat"


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="strict")


def _javascript_set(source: str, name: str) -> set[str]:
    match = re.search(
        rf"const\s+{re.escape(name)}\s*=\s*new Set\(\[(.*?)\]\);",
        source,
        re.DOTALL,
    )
    assert match is not None, f"JavaScript set not found: {name}"
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def test_dashboard_maps_staged_v8_card_to_isolated_ops_artifacts() -> None:
    assert dashboard.FNO_OI_CARD_REPORTS[CARD_ID] == "latest_fno_v8_combined_paper.md"
    assert dashboard.LOG_FILES[CARD_ID] == "fno_v8_combined_paper.log"
    assert dashboard.STATUS_FILES[CARD_ID] == "fno_v8_combined_paper.status"
    assert dashboard.HEARTBEAT_FILES[CARD_ID] == "fno_v8_combined_paper.heartbeat"
    assert dashboard.CARD_TASK_NAMES[CARD_ID] == (f"\\{TASK_NAME}",)


def test_v8_card_is_visible_in_fno_but_has_no_restart_or_autofix_path() -> None:
    source = _source(ROOT / "log_dashboard_server.py")
    fno_group = source[source.index('key: "fno"') : source.index('key: "forensic-positional"')]

    assert CARD_ID in fno_group
    assert CARD_ID in _javascript_set(source, "SECTION_LOCKED_DISABLED_IDS")
    assert CARD_ID in _javascript_set(source, "MD_REPORT_CARDS")
    assert CARD_ID not in dashboard.RESTARTABLE_CARDS
    assert CARD_ID not in _javascript_set(source, "RESTARTABLE_CARDS")
    assert TASK_NAME not in preopen.REQUIRED_DASHBOARD_SESSION_TASKS
    assert TASK_NAME not in preopen.DASHBOARD_SESSION_TASKS
    assert TASK_NAME not in preopen_autofix.TASK_TO_BAT
    assert CARD_ID not in preopen_autofix.FAIL_CHECK_TO_BAT
    assert all(
        path.name != RUNNER_NAME
        for path in (
            *preopen_autofix.TASK_TO_BAT.values(),
            *preopen_autofix.FAIL_CHECK_TO_BAT.values(),
        )
    )


def test_dashboard_refuses_disabled_v6_restart_before_identity_or_pid_actions() -> None:
    with (
        patch(
            "log_dashboard_server._fresh_task_restart_eligibility",
            return_value=(False, "Scheduled task is not enabled (state=DISABLED)."),
        ) as eligibility,
        patch("log_dashboard_server._read_restart_identity") as read_identity,
        patch("log_dashboard_server._run_cmd_silent") as run_command,
        patch("log_dashboard_server._collect_restart_candidate_pids") as collect_pids,
        patch("log_dashboard_server._kill_pid_tree") as kill_tree,
    ):
        result = dashboard._restart_card_session("fno_v6_live_long")

    assert result["ok"] is False
    assert "state=DISABLED" in result["message"]
    eligibility.assert_called_once_with("\\EQIDV2_fno_v6_live_long_0920")
    read_identity.assert_not_called()
    run_command.assert_not_called()
    collect_pids.assert_not_called()
    kill_tree.assert_not_called()


def test_dashboard_fresh_restart_probe_requires_explicit_enabled_state() -> None:
    enabled_output = (
        "TaskName: \\EQIDV2_fno_v6_live_long_0920\n"
        "Scheduled Task State: Enabled\n"
        "Status: Ready\n"
    )
    disabled_output = enabled_output.replace("Enabled", "Disabled")

    with patch(
        "log_dashboard_server.subprocess.run",
        return_value=SimpleNamespace(returncode=0, stdout=enabled_output, stderr=""),
    ):
        allowed, reason = dashboard._fresh_task_restart_eligibility(
            "\\EQIDV2_fno_v6_live_long_0920"
        )
    assert allowed is True
    assert reason == "ENABLED"

    with patch(
        "log_dashboard_server.subprocess.run",
        return_value=SimpleNamespace(returncode=0, stdout=disabled_output, stderr=""),
    ):
        allowed, reason = dashboard._fresh_task_restart_eligibility(
            "\\EQIDV2_fno_v6_live_long_0920"
        )
    assert allowed is False
    assert "state=DISABLED" in reason

    with patch(
        "log_dashboard_server.subprocess.run",
        return_value=SimpleNamespace(returncode=1, stdout="", stderr="query failed"),
    ):
        allowed, reason = dashboard._fresh_task_restart_eligibility(
            "\\EQIDV2_fno_v6_live_long_0920"
        )
    assert allowed is False
    assert "query failed" in reason

    source = _source(ROOT / "log_dashboard_server.py")
    assert "DASHBOARD_RUNTIME_IDENTITY_PATH" in source
    assert '"heartbeat_at_utc"' in source
    assert "identity_stop.wait(5.0)" in source
    assert 'str(args.host) != "127.0.0.1"' in source
    assert "int(args.port) != 8787" in source


def test_runner_is_paper_only_fail_closed_and_uses_run_subcommand() -> None:
    source = _source(ROOT / "bat" / RUNNER_NAME)

    assert 'set "FNO_V8_COMBINED_EXECUTION_MODE=PAPER"' in source
    assert (
        'set "EQIDV2_DATA_5M_DIR=C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live"'
        in source
    )
    assert 'fno_v8_combined_paper_session.py' in source
    assert '"%PYTHON_EXE%" -u "%SESSION_SCRIPT%" run %*' in source
    assert 'fno_v8_combined_paper.log' in source
    assert 'if not exist "%SESSION_SCRIPT%"' in source
    assert "FNO_V8_COMBINED_EXECUTION_MODE=LIVE" not in source
    assert "fno_v6_live.py" not in source


def test_installer_registers_disabled_from_first_state_and_verifies() -> None:
    source = _source(ROOT / "bat" / "schedule_fno_v8_combined_paper_disabled.ps1")

    settings_at = source.index("New-ScheduledTaskSettingsSet")
    disabled_at = source.index("-Disable", settings_at)
    register_at = source.index("Register-ScheduledTask")
    verify_at = source.index("Get-ScheduledTask -TaskName $TaskName", register_at)

    assert settings_at < disabled_at < register_at < verify_at
    assert TASK_NAME in source
    assert RUNNER_NAME in source
    assert "Settings.Enabled" in source
    assert 'State -ne "Disabled"' in source
    assert 'State -eq "Running"' in source
    assert "harden_scheduled_task.ps1" not in source
    assert "-StartWhenAvailable" not in source
    assert 'The frozen V8 paper trigger must be exactly 09:15' in source
    assert "StartWhenAvailable must be false" in source
    assert "New-ScheduledTaskPrincipal" in source
    assert "principal must be Saarit/Interactive/Limited" in source
    assert "-DisallowDemandStart" in source
    assert "AllowDemandStart must be false" in source
    assert "-RestartCount 0" in source
    assert "automatic task restart must remain disabled" in source
    assert "-ErrorAction SilentlyContinue" not in source
    assert "CmdletizationQuery_NotFound_TaskName" in source
    assert "Could not safely determine whether the V8 task exists" in source
    assert 'FindSystemTimeZoneById("India Standard Time")' in source
    assert "[TimeZoneInfo]::ConvertTime($startBoundary, $indiaTimeZone)" in source
    assert "task action arguments must be empty" in source
    assert "task working directory must be empty" in source
    assert "expected exactly one task action" in source
    assert "sole 09:15 trigger must be enabled" in source
    assert "DaysOfWeek -ne 62" in source
    assert "WeeksInterval -ne 1" in source
    assert "trigger ExecutionTimeLimit must be empty or PT0S" in source
    assert "trigger must not repeat, delay, or have an end boundary" in source
    assert "schtasks.exe /Create" not in source
    assert "schtasks.exe /Change /TN $TaskName /Disable" in source
    assert "forced-disable attempted" in source
    assert "fno_v6_" not in source.lower()
    assert "schtasks.exe /Run" not in source


def test_approval_switch_has_exact_narrow_scope_and_no_start_command() -> None:
    source = _source(ROOT / "bat" / "switch_fno_v6_1m_to_v8_paper_after_approval.ps1")
    expected = {
        "EQIDV2_fno_v6_equity_1min_feed_0919",
        "EQIDV2_fno_v6_confirmation_1min_0919",
        "EQIDV2_fno_v6_live_long_0920",
        "EQIDV2_fno_v6_live_short_0920",
        "EQIDV2_fno_v6_trade_logger_0920",
        "EQIDV2_fno_v6_net_result_0920",
    }
    block = re.search(r"\$v6DownstreamTasks\s*=\s*@\((.*?)\n\)", source, re.DOTALL)

    assert block is not None
    assert set(re.findall(r'"(EQIDV2_fno_v6_[^"]+)"', block.group(1))) == expected
    assert "EQIDV2_fno_v6_scanner_5min_0918" not in block.group(1)
    assert "[switch]$Execute" in source
    assert 'I_APPROVE_FNO_V6_1M_TO_V8_COMBINED_PAPER' in source
    assert "Approval phrase mismatch; no scheduler state was changed." in source
    assert source.index("if (-not $Execute)") < source.index("$mutationsStarted = $true")
    assert source.index("Approval phrase mismatch") < source.index("$mutationsStarted = $true")
    assert "Cutover must begin and pass preflight before 09:13" in source
    assert source.index("Cutover must begin and pass preflight before 09:13") < source.index(
        "$mutationsStarted = $true"
    )
    assert "preflight --require-activation --authenticate-apps" in source
    assert source.index("preflight --require-activation --authenticate-apps") < source.index(
        "$mutationsStarted = $true"
    )
    assert "V8 activation/eight-app preflight failed; V6 remains enabled." in source
    assert "V8 preflight crossed the 09:13 cutover deadline" in source
    assert "V6 downstream state changed during preflight" in source
    assert source.count("Get-ManagedTask -TaskName $taskName") >= 2
    assert source.count("Assert-V8TaskDefinition -Task") >= 3
    assert "StartWhenAvailable must remain false" in source
    assert "V8 task principal must remain Saarit/Interactive/Limited" in source
    assert "V8 task AllowDemandStart must remain false" in source
    assert "V8 task automatic restart must remain disabled" in source
    assert 'FindSystemTimeZoneById("India Standard Time")' in source
    assert "[TimeZoneInfo]::ConvertTime($startBoundary, $indiaTimeZone)" in source
    assert "task action arguments must be empty" in source
    assert "task working directory must be empty" in source
    assert "09:15 trigger must be enabled" in source
    assert "trigger must be exactly 09:15" in source
    assert "trigger must be weekly Monday-Friday" in source
    assert "trigger ExecutionTimeLimit must be empty or PT0S" in source
    assert "V8 runner changed during preflight" in source
    assert "Disable-ScheduledTask -TaskName $TaskName" in source
    assert "Enable-ScheduledTask -TaskName $TaskName" in source
    assert "$originalEnabled" in source
    assert "original task states were restored" in source
    consumer_disable_at = source.index("foreach ($taskName in $v6ConsumerTasks)")
    v8_enable_at = source.index(
        "Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $true"
    )
    feed_disable_at = source.index(
        "Set-ManagedTaskEnabled -TaskName $v6FeedTaskName -Enabled $false"
    )
    assert consumer_disable_at < v8_enable_at < feed_disable_at
    assert "preopen autofix loop" in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks.exe /Run" not in source


def test_preopen_cutover_mode_never_autofixes_v6_feed_behind_v8() -> None:
    now = dt.datetime(2026, 8, 24, 9, 10, tzinfo=preopen.IST)
    allowed = SimpleNamespace(
        allowed=True,
        reason="PAPER_ACTIVATION_VALID",
        permit_id="permit-1",
    )
    blocked = SimpleNamespace(
        allowed=False,
        reason="ACTIVATION_SESSION_DATE_MISMATCH",
        permit_id="",
    )

    def clean_v8_mode(task_name: str):
        if task_name == preopen.FNO_V6_SCANNER_TASK:
            return True, True, False, "ENABLED", "READY"
        assert task_name in preopen.FNO_V6_CUTOVER_DOWNSTREAM_TASKS
        return True, False, False, "DISABLED", "READY"

    with patch(
        "fno_v8_combined_paper_control.evaluate_activation",
        return_value=allowed,
    ):
        result = preopen.check_v8_paper_cutover_activation(
            v8_task_enabled=True,
            observed_at=now,
            task_probe=clean_v8_mode,
            v8_task_state=(True, True, False, "ENABLED", "READY"),
        )
    assert result.status == "PASS"
    assert "permit-1" in result.detail

    with patch(
        "fno_v8_combined_paper_control.evaluate_activation",
        return_value=blocked,
    ):
        result = preopen.check_v8_paper_cutover_activation(
            v8_task_enabled=True,
            observed_at=now,
            task_probe=clean_v8_mode,
            v8_task_state=(True, True, False, "ENABLED", "READY"),
        )
    assert result.status == "FAIL"
    assert "ACTIVATION_SESSION_DATE_MISMATCH" in result.detail
    assert list(preopen_autofix._iter_actions_for_fail(result.name)) == []

    source = _source(ROOT / "preopen_session_healthcheck.py")
    assert "fno_v6_equity_1min_feed_autofix_suppressed" in source
    assert "the separate cutover-coherence check remains the fail-closed alert" in source
    assert "has no autofix mapping" in source

    for v8_positively_disabled in (False,):
        with patch(
            "preopen_session_healthcheck.check_task_enabled_state",
            side_effect=AssertionError("V6 feed task query must be suppressed"),
        ):
            suppressed = preopen.check_dashboard_session_task(
                task_name=preopen.FNO_V6_EQUITY_1MIN_FEED_TASK,
                observed_at=dt.datetime(2026, 8, 24, 9, 20, tzinfo=preopen.IST),
                v8_positively_disabled=v8_positively_disabled,
            )
        assert suppressed.status == "PASS"
        assert not suppressed.name.startswith("task_")
        assert list(preopen_autofix._iter_actions_for_fail(suppressed.name)) == []

    with patch(
        "preopen_session_healthcheck.check_task_enabled_state",
        return_value=preopen.CheckResult(
            f"task_{preopen.FNO_V6_EQUITY_1MIN_FEED_TASK}",
            "PASS",
            "normal V6 mode",
        ),
    ) as enabled_check:
        normal = preopen.check_dashboard_session_task(
            task_name=preopen.FNO_V6_EQUITY_1MIN_FEED_TASK,
            observed_at=dt.datetime(2026, 8, 24, 9, 20, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )
    assert normal.status == "PASS"
    enabled_check.assert_called_once()

    def overlapping_mode(task_name: str):
        if task_name == preopen.FNO_V6_SCANNER_TASK:
            return True, True, False, "ENABLED", "READY"
        if task_name == preopen.FNO_V6_EQUITY_1MIN_FEED_TASK:
            return True, True, True, "ENABLED", "RUNNING"
        return True, False, False, "DISABLED", "READY"

    result = preopen.check_v8_paper_cutover_activation(
        v8_task_enabled=True,
        observed_at=now,
        task_probe=overlapping_mode,
        v8_task_state=(True, True, False, "ENABLED", "READY"),
    )
    assert result.status == "FAIL"
    assert "not mutually exclusive" in result.detail
    assert list(preopen_autofix._iter_actions_for_fail(result.name)) == []

    def inactive_feed(_task_name: str):
        return True, False, False, "DISABLED", "READY"

    unknown = preopen.check_v8_paper_cutover_activation(
        v8_task_enabled=False,
        observed_at=now,
        task_probe=inactive_feed,
        v8_task_state=(False, False, False, "", ""),
    )
    assert unknown.status == "FAIL"
    assert "missing/unavailable/ambiguous" in unknown.detail
    assert list(preopen_autofix._iter_actions_for_fail(unknown.name)) == []

    def active_feed(_task_name: str):
        return True, True, False, "ENABLED", "READY"

    query_failure = preopen.check_v8_paper_cutover_activation(
        v8_task_enabled=False,
        observed_at=now,
        task_probe=active_feed,
        v8_task_state=(False, False, False, "", ""),
    )
    assert query_failure.status == "FAIL"
    assert "regardless of V6 feed state" in query_failure.detail


def test_preopen_reports_selected_v8_that_failed_to_stay_running(
    tmp_path: Path,
) -> None:
    before_grace = dt.datetime(2026, 8, 24, 9, 16, tzinfo=preopen.IST)
    after_grace = dt.datetime(2026, 8, 24, 9, 18, tzinfo=preopen.IST)
    enabled_not_running = (True, True, False, "ENABLED", "READY")
    running = (True, True, True, "ENABLED", "RUNNING")
    heartbeat = tmp_path / "fno_v8_combined_paper.heartbeat"

    result = preopen.check_v8_paper_runtime_liveness(
        v8_task_enabled=True,
        observed_at=before_grace,
        v8_task_state=enabled_not_running,
        heartbeat_path=heartbeat,
    )
    assert result.status == "PASS"
    assert "startup grace" in result.detail

    result = preopen.check_v8_paper_runtime_liveness(
        v8_task_enabled=True,
        observed_at=after_grace,
        v8_task_state=enabled_not_running,
        heartbeat_path=heartbeat,
    )
    assert result.status == "FAIL"
    assert "not running after grace" in result.detail
    assert list(preopen_autofix._iter_actions_for_fail(result.name)) == []

    heartbeat.write_text(
        "state=RUNNING\n"
        "ts=2026-08-24T09:17:30+05:30\n"
        "mode=PAPER\n",
        encoding="utf-8",
    )
    result = preopen.check_v8_paper_runtime_liveness(
        v8_task_enabled=True,
        observed_at=after_grace,
        v8_task_state=running,
        heartbeat_path=heartbeat,
    )
    assert result.status == "PASS"
    assert "heartbeat age=30.0s" in result.detail

    result = preopen.check_v8_paper_runtime_liveness(
        v8_task_enabled=True,
        observed_at=after_grace + dt.timedelta(minutes=5),
        v8_task_state=running,
        heartbeat_path=heartbeat,
    )
    assert result.status == "FAIL"
    assert "not a fresh same-session" in result.detail


def test_preopen_restore_disables_v8_before_enabling_exact_v6_scope() -> None:
    source = _source(ROOT / "bat" / "restore_fno_v6_1m_after_v8_paper.ps1")
    expected = {
        "EQIDV2_fno_v6_equity_1min_feed_0919",
        "EQIDV2_fno_v6_confirmation_1min_0919",
        "EQIDV2_fno_v6_live_long_0920",
        "EQIDV2_fno_v6_live_short_0920",
        "EQIDV2_fno_v6_trade_logger_0920",
        "EQIDV2_fno_v6_net_result_0920",
    }
    block = re.search(r"\$v6DownstreamTasks\s*=\s*@\((.*?)\n\)", source, re.DOTALL)

    assert block is not None
    assert set(re.findall(r'"(EQIDV2_fno_v6_[^"]+)"', block.group(1))) == expected
    assert "I_RESTORE_FNO_V6_1M_AND_DISABLE_V8_PAPER" in source
    assert "Restore must complete before 08:55" in source
    assert "StartWhenAvailable=True" in source
    assert "after 15:35" not in source
    disable_at = source.index(
        "Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $false"
    )
    enable_at = source.index(
        "Set-ManagedTaskEnabled -TaskName $taskName -Enabled $true"
    )
    assert disable_at < enable_at
    assert "Shared V6 scanner changed state unexpectedly" in source
    assert "original enabled states were restored" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks.exe /Run" not in source


def test_runbook_documents_disabled_stage_and_explicit_cutover_boundary() -> None:
    source = _source(ROOT / "FNO_V8_COMBINED_PAPER_LIVE_RUNBOOK.md")

    assert "registers the task non-runnable from its first observable state" in source
    assert "does\nnot call the common hardener" in source
    assert "I_APPROVE_FNO_V6_1M_TO_V8_COMBINED_PAPER" in source
    assert "I APPROVE ONE SESSION OF FNO V8-COMBINED PAPER ONLY" in source
    assert "preflight" in source and "--authenticate-apps" in source
    assert "not eight V8 strategy processes" in source
    assert "Do not launch a second writer" in source
    assert "Restart All" in source
    assert "before **09:13 IST**" in source
    assert "two-minute safety margin" in source
    assert "rechecks the clock and every V6/V8 task" in source
    assert "before **08:55 IST**" in source
    assert "run_log_dashboard_restart_keep_url.bat" in source
    assert "require_dashboard_runtime_identity" in source
    assert "Do not approve or" in source and "cut over V8 until this command succeeds" in source
