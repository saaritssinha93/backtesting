"""Offline contract tests for the recurring G3 standing-policy gate."""
from __future__ import annotations

import base64
from datetime import date, datetime, time, timedelta
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"


def source(name: str) -> str:
    return (BAT / name).read_text(encoding="utf-8")


def function_body(script: str, name: str, next_name: str) -> str:
    start = script.index(f"function {name}")
    end = script.index(f"function {next_name}", start)
    return script[start:end]


def test_recurring_schedule_starts_after_oct22_one_shot_and_is_exact_weekday_0910():
    script = source("schedule_g3_live_standing_weekday.ps1")

    for evidence in (
        "EQIDV2_g3_live_standing_approval_0910",
        "2026-10-23 09:10:00",
        "activate_g3_live_standing.ps1",
        "New-ScheduledTaskTrigger -Weekly -WeeksInterval 1",
        "-DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $FirstRun",
        "Monday|Tuesday|Wednesday|Thursday|Friday",
        "-LogonType Interactive",
        "-RunLevel Limited",
        "-MultipleInstances IgnoreNew",
        "-ExecutionTimeLimit (New-TimeSpan -Minutes 3)",
        "StartWhenAvailable -or",
        "RestartCount -ne 0",
        "ExecutionTimeLimit -ne 'PT3M'",
    ):
        assert evidence in script
    assert "New-ScheduledTaskTrigger -Once" not in script
    assert "Start-ScheduledTask" not in script
    assert "RestartOnFailure" in script  # XML verifier requires it to be absent.
    assert "$null -ne $restart" in script

    # Offline recurrence simulation: the Oct-22 one-shot and Oct-23 recurring
    # trigger cannot race, and weekends are skipped.
    first = datetime.combine(date(2026, 10, 23), time(9, 10))
    assert first.weekday() == 4
    observed = []
    cursor = first
    while len(observed) < 6:
        if cursor.weekday() < 5:
            observed.append(cursor)
        cursor += timedelta(days=1)
    assert observed[0] == datetime(2026, 10, 23, 9, 10)
    assert datetime(2026, 10, 22, 9, 10) not in observed
    assert all(row.weekday() < 5 and row.time() == time(9, 10) for row in observed)


def test_scheduler_action_is_exact_and_install_is_transactional_without_touching_target():
    script = source("schedule_g3_live_standing_weekday.ps1")

    for evidence in (
        "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File",
        "$Runner + '\"'",
        "-WorkingDirectory $Workspace",
        "prior_standing_task.xml",
        "reviewed_target_task.xml",
        "Register-ScheduledTask -TaskPath '\\' -TaskName $TaskName",
        "Register-ScheduledTask -TaskPath '\\' -TaskName $TaskName `\n                -Xml $existingXml -Force",
        "Unregister-ScheduledTask -TaskName $TaskName",
        "Partial standing task was not removed",
        "ROLLBACK FAILED",
        "Assert-InstalledRecurringGate",
        "Assert-ReviewedTargetForInstallation | Out-Null",
        "StartsTargetNow = $false",
        "MutatesTargetNow = $false",
    ):
        assert evidence in script
    assert "Enable-ScheduledTask -TaskName $TargetTask" not in script
    assert "Disable-ScheduledTask -TaskName $TargetTask" not in script
    assert "Start-ScheduledTask" not in script


def test_installer_and_gate_pin_the_existing_target_to_exact_weekly_launcher():
    activation = source("activate_g3_live_standing.ps1")
    scheduler = source("schedule_g3_live_standing_weekday.ps1")

    for script in (activation, scheduler):
        for evidence in (
            "EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915",
            "run_g3_approved_live.bat",
            "Monday|Tuesday|Wednesday|Thursday|Friday",
            "[timespan]::FromHours(9.25)",
            "t:Repetition",
            "t:EndBoundary",
            "'/d /c \"\"' + $expectedLauncher + '\"\"'",
            "StartWhenAvailable",
            "RestartCount",
            "MultipleInstances",
            "Interactive",
            "Limited",
        ):
            assert evidence in script


def test_gate_disables_first_and_enables_only_as_final_commit_without_starting_live():
    script = source("activate_g3_live_standing.ps1")
    main = script.index("$approvalTransactionStarted = $false")
    disable = script.index("Disable-G3TargetFailClosed", main)
    time_gate = script.index("$windowStart =", disable)
    policy_gate = script.index("Assert-StandingPolicyArtifact", time_gate)
    broker_probe = script.index(
        "& $Python $BrokerProbe --app all --minimum-live-balance 20000",
        policy_gate,
    )
    approval = script.index("& $Python $Operator approve-standing-session", broker_probe)
    approved_state = script.index("Assert-ApprovedStandingState", approval)
    enable = script.index("Enable-ScheduledTask -TaskName $TargetTask", approved_state)
    success = script.index("SUCCESS: standing policy approved", enable)

    assert disable < time_gate < policy_gate < broker_probe < approval
    assert approval < approved_state < enable < success
    assert script.count("Enable-ScheduledTask -TaskName $TargetTask") == 1
    assert "Start-ScheduledTask" not in script
    assert "--activate-live" not in script
    assert "approve_g3_live.ps1" not in script
    assert "Enabling it is the final commit only" in script


def test_gate_uses_exact_window_deadline_effective_date_and_global_mutex():
    script = source("activate_g3_live_standing.ps1")

    for evidence in (
        "2026-10-23",
        "$windowStart = $session.AddHours(9).AddMinutes(9)",
        "$deadline = $session.AddHours(9).AddMinutes(12).AddSeconds(30)",
        "$now -lt $windowStart -or $now -ge $deadline",
        "09:09:00 through 09:12:29 IST",
        "missed the 09:12:30 IST enable deadline",
        "Global\\EQIDV2_G3_LIVE_STANDING_ACTIVATION",
        ".WaitOne(0)",
        "Another G3 standing activation gate is running; target disabled fail-closed",
    ):
        assert evidence in script


def test_clock_gate_uses_read_only_kite_https_date_and_never_changes_system_time():
    script = source("activate_g3_live_standing.ps1")
    clock = function_body(script, "Assert-HttpsClockSkew", "Assert-NoG3LiveWorkers")

    for evidence in (
        "https://api.kite.trade/",
        "System.Net.Http.HttpMethod]::Head",
        "$response.Headers.Date",
        "$ClockMaxSkewSeconds = 30",
        "$midpoint",
        "$skew -gt $ClockMaxSkewSeconds",
        "Date header",
    ):
        assert evidence in script or evidence in clock
    for forbidden in ("w32tm", "Set-Date", "resync", "config /update"):
        assert forbidden.lower() not in script.lower()


def test_auth_health_kills_and_worker_checks_are_strict_and_rechecked():
    script = source("activate_g3_live_standing.ps1")

    for evidence in (
        "Read-StrictLiteralKeyValueFile",
        "Refresh-PreopenHealth",
        "preopen_session_healthcheck.py",
        "--max-age-min 35 --report-path $HealthRefreshLog",
        "'status', 'script', 'ts', 'exit_code', 'log_file'",
        "$age -gt $AuthMaxAgeSeconds",
        "$AuthMaxAgeSeconds = 5400",
        "$health.overall -ne 'WAIT'",
        "$HealthMaxAgeSeconds = 120",
        "$warned.Count -ne 1",
        "fno_fast_production_first_slot",
        "only the expected pre-09:20 warning",
        'target\'s existing "session not enabled" detail',
        "Assert-AllKillSwitchesClear",
        "v13_v10_g_3_live",
        "v6_live",
        "v13_v10_g_live",
        "control",
        "Assert-NoG3LiveWorkers",
        "fno_v13_v10_g_3_live_kite_session.py",
        "fno_v13_v10_g_3_live.py",
        "supervise_command.ps1",
    ):
        assert evidence in script
    assert script.count("Assert-AllKillSwitchesClear") >= 3
    assert script.count("Assert-NoG3LiveWorkers") >= 3
    assert "ConvertFrom-StringData" not in script
    assert script.index("Refresh-PreopenHealth", script.index("$approvalTransactionStarted")) < script.index(
        "Assert-PreopenHealth (Get-Date)", script.index("$approvalTransactionStarted")
    )


def test_only_expected_wait_still_accepts_disabled_target_pass_detail():
    script = source("activate_g3_live_standing.ps1")
    health = function_body(
        script, "Assert-PreopenHealth", "Assert-FreshBrokerFlatReadReconciliation"
    )

    assert "$health.overall -ne 'WAIT'" in health
    assert "[string]$match[0].status -ne 'PASS'" in health
    assert "[string]$match[0].detail" not in health
    assert '"session not enabled" detail is the expected recovery' in health


def test_fresh_broker_gate_requires_exact_eight_flat_readable_capable_apps_and_calendar():
    script = source("activate_g3_live_standing.ps1")
    broker = function_body(
        script,
        "Assert-FreshBrokerFlatReadReconciliation",
        "Assert-ApprovedStandingState",
    )

    for evidence in (
        "kite_order_readiness_validation_only_v1",
        "READ_ACCESS_CONFIRMED",
        "$BrokerMaxAgeSeconds = 60",
        "orders_submitted",
        "order_mutations_available",
        "strategy_controls_changed",
        "BEFORE_REGULAR_SESSION",
        "scheduled_trading_day",
        "https://www.nseindia.com/resources/exchange-communication-holidays?apiversion=v2",
        "regular_open_ist -ne '09:15'",
        "app1",
        "app8",
        "open_net_positions",
        "nonterminal_orders",
        "funds_positive",
        "minimum_live_balance_rupees",
        "live_balance_covers_minimum",
        "equity_segment_enabled",
        "'nse', 'mis', 'limit', 'market', 'sl', 'sl_m'",
    ):
        assert evidence in broker or evidence in script
    assert "& $Python $BrokerProbe --app all --minimum-live-balance 20000" in script
    assert script.index("& $Python $BrokerProbe --app all --minimum-live-balance 20000") < script.index(
        "& $Python $Operator approve-standing-session"
    )


def test_gate_pins_exact_standing_policy_strategy_setups_and_live_caps():
    script = source("activate_g3_live_standing.ps1")

    for evidence in (
        "g3_live_standing_operator_policy_v1",
        "STANDING_OPERATOR_POLICY",
        "G3_LIVE_STANDING_20261023_V1",
        "25d18e6f028a004159a7caf9ce6e717321852c01435c17c4b3b6efb4ca5f7a43",
        "82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4",
        "cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e",
        "f555f8f81cad15b9c5d33a74a6f277ae0ed5dfe077c07657a19f472b7e5028b5",
        "$ExpectedSetupCount = 61",
        "Assert-ExactSetupIds",
        "capital_per_trade_rs -ne 10000.0",
        "leverage -ne 5.0",
        "target_exposure_rs -ne 50000.0",
        "$null -ne $Policy.max_reserved_open_positions",
        "$null -ne $Policy.max_daily_entries",
        "$null -ne $Policy.max_reserved_gross_exposure_rs",
        "Assert-ExactPropertySet",
    ):
        assert evidence in script


def test_operator_call_and_postapproval_checks_bind_source_policy_broker_and_epoch():
    script = source("activate_g3_live_standing.ps1")
    approved = function_body(
        script, "Assert-ApprovedStandingState", "Assert-SameFileHash"
    )

    assert (
        "& $Python $Operator approve-standing-session `\n"
        "                --session-date $session.ToString('yyyy-MM-dd') `\n"
        "                --standing-policy-confirmed"
    ) in script
    for evidence in (
        "live_approval.source -ne 'STANDING_OPERATOR_POLICY'",
        "arm.source -ne 'STANDING_OPERATOR_POLICY'",
        "STANDING_OPERATOR_POLICY_OVERRIDE_NOT_ATTESTED",
        "Assert-ExactPropertySet $profile.live_approval",
        "Assert-ExactPropertySet $arm",
        "standing_policy_id",
        "standing_policy_fingerprint",
        "setup_ids_sha256",
        "broker_checked_at_ist",
        "completed_at_ist",
        "entry_not_before_ist",
        "approved_at_ist",
        "updated_at_ist",
        "$arm.approved_at_ist -ne $entryText",
        "$arm.broker_checked_at_ist -ne",
        "$brokerEpoch -gt $entryEpoch",
        "$brokerAgeAtApproval -gt $BrokerMaxAgeSeconds",
        "$entryEpoch -lt $approvalFloor",
        "$entryEpoch -ge $deadline",
    ):
        assert evidence in approved


def test_failure_path_disables_target_and_restores_or_disables_authorization_artifacts():
    script = source("activate_g3_live_standing.ps1")
    catch = script[script.index("} catch {\n    $originalFailure"):]

    for evidence in (
        "Disable-G3TargetFailClosed",
        "Assert-TargetState $false",
        "Disable-ArmFileFailClosed",
        "Copy-Item -LiteralPath $savedProfile -Destination $ProfilePath -Force",
        "Assert-SameFileHash $savedProfile $ProfilePath",
        "Copy-Item -LiteralPath $savedArm -Destination $ArmPath -Force",
        "Assert-SameFileHash $savedArm $ArmPath",
        "Remove-Item -LiteralPath $ArmPath -Force",
        "FAIL-CLOSED CLEANUP ERROR",
        "throw $message",
    ):
        assert evidence in catch


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="Windows PowerShell parser is unavailable",
)
@pytest.mark.parametrize(
    "name",
    ("activate_g3_live_standing.ps1", "schedule_g3_live_standing_weekday.ps1"),
)
def test_powershell_files_parse_without_executing(name: str):
    path = str(BAT / name).replace("'", "''")
    command = (
        "$tokens = $null; $errors = $null; "
        f"[System.Management.Automation.Language.Parser]::ParseFile('{path}', "
        "[ref]$tokens, [ref]$errors) | Out-Null; "
        "if ($errors.Count) { $errors | ForEach-Object { "
        "[Console]::Error.WriteLine($_.Message) }; exit 1 }"
    )
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="Windows PowerShell is unavailable",
)
def test_strict_auth_parser_preserves_windows_paths_and_rejects_duplicates(tmp_path: Path):
    valid = tmp_path / "valid.status"
    duplicate = tmp_path / "duplicate.status"
    valid.write_text(
        "status=SUCCESS\nscript=authentication_v2.py\nts=2026-10-23_08:35:00\n"
        "exit_code=0\nlog_file=C:\\Users\\Saarit\\authentication.log\n",
        encoding="utf-8",
    )
    duplicate.write_text("status=SUCCESS\nstatus=SUCCESS\n", encoding="utf-8")
    helper = str(BAT / "g3_activation_status_helpers.ps1").replace("'", "''")
    valid_ps = str(valid).replace("'", "''")
    duplicate_ps = str(duplicate).replace("'", "''")
    command = (
        f". '{helper}'; "
        f"$v=Read-StrictLiteralKeyValueFile -Path '{valid_ps}' -RequiredKeys "
        "@('status','script','ts','exit_code','log_file'); "
        "if ($v.log_file -cne 'C:\\Users\\Saarit\\authentication.log') { exit 2 }; "
        "$failed=$false; try { "
        f"Read-StrictLiteralKeyValueFile -Path '{duplicate_ps}' -RequiredKeys @('status') "
        "| Out-Null } catch { $failed=$true }; if (-not $failed) { exit 3 }"
    )
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
