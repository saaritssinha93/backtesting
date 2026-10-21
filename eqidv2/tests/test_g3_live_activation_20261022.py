"""Offline safety contract for the explicitly approved 22 October gate."""
from __future__ import annotations

import base64
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

import fno_v13_v10_g_3_live_config as config
from tools import g3_live_operator as operator


ROOT = Path(__file__).resolve().parents[1]


def source(name: str) -> str:
    return (ROOT / "bat" / name).read_text(encoding="utf-8")


def function_body(script: str, name: str, next_name: str) -> str:
    start = script.index(f"function {name}")
    end = script.index(f"function {next_name}", start)
    return script[start:end]


def test_one_time_gate_is_exactly_dated_and_has_no_catch_up_retry_or_start():
    script = source("schedule_g3_live_activation_20261022.ps1")

    assert "EQIDV2_g3_live_approve_20261022_0910" in script
    assert "2026-10-22 09:10:00" in script
    assert "activate_g3_live_20261022.ps1" in script
    assert "New-ScheduledTaskTrigger -Once -At $At" in script
    assert "(Get-Date) -ge $At" in script
    assert "no catch-up task will be registered" in script
    assert "StartWhenAvailable -or" in script
    assert "RestartCount -ne 0" in script
    assert "MultipleInstances -ne 'IgnoreNew'" in script
    assert "-LogonType Interactive" in script
    assert "-RunLevel Limited" in script
    assert "Start-ScheduledTask" not in script
    assert "-Weekly" not in script
    assert "RestartOnFailure" not in script


def test_scheduler_accepts_only_the_existing_enabled_ready_approved_target():
    script = source("schedule_g3_live_activation_20261022.ps1")
    target_gate = script[
        script.index("function Assert-EnabledApprovedTarget"):
        script.index("if ([System.TimeZoneInfo]", script.index("function Assert-EnabledApprovedTarget"))
    ]

    assert "-not $target.Settings.Enabled" in target_gate
    assert "[string]$target.State -ne 'Ready'" in target_gate
    assert "run_g3_approved_live.bat" in target_gate
    assert "$targetActions[0].Execute -ne $expectedCommand" in target_gate
    assert "$targetActions[0].Arguments -ne $expectedArguments" in target_gate
    assert "Assert-ReviewedTargetDefinitionXml" in target_gate
    assert "2026-10-22 09:15:00" in target_gate
    assert "Interactive" in target_gate
    assert "Limited" in target_gate
    assert "must remain disabled" not in target_gate
    assert "Assert-EnabledApprovedTarget | Out-Null" in script


def test_activation_accepts_enabled_approved_target_and_never_starts_it():
    script = source("activate_g3_live_20261022.ps1")
    gate = function_body(
        script, "Assert-TargetIsEnabledApproved", "Assert-NoG3LiveWorkers"
    )

    assert "-not $task.Settings.Enabled" in gate
    assert "[string]$task.State -ne 'Ready'" in gate
    assert "run_g3_approved_live.bat" in gate
    assert "$actions[0].Execute -ne $expectedCommand" in gate
    assert "$actions[0].Arguments -ne $expectedArguments" in gate
    assert "Assert-ReviewedTargetDefinitionXml" in gate
    assert "2026-10-22 09:15:00" in gate
    assert "StartWhenAvailable" in gate
    assert "RestartCount" in gate
    assert "MultipleInstances" in gate
    assert "Start-ScheduledTask" not in script
    assert "--activate-live" not in script


def test_activation_has_all_fail_closed_same_session_gates():
    script = source("activate_g3_live_20261022.ps1")

    for evidence in (
        "2026-10-22 from 09:09:00 through 09:12:59",
        "-EnableBefore '09:12:30'",
        "Read-StrictLiteralKeyValueFile",
        "'status', 'script', 'ts', 'exit_code', 'log_file'",
        "$stamp.Date -ne $Now.Date",
        "$age -gt $AuthMaxAgeSeconds",
        "$status.log_file -ne $ExpectedAuthLogPath",
        "preopen_session_healthcheck_latest.json",
        "Refresh-PreopenHealth",
        "preopen_session_healthcheck.py",
        "--max-age-min 35 --report-path $HealthRefreshLog",
        "$age -gt 120",
        "Assert-NoG3LiveWorkers",
        "completed_at_ist",
        "$age -gt 60",
        "READ_ACCESS_CONFIRMED",
        "open_net_positions",
        "nonterminal_orders",
        "funds_positive",
        "minimum_live_balance_rupees",
        "live_balance_covers_minimum",
        "equity_segment_enabled",
        "orders_submitted",
        "order_mutations_available",
        "app1",
        "app8",
    ):
        assert evidence in script
    assert "test_kite_order_readiness.py" in source("approve_g3_live.ps1")
    assert "ConvertFrom-StringData" not in script
    main = script.index("Write-G3ActivationLog 'Starting fail-closed dated activation checks.'")
    assert script.index("Refresh-PreopenHealth", main) < script.index(
        "Assert-PreopenHealth $now", main
    )


def test_activation_uses_https_clock_sanity_without_mutating_system_time():
    script = source("activate_g3_live_20261022.ps1")
    clock = function_body(script, "Assert-HttpsClockSkew", "Assert-NoG3LiveWorkers")
    for evidence in (
        "https://api.kite.trade/", "HttpMethod]::Head", "$response.Headers.Date",
        "$ClockMaxSkewSeconds = 30", "$midpoint", "$skew -gt $ClockMaxSkewSeconds",
    ):
        assert evidence in script or evidence in clock
    assert script.count("Assert-HttpsClockSkew") >= 3  # function plus pre/post calls
    for forbidden in ("w32tm", "Set-Date", "resync", "config /update"):
        assert forbidden.lower() not in script.lower()


def test_activation_is_pinned_to_exact_strategy_policy_setups_and_caps():
    script = source("activate_g3_live_20261022.ps1")

    for evidence in (
        "V13-V10-G-3-PRODUCTION-V11",
        "82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4",
        "cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e",
        "$ExpectedSetupCount = 61",
        "capital_per_trade_rs -ne 10000.0",
        "leverage -ne 5.0",
        "target_exposure_rs -ne 50000.0",
        "$null -ne $profile.live_execution_policy.max_reserved_open_positions",
        "$null -ne $profile.live_execution_policy.max_daily_entries",
        "$null -ne $profile.live_execution_policy.max_reserved_gross_exposure_rs",
    ):
        assert evidence in script


def test_stale_prior_day_authorization_is_not_reused_and_failure_disables_target():
    script = source("activate_g3_live_20261022.ps1")
    stale_gate = function_body(
        script,
        "Assert-StalePriorDayArmCannotAuthorizeCurrentSession",
        "Assert-AuthenticationReady",
    )
    catch = script[script.index('$originalFailure = $_.Exception.Message'):]

    assert "$PriorSessionDate = '2026-10-21'" in script
    assert (
        "$ExpectedPriorPolicyFingerprint = "
        "'979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362'"
    ) in script
    assert (
        "$arm.live_execution_policy_fingerprint -ne "
        "$ExpectedPriorPolicyFingerprint"
    ) in stale_gate
    assert (
        "$profile.live_execution_policy.live_execution_policy_fingerprint -ne\n"
        "            $ExpectedPolicyFingerprint"
    ) in stale_gate
    assert "$approvalDate -ge $session" in stale_gate
    assert "$armDate -ge $session" in stale_gate
    assert "same-day or future" in stale_gate
    assert "Disable-G3TargetFailClosed" in catch
    assert "Assert-SameFileHash" in catch
    assert "Copy-Item -LiteralPath $savedProfile -Destination $ProfilePath" in catch
    assert "throw" in catch


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="PowerShell is available only on Windows",
)
@pytest.mark.parametrize(
    ("arm_date", "arm_policy_fingerprint", "expected_ok", "expected_error"),
    (
        (
            "2026-10-21",
            "979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362",
            True,
            None,
        ),
        (
            "2026-10-21",
            "cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e",
            False,
            "not the reviewed prior-session authorization",
        ),
        (
            "2026-10-22",
            "979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362",
            False,
            "same-day or future LIVE arm",
        ),
        (
            "2026-10-22",
            "cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e",
            False,
            "same-day or future LIVE arm",
        ),
    ),
)
def test_stale_arm_gate_accepts_only_exact_prior_policy_identity(
    tmp_path: Path,
    arm_date: str,
    arm_policy_fingerprint: str,
    expected_ok: bool,
    expected_error: str | None,
):
    activation = source("activate_g3_live_20261022.ps1")
    stale_gate = function_body(
        activation,
        "Assert-StalePriorDayArmCannotAuthorizeCurrentSession",
        "Assert-AuthenticationReady",
    )
    strategy_fingerprint = (
        "82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4"
    )
    current_policy_fingerprint = (
        "cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e"
    )
    prior_policy_fingerprint = (
        "979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362"
    )
    profile_path = tmp_path / "active_profile.json"
    arm_path = tmp_path / "live_arm.json"
    profile_path.write_text(
        json.dumps(
            {
                "profile": "V13_V10_G_3",
                "strategy_version": "V13-V10-G-3-PRODUCTION-V11",
                "strategy_fingerprint": strategy_fingerprint,
                "live_execution_policy": {
                    "live_execution_policy_fingerprint": current_policy_fingerprint,
                },
                "live_activation_authorized": True,
                "live_approval": {"session_date": "2026-10-21"},
            }
        ),
        encoding="utf-8",
    )
    arm_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "session_date": arm_date,
                "strategy_version": "V13-V10-G-3-PRODUCTION-V11",
                "strategy_fingerprint": strategy_fingerprint,
                "live_execution_policy_fingerprint": arm_policy_fingerprint,
            }
        ),
        encoding="utf-8",
    )

    ps_profile_path = str(profile_path).replace("'", "''")
    ps_arm_path = str(arm_path).replace("'", "''")
    command = f"""
$ErrorActionPreference = 'Stop'
$SessionDate = '2026-10-22'
$PriorSessionDate = '2026-10-21'
$ExpectedStrategyVersion = 'V13-V10-G-3-PRODUCTION-V11'
$ExpectedStrategyFingerprint = '{strategy_fingerprint}'
$ExpectedPolicyFingerprint = '{current_policy_fingerprint}'
$ExpectedPriorPolicyFingerprint = '{prior_policy_fingerprint}'
$ProfilePath = '{ps_profile_path}'
$ArmPath = '{ps_arm_path}'
function Read-StrictJsonWithRetry([string]$Path) {{
    Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}}
{stale_gate}
try {{
    Assert-StalePriorDayArmCannotAuthorizeCurrentSession
    [Console]::Out.WriteLine('ACCEPTED')
    exit 0
}} catch {{
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 7
}}
"""
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

    if expected_ok:
        assert result.returncode == 0, result.stderr
        assert "ACCEPTED" in result.stdout
    else:
        assert result.returncode == 7, result.stderr
        assert expected_error is not None
        assert expected_error in result.stderr


def test_target_definition_is_exact_weekday_non_repeating_approved_launcher():
    activation = source("activate_g3_live_20261022.ps1")
    scheduler = source("schedule_g3_live_activation_20261022.ps1")

    for script in (activation, scheduler):
        assert "Monday|Tuesday|Wednesday|Thursday|Friday" in script
        assert "t:Repetition" in script
        assert "t:EndBoundary" in script
        assert "'/d /c \"\"' + $expectedLauncher + '\"\"'" in script
        assert "run_g3_approved_live.bat" in script


def test_post_commit_failure_restores_artifacts_and_verifies_target_disabled():
    script = source("activate_g3_live_20261022.ps1")

    assert "Global\\EQIDV2_G3_LIVE_ACTIVATION_20261022" in script
    assert "$approvalTransactionStarted = $true" in script
    assert "active_profile.json" in script
    assert "live_arm.json" in script
    assert "Disable-ScheduledTask -TaskName $TargetTask -ErrorAction Stop" in script
    assert "[string]$disabled.State -ne 'Disabled'" in script
    assert "FAIL-CLOSED CLEANUP ERROR" in script


def test_date_bound_launcher_rejects_the_stale_prior_day_arm_offline(tmp_path: Path):
    setup_ids = [setup.setup_id for setup in config.ACTIVE_SETUPS]
    policy = config.live_execution_policy.execution_metadata()
    state = {
        "profile": config.STRATEGY_PROFILE,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "live_execution_policy": policy,
        "deployment_stage": "LIVE_APPROVED",
        "live_activation_authorized": True,
        "live_approval": {
            "session_date": "2026-10-21",
            "strategy_fingerprint": config.strategy_fingerprint(),
            "live_execution_policy_fingerprint": (
                config.live_execution_policy.policy_fingerprint()
            ),
            "setup_ids": setup_ids,
            "entry_not_before_ist": "2026-10-21T11:34:52+05:30",
        },
    }
    arm = {
        "enabled": True,
        "session_date": "2026-10-21",
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "live_execution_policy_fingerprint": (
            config.live_execution_policy.policy_fingerprint()
        ),
        "setup_ids": setup_ids,
        "entry_not_before_ist": "2026-10-21T11:34:52+05:30",
    }

    with pytest.raises(ValueError, match="Dated approval/arm does not match today"):
        operator.validate_launch(
            state,
            arm,
            now=datetime(2026, 10, 22, 9, 15, tzinfo=config.IST),
            runtime_root=tmp_path,
        )


def test_wrapper_calls_only_the_existing_operator_for_exact_oct22_approval():
    script = source("activate_g3_live_20261022.ps1")

    assert "$SessionDate = '2026-10-22'" in script
    assert "& $Approval -SessionDate $SessionDate -Apply -Python $Python" in script
    assert "AddDays" not in script
    assert "2026-10-23" not in script
    assert "StartsTaskNow" not in script


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="PowerShell parser is available only on Windows",
)
@pytest.mark.parametrize(
    "name",
    (
        "activate_g3_live_20261022.ps1",
        "schedule_g3_live_activation_20261022.ps1",
    ),
)
def test_powershell_files_parse_without_executing(name: str):
    path = str(ROOT / "bat" / name).replace("'", "''")
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
