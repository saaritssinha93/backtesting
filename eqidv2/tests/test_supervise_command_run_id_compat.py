"""Run-correlation supervision must not kill unmigrated legacy workers."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"


def test_worker_run_id_enforcement_is_explicitly_opt_in() -> None:
    source = (BAT / "supervise_command.ps1").read_text(encoding="utf-8")

    assert "[switch]$RequireWorkerRunIdMatch" in source
    guard = source.index("if ($RequireWorkerRunIdMatch)")
    missing = source.index('"worker_run_id_missing"')
    mismatch = source.index('"worker_run_id_mismatch"')
    assert guard < missing < mismatch
    # All children may inherit correlation metadata; only opted-in launchers
    # are allowed to kill a worker for not publishing it back yet.
    assert '$env:EQIDV2_OBS_RUN_ID = $script:CurrentRunId' in source


def test_only_migrated_v13_live_launcher_requires_run_id_match() -> None:
    live = (BAT / "run_fno_v13_v10_g_live_kite_qty1.bat").read_text(
        encoding="utf-8"
    )
    nifty = (
        BAT / "run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat"
    ).read_text(encoding="utf-8")

    assert "-RequireWorkerRunIdMatch" in live
    assert "-RequireWorkerRunIdMatch" not in nifty
    assert '-WorkerStatusFile "%WORKER_STATUS_FILE%"' in nifty
    assert '-WorkerHeartbeatFile "%WORKER_HEARTBEAT_FILE%"' in nifty


def test_legacy_nifty_liveness_contract_has_no_run_id_requirement() -> None:
    """Regression fixture for the 2026-09-25 NIFTY restart storm."""

    worker = (
        ROOT / "eqidv2_nifty_guard_fetcher_supervised_v16_5min.py"
    ).read_text(encoding="utf-8")
    runner = (
        BAT / "run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat"
    ).read_text(encoding="utf-8")

    assert 'f"pid={os.getpid()}"' in worker
    assert "run_id" not in worker
    assert "-RequireWorkerRunIdMatch" not in runner


def test_supervisor_diagnostic_log_writes_retry_and_fail_open() -> None:
    source = (BAT / "supervise_command.ps1").read_text(encoding="utf-8")
    helper_start = source.index("function Write-SupervisorDiagnosticLine")
    helper_end = source.index("function Write-KeyFile", helper_start)
    helper = source[helper_start:helper_end]
    write_log_start = source.index("function Write-LogLine")
    write_log_end = source.index("function Is-AfterCutoff", write_log_start)
    write_log = source[write_log_start:write_log_end]

    assert "for ($attempt = 1; $attempt -le 4; $attempt++)" in helper
    assert (
        "Add-Content -LiteralPath $script:SupervisorLogFile -Value $Line "
        "-Encoding UTF8 -ErrorAction Stop"
    ) in helper
    assert "Start-Sleep -Milliseconds (50 * $attempt)" in helper
    assert "Write-Warning" in helper
    assert "catch { }" in helper
    assert "Write-SupervisorDiagnosticLine -Line $line" in write_log
    # Every supervisor-file append, including early singleton-lock diagnostics,
    # must pass through the retry/fail-open helper.
    assert source.count("Add-Content") == 1
    assert source.count("Write-SupervisorDiagnosticLine -Line") == 6


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="Windows PowerShell behavior test",
)
def test_supervisor_diagnostic_log_helper_handles_sharing_violations(tmp_path) -> None:
    """Load only the helper AST; never execute the supervisor or its worker."""

    command = r"""
$ErrorActionPreference = "Stop"
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:SUPERVISOR_SOURCE,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -gt 0) {
    throw ($parseErrors | Out-String)
}
$functionAst = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Write-SupervisorDiagnosticLine"
}, $true)
if ($null -eq $functionAst) {
    throw "Write-SupervisorDiagnosticLine was not found."
}
Invoke-Expression $functionAst.Extent.Text

$script:SupervisorLogFile = $env:SUPERVISOR_TEST_LOG
$script:Attempts = 0
function Add-Content {
    [CmdletBinding()]
    param(
        [string]$LiteralPath,
        [object]$Value,
        [string]$Encoding
    )
    $script:Attempts += 1
    if ($script:Attempts -lt 3) {
        throw [System.IO.IOException]::new("sharing violation")
    }
    Microsoft.PowerShell.Management\Add-Content `
        -LiteralPath $LiteralPath -Value $Value -Encoding $Encoding
}

Write-SupervisorDiagnosticLine -Line "eventual-success"
if ($script:Attempts -ne 3) {
    throw "Expected 3 transient-write attempts; got $script:Attempts."
}
if ((Get-Content -LiteralPath $script:SupervisorLogFile -Raw) -notmatch "eventual-success") {
    throw "The retry-success diagnostic line was not persisted."
}

$script:Attempts = 0
function Add-Content {
    [CmdletBinding()]
    param(
        [string]$LiteralPath,
        [object]$Value,
        [string]$Encoding
    )
    $script:Attempts += 1
    throw [System.IO.IOException]::new("persistent sharing violation")
}
$WarningPreference = "Stop"
Write-SupervisorDiagnosticLine -Line "safe-to-drop"
if ($script:Attempts -ne 4) {
    throw "Expected 4 persistent-write attempts; got $script:Attempts."
}
Write-Output "SUPERVISOR_LOG_RETRY_OK"
"""
    env = os.environ.copy()
    env["SUPERVISOR_SOURCE"] = str(BAT / "supervise_command.ps1")
    env["SUPERVISOR_TEST_LOG"] = str(tmp_path / "supervisor.log")
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "SUPERVISOR_LOG_RETRY_OK" in completed.stdout
