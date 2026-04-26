[CmdletBinding()]
param(
    [switch]$Preview
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$BaseDir = Split-Path -Parent $PSScriptRoot
$LogDir  = Join-Path $BaseDir "logs"
$LogFile = Join-Path $LogDir "eqidv2_v16_5min_two_stage_stop.log"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Write-LogLine {
    param([Parameter(Mandatory = $true)][string]$Message)
    $ts   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}] {1}" -f $ts, $Message
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Write-KeyFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Data
    )
    $parent = Split-Path -Path $Path -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $lines = foreach ($entry in ($Data.GetEnumerator() | Sort-Object Name)) {
        "{0}={1}" -f $entry.Key, [string]$entry.Value
    }
    Set-Content -Path $Path -Value $lines -Encoding UTF8
}

function Write-StoppedStatus {
    param([string]$ScriptName)
    $statusPath    = Join-Path $LogDir "$ScriptName.status"
    $heartbeatPath = Join-Path $LogDir "$ScriptName.heartbeat"
    Write-KeyFile -Path $statusPath -Data @{
        status = "STOPPED"
        script = "$ScriptName.py"
        ts     = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        reason = "manual_stop_two_stage"
        source = "stop_eqidv2_v16_5min_two_stage_stack.ps1"
    }
    Write-KeyFile -Path $heartbeatPath -Data @{
        name  = "$ScriptName.py"
        ts_utc = (Get-Date).ToUniversalTime().ToString("o")
        state = "STOPPED"
        note  = "manual_stop_two_stage"
    }
}

# Patterns to match — Signal Early Engine (SEE), legacy Signal Engine (SE),
# Pending Fetcher, Detection Engine, and their supervisors.
# strategy_v2 §SEE-A13 fix (2026-04-22): scheduler now launches SEE
# (eqidv2_signal_early_engine_v16_5min.py) but this stop script previously
# only matched the legacy SE name, so the SEE process survived stop-stack.
# Keep the legacy SE patterns for one release cycle in case replay tooling
# or residual sessions still spawn SE.
$patterns = @(
    "run_eqidv2_signal_early_engine_v16_5min\.bat",
    "eqidv2_signal_early_engine_v16_5min\.py",
    "run_eqidv2_signal_engine_v16_5min\.bat",
    "eqidv2_signal_engine_v16_5min\.py",
    "run_eqidv2_pending_data_fetcher_v16_5min\.bat",
    "eqidv2_pending_data_fetcher_v16_5min\.py",
    "run_eqidv2_detection_engine_v16_5min\.bat",
    "eqidv2_detection_engine_v16_5min\.py",
    "supervise_command\.ps1.*eqidv2_signal_early_engine_v16_5min",
    "supervise_command\.ps1.*eqidv2_signal_engine_v16_5min",
    "supervise_command\.ps1.*eqidv2_pending_data_fetcher_v16_5min",
    "supervise_command\.ps1.*eqidv2_detection_engine_v16_5min"
)

$selfPid = $PID
$targets = @(Get-CimInstance Win32_Process | Where-Object {
    if ($_.ProcessId -eq $selfPid) {
        return $false
    }
    $cmd = [string]$_.CommandLine
    if ([string]::IsNullOrWhiteSpace($cmd)) {
        return $false
    }
    foreach ($pattern in $patterns) {
        if ($cmd -match $pattern) {
            return $true
        }
    }
    return $false
} | Sort-Object ProcessId -Unique)

Write-LogLine -Message ("Matched {0} process(es) for V16_5MIN two-stage stop. preview={1}" -f $targets.Count, $Preview.IsPresent)

foreach ($target in $targets) {
    $line = "PID={0} NAME={1}" -f $target.ProcessId, $target.Name
    if ($Preview) {
        Write-LogLine -Message ("MATCH {0}" -f $line)
        continue
    }

    Write-LogLine -Message ("STOP {0}" -f $line)
    try {
        $taskkillOutput = & taskkill /PID $target.ProcessId /T /F 2>&1
        foreach ($msg in @($taskkillOutput)) {
            if (-not [string]::IsNullOrWhiteSpace([string]$msg)) {
                Write-LogLine -Message ([string]$msg)
            }
        }
    } catch {
        Write-LogLine -Message ("WARN stop failed for PID={0}: {1}" -f $target.ProcessId, $_.Exception.Message)
    }
}

if (-not $Preview) {
    # strategy_v2 §SEE-A13 fix: write SEE status first (current producer),
    # keep legacy SE status for one release cycle so any stale supervisor
    # polling on the old filename also sees STOPPED and does not flag a
    # phantom failure.
    Write-StoppedStatus -ScriptName "eqidv2_signal_early_engine_v16_5min"
    Write-StoppedStatus -ScriptName "eqidv2_signal_engine_v16_5min"
    Write-StoppedStatus -ScriptName "eqidv2_pending_data_fetcher_v16_5min"
    Write-StoppedStatus -ScriptName "eqidv2_detection_engine_v16_5min"
    Write-LogLine -Message "Wrote STOPPED state for SEE + legacy SE + PF + DE status files."
}
