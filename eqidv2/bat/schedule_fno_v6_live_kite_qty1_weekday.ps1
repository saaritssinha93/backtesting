$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$runner = Join-Path $baseDir "bat\run_fno_v6_live_kite_qty1.bat"
$sessionScript = Join-Path $baseDir "fno_v6_live_kite_session.py"
$hardener = Join-Path $baseDir "bat\harden_scheduled_task.ps1"
$taskLeaf = "EQIDV2_fno_v6_live_kite_qty1_0915"
$taskName = "\$taskLeaf"
$sessionId = "fno_v6_live_kite_qty1"
$startTime = "09:15"

function Get-TaskIfPresent {
    try {
        return Get-ScheduledTask -TaskName $taskLeaf -ErrorAction Stop
    }
    catch {
        $errorId = [string]$_.FullyQualifiedErrorId
        if ($errorId.StartsWith(
            "CmdletizationQuery_NotFound_TaskName",
            [System.StringComparison]::Ordinal
        )) {
            return $null
        }
        throw
    }
}

if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
    throw "Missing frozen live runner: $runner"
}
if (-not (Test-Path -LiteralPath $sessionScript -PathType Leaf)) {
    throw "Missing live coordinator: $sessionScript"
}
if (-not (Test-Path -LiteralPath $hardener -PathType Leaf)) {
    throw "Missing scheduled-task hardener: $hardener"
}

$runnerSource = Get-Content -LiteralPath $runner -Raw
if ($runnerSource -notmatch 'SESSION_ID=fno_v6_live_kite_qty1' -or
    $runnerSource -notmatch 'FNO_V6_EXECUTION_MODE=LIVE' -or
    $runnerSource -notmatch 'FNO_V6_LIVE_ACK=I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS' -or
    $runnerSource -notmatch 'fno_v6_live_kite_session\.py' -or
    $runnerSource -notmatch 'supervise_command\.ps1') {
    throw "Runner does not match the frozen FnO V6 LIVE quantity-one session contract."
}
if ($runnerSource -match '(?i)live_arm|kill_switch') {
    throw "Runner must not create or alter live-arm or kill-switch state."
}

$existing = Get-TaskIfPresent
if ($null -ne $existing) {
    if ([string]::Equals(
        [string]$existing.State,
        "Running",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Existing task is running; replacement was refused."
    }
    if (@($existing.Actions).Count -ne 1 -or -not [string]::Equals(
        [string]$existing.Actions[0].Execute,
        $runner,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Existing task name is owned by a different action; replacement was refused."
    }
}

Write-Output "[INFO] Creating $taskName for weekdays at $startTime ..."
& schtasks.exe /Create /F /TN $taskName /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $startTime /TR $runner
if ($LASTEXITCODE -ne 0) {
    throw "schtasks failed for $taskName with exit code $LASTEXITCODE"
}

& $hardener -TaskName $taskName
if ($LASTEXITCODE -ne 0) {
    throw "Scheduled-task hardening failed for $taskName with exit code $LASTEXITCODE"
}

$installed = Get-ScheduledTask -TaskName $taskLeaf -ErrorAction Stop
if (-not [string]::Equals(
    [string]$installed.TaskPath,
    "\",
    [System.StringComparison]::Ordinal
) -or @($installed.Actions).Count -ne 1 -or -not [string]::Equals(
    [string]$installed.Actions[0].Execute,
    $runner,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "Installed task identity or action failed verification."
}
if (-not [bool]$installed.Settings.Enabled) {
    throw "Installed task is not enabled after hardening."
}
if ([string]::Equals(
    [string]$installed.State,
    "Running",
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "Task unexpectedly started during installation."
}

Write-Output "[SUCCESS] Scheduled $taskName ($sessionId) for Monday-Friday at $startTime."
Write-Output "[INFO] The installer did not request a task run or touch live-arm/kill-switch state."
