param(
    [switch]$VerifyOnly
)

$ErrorActionPreference = "Stop"

$taskName = "EQIDV2_fno_oi_fetch_5min_fast_production_0905"
$trialAt = [datetime]::ParseExact(
    "2026-09-02 09:05",
    "yyyy-MM-dd HH:mm",
    [Globalization.CultureInfo]::InvariantCulture
)
$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$runner = Join-Path $baseDir "bat\run_fno_oi_fetch_5min_fast_production.bat"
$producer = Join-Path $baseDir "fno_oi_fetch_5min_fast_production.py"
$hardener = Join-Path $baseDir "bat\harden_scheduled_task.ps1"

function Assert-TrialTaskContract {
    param([Microsoft.Management.Infrastructure.CimInstance]$Task)

    $actions = @($Task.Actions)
    $triggers = @($Task.Triggers)
    if ($actions.Count -ne 1) {
        throw "$taskName must have exactly one action."
    }
    $observedAction = [IO.Path]::GetFullPath([string]$actions[0].Execute)
    if (-not $observedAction.Equals(
        [IO.Path]::GetFullPath($runner),
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw "$taskName action differs from the approved trial runner: $observedAction"
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$actions[0].Arguments)) {
        throw "$taskName must not inject unreviewed runner arguments."
    }
    if ($triggers.Count -ne 1) {
        throw "$taskName must have exactly one trigger."
    }
    $observedStart = [datetime]::Parse([string]$triggers[0].StartBoundary)
    if ($observedStart -ne $trialAt) {
        throw "$taskName trigger differs from $($trialAt.ToString('s')): $($observedStart.ToString('s'))"
    }
    if (-not $Task.Settings.Enabled) {
        throw "$taskName is disabled."
    }
    if ($Task.Settings.MultipleInstances -ne "IgnoreNew") {
        throw "$taskName must use IgnoreNew instance policy."
    }
    if (-not $Task.Settings.StartWhenAvailable) {
        throw "$taskName must be StartWhenAvailable; the runner date gate prevents late-day execution."
    }
}

if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
    throw "Trial runner is missing: $runner"
}
if (-not (Test-Path -LiteralPath $producer -PathType Leaf)) {
    throw "Fast production entrypoint is missing: $producer"
}

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    Assert-TrialTaskContract -Task $existing
    Write-Output "[VERIFIED] Existing one-time trial task matches the approved contract: $taskName"
    exit 0
}

if ($VerifyOnly) {
    throw "One-time trial task is not installed: $taskName"
}

$action = New-ScheduledTaskAction -Execute $runner
$trigger = New-ScheduledTaskTrigger -Once -At $trialAt
$settingsArguments = @{
    AllowStartIfOnBatteries = $true
    DontStopIfGoingOnBatteries = $true
    StartWhenAvailable = $true
    ExecutionTimeLimit = [TimeSpan]::Zero
    MultipleInstances = "IgnoreNew"
}
$settings = New-ScheduledTaskSettingsSet @settingsArguments
$principalArguments = @{
    UserId = $env:USERNAME
    LogonType = "Interactive"
    RunLevel = "Limited"
}
$principal = New-ScheduledTaskPrincipal @principalArguments

$registrationArguments = @{
    TaskName = $taskName
    Action = $action
    Trigger = $trigger
    Settings = $settings
    Principal = $principal
    Description = "One-time FnO fast production trial for 2026-09-02; runner is fail-closed outside that IST date."
}
$null = Register-ScheduledTask @registrationArguments

if (Test-Path -LiteralPath $hardener -PathType Leaf) {
    & $hardener -TaskName $taskName
    if (-not $?) {
        throw "Task hardening failed for $taskName"
    }
}

$installed = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
Assert-TrialTaskContract -Task $installed
$info = Get-ScheduledTaskInfo -TaskName $taskName
$message = "[SUCCESS] Installed and verified {0}; next_run={1}; action={2}" -f (
    $taskName,
    $info.NextRunTime.ToString("yyyy-MM-dd HH:mm:ss"),
    $runner
)
Write-Output $message
