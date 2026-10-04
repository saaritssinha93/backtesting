$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$hardener = Join-Path $baseDir "bat\harden_scheduled_task.ps1"
$definitions = @(
    @{
        Leaf = "EQIDV2_v13_shadow_prepare_0850"
        Name = "\EQIDV2_v13_shadow_prepare_0850"
        Time = "08:50"
        Runner = Join-Path $baseDir "bat\run_v13_shadow_prepare.bat"
    },
    @{
        Leaf = "EQIDV2_v13_shadow_seal_1525"
        Name = "\EQIDV2_v13_shadow_seal_1525"
        Time = "15:25"
        Runner = Join-Path $baseDir "bat\run_v13_shadow_seal.bat"
    }
)
$scheduleStartDate = [DateTime]::Now.Date.AddDays(1).ToString(
    "dd/MM/yyyy", [Globalization.CultureInfo]::InvariantCulture
)

function Get-TaskIfPresent {
    param([string]$Leaf)
    try {
        return Get-ScheduledTask -TaskName $Leaf -ErrorAction Stop
    }
    catch {
        if ([string]$_.FullyQualifiedErrorId -like "CmdletizationQuery_NotFound_TaskName*") {
            return $null
        }
        throw
    }
}

if (-not (Test-Path -LiteralPath $hardener -PathType Leaf)) {
    throw "Missing scheduled-task hardener: $hardener"
}
$hardenerSource = Get-Content -LiteralPath $hardener -Raw
if ($hardenerSource -notmatch 'DisallowStartIfOnBatteries' -or
    $hardenerSource -notmatch 'ExecutionTimeLimit' -or
    $hardenerSource -notmatch 'MultipleInstances') {
    throw "Scheduled-task hardener does not expose the required safety settings."
}

foreach ($definition in $definitions) {
    $runner = [string]$definition.Runner
    if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
        throw "Missing shadow runner: $runner"
    }
    $source = Get-Content -LiteralPath $runner -Raw
    $forbiddenTaskStart = 'Start' + '-ScheduledTask'
    if ($source -notmatch 'execution_authority=false' -or
        $source -notmatch 'v13_shadow_automation\.py' -or
        $source -match '(?i)live_arm|kill_switch|FNO_V6_EXECUTION_MODE=LIVE' -or
        $source.Contains($forbiddenTaskStart)) {
        throw "Shadow runner violates the no-authority scheduling contract: $runner"
    }
    $existing = Get-TaskIfPresent -Leaf $definition.Leaf
    if ($null -ne $existing) {
        if ([string]::Equals(
            [string]$existing.State, "Running", [System.StringComparison]::OrdinalIgnoreCase
        )) {
            throw "Existing task is running; replacement was refused: $($definition.Name)"
        }
        if (@($existing.Actions).Count -ne 1 -or -not [string]::Equals(
            [string]$existing.Actions[0].Execute,
            $runner,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
            throw "Existing task name is owned by a different action: $($definition.Name)"
        }
    }
}

foreach ($definition in $definitions) {
    Write-Output "[INFO] Creating $($definition.Name) for weekdays at $($definition.Time) ..."
    # The start boundary is tomorrow, so neither an 08:50 nor 15:25 trigger
    # can become due while this installer is creating and hardening the tasks.
    & schtasks.exe /Create /F /TN $definition.Name /SC WEEKLY /D MON,TUE,WED,THU,FRI /SD $scheduleStartDate /ST $definition.Time /TR $definition.Runner
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks failed for $($definition.Name) with exit code $LASTEXITCODE"
    }
    & $hardener -TaskName $definition.Name
    if ($LASTEXITCODE -ne 0) {
        throw "Scheduled-task hardening failed for $($definition.Name)"
    }
}

foreach ($definition in $definitions) {
    $installed = Get-ScheduledTask -TaskName $definition.Leaf -ErrorAction Stop
    if (@($installed.Actions).Count -ne 1 -or -not [string]::Equals(
        [string]$installed.Actions[0].Execute,
        [string]$definition.Runner,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Installed task action failed verification: $($definition.Name)"
    }
    if (-not [bool]$installed.Settings.Enabled) {
        throw "Installed task is disabled: $($definition.Name)"
    }
    if ([string]::Equals(
        [string]$installed.State, "Running", [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Task unexpectedly started during installation: $($definition.Name)"
    }
}

Write-Output "[SUCCESS] Installed weekday shadow prepare at 08:50 and seal at 15:25."
Write-Output "[INFO] Installation did not request either task to run and did not touch trading controls."
