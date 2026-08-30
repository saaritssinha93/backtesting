param(
    [string]$TaskName = "EQIDV2_fno_v8_combined_paper_0915",
    [string]$StartTime = "09:15"
)

$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$batDir = Join-Path $baseDir "bat"
$runner = Join-Path $batDir "run_fno_v8_combined_paper_session.bat"
$frozenTaskName = "EQIDV2_fno_v8_combined_paper_0915"
$frozenUserId = "Saarit"
$indiaTimeZone = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")

# Reject a mistyped/repurposed target before entering the catch block, whose
# fail-closed cleanup is allowed to disable only the frozen V8 task.
if (-not [string]::Equals(
    $TaskName,
    $frozenTaskName,
    [System.StringComparison]::Ordinal
)) {
    Write-Error "TaskName is frozen as $frozenTaskName; no task was changed."
    exit 2
}
if (-not [string]::Equals($StartTime, "09:15", [System.StringComparison]::Ordinal)) {
    Write-Error "The frozen V8 paper trigger must be exactly 09:15; no task was changed."
    exit 2
}
$existingTask = $null
try {
    $existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
}
catch {
    $fullyQualifiedErrorId = [string]$_.FullyQualifiedErrorId
    if (-not $fullyQualifiedErrorId.StartsWith(
        "CmdletizationQuery_NotFound_TaskName",
        [System.StringComparison]::Ordinal
    )) {
        Write-Error "Could not safely determine whether the V8 task exists; no task was changed: $($_.Exception.Message)"
        exit 2
    }
}
if ($null -ne $existingTask -and (
    [bool]$existingTask.Settings.Enabled -or
    -not [string]::Equals([string]$existingTask.State, "Disabled", [System.StringComparison]::OrdinalIgnoreCase)
)) {
    Write-Error "Existing V8 task is not safely Disabled; staging refused without mutation."
    exit 2
}
$registrationAttempted = $false

try {
    if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
        throw "Missing V8 paper runner: $runner"
    }
    Write-Output "[INFO] Registering staged V8 paper task $TaskName disabled at creation ..."
    $action = New-ScheduledTaskAction -Execute $runner
    $principal = New-ScheduledTaskPrincipal `
        -UserId $frozenUserId `
        -LogonType Interactive `
        -RunLevel Limited
    $trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
        -At $StartTime
    # This is intentionally a dedicated settings object.  The common hardener
    # enables tasks, creating a race with catch-up execution.  -Disable makes
    # the registered definition non-runnable from its first observable state.
    # StartWhenAvailable is intentionally left false: enabling before 09:15
    # must wait for that day's trigger, never replay a missed earlier trigger.
    $settings = New-ScheduledTaskSettingsSet `
        -Disable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -MultipleInstances IgnoreNew `
        -DisallowDemandStart `
        -RestartCount 0 `
        -Priority 7
    $registrationAttempted = $true
    $null = Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "FNO V8-Combined PAPER shadow; approval-gated and disabled by default." `
        -Force `
        -ErrorAction Stop

    $installed = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    if (-not [string]::Equals([string]$installed.TaskName, $frozenTaskName, [System.StringComparison]::Ordinal) -or
        -not [string]::Equals([string]$installed.TaskPath, "\", [System.StringComparison]::Ordinal)) {
        throw "Fail-closed verification failed: task identity/root path mismatch."
    }
    if ([bool]$installed.Settings.Enabled -or [string]$installed.State -ne "Disabled") {
        throw "Fail-closed verification failed: $TaskName is not disabled."
    }
    if (-not [string]::Equals(
            [string]$installed.Principal.UserId,
            $frozenUserId,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        -not [string]::Equals(
            [string]$installed.Principal.LogonType,
            "Interactive",
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        -not [string]::Equals(
            [string]$installed.Principal.RunLevel,
            "Limited",
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "Fail-closed verification failed: principal must be Saarit/Interactive/Limited."
    }
    if (@($installed.Actions).Count -ne 1) {
        throw "Fail-closed verification failed: expected exactly one task action."
    }
    $actionPath = [string]$installed.Actions[0].Execute
    if (-not [string]::Equals($actionPath, $runner, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Installed action mismatch: expected '$runner', observed '$actionPath'."
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$installed.Actions[0].Arguments)) {
        throw "Fail-closed verification failed: task action arguments must be empty."
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$installed.Actions[0].WorkingDirectory)) {
        throw "Fail-closed verification failed: task working directory must be empty."
    }
    if ([bool]$installed.Settings.StartWhenAvailable) {
        throw "Fail-closed verification failed: StartWhenAvailable must be false."
    }
    if ([string]$installed.Settings.MultipleInstances -ne "IgnoreNew") {
        throw "Fail-closed verification failed: MultipleInstances must be IgnoreNew."
    }
    if ([string]$installed.Settings.ExecutionTimeLimit -ne "PT0S") {
        throw "Fail-closed verification failed: ExecutionTimeLimit must be PT0S."
    }
    if ([bool]$installed.Settings.AllowDemandStart) {
        throw "Fail-closed verification failed: AllowDemandStart must be false."
    }
    if ([int]$installed.Settings.RestartCount -ne 0 -or
        -not [string]::IsNullOrWhiteSpace([string]$installed.Settings.RestartInterval)) {
        throw "Fail-closed verification failed: automatic task restart must remain disabled."
    }
    if ($installed.Triggers.Count -ne 1) {
        throw "Fail-closed verification failed: expected exactly one weekly trigger."
    }
    $installedTrigger = $installed.Triggers[0]
    if (-not [bool]$installedTrigger.Enabled) {
        throw "Fail-closed verification failed: the sole 09:15 trigger must be enabled."
    }
    $startBoundary = [DateTimeOffset]::Parse([string]$installedTrigger.StartBoundary)
    $startBoundaryIst = [TimeZoneInfo]::ConvertTime($startBoundary, $indiaTimeZone)
    if ($startBoundaryIst.Hour -ne 9 -or $startBoundaryIst.Minute -ne 15 -or $startBoundaryIst.Second -ne 0) {
        throw "Fail-closed verification failed: trigger is not exactly 09:15."
    }
    if ([int]$installedTrigger.DaysOfWeek -ne 62 -or [int]$installedTrigger.WeeksInterval -ne 1) {
        throw "Fail-closed verification failed: trigger is not weekly Monday-Friday."
    }
    $triggerExecutionTimeLimit = [string]$installedTrigger.ExecutionTimeLimit
    if (-not [string]::IsNullOrWhiteSpace($triggerExecutionTimeLimit) -and
        -not [string]::Equals(
            $triggerExecutionTimeLimit,
            "PT0S",
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "Fail-closed verification failed: trigger ExecutionTimeLimit must be empty or PT0S."
    }
    $repetitionInterval = ""
    $repetitionDuration = ""
    if ($null -ne $installedTrigger.Repetition) {
        $repetitionInterval = [string]$installedTrigger.Repetition.Interval
        $repetitionDuration = [string]$installedTrigger.Repetition.Duration
    }
    foreach ($value in @(
        [string]$installedTrigger.EndBoundary,
        [string]$installedTrigger.RandomDelay,
        $repetitionInterval,
        $repetitionDuration
    )) {
        if (-not [string]::IsNullOrWhiteSpace($value) -and $value -ne "PT0S") {
            throw "Fail-closed verification failed: trigger must not repeat, delay, or have an end boundary."
        }
    }

    if ([string]$installed.State -eq "Running") {
        throw "Fail-closed verification failed: $TaskName unexpectedly started."
    }
    Write-Output "[SUCCESS] Staged $TaskName disabled from creation and verified state=Disabled."
    Write-Output "[INFO] No V6 task was changed and the V8 session was not started."
}
catch {
    $failure = $_.Exception.Message
    # Fail closed even if registration succeeded before a later validation
    # step failed.  A partially staged V8 task must never be
    # left enabled and become runnable at the next 09:15 trigger.
    $forcedDisabled = $false
    if ($registrationAttempted) {
        try {
            & schtasks.exe /Change /TN $TaskName /Disable 2>$null | Out-Null
            $forcedDisabled = ($LASTEXITCODE -eq 0)
            if (-not $forcedDisabled) {
                $null = Disable-ScheduledTask -TaskName $TaskName -ErrorAction Stop
                $forcedDisabled = $true
            }
        }
        catch {
            $forcedDisabled = $false
        }
    }
    if ($registrationAttempted -and -not $forcedDisabled) {
        Write-Warning (
            "URGENT: could not force-disable partially staged task $TaskName; " +
            "disable or delete it manually before 09:15."
        )
    }
    $cleanup = if ($registrationAttempted) { "forced-disable attempted" } else { "no task mutation" }
    Write-Error "V8 paper task staging failed ($cleanup): $failure"
    exit 1
}
