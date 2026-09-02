$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$runner = Join-Path $baseDir "bat\run_fno_v10_v11_v12_paper_session.bat"
$sessionScript = Join-Path $baseDir "fno_multi_paper_session.py"
$taskName = "EQIDV2_fno_v10_v11_v12_paper_0915"
$sessionId = "fno_v10_v11_v12_paper"
$startTime = "09:15"
$restartCount = 5
$restartInterval = [TimeSpan]::FromMinutes(1)
$frozenUserId = "Saarit"
$indiaTimeZone = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
$registrationAttempted = $false

function Get-TaskIfPresent {
    param([Parameter(Mandatory = $true)][string]$Name)

    try {
        return Get-ScheduledTask -TaskName $Name -ErrorAction Stop
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

function Test-TaskDefinition {
    param(
        [Parameter(Mandatory = $true)]$Task,
        [Parameter(Mandatory = $true)][bool]$ExpectedEnabled
    )

    if (-not [string]::Equals(
        [string]$Task.TaskName,
        $taskName,
        [System.StringComparison]::Ordinal
    ) -or -not [string]::Equals(
        [string]$Task.TaskPath,
        "\",
        [System.StringComparison]::Ordinal
    )) {
        throw "Task identity/root path mismatch."
    }
    if ([bool]$Task.Settings.Enabled -ne $ExpectedEnabled) {
        throw "Task enabled state mismatch; expected Enabled=$ExpectedEnabled."
    }
    if (@($Task.Actions).Count -ne 1) {
        throw "Expected exactly one task action."
    }
    $action = $Task.Actions[0]
    if (-not [string]::Equals(
        [string]$action.Execute,
        $runner,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Task action must point only to the frozen PAPER runner."
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$action.Arguments) -or
        -not [string]::IsNullOrWhiteSpace([string]$action.WorkingDirectory)) {
        throw "Task action arguments and working directory must be empty."
    }
    if (-not [string]::Equals(
        [string]$Task.Principal.UserId,
        $frozenUserId,
        [System.StringComparison]::OrdinalIgnoreCase
    ) -or -not [string]::Equals(
        [string]$Task.Principal.LogonType,
        "Interactive",
        [System.StringComparison]::OrdinalIgnoreCase
    ) -or -not [string]::Equals(
        [string]$Task.Principal.RunLevel,
        "Limited",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Principal must be Saarit/Interactive/Limited."
    }
    if ([bool]$Task.Settings.StartWhenAvailable) {
        throw "StartWhenAvailable must be false; missed 09:15 triggers must not catch up."
    }
    if (-not [bool]$Task.Settings.WakeToRun) {
        throw "WakeToRun must be true so the prospective 09:15 start is not missed."
    }
    if ([string]$Task.Settings.MultipleInstances -ne "IgnoreNew") {
        throw "MultipleInstances must be IgnoreNew."
    }
    if ([string]$Task.Settings.ExecutionTimeLimit -ne "PT0S") {
        throw "ExecutionTimeLimit must be PT0S."
    }
    if (-not [bool]$Task.Settings.AllowDemandStart) {
        throw "Demand starts must be enabled for the dashboard restart contract."
    }
    if ([int]$Task.Settings.RestartCount -ne $restartCount -or
        -not [string]::Equals(
            [string]$Task.Settings.RestartInterval,
            "PT1M",
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "Task restart policy must be five attempts at one-minute intervals."
    }
    if (@($Task.Triggers).Count -ne 1) {
        throw "Expected exactly one weekly trigger."
    }
    $trigger = $Task.Triggers[0]
    if (-not [bool]$trigger.Enabled) {
        throw "The sole weekly trigger must be enabled."
    }
    $startBoundary = [DateTimeOffset]::Parse([string]$trigger.StartBoundary)
    $startBoundaryIst = [TimeZoneInfo]::ConvertTime($startBoundary, $indiaTimeZone)
    if ($startBoundaryIst.Hour -ne 9 -or
        $startBoundaryIst.Minute -ne 15 -or
        $startBoundaryIst.Second -ne 0) {
        throw "Trigger must be exactly 09:15 India time."
    }
    if ([int]$trigger.DaysOfWeek -ne 62 -or [int]$trigger.WeeksInterval -ne 1) {
        throw "Trigger must be weekly Monday-Friday."
    }
    foreach ($value in @(
        [string]$trigger.EndBoundary,
        [string]$trigger.RandomDelay,
        [string]$trigger.Repetition.Interval,
        [string]$trigger.Repetition.Duration
    )) {
        if (-not [string]::IsNullOrWhiteSpace($value) -and $value -ne "PT0S") {
            throw "Trigger must not repeat, delay, or have an end boundary."
        }
    }
}

try {
    if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
        throw "Missing PAPER runner: $runner"
    }
    if (-not (Test-Path -LiteralPath $sessionScript -PathType Leaf)) {
        throw "Missing session entry point: $sessionScript"
    }

    $runnerSource = Get-Content -LiteralPath $runner -Raw
    if ($runnerSource -notmatch 'FNO_MULTI_PAPER_EXECUTION_MODE=PAPER' -or
        $runnerSource -notmatch 'FNO_MULTI_PAPER_SESSION_ID=fno_v10_v11_v12_paper' -or
        $runnerSource -notmatch 'fno_multi_paper_session\.py' -or
        $runnerSource -notmatch '"%SESSION_SCRIPT%" run') {
        throw "Runner does not match the frozen PAPER/session/entry-point contract."
    }
    if ($runnerSource -match 'FNO_MULTI_PAPER_EXECUTION_MODE=LIVE') {
        throw "Runner contains a forbidden LIVE execution mode."
    }

    $existing = Get-TaskIfPresent -Name $taskName
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

    Write-Output "[INFO] Staging one PAPER-only task $taskName disabled for verification ..."
    $action = New-ScheduledTaskAction -Execute $runner
    $principal = New-ScheduledTaskPrincipal `
        -UserId $frozenUserId `
        -LogonType Interactive `
        -RunLevel Limited
    $trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
        -At $startTime
    # Demand start stays enabled so the dashboard's explicit Restart action
    # works.  The Python session remains the authority for trading-day,
    # prospective-start, immutable-checkpoint, and PAPER-only gates.
    $settings = New-ScheduledTaskSettingsSet `
        -Disable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -WakeToRun `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -MultipleInstances IgnoreNew `
        -RestartCount $restartCount `
        -RestartInterval $restartInterval `
        -Priority 7

    $registrationAttempted = $true
    $null = Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "FnO V10/V11/V12 shared PAPER session ($sessionId); one weekday coordinator." `
        -Force `
        -ErrorAction Stop

    $staged = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    Test-TaskDefinition -Task $staged -ExpectedEnabled $false
    if (-not [string]::Equals(
        [string]$staged.State,
        "Disabled",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Staged task was not Disabled before final enable."
    }

    # This enables only future weekly triggers. StartWhenAvailable=False was
    # verified above, so an already missed 09:15 trigger is never replayed.
    $null = Enable-ScheduledTask -TaskName $taskName -ErrorAction Stop
    $installed = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    Test-TaskDefinition -Task $installed -ExpectedEnabled $true
    if ([string]::Equals(
        [string]$installed.State,
        "Running",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Task unexpectedly started during installation."
    }

    Write-Output "[SUCCESS] Scheduled $taskName for weekdays at 09:15 India time."
    Write-Output "[INFO] State=$($installed.State); StartWhenAvailable=False; WakeToRun=True; AllowDemandStart=True; restart=5x/PT1M; no task run was requested."
}
catch {
    $failure = $_.Exception.Message
    $forcedDisabled = $false
    if ($registrationAttempted) {
        try {
            $null = Disable-ScheduledTask -TaskName $taskName -ErrorAction Stop
            $forcedDisabled = $true
        }
        catch {
            $forcedDisabled = $false
        }
    }
    if ($registrationAttempted -and -not $forcedDisabled) {
        Write-Warning "URGENT: could not disable partially installed task $taskName."
    }
    $cleanup = if ($registrationAttempted) { "forced-disable attempted" } else { "no task mutation" }
    Write-Error "Multi-strategy PAPER scheduling failed ($cleanup): $failure"
    exit 1
}
