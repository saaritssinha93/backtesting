[CmdletBinding()]
param(
    [switch]$Execute,
    [string]$ApprovalPhrase = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$requiredApprovalPhrase = "I_APPROVE_FNO_V6_1M_TO_V8_COMBINED_PAPER"
$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$pythonExe = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) {
    $pythonExe = "python"
}
$v8Runner = Join-Path $baseDir "bat\run_fno_v8_combined_paper_session.bat"
$v8Session = Join-Path $baseDir "fno_v8_combined_paper_session.py"
$v8TaskName = "EQIDV2_fno_v8_combined_paper_0915"
$v8TaskUserId = "Saarit"
$indiaTimeZone = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
$v6ScannerTaskName = "EQIDV2_fno_v6_scanner_5min_0918"
$v6FeedTaskName = "EQIDV2_fno_v6_equity_1min_feed_0919"

# This is the complete and intentionally narrow cutover scope. The V6 scanner,
# universe, futures fetcher, feature ranker and EOD QC remain enabled.
$v6DownstreamTasks = @(
    "EQIDV2_fno_v6_equity_1min_feed_0919",
    "EQIDV2_fno_v6_confirmation_1min_0919",
    "EQIDV2_fno_v6_live_long_0920",
    "EQIDV2_fno_v6_live_short_0920",
    "EQIDV2_fno_v6_live_kite_qty1_0915",
    "EQIDV2_fno_v6_trade_logger_0920",
    "EQIDV2_fno_v6_net_result_0920"
)
$v6ConsumerTasks = @(
    $v6DownstreamTasks | Where-Object { $_ -ne $v6FeedTaskName }
)

if (-not $Execute) {
    Write-Error (
        "Refusing to change scheduler state. Re-run with -Execute and " +
        "-ApprovalPhrase '$requiredApprovalPhrase' after reviewing the runbook."
    )
    exit 2
}
if (-not [string]::Equals(
    $ApprovalPhrase,
    $requiredApprovalPhrase,
    [System.StringComparison]::Ordinal
)) {
    Write-Error "Approval phrase mismatch; no scheduler state was changed."
    exit 2
}
# Leave a two-minute safety margin before the 09:15 trigger.  This prevents
# network preflight and scheduler mutations from racing V6 startup.
$cutoverDeadline = [TimeSpan]::Parse("09:13:00")
if ((Get-Date).TimeOfDay -ge $cutoverDeadline) {
    Write-Error "Cutover must begin and pass preflight before 09:13; no scheduler state was changed."
    exit 2
}

function Get-ManagedTask {
    param([Parameter(Mandatory = $true)][string]$TaskName)

    return Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
}

function Test-TaskEnabled {
    param([Parameter(Mandatory = $true)]$Task)

    return [bool]$Task.Settings.Enabled
}

function Assert-ExactTaskState {
    param(
        [Parameter(Mandatory = $true)]$Task,
        [Parameter(Mandatory = $true)][string]$Expected,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $observed = [string]$Task.State
    if (-not [string]::Equals($observed, $Expected, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must be state=$Expected, observed=$observed."
    }
}

function Assert-V8TaskDefinition {
    param([Parameter(Mandatory = $true)]$Task)

    if (@($Task.Actions).Count -ne 1) {
        throw "V8 task must have exactly one action."
    }
    if (-not [string]::Equals([string]$Task.TaskName, $v8TaskName, [System.StringComparison]::Ordinal)) {
        throw "V8 task name identity mismatch."
    }
    if (-not [string]::Equals([string]$Task.TaskPath, "\", [System.StringComparison]::Ordinal)) {
        throw "V8 task must be the unique root-folder task."
    }
    if (-not [string]::Equals(
            [string]$Task.Principal.UserId,
            $v8TaskUserId,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        -not [string]::Equals(
            [string]$Task.Principal.LogonType,
            "Interactive",
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        -not [string]::Equals(
            [string]$Task.Principal.RunLevel,
            "Limited",
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "V8 task principal must remain Saarit/Interactive/Limited."
    }
    $actionPath = [string]$Task.Actions[0].Execute
    if (-not [string]::Equals(
        $actionPath,
        $v8Runner,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "V8 task action mismatch: expected '$v8Runner', observed '$actionPath'."
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$Task.Actions[0].Arguments)) {
        throw "V8 task action arguments must be empty."
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$Task.Actions[0].WorkingDirectory)) {
        throw "V8 task working directory must be empty."
    }
    if ([bool]$Task.Settings.StartWhenAvailable) {
        throw "V8 task StartWhenAvailable must remain false."
    }
    if (-not [string]::Equals(
        [string]$Task.Settings.MultipleInstances,
        "IgnoreNew",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "V8 task MultipleInstances must remain IgnoreNew."
    }
    if (-not [string]::Equals(
        [string]$Task.Settings.ExecutionTimeLimit,
        "PT0S",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "V8 task ExecutionTimeLimit must remain PT0S."
    }
    if ([bool]$Task.Settings.AllowDemandStart) {
        throw "V8 task AllowDemandStart must remain false."
    }
    if ([int]$Task.Settings.RestartCount -ne 0 -or
        -not [string]::IsNullOrWhiteSpace([string]$Task.Settings.RestartInterval)) {
        throw "V8 task automatic restart must remain disabled."
    }
    if (@($Task.Triggers).Count -ne 1) {
        throw "V8 task must have exactly one trigger."
    }
    $trigger = $Task.Triggers[0]
    if (-not [bool]$trigger.Enabled) {
        throw "V8 task 09:15 trigger must be enabled."
    }
    $startBoundary = [DateTimeOffset]::Parse([string]$trigger.StartBoundary)
    $startBoundaryIst = [TimeZoneInfo]::ConvertTime($startBoundary, $indiaTimeZone)
    if ($startBoundaryIst.Hour -ne 9 -or $startBoundaryIst.Minute -ne 15 -or $startBoundaryIst.Second -ne 0) {
        throw "V8 task trigger must be exactly 09:15."
    }
    if ([int]$trigger.DaysOfWeek -ne 62 -or [int]$trigger.WeeksInterval -ne 1) {
        throw "V8 task trigger must be weekly Monday-Friday."
    }
    $triggerExecutionTimeLimit = [string]$trigger.ExecutionTimeLimit
    if (-not [string]::IsNullOrWhiteSpace($triggerExecutionTimeLimit) -and
        -not [string]::Equals(
            $triggerExecutionTimeLimit,
            "PT0S",
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
        throw "V8 task trigger ExecutionTimeLimit must be empty or PT0S."
    }
    $repetitionInterval = ""
    $repetitionDuration = ""
    if ($null -ne $trigger.Repetition) {
        $repetitionInterval = [string]$trigger.Repetition.Interval
        $repetitionDuration = [string]$trigger.Repetition.Duration
    }
    foreach ($value in @(
        [string]$trigger.EndBoundary,
        [string]$trigger.RandomDelay,
        $repetitionInterval,
        $repetitionDuration
    )) {
        if (-not [string]::IsNullOrWhiteSpace($value) -and $value -ne "PT0S") {
            throw "V8 task trigger must not repeat, delay, or have an end boundary."
        }
    }
}

function Set-ManagedTaskEnabled {
    param(
        [Parameter(Mandatory = $true)][string]$TaskName,
        [Parameter(Mandatory = $true)][bool]$Enabled
    )

    if ($Enabled) {
        $null = Enable-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    }
    else {
        $null = Disable-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    }
}

$originalEnabled = @{}
$mutationsStarted = $false

try {
    if (-not (Test-Path -LiteralPath $v8Runner -PathType Leaf)) {
        throw "Missing V8 paper runner: $v8Runner"
    }
    if (-not (Test-Path -LiteralPath $v8Session -PathType Leaf)) {
        throw "Missing V8 paper session: $v8Session"
    }

    $runnerSource = Get-Content -LiteralPath $v8Runner -Raw -ErrorAction Stop
    if ($runnerSource -notmatch 'FNO_V8_COMBINED_EXECUTION_MODE=PAPER') {
        throw "V8 runner does not pin FNO_V8_COMBINED_EXECUTION_MODE=PAPER."
    }
    if ($runnerSource -notmatch 'fno_v8_combined_paper_session\.py') {
        throw "V8 runner does not target the approved paper-session entry point."
    }
    if ($runnerSource -match 'FNO_V8_COMBINED_EXECUTION_MODE=LIVE') {
        throw "V8 runner contains a LIVE execution-mode assignment."
    }
    $runnerSha256 = (Get-FileHash -LiteralPath $v8Runner -Algorithm SHA256).Hash

    $scannerTask = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $scannerTask)) {
        throw "Shared V6 5-minute scanner is not enabled: $v6ScannerTaskName"
    }
    Assert-ExactTaskState -Task $scannerTask -Expected "Ready" -Label "Shared V6 scanner"

    $v8Task = Get-ManagedTask -TaskName $v8TaskName
    if (Test-TaskEnabled -Task $v8Task) {
        throw "V8 paper task must be staged Disabled before this cutover."
    }
    Assert-ExactTaskState -Task $v8Task -Expected "Disabled" -Label "V8 paper task"
    Assert-V8TaskDefinition -Task $v8Task

    foreach ($taskName in $v6DownstreamTasks) {
        $task = Get-ManagedTask -TaskName $taskName
        if (-not (Test-TaskEnabled -Task $task)) {
            throw "V6 downstream task is not enabled; refusing a partial-state cutover: $taskName"
        }
        Assert-ExactTaskState -Task $task -Expected "Ready" -Label "V6 downstream task $taskName"
        $originalEnabled[$taskName] = $true
    }
    $originalEnabled[$v8TaskName] = $false

    # The scheduler approval is deliberately separate from the session's
    # one-date permit and kill switch.  Authenticate all eight market-data
    # apps before changing V6 so a bad/stale credential can never leave both
    # entry pipelines unavailable at the open.
    & $pythonExe -u $v8Session preflight --require-activation --authenticate-apps
    if ($LASTEXITCODE -ne 0) {
        throw "V8 activation/eight-app preflight failed; V6 remains enabled."
    }

    # Network authentication takes time.  Revalidate the boundary and every
    # task immediately before the first mutation so the 09:15 trigger cannot
    # race this cutover into overlapping V6/V8 processes.
    if ((Get-Date).TimeOfDay -ge $cutoverDeadline) {
        throw "V8 preflight crossed the 09:13 cutover deadline; V6 remains enabled."
    }
    $scannerTask = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $scannerTask)) {
        throw "V6 scanner changed state during preflight; no cutover was performed."
    }
    Assert-ExactTaskState -Task $scannerTask -Expected "Ready" -Label "Shared V6 scanner after preflight"
    $v8Task = Get-ManagedTask -TaskName $v8TaskName
    if (Test-TaskEnabled -Task $v8Task) {
        throw "V8 task changed state during preflight; no cutover was performed."
    }
    Assert-ExactTaskState -Task $v8Task -Expected "Disabled" -Label "V8 task after preflight"
    Assert-V8TaskDefinition -Task $v8Task
    $runnerSha256AfterPreflight = (Get-FileHash -LiteralPath $v8Runner -Algorithm SHA256).Hash
    if (-not [string]::Equals(
        $runnerSha256AfterPreflight,
        $runnerSha256,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "V8 runner changed during preflight; no cutover was performed."
    }
    foreach ($taskName in $v6DownstreamTasks) {
        $task = Get-ManagedTask -TaskName $taskName
        if (-not (Test-TaskEnabled -Task $task)) {
            throw "V6 downstream state changed during preflight; no cutover was performed: $taskName"
        }
        Assert-ExactTaskState -Task $task -Expected "Ready" -Label "V6 downstream after preflight $taskName"
    }

    # Recompute the complete bound runtime bundle and two-key activation after
    # the slower eight-app network authentication.  This closes source drift
    # across every Python/PowerShell/BAT dependency, not just the runner BAT.
    & $pythonExe -u $v8Session preflight --require-activation
    if ($LASTEXITCODE -ne 0) {
        throw "V8 runtime/activation changed during app authentication; V6 remains enabled."
    }
    if ((Get-Date).TimeOfDay -ge $cutoverDeadline) {
        throw "V8 final preflight crossed the 09:13 cutover deadline; V6 remains enabled."
    }
    $scannerTask = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $scannerTask)) {
        throw "V6 scanner changed during final preflight; no cutover was performed."
    }
    Assert-ExactTaskState -Task $scannerTask -Expected "Ready" -Label "Shared V6 scanner final preflight"
    $v8Task = Get-ManagedTask -TaskName $v8TaskName
    if (Test-TaskEnabled -Task $v8Task) {
        throw "V8 task changed during final preflight; no cutover was performed."
    }
    Assert-ExactTaskState -Task $v8Task -Expected "Disabled" -Label "V8 task final preflight"
    Assert-V8TaskDefinition -Task $v8Task
    foreach ($taskName in $v6DownstreamTasks) {
        $task = Get-ManagedTask -TaskName $taskName
        if (-not (Test-TaskEnabled -Task $task)) {
            throw "V6 downstream changed during final preflight; no cutover was performed: $taskName"
        }
        Assert-ExactTaskState -Task $task -Expected "Ready" -Label "V6 downstream final preflight $taskName"
    }

    Write-Output "[PREFLIGHT] V8 task is staged Disabled and PAPER-only."
    Write-Output "[PREFLIGHT] Today's V8 permit, kill switch and all eight apps are valid."
    Write-Output "[PREFLIGHT] Six V6 downstream tasks are enabled and idle."
    Write-Output "[PREFLIGHT] Shared V6 5-minute scanner is enabled and outside the mutation scope."

    $mutationsStarted = $true
    # Keep the required V6 data feed enabled while the five V6 consumers are
    # disabled.  Then enable/verify V8 before disabling the feed last.  This
    # ordering prevents the still-running preopen autofix loop from seeing
    # both pipelines disabled and launching the V6 feed BAT directly.
    foreach ($taskName in $v6ConsumerTasks) {
        Set-ManagedTaskEnabled -TaskName $taskName -Enabled $false
    }
    foreach ($taskName in $v6ConsumerTasks) {
        $disabledConsumer = Get-ManagedTask -TaskName $taskName
        if (Test-TaskEnabled -Task $disabledConsumer) {
            throw "Post-change verification found V6 task still enabled: $taskName"
        }
        Assert-ExactTaskState -Task $disabledConsumer -Expected "Disabled" -Label "Disabled V6 consumer $taskName"
    }

    Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $true
    $enabledV8Task = Get-ManagedTask -TaskName $v8TaskName
    if (-not (Test-TaskEnabled -Task $enabledV8Task)) {
        throw "Post-change verification found V8 paper task still disabled."
    }
    Assert-ExactTaskState -Task $enabledV8Task -Expected "Ready" -Label "Enabled V8 paper task"
    Assert-V8TaskDefinition -Task $enabledV8Task

    Set-ManagedTaskEnabled -TaskName $v6FeedTaskName -Enabled $false
    $disabledFeed = Get-ManagedTask -TaskName $v6FeedTaskName
    if (Test-TaskEnabled -Task $disabledFeed) {
        throw "Post-change verification found V6 feed still enabled: $v6FeedTaskName"
    }
    Assert-ExactTaskState -Task $disabledFeed -Expected "Disabled" -Label "Disabled V6 feed"
    foreach ($taskName in $v6DownstreamTasks) {
        $disabledDownstream = Get-ManagedTask -TaskName $taskName
        if (Test-TaskEnabled -Task $disabledDownstream) {
            throw "Post-change verification found V6 downstream still enabled: $taskName"
        }
        Assert-ExactTaskState -Task $disabledDownstream -Expected "Disabled" -Label "Disabled V6 downstream $taskName"
    }
    $unchangedScanner = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $unchangedScanner)) {
        throw "Shared V6 scanner changed state unexpectedly."
    }
    Assert-ExactTaskState -Task $unchangedScanner -Expected "Ready" -Label "Unchanged V6 scanner"

    Write-Output "[SUCCESS] Enabled $v8TaskName."
    Write-Output "[SUCCESS] Disabled exactly six V6 1-minute/downstream tasks."
    Write-Output "[UNCHANGED] $v6ScannerTaskName remains enabled."
    Write-Output "[INFO] No task was started; the new state applies at the next scheduled trigger."
    exit 0
}
catch {
    $failure = $_.Exception.Message
    $rollbackErrors = @()
    if ($mutationsStarted) {
        # Restore the required V6 feed first, then the five consumers, and
        # disable the V8 scheduler-mode marker last.  Hashtable iteration is
        # deliberately avoided: the preopen autofix must never observe both
        # V8 disabled and the required V6 feed disabled during rollback.
        foreach ($taskName in @($v6FeedTaskName) + $v6ConsumerTasks) {
            try {
                Set-ManagedTaskEnabled -TaskName $taskName -Enabled $true
                $restoredV6 = Get-ManagedTask -TaskName $taskName
                if (-not (Test-TaskEnabled -Task $restoredV6)) {
                    throw "task remained disabled"
                }
                Assert-ExactTaskState -Task $restoredV6 -Expected "Ready" -Label "Rollback V6 task $taskName"
            }
            catch {
                $rollbackErrors += "$taskName => $($_.Exception.Message)"
            }
        }
        try {
            Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $false
            $restoredV8 = Get-ManagedTask -TaskName $v8TaskName
            if (Test-TaskEnabled -Task $restoredV8) {
                throw "task remained enabled"
            }
            Assert-ExactTaskState -Task $restoredV8 -Expected "Disabled" -Label "Rollback V8 task"
        }
        catch {
            $rollbackErrors += "$v8TaskName => $($_.Exception.Message)"
        }
    }

    if ($rollbackErrors.Count -gt 0) {
        Write-Error (
            "Cutover failed: $failure | ROLLBACK INCOMPLETE: " +
            ($rollbackErrors -join " | ")
        )
    }
    elseif ($mutationsStarted) {
        Write-Error "Cutover failed and original task states were restored: $failure"
    }
    else {
        Write-Error "Cutover preflight failed; no task state was changed: $failure"
    }
    exit 1
}
