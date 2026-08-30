[CmdletBinding()]
param(
    [switch]$Execute,
    [string]$ApprovalPhrase = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$requiredPhrase = "I_RESTORE_FNO_V6_1M_AND_DISABLE_V8_PAPER"
$v8TaskName = "EQIDV2_fno_v8_combined_paper_0915"
$v6ScannerTaskName = "EQIDV2_fno_v6_scanner_5min_0918"
$v6DownstreamTasks = @(
    "EQIDV2_fno_v6_equity_1min_feed_0919",
    "EQIDV2_fno_v6_confirmation_1min_0919",
    "EQIDV2_fno_v6_live_long_0920",
    "EQIDV2_fno_v6_live_short_0920",
    "EQIDV2_fno_v6_trade_logger_0920",
    "EQIDV2_fno_v6_net_result_0920"
)

if (-not $Execute -or -not [string]::Equals(
    $ApprovalPhrase,
    $requiredPhrase,
    [System.StringComparison]::Ordinal
)) {
    Write-Error (
        "No task state changed. Re-run with -Execute and " +
        "-ApprovalPhrase '$requiredPhrase'."
    )
    exit 2
}

# This is an early-preopen state restoration, never a post-close or mid-market
# process switch. V6 tasks use StartWhenAvailable=True; enabling them after a
# missed 09:15 trigger could launch catch-up work immediately.
$now = (Get-Date).TimeOfDay
$restoreDeadline = [TimeSpan]::Parse("08:55:00")
if ($now -ge $restoreDeadline) {
    Write-Error "Restore must complete before 08:55; no state changed."
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

$original = @{}
$mutationsStarted = $false

try {
    $scanner = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $scanner)) {
        throw "Shared V6 scanner must be enabled and idle before restoration."
    }
    Assert-ExactTaskState -Task $scanner -Expected "Ready" -Label "Shared V6 scanner"

    $v8Before = Get-ManagedTask -TaskName $v8TaskName
    $v8BeforeEnabled = Test-TaskEnabled -Task $v8Before
    Assert-ExactTaskState `
        -Task $v8Before `
        -Expected $(if ($v8BeforeEnabled) { "Ready" } else { "Disabled" }) `
        -Label "V8 paper task"
    $original[$v8TaskName] = $v8BeforeEnabled
    foreach ($taskName in $v6DownstreamTasks) {
        $task = Get-ManagedTask -TaskName $taskName
        if (Test-TaskEnabled -Task $task) {
            throw "V6 downstream task must still be disabled before restoration: $taskName"
        }
        Assert-ExactTaskState -Task $task -Expected "Disabled" -Label "V6 downstream task $taskName"
        $original[$taskName] = $false
    }

    if ((Get-Date).TimeOfDay -ge $restoreDeadline) {
        throw "Restore preflight crossed the 08:55 deadline; no state changed."
    }

    $mutationsStarted = $true
    # Disable V8 first so there is never an interval with both entry pipelines
    # enabled for the next trigger.
    Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $false
    foreach ($taskName in $v6DownstreamTasks) {
        Set-ManagedTaskEnabled -TaskName $taskName -Enabled $true
    }

    $v8After = Get-ManagedTask -TaskName $v8TaskName
    if (Test-TaskEnabled -Task $v8After) {
        throw "V8 paper task remained enabled after restoration."
    }
    Assert-ExactTaskState -Task $v8After -Expected "Disabled" -Label "Restored V8 task"
    foreach ($taskName in $v6DownstreamTasks) {
        $v6After = Get-ManagedTask -TaskName $taskName
        if (-not (Test-TaskEnabled -Task $v6After)) {
            throw "V6 downstream task remained disabled: $taskName"
        }
        Assert-ExactTaskState -Task $v6After -Expected "Ready" -Label "Restored V6 downstream $taskName"
    }
    $scannerAfter = Get-ManagedTask -TaskName $v6ScannerTaskName
    if (-not (Test-TaskEnabled -Task $scannerAfter)) {
        throw "Shared V6 scanner changed state unexpectedly."
    }
    Assert-ExactTaskState -Task $scannerAfter -Expected "Ready" -Label "Restored V6 scanner"

    Write-Output "[SUCCESS] V8 paper task is disabled."
    Write-Output "[SUCCESS] All six V6 1-minute/downstream tasks are enabled."
    Write-Output "[INFO] No scheduled task was started or ended."
    exit 0
}
catch {
    $failure = $_.Exception.Message
    $rollbackErrors = @()
    if ($mutationsStarted) {
        # Re-establish the prior V8 mode marker before disabling restored V6
        # definitions.  Then disable V6 consumers and the feed deterministically.
        if ([bool]$original[$v8TaskName]) {
            try {
                Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $true
            }
            catch {
                $rollbackErrors += "$v8TaskName => $($_.Exception.Message)"
            }
        }
        $v6FeedTaskName = "EQIDV2_fno_v6_equity_1min_feed_0919"
        $v6ConsumerTasks = @(
            $v6DownstreamTasks | Where-Object { $_ -ne $v6FeedTaskName }
        )
        foreach ($taskName in @($v6ConsumerTasks) + @($v6FeedTaskName)) {
            try {
                Set-ManagedTaskEnabled -TaskName $taskName -Enabled $false
            }
            catch {
                $rollbackErrors += "$taskName => $($_.Exception.Message)"
            }
        }
        if (-not [bool]$original[$v8TaskName]) {
            try {
                Set-ManagedTaskEnabled -TaskName $v8TaskName -Enabled $false
            }
            catch {
                $rollbackErrors += "$v8TaskName => $($_.Exception.Message)"
            }
        }
    }
    if ($rollbackErrors.Count) {
        Write-Error "Restore failed: $failure | ROLLBACK INCOMPLETE: $($rollbackErrors -join ' | ')"
    }
    elseif ($mutationsStarted) {
        Write-Error "Restore failed and original enabled states were restored: $failure"
    }
    else {
        Write-Error "Restore preflight failed; no task state changed: $failure"
    }
    exit 1
}
