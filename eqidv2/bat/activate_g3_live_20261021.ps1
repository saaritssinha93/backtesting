[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$SessionDate = '2026-10-21'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$AuthTask = 'EQIDV2_authentication_v2_0900'
$Workspace = Split-Path $PSScriptRoot -Parent
$Python = 'C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe'
$Approval = Join-Path $PSScriptRoot 'approve_g3_live.ps1'
$HealthPath = Join-Path $Workspace 'logs\preopen_session_healthcheck_latest.json'
$AuthStatusPath = Join-Path $Workspace 'logs\authentication_v2_runner.status'
$ProfilePath = Join-Path $Workspace 'configs\strategies\active_profile.json'
$ArmPath = 'C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live\live_arm.json'
$LogPath = Join-Path $Workspace 'logs\g3_live_activation_2026-10-21.log'
$ExpectedHealthWarning = 'acceptance pending: waiting for the first canonical 09:20 stock-futures marker'
$StatusHelpers = Join-Path $PSScriptRoot 'g3_activation_status_helpers.ps1'

if (-not (Test-Path -LiteralPath $StatusHelpers -PathType Leaf)) {
    throw "Activation status parser is missing: $StatusHelpers"
}
. $StatusHelpers

function Write-G3ActivationLog([string]$Message) {
    try {
        $parent = Split-Path $LogPath -Parent
        if (-not (Test-Path -LiteralPath $parent)) {
            New-Item -ItemType Directory -Path $parent | Out-Null
        }
        Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value (
            "[$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))] $Message"
        )
    } catch {
        # A log sink can never turn a failed gate into an approved session.
    }
}

function Read-StrictJsonWithRetry([string]$Path) {
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
                throw "Missing JSON evidence: $Path"
            }
            return (Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop)
        } catch {
            if ($attempt -eq 3) { throw }
            Start-Sleep -Milliseconds 200
        }
    }
}

function Assert-TargetIsReadinessOnly {
    $task = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    if ($task.State -ne 'Disabled' -or $task.Settings.Enabled) {
        throw 'G3 LIVE target must still be disabled before approval.'
    }
    $actions = @($task.Actions)
    if ($actions.Count -ne 1 -or
        ($actions[0].Execute + ' ' + $actions[0].Arguments) -notlike '*run_fno_v13_v10_g_3_live_kite_qty1.bat*' -or
        ($actions[0].Execute + ' ' + $actions[0].Arguments) -like '*run_g3_approved_live.bat*') {
        throw 'G3 LIVE target is not the reviewed readiness-only action.'
    }
    if ([string]$task.Principal.LogonType -ne 'Interactive' -or
        [string]$task.Principal.RunLevel -ne 'Limited') {
        throw 'G3 LIVE target principal differs from the reviewed Interactive/Limited principal.'
    }
    $trigger = @($task.Triggers)[0]
    if ($null -eq $trigger -or [datetime]$trigger.StartBoundary -ne
        ([datetime]::ParseExact('2026-10-20 09:15:00', 'yyyy-MM-dd HH:mm:ss',
            [Globalization.CultureInfo]::InvariantCulture))) {
        # The date portion is the task's next weekday boundary and can roll.
        if ($null -eq $trigger -or ([datetime]$trigger.StartBoundary).TimeOfDay -ne
            ([timespan]::FromHours(9.25))) {
            throw 'G3 LIVE target no longer has its reviewed 09:15 trigger.'
        }
    }
}

function Assert-AuthenticationReady([datetime]$Now) {
    $task = Get-ScheduledTask -TaskName $AuthTask -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $AuthTask -ErrorAction Stop
    if ($task.State -eq 'Running' -or $info.LastRunTime.Date -ne $Now.Date -or
        [int]$info.LastTaskResult -ne 0) {
        throw 'Authentication task has not completed successfully for this session.'
    }
    if (-not (Test-Path -LiteralPath $AuthStatusPath -PathType Leaf)) {
        throw 'Authentication status evidence is missing.'
    }
    $status = Read-StrictLiteralKeyValueFile -Path $AuthStatusPath -RequiredKeys @(
        'status', 'script', 'ts', 'exit_code', 'log_file'
    )
    $stamp = [datetime]::ParseExact(
        [string]$status.ts, 'yyyy-MM-dd_HH:mm:ss',
        [Globalization.CultureInfo]::InvariantCulture
    )
    if ($status.status -ne 'SUCCESS' -or
        $status.script -ne 'authentication_v2.py' -or
        [int]$status.exit_code -ne 0 -or
        $stamp.Date -ne $Now.Date) {
        throw 'Authentication status file is not a same-session success.'
    }
}

function Assert-PreopenHealth([datetime]$Now) {
    $health = Read-StrictJsonWithRetry $HealthPath
    $stamp = [datetimeoffset]::Parse(
        [string]$health.ts_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $nowOffset = [datetimeoffset]$Now
    $age = ($nowOffset - $stamp).TotalSeconds
    $notBefore = $Now.Date.AddHours(9).AddMinutes(6)
    if ($stamp.Date -ne $Now.Date -or $stamp.LocalDateTime -lt $notBefore -or
        $age -lt -5 -or $age -gt 120) {
        throw 'Pre-open health evidence is stale, early, or from another session.'
    }
    $checks = @($health.checks)
    if (-not $checks.Count) { throw 'Pre-open health evidence has no checks.' }
    $failed = @($checks | Where-Object { $_.status -eq 'FAIL' })
    $warned = @($checks | Where-Object { $_.status -eq 'WARN' })
    $unknown = @($checks | Where-Object { $_.status -notin @('PASS', 'WARN', 'FAIL') })
    if ($failed.Count -or $unknown.Count) {
        throw 'Pre-open health contains a failed or unknown gate.'
    }
    if ([string]$health.overall -eq 'WAIT') {
        if ($warned.Count -ne 1 -or
            [string]$warned[0].name -ne 'fno_fast_production_first_slot' -or
            [string]$warned[0].detail -ne $ExpectedHealthWarning) {
            throw 'Pre-open WAIT contains a warning other than the reviewed pre-09:20 marker wait.'
        }
    } elseif ([string]$health.overall -eq 'PASS') {
        if ($warned.Count) { throw 'Pre-open PASS contains a warning.' }
    } else {
        throw 'Pre-open overall state is not an acceptable PASS/expected WAIT.'
    }
    foreach ($required in @(
        'task_EQIDV2_authentication_v2_0900',
        'authentication_v2',
        'fno_fast_production_runtime',
        'preopen_healthcheck'
    )) {
        $match = @($checks | Where-Object { $_.name -eq $required })
        if ($match.Count -ne 1 -or [string]$match[0].status -ne 'PASS') {
            throw "Required pre-open gate did not pass: $required"
        }
    }
}

function Assert-ApprovedState {
    $task = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $TargetTask -ErrorAction Stop
    if (-not $task.Settings.Enabled -or $task.State -eq 'Disabled' -or $task.State -eq 'Running' -or
        $task.Settings.StartWhenAvailable -or [int]$task.Settings.RestartCount -ne 0 -or
        [string]$task.Settings.MultipleInstances -ne 'IgnoreNew') {
        throw 'G3 LIVE target was not enabled in a pre-trigger Ready state.'
    }
    $actionText = (@($task.Actions) | ForEach-Object { $_.Execute + ' ' + $_.Arguments }) -join ' | '
    if ($actionText -notlike '*run_g3_approved_live.bat*') {
        throw 'G3 LIVE target does not use the approved launcher.'
    }
    if ($info.NextRunTime.Date -ne [datetime]::ParseExact(
            $SessionDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture).Date -or
        $info.NextRunTime.TimeOfDay -ne [timespan]::FromHours(9.25)) {
        throw 'G3 LIVE target does not have the expected 09:15 run for this session.'
    }
    $profile = Read-StrictJsonWithRetry $ProfilePath
    $arm = Read-StrictJsonWithRetry $ArmPath
    if ($profile.deployment_stage -ne 'LIVE_APPROVED' -or
        $profile.live_activation_authorized -ne $true -or
        $profile.live_approval.session_date -ne $SessionDate -or
        $arm.enabled -ne $true -or $arm.session_date -ne $SessionDate -or
        $profile.strategy_fingerprint -ne $arm.strategy_fingerprint -or
        $profile.live_execution_policy.live_execution_policy_fingerprint -ne
            $arm.live_execution_policy_fingerprint -or
        $profile.live_approval.live_execution_policy_fingerprint -ne
            $arm.live_execution_policy_fingerprint) {
        throw 'G3 LIVE dated approval, arm, strategy, or execution policy did not reconcile.'
    }
    if ($profile.live_execution_policy.capital_per_trade_rs -ne 10000.0 -or
        $profile.live_execution_policy.leverage -ne 5.0 -or
        $profile.live_execution_policy.target_exposure_rs -ne 50000.0 -or
        $null -ne $profile.live_execution_policy.max_reserved_open_positions -or
        $null -ne $profile.live_execution_policy.max_daily_entries -or
        $null -ne $profile.live_execution_policy.max_reserved_gross_exposure_rs) {
        throw 'G3 LIVE capital or unlimited portfolio-admission policy differs from the approved policy.'
    }
}

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Frozen Python interpreter is missing: $Python"
}
if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
    throw 'The activation host is not using India Standard Time.'
}

$activationMutex = New-Object System.Threading.Mutex(
    $false, 'Local\EQIDV2_G3_LIVE_ACTIVATION_20261021'
)
if (-not $activationMutex.WaitOne(0)) {
    throw 'Another 21 October G3 activation attempt is already running.'
}
try {
    $now = Get-Date
    $session = [datetime]::ParseExact(
        $SessionDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture
    )
    $windowStart = $session.Date.AddHours(9).AddMinutes(9)
    $windowEnd = $session.Date.AddHours(9).AddMinutes(13)
    if ($now.Date -ne $session.Date -or $now -lt $windowStart -or $now -ge $windowEnd) {
        throw 'This approval is valid only on 2026-10-21 from 09:09:00 through 09:12:59 IST.'
    }
    Write-G3ActivationLog 'Starting fail-closed dated activation checks.'
    Assert-TargetIsReadinessOnly
    Assert-AuthenticationReady $now
    Assert-PreopenHealth $now
    Assert-TargetIsReadinessOnly

    $approvalOutput = @(
        & $Approval -SessionDate $SessionDate -Apply -Python $Python -EnableBefore '09:12:30'
    )
    if ($LASTEXITCODE -ne 0) { throw 'Dated approval wrapper returned a failure.' }
    $approvalOutput | ForEach-Object { Write-G3ActivationLog ([string]$_) }
    Assert-ApprovedState
    Write-G3ActivationLog 'SUCCESS: G3 LIVE is approved for 2026-10-21; 09:15 task is enabled.'
} catch {
    Write-G3ActivationLog ("BLOCKED: " + $_.Exception.Message)
    try {
        Disable-ScheduledTask -TaskName $TargetTask -ErrorAction SilentlyContinue | Out-Null
    } catch {
    }
    throw
} finally {
    $activationMutex.ReleaseMutex()
    $activationMutex.Dispose()
}
