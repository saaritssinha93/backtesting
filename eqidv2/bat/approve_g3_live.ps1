[CmdletBinding()]
param(
    [string]$SessionDate = '',
    [switch]$Apply,
    [string]$Python = 'python',
    [string]$EnableBefore = ''
)
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
$Operator = Join-Path $Workspace 'tools/g3_live_operator.py'
$enableDeadline = $null
if ($EnableBefore) {
    if (-not $SessionDate) { throw '-EnableBefore requires an explicit -SessionDate.' }
    $enableDeadline = [datetime]::ParseExact(
        "$SessionDate $EnableBefore",
        'yyyy-MM-dd HH:mm:ss',
        [Globalization.CultureInfo]::InvariantCulture
    )
}
function Assert-G3EnableDeadline {
    if ($null -ne $enableDeadline -and (Get-Date) -ge $enableDeadline) {
        throw "G3 activation missed its safe enable deadline $($enableDeadline.ToString('yyyy-MM-dd HH:mm:ss'))."
    }
}
function Read-G3ApprovalJson([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Missing G3 approval evidence: $Path"
    }
    return (Get-Content -LiteralPath $Path -Raw -Encoding UTF8 |
        ConvertFrom-Json -ErrorAction Stop)
}
function Assert-G3PreparedApproval([bool]$ExpectEnabled) {
    $preparedTask = Get-ScheduledTask -TaskName $plan.task -ErrorAction Stop
    $preparedInfo = Get-ScheduledTaskInfo -TaskName $plan.task -ErrorAction Stop
    $actionText = (@($preparedTask.Actions) | ForEach-Object {
        $_.Execute + ' ' + $_.Arguments
    }) -join ' | '
    $expectedDay = [datetime]::ParseExact(
        [string]$plan.session_date,
        'yyyy-MM-dd',
        [Globalization.CultureInfo]::InvariantCulture
    ).Date
    if ($preparedTask.Settings.Enabled -ne $ExpectEnabled -or
        [bool]$preparedTask.Settings.StartWhenAvailable -or
        [int]$preparedTask.Settings.RestartCount -ne 0 -or
        [string]$preparedTask.Settings.MultipleInstances -ne 'IgnoreNew' -or
        $actionText -notlike '*run_g3_approved_live.bat*' -or
        $preparedInfo.NextRunTime.Date -ne $expectedDay -or
        $preparedInfo.NextRunTime.TimeOfDay -ne [timespan]::FromHours(9.25)) {
        throw 'Prepared G3 LIVE task failed transactional verification.'
    }
    if ($ExpectEnabled -and ($preparedTask.State -eq 'Disabled' -or
        $preparedTask.State -eq 'Running')) {
        throw 'Prepared G3 LIVE task is not enabled in a pre-trigger Ready state.'
    }
    if (-not $ExpectEnabled -and $preparedTask.State -ne 'Disabled') {
        throw 'Prepared G3 LIVE task was not kept disabled before final commit.'
    }

    $preparedProfile = Read-G3ApprovalJson ([string]$plan.deployment_file)
    $preparedArm = Read-G3ApprovalJson ([string]$plan.arm_file)
    if ($preparedProfile.deployment_stage -ne 'LIVE_APPROVED' -or
        $preparedProfile.live_activation_authorized -ne $true -or
        $preparedProfile.live_approval.session_date -ne $plan.session_date -or
        $preparedArm.enabled -ne $true -or
        $preparedArm.session_date -ne $plan.session_date -or
        $preparedProfile.strategy_fingerprint -ne $plan.strategy_fingerprint -or
        $preparedArm.strategy_fingerprint -ne $plan.strategy_fingerprint -or
        $preparedProfile.live_execution_policy.live_execution_policy_fingerprint -ne
            $plan.live_execution_policy.live_execution_policy_fingerprint -or
        $preparedProfile.live_approval.live_execution_policy_fingerprint -ne
            $plan.live_execution_policy.live_execution_policy_fingerprint -or
        $preparedArm.live_execution_policy_fingerprint -ne
            $plan.live_execution_policy.live_execution_policy_fingerprint) {
        throw 'Prepared G3 LIVE approval, arm, strategy, or policy failed verification.'
    }
    foreach ($field in @(
        'capital_per_trade_rs',
        'leverage',
        'target_exposure_rs',
        'max_reserved_open_positions',
        'max_daily_entries',
        'max_reserved_gross_exposure_rs'
    )) {
        if ($preparedProfile.live_execution_policy.$field -ne
            $plan.live_execution_policy.$field) {
            throw "Prepared G3 LIVE policy differs from the plan: $field"
        }
    }
}
$arguments = @($Operator, 'plan')
if ($SessionDate) { $arguments += @('--session-date', $SessionDate) }
$planText = & $Python @arguments
if ($LASTEXITCODE -ne 0) { throw 'G3 activation plan failed; no controls were changed.' }
$plan = ($planText -join "`n") | ConvertFrom-Json
if (-not $Apply) { $plan | ConvertTo-Json -Depth 8; return }

# Only an operator's explicit -Apply reaches any control/task mutation.
$activationMutex = New-Object System.Threading.Mutex($false, 'Local\EQIDV2_G3_LIVE_ACTIVATION')
if (-not $activationMutex.WaitOne(0)) { throw 'Another G3 activation is in progress.' }
try {
    $task = Get-ScheduledTask -TaskName $plan.task
    if ($task.State -eq 'Running') { throw 'G3 LIVE task is running; do not replace its active approval.' }
    $workers = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -like 'python*' -and ($_.CommandLine -like '*fno_v13_v10_g_3_live_kite_session.py*' -or
        ($_.CommandLine -like '*fno_v13_v10_g_3_live.py*' -and $_.CommandLine -match '--execution-mode[ ="]+LIVE'))
    })
    if ($workers.Count) { throw 'Existing G3 LIVE workers need operator review before changing authorization.' }
    & $Python (Join-Path $Workspace 'tools/test_kite_order_readiness.py') `
        --app all --minimum-live-balance 20000
    if ($LASTEXITCODE -ne 0) { throw 'Broker read-only checks failed; no controls were changed.' }
    Assert-G3EnableDeadline
    $backup = Join-Path $Workspace ('.codex_tmp/g3_live_activation_' + (Get-Date -Format 'yyyyMMddTHHmmssfff'))
    New-Item -ItemType Directory -Path $backup | Out-Null
    $originalXml = Export-ScheduledTask -TaskName $plan.task
    $originalXml | Set-Content -LiteralPath (Join-Path $backup 'task.xml') -Encoding UTF8
    Copy-Item -LiteralPath $plan.deployment_file -Destination (Join-Path $backup 'active_profile.json')
    $hadArm = Test-Path -LiteralPath $plan.arm_file
    if ($hadArm) { Copy-Item -LiteralPath $plan.arm_file -Destination (Join-Path $backup 'live_arm.json') }
    $plan | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $backup 'plan.json') -Encoding UTF8
    . (Join-Path $PSScriptRoot 'g3_task_xml_helpers.ps1')
    $replacement = ConvertTo-G3ScheduledTaskXml -SourceXml $originalXml `
        -NewTaskName $plan.task -Launcher $plan.scheduled_launcher `
        -FailClosedLiveActivation
    try {
        Assert-G3EnableDeadline
        # Register disabled first. Keep the existing weekday trigger and user.
        Register-ScheduledTask -TaskName $plan.task -Xml $replacement -Force | Out-Null
        & $Python $Operator approve --session-date $plan.session_date --operator-confirmed
        if ($LASTEXITCODE -ne 0) { throw 'Approval could not be committed.' }
        # Validate every approval artifact while the task is still disabled.
        # Enabling the 09:15 trigger is the final commit step.
        Assert-G3PreparedApproval $false
        Assert-G3EnableDeadline
        Enable-ScheduledTask -TaskName $plan.task | Out-Null
        Assert-G3EnableDeadline
        Assert-G3PreparedApproval $true
    } catch {
        # No task is started by this script. Restore the original state on error.
        Disable-ScheduledTask -TaskName $plan.task -ErrorAction SilentlyContinue | Out-Null
        Copy-Item -LiteralPath (Join-Path $backup 'active_profile.json') -Destination $plan.deployment_file -Force
        if ($hadArm) { Copy-Item -LiteralPath (Join-Path $backup 'live_arm.json') -Destination $plan.arm_file -Force }
        elseif (Test-Path -LiteralPath $plan.arm_file) { Remove-Item -LiteralPath $plan.arm_file }
        Register-ScheduledTask -TaskName $plan.task -Xml $originalXml -Force | Out-Null
        throw
    }
    [pscustomobject]@{State='OPERATOR_APPROVED'; SessionDate=$plan.session_date; Setups=$plan.active_setup_count;
        ScheduledTaskEnabled=$true; StartsTaskNow=$false; BackupDirectory=$backup;
        Note='Weekday 09:15 launcher enabled. A new explicit dated approval is required for each session.'} | ConvertTo-Json
} finally {
    $activationMutex.ReleaseMutex()
    $activationMutex.Dispose()
}
