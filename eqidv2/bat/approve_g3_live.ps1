[CmdletBinding()]
param([string]$SessionDate = '', [switch]$Apply, [string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
$Operator = Join-Path $Workspace 'tools/g3_live_operator.py'
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
    & $Python (Join-Path $Workspace 'tools/test_kite_order_readiness.py') --app all
    if ($LASTEXITCODE -ne 0) { throw 'Broker read-only checks failed; no controls were changed.' }
    $backup = Join-Path $Workspace ('.codex_tmp/g3_live_activation_' + (Get-Date -Format 'yyyyMMddTHHmmssfff'))
    New-Item -ItemType Directory -Path $backup | Out-Null
    $originalXml = Export-ScheduledTask -TaskName $plan.task
    $originalXml | Set-Content -LiteralPath (Join-Path $backup 'task.xml') -Encoding UTF8
    Copy-Item -LiteralPath $plan.deployment_file -Destination (Join-Path $backup 'active_profile.json')
    $hadArm = Test-Path -LiteralPath $plan.arm_file
    if ($hadArm) { Copy-Item -LiteralPath $plan.arm_file -Destination (Join-Path $backup 'live_arm.json') }
    $plan | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $backup 'plan.json') -Encoding UTF8
    . (Join-Path $PSScriptRoot 'g3_task_xml_helpers.ps1')
    $replacement = ConvertTo-G3ScheduledTaskXml -SourceXml $originalXml -NewTaskName $plan.task -Launcher $plan.scheduled_launcher
    try {
        # Register disabled first. Keep the existing weekday trigger and user.
        Register-ScheduledTask -TaskName $plan.task -Xml $replacement -Force | Out-Null
        & $Python $Operator approve --session-date $plan.session_date --operator-confirmed
        if ($LASTEXITCODE -ne 0) { throw 'Approval could not be committed.' }
        Enable-ScheduledTask -TaskName $plan.task | Out-Null
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
