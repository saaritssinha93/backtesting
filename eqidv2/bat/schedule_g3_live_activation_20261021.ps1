[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$TaskName = 'EQIDV2_g3_live_approve_20261021_0910'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$Workspace = Split-Path $PSScriptRoot -Parent
$Runner = Join-Path $PSScriptRoot 'activate_g3_live_20261021.ps1'
$PowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$At = [datetime]::ParseExact(
    '2026-10-21 09:10:00', 'yyyy-MM-dd HH:mm:ss',
    [Globalization.CultureInfo]::InvariantCulture
)

if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
    throw 'The scheduler host must use India Standard Time.'
}
if ((Get-Date) -ge $At) { throw 'The reviewed one-time activation time has passed.' }
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "Missing activation runner: $Runner"
}
$target = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
if ($target.State -ne 'Disabled' -or $target.Settings.Enabled) {
    throw 'The real G3 LIVE target must remain disabled while scheduling approval.'
}
$targetAction = (@($target.Actions) | ForEach-Object { $_.Execute + ' ' + $_.Arguments }) -join ' | '
if ($targetAction -notlike '*run_fno_v13_v10_g_3_live_kite_qty1.bat*' -or
    $targetAction -like '*run_g3_approved_live.bat*') {
    throw 'The real G3 LIVE target is not the reviewed readiness-only task.'
}

$existingXml = $null
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    $existingXml = Export-ScheduledTask -TaskName $TaskName
}

$action = New-ScheduledTaskAction -Execute $PowerShell -Argument (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $Runner + '"'
) -WorkingDirectory $Workspace
$trigger = New-ScheduledTaskTrigger -Once -At $At
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 4)
$principal = New-ScheduledTaskPrincipal `
    -UserId $target.Principal.UserId `
    -LogonType Interactive `
    -RunLevel Limited
$definition = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal -Description (
        'One-time, date-bound G3 LIVE approval for 2026-10-21. Fail-closed auth, health, broker-flat, policy and timing gates; never starts orders directly.'
    )

try {
    Register-ScheduledTask -TaskPath '\' -TaskName $TaskName -InputObject $definition -Force | Out-Null
    $installed = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
    $installedAction = @($installed.Actions)[0]
    $installedTrigger = @($installed.Triggers)[0]
    if (-not $installed.Settings.Enabled -or $installed.Settings.StartWhenAvailable -or
        [int]$installed.Settings.RestartCount -ne 0 -or
        [string]$installed.Settings.MultipleInstances -ne 'IgnoreNew' -or
        [string]$installed.Principal.LogonType -ne 'Interactive' -or
        [string]$installed.Principal.RunLevel -ne 'Limited' -or
        [datetime]$installedTrigger.StartBoundary -ne $At -or
        $installedAction.Execute -ne $PowerShell -or
        $installedAction.Arguments -notlike '*activate_g3_live_20261021.ps1*' -or
        $info.NextRunTime -ne $At) {
        throw 'Installed one-time G3 activation task failed verification.'
    }
} catch {
    if ($null -ne $existingXml) {
        Register-ScheduledTask -TaskName $TaskName -Xml $existingXml -Force | Out-Null
    } else {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    }
    throw
}

[pscustomobject]@{
    TaskName = $TaskName
    Enabled = $installed.Settings.Enabled
    NextRunTime = $info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss')
    StartWhenAvailable = $installed.Settings.StartWhenAvailable
    RestartCount = $installed.Settings.RestartCount
    Principal = $installed.Principal.UserId
    LogonType = [string]$installed.Principal.LogonType
    RunLevel = [string]$installed.Principal.RunLevel
    Action = ($installedAction.Execute + ' ' + $installedAction.Arguments)
    TargetStillDisabled = -not (Get-ScheduledTask -TaskName $TargetTask).Settings.Enabled
} | ConvertTo-Json -Depth 4
