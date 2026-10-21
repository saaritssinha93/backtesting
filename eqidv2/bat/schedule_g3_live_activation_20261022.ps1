[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$TaskName = 'EQIDV2_g3_live_approve_20261022_0910'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$Workspace = Split-Path $PSScriptRoot -Parent
$Runner = Join-Path $PSScriptRoot 'activate_g3_live_20261022.ps1'
$PowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$At = [datetime]::ParseExact(
    '2026-10-22 09:10:00', 'yyyy-MM-dd HH:mm:ss',
    [Globalization.CultureInfo]::InvariantCulture
)

function Assert-ReviewedTargetDefinitionXml {
    [xml]$definition = Export-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $namespace = New-Object System.Xml.XmlNamespaceManager($definition.NameTable)
    $namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($definition.SelectNodes('/t:Task/t:Triggers/*', $namespace))
    $actions = @($definition.SelectNodes('/t:Task/t:Actions/t:Exec', $namespace))
    if ($triggers.Count -ne 1 -or $triggers[0].LocalName -ne 'CalendarTrigger' -or
        $actions.Count -ne 1) {
        throw 'The existing G3 LIVE target does not have one reviewed trigger and action.'
    }
    $trigger = $triggers[0]
    $start = $trigger.SelectSingleNode('t:StartBoundary', $namespace)
    $interval = $trigger.SelectSingleNode('t:ScheduleByWeek/t:WeeksInterval', $namespace)
    $days = @($trigger.SelectNodes('t:ScheduleByWeek/t:DaysOfWeek/*', $namespace) |
        ForEach-Object { $_.LocalName })
    if ($null -eq $start -or ([datetime]$start.InnerText).TimeOfDay -ne
            [timespan]::FromHours(9.25) -or
        $null -eq $interval -or [string]$interval.InnerText -ne '1' -or
        ($days -join '|') -ne 'Monday|Tuesday|Wednesday|Thursday|Friday' -or
        $null -ne $trigger.SelectSingleNode('t:Repetition', $namespace) -or
        $null -ne $trigger.SelectSingleNode('t:EndBoundary', $namespace)) {
        throw 'The existing G3 LIVE target is not the non-repeating Monday-Friday 09:15 trigger.'
    }
    $expectedCommand = Join-Path $env:SystemRoot 'System32\cmd.exe'
    $expectedLauncher = Join-Path $PSScriptRoot 'run_g3_approved_live.bat'
    $expectedArguments = '/d /c ""' + $expectedLauncher + '""'
    $command = $actions[0].SelectSingleNode('t:Command', $namespace)
    $arguments = $actions[0].SelectSingleNode('t:Arguments', $namespace)
    $workingDirectory = $actions[0].SelectSingleNode('t:WorkingDirectory', $namespace)
    if ($null -eq $command -or $command.InnerText -ne $expectedCommand -or
        $null -eq $arguments -or $arguments.InnerText -ne $expectedArguments -or
        ($null -ne $workingDirectory -and
            -not [string]::IsNullOrWhiteSpace([string]$workingDirectory.InnerText))) {
        throw 'The existing G3 LIVE target action is not the exact approved launcher.'
    }
}

function Assert-OneTimeActivationDefinitionXml {
    [xml]$definition = Export-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $namespace = New-Object System.Xml.XmlNamespaceManager($definition.NameTable)
    $namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($definition.SelectNodes('/t:Task/t:Triggers/*', $namespace))
    $actions = @($definition.SelectNodes('/t:Task/t:Actions/t:Exec', $namespace))
    if ($triggers.Count -ne 1 -or $triggers[0].LocalName -ne 'TimeTrigger' -or
        $actions.Count -ne 1) {
        throw 'Installed one-time gate does not have exactly one time trigger and action.'
    }
    $trigger = $triggers[0]
    $start = $trigger.SelectSingleNode('t:StartBoundary', $namespace)
    if ($null -eq $start -or [datetime]$start.InnerText -ne $At -or
        $null -ne $trigger.SelectSingleNode('t:Repetition', $namespace) -or
        $null -ne $trigger.SelectSingleNode('t:EndBoundary', $namespace)) {
        throw 'Installed one-time gate has catch-up, repetition, or an unexpected boundary.'
    }
    $expectedArguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' +
        $Runner + '"'
    $command = $actions[0].SelectSingleNode('t:Command', $namespace)
    $arguments = $actions[0].SelectSingleNode('t:Arguments', $namespace)
    $workingDirectory = $actions[0].SelectSingleNode('t:WorkingDirectory', $namespace)
    if ($null -eq $command -or $command.InnerText -ne $PowerShell -or
        $null -eq $arguments -or $arguments.InnerText -ne $expectedArguments -or
        $null -eq $workingDirectory -or $workingDirectory.InnerText -ne $Workspace) {
        throw 'Installed one-time gate action differs from the reviewed PowerShell runner.'
    }
}

function Assert-EnabledApprovedTarget {
    $target = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $targetInfo = Get-ScheduledTaskInfo -TaskName $TargetTask -ErrorAction Stop
    $targetActions = @($target.Actions)
    $expectedCommand = Join-Path $env:SystemRoot 'System32\cmd.exe'
    $expectedLauncher = Join-Path $PSScriptRoot 'run_g3_approved_live.bat'
    $expectedArguments = '/d /c ""' + $expectedLauncher + '""'
    $expectedTargetRun = [datetime]::ParseExact(
        '2026-10-22 09:15:00', 'yyyy-MM-dd HH:mm:ss',
        [Globalization.CultureInfo]::InvariantCulture
    )
    if (-not $target.Settings.Enabled -or [string]$target.State -ne 'Ready' -or
        [bool]$target.Settings.StartWhenAvailable -or
        [int]$target.Settings.RestartCount -ne 0 -or
        [string]$target.Settings.MultipleInstances -ne 'IgnoreNew' -or
        $targetActions.Count -ne 1 -or
        $targetActions[0].Execute -ne $expectedCommand -or
        $targetActions[0].Arguments -ne $expectedArguments -or
        -not [string]::IsNullOrWhiteSpace([string]$targetActions[0].WorkingDirectory) -or
        [string]$target.Principal.LogonType -ne 'Interactive' -or
        [string]$target.Principal.RunLevel -ne 'Limited' -or
        $targetInfo.NextRunTime -ne $expectedTargetRun) {
        throw 'The existing G3 LIVE target is not the reviewed enabled/Ready approved 09:15 task.'
    }
    Assert-ReviewedTargetDefinitionXml
    return $target
}

if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
    throw 'The scheduler host must use India Standard Time.'
}
if ((Get-Date) -ge $At) {
    throw 'The reviewed one-time activation time has passed; no catch-up task will be registered.'
}
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "Missing activation runner: $Runner"
}
if (-not (Test-Path -LiteralPath $PowerShell -PathType Leaf)) {
    throw "Missing Windows PowerShell executable: $PowerShell"
}
$target = Assert-EnabledApprovedTarget

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
        'One-time, date-bound G3 LIVE approval for 2026-10-22. Fail-closed auth, health, no-worker, broker-flat, exact-policy and timing gates; never starts the LIVE task.'
    )

try {
    Register-ScheduledTask -TaskPath '\' -TaskName $TaskName -InputObject $definition -Force | Out-Null
    $installed = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
    $installedActions = @($installed.Actions)
    $installedTriggers = @($installed.Triggers)
    if (-not $installed.Settings.Enabled -or [string]$installed.State -ne 'Ready' -or
        [bool]$installed.Settings.StartWhenAvailable -or
        [int]$installed.Settings.RestartCount -ne 0 -or
        [string]$installed.Settings.MultipleInstances -ne 'IgnoreNew' -or
        [string]$installed.Principal.LogonType -ne 'Interactive' -or
        [string]$installed.Principal.RunLevel -ne 'Limited' -or
        $installedActions.Count -ne 1 -or $installedTriggers.Count -ne 1 -or
        [datetime]$installedTriggers[0].StartBoundary -ne $At -or
        $installedActions[0].Execute -ne $PowerShell -or
        $installedActions[0].Arguments -ne (
            '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $Runner + '"'
        ) -or
        $installedActions[0].WorkingDirectory -ne $Workspace -or
        $info.NextRunTime -ne $At) {
        throw 'Installed one-time G3 activation task failed no-catch-up verification.'
    }
    Assert-OneTimeActivationDefinitionXml
    Assert-EnabledApprovedTarget | Out-Null
} catch {
    $installFailure = $_.Exception.Message
    if ($null -ne $existingXml) {
        Register-ScheduledTask -TaskName $TaskName -Xml $existingXml -Force | Out-Null
        $restored = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        if ($null -eq $restored) {
            throw "Scheduler install failed and the prior activation task could not be restored: $installFailure"
        }
    } else {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        if ($null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
            throw "Scheduler install failed and the partial activation task could not be removed: $installFailure"
        }
    }
    throw $installFailure
}

[pscustomobject]@{
    TaskName = $TaskName
    Enabled = $installed.Settings.Enabled
    NextRunTime = $info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss')
    StartWhenAvailable = $installed.Settings.StartWhenAvailable
    RestartCount = $installed.Settings.RestartCount
    MultipleInstances = [string]$installed.Settings.MultipleInstances
    Principal = $installed.Principal.UserId
    LogonType = [string]$installed.Principal.LogonType
    RunLevel = [string]$installed.Principal.RunLevel
    Action = ($installedActions[0].Execute + ' ' + $installedActions[0].Arguments)
    TargetRemainsEnabledReady = (
        (Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop).Settings.Enabled -and
        [string](Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop).State -eq 'Ready'
    )
    StartsTargetNow = $false
} | ConvertTo-Json -Depth 4
