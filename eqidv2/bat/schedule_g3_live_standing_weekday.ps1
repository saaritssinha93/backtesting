[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$TaskName = 'EQIDV2_g3_live_standing_approval_0910'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$Workspace = Split-Path $PSScriptRoot -Parent
$Runner = Join-Path $PSScriptRoot 'activate_g3_live_standing.ps1'
$PowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$FirstRun = [datetime]::ParseExact(
    '2026-10-23 09:10:00', 'yyyy-MM-dd HH:mm:ss',
    [Globalization.CultureInfo]::InvariantCulture
)
$ExpectedArguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' +
    $Runner + '"'

function Assert-ReviewedTargetDefinitionXml {
    [xml]$definition = Export-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $namespace = New-Object System.Xml.XmlNamespaceManager($definition.NameTable)
    $namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($definition.SelectNodes('/t:Task/t:Triggers/*', $namespace))
    $actions = @($definition.SelectNodes('/t:Task/t:Actions/t:Exec', $namespace))
    if ($triggers.Count -ne 1 -or $triggers[0].LocalName -ne 'CalendarTrigger' -or
        $actions.Count -ne 1) {
        throw 'The G3 LIVE target does not have exactly one reviewed trigger and action.'
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
        throw 'The G3 LIVE target is not the non-repeating Monday-Friday 09:15 trigger.'
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
        throw 'The G3 LIVE target action is not the exact approved launcher.'
    }
}

function Assert-ReviewedTargetForInstallation {
    $target = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $actions = @($target.Actions)
    $expectedCommand = Join-Path $env:SystemRoot 'System32\cmd.exe'
    $expectedLauncher = Join-Path $PSScriptRoot 'run_g3_approved_live.bat'
    $expectedArguments = '/d /c ""' + $expectedLauncher + '""'
    if ([bool]$target.Settings.StartWhenAvailable -or
        [int]$target.Settings.RestartCount -ne 0 -or
        [string]$target.Settings.MultipleInstances -ne 'IgnoreNew' -or
        [string]$target.Principal.LogonType -ne 'Interactive' -or
        [string]$target.Principal.RunLevel -ne 'Limited' -or
        $actions.Count -ne 1 -or
        $actions[0].Execute -ne $expectedCommand -or
        $actions[0].Arguments -ne $expectedArguments -or
        -not [string]::IsNullOrWhiteSpace([string]$actions[0].WorkingDirectory)) {
        throw 'The existing G3 LIVE target differs from the reviewed fail-closed task.'
    }
    Assert-ReviewedTargetDefinitionXml
    return $target
}

function Assert-RecurringGateDefinitionXml {
    [xml]$definition = Export-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $namespace = New-Object System.Xml.XmlNamespaceManager($definition.NameTable)
    $namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($definition.SelectNodes('/t:Task/t:Triggers/*', $namespace))
    $actions = @($definition.SelectNodes('/t:Task/t:Actions/t:Exec', $namespace))
    if ($triggers.Count -ne 1 -or $triggers[0].LocalName -ne 'CalendarTrigger' -or
        $actions.Count -ne 1) {
        throw 'Installed standing gate does not have exactly one calendar trigger and action.'
    }
    $trigger = $triggers[0]
    $start = $trigger.SelectSingleNode('t:StartBoundary', $namespace)
    $interval = $trigger.SelectSingleNode('t:ScheduleByWeek/t:WeeksInterval', $namespace)
    $days = @($trigger.SelectNodes('t:ScheduleByWeek/t:DaysOfWeek/*', $namespace) |
        ForEach-Object { $_.LocalName })
    if ($null -eq $start -or [datetime]$start.InnerText -ne $FirstRun -or
        $null -eq $interval -or [string]$interval.InnerText -ne '1' -or
        ($days -join '|') -ne 'Monday|Tuesday|Wednesday|Thursday|Friday' -or
        $null -ne $trigger.SelectSingleNode('t:Repetition', $namespace) -or
        $null -ne $trigger.SelectSingleNode('t:EndBoundary', $namespace)) {
        throw 'Installed standing gate is not the exact non-repeating weekday schedule beginning 2026-10-23 09:10.'
    }
    $command = $actions[0].SelectSingleNode('t:Command', $namespace)
    $arguments = $actions[0].SelectSingleNode('t:Arguments', $namespace)
    $workingDirectory = $actions[0].SelectSingleNode('t:WorkingDirectory', $namespace)
    if ($null -eq $command -or $command.InnerText -ne $PowerShell -or
        $null -eq $arguments -or $arguments.InnerText -ne $ExpectedArguments -or
        $null -eq $workingDirectory -or $workingDirectory.InnerText -ne $Workspace) {
        throw 'Installed standing gate action differs from the exact reviewed PowerShell runner.'
    }
    $settings = $definition.SelectSingleNode('/t:Task/t:Settings', $namespace)
    $startWhenAvailable = $settings.SelectSingleNode('t:StartWhenAvailable', $namespace)
    $restart = $settings.SelectSingleNode('t:RestartOnFailure', $namespace)
    $multiple = $settings.SelectSingleNode('t:MultipleInstancesPolicy', $namespace)
    $limit = $settings.SelectSingleNode('t:ExecutionTimeLimit', $namespace)
    if (($null -ne $startWhenAvailable -and $startWhenAvailable.InnerText -ne 'false') -or
        $null -ne $restart -or $null -eq $multiple -or
        $multiple.InnerText -ne 'IgnoreNew' -or $null -eq $limit -or
        $limit.InnerText -ne 'PT3M') {
        throw 'Installed standing gate has catch-up, retry, overlap, or execution-limit drift.'
    }
}

function Assert-InstalledRecurringGate {
    $installed = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
    $actions = @($installed.Actions)
    $triggers = @($installed.Triggers)
    if (-not $installed.Settings.Enabled -or [string]$installed.State -ne 'Ready' -or
        [bool]$installed.Settings.StartWhenAvailable -or
        [int]$installed.Settings.RestartCount -ne 0 -or
        [string]$installed.Settings.MultipleInstances -ne 'IgnoreNew' -or
        [string]$installed.Principal.LogonType -ne 'Interactive' -or
        [string]$installed.Principal.RunLevel -ne 'Limited' -or
        [string]$installed.Settings.ExecutionTimeLimit -ne 'PT3M' -or
        $actions.Count -ne 1 -or $triggers.Count -ne 1 -or
        [datetime]$triggers[0].StartBoundary -ne $FirstRun -or
        $actions[0].Execute -ne $PowerShell -or
        $actions[0].Arguments -ne $ExpectedArguments -or
        $actions[0].WorkingDirectory -ne $Workspace -or
        $info.NextRunTime -ne $FirstRun) {
        throw 'Installed recurring G3 standing gate failed exact no-catch-up verification.'
    }
    Assert-RecurringGateDefinitionXml
    return [pscustomobject]@{Task=$installed; Info=$info}
}

if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
    throw 'The scheduler host must use India Standard Time.'
}
foreach ($requiredFile in @($Runner, $PowerShell)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Missing standing scheduler prerequisite: $requiredFile"
    }
}
$target = Assert-ReviewedTargetForInstallation

$backup = Join-Path $Workspace (
    '.codex_tmp/g3_live_standing_scheduler_' +
    (Get-Date -Format 'yyyyMMddTHHmmssfff')
)
New-Item -ItemType Directory -Path $backup | Out-Null
$existingXml = $null
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    $existingXml = Export-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $existingXml | Set-Content -LiteralPath (
        Join-Path $backup 'prior_standing_task.xml'
    ) -Encoding UTF8
} else {
    'NO_PRIOR_TASK' | Set-Content -LiteralPath (
        Join-Path $backup 'prior_standing_task.absent'
    ) -Encoding ASCII
}
Export-ScheduledTask -TaskName $TargetTask -ErrorAction Stop |
    Set-Content -LiteralPath (Join-Path $backup 'reviewed_target_task.xml') -Encoding UTF8

$action = New-ScheduledTaskAction -Execute $PowerShell `
    -Argument $ExpectedArguments -WorkingDirectory $Workspace
$trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $FirstRun
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 3)
$principal = New-ScheduledTaskPrincipal `
    -UserId $target.Principal.UserId `
    -LogonType Interactive `
    -RunLevel Limited
$definition = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal -Description (
        'Weekday 09:10 G3 LIVE standing-policy gate, effective 2026-10-23. ' +
        'Disables the 09:15 target first; enables it only after exact same-day fail-closed checks. Never starts LIVE.'
    )

try {
    Register-ScheduledTask -TaskPath '\' -TaskName $TaskName `
        -InputObject $definition -Force | Out-Null
    $verified = Assert-InstalledRecurringGate
    # Installation is not an activation operation and must not alter or start
    # the existing 09:15 target.
    Assert-ReviewedTargetForInstallation | Out-Null
} catch {
    $installFailure = $_.Exception.Message
    $rollbackFailure = $null
    try {
        if ($null -ne $existingXml) {
            Register-ScheduledTask -TaskPath '\' -TaskName $TaskName `
                -Xml $existingXml -Force | Out-Null
            if ($null -eq (Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop)) {
                throw 'Prior standing task was not restored.'
            }
        } else {
            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false `
                -ErrorAction SilentlyContinue
            if ($null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
                throw 'Partial standing task was not removed.'
            }
        }
    } catch {
        $rollbackFailure = $_.Exception.Message
    }
    if ($null -ne $rollbackFailure) {
        throw "Scheduler install failed: $installFailure ROLLBACK FAILED: $rollbackFailure"
    }
    throw $installFailure
}

[pscustomobject]@{
    TaskName = $TaskName
    Enabled = $verified.Task.Settings.Enabled
    FirstRun = $FirstRun.ToString('yyyy-MM-dd HH:mm:ss')
    NextRunTime = $verified.Info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss')
    Days = 'Monday,Tuesday,Wednesday,Thursday,Friday'
    StartWhenAvailable = $verified.Task.Settings.StartWhenAvailable
    RestartCount = $verified.Task.Settings.RestartCount
    MultipleInstances = [string]$verified.Task.Settings.MultipleInstances
    ExecutionTimeLimit = [string]$verified.Task.Settings.ExecutionTimeLimit
    LogonType = [string]$verified.Task.Principal.LogonType
    RunLevel = [string]$verified.Task.Principal.RunLevel
    Action = ($verified.Task.Actions[0].Execute + ' ' + $verified.Task.Actions[0].Arguments)
    BackupDirectory = $backup
    StartsTargetNow = $false
    MutatesTargetNow = $false
} | ConvertTo-Json -Depth 4
