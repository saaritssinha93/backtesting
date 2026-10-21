[CmdletBinding()]
param(
    [switch]$Apply,
    [string]$RollbackDirectory = ''
)
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot 'g3_task_xml_helpers.ps1')

$roles = @('scanner_5min', 'equity_1min_feed', 'confirmation_1min')
$plan = @($roles | ForEach-Object {
    [pscustomobject]@{
        TaskName = "EQIDV2_fno_v13_v10_g_3_${_}_0915"
        Launcher = Join-Path $PSScriptRoot "run_fno_v13_v10_g_3_$_.bat"
    }
})
$policy = [pscustomobject]@{
    RepetitionInterval = 'PT5M'
    RepetitionDuration = 'PT4H20M'
    RestartCount = 3
    RestartInterval = 'PT1M'
    MultipleInstances = 'IgnoreNew'
    StartWhenAvailable = $true
    ExecutionTimeLimit = 'PT0S'
}

if (-not $Apply) {
    [pscustomobject]@{
        Mode = 'DRY_RUN'
        StartsTasks = $false
        StopsTasks = $false
        Tasks = $plan
        Policy = $policy
    } | ConvertTo-Json -Depth 5
    return
}
if ([string]::IsNullOrWhiteSpace($RollbackDirectory)) {
    throw 'Apply requires a new -RollbackDirectory for exact task XML backups.'
}
if (Test-Path -LiteralPath $RollbackDirectory) {
    throw 'Rollback directory must not already exist.'
}
$deploymentPath = Join-Path $Workspace 'configs/strategies/active_profile.json'
$deployment = Get-Content -LiteralPath $deploymentPath -Raw | ConvertFrom-Json
if ($deployment.profile -ne 'V13_V10_G_3') {
    throw 'Pipeline task repair requires the persisted V13_V10_G_3 profile.'
}

$originalEnabled = @{}
foreach ($item in $plan) {
    if (-not (Test-Path -LiteralPath $item.Launcher -PathType Leaf)) {
        throw "Missing launcher: $($item.Launcher)"
    }
    $task = Get-ScheduledTask -TaskName $item.TaskName -ErrorAction Stop
    if ($task.State -eq 'Running') {
        throw "Task is running; retry after it exits: $($item.TaskName)"
    }
    $originalEnabled[$item.TaskName] = [bool]$task.Settings.Enabled
}

function Assert-G3PipelineRecoveryXml {
    param([Parameter(Mandatory=$true)][string]$SourceXml)
    [xml]$xml = $SourceXml
    $ns = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $ns.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($xml.SelectNodes('/t:Task/t:Triggers/t:CalendarTrigger', $ns))
    if ($triggers.Count -ne 1 -or @($xml.SelectNodes('/t:Task/t:Triggers/*', $ns)).Count -ne 1) {
        throw 'Expected exactly one CalendarTrigger after repair.'
    }
    $repetition = $triggers[0].SelectSingleNode('t:Repetition', $ns)
    $settings = $xml.SelectSingleNode('/t:Task/t:Settings', $ns)
    $restart = $settings.SelectSingleNode('t:RestartOnFailure', $ns)
    $stopAtDurationEnd = if ($null -ne $repetition) {
        $repetition.SelectSingleNode('t:StopAtDurationEnd', $ns)
    } else { $null }
    if (
        $null -eq $repetition -or
        $repetition.SelectSingleNode('t:Interval', $ns).InnerText -ne 'PT5M' -or
        $repetition.SelectSingleNode('t:Duration', $ns).InnerText -ne 'PT4H20M' -or
        ($null -ne $stopAtDurationEnd -and $stopAtDurationEnd.InnerText -ne 'false') -or
        $settings.SelectSingleNode('t:MultipleInstancesPolicy', $ns).InnerText -ne 'IgnoreNew' -or
        $settings.SelectSingleNode('t:StartWhenAvailable', $ns).InnerText -ne 'true' -or
        $settings.SelectSingleNode('t:ExecutionTimeLimit', $ns).InnerText -ne 'PT0S' -or
        $null -eq $restart -or
        $restart.SelectSingleNode('t:Count', $ns).InnerText -ne '3' -or
        $restart.SelectSingleNode('t:Interval', $ns).InnerText -ne 'PT1M'
    ) {
        $observed = [ordered]@{
            CalendarTriggers = $triggers.Count
            AllTriggers = @($xml.SelectNodes('/t:Task/t:Triggers/*', $ns)).Count
            Interval = if ($null -ne $repetition) { $repetition.SelectSingleNode('t:Interval', $ns).InnerText } else { '' }
            Duration = if ($null -ne $repetition) { $repetition.SelectSingleNode('t:Duration', $ns).InnerText } else { '' }
            StopAtDurationEnd = if ($null -ne $repetition) { $repetition.SelectSingleNode('t:StopAtDurationEnd', $ns).InnerText } else { '' }
            MultipleInstances = $settings.SelectSingleNode('t:MultipleInstancesPolicy', $ns).InnerText
            StartWhenAvailable = $settings.SelectSingleNode('t:StartWhenAvailable', $ns).InnerText
            ExecutionTimeLimit = $settings.SelectSingleNode('t:ExecutionTimeLimit', $ns).InnerText
            RestartCount = if ($null -ne $restart) { $restart.SelectSingleNode('t:Count', $ns).InnerText } else { '' }
            RestartInterval = if ($null -ne $restart) { $restart.SelectSingleNode('t:Interval', $ns).InnerText } else { '' }
        }
        throw "Registered task does not satisfy the G3 pipeline recovery contract: $($observed | ConvertTo-Json -Compress)"
    }
}

New-Item -ItemType Directory -Path $RollbackDirectory | Out-Null
$converted = @{}
foreach ($item in $plan) {
    $backup = Join-Path $RollbackDirectory ($item.TaskName + '.xml')
    Export-ScheduledTask -TaskName $item.TaskName |
        Set-Content -LiteralPath $backup -Encoding UTF8
}
$plan | ConvertTo-Json -Depth 4 |
    Set-Content -LiteralPath (Join-Path $RollbackDirectory 'task_plan.json') -Encoding UTF8
$policy | ConvertTo-Json -Depth 4 |
    Set-Content -LiteralPath (Join-Path $RollbackDirectory 'recovery_policy.json') -Encoding UTF8
foreach ($item in $plan) {
    $source = Get-Content -LiteralPath (Join-Path $RollbackDirectory ($item.TaskName + '.xml')) -Raw
    $converted[$item.TaskName] = ConvertTo-G3ScheduledTaskXml `
        -SourceXml $source `
        -NewTaskName $item.TaskName `
        -Launcher $item.Launcher `
        -PipelineRecovery
    Assert-G3PipelineRecoveryXml -SourceXml $converted[$item.TaskName]
}

try {
    foreach ($item in $plan) {
        Register-ScheduledTask -TaskName $item.TaskName -Xml $converted[$item.TaskName] -Force | Out-Null
    }
    foreach ($item in $plan) {
        Assert-G3PipelineRecoveryXml -SourceXml (Export-ScheduledTask -TaskName $item.TaskName)
    }
    foreach ($item in $plan) {
        if ($originalEnabled[$item.TaskName]) {
            Enable-ScheduledTask -TaskName $item.TaskName | Out-Null
        } else {
            Disable-ScheduledTask -TaskName $item.TaskName | Out-Null
        }
    }
} catch {
    $failure = $_
    $rollbackErrors = @()
    foreach ($item in $plan) {
        try {
            $source = Get-Content -LiteralPath (Join-Path $RollbackDirectory ($item.TaskName + '.xml')) -Raw
            Register-ScheduledTask -TaskName $item.TaskName -Xml $source -Force | Out-Null
            if ($originalEnabled[$item.TaskName]) {
                Enable-ScheduledTask -TaskName $item.TaskName | Out-Null
            } else {
                Disable-ScheduledTask -TaskName $item.TaskName | Out-Null
            }
        } catch {
            $rollbackErrors += "$($item.TaskName): $($_.Exception.Message)"
        }
    }
    if ($rollbackErrors.Count) {
        throw "G3 pipeline task repair failed: $($failure.Exception.Message); rollback errors: $($rollbackErrors -join '; ')"
    }
    throw $failure
}

[pscustomobject]@{
    Mode = 'APPLIED'
    StartsTasks = $false
    StopsTasks = $false
    RollbackDirectory = $RollbackDirectory
    Tasks = @($plan.TaskName)
    Policy = $policy
}
