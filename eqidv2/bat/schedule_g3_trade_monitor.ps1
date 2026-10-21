[CmdletBinding()]
param(
    [switch]$Apply,
    [string]$DataRoot = $(if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { 'C:\TradingData\eqidv2' }),
    [string]$Python = 'python.exe'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$runner = Join-Path $PSScriptRoot 'run_g3_monitor.ps1'
$resolvedRoot = [IO.Path]::GetFullPath($DataRoot)
$pythonCommand = (Get-Command $Python -ErrorAction Stop).Source
$shellCommand = (Get-Command powershell.exe -ErrorAction Stop).Source
if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) { throw 'Monitor runner is missing.' }
foreach ($argument in @($resolvedRoot, $pythonCommand, $runner)) {
    if ($argument -match '["\r\n]') { throw 'Invalid path in task arguments.' }
}
$specs = @(
    @{Name='EQIDV2_G3_trade_monitor_0915'; Time='09:15'; Mode='Monitor'; Minutes=420; Description='Read-only G3 entered-trade observation every 15 seconds. Quotes and completed one-minute candles; no orders or strategy changes.'},
    @{Name='EQIDV2_G3_trade_monitor_research_1740'; Time='17:40'; Mode='Research'; Minutes=120; Description='Separate fixed-policy G3 monitoring backtest after the current-day historical run. Advisory research only; no live policy promotion.'}
)
$existing = @{}
$rows = foreach ($spec in $specs) {
    $task = Get-ScheduledTask -TaskPath '\' -TaskName $spec.Name -ErrorAction SilentlyContinue
    if ($null -ne $task) {
        if ([string]$task.State -eq 'Running') { throw "Cannot replace a running monitor task: $($spec.Name)" }
        if (@($task.Actions).Count -ne 1 -or [string]$task.Actions[0].Execute -ne $shellCommand -or
            [string]$task.Actions[0].Arguments -notlike ('*-File "' + $runner + '"*')) {
            throw "Task name belongs to another action: $($spec.Name)"
        }
    }
    $existing[$spec.Name] = $task
    [pscustomobject]@{task=$spec.Name;time_ist=$spec.Time;mode=$spec.Mode;weekdays='Monday-Friday';read_only=$true;starts_now=$false}
}
if (-not $Apply) { $rows | ConvertTo-Json; return }
if ((Get-TimeZone).BaseUtcOffset.TotalMinutes -ne 330) { throw 'Task scheduling requires the Windows timezone to be IST.' }
$backupRoot = Join-Path $resolvedRoot ('v13_v10_g_3_trade_monitor\task_backups\' + (Get-Date -Format 'yyyyMMdd_HHmmss_fff'))
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null
$backups = @{}
$mutated = New-Object 'System.Collections.Generic.List[string]'
try {
    foreach ($spec in $specs) {
        if ($null -ne $existing[$spec.Name]) {
            $xml = Export-ScheduledTask -TaskPath '\' -TaskName $spec.Name
            $backups[$spec.Name] = $xml
            [IO.File]::WriteAllText((Join-Path $backupRoot ($spec.Name + '.xml')), $xml, [Text.Encoding]::Unicode)
        }
        $arguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $runner + '" -Mode ' + $spec.Mode + ' -DataRoot "' + $resolvedRoot + '" -Python "' + $pythonCommand + '" -Scheduled'
        $action = New-ScheduledTaskAction -Execute $shellCommand -Argument $arguments -WorkingDirectory $projectRoot
        $trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At ([datetime]::ParseExact($spec.Time,'HH:mm',[Globalization.CultureInfo]::InvariantCulture))
        $settings = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes $spec.Minutes) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
        $settings.StartWhenAvailable = $false
        $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
        Register-ScheduledTask -TaskPath '\' -TaskName $spec.Name -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description $spec.Description -Force | Out-Null
        $mutated.Add($spec.Name)
        $installed = Get-ScheduledTask -TaskPath '\' -TaskName $spec.Name
        if ([string]$installed.Actions[0].Arguments -ne $arguments -or -not $installed.Settings.Hidden -or $installed.Settings.StartWhenAvailable -or [string]$installed.Settings.MultipleInstances -ne 'IgnoreNew' -or [int]$installed.Settings.RestartCount -ne 3 -or [string]$installed.Settings.RestartInterval -ne 'PT1M') {
            throw "Installed task failed verification: $($spec.Name)"
        }
    }
} catch {
    foreach ($name in $mutated) {
        if ($backups.ContainsKey($name)) { Register-ScheduledTask -TaskPath '\' -TaskName $name -Xml $backups[$name] -Force | Out-Null }
        else { Unregister-ScheduledTask -TaskPath '\' -TaskName $name -Confirm:$false }
    }
    throw
}
$rows | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $backupRoot 'installed.json') -Encoding UTF8
[pscustomobject]@{result='installed';tasks=@($specs.Name);backup=$backupRoot;started_now=$false} | ConvertTo-Json
