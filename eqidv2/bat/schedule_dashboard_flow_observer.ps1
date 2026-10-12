[CmdletBinding(DefaultParameterSetName = 'Plan')]
param(
    [Parameter(ParameterSetName = 'Plan')][switch]$Plan,
    [Parameter(Mandatory = $true, ParameterSetName = 'Apply')][switch]$Apply,
    [Parameter(Mandatory = $true, ParameterSetName = 'Remove')][switch]$Remove,
    [string]$DataRoot = $(if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { 'C:\TradingData\eqidv2' }),
    [string]$Python = 'python.exe',
    [ValidatePattern('^app[1-8]$')][string]$App = 'app1'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$resolvedRoot = [IO.Path]::GetFullPath($DataRoot)
$pythonCommand = (Get-Command $Python -ErrorAction Stop).Source
$shellCommand = (Get-Command powershell.exe -ErrorAction Stop).Source
$specifications = @(
    @{ Name = 'EQIDV2_dashboard_flow_observer_0855'; Time = '08:55'; Runner = 'run_dashboard_flow_observer.ps1'; Minutes = 35; Description = 'G3 LIVE/PAPER shared opening observer; reporting only; 08:55-09:21 IST; waits for existing authentication. No order submission.' },
    @{ Name = 'EQIDV2_dashboard_flow_observer_history_1700'; Time = '17:00'; Runner = 'run_dashboard_flow_observer_history.ps1'; Minutes = 120; Description = 'G3 BACKTEST opening context recovery; reporting only; after EOD; original candles and backtests unchanged.' }
)
foreach ($argument in @($resolvedRoot, $pythonCommand, $projectRoot)) {
    if ($argument -match '["\r\n]') { throw 'Paths must not contain quotes or newlines.' }
}

function Get-ObserverTask([string]$Name) {
    try { return Get-ScheduledTask -TaskPath '\' -TaskName $Name -ErrorAction Stop }
    catch {
        if ([string]$_.FullyQualifiedErrorId -like 'CmdletizationQuery_NotFound*') { return $null }
        throw
    }
}

# Validate every target before any mutation. Only these two exact names belong
# to this installer; production scheduler scripts are never invoked.
$existingTasks = @{}
$planRows = foreach ($specification in $specifications) {
    $runner = Join-Path $PSScriptRoot $specification.Runner
    if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) { throw "Runner missing: $runner" }
    $existing = Get-ObserverTask $specification.Name
    $existingTasks[$specification.Name] = $existing
    if ($null -ne $existing) {
        if ([string]$existing.State -eq 'Running') { throw "Task is running; refusing modification: $($specification.Name)" }
        if (@($existing.Actions).Count -ne 1 -or [string]$existing.Actions[0].Execute -ne $shellCommand -or
            [string]$existing.Actions[0].Arguments -notlike ('*-File "' + $runner + '"*')) {
            throw "Task name belongs to a different action: $($specification.Name)"
        }
    }
    [pscustomobject]@{
        task = $specification.Name; weekdays = 'Monday-Friday'; time_ist = $specification.Time
        operation = $(if ($Remove) { 'remove' } elseif ($null -eq $existing) { 'create' } else { 'replace' })
        runner = $runner; python = $pythonCommand; data_root = $resolvedRoot; app = $App
        start_when_available = $false; multiple_instances = 'IgnoreNew'; hidden = $true
        starts_now = $false; reporting_only = $true; execution_limit_minutes = $specification.Minutes
    }
}
if (-not ($Apply -or $Remove)) {
    $planRows | ConvertTo-Json -Depth 4
    return
}

# The triggers below are local Windows times; require the machine to use IST.
if ((Get-TimeZone).BaseUtcOffset.TotalMinutes -ne 330) { throw 'Task installation requires the Windows timezone to be IST (UTC+05:30).' }
$backupRoot = Join-Path (Join-Path $resolvedRoot 'dashboard_flow_observer\state\task_backups') (Get-Date -Format 'yyyyMMdd_HHmmss_fff')
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null
$backups = @{}
foreach ($specification in $specifications) {
    $name = $specification.Name
    if ($null -ne $existingTasks[$name]) {
        $xml = Export-ScheduledTask -TaskPath '\' -TaskName $name -ErrorAction Stop
        $backups[$name] = $xml
        [IO.File]::WriteAllText((Join-Path $backupRoot ($name + '.xml')), $xml, [Text.Encoding]::Unicode)
    }
}
$planRows | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $backupRoot 'plan.json') -Encoding UTF8
$mutated = New-Object 'System.Collections.Generic.List[string]'
try {
    foreach ($specification in $specifications) {
        $name = $specification.Name
        if ($Remove) {
            if ($null -ne $existingTasks[$name]) {
                Unregister-ScheduledTask -TaskPath '\' -TaskName $name -Confirm:$false -ErrorAction Stop
                $mutated.Add($name)
            }
            continue
        }
        $runner = Join-Path $PSScriptRoot $specification.Runner
        $arguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $runner + '" -DataRoot "' + $resolvedRoot + '" -Python "' + $pythonCommand + '" -App ' + $App + ' -Scheduled'
        $action = New-ScheduledTaskAction -Execute $shellCommand -Argument $arguments -WorkingDirectory $projectRoot
        $at = [DateTime]::ParseExact($specification.Time, 'HH:mm', [Globalization.CultureInfo]::InvariantCulture)
        $trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $at
        $settings = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes $specification.Minutes) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
        $settings.StartWhenAvailable = $false
        $settings.RestartCount = 0
        $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        $principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
        $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description $specification.Description
        Register-ScheduledTask -TaskPath '\' -TaskName $name -InputObject $task -Force -ErrorAction Stop | Out-Null
        $mutated.Add($name)
        $installed = Get-ObserverTask $name
        if ($null -eq $installed -or $installed.Settings.StartWhenAvailable -or
            [string]$installed.Settings.MultipleInstances -ne 'IgnoreNew' -or
            -not $installed.Settings.Hidden -or @($installed.Actions).Count -ne 1 -or
            [string]$installed.Actions[0].Arguments -ne $arguments -or [string]$installed.State -eq 'Running') {
            throw "Installed task failed verification: $name"
        }
    }
    # Published only after every task passed verification. The dashboard can
    # distinguish a newly installed schedule from a worker's first heartbeat.
    $manifestPath = Join-Path $resolvedRoot 'dashboard_flow_observer\state\task_schedule.json'
    $temporaryManifest = $manifestPath + '.' + [Guid]::NewGuid().ToString('N') + '.tmp'
    $installedNames = @()
    if (-not $Remove) { $installedNames = @($specifications | ForEach-Object { $_.Name }) }
    $manifest = [pscustomobject]@{
        schema = 'dashboard_flow_observer_schedule_v1'
        installed = -not [bool]$Remove
        tasks = $installedNames
        app = $App
        timezone = 'Asia/Kolkata'
        updated_at_ist = (Get-Date).ToString('o')
        reporting_only = $true
    }
    try {
        [IO.File]::WriteAllText($temporaryManifest, ($manifest | ConvertTo-Json -Depth 3), [Text.UTF8Encoding]::new($false))
        if (Test-Path -LiteralPath $manifestPath) { [IO.File]::Replace($temporaryManifest, $manifestPath, (Join-Path $backupRoot 'task_schedule.previous.json')) }
        else { [IO.File]::Move($temporaryManifest, $manifestPath) }
    }
    finally {
        if (Test-Path -LiteralPath $temporaryManifest) { Remove-Item -LiteralPath $temporaryManifest -Force }
    }
}
catch {
    $failure = $_
    foreach ($name in $mutated) {
        try {
            $current = Get-ObserverTask $name
            if ($null -ne $current -and [string]$current.State -eq 'Running') { throw "Rollback refused for running task: $name" }
            if ($backups.ContainsKey($name)) {
                Register-ScheduledTask -TaskPath '\' -TaskName $name -Xml $backups[$name] -Force -ErrorAction Stop | Out-Null
            } elseif ($null -ne $current) {
                Unregister-ScheduledTask -TaskPath '\' -TaskName $name -Confirm:$false -ErrorAction Stop
            }
        } catch { Write-Warning "Rollback could not restore $name; backup directory: $backupRoot" }
    }
    throw $failure
}
[pscustomobject]@{ result = $(if ($Remove) { 'removed' } else { 'installed' }); tasks = @($specifications | ForEach-Object { $_.Name }); backup = $backupRoot; started_now = $false } | ConvertTo-Json -Depth 3
