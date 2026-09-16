param(
    [switch]$Apply,
    [string]$StartDate = (Get-Date).Date.AddDays(1).ToString('yyyy-MM-dd')
)

$ErrorActionPreference = 'Stop'
$baseDir = Split-Path -Parent $PSScriptRoot
$hardener = Join-Path $PSScriptRoot 'harden_scheduled_task.ps1'
$tasks = @(
    @{ Name = 'EQIDV2_fno_options_atm_fetch_5min_0907'; Time = '09:07'; Runner = 'run_fno_options_atm_fetch_5min.bat' }
    @{ Name = 'EQIDV2_fno_v13_v10_g_options_live_long_0915'; Time = '09:15'; Runner = 'run_fno_v13_v10_g_options_live_long.bat' }
    @{ Name = 'EQIDV2_fno_v13_v10_g_options_live_short_0915'; Time = '09:15'; Runner = 'run_fno_v13_v10_g_options_live_short.bat' }
    @{ Name = 'EQIDV2_fno_v13_v10_g_options_trade_logger_0915'; Time = '09:15'; Runner = 'run_fno_v13_v10_g_options_trade_logger.bat' }
    @{ Name = 'EQIDV2_fno_v13_v10_g_options_net_result_0915'; Time = '09:15'; Runner = 'run_fno_v13_v10_g_options_net_result.bat' }
)

$culture = [Globalization.CultureInfo]::InvariantCulture
$startDay = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', $culture).Date
$now = Get-Date
$plan = foreach ($item in $tasks) {
    $runner = Join-Path $PSScriptRoot $item.Runner
    if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
        throw "Missing options runner: $runner"
    }
    $clock = [TimeSpan]::ParseExact($item.Time, 'hh\:mm', $culture)
    $firstRun = $startDay.Add($clock)
    if ($firstRun -le $now) {
        throw "First run must be in the future: $($item.Name) -> $($firstRun.ToString('yyyy-MM-dd HH:mm:ss'))"
    }
    [pscustomobject]@{
        Name = $item.Name
        Time = $item.Time
        Runner = $runner
        FirstRun = $firstRun
    }
}

$plan | Select-Object Name, Time, FirstRun, Runner | Format-Table -AutoSize
if (-not $Apply) {
    Write-Output 'Review only: no scheduled tasks changed. Re-run with -Apply after reviewing the plan.'
    exit 0
}

$userId = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

foreach ($item in $plan) {
    $existing = Get-ScheduledTask -TaskPath '\' -TaskName $item.Name -ErrorAction SilentlyContinue
    if ($null -ne $existing -and $existing.State -eq 'Running') {
        throw "Existing task is running; update refused: $($item.Name)"
    }
}

foreach ($item in $plan) {
    $action = New-ScheduledTaskAction -Execute $item.Runner
    $trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
        -At $item.FirstRun
    Register-ScheduledTask `
        -TaskPath '\' `
        -TaskName $item.Name `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description 'V13-V10-G ATM options data and paper-trading session' `
        -Force | Out-Null
    if (Test-Path -LiteralPath $hardener -PathType Leaf) {
        & $hardener -TaskName $item.Name -WakeToRun
    }
}

$verified = foreach ($item in $plan) {
    $task = Get-ScheduledTask -TaskPath '\' -TaskName $item.Name
    $info = Get-ScheduledTaskInfo -InputObject $task
    if (-not $task.Settings.Enabled) {
        throw "Task was registered disabled: $($item.Name)"
    }
    if ($task.Actions.Count -ne 1 -or $task.Actions[0].Execute -ne $item.Runner) {
        throw "Task action verification failed: $($item.Name)"
    }
    if ($info.NextRunTime -ne $item.FirstRun) {
        throw "Task next-run verification failed: $($item.Name) observed=$($info.NextRunTime) expected=$($item.FirstRun)"
    }
    [pscustomobject]@{
        TaskName = $item.Name
        Enabled = $task.Settings.Enabled
        NextRun = $info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss')
        Action = $task.Actions[0].Execute
        MultipleInstances = [string]$task.Settings.MultipleInstances
    }
}

$verified | Format-Table TaskName, Enabled, NextRun, MultipleInstances -AutoSize
Write-Output "Scheduled ATM options fetch plus four V13-V10-G options PAPER sessions from $StartDate. No task was started."

