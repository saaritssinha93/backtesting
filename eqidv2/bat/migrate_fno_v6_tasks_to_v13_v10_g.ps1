param(
    [switch]$Apply,
    [string]$StartDate = '2026-09-15',
    [string]$BackupDirectory = 'C:\TradingData\eqidv2\fno_oi\v13_v10_g_live\session_rename_20260914'
)
$ErrorActionPreference = 'Stop'
$baseDir = Split-Path -Parent $PSScriptRoot
$firstStart = [datetime]::ParseExact("$StartDate 09:15", 'yyyy-MM-dd HH:mm', [Globalization.CultureInfo]::InvariantCulture)
if ($firstStart -le (Get-Date)) { throw 'The new first trigger must be in the future; no tasks changed.' }
$roles = [ordered]@{
    scanner_5min = '0918'; equity_1min_feed = '0919'; confirmation_1min = '0919'
    live_long = '0920'; live_short = '0920'; trade_logger = '0920'; net_result = '0920'
    live_kite_qty1 = '0915'
}
$plan = @()
function Set-TaskXmlText($parent, [string]$name, [string]$value) {
    $node = $parent.SelectSingleNode("*[local-name()='$name']")
    if ($null -eq $node) {
        $node = $parent.OwnerDocument.CreateElement($name, $parent.NamespaceURI)
        $parent.AppendChild($node) | Out-Null
    }
    $node.InnerText = $value
}
foreach ($role in $roles.Keys) {
    $oldName = "EQIDV2_fno_v6_${role}_$($roles[$role])"
    $newName = "EQIDV2_fno_v13_v10_g_${role}_0915"
    $oldRunner = Join-Path $PSScriptRoot "run_fno_v6_$role.bat"
    $newRunner = Join-Path $PSScriptRoot "run_fno_v13_v10_g_$role.bat"
    $oldTask = Get-ScheduledTask -TaskPath '\' -TaskName $oldName -ErrorAction Stop
    if ($oldTask.State -eq 'Running') { throw "$oldName is running; migration refused." }
    if (@($oldTask.Actions).Count -ne 1 -or $oldTask.Actions[0].Execute -ne $oldRunner -or $oldTask.Actions[0].Arguments) {
        throw "Unexpected action on $oldName; migration refused."
    }
    if (-not (Test-Path -LiteralPath $newRunner -PathType Leaf)) { throw "Missing canonical runner: $newRunner" }
    if (Get-ScheduledTask -TaskPath '\' -TaskName $newName -ErrorAction SilentlyContinue) { throw "$newName already exists; migration refused." }
    $originalXml = Export-ScheduledTask -TaskPath '\' -TaskName $oldName
    [xml]$replacement = $originalXml
    if (@($replacement.Task.Triggers.ChildNodes).Count -ne 1 -or -not $replacement.Task.Triggers.CalendarTrigger.ScheduleByWeek) {
        throw "Expected one weekly calendar trigger on $oldName."
    }
    Set-TaskXmlText $replacement.Task.Settings 'Enabled' 'false'
    Set-TaskXmlText $replacement.Task.Triggers.CalendarTrigger 'StartBoundary' $firstStart.ToString('yyyy-MM-ddTHH:mm:ss')
    Set-TaskXmlText $replacement.Task.Actions.Exec 'Command' $newRunner
    Set-TaskXmlText $replacement.Task.RegistrationInfo 'URI' "\$newName"
    $plan += [pscustomobject]@{Old=$oldName;New=$newName;Runner=$newRunner;WasEnabled=[bool]$oldTask.Settings.Enabled;OriginalXml=$originalXml;NewXml=$replacement.OuterXml}
}
$plan | Select-Object Old,New,WasEnabled,@{Name='FirstRun';Expression={$firstStart.ToString('yyyy-MM-dd HH:mm')}} | Format-Table -AutoSize
if (-not $Apply) { Write-Output 'Review only: no tasks changed. Use -Apply to migrate the verified plan.'; exit 0 }

[IO.Directory]::CreateDirectory($BackupDirectory) | Out-Null
foreach ($item in $plan) {
    $backup = Join-Path $BackupDirectory "$($item.Old).xml"
    if (Test-Path -LiteralPath $backup) { throw "Backup already exists: $backup; no tasks changed." }
    [IO.File]::WriteAllText($backup, $item.OriginalXml, [Text.Encoding]::Unicode)
}
$created = @()
$oldDisabled = @()
try {
    # Build every replacement disabled before touching any existing schedule.
    foreach ($item in $plan) {
        Register-ScheduledTask -TaskPath '\' -TaskName $item.New -Xml $item.NewXml | Out-Null
        $created += $item.New
        $verified = Get-ScheduledTask -TaskPath '\' -TaskName $item.New
        if ($verified.Settings.Enabled -or $verified.Actions[0].Execute -ne $item.Runner) { throw "New task verification failed: $($item.New)" }
    }
    foreach ($item in $plan) {
        $oldTask = Get-ScheduledTask -TaskPath '\' -TaskName $item.Old
        if ($oldTask.State -eq 'Running') { throw "Old task started during migration: $($item.Old)" }
        Disable-ScheduledTask -TaskPath '\' -TaskName $item.Old | Out-Null
        $oldDisabled += $item.Old
    }
    foreach ($item in $plan) {
        if ($item.WasEnabled) { Enable-ScheduledTask -TaskPath '\' -TaskName $item.New | Out-Null }
        $verified = Get-ScheduledTask -TaskPath '\' -TaskName $item.New
        $info = Get-ScheduledTaskInfo -InputObject $verified
        if ([bool]$verified.Settings.Enabled -ne $item.WasEnabled -or
            ($item.WasEnabled -and $info.NextRunTime -ne $firstStart) -or $verified.State -eq 'Running') {
            throw "Enabled/next-run verification failed: $($item.New)"
        }
    }
} catch {
    # Restore original schedules if the transaction has not fully verified.
    foreach ($name in $created) { Disable-ScheduledTask -TaskPath '\' -TaskName $name -ErrorAction SilentlyContinue | Out-Null }
    foreach ($item in $plan) {
        if ($oldDisabled -contains $item.Old -and $item.WasEnabled) { Enable-ScheduledTask -TaskPath '\' -TaskName $item.Old | Out-Null }
    }
    throw
}

foreach ($item in $plan) {
    # Original XML is retained for rollback; remove duplicate registrations.
    Unregister-ScheduledTask -TaskPath '\' -TaskName $item.Old -Confirm:$false
}
$result = foreach ($item in $plan) {
    $task = Get-ScheduledTask -TaskPath '\' -TaskName $item.New
    $info = Get-ScheduledTaskInfo -InputObject $task
    [pscustomobject]@{TaskName=$item.New;Enabled=$task.Settings.Enabled;NextRun=$info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss');Action=$task.Actions[0].Execute;LogonType=[string]$task.Principal.LogonType;WakeToRun=$task.Settings.WakeToRun}
}
$result | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $BackupDirectory 'renamed_tasks.json') -Encoding UTF8
$result | Format-Table TaskName,Enabled,NextRun -AutoSize
Write-Output "Renamed eight tasks. No workers started. Original XML backup: $BackupDirectory"
