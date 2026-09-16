param(
    [switch]$Apply,
    [string]$StartDate = '2026-09-15',
    [string]$BackupDirectory = 'C:\TradingData\eqidv2\backtesting_result_v13_v10_g\migration_20260914'
)
$ErrorActionPreference = 'Stop'
$oldName = 'EQIDV2_backtesting_result_v11_1600'
$newName = 'EQIDV2_backtesting_result_v13_v10_g_1620'
$oldRunner = Join-Path $PSScriptRoot 'run_backtesting_result_v11_1600.bat'
$newRunner = Join-Path $PSScriptRoot 'run_backtesting_result_v13_v10_g_1620.bat'
$firstStart = [datetime]::ParseExact("$StartDate 16:20", 'yyyy-MM-dd HH:mm', [Globalization.CultureInfo]::InvariantCulture)
if ($firstStart -le (Get-Date)) { throw 'First trigger must be in the future.' }
if (-not (Test-Path -LiteralPath $newRunner -PathType Leaf)) { throw 'Canonical G runner is missing.' }
if (Get-ScheduledTask -TaskPath '\' -TaskName $newName -ErrorAction SilentlyContinue) { throw 'Canonical task already exists; refusing duplicate migration.' }
$oldTask = Get-ScheduledTask -TaskPath '\' -TaskName $oldName
if ($oldTask.State -eq 'Running' -or @($oldTask.Actions).Count -ne 1 -or
    $oldTask.Actions[0].Execute -ne $oldRunner -or $oldTask.Actions[0].Arguments) { throw 'Unexpected or running original task.' }
$currentXml = Export-ScheduledTask -TaskPath '\' -TaskName $oldName
$backup = Join-Path $BackupDirectory "$oldName.xml"
# The original enabled task may already have been exported before disabling it.
$originalXml = if (Test-Path -LiteralPath $backup) { [IO.File]::ReadAllText($backup) } else { $currentXml }
[xml]$replacement = $originalXml
if ($replacement.Task.Actions.Exec.Command -ne $oldRunner -or
    $replacement.Task.RegistrationInfo.URI -ne "\$oldName" -or
    @($replacement.Task.Triggers.ChildNodes).Count -ne 1 -or
    -not $replacement.Task.Triggers.CalendarTrigger.ScheduleByWeek) { throw 'Backup does not match the expected daily task.' }
function Set-TaskXmlText($parent, [string]$name, [string]$value) {
    $node = $parent.SelectSingleNode("*[local-name()='$name']")
    if ($null -eq $node) {
        $node = $parent.OwnerDocument.CreateElement($name, $parent.NamespaceURI)
        $parent.AppendChild($node) | Out-Null
    }
    $node.InnerText = $value
}
Set-TaskXmlText $replacement.Task.Settings 'Enabled' 'false'
Set-TaskXmlText $replacement.Task.Triggers.CalendarTrigger 'StartBoundary' $firstStart.ToString('yyyy-MM-ddTHH:mm:ss')
Set-TaskXmlText $replacement.Task.Actions.Exec 'Command' $newRunner
Set-TaskXmlText $replacement.Task.RegistrationInfo 'URI' "\$newName"
Write-Output "$oldName -> $newName; first run $firstStart; one G-only action."
if (-not $Apply) { Write-Output 'Review only. No tasks changed.'; exit 0 }
[IO.Directory]::CreateDirectory($BackupDirectory) | Out-Null
if (-not (Test-Path -LiteralPath $backup)) { [IO.File]::WriteAllText($backup, $originalXml, [Text.Encoding]::Unicode) }
$newCreated = $false
try {
    Register-ScheduledTask -TaskPath '\' -TaskName $newName -Xml $replacement.OuterXml | Out-Null
    $newCreated = $true
    $registered = Get-ScheduledTask -TaskPath '\' -TaskName $newName
    if ($registered.Settings.Enabled -or $registered.Actions[0].Execute -ne $newRunner) { throw 'Replacement registration verification failed.' }
    $oldTask = Get-ScheduledTask -TaskPath '\' -TaskName $oldName
    if ($oldTask.State -eq 'Running') { throw 'Original task started during migration.' }
    Disable-ScheduledTask -TaskPath '\' -TaskName $oldName | Out-Null
    Enable-ScheduledTask -TaskPath '\' -TaskName $newName | Out-Null
    $registered = Get-ScheduledTask -TaskPath '\' -TaskName $newName
    $info = Get-ScheduledTaskInfo -InputObject $registered
    if (-not $registered.Settings.Enabled -or $info.NextRunTime -ne $firstStart -or $registered.State -eq 'Running') {
        throw 'Replacement enablement or next-run verification failed.'
    }
} catch {
    if ($newCreated) { Disable-ScheduledTask -TaskPath '\' -TaskName $newName -ErrorAction SilentlyContinue | Out-Null }
    # Preserve the observed original enablement state on failure.
    Register-ScheduledTask -TaskPath '\' -TaskName $oldName -Xml $currentXml -Force | Out-Null
    throw
}
Unregister-ScheduledTask -TaskPath '\' -TaskName $oldName -Confirm:$false
$result = [pscustomobject]@{TaskName=$newName; Enabled=$registered.Settings.Enabled;
    NextRun=$info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss'); Action=$registered.Actions[0].Execute;
    LogonType=[string]$registered.Principal.LogonType; MultipleInstances=[string]$registered.Settings.MultipleInstances;
    OriginalXmlBackup=$backup; WorkersStarted=$false}
$result | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $BackupDirectory 'renamed_task.json') -Encoding UTF8
$result | Format-List
