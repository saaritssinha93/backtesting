[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$RollbackDirectory, [switch]$Apply)
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
$plan = Get-Content -LiteralPath (Join-Path $RollbackDirectory 'task_plan.json') -Raw | ConvertFrom-Json
if (-not $Apply) {
    [pscustomobject]@{ Mode = 'DRY_RUN'; RestoreTasks = @($plan.OldTask); DisableTasks = @($plan.NewTask); StartsTasks = $false } | ConvertTo-Json -Depth 4
    return
}
$state = Get-Content -LiteralPath (Join-Path $Workspace 'configs/strategies/active_profile.json') -Raw | ConvertFrom-Json
if ($state.profile -ne 'V13_V10_G') { throw 'Restore the approved G deployment record only after broker-flat reconciliation before task rollback.' }
foreach ($item in $plan) {
    $task = Get-ScheduledTask -TaskName $item.NewTask -ErrorAction SilentlyContinue
    if ($null -ne $task -and $task.State -eq 'Running') { throw "G3 task is running: $($item.NewTask)" }
    if (-not (Test-Path -LiteralPath (Join-Path $RollbackDirectory ($item.OldTask + '.xml')))) { throw 'Missing rollback XML.' }
}
foreach ($item in $plan) {
    Get-ScheduledTask -TaskName $item.NewTask -ErrorAction SilentlyContinue | Disable-ScheduledTask -ErrorAction SilentlyContinue | Out-Null
}
foreach ($item in $plan) {
    Register-ScheduledTask -TaskName $item.OldTask -Xml (Get-Content -LiteralPath (Join-Path $RollbackDirectory ($item.OldTask + '.xml')) -Raw) -Force | Out-Null
}
