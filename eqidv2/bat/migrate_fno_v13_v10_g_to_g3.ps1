[CmdletBinding()]
param(
    [switch]$Apply,
    [switch]$PaperValidationApproved,
    [string]$RollbackDirectory = ''
)
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot 'g3_task_xml_helpers.ps1')
$roles = @('scanner_5min','equity_1min_feed','confirmation_1min','live_long','live_short',
    'trade_logger','net_result','live_kite_qty1','options_live_long','options_live_short',
    'options_trade_logger','options_net_result')
$plan = @($roles | ForEach-Object {
    [pscustomobject]@{ OldTask = "EQIDV2_fno_v13_v10_g_${_}_0915";
        NewTask = "EQIDV2_fno_v13_v10_g_3_${_}_0915";
        Launcher = Join-Path $PSScriptRoot "run_fno_v13_v10_g_3_$_.bat";
        Enable = ($_ -ne 'live_kite_qty1') }
})
$plan += [pscustomobject]@{ OldTask = 'EQIDV2_backtesting_result_v13_v10_g_1620';
    NewTask = 'EQIDV2_backtesting_result_v13_v10_g_3_1620';
    Launcher = Join-Path $PSScriptRoot 'run_backtesting_result_v13_v10_g_3_1620.bat'; Enable = $true }
if (-not $Apply) {
    [pscustomobject]@{ Mode = 'DRY_RUN'; StartsTasks = $false; ArmsLive = $false;
        DisableLegacy = $true; Tasks = $plan } | ConvertTo-Json -Depth 5
    return
}
if (-not $PaperValidationApproved) { throw 'Apply requires explicit -PaperValidationApproved; LIVE remains disabled.' }
if ([string]::IsNullOrWhiteSpace($RollbackDirectory)) { throw 'Apply requires a new -RollbackDirectory for exact task XML backups.' }
if (Test-Path -LiteralPath $RollbackDirectory) { throw 'Rollback directory must not already exist.' }
$state = Get-Content -LiteralPath (Join-Path $Workspace 'configs/strategies/active_profile.json') -Raw | ConvertFrom-Json
if ($state.profile -ne 'V13_V10_G_3' -or $state.deployment_stage -ne 'PAPER_VALIDATION' -or $state.live_activation_authorized -ne $false) {
    throw 'Apply requires the persisted G3 PAPER_VALIDATION deployment with LIVE authorization false.'
}
# Validate the entire transition before any task mutation. Never dump actions:
# exported rollback XML may include local configuration and is kept on disk.
foreach ($item in $plan) {
    if (-not (Test-Path -LiteralPath $item.Launcher)) { throw "Missing launcher: $($item.Launcher)" }
    $old = Get-ScheduledTask -TaskName $item.OldTask -ErrorAction Stop
    if ($old.State -eq 'Running') { throw "Legacy task is still running: $($item.OldTask). Reconcile and stop separately." }
    if (Get-ScheduledTask -TaskName $item.NewTask -ErrorAction SilentlyContinue) { throw "Target task already exists: $($item.NewTask)" }
}
New-Item -ItemType Directory -Path $RollbackDirectory | Out-Null
foreach ($item in $plan) {
    Export-ScheduledTask -TaskName $item.OldTask | Set-Content -LiteralPath (Join-Path $RollbackDirectory ($item.OldTask + '.xml')) -Encoding UTF8
}
$plan | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $RollbackDirectory 'task_plan.json') -Encoding UTF8
$converted = @{}
# Complete every XML conversion before the first registration, so an unsupported
# source definition cannot leave a partially-created replacement task set.
foreach ($item in $plan) {
    $source = Get-Content -LiteralPath (Join-Path $RollbackDirectory ($item.OldTask + '.xml')) -Raw
    $converted[$item.NewTask] = ConvertTo-G3ScheduledTaskXml -SourceXml $source -NewTaskName $item.NewTask -Launcher $item.Launcher
}
try {
    foreach ($item in $plan) {
        Register-ScheduledTask -TaskName $item.NewTask -Xml $converted[$item.NewTask] | Out-Null
    }
    # All replacements exist disabled before removing old entry authority.
    foreach ($item in $plan) { Disable-ScheduledTask -TaskName $item.OldTask | Out-Null }
    foreach ($item in $plan) { if ($item.Enable) { Enable-ScheduledTask -TaskName $item.NewTask | Out-Null } }
} catch {
    # Fail closed. Keep all newly created tasks disabled for explicit rollback.
    foreach ($item in $plan) { Get-ScheduledTask -TaskName $item.NewTask -ErrorAction SilentlyContinue | Disable-ScheduledTask -ErrorAction SilentlyContinue | Out-Null }
    throw
}
[pscustomobject]@{ Mode = 'APPLIED_PAPER_ONLY'; RollbackDirectory = $RollbackDirectory; StartsTasks = $false; ArmsLive = $false }
