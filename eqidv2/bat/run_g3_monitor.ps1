[CmdletBinding()]
param(
    [ValidateSet('Monitor', 'Research')][string]$Mode = 'Monitor',
    [string]$DataRoot = $(if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { 'C:\TradingData\eqidv2' }),
    [string]$Python = 'python.exe',
    [switch]$Scheduled
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$pythonCommand = (Get-Command $Python -ErrorAction Stop).Source
$resolvedRoot = [IO.Path]::GetFullPath($DataRoot)
$env:EQIDV2_RUNTIME_ROOT = $resolvedRoot
Set-Location -LiteralPath $projectRoot
if ($Scheduled) {
    & $pythonCommand -c "from datetime import datetime; from zoneinfo import ZoneInfo; from nse_market_calendar import market_closed_reason; import sys; sys.exit(1 if market_closed_reason(datetime.now(ZoneInfo('Asia/Kolkata')).date()) else 0)"
    if ($LASTEXITCODE -eq 1) { Write-Output 'Market holiday: monitor task skipped.'; exit 0 }
    if ($LASTEXITCODE -ne 0) { throw 'Market calendar verification failed.' }
    if ($Mode -eq 'Research') {
        $pointer = Join-Path $resolvedRoot 'backtesting_result_v13_v10_g_3\latest\current_backtest.json'
        $current = Get-Content -LiteralPath $pointer -Raw | ConvertFrom-Json
        if ($current.session_date -ne (Get-Date -Format 'yyyy-MM-dd')) {
            throw 'Current-day G3 historical backtest is not ready; monitor research was not refreshed.'
        }
    }
}
$scriptName = if ($Mode -eq 'Monitor') { 'g3_trade_monitor.py' } else { 'g3_trade_monitor_backtest.py' }
$scriptPath = Join-Path $projectRoot ('tools\' + $scriptName)
$logRoot = Join-Path $resolvedRoot 'v13_v10_g_3_trade_monitor\logs'
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss_fff'
$arguments = @('"' + $scriptPath + '"')
if ($Mode -eq 'Monitor') { $arguments += @('--market-data', '--interval', '15', '--end-time', '15:35') }
$worker = Start-Process -FilePath $pythonCommand -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -Wait -RedirectStandardOutput (Join-Path $logRoot "$Mode`_$stamp.stdout.log") -RedirectStandardError (Join-Path $logRoot "$Mode`_$stamp.stderr.log")
exit $worker.ExitCode
