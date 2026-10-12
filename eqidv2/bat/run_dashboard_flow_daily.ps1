$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$pythonExe = (Get-Command python.exe -ErrorAction Stop).Source
$logDir = Join-Path $projectRoot 'logs'
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}
$stamp = Get-Date -Format 'yyyy-MM-dd'
$logPath = Join-Path $logDir "dashboard_flow_daily_$stamp.log"
Push-Location -LiteralPath $projectRoot
try {
    "[$(Get-Date -Format o)] START reporting-only daily OHLC refresh" | Add-Content -LiteralPath $logPath -Encoding UTF8
    & $pythonExe -u (Join-Path $projectRoot 'dashboard_flow_daily_backfill.py') --fetch --latest-completed 2>&1 | Out-File -LiteralPath $logPath -Append -Encoding UTF8
    $runExit = $LASTEXITCODE
    "[$(Get-Date -Format o)] END daily OHLC refresh exit=$runExit" | Add-Content -LiteralPath $logPath -Encoding UTF8
} finally {
    Pop-Location
}
exit $runExit
