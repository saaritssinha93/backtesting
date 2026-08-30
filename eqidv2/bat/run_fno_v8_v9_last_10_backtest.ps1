$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$pythonExe = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
$logDir = Join-Path $baseDir "logs"
$logFile = Join-Path $logDir "fno_v8_v9_last_10_backtest.log"

if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) {
    $pythonExe = "python"
}
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
Set-Location -LiteralPath $baseDir
$env:EQIDV2_RUNTIME_ROOT = "C:\TradingData\eqidv2"
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

$now = Get-Date
$safeStart = Get-Date -Date ($now.ToString("yyyy-MM-dd") + " 15:31:00")
if ($now -lt $safeStart) {
    "[{0}] BLOCKED: V8/V9 repair and replay may start only after 15:30 IST." -f $now.ToString("yyyy-MM-dd HH:mm:ss") |
        Tee-Object -FilePath $logFile -Append
    exit 2
}

"[{0}] START frozen-universe data repair and V8/V9 ten-session replay" -f $now.ToString("yyyy-MM-dd HH:mm:ss") |
    Tee-Object -FilePath $logFile -Append

& $pythonExe -u (Join-Path $baseDir "fno_v8_v9_last_10_backtest.py") `
    --from-day 2026-08-12 `
    --through-day 2026-08-25 `
    --cost-bps 15 `
    --slippage-bps 0 `
    --max-apps 8 *>> $logFile
$exitCode = $LASTEXITCODE

"[{0}] END frozen-universe V8/V9 replay exit={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $exitCode |
    Tee-Object -FilePath $logFile -Append
exit $exitCode
