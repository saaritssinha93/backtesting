$ErrorActionPreference = "Continue"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$pythonExe = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
$logDir = Join-Path $baseDir "logs"
$logFile = Join-Path $logDir "fno_5min_rankings_backfill.log"

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
    "[{0}] BLOCKED: historical backfill may start only after 15:30 IST." -f $now.ToString("yyyy-MM-dd HH:mm:ss") |
        Tee-Object -FilePath $logFile -Append
    exit 2
}

function Invoke-LoggedPython {
    param([string[]]$Arguments)
    & $pythonExe @Arguments *>> $logFile
    return $LASTEXITCODE
}

"[{0}] START F&O 5m OI and ranking repair" -f $now.ToString("yyyy-MM-dd HH:mm:ss") |
    Tee-Object -FilePath $logFile -Append

# Kite invalidates expired AUG instrument tokens.  The immediate retry on
# 2026-08-26 rejected every retained token, so do not consume API capacity on
# a known-impossible request.  Genuine AUG ranking history is rebuilt only
# through its locally archived final available session (August 21).
"[{0}] AUG 5m fetch skipped: expired tokens are invalid; retained data ends 2026-08-21" -f `
    (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") |
    Tee-Object -FilePath $logFile -Append

# The near-month universe rolled to SEP for August 24 onward.
$sepBackfill = Invoke-LoggedPython @(
    "-u", (Join-Path $baseDir "fno_oi_backfill_5min.py"),
    "--contract-months", "2026-09",
    "--from-date", "2026-08-24",
    "--to-date", "2026-08-26",
    "--max-apps", "8"
)
"[{0}] SEP 5m backfill exit={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $sepBackfill |
    Tee-Object -FilePath $logFile -Append

# Rebuild historical gainers/losers/activity boards month-by-month. The live
# layout writer fills only absent slots and preserves every genuine live file.
$augRank = Invoke-LoggedPython @(
    "-u", (Join-Path $baseDir "fno_oi_rank_history.py"),
    "--months", "26AUG",
    "--from-date", "2026-08-21",
    "--to-date", "2026-08-21",
    "--cohort", "month",
    "--emit-live-layout"
)
"[{0}] AUG ranking rebuild exit={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $augRank |
    Tee-Object -FilePath $logFile -Append

$sepRank = Invoke-LoggedPython @(
    "-u", (Join-Path $baseDir "fno_oi_rank_history.py"),
    "--months", "26SEP",
    "--from-date", "2026-08-24",
    "--to-date", "2026-08-26",
    "--cohort", "month",
    "--emit-live-layout"
)
"[{0}] SEP ranking rebuild exit={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $sepRank |
    Tee-Object -FilePath $logFile -Append

$exitCode = 0
if (@($sepBackfill, $augRank, $sepRank) | Where-Object { $_ -ne 0 }) {
    $exitCode = 1
}
"[{0}] END F&O 5m OI and ranking repair exit={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $exitCode |
    Tee-Object -FilePath $logFile -Append
exit $exitCode
