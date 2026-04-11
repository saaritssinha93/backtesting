[CmdletBinding()]
param(
    [switch]$Preview
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$BaseDir = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $BaseDir "logs"
$LogFile = Join-Path $LogDir "restart_live_trade_executor_v16_5min.log"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Write-LogLine {
    param([Parameter(Mandatory = $true)][string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}] {1}" -f $ts, $Message
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

# Patterns for EXECUTOR only -- does NOT touch the analyser or data fetcher
$patterns = @(
    "run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.bat",
    "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.py",
    "supervise_command\.ps1.*avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.py"
)

$selfPid = $PID
$targets = @(Get-CimInstance Win32_Process | Where-Object {
    if ($_.ProcessId -eq $selfPid) { return $false }
    $cmd = [string]$_.CommandLine
    if ([string]::IsNullOrWhiteSpace($cmd)) { return $false }
    foreach ($pattern in $patterns) {
        if ($cmd -match $pattern) { return $true }
    }
    return $false
} | Sort-Object ProcessId -Unique)

Write-LogLine -Message ("Found {0} executor process(es) to stop." -f $targets.Count)

foreach ($target in $targets) {
    $line = "PID={0} NAME={1}" -f $target.ProcessId, $target.Name
    if ($Preview) {
        Write-LogLine -Message ("MATCH {0}" -f $line)
        continue
    }
    Write-LogLine -Message ("STOP {0}" -f $line)
    try {
        $out = & taskkill /PID $target.ProcessId /T /F 2>&1
        foreach ($msg in @($out)) {
            if (-not [string]::IsNullOrWhiteSpace([string]$msg)) {
                Write-LogLine -Message ([string]$msg)
            }
        }
    } catch {
        Write-LogLine -Message ("WARN stop failed for PID={0}: {1}" -f $target.ProcessId, $_.Exception.Message)
    }
}

if ($Preview) {
    Write-LogLine -Message "Preview mode -- no processes stopped, not restarting."
    exit 0
}

Start-Sleep -Seconds 2

$BatFile = Join-Path $PSScriptRoot "run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat"
if (-not (Test-Path -LiteralPath $BatFile)) {
    Write-LogLine -Message ("ERROR: bat file not found: {0}" -f $BatFile)
    exit 1
}

Write-LogLine -Message ("Starting executor: {0}" -f $BatFile)
Start-Process -FilePath $BatFile -WindowStyle Normal
Start-Sleep -Seconds 5

Write-LogLine -Message "Executor restarted. Analyser was NOT touched."
