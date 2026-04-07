[CmdletBinding()]
param(
    [switch]$Preview
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$BaseDir = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $BaseDir "logs"
$LogFile = Join-Path $LogDir "eqidv2_v16_5min_stop_1615.log"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Write-LogLine {
    param([Parameter(Mandatory = $true)][string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[{0}] {1}" -f $ts, $Message
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Write-KeyFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Data
    )
    $parent = Split-Path -Path $Path -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $lines = foreach ($entry in ($Data.GetEnumerator() | Sort-Object Name)) {
        "{0}={1}" -f $entry.Key, [string]$entry.Value
    }
    Set-Content -Path $Path -Value $lines -Encoding UTF8
}

function Write-ScannerStopped {
    $statusPath = Join-Path $LogDir "eqidv2_live_combined_analyser_csv_v16_5min.status"
    $scriptName = "eqidv2_live_combined_analyser_csv_v16_5min.py"
    Write-KeyFile -Path $statusPath -Data @{
        status = "STOPPED"
        script = $scriptName
        ts     = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        reason = "scheduled_stop_1615"
        source = "stop_eqidv2_v16_5min_stack.ps1"
    }
}

function Write-LiveTradeStopped {
    $statusPath = Join-Path $LogDir "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.status"
    $heartbeatPath = Join-Path $LogDir "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.heartbeat"
    $logPath = ""
    try {
        $today = Get-Date -Format "yyyy-MM-dd"
        $candidate = Join-Path $LogDir ("avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_{0}.log" -f $today)
        if (Test-Path -LiteralPath $candidate) {
            $logPath = $candidate
        }
    } catch {
        $logPath = ""
    }

    Write-KeyFile -Path $statusPath -Data @{
        status               = "STOPPED"
        name                 = "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py"
        ts                   = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        exit_code            = 0
        restart_count        = 0
        reason               = "scheduled_stop_1615"
        cutoff_hhmm          = "1615"
        log_file             = $logPath
        heartbeat_file       = $heartbeatPath
        supervisor_log_file  = "$logPath.supervisor.log"
    }

    Write-KeyFile -Path $heartbeatPath -Data @{
        name               = "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py"
        ts_utc             = (Get-Date).ToUniversalTime().ToString("o")
        state              = "STOPPED"
        pid                = ""
        restart_count      = 0
        idle_sec           = ""
        last_log_utc       = ""
        cutoff_hhmm        = "1615"
        note               = "scheduled_stop_1615"
        log_file           = $logPath
        status_file        = $statusPath
        supervisor_log_file = "$logPath.supervisor.log"
    }
}

function Write-AllStoppedState {
    Write-ScannerStopped
    Write-LiveTradeStopped
}

$patterns = @(
    "run_eqidv2_live_combined_analyser_csv_v16_5min\.bat",
    "eqidv2_live_combined_analyser_csv_v16_5min\.py",
    "run_avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min\.bat",
    "avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min\.py",
    "run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.bat",
    "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.py",
    "supervise_command\.ps1.*avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min\.py"
)

$selfPid = $PID
$targets = @(Get-CimInstance Win32_Process | Where-Object {
    if ($_.ProcessId -eq $selfPid) {
        return $false
    }
    $cmd = [string]$_.CommandLine
    if ([string]::IsNullOrWhiteSpace($cmd)) {
        return $false
    }
    foreach ($pattern in $patterns) {
        if ($cmd -match $pattern) {
            return $true
        }
    }
    return $false
} | Sort-Object ProcessId -Unique)

Write-LogLine -Message ("Matched {0} process(es) for V16_5MIN stop. preview={1}" -f $targets.Count, $Preview.IsPresent)

foreach ($target in $targets) {
    $line = "PID={0} NAME={1}" -f $target.ProcessId, $target.Name
    if ($Preview) {
        Write-LogLine -Message ("MATCH {0}" -f $line)
        continue
    }

    Write-LogLine -Message ("STOP {0}" -f $line)
    try {
        $taskkillOutput = & taskkill /PID $target.ProcessId /T /F 2>&1
        foreach ($msg in @($taskkillOutput)) {
            if (-not [string]::IsNullOrWhiteSpace([string]$msg)) {
                Write-LogLine -Message ([string]$msg)
            }
        }
    } catch {
        Write-LogLine -Message ("WARN stop failed for PID={0}: {1}" -f $target.ProcessId, $_.Exception.Message)
    }
}

if (-not $Preview) {
    Write-AllStoppedState
    Write-LogLine -Message "Wrote STOPPED state for V16_5MIN scanner/live-trade status files."
}
