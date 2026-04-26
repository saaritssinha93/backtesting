[CmdletBinding()]
param(
    [switch]$Remove,
    [switch]$DryRun
)

# Mo3 (2026-04-22) — configure Windows Defender exclusions so AV real-time
# scanning cannot contend for locks on hot runtime files (pending pool JSON,
# lifecycle JSONL, live parquets, status/heartbeat/lock sidecar files).
# Real-time AV handles on paths we rewrite many times per slot are the main
# source of WinError 32 / PermissionError during atomic os.replace.
#
# Requires an elevated PowerShell session (Run as Administrator).
# Idempotent — re-running adds nothing new; -Remove reverses the changes.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$isElevated = $false
try {
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object System.Security.Principal.WindowsPrincipal($identity)
    $isElevated = $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
} catch { }

if (-not $isElevated) {
    Write-Host "[ERROR] This script must run in an elevated PowerShell session (Run as Administrator)." -ForegroundColor Red
    Write-Host "        Right-click PowerShell -> 'Run as administrator' and re-run this script."
    exit 1
}

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$runtimeRoot = "C:\TradingData\eqidv2"

$pathExclusions = @(
    $projectRoot,
    (Join-Path $projectRoot "eqidv2"),
    (Join-Path $projectRoot "eqidv2\logs"),
    $runtimeRoot,
    (Join-Path $runtimeRoot "live_signals"),
    (Join-Path $runtimeRoot "stocks_indicators_5min_eq"),
    (Join-Path $runtimeRoot "stocks_indicators_5min_eq_live"),
    (Join-Path $runtimeRoot "stocks_indicators_5min_eq_pending"),
    (Join-Path $runtimeRoot "nifty_slot_ready_5m"),
    (Join-Path $runtimeRoot "nifty_slot_fail_5m"),
    (Join-Path $runtimeRoot "slot_ready_5m_pending"),
    (Join-Path $runtimeRoot "runtime_status")
)

$processExclusions = @(
    "python.exe",
    "pythonw.exe",
    "py.exe"
)

function Ensure-DefenderCmdletsAvailable {
    if (-not (Get-Command Add-MpPreference -ErrorAction SilentlyContinue)) {
        Write-Host "[ERROR] Defender cmdlets (Add-MpPreference) not available on this system." -ForegroundColor Red
        Write-Host "        Install 'Windows Defender' feature or run on a Windows client/server with Defender enabled."
        exit 2
    }
}

Ensure-DefenderCmdletsAvailable

if ($Remove) {
    Write-Host "[INFO] Removing Defender exclusions added by this script..." -ForegroundColor Yellow
    foreach ($p in $pathExclusions) {
        if ($DryRun) {
            Write-Host "  [DRY] Remove-MpPreference -ExclusionPath $p"
        } else {
            try {
                Remove-MpPreference -ExclusionPath $p -ErrorAction Stop
                Write-Host "  [OK]  removed path $p"
            } catch {
                Write-Host "  [WARN] $p -> $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }
    }
    foreach ($proc in $processExclusions) {
        if ($DryRun) {
            Write-Host "  [DRY] Remove-MpPreference -ExclusionProcess $proc"
        } else {
            try {
                Remove-MpPreference -ExclusionProcess $proc -ErrorAction Stop
                Write-Host "  [OK]  removed process $proc"
            } catch {
                Write-Host "  [WARN] $proc -> $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }
    }
    exit 0
}

Write-Host "[INFO] Adding Defender exclusions for EQIDV2 V16 5min runtime..." -ForegroundColor Cyan
foreach ($p in $pathExclusions) {
    if (-not (Test-Path -LiteralPath $p)) {
        Write-Host "  [SKIP] $p (does not exist — create it first or ignore if unused)" -ForegroundColor DarkGray
        continue
    }
    if ($DryRun) {
        Write-Host "  [DRY] Add-MpPreference -ExclusionPath $p"
    } else {
        try {
            Add-MpPreference -ExclusionPath $p -ErrorAction Stop
            Write-Host "  [OK]  excluded path $p"
        } catch {
            Write-Host "  [WARN] $p -> $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}
foreach ($proc in $processExclusions) {
    if ($DryRun) {
        Write-Host "  [DRY] Add-MpPreference -ExclusionProcess $proc"
    } else {
        try {
            Add-MpPreference -ExclusionProcess $proc -ErrorAction Stop
            Write-Host "  [OK]  excluded process $proc"
        } catch {
            Write-Host "  [WARN] $proc -> $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "[INFO] Done. Verify with: Get-MpPreference | Select-Object ExclusionPath, ExclusionProcess" -ForegroundColor Green
Write-Host "[INFO] To reverse these changes, re-run this script with -Remove."
