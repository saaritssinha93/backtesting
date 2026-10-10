param(
    [Parameter(Mandatory = $true)]
    [string]$BaseDir,
    [Parameter(Mandatory = $true)]
    [string]$PythonExe,
    [Parameter(Mandatory = $true)]
    [string]$TodayIst
)

$ErrorActionPreference = "Stop"

$logDir = Join-Path $BaseDir "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$tasks = @(
    @{
        Name = "moving_files.py"
        Script = Join-Path $BaseDir "moving_files.py"
        Log = Join-Path $logDir "moving_files_$TodayIst.log"
        Arguments = @()
    },
    @{
        Name = "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py"
        Script = Join-Path $BaseDir "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py"
        Log = Join-Path $logDir "stocksonly_1min_$TodayIst.log"
        Arguments = @("--universe-scope", "fno")
    }
)

$running = @()
foreach ($task in $tasks) {
    $name = [string]$task.Name
    $script = [string]$task.Script
    $log = [string]$task.Log
    $stdout = "$log.stdout.tmp"
    $stderr = "$log.stderr.tmp"
    $arguments = @("-u", $script) + @($task.Arguments)
    Remove-Item -LiteralPath $stdout, $stderr -Force -ErrorAction SilentlyContinue
    Add-Content -LiteralPath $log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] START $name"
    $process = Start-Process -FilePath $PythonExe `
        -ArgumentList $arguments `
        -WorkingDirectory $BaseDir `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    $running += [pscustomobject]@{
        Name = $name
        Script = $script
        Arguments = @($task.Arguments)
        Log = $log
        Stdout = $stdout
        Stderr = $stderr
        Process = $process
    }
}

$exitCode = 0
foreach ($item in $running) {
    $item.Process.WaitForExit()
    $code = [int]$item.Process.ExitCode
    # Some scheduled launches have reported exit=0 despite a Python traceback.
    # Preserve the failure even when the process handle reports success.
    if ($code -eq 0 -and (Test-Path -LiteralPath $item.Stderr) -and
        (Select-String -LiteralPath $item.Stderr -Pattern '^Traceback \(most recent call last\):' -Quiet)) {
        $code = 1
    }
    if (Test-Path $item.Stdout) {
        Get-Content -LiteralPath $item.Stdout -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $item.Log -Encoding UTF8
    }
    if (Test-Path $item.Stderr) {
        Get-Content -LiteralPath $item.Stderr -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $item.Log -Encoding UTF8
    }
    Remove-Item -LiteralPath $item.Stdout, $item.Stderr -Force -ErrorAction SilentlyContinue
    Add-Content -LiteralPath $item.Log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] END $($item.Name) (exit=$code)"
    if ($code -ne 0 -and $item.Name -eq 'trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py') {
        for ($attempt = 2; $attempt -le 3; $attempt++) {
            Add-Content -LiteralPath $item.Log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] RETRY $($item.Name) attempt=$attempt after 30 seconds"
            Start-Sleep -Seconds 30
            $retryProcess = Start-Process -FilePath $PythonExe `
                -ArgumentList (@('-u', $item.Script) + @($item.Arguments)) `
                -WorkingDirectory $BaseDir `
                -RedirectStandardOutput $item.Stdout `
                -RedirectStandardError $item.Stderr `
                -WindowStyle Hidden `
                -PassThru
            $retryProcess.WaitForExit()
            $code = [int]$retryProcess.ExitCode
            if ($code -eq 0 -and (Test-Path -LiteralPath $item.Stderr) -and
                (Select-String -LiteralPath $item.Stderr -Pattern '^Traceback \(most recent call last\):' -Quiet)) {
                $code = 1
            }
            if (Test-Path -LiteralPath $item.Stdout) {
                Get-Content -LiteralPath $item.Stdout -Raw -ErrorAction SilentlyContinue |
                    Add-Content -LiteralPath $item.Log -Encoding UTF8
            }
            if (Test-Path -LiteralPath $item.Stderr) {
                Get-Content -LiteralPath $item.Stderr -Raw -ErrorAction SilentlyContinue |
                    Add-Content -LiteralPath $item.Log -Encoding UTF8
            }
            Remove-Item -LiteralPath $item.Stdout, $item.Stderr -Force -ErrorAction SilentlyContinue
            Add-Content -LiteralPath $item.Log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] END $($item.Name) attempt=$attempt (exit=$code)"
            if ($code -eq 0) { break }
        }
    }
    if ($code -ne 0 -and $exitCode -eq 0) {
        $exitCode = $code
    }
}

# --- Parse 1MIN VERIFY failures from the 1min data log ---
$oneMinLog = Join-Path $logDir "stocksonly_1min_$TodayIst.log"
$verifyFailCount = 0
$verifyFailLines = @()
if (Test-Path $oneMinLog) {
    $lines = Get-Content -LiteralPath $oneMinLog -ErrorAction SilentlyContinue
    foreach ($line in $lines) {
        if ($line -match '\[1MIN\]\[VERIFY\].*Failed=(\d+)') {
            $n = [int]$Matches[1]
            # A recovered retry supersedes earlier failed attempts.
            $verifyFailCount = $n
            $verifyFailLines = if ($n -gt 0) { @($line) } else { @() }
        }
    }
}

# --- Run data completeness verify ---
$verifyScript = Join-Path $BaseDir "data_for_backtesting_verify.py"
$verifyLog    = Join-Path $logDir "data_verify_$TodayIst.log"
$verifyExit   = 0
if (Test-Path $verifyScript) {
    Add-Content -LiteralPath $verifyLog -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] START data_for_backtesting_verify.py"
    $verifyOut = "$verifyLog.stdout.tmp"
    $verifyErr = "$verifyLog.stderr.tmp"
    $verifyStartedUtc = [DateTime]::UtcNow
    $vProc = Start-Process -FilePath $PythonExe `
        -ArgumentList @("-u", $verifyScript, "--date", $TodayIst, "--scope", "fno") `
        -WorkingDirectory $BaseDir `
        -RedirectStandardOutput $verifyOut `
        -RedirectStandardError $verifyErr `
        -WindowStyle Hidden `
        -PassThru
    $vProc.WaitForExit()
    $verifyExit = [int]$vProc.ExitCode
    # The dated verifier payload is the authoritative result. A stale PASS or
    # an incorrect process exit code must never release the scheduled backtest.
    $runtimeRoot = if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { 'C:\TradingData\eqidv2' }
    $verifyPayloadPath = Join-Path $runtimeRoot "backtesting_result_v11\latest\data_verify_$TodayIst.json"
    try {
        $verifyPayloadFile = Get-Item -LiteralPath $verifyPayloadPath -ErrorAction Stop
        if ($verifyPayloadFile.LastWriteTimeUtc -lt $verifyStartedUtc.AddSeconds(-5)) {
            throw 'Dated verifier payload was not refreshed by this run.'
        }
        $verifyPayload = Get-Content -LiteralPath $verifyPayloadPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        if ($verifyPayload.date -ne $TodayIst -or $verifyPayload.scope -ne 'fno' -or
            $verifyPayload.overall_status -ne 'PASS' -or [int]$verifyPayload.overall_exit_code -ne 0) {
            $verifyExit = [Math]::Max($verifyExit, 2)
        }
    } catch {
        $verifyExit = [Math]::Max($verifyExit, 2)
        Add-Content -LiteralPath $verifyLog -Encoding UTF8 -Value "[ERROR] Dated FnO verifier payload unavailable or stale: $($_.Exception.Message)"
    }
    if (Test-Path $verifyOut) {
        Get-Content -LiteralPath $verifyOut -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $verifyLog -Encoding UTF8
    }
    if (Test-Path $verifyErr) {
        Get-Content -LiteralPath $verifyErr -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $verifyLog -Encoding UTF8
    }
    Remove-Item -LiteralPath $verifyOut, $verifyErr -Force -ErrorAction SilentlyContinue
    Add-Content -LiteralPath $verifyLog -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] END data_for_backtesting_verify.py (exit=$verifyExit)"
    if ($verifyExit -ne 0 -and $exitCode -eq 0) {
        $exitCode = $verifyExit
    }
} else {
    Add-Content -LiteralPath $verifyLog -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] SKIP data_for_backtesting_verify.py (script not found)"
}

# --- Combine logs ---
$combined = Join-Path $logDir "data_for_backtesting_latest.log"
$movingLatest = Join-Path $logDir "moving_files_latest.log"
Set-Content -LiteralPath $combined -Encoding UTF8 -Value "Data for backtesting parallel session - $TodayIst"

foreach ($task in $tasks) {
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value ""
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value ([string]$task.Name)
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
    if (Test-Path $task.Log) {
        Get-Content -LiteralPath $task.Log -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $combined -Encoding UTF8
    }
}

# --- Append verify section ---
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ""
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
Add-Content -LiteralPath $combined -Encoding UTF8 -Value "DATA COMPLETENESS VERIFY"
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
if (Test-Path $verifyLog) {
    Get-Content -LiteralPath $verifyLog -Raw -ErrorAction SilentlyContinue |
        Add-Content -LiteralPath $combined -Encoding UTF8
}

# --- Append 1MIN VERIFY summary ---
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ""
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
Add-Content -LiteralPath $combined -Encoding UTF8 -Value "1MIN VERIFY FAILURE SUMMARY"
Add-Content -LiteralPath $combined -Encoding UTF8 -Value ("=" * 90)
if ($verifyFailCount -gt 0) {
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value "TOTAL_1MIN_VERIFY_FAILURES=$verifyFailCount"
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value "ACTION: These tickers had 1-min bar download failures. V7 backtesting entry engine"
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value "        may use stale 1-min data for them. Check worst_tickers in data_verify_latest.json."
    foreach ($fl in $verifyFailLines) {
        Add-Content -LiteralPath $combined -Encoding UTF8 -Value "  $fl"
    }
} else {
    Add-Content -LiteralPath $combined -Encoding UTF8 -Value "TOTAL_1MIN_VERIFY_FAILURES=0  (all 1-min verifications passed)"
}

Copy-Item -LiteralPath $combined -Destination $movingLatest -Force -ErrorAction SilentlyContinue
exit $exitCode
