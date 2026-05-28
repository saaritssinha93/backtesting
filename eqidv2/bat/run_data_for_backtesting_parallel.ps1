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
    },
    @{
        Name = "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py"
        Script = Join-Path $BaseDir "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py"
        Log = Join-Path $logDir "stocksonly_1min_$TodayIst.log"
    }
)

$running = @()
foreach ($task in $tasks) {
    $name = [string]$task.Name
    $script = [string]$task.Script
    $log = [string]$task.Log
    $stdout = "$log.stdout.tmp"
    $stderr = "$log.stderr.tmp"
    Remove-Item -LiteralPath $stdout, $stderr -Force -ErrorAction SilentlyContinue
    Add-Content -LiteralPath $log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] START $name"
    $process = Start-Process -FilePath $PythonExe `
        -ArgumentList @("-u", $script) `
        -WorkingDirectory $BaseDir `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    $running += [pscustomobject]@{
        Name = $name
        Log = $log
        Stdout = $stdout
        Stderr = $stderr
        Process = $process
    }
}

$exitCode = 0
foreach ($item in $running) {
    $item.Process.WaitForExit()
    if (Test-Path $item.Stdout) {
        Get-Content -LiteralPath $item.Stdout -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $item.Log -Encoding UTF8
    }
    if (Test-Path $item.Stderr) {
        Get-Content -LiteralPath $item.Stderr -Raw -ErrorAction SilentlyContinue |
            Add-Content -LiteralPath $item.Log -Encoding UTF8
    }
    Remove-Item -LiteralPath $item.Stdout, $item.Stderr -Force -ErrorAction SilentlyContinue
    $code = [int]$item.Process.ExitCode
    Add-Content -LiteralPath $item.Log -Encoding UTF8 -Value "[$(Get-Date -Format 'dd-MM-yyyy HH:mm:ss.ff')] END $($item.Name) (exit=$code)"
    if ($code -ne 0 -and $exitCode -eq 0) {
        $exitCode = $code
    }
}

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

Copy-Item -LiteralPath $combined -Destination $movingLatest -Force -ErrorAction SilentlyContinue
exit $exitCode
