$ErrorActionPreference = "Stop"

$baseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$batDir = Join-Path $baseDir "bat"
$hardener = Join-Path $batDir "harden_scheduled_task.ps1"
$tasks = @(
    @{ Name = "EQIDV2_fno_oi_universe_0850"; Time = "08:50"; Runner = "run_fno_oi_universe.bat" }
    @{ Name = "EQIDV2_fno_oi_fetch_5min_fast_production_0905"; Time = "09:05"; Runner = "run_fno_oi_fetch_5min_fast_production.bat" }
    @{ Name = "EQIDV2_fno_oi_feature_ranker_0915"; Time = "09:15"; Runner = "run_fno_oi_feature_ranker.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_scanner_5min_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_scanner_5min.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_equity_1min_feed_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_equity_1min_feed.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_confirmation_1min_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_confirmation_1min.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_live_long_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_live_long.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_live_short_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_live_short.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_trade_logger_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_trade_logger.bat" }
    @{ Name = "EQIDV2_fno_v13_v10_g_net_result_0915"; Time = "09:15"; Runner = "run_fno_v13_v10_g_net_result.bat" }
    @{ Name = "EQIDV2_fno_oi_eod_qc_1540"; Time = "15:40"; Runner = "run_fno_oi_eod_qc.bat" }
)

try {
    $retiredTasks = @(
        # Fast Production is now the sole scheduled 5-minute futures/OI fetcher.
        "EQIDV2_fno_oi_fetch_5min_0905",
        "EQIDV2_fno_oi_fetch_5min_fast_shadow_0906",
        "EQIDV2_fno_v5_scanner_5min_0918",
        "EQIDV2_fno_v5_confirmation_1min_0919",
        "EQIDV2_fno_v5_live_long_0920",
        "EQIDV2_fno_v5_live_short_0920",
        "EQIDV2_fno_v5_trade_logger_0920",
        "EQIDV2_fno_v5_net_result_0920",
        # Compatibility launchers forward to G; retire their old schedules.
        "EQIDV2_fno_v6_scanner_5min_0918",
        "EQIDV2_fno_v6_equity_1min_feed_0919",
        "EQIDV2_fno_v6_confirmation_1min_0919",
        "EQIDV2_fno_v6_live_long_0920",
        "EQIDV2_fno_v6_live_short_0920",
        "EQIDV2_fno_v6_trade_logger_0920",
        "EQIDV2_fno_v6_net_result_0920"
    )
    foreach ($retiredTask in $retiredTasks) {
        & schtasks.exe /Query /TN $retiredTask *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Output "[INFO] Disabling retired task $retiredTask ..."
            & schtasks.exe /Change /TN $retiredTask /Disable
            if ($LASTEXITCODE -ne 0) {
                throw "Unable to disable retired task $retiredTask"
            }
        }
    }
    foreach ($task in $tasks) {
        $runner = Join-Path $batDir $task.Runner
        if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
            throw "Missing runner: $runner"
        }
        Write-Output "[INFO] Creating $($task.Name) at $($task.Time) ..."
        & schtasks.exe /Create /F /TN $task.Name /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $task.Time /TR $runner
        if ($LASTEXITCODE -ne 0) {
            throw "schtasks failed for $($task.Name) with exit code $LASTEXITCODE"
        }
        if (Test-Path -LiteralPath $hardener -PathType Leaf) {
            & $hardener -TaskName $task.Name
        }
    }
    Write-Output "[SUCCESS] FnO weekday tasks created and hardened."
}
catch {
    Write-Error "FnO task installation failed: $($_.Exception.Message)"
    exit 1
}
