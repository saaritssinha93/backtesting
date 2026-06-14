@echo off
REM Backup reference (2026-02-26):
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_CACHE_5MIN_DIR=C:\TradingData\eqidv2\stocks_cache_5min_eq_live"
set "EQIDV2_5M_ENFORCE_SESSION_COMPLETENESS=1"
set "EQIDV2_5M_SYNTHETIC_GAP_FILL=1"
set "EQIDV2_5M_LIVE_SLIM_MODE=1"
set "EQIDV2_5M_LIVE_SLIM_CALENDAR_DAYS=10"
set "EQIDV2_5M_BUFFER_SEC=2"
set "EQIDV2_5M_QUARTER_HOUR_BUFFER_SEC=2"
REM 2026-04-27 outage tuning: with 4 retries x 12s timeout x 5 batches per
REM partition, a hung Kite endpoint took the partition to ~240s (>150s
REM partition_timeout) and SIGKILLed every worker for 5 consecutive slots.
REM Drop per-call timeout to 8s so worst-case per-ticker = 4*8 + backoff
REM (~40s), keeping partitions inside the 150s SIGKILL boundary.
set "EQIDV2_5M_KITE_TIMEOUT_SEC=8"
set "EQIDV2_5M_PARTITION_TIMEOUT_SEC=150"
set "EQIDV2_5M_ADAPTIVE_THROTTLE=1"
set "EQIDV2_5M_ADAPTIVE_MIN_WORKERS=40"
set "EQIDV2_5M_ADAPTIVE_MIN_WORKERS_PER_APP=5"
set "EQIDV2_5M_ADAPTIVE_TOTAL_STEP=32"
set "EQIDV2_5M_ADAPTIVE_PER_APP_STEP=4"
set "EQIDV2_5M_ADAPTIVE_RECOVERY_OK_RATIO=0.90"
set "EQIDV2_5M_ADAPTIVE_RECOVERY_STREAK=2"
REM v2 universe (1262 syms after quarantine) takes ~22-24s/slot; bump warn from 20s to 30s
REM so the adaptive throttle does not keep stepping workers down on cosmetic SLA breaches.
set "EQIDV2_5M_SLOT_SLA_WARN_SEC=50"
set "EQIDV2_VERIFY_SAMPLE_SIZE=32"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_5mins_data_live_minimal.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.heartbeat"
set "FRESHNESS_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
REM 2026-05-19: settled on 320/40 after testing 32 (29s baseline) and 48 (22-31s, variance).
REM 40 workers gives ~4 batches per partition (vs 5 at 32, 3.3 at 48) — balances
REM speed and per-app spread. Combined with Fix A (1-slot lag tolerance), expect
REM ~24-26s clean slots.
REM 2026-06-11: 384/48 created 384 Python workers on a 16-logical-CPU host,
REM producing 150s partition timeouts under parquet/Kite contention. Restore
REM the measured 320/40 ceiling and let adaptive throttle step down by 32/4
REM when a slot breaches the SLA or times out.
set "MAX_WORKERS=320"
set "MAX_WORKERS_PER_APP=40"
set "BUFFER_SEC=%EQIDV2_5M_BUFFER_SEC%"
if "%BUFFER_SEC%"=="" set "BUFFER_SEC=2"
set "QUARTER_HOUR_BUFFER_SEC=%EQIDV2_5M_QUARTER_HOUR_BUFFER_SEC%"
if "%QUARTER_HOUR_BUFFER_SEC%"=="" set "QUARTER_HOUR_BUFFER_SEC=%BUFFER_SEC%"
set "REFRESH_TOKENS_ARG="
if /I "%EQIDV2_5M_REFRESH_TOKENS%"=="1" set "REFRESH_TOKENS_ARG=--refresh-tokens"
if /I "%EQIDV2_5M_REFRESH_TOKENS%"=="true" set "REFRESH_TOKENS_ARG=--refresh-tokens"
set "END_CUTOFF_HHMM=1531"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set "MONITOR_INTERVAL_SEC=5"
set "HUNG_TIMEOUT_SEC=720"
set "FRESHNESS_TIMEOUT_SEC=780"
set "FRESHNESS_GRACE_SEC=1500"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--max-workers","%MAX_WORKERS%","--max-workers-per-app","%MAX_WORKERS_PER_APP%","--buffer-sec","%BUFFER_SEC%","--quarter-hour-buffer-sec","%QUARTER_HOUR_BUFFER_SEC%","--enable-opening-slot-fetch","%REFRESH_TOKENS_ARG%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts %MAX_RESTARTS% ^
  -RestartDelaySec %RESTART_DELAY_SEC% ^
  -MonitorIntervalSec %MONITOR_INTERVAL_SEC% ^
  -HungTimeoutSec %HUNG_TIMEOUT_SEC% ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -FreshnessFile "%FRESHNESS_FILE%" ^
  -FreshnessTimeoutSec %FRESHNESS_TIMEOUT_SEC% ^
  -FreshnessGraceSec %FRESHNESS_GRACE_SEC% ^
  -CutoffHHmm %END_CUTOFF_HHMM% ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
