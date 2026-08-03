@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"

REM This feed is intentionally isolated from the production REST feed.
REM Do not point the scanner at this shadow directory until live OHLCV and
REM indicator parity have been benchmarked.
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_KITETICKER_5M_DATA_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live_kiteticker"
set "EQIDV2_KITETICKER_5M_SEED_DATA_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_KITETICKER_5M_UNIVERSE_MANIFEST=C:\TradingData\eqidv2\runtime_status\feed_universe_kiteticker_5m.json"
set "EQIDV2_KITETICKER_5M_BOUNDARY_GRACE_SEC=2"
set "EQIDV2_KITETICKER_5M_WRITE_WORKERS=32"
set "EQIDV2_KITETICKER_5M_REST_REPAIR=1"
set "EQIDV2_KITETICKER_5M_REST_REPAIR_WORKERS_PER_APP=10"
set "EQIDV2_KITETICKER_5M_STATUS_HEARTBEAT_SEC=5"

REM Match the calculations and storage behavior of the existing minimal feed.
set "EQIDV2_5M_ENFORCE_SESSION_COMPLETENESS=1"
set "EQIDV2_5M_SYNTHETIC_GAP_FILL=1"
set "EQIDV2_5M_LIVE_SLIM_MODE=1"
set "EQIDV2_5M_LIVE_SLIM_CALENDAR_DAYS=10"
set "EQIDV2_5M_KITE_TIMEOUT_SEC=8"
set "EQIDV2_VERIFY_SAMPLE_SIZE=32"

set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_kiteticker_5min_live.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_kiteticker_5min_live.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_kiteticker_5min_live.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_kiteticker_5min_live.supervisor.heartbeat"
set "FRESHNESS_FILE=%LOG_DIR%\eqidv2_kiteticker_5min_live.status.json"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

set "END_CUTOFF_HHMM=1531"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set "MONITOR_INTERVAL_SEC=5"
set "HUNG_TIMEOUT_SEC=720"
REM Slot sealing rewrites 1,237 Parquet snapshots and a startup/reconnect slot
REM may also invoke REST repair. Allow that bounded work to finish; HungTimeout
REM remains the independent hard-stop guard.
set "FRESHNESS_TIMEOUT_SEC=180"
set "FRESHNESS_GRACE_SEC=180"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--boundary-grace-sec","%EQIDV2_KITETICKER_5M_BOUNDARY_GRACE_SEC%","--write-workers","%EQIDV2_KITETICKER_5M_WRITE_WORKERS%","--rest-repair-workers-per-app","%EQIDV2_KITETICKER_5M_REST_REPAIR_WORKERS_PER_APP%","--output-dir","%EQIDV2_KITETICKER_5M_DATA_DIR%" ^
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
