@echo off
REM ============================================================================
REM run_eqidv2_eod_scheduler_for_1min_data_live.bat
REM Supervised live 1-minute data fetch scheduler (dashboard card: eod_1min_data)
REM Weekdays only. First fetch 09:17:30 IST, then every 5 min, last slot <= 15:40.
REM Uses all 8 Kite apps (app1..app8) via the 1-min core's round-robin pool.
REM ============================================================================
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

REM --- Scheduler timing (env-overridable; CLI below pins the requested values) ---
set "EQIDV2_1M_FIRST_SLOT=09:17:30"
set "EQIDV2_1M_INTERVAL_MIN=5"
set "EQIDV2_1M_END_CUTOFF=15:40"
REM Modest worker count: runs alongside the live 5-min feed / v7 on the shared
REM machine. Core round-robins across 8 apps => 16 threads ~= 2 req/app.
set "MAX_WORKERS=16"

set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_1min_data_live.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_1min_data_live.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_1min_data_live.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_1min_data_live.supervisor.heartbeat"
set "FRESHNESS_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_1min_data_live.status.json"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set "MONITOR_INTERVAL_SEC=5"
REM The scheduler idles (sleeping) for up to 5 minutes between slots but emits a
REM [WAIT] heartbeat to stdout every 30s, so log idle stays well under this.
set "HUNG_TIMEOUT_SEC=600"
REM Freshness file (status.json) is refreshed every ~30s while waiting and after
REM every fetch; 300s timeout tolerates a slow slot without false kills.
set "FRESHNESS_TIMEOUT_SEC=300"
set "FRESHNESS_GRACE_SEC=900"
REM Stop launching/restarting after the last slot window closes.
set "END_CUTOFF_HHMM=1541"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--first-slot","%EQIDV2_1M_FIRST_SLOT%","--interval-min","%EQIDV2_1M_INTERVAL_MIN%","--end-cutoff","%EQIDV2_1M_END_CUTOFF%","--max-workers","%MAX_WORKERS%" ^
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
