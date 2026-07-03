@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=%~dp0.."
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_DATA_1MIN_DIR=C:\TradingData\eqidv2\stocks_indicators_1min_eq"
set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_entry_engine_1min_v5_id.py"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.supervisor.status"
set "HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.supervisor.heartbeat"
set "WORKER_STATUS_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.status"
set "WORKER_HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.heartbeat"
set "LOCK_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.supervisor.lock"
set "SPAWN_RECORD_FILE=%RUNTIME_STATUS_DIR%\entry_engine_1min_v5_ID.supervisor.spawn"
set "END_CUTOFF_HHMM=1535"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set "MONITOR_INTERVAL_SEC=5"
rem The entry engine is intentionally quiet while waiting for the first slot
rem and after END_TIME. Worker heartbeat freshness is the authoritative
rem liveness check; log-idle hung kills create false restarts.
set "HUNG_TIMEOUT_SEC=0"
set "WORKER_STALE_TIMEOUT_SEC=180"
set "WORKER_START_GRACE_SEC=120"

rem T+1 execution: engine wakes one wall-clock minute after signal close.
rem The paper executor uses live LTP; minute OHLC is a reference/filter input.
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DELAY_SEC=60
rem Final handoff deadline is T+1:30; five seconds remain for processing.
set EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_POLL_SEC=0.5
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_PROCESS_RESERVE_SEC=2
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC=1
rem Entry handoff is T+1:30; keep live fetch/fallback calls bounded.
set EQIDV2_5M_KITE_TIMEOUT_SEC=3
set EQIDV2_ENTRY_ENGINE_FNO_BAN_FETCH_TIMEOUT_SEC=3
rem T+1 is the universal production entry contract.
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_ENTRY_LAG_MIN=1
set EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN=3
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES=1
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION=block
rem Correct script identity for status/heartbeat files (base_v15 otherwise writes its own name).
set EQIDV2_RUNTIME_SCRIPT_NAME=eqidv2_entry_engine_1min_v5_id.py

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_entry_engine_1min_v5_id_%TODAY_IST%.log"

cd /d "%BASE_DIR%"

:SUPERVISE_LOOP
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%LOG_FILE%"
  endlocal & exit /b 0
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%" ^
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
  -WorkerStatusFile "%WORKER_STATUS_FILE%" ^
  -WorkerHeartbeatFile "%WORKER_HEARTBEAT_FILE%" ^
  -WorkerStaleTimeoutSec %WORKER_STALE_TIMEOUT_SEC% ^
  -WorkerStartGraceSec %WORKER_START_GRACE_SEC% ^
  -WorkerDiscoveryTimeoutSec 30 ^
  -LockFile "%LOCK_FILE%" ^
  -SpawnRecordFile "%SPAWN_RECORD_FILE%" ^
  -CutoffHHmm %END_CUTOFF_HHMM% ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo [%DATE% %TIME%] END %SCRIPT_NAME% supervisor exit=%EXIT_CODE%>>"%LOG_FILE%"
  endlocal & exit /b %EXIT_CODE%
)

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! LSS %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] WARN supervisor exited before cutoff; relaunching in %RESTART_DELAY_SEC%s>>"%LOG_FILE%"
  timeout /t %RESTART_DELAY_SEC% >nul
  goto SUPERVISE_LOOP
)

endlocal & exit /b 0
