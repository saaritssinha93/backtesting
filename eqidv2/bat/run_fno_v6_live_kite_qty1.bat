@echo off
setlocal EnableExtensions DisableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
set "SESSION_ID=fno_v6_live_kite_qty1"
set "SESSION_SCRIPT=%BASE_DIR%\fno_v6_live_kite_session.py"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "LOG_DIR=%BASE_DIR%\logs"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "FNO_V6_EXECUTION_MODE=LIVE"
set "FNO_V6_LIVE_ACK=I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\%SESSION_ID%_%TODAY_IST%.log"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\%SESSION_ID%.status"
set "HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\%SESSION_ID%.heartbeat"
set "LOCK_FILE=%RUNTIME_STATUS_DIR%\%SESSION_ID%.supervisor.lock"
set "SPAWN_RECORD_FILE=%RUNTIME_STATUS_DIR%\%SESSION_ID%.supervisor.spawn"
set "OPEN_POSITIONS_PATTERN=%EQIDV2_RUNTIME_ROOT%\fno_oi\v6_live\live_kite\open_positions_{date}.json"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%" >nul 2>&1

rem The live contract is frozen in the coordinator. Reject caller-supplied
rem arguments so execution mode, quantity, or session scope cannot be changed.
if not "%~1"=="" (
  >>"%LOG_FILE%" echo [FATAL] Unsupported arguments. This runner is frozen to FnO V6 LIVE quantity 1.
  endlocal & exit /b 2
)

if not exist "%PYTHON_EXE%" (
  where python >nul 2>&1
  if errorlevel 1 (
    >>"%LOG_FILE%" echo [FATAL] Python was not found at the frozen path or on PATH.
    endlocal & exit /b 2
  )
  set "PYTHON_EXE=python"
)

if not exist "%SESSION_SCRIPT%" (
  >>"%LOG_FILE%" echo [FATAL] Missing live coordinator: %SESSION_SCRIPT%
  endlocal & exit /b 2
)
if not exist "%SUPERVISOR_PS1%" (
  >>"%LOG_FILE%" echo [FATAL] Missing supervisor: %SUPERVISOR_PS1%
  endlocal & exit /b 2
)

cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SESSION_ID%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%SESSION_SCRIPT%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -LockFile "%LOCK_FILE%" ^
  -SpawnRecordFile "%SPAWN_RECORD_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 900 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 1545 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff ^
  -OpenPositionsStateFilePattern "%OPEN_POSITIONS_PATTERN%" ^
  -NtpServer "pool.ntp.org" ^
  -NtpMaxDriftSec 30.0

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
