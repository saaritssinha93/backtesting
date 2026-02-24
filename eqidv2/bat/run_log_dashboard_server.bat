@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=log_dashboard_server.py"
set "RUN_LOG=%LOG_DIR%\log_dashboard_server.log"
set "STATUS_FILE=%LOG_DIR%\log_dashboard_server.status"
set "HEARTBEAT_FILE=%LOG_DIR%\log_dashboard_server.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "HOST=127.0.0.1"
set "PORT=8787"

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [ERROR] LOG_DASH_PASS is not set. Set it before running.
  echo Example: set LOG_DASH_PASS=your_strong_password
  endlocal & exit /b 2
)
if "%LOG_DASH_TOKEN%"=="" set "LOG_DASH_TOKEN=%LOG_DASH_PASS%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--host","%HOST%","--port","%PORT%","--username","%LOG_DASH_USER%","--password","%LOG_DASH_PASS%","--api-token","%LOG_DASH_TOKEN%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%RUN_LOG%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 100 ^
  -RestartDelaySec 10 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 14400 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 10 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 2359 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
