@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=C:\Windows\py.exe"
if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found. Expected Python 3.12 or C:\Windows\py.exe.
  endlocal & exit /b 1
)
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=fno_options_atm_fetch_5min.py"
set "LOG_FILE=%LOG_DIR%\fno_options_atm_fetch_5min.log"
set "STATUS_FILE=%LOG_DIR%\fno_options_atm_fetch_5min.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\fno_options_atm_fetch_5min.supervisor.heartbeat"
set "FRESHNESS_FILE=%RUNTIME_STATUS_DIR%\fno_options_atm_fetch_5min.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" endlocal & exit /b 1
if not exist "%SUPERVISOR_PS1%" endlocal & exit /b 1
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--mode","live","--intervals","both","--max-apps","8","--boundary-buffer-sec","3","--request-interval-sec","0.34","--workers-per-app","2","--writer-workers","8","--min-coverage","0.99","--slot-retry-attempts","2" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 0 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -FreshnessFile "%FRESHNESS_FILE%" ^
  -FreshnessTimeoutSec 180 ^
  -FreshnessGraceSec 180 ^
  -CutoffHHmm 1534 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
