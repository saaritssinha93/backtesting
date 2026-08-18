@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=fno_oi_feature_ranker.py"
set "LOG_FILE=%LOG_DIR%\fno_oi_feature_ranker.log"
set "STATUS_FILE=%LOG_DIR%\fno_oi_feature_ranker.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\fno_oi_feature_ranker.supervisor.heartbeat"
set "FRESHNESS_FILE=%RUNTIME_STATUS_DIR%\fno_oi_feature_ranker.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--workers","8","--top-n","20","--min-rank-coverage","0.70","--min-price-move-pct","0.10","--min-oi-move-pct","0.25" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 600 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -FreshnessFile "%FRESHNESS_FILE%" ^
  -FreshnessTimeoutSec 180 ^
  -FreshnessGraceSec 180 ^
  -CutoffHHmm 1536 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
