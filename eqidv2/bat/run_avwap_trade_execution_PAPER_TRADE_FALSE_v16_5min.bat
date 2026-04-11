@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV16_5MIN_SHORT_STOP_PCT=0.0075"
set "EQIDV16_5MIN_LONG_STOP_PCT=0.0075"
set "EQIDV16_5MIN_SHORT_TARGET_PCT=0.0080"
set "EQIDV16_5MIN_LONG_TARGET_PCT=0.0080"
set "EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS=10000"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py"
set "MAX_TRADES=5"
set "STATUS_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--max-trades","%MAX_TRADES%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 28800 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 1540 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
