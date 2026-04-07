@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "INTERVAL_SEC=300"
set "LOG_DIR=%EQIDV2_RUNTIME_ROOT%\logs\public_ip_monitor"
set "LOG_FILE=%LOG_DIR%\public_ip_monitor_console.log"
set "SCRIPT_NAME=public_ip_monitor.py"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME% interval=%INTERVAL_SEC%s >>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --interval-sec %INTERVAL_SEC% >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END %SCRIPT_NAME% exit=%EXIT_CODE% >>"%LOG_FILE%"

endlocal & exit /b %EXIT_CODE%
