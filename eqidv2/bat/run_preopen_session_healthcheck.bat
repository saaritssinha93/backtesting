@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"

set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=preopen_session_healthcheck.py"
set "LATEST_LOG=%LOG_DIR%\preopen_session_healthcheck_latest.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --max-age-min 35 --report-path "%LATEST_LOG%"
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)

endlocal & exit /b %EXIT_CODE%
