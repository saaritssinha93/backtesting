@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Audit #2 (2026-04-22) — LF prewarm runner. Scheduled at 09:13 IST so the
REM 8 Kite app sessions are authenticated and the NSE instrument list is
REM cached before the LF supervisor wakes at 09:15. Eliminates the
REM "Too many requests" cascade at 09:20 caused by 8 cold-start app
REM auths racing the live path.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

set "SCRIPT_NAME=eqidv2_lf_prewarm_v16_5min.py"
set "LOG_DIR=%BASE_DIR%\logs"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_lf_prewarm_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
