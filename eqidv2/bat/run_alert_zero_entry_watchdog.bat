@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "GMAIL_CREDENTIALS_FILE=%BASE_DIR%\bat\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%BASE_DIR%\bat\gmail_token.json"
if "%EQIDV2_ALERT_EMAIL_TO%"=="" set "EQIDV2_ALERT_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%EQIDV2_ALERT_EMAIL_FROM%"=="" set "EQIDV2_ALERT_EMAIL_FROM=saaritssinha93@gmail.com"

set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\alert_zero_entry_watchdog_%TODAY_IST%.log"

set "TIER=%~1"
if "%TIER%"=="" set "TIER=1030"
set "MIN_CONFIRMED=%~2"
if "%MIN_CONFIRMED%"=="" set "MIN_CONFIRMED=1"

cd /d "%BASE_DIR%"

"%PYTHON_EXE%" "%BASE_DIR%\alert_zero_entry_watchdog.py" --tier "%TIER%" --min-confirmed "%MIN_CONFIRMED%" >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
