@echo off
setlocal EnableExtensions

for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
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
set "SUPERVISOR_PY=%BASE_DIR%\tools\fno_data_supervisor.py"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" (
  endlocal & exit /b 1
)
if not exist "%SUPERVISOR_PY%" (
  endlocal & exit /b 1
)
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
cd /d "%BASE_DIR%"
REM Data-only supervisor retains the worker arguments, singleton, freshness and cutoff checks.
"%PYTHON_EXE%" -u "%SUPERVISOR_PY%" --service options >>"%LOG_DIR%\fno_options_atm_fetch_5min.launcher.log" 2>&1

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
