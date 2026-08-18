@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\fno_oi_backfill_daily.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

rem Daily continuous futures OHLCV + OI back to the 2019-01-22 OI floor.
rem Incremental by default: existing per-underlying parquet is extended, not refetched.
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_oi_backfill_daily.py" %* >> "%LOG_FILE%" 2>&1

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
