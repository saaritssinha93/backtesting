@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

rem ============================================================
rem  SAME-DAY EOD FULL FETCH.
rem  The live intraday defaults (EQIDV2_1MIN_SAME_DAY_STALE_OK=1,
rem  EQIDV2_1MIN_STALE_OK_MIN=60) treat any ticker whose last 1-min bar
rem  falls on today's date as "fresh" and skip it. At 15:45 that leaves the
rem  store stuck at the ~09:15 morning snapshot (last bar ~09:38) and the
rem  full trading day never lands until the NEXT morning. For this post-close
rem  EOD prep run we WANT a real fetch through 15:30, so force strict
rem  freshness. This is scoped to the 15:45 job only; the live intraday
rem  fetcher keeps its own lenient defaults.
rem ============================================================
set "EQIDV2_1MIN_SAME_DAY_STALE_OK=0"
set "EQIDV2_1MIN_STALE_OK_MIN=0"

set "LOG_DIR=%BASE_DIR%\logs"
set "PARALLEL_RUNNER=%BASE_DIR%\bat\run_data_for_backtesting_parallel.ps1"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\data_for_backtesting_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START Data for backtesting parallel session>>"%LOG_FILE%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%PARALLEL_RUNNER%" -BaseDir "%BASE_DIR%" -PythonExe "%PYTHON_EXE%" -TodayIst "%TODAY_IST%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END Data for backtesting parallel session ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

endlocal & exit /b %EXIT_CODE%
