@echo off
REM Backup reference (2026-02-26):
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_15mins_data_live_minimal.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_15mins_data_live_minimal.log"
set "MAX_WORKERS=%EQIDV2_15M_MAX_WORKERS%"
if "%MAX_WORKERS%"=="" set "MAX_WORKERS=64"
set "MAX_WORKERS_PER_APP=%EQIDV2_15M_MAX_WORKERS_PER_APP%"
if "%MAX_WORKERS_PER_APP%"=="" set "MAX_WORKERS_PER_APP=8"
set "FETCH_PACE_SEC=%EQIDV2_FETCH_PACE_SEC%"
if "%FETCH_PACE_SEC%"=="" set "FETCH_PACE_SEC=0.25"
set "EQIDV2_FETCH_PACE_SEC=%FETCH_PACE_SEC%"
set "EQIDV2_LOG_UPDATED_TICKERS=0"
set "EQIDV2_LOG_UPDATED_TICKERS_TOP_N=6"
set "EQIDV2_SAVE_NEW_ROWS_REPORTS=0"
set "EQIDV2_VERIFY_SAMPLE_SIZE=16"
set "EQIDV2_LOG_INDICATOR_QUALITY=0"
set "EQIDV2_DOWNCAST_NUMERIC=0"
set "EQIDV2_PARQUET_COMPRESSION=none"
set "BUFFER_SEC=%EQIDV2_15M_BUFFER_SEC%"
if "%BUFFER_SEC%"=="" set "BUFFER_SEC=1"
set "READY_MARKER_ENABLED=%EQIDV2_15M_READY_MARKER_ENABLED%"
if "%READY_MARKER_ENABLED%"=="" set "READY_MARKER_ENABLED=1"
set "READY_MARKER_SAMPLE_SIZE=%EQIDV2_15M_READY_MARKER_SAMPLE_SIZE%"
if "%READY_MARKER_SAMPLE_SIZE%"=="" set "READY_MARKER_SAMPLE_SIZE=24"
set "READY_MARKER_POLL_SECONDS=%EQIDV2_15M_READY_MARKER_POLL_SECONDS%"
if "%READY_MARKER_POLL_SECONDS%"=="" set "READY_MARKER_POLL_SECONDS=1"
set "READY_MARKER_MIN_FRESH_RATIO=%EQIDV2_15M_READY_MARKER_MIN_FRESH_RATIO%"
if "%READY_MARKER_MIN_FRESH_RATIO%"=="" set "READY_MARKER_MIN_FRESH_RATIO=0.70"
set "REFRESH_TOKENS_ARG="
if /I "%EQIDV2_15M_REFRESH_TOKENS%"=="1" set "REFRESH_TOKENS_ARG=--refresh-tokens"
if /I "%EQIDV2_15M_REFRESH_TOKENS%"=="true" set "REFRESH_TOKENS_ARG=--refresh-tokens"
set "READY_MARKER_ARG=--no-ready-marker-enabled"
if /I "%READY_MARKER_ENABLED%"=="1" set "READY_MARKER_ARG=--ready-marker-enabled"
if /I "%READY_MARKER_ENABLED%"=="true" set "READY_MARKER_ARG=--ready-marker-enabled"
set "END_CUTOFF_HHMM=1535"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^) 
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%LOG_FILE%"
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] Auto-restart enabled: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"
echo [INFO] Runtime args: --max-workers %MAX_WORKERS% --max-workers-per-app %MAX_WORKERS_PER_APP% --buffer-sec %BUFFER_SEC% %READY_MARKER_ARG% --ready-marker-sample-size %READY_MARKER_SAMPLE_SIZE% --ready-marker-poll-seconds %READY_MARKER_POLL_SECONDS% --ready-marker-min-fresh-ratio %READY_MARKER_MIN_FRESH_RATIO% %REFRESH_TOKENS_ARG%>>"%LOG_FILE%"
echo [INFO] Fetch pacing: EQIDV2_FETCH_PACE_SEC=%EQIDV2_FETCH_PACE_SEC%>>"%LOG_FILE%"
echo [INFO] Fast fetch toggles: LOG_UPDATED=%EQIDV2_LOG_UPDATED_TICKERS% SAVE_NEW_ROWS_REPORTS=%EQIDV2_SAVE_NEW_ROWS_REPORTS% VERIFY_SAMPLE_SIZE=%EQIDV2_VERIFY_SAMPLE_SIZE% DOWNCAST=%EQIDV2_DOWNCAST_NUMERIC% PARQUET_COMPRESSION=%EQIDV2_PARQUET_COMPRESSION%>>"%LOG_FILE%"

:RUN_LOOP
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --max-workers %MAX_WORKERS% --max-workers-per-app %MAX_WORKERS_PER_APP% --buffer-sec %BUFFER_SEC% %READY_MARKER_ARG% --ready-marker-sample-size %READY_MARKER_SAMPLE_SIZE% --ready-marker-poll-seconds %READY_MARKER_POLL_SECONDS% --ready-marker-min-fresh-ratio %READY_MARKER_MIN_FRESH_RATIO% %REFRESH_TOKENS_ARG% >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

if "%EXIT_CODE%"=="0" goto DONE

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [WARN] Crash after cutoff ^(HHmm=!NOW_HHMM!^). Not restarting.>>"%LOG_FILE%"
  set "EXIT_CODE=0"
  goto DONE
)

set /a RESTART_COUNT+=1
if !RESTART_COUNT! GTR %MAX_RESTARTS% (
  echo [ERROR] Max restarts exceeded for %SCRIPT_NAME% ^(attempts=!RESTART_COUNT!^).>>"%LOG_FILE%"
  goto DONE
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%LOG_FILE%"
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

:DONE
endlocal & exit /b %EXIT_CODE%

