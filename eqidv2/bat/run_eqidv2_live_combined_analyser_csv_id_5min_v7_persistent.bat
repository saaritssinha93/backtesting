@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"

set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_ID5MIN_V7_ENTRY_WINDOW_START=09:30"
set "EQIDV2_ID5MIN_V7_ENTRY_WINDOW_END=14:30"
set "EQIDV2_ID5MIN_V7_ENTRY_LAG_MIN=5"
set "EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC=75"
REM Retired scanner is diagnostics-only; production CSVs are written by the 1-min entry engine.
set "EQIDV2_ID5MIN_V7_LEGACY_WRITE_LIVE_CSVS=0"
set "EQIDV2_INITIAL_DELAY_SECONDS=0"
set "EQIDV2_SLOT_START_OFFSET_SECONDS=0"
REM 10s post-slot-ready stagger as requested in the ID 5min v7 spec
set "EQIDV2_POST_READY_STAGGER_SECONDS=10"
set "EQIDV2_SLOT_READY_MARKER_ENABLED=1"
set "EQIDV2_SLOT_READY_MARKER_PREFER=1"
set "EQIDV2_SLOT_READY_POLL_ENABLED=1"
set "EQIDV2_SLOT_READY_MAX_WAIT_SECONDS=60"
set "EQIDV2_SLOT_READY_POLL_SECONDS=1"
set "EQIDV2_SLOT_READY_SAMPLE_SIZE=24"
REM 0.95 freshness gate as requested
set "EQIDV2_SLOT_READY_MIN_FRESH_RATIO=0.95"
set "EQIDV2_SLOT_READY_MARKER_MIN_RATIO=0.95"
REM underlying scan still uses v15_new shards; reuse the same tuning knobs
set "EQIDV15_NEW_SNAPSHOT_MAX_WORKERS=6"
set "EQIDV15_NEW_SCAN_MAX_WORKERS=1"
set "EQIDV15_NEW_USE_ROLLING_CACHE=1"
set "EQIDV15_NEW_INCREMENTAL_REFRESH_ROWS=8"

set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.log"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.status"
set "HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.heartbeat"
set "EQIDV2_RUNTIME_STATUS_FILE=%STATUS_FILE%"
set "EQIDV2_RUNTIME_HEARTBEAT_FILE=%HEARTBEAT_FILE%"
set "EQIDV2_RUNTIME_SCRIPT_NAME=%SCRIPT_NAME%"
set "END_CUTOFF_HHMM=1535"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0
set "STATUS_OVERRIDE="

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%ALERT_DIR%" mkdir "%ALERT_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"

cd /d "%BASE_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%LOG_FILE%"
  for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
  >"%STATUS_FILE%" echo status=SKIPPED_CUTOFF
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo cutoff_hhmm=%END_CUTOFF_HHMM%
  >>"%STATUS_FILE%" echo now_hhmm=!NOW_HHMM!
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] Auto-restart enabled: max_restarts=!MAX_RESTARTS!, retry_delay=!RESTART_DELAY_SEC!s, cutoff=!END_CUTOFF_HHMM!>>"%LOG_FILE%"
echo [INFO] ID5MIN_V7 tuning: snapshot_workers=!EQIDV15_NEW_SNAPSHOT_MAX_WORKERS!, scan_workers=!EQIDV15_NEW_SCAN_MAX_WORKERS!, rolling_cache=!EQIDV15_NEW_USE_ROLLING_CACHE!, ready_max_wait=!EQIDV2_SLOT_READY_MAX_WAIT_SECONDS!s, ready_poll_s=!EQIDV2_SLOT_READY_POLL_SECONDS!s, ready_sample=!EQIDV2_SLOT_READY_SAMPLE_SIZE!, ready_ratio=!EQIDV2_SLOT_READY_MIN_FRESH_RATIO!, post_ready_stagger=!EQIDV2_POST_READY_STAGGER_SECONDS!s>>"%LOG_FILE%"
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
>"%STATUS_FILE%" echo status=RUNNING
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
>>"%STATUS_FILE%" echo cutoff_hhmm=%END_CUTOFF_HHMM%
>>"%STATUS_FILE%" echo log_file=%LOG_FILE%

:RUN_LOOP
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

if "%EXIT_CODE%"=="0" goto AFTER_RUN

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  set "STATUS_OVERRIDE=STOPPED_AFTER_CUTOFF"
  set "EXIT_CODE=0"
  echo [WARN] Crash after cutoff ^(HHmm=!NOW_HHMM!^). Not restarting.>>"%LOG_FILE%"
  goto AFTER_RUN
)

set /a RESTART_COUNT+=1
if !RESTART_COUNT! GTR %MAX_RESTARTS% (
  echo [ERROR] Max restarts exceeded for %SCRIPT_NAME% ^(attempts=!RESTART_COUNT!^).>>"%LOG_FILE%"
  >"%ALERT_LOG%" echo [%DATE% %TIME%] CRITICAL %SCRIPT_NAME% exceeded max restarts
  goto AFTER_RUN
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/!MAX_RESTARTS! in !RESTART_DELAY_SEC!s...>>"%LOG_FILE%"
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

:AFTER_RUN
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
if defined STATUS_OVERRIDE (
  >"%STATUS_FILE%" echo status=!STATUS_OVERRIDE!
) else (
  if "%EXIT_CODE%"=="0" (
    >"%STATUS_FILE%" echo status=STOPPED
  ) else (
    >"%STATUS_FILE%" echo status=CRASHED
  )
)
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
>>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
>>"%STATUS_FILE%" echo log_file=%LOG_FILE%

endlocal & exit /b %EXIT_CODE%
