@echo off
setlocal EnableExtensions EnableDelayedExpansion

if "%EQIDV15_LONG_SHARD_ID%"=="" (
  echo [ERROR] EQIDV15_LONG_SHARD_ID is required.
  endlocal & exit /b 1
)

set "SHARD_ID=%EQIDV15_LONG_SHARD_ID%"
if "%EQIDV15_LONG_SHARD_COUNT%"=="" (
  set "EQIDV15_LONG_SHARD_COUNT=10"
)
set /a SHARD_NUM=1%SHARD_ID%-100
set /a POST_READY_STAGGER_SEC=!SHARD_NUM!+34

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_INITIAL_DELAY_SECONDS=0"
set "EQIDV2_SLOT_START_OFFSET_SECONDS=0"
set "EQIDV2_POST_READY_STAGGER_SECONDS=!POST_READY_STAGGER_SEC!"
set "EQIDV2_NUM_SCANS_PER_SLOT=3"
set "EQIDV2_SCAN_INTERVAL_SECONDS=1"
set "EQIDV2_BLOCK_PARALLEL_SCAN_ENABLED=0"
set "EQIDV2_SLOT_READY_MARKER_ENABLED=1"
set "EQIDV2_SLOT_READY_POLL_ENABLED=1"
set "EQIDV2_SLOT_READY_MAX_WAIT_SECONDS=70"
set "EQIDV2_SLOT_READY_POLL_SECONDS=1"
set "EQIDV2_SLOT_READY_SAMPLE_SIZE=12"
set "EQIDV2_SLOT_READY_MIN_FRESH_RATIO=0.75"
set "EQIDV15_STALE_ONLY_RETRY=1"
set "EQIDV15_STALE_RETRY_MAX_TICKERS=6"
set "EQIDV15_STALE_RETRY_MAX_RATIO=0.08"
set "EQIDV15_SHARED_SCAN_WAIT_SECONDS=120"
set "EQIDV15_SHARED_SCAN_WAIT_POLL_SECONDS=0.5"
set "EQIDV15_LONG_SHARED_FALLBACK_LOCAL_SCAN=0"
set "EQIDV15_LONG_STOP_PCT=0.0075"
set "EQIDV15_LONG_TARGET_PCT=0.011"
set "EQIDV15_LONG_SHARD_SCAN_MAX_WORKERS=1"
set "EQIDV15_LONG_SHARD_SCAN_BLOCK_SIZE=27"
set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_v15_long_shard_%SHARD_ID%.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v15_long_shard_%SHARD_ID%.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv_v15_long_shard_%SHARD_ID%.log"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_live_combined_analyser_csv_v15_long_shard_%SHARD_ID%.status"
set "HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\eqidv2_live_combined_analyser_csv_v15_long_shard_%SHARD_ID%.heartbeat"
set "EQIDV2_RUNTIME_STATUS_FILE=%STATUS_FILE%"
set "EQIDV2_RUNTIME_HEARTBEAT_FILE=%HEARTBEAT_FILE%"
set "EQIDV2_RUNTIME_SCRIPT_NAME=%SCRIPT_NAME%"
set "END_CUTOFF_HHMM=1500"
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
echo [INFO] Shard: id=!SHARD_ID!/!EQIDV15_LONG_SHARD_COUNT! ^| signal_csv=signals_YYYY-MM-DD_v15_long.csv>>"%LOG_FILE%"
echo [INFO] Scan tuning: initial_delay=!EQIDV2_INITIAL_DELAY_SECONDS!s, slot_offset=!EQIDV2_SLOT_START_OFFSET_SECONDS!s, post_ready_stagger=!EQIDV2_POST_READY_STAGGER_SECONDS!s, scans_per_slot=!EQIDV2_NUM_SCANS_PER_SLOT!, interval=!EQIDV2_SCAN_INTERVAL_SECONDS!s, block_parallel=!EQIDV2_BLOCK_PARALLEL_SCAN_ENABLED!, block_size=!EQIDV15_LONG_SHARD_SCAN_BLOCK_SIZE!, max_workers=!EQIDV15_LONG_SHARD_SCAN_MAX_WORKERS!, ready_marker=!EQIDV2_SLOT_READY_MARKER_ENABLED!, ready_poll=!EQIDV2_SLOT_READY_POLL_ENABLED!, ready_max_wait=!EQIDV2_SLOT_READY_MAX_WAIT_SECONDS!s, ready_poll_s=!EQIDV2_SLOT_READY_POLL_SECONDS!s, ready_sample=!EQIDV2_SLOT_READY_SAMPLE_SIZE!, ready_ratio=!EQIDV2_SLOT_READY_MIN_FRESH_RATIO!, stale_only_retry=!EQIDV15_STALE_ONLY_RETRY!, stale_retry_max_tickers=!EQIDV15_STALE_RETRY_MAX_TICKERS!, stale_retry_max_ratio=!EQIDV15_STALE_RETRY_MAX_RATIO!, long_stop_pct=!EQIDV15_LONG_STOP_PCT!, long_target_pct=!EQIDV15_LONG_TARGET_PCT!>>"%LOG_FILE%"
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
  goto AFTER_RUN
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/!MAX_RESTARTS! in !RESTART_DELAY_SEC!s...>>"%LOG_FILE%"
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

:AFTER_RUN
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
if defined STATUS_OVERRIDE (
  >"%STATUS_FILE%" echo status=%STATUS_OVERRIDE%
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
  >>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
  >>"%STATUS_FILE%" echo cutoff_hhmm=%END_CUTOFF_HHMM%
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
) else if "%EXIT_CODE%"=="0" (
  >"%STATUS_FILE%" echo status=SUCCESS
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
  >>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
) else (
  >"%STATUS_FILE%" echo status=FAILED
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
  >>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
)

endlocal & exit /b %EXIT_CODE%
