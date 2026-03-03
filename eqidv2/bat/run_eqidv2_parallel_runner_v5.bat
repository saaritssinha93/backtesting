@echo off
REM run_eqidv2_parallel_runner_v5.bat
REM ===================================
REM Launches eqidv2_parallel_runner_v5.py which runs all three live-day
REM processes simultaneously in isolated subprocesses:
REM   [15M  ] eqidv2_eod_scheduler_for_15mins_data.py
REM   [SHORT] eqidv2_live_combined_analyser_csv_v5_short.py
REM   [LONG ] eqidv2_live_combined_analyser_csv_v5_long.py
REM
REM The Python runner handles per-process auto-restart internally.
REM This bat adds an outer restart loop for the runner itself (e.g. Python crash).
REM
REM Replaces running these three bat files separately:
REM   run_eqidv2_eod_scheduler_for_15mins_data.bat
REM   run_eqidv2_live_combined_analyser_csv_v5_short.bat
REM   run_eqidv2_live_combined_analyser_csv_v5_long.bat
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

set "PYTHONUNBUFFERED=1"

REM -- 15m data scheduler env vars (matches run_eqidv2_eod_scheduler_for_15mins_data.bat) --
set "EQIDV2_15M_MAX_WORKERS=24"
set "EQIDV2_15M_BUFFER_SEC=6"
set "EQIDV2_15M_REFRESH_TOKENS=0"

REM -- Live scanner shared env vars (matches both v5_short and v5_long bat files) --
set "EQIDV2_INITIAL_DELAY_SECONDS=10"
set "EQIDV2_NUM_SCANS_PER_SLOT=3"
set "EQIDV2_SCAN_INTERVAL_SECONDS=5"
set "EQIDV5_STALE_ONLY_RETRY=1"
set "EQIDV5_LONG_PENDING_POLL_ENABLED=1"
set "EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC=5"

set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "SCRIPT_NAME=eqidv2_parallel_runner_v5.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_parallel_runner_v5.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_parallel_runner_v5.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_parallel_runner_v5.status"

REM Outer bat restart cutoff = HARD_CUTOFF of the Python runner (15:55).
set "END_CUTOFF_HHMM=1555"

REM Low outer restart count -- the Python runner manages per-process restarts internally.
set "MAX_RESTARTS=3"
set "RESTART_DELAY_SEC=30"
set /a RESTART_COUNT=0
set "STATUS_OVERRIDE="

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%ALERT_DIR%" mkdir "%ALERT_DIR%"

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
echo [INFO] Parallel runner covers: eod_15min_data + live_combined_v5_short + live_combined_v5_long>>"%LOG_FILE%"
echo [INFO] Outer restart loop: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"
echo [INFO] 15m env: max_workers=%EQIDV2_15M_MAX_WORKERS%, buffer_sec=%EQIDV2_15M_BUFFER_SEC%, refresh_tokens=%EQIDV2_15M_REFRESH_TOKENS%>>"%LOG_FILE%"
echo [INFO] Scan tuning: initial_delay=%EQIDV2_INITIAL_DELAY_SECONDS%s, scans_per_slot=%EQIDV2_NUM_SCANS_PER_SLOT%, interval=%EQIDV2_SCAN_INTERVAL_SECONDS%s>>"%LOG_FILE%"
echo [INFO] v5 tuning: stale_only_retry=%EQIDV5_STALE_ONLY_RETRY%, pending_poll=%EQIDV5_LONG_PENDING_POLL_ENABLED%, pending_poll_interval=%EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC%s>>"%LOG_FILE%"

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

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%LOG_FILE%"
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

if not "%EXIT_CODE%"=="0" (
  for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%a"
  set "ALERT_FILE=%ALERT_DIR%\CRITICAL_%SCRIPT_NAME:.py=%_FAILED_!TS!.txt"

  (
    echo ================================================
    echo CRITICAL FAILURE: %SCRIPT_NAME%
    echo DateTime: %DATE% %TIME%
    echo ExitCode: %EXIT_CODE%
    echo Host: %COMPUTERNAME%
    echo User: %USERNAME%
    echo LogFile: %LOG_FILE%
    echo ================================================
  )>"!ALERT_FILE!"

  type "!ALERT_FILE!"

  (
    echo ================================================
    echo CRITICAL FAILURE: %SCRIPT_NAME%
    echo DateTime: %DATE% %TIME%
    echo ExitCode: %EXIT_CODE%
    echo AlertFile: !ALERT_FILE!
    echo LogFile: %LOG_FILE%
    echo ================================================
  )>>"%ALERT_LOG%"

  eventcreate /L APPLICATION /T ERROR /ID 9001 /SO EQIDV2 /D "CRITICAL: %SCRIPT_NAME% failed with exit code %EXIT_CODE%. See %LOG_FILE%" >nul 2>&1
  msg %USERNAME% /TIME:15 "CRITICAL: %SCRIPT_NAME% failed (exit=%EXIT_CODE%) - check %LOG_FILE%" >nul 2>&1

  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "try { $ws = New-Object -ComObject WScript.Shell; [void]$ws.Popup('CRITICAL FAILURE in eqidv2_parallel_runner_v5.py`nExitCode: %EXIT_CODE%`nSee log: %LOG_FILE%', 10, 'EQIDV2 ALERT', 16) } catch { }"

  echo [ALERT] CRITICAL: %SCRIPT_NAME% failed. ExitCode=%EXIT_CODE%
  echo [ALERT] AlertFile=!ALERT_FILE!
)

endlocal & exit /b %EXIT_CODE%
