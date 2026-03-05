@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"

REM Aggressive unified scan tuning:
REM - start sooner after slot boundary
REM - rescan more times per slot even after fetch completion
REM - rescan previous slots to catch delayed parquet finalization
set "EQIDV2_INITIAL_DELAY_SECONDS=6"
set "EQIDV2_NUM_SCANS_PER_SLOT=999"
set "EQIDV2_SCAN_INTERVAL_SECONDS=8"
set "EQIDV2_SLOT_SCAN_BUDGET_SECONDS=840"
set "EQIDV2_SLOT_LOOKBACK_COUNT=2"
set "EQIDV5_STALE_ONLY_RETRY=1"
set "EQIDV5_SHORT_TARGET_PCT=0.009"
set "EQIDV5_LONG_TARGET_PCT=0.011"
set "EQIDV5_LONG_PENDING_POLL_ENABLED=1"
set "EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC=5"
set "EQIDV5_UNIFIED_EMBED_15M_FETCH=1"
set "EQIDV5_UNIFIED_15M_FETCH_MAX_WORKERS=24"
set "EQIDV5_UNIFIED_15M_FETCH_BUFFER_SEC=6"
set "EQIDV5_UNIFIED_15M_FETCH_REFRESH_TOKENS=0"
set "EQIDV5_UNIFIED_15M_FETCH_RESTART_DELAY_SEC=20"

set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_v5_unified.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v5_unified.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv_v5_unified.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v5_unified.status"
set "STATUS_TMP=%STATUS_FILE%.tmp"

REM Unified scanner runs through long horizon.
set "END_CUTOFF_HHMM=1500"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
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
  (
    echo status=SKIPPED_CUTOFF
    echo script=%SCRIPT_NAME%
    echo ts=!RUN_TS!
    echo cutoff_hhmm=%END_CUTOFF_HHMM%
    echo now_hhmm=!NOW_HHMM!
    echo log_file=%LOG_FILE%
  )>"!STATUS_TMP!" 2>nul
  move /Y "!STATUS_TMP!" "%STATUS_FILE%" >nul 2>&1
  if exist "!STATUS_TMP!" del /q "!STATUS_TMP!" >nul 2>&1
  endlocal & exit /b 0
)

REM Single-instance guard: if unified python is already running, skip this launch.
for /f %%a in ('powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process ^| Where-Object { $_.Name -ieq 'python.exe' -and $_.CommandLine -match 'eqidv2_live_combined_analyser_csv_v5_unified\.py' }; if($p){($p ^| Measure-Object).Count}else{0}"') do set "RUNNING_UNIFIED=%%a"
if not defined RUNNING_UNIFIED set "RUNNING_UNIFIED=0"
if !RUNNING_UNIFIED! GEQ 1 (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(already running: count=!RUNNING_UNIFIED!^)
  for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
  (
    echo status=ALREADY_RUNNING
    echo script=%SCRIPT_NAME%
    echo ts=!RUN_TS!
    echo running_count=!RUNNING_UNIFIED!
    echo log_file=%LOG_FILE%
  )>"!STATUS_TMP!" 2>nul
  move /Y "!STATUS_TMP!" "%STATUS_FILE%" >nul 2>&1
  if exist "!STATUS_TMP!" del /q "!STATUS_TMP!" >nul 2>&1
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] Auto-restart enabled: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"
echo [INFO] Scan tuning: initial_delay=%EQIDV2_INITIAL_DELAY_SECONDS%s, scans_per_slot=%EQIDV2_NUM_SCANS_PER_SLOT%, interval=%EQIDV2_SCAN_INTERVAL_SECONDS%s, slot_budget=%EQIDV2_SLOT_SCAN_BUDGET_SECONDS%s, slot_lookback=%EQIDV2_SLOT_LOOKBACK_COUNT%, stale_only_retry=%EQIDV5_STALE_ONLY_RETRY%, short_target_pct=%EQIDV5_SHORT_TARGET_PCT%, long_target_pct=%EQIDV5_LONG_TARGET_PCT%>>"%LOG_FILE%"
echo [INFO] Long pending poll: enabled=%EQIDV5_LONG_PENDING_POLL_ENABLED%, interval=%EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC%s>>"%LOG_FILE%"
echo [INFO] Embedded 15m fetch: enabled=%EQIDV5_UNIFIED_EMBED_15M_FETCH%, workers=%EQIDV5_UNIFIED_15M_FETCH_MAX_WORKERS%, buffer=%EQIDV5_UNIFIED_15M_FETCH_BUFFER_SEC%s, refresh_tokens=%EQIDV5_UNIFIED_15M_FETCH_REFRESH_TOKENS%, restart_delay=%EQIDV5_UNIFIED_15M_FETCH_RESTART_DELAY_SEC%s>>"%LOG_FILE%"

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
set "FINAL_STATUS=FAILED"
if defined STATUS_OVERRIDE (
  set "FINAL_STATUS=%STATUS_OVERRIDE%"
) else if "%EXIT_CODE%"=="0" (
  set "FINAL_STATUS=SUCCESS"
)
(
  echo status=!FINAL_STATUS!
  echo script=%SCRIPT_NAME%
  echo ts=!RUN_TS!
  echo exit_code=%EXIT_CODE%
  echo restart_count=!RESTART_COUNT!
  if defined STATUS_OVERRIDE echo cutoff_hhmm=%END_CUTOFF_HHMM%
  echo log_file=%LOG_FILE%
)>"!STATUS_TMP!" 2>nul
move /Y "!STATUS_TMP!" "%STATUS_FILE%" >nul 2>&1
if exist "!STATUS_TMP!" del /q "!STATUS_TMP!" >nul 2>&1

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
    "try { $ws = New-Object -ComObject WScript.Shell; [void]$ws.Popup('CRITICAL FAILURE in eqidv2_live_combined_analyser_csv_v5_unified.py`nExitCode: %EXIT_CODE%`nSee log: %LOG_FILE%', 10, 'EQIDV2 ALERT', 16) } catch { }"

  echo [ALERT] CRITICAL: %SCRIPT_NAME% failed. ExitCode=%EXIT_CODE%
  echo [ALERT] AlertFile=!ALERT_FILE!
)

endlocal & exit /b %EXIT_CODE%
