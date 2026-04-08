@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS=10000"
set "EQIDV16_5MIN_FORCE_SIGNAL_QUANTITY=1"
set "EQIDV16_5MIN_SLOT_READY_MIN_FRESH_RATIO=0.90"
set "EQIDV16_5MIN_SLOT_READY_MARKER_MIN_FRESH_RATIO=0.90"
set "EQIDV16_5MIN_SLOT_READY_PRIORITY_TICKERS=NIFTYBEES,SBIN,RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY"

set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_v16_5min.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v16_5min.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv_v16_5min.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v16_5min.heartbeat"
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
