@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
rem Flat 1% stop and 1% target on both sides, per the FPA paper-trade spec.
set "FPA_V1_STOP_PCT=0.01"
set "FPA_V1_TARGET_PCT=0.01"
set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=fundamental_price_action_v1_papertrade.py"
set "SCRIPT_PATH=%BASE_DIR%\%SCRIPT_NAME%"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\fundamental_price_action_v1_papertrade.status"
set "HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\fundamental_price_action_v1_papertrade.heartbeat"
set "END_CUTOFF_HHMM=1530"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\fundamental_price_action_v1_papertrade_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
cd /d "%BASE_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%LOG_FILE%"
  call :WRITE_STATUS SKIPPED_CUTOFF 0
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] Auto-restart enabled: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"

:RUN_LOOP
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --stop-pct %FPA_V1_STOP_PCT% --target-pct %FPA_V1_TARGET_PCT% --square-off-time 15:15 --end-time 15:30 %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=!EXIT_CODE!^)>>"%LOG_FILE%"

if "!EXIT_CODE!"=="0" goto DONE

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [WARN] Crash after cutoff ^(HHmm=!NOW_HHMM!^). Not restarting.>>"%LOG_FILE%"
  set "EXIT_CODE=0"
  call :WRITE_STATUS STOPPED_AFTER_CUTOFF !EXIT_CODE!
  goto DONE
)

set /a RESTART_COUNT+=1
if !RESTART_COUNT! GTR %MAX_RESTARTS% (
  echo [ERROR] Max restarts exceeded for %SCRIPT_NAME% ^(attempts=!RESTART_COUNT!^).>>"%LOG_FILE%"
  call :WRITE_STATUS CRASHED !EXIT_CODE!
  goto DONE
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=!EXIT_CODE!^). Restart !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%LOG_FILE%"
call :WRITE_STATUS RESTARTING !EXIT_CODE!
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

:WRITE_STATUS
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
>"%STATUS_FILE%" echo status=%~1
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo session=fundamental_price_action_v1_papertrade
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo restart_count=!RESTART_COUNT!
>>"%STATUS_FILE%" echo exit_code=%~2
>>"%STATUS_FILE%" echo cutoff_hhmm=%END_CUTOFF_HHMM%
>>"%STATUS_FILE%" echo log_file=%LOG_FILE%
>"%HEARTBEAT_FILE%" echo state=%~1
>>"%HEARTBEAT_FILE%" echo script=%SCRIPT_NAME%
>>"%HEARTBEAT_FILE%" echo session=fundamental_price_action_v1_papertrade
>>"%HEARTBEAT_FILE%" echo ts=!RUN_TS!
>>"%HEARTBEAT_FILE%" echo restart_count=!RESTART_COUNT!
exit /b 0

:DONE
endlocal & exit /b %EXIT_CODE%
