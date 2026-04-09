@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py"
set "LOG_FILE=%LOG_DIR%\nifty_guard_fetcher_v16_5min.log"
set "STATUS_FILE=%LOG_DIR%\nifty_guard_fetcher_v16_5min.status"
set "NIFTY_SYMBOL=NIFTYBEES"
set "NIFTY_ALIASES=NIFTYBEES,NIFTY50,NIFTY_50,NIFTY"
set "FIRST_SLOT_HHMM=0915"
set "POLL_SEC=2"
set "END_CUTOFF_HHMM=1531"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0
set "LAST_SLOT="

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] NIFTY guard fetch loop: symbol=%NIFTY_SYMBOL%, aliases=%NIFTY_ALIASES%, slot_minutes=00/05/10/15/20/25/30/35/40/45/50/55, first_slot=%FIRST_SLOT_HHMM%, cutoff=%END_CUTOFF_HHMM%, poll=%POLL_SEC%s>>"%LOG_FILE%"

:RUN_LOOP
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% goto DONE
if !NOW_HHMM! LSS %FIRST_SLOT_HHMM% (
  timeout /t %POLL_SEC% >nul
  goto RUN_LOOP
)

set "NOW_MM=!NOW_HHMM:~2,2!"
set "SLOT_READY=0"
if "!NOW_MM!"=="00" set "SLOT_READY=1"
if "!NOW_MM!"=="05" set "SLOT_READY=1"
if "!NOW_MM!"=="10" set "SLOT_READY=1"
if "!NOW_MM!"=="15" set "SLOT_READY=1"
if "!NOW_MM!"=="20" set "SLOT_READY=1"
if "!NOW_MM!"=="25" set "SLOT_READY=1"
if "!NOW_MM!"=="30" set "SLOT_READY=1"
if "!NOW_MM!"=="35" set "SLOT_READY=1"
if "!NOW_MM!"=="40" set "SLOT_READY=1"
if "!NOW_MM!"=="45" set "SLOT_READY=1"
if "!NOW_MM!"=="50" set "SLOT_READY=1"
if "!NOW_MM!"=="55" set "SLOT_READY=1"
if not "!SLOT_READY!"=="1" (
  timeout /t %POLL_SEC% >nul
  goto RUN_LOOP
)
if "!LAST_SLOT!"=="!NOW_HHMM!" (
  timeout /t %POLL_SEC% >nul
  goto RUN_LOOP
)

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --symbol "%NIFTY_SYMBOL%" --aliases "%NIFTY_ALIASES%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
set "LAST_SLOT=!NOW_HHMM!"

echo [%DATE% %TIME%] FETCH %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] FETCH %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

if not "%EXIT_CODE%"=="0" (
  set /a RESTART_COUNT+=1
  if !RESTART_COUNT! GTR %MAX_RESTARTS% goto DONE
  echo [WARN] %SCRIPT_NAME% failed ^(exit=%EXIT_CODE%^). Retry !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%LOG_FILE%"
  timeout /t %RESTART_DELAY_SEC% >nul
  goto RUN_LOOP
)

for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
>"%STATUS_FILE%" echo status=SUCCESS
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo symbol=%NIFTY_SYMBOL%
>>"%STATUS_FILE%" echo aliases=%NIFTY_ALIASES%
>>"%STATUS_FILE%" echo log_file=%LOG_FILE%

timeout /t %POLL_SEC% >nul
goto RUN_LOOP

:DONE
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
>"%STATUS_FILE%" echo status=STOPPED
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo symbol=%NIFTY_SYMBOL%
>>"%STATUS_FILE%" echo aliases=%NIFTY_ALIASES%
>>"%STATUS_FILE%" echo log_file=%LOG_FILE%

endlocal & exit /b 0
