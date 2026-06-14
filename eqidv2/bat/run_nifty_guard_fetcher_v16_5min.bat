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
REM Legacy loop fetches only the live-gate ETF proxy. The supervised wrapper
REM fetches true NIFTY 50 separately into NIFTY/NIFTY50 aliases.
set "NIFTY_ALIASES=NIFTYBEES,NIFTYBEES_PROXY"
set "FIRST_SLOT_HHMM=0915"
set "POLL_SEC=1"
set "SLOT_OFFSET_SEC=2"
set "END_CUTOFF_HHMM=1531"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
REM strategy_v2 §J3 fix #12 — bounded intra-slot retries so one transient
REM fetch failure doesn't blank out an entire slot's NIFTY RS gate.
set "NF_SLOT_MAX_RETRIES=3"
set "NF_SLOT_RETRY_DELAY_SEC=5"
set "NF_SLOT_FAIL_DIR=%EQIDV2_RUNTIME_ROOT%\nifty_slot_fail_5m"
set /a RESTART_COUNT=0
set "LAST_SLOT="
set "SLOT_RETRY_HHMM="
set /a SLOT_RETRY_COUNT=0

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] NIFTY guard fetch loop: symbol=%NIFTY_SYMBOL%, aliases=%NIFTY_ALIASES%, slot_minutes=00/05/10/15/20/25/30/35/40/45/50/55, first_slot=%FIRST_SLOT_HHMM%, cutoff=%END_CUTOFF_HHMM%, poll=%POLL_SEC%s, slot_offset=%SLOT_OFFSET_SEC%s>>"%LOG_FILE%"

:RUN_LOOP
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmmss')"') do set "NOW_HHMMSS=%%a"
set "NOW_HHMM=!NOW_HHMMSS:~0,4!"
set "NOW_SS=!NOW_HHMMSS:~4,2!"
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
if !NOW_SS! LSS %SLOT_OFFSET_SEC% set "SLOT_READY=0"
if not "!SLOT_READY!"=="1" (
  timeout /t %POLL_SEC% >nul
  goto RUN_LOOP
)
if "!LAST_SLOT!"=="!NOW_HHMM!" (
  timeout /t %POLL_SEC% >nul
  goto RUN_LOOP
)

REM strategy_v2 §J3 fix #12 — reset the per-slot retry counter when the
REM slot boundary changes so each new slot gets a fresh attempt budget.
if not "!SLOT_RETRY_HHMM!"=="!NOW_HHMM!" (
  set "SLOT_RETRY_HHMM=!NOW_HHMM!"
  set /a SLOT_RETRY_COUNT=0
)

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --symbol "%NIFTY_SYMBOL%" --aliases "%NIFTY_ALIASES%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] FETCH %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] FETCH %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

if not "%EXIT_CODE%"=="0" (
  set /a RESTART_COUNT+=1
  if !RESTART_COUNT! GTR %MAX_RESTARTS% goto DONE
  set /a SLOT_RETRY_COUNT+=1
  echo [WARN] %SCRIPT_NAME% failed ^(exit=%EXIT_CODE%^). Slot-retry !SLOT_RETRY_COUNT!/%NF_SLOT_MAX_RETRIES% for slot !NOW_HHMM! ^(global !RESTART_COUNT!/%MAX_RESTARTS%^) in %NF_SLOT_RETRY_DELAY_SEC%s...>>"%LOG_FILE%"
  if !SLOT_RETRY_COUNT! GEQ %NF_SLOT_MAX_RETRIES% (
    REM Slot-retry budget exhausted: mark this slot done so the loop moves
    REM on instead of spinning, and emit a fail marker DE can consult.
    set "LAST_SLOT=!NOW_HHMM!"
    if not exist "%NF_SLOT_FAIL_DIR%" mkdir "%NF_SLOT_FAIL_DIR%"
    for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyyMMdd')"') do set "TODAY_YMD=%%a"
    for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-ddTHH:mm:sszzz"') do set "FAIL_TS=%%a"
    set "FAIL_MARKER=%NF_SLOT_FAIL_DIR%\nifty_slot_fail_!TODAY_YMD!_!NOW_HHMM!.json"
    >"!FAIL_MARKER!" echo {"slot":"!TODAY_YMD!_!NOW_HHMM!","symbol":"%NIFTY_SYMBOL%","exit_code":%EXIT_CODE%,"retries":!SLOT_RETRY_COUNT!,"emitted_at":"!FAIL_TS!"}
    echo [ABORT] NF_SLOT_RETRY_EXHAUSTED slot=!NOW_HHMM! retries=!SLOT_RETRY_COUNT! marker=!FAIL_MARKER!>>"%LOG_FILE%"
  )
  timeout /t %NF_SLOT_RETRY_DELAY_SEC% >nul
  goto RUN_LOOP
)

REM strategy_v2 §J3 fix #12 — only mark the slot as complete on success.
REM Under the old code this was set unconditionally above the exit-code
REM check, which blocked retries of transient failures for the rest of the
REM minute and silently blanked the slot's RS gate.
set "LAST_SLOT=!NOW_HHMM!"

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
