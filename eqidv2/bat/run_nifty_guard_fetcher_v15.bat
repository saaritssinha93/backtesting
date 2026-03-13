@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py"
set "LOG_FILE=%LOG_DIR%\nifty_guard_fetcher_v15.log"
set "STATUS_FILE=%LOG_DIR%\nifty_guard_fetcher_v15.status"
set "NIFTY_SYMBOL=NIFTYBEES"
set "NIFTY_ALIASES=NIFTYBEES,NIFTY50,NIFTY_50,NIFTY"
set "FETCH_INTERVAL_SEC=900"
set "END_CUTOFF_HHMM=1531"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] NIFTY guard fetch loop: symbol=%NIFTY_SYMBOL%, aliases=%NIFTY_ALIASES%, interval=%FETCH_INTERVAL_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"

:RUN_LOOP
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% goto DONE

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --symbol "%NIFTY_SYMBOL%" --aliases "%NIFTY_ALIASES%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

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

timeout /t %FETCH_INTERVAL_SEC% >nul
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
