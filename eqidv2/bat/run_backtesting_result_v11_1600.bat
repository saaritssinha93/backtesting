@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_PATH=%BASE_DIR%\tools\fno_daily_strategy_dashboard.py"
set "VERIFY_SCRIPT=%BASE_DIR%\data_for_backtesting_verify.py"
set "WAIT_SCRIPT=%BASE_DIR%\wait_for_data_backtesting_ready.py"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LATEST_LOG_FILE=%LOG_DIR%\backtesting_result_v11_latest.log"

rem The task/card IDs retain their legacy v11 name for dashboard compatibility.
rem The executed pipeline is exclusively the five-version FnO comparison.
if defined EQIDV2_FNO_BACKTEST_TARGET_DAY (
    set "TARGET_DAY=%EQIDV2_FNO_BACKTEST_TARGET_DAY%"
) else if defined EQIDV2_V11_TARGET_DAY (
    rem Backward-compatible manual override.
    set "TARGET_DAY=%EQIDV2_V11_TARGET_DAY%"
) else (
    for /f %%a in ('powershell -NoProfile -Command "$d=(Get-Date); while($d.DayOfWeek -eq 'Saturday' -or $d.DayOfWeek -eq 'Sunday'){$d=$d.AddDays(-1)}; $d.ToString('yyyy-MM-dd')"') do set "TARGET_DAY=%%a"
)
if not defined TARGET_DAY (
    echo [%DATE% %TIME%] ERROR could not compute TARGET_DAY; aborting FnO comparison.>>"%LATEST_LOG_FILE%"
    endlocal & exit /b 3
)
set "LOG_FILE=%LOG_DIR%\backtesting_result_v11_%TARGET_DAY%.log"

cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START Backtesting result v6/v8/v10/v11/v12 - FnO ^(target trading day=%TARGET_DAY%^)>>"%LOG_FILE%"

rem Same-day runs wait for the 15:45 data build. Historical overrides verify directly.
for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if "%TARGET_DAY%"=="%TODAY_IST%" goto WAIT_FOR_SAME_DAY_DATA
goto VERIFY_HISTORICAL_DATA

:WAIT_FOR_SAME_DAY_DATA
echo [%DATE% %TIME%] Waiting for FnO backtesting data readiness for %TARGET_DAY%...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%WAIT_SCRIPT%" --date "%TARGET_DAY%" --scope fno --timeout-sec 5400 --poll-sec 15 >>"%LOG_FILE%" 2>&1
set "WAIT_EXIT=%ERRORLEVEL%"
if "%WAIT_EXIT%"=="0" goto RUN_BACKTEST
echo [%DATE% %TIME%] END FnO comparison - exit=%WAIT_EXIT% data_not_ready>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
endlocal & exit /b %WAIT_EXIT%

:VERIFY_HISTORICAL_DATA
echo [%DATE% %TIME%] Checking FnO data completeness for %TARGET_DAY%...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%VERIFY_SCRIPT%" --date "%TARGET_DAY%" --scope fno >>"%LOG_FILE%" 2>&1
set "VERIFY_EXIT=%ERRORLEVEL%"
if not "%VERIFY_EXIT%"=="0" (
    echo [%DATE% %TIME%] END FnO comparison - exit=%VERIFY_EXIT% data_verify_failed>>"%LOG_FILE%"
    copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
    endlocal & exit /b %VERIFY_EXIT%
)

:RUN_BACKTEST
echo [%DATE% %TIME%] Running FnO V6/V8/V10/V11/V12 and publishing comparison...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --date "%TARGET_DAY%" %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END Backtesting result v6/v8/v10/v11/v12 - FnO ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1

endlocal & exit /b %EXIT_CODE%
