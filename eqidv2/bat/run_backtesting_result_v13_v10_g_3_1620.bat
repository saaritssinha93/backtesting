@echo off
setlocal EnableExtensions DisableDelayedExpansion
title Backtesting result v13-v10-G-3
rem The installed weekday trigger is 17:15; _1620 is the historical filename.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "FNO_ACTIVE_STRATEGY_PROFILE=V13_V10_G_3"
set "EQIDV2_OBSERVABILITY_ENABLED=1"
if "%OTEL_EXPORTER_OTLP_TRACES_ENDPOINT%"=="" set "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces"
if "%OTEL_EXPORTER_OTLP_TRACES_PROTOCOL%"=="" set "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf"
for /f %%a in ('powershell -NoProfile -NonInteractive -Command "[guid]::NewGuid().ToString()"') do set "EQIDV2_OBS_RUN_ID=%%a"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "SCRIPT_PATH=%BASE_DIR%\backtesting_result_v13_v10_g_3_daily.py"
set "LOG_DIR=%BASE_DIR%\logs"
set "LATEST_LOG_FILE=%LOG_DIR%\backtesting_result_v13_v10_g_3_latest.log"

rem Resolve exactly one date before naming the log or starting the orchestrator.
rem Explicit CLI date wins over the current and legacy environment overrides.
set "TARGET_DAY="
if defined EQIDV2_V11_TARGET_DAY set "TARGET_DAY=%EQIDV2_V11_TARGET_DAY%"
if defined EQIDV2_FNO_BACKTEST_TARGET_DAY set "TARGET_DAY=%EQIDV2_FNO_BACKTEST_TARGET_DAY%"
if not "%~1"=="" (
    if /I not "%~1"=="--date" goto INVALID_ARGUMENTS
    if "%~2"=="" goto INVALID_ARGUMENTS
    if not "%~3"=="" goto INVALID_ARGUMENTS
    set "TARGET_DAY=%~2"
)
set "VALIDATED_TARGET_DAY="
for /f %%a in ('powershell -NoProfile -NonInteractive -Command "try { if ([string]::IsNullOrWhiteSpace($env:TARGET_DAY)) { $taskDay=[TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow,'India Standard Time') } else { $taskDay=[DateTime]::ParseExact($env:TARGET_DAY,'yyyy-MM-dd',[Globalization.CultureInfo]::InvariantCulture) }; $taskDay.ToString('yyyy-MM-dd') } catch { exit 3 }"') do set "VALIDATED_TARGET_DAY=%%a"
if not defined VALIDATED_TARGET_DAY (
    echo [ERROR] A valid YYYY-MM-DD target date is required; no backtest was started.
    endlocal & exit /b 3
)
set "TARGET_DAY=%VALIDATED_TARGET_DAY%"
if not exist "%SCRIPT_PATH%" (
    echo [ERROR] Missing G daily orchestrator: %SCRIPT_PATH%
    endlocal & exit /b 3
)
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\backtesting_result_v13_v10_g_3_%TARGET_DAY%.log"

cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START Backtesting result v13-v10-G-3 ^(session date=%TARGET_DAY% IST^)>>"%LOG_FILE%"
rem Recover the exact 09:15-09:16 futures candle before snapshotting current G3.
rem The separate 17:00 task remains a retry; writes stay in observer storage.
"%PYTHON_EXE%" -u "%BASE_DIR%\dashboard_flow_observer_history.py" --from-date "%TARGET_DAY%" --to-date "%TARGET_DAY%" --fetch --import-local >>"%LOG_FILE%" 2>&1
if errorlevel 1 echo [WARN] Opening recovery failed; replay coverage will block any incomplete result.>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --date "%TARGET_DAY%" --wait-for-data >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" goto FINALIZE

rem Extend the verified full historical backtest through this completed day.
rem Publish only after all same-contract sessions and accounting reconcile.
echo [%DATE% %TIME%] START G3 full historical backtest refresh>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_v13_v10_g_3_backtest.py" --through "%TARGET_DAY%" >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" goto FINALIZE

rem The publisher has committed the daily run and cumulative history. Refresh
rem the G3 analysis artifacts without changing its successful backtest status.
echo [%DATE% %TIME%] START G3 execution analysis>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%BASE_DIR%\tools\v13_execution_research.py" >>"%LOG_FILE%" 2>&1
set "EXECUTION_ANALYSIS_EXIT=%ERRORLEVEL%"
if not "%EXECUTION_ANALYSIS_EXIT%"=="0" (
    echo [ERROR] G3 execution analysis failed; exit=%EXECUTION_ANALYSIS_EXIT%>>"%LOG_FILE%"
    set "EXIT_CODE=%EXECUTION_ANALYSIS_EXIT%"
)
echo [%DATE% %TIME%] START G3 strategy analysis>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%BASE_DIR%\tools\v13_strategy_research.py" >>"%LOG_FILE%" 2>&1
set "STRATEGY_ANALYSIS_EXIT=%ERRORLEVEL%"
if not "%STRATEGY_ANALYSIS_EXIT%"=="0" (
    echo [ERROR] G3 strategy analysis failed; exit=%STRATEGY_ANALYSIS_EXIT%>>"%LOG_FILE%"
    set "EXIT_CODE=%STRATEGY_ANALYSIS_EXIT%"
)

rem G3 daily/historical backtests use V12: 51 setups, target above 0.75%% plus three restored exceptions.
rem Restored: 09:50 LONG 0.75%%, 10:05 LONG 0.56%%, 10:15 SHORT 0.68%%; ten other low-target setups remain disabled.
rem The 10:35 LONG/SHORT confirmations reject 1m EMA20 extension above 3x ATR14.
rem PAPER/LIVE retain their separately versioned 61-setup production V10 profile with the same 10:35 guard.

:FINALIZE
echo [%DATE% %TIME%] END G3 production backtest (exit=%EXIT_CODE%)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
endlocal & exit /b %EXIT_CODE%

:INVALID_ARGUMENTS
echo [ERROR] Usage: run_backtesting_result_v13_v10_g_3_1620.bat [--date YYYY-MM-DD]
endlocal & exit /b 2
