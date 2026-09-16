@echo off
setlocal EnableExtensions DisableDelayedExpansion
title Backtesting result v13-v10-G

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "SCRIPT_PATH=%BASE_DIR%\backtesting_result_v13_v10_g_daily.py"
set "LOG_DIR=%BASE_DIR%\logs"
set "LATEST_LOG_FILE=%LOG_DIR%\backtesting_result_v13_v10_g_latest.log"

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
set "LOG_FILE=%LOG_DIR%\backtesting_result_v13_v10_g_%TARGET_DAY%.log"

cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START Backtesting result v13-v10-G ^(session date=%TARGET_DAY% IST^)>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --date "%TARGET_DAY%" --wait-for-data >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END Backtesting result v13-v10-G ^(session date=%TARGET_DAY% IST, exit=%EXIT_CODE%^)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
endlocal & exit /b %EXIT_CODE%

:INVALID_ARGUMENTS
echo [ERROR] Usage: run_backtesting_result_v13_v10_g_1620.bat [--date YYYY-MM-DD]
echo [ERROR] Additional arguments are refused so the executed date and log date remain identical.
endlocal & exit /b 2
