@echo off
setlocal EnableExtensions EnableDelayedExpansion

:: ============================================================
:: V20 5-min backtest — Relative Acceptance Index (RAI)
::
:: RAI = 0.35*RS_2bar + 0.25*RS_4bar + 0.20*AVWAP_acceptance
::     + 0.10*OR_break_quality + 0.10*volume_quality
::     - exhaustion_penalty
::
:: Tunable via constants in avwap_combined_runner_v20_5min.py:
::   RAI_ENABLED              default True
::   RAI_LONG_THRESHOLD       default 0.35
::   RAI_SHORT_THRESHOLD      default 0.35
::   RAI_RS2_LOOKBACK_BARS    default 2
:: ============================================================

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

:: ── V16 base runtime settings ────────────────────────────────────────────
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\backtest_v20_5min.log"

set "SCRIPT_NAME=avwap_combined_runner_v20_5min.py"

cd /d "%BASE_DIR%"

echo ============================================================
echo  V20 5-min BACKTEST — Relative Acceptance Index (RAI)
echo  RAI LONG  threshold : 0.35
echo  RAI SHORT threshold : 0.35
echo  RS_2bar lookback    : 2 bars
echo  RS_4bar lookback    : 4 bars
echo  Output dir          : outputs_v20_5min\
echo  Log                 : %LOG_FILE%
echo ============================================================

echo [%DATE% %TIME%] START %SCRIPT_NAME% >> "%LOG_FILE%"

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" 2>&1 | tee -a "%LOG_FILE%"
set "EXIT_CODE=!ERRORLEVEL!"

echo [%DATE% %TIME%] END %SCRIPT_NAME% (exit=!EXIT_CODE!) >> "%LOG_FILE%"
echo.
echo Done. Exit code: !EXIT_CODE!
if !EXIT_CODE! NEQ 0 (
    echo [ERROR] Backtest failed. Check %LOG_FILE%
) else (
    echo [OK] Results in: %BASE_DIR%\outputs_v20_5min\
)

endlocal & exit /b %EXIT_CODE%
