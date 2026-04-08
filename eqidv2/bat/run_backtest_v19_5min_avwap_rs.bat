@echo off
setlocal EnableExtensions EnableDelayedExpansion

:: ============================================================
:: V19 5-min backtest — AVWAP-RS filter
::
:: AVWAP_RS = (stock_close/stock_day_VWAP - 1)*100
::          - (nifty_close/nifty_day_VWAP - 1)*100
::
:: Tunable env vars (override defaults):
::   EQIDV19_AVWAP_RS_THRESH_LONG_BOTH        default 0.30
::   EQIDV19_AVWAP_RS_THRESH_SHORT_BOTH       default 0.30
::   EQIDV19_AVWAP_RS_THRESH_LONG_DIRECTIONAL default 0.15
::   EQIDV19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL default 0.15
::   EQIDV19_AVWAP_RS_EXHAUST_CAP             default 3.0
::   EQIDV19_RS_MOMENTUM_ENABLED              default false
::   EQIDV19_FALLBACK_RS_LONG                 default 0.40
::   EQIDV19_FALLBACK_RS_SHORT                default 0.40
:: ============================================================

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

:: ── V19 AVWAP-RS thresholds (edit here or pass as env vars) ──────────────
:: BOTH mode (neutral market): how much more above VWAP vs Nifty required
set "EQIDV19_AVWAP_RS_THRESH_LONG_BOTH=0.30"
set "EQIDV19_AVWAP_RS_THRESH_SHORT_BOTH=0.30"
:: Directional modes (LONG_ONLY / SHORT_ONLY): lower bar
set "EQIDV19_AVWAP_RS_THRESH_LONG_DIRECTIONAL=0.15"
set "EQIDV19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL=0.15"
:: Anti-exhaustion cap: reject if AVWAP_RS > +cap or < -cap
set "EQIDV19_AVWAP_RS_EXHAUST_CAP=3.0"
:: RS momentum gate (false = pure AVWAP_RS only; true = also require RS direction)
set "EQIDV19_RS_MOMENTUM_ENABLED=false"
:: Fallback when VWAP data is unavailable for a bar
set "EQIDV19_FALLBACK_RS_LONG=0.40"
set "EQIDV19_FALLBACK_RS_SHORT=0.40"

:: ── V16 base runtime settings ────────────────────────────────────────────
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\backtest_v19_5min.log"

set "SCRIPT_NAME=avwap_combined_runner_v19_5min.py"

cd /d "%BASE_DIR%"

echo ============================================================
echo  V19 5-min BACKTEST — AVWAP-RS filter
echo  LONG  BOTH  threshold : %EQIDV19_AVWAP_RS_THRESH_LONG_BOTH%%%
echo  SHORT BOTH  threshold : %EQIDV19_AVWAP_RS_THRESH_SHORT_BOTH%%%
echo  LONG  dir   threshold : %EQIDV19_AVWAP_RS_THRESH_LONG_DIRECTIONAL%%%
echo  SHORT dir   threshold : %EQIDV19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL%%%
echo  Exhaust cap           : +/-%EQIDV19_AVWAP_RS_EXHAUST_CAP%%%
echo  RS momentum gate      : %EQIDV19_RS_MOMENTUM_ENABLED%
echo  Output dir            : outputs_v19_5min\
echo  Log                   : %LOG_FILE%
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
    echo [OK] Results in: %BASE_DIR%\outputs_v19_5min\
)

endlocal & exit /b %EXIT_CODE%
