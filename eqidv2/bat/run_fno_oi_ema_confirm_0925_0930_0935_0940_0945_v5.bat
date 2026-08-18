@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR=C:\TradingData\eqidv2\stocks_indicators_1min_eq"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.log"
set "SCRIPT_PATH=%BASE_DIR%\fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 mode=current-replay>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --mode current-replay --split-day 2026-07-17 --cost-bps 5 --square-off 1530 %* >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
  set "EXIT_CODE=1"
) else (
  set "EXIT_CODE=0"
)
echo [%DATE% %TIME%] END fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 mode=current-replay exit=!EXIT_CODE!>>"%LOG_FILE%"
endlocal & exit /b %EXIT_CODE%
