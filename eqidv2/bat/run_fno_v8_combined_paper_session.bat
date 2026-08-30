@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "FNO_V8_COMBINED_EXECUTION_MODE=PAPER"

set "SESSION_SCRIPT=%BASE_DIR%\fno_v8_combined_paper_session.py"
set "LOG_FILE=%BASE_DIR%\logs\fno_v8_combined_paper.log"

if not exist "%BASE_DIR%\logs" mkdir "%BASE_DIR%\logs"
if not exist "%SESSION_SCRIPT%" (
  >>"%LOG_FILE%" echo [FATAL] Missing V8-Combined paper session: %SESSION_SCRIPT%
  endlocal & exit /b 2
)

cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%SESSION_SCRIPT%" run %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
