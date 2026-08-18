@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\fno_oi_eod_qc.log"
set "SCRIPT_PATH=%BASE_DIR%\fno_oi_eod_qc.py"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START fno_oi_eod_qc>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END fno_oi_eod_qc exit=%EXIT_CODE%>>"%LOG_FILE%"
endlocal & exit /b %EXIT_CODE%
