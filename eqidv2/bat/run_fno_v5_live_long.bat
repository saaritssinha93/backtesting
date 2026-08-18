@echo off
setlocal EnableExtensions
set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "FNO_V5_EXECUTION_MODE=PAPER"
cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_v5_live.py" --role long-entry %* >>"%BASE_DIR%\logs\fno_v5_live_long.log" 2>&1
endlocal & exit /b %ERRORLEVEL%
