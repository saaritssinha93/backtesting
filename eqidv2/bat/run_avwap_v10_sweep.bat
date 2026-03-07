@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"

set "OUT_DIR=%BASE_DIR%\outputs_v10_sweep"
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

cd /d "%BASE_DIR%"

echo [INFO] Starting V10 backtest...
"%PYTHON_EXE%" -u "%BASE_DIR%\avwap_combined_runner_v10_sweep.py" > "%OUT_DIR%\run_v10.txt" 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo [INFO] Done. Exit code=%EXIT_CODE%
endlocal & exit /b %EXIT_CODE%
