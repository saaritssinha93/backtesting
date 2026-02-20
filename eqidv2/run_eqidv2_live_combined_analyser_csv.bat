@echo off
setlocal

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=python"
set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START eqidv2_live_combined_analyser_csv.py >> "%LOG_DIR%\eqidv2_live_combined_analyser_csv.log"
"%PYTHON_EXE%" "%BASE_DIR%\eqidv2_live_combined_analyser_csv.py" >> "%LOG_DIR%\eqidv2_live_combined_analyser_csv.log" 2>&1
echo [%DATE% %TIME%] END eqidv2_live_combined_analyser_csv.py >> "%LOG_DIR%\eqidv2_live_combined_analyser_csv.log"

endlocal
