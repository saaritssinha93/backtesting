@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_PATH=%BASE_DIR%\collect_filtered_stock_data_session.py"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\collect_filtered_stock_data_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START collect_filtered_stock_data>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END collect_filtered_stock_data ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

endlocal & exit /b %EXIT_CODE%
