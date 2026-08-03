@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_PATH=%BASE_DIR%\v7_full_pipeline_entry_research_v2.py"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\v7_full_pipeline_entry_research_v2_%TODAY_IST%.log"
set "LATEST_LOG_FILE=%LOG_DIR%\v7_full_pipeline_entry_research_v2_latest.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START Full-Pipeline Entry Research v2>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --since 2025-11-01 --sl-pct 0.85 --tgt-pct 1.10 --cost-bps 16 --adv-min-cr 50 %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END Full-Pipeline Entry Research v2 ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1

endlocal & exit /b %EXIT_CODE%
