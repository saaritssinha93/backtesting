@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
if not defined EQIDV2_V11_LAB_SHADOW_CANDIDATE_MODULE set "EQIDV2_V11_LAB_SHADOW_CANDIDATE_MODULE=final_setup_conf_v11_conf_d"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_PATH=%BASE_DIR%\v11_lab_shadow_monitor.py"

if defined EQIDV2_V11_LAB_SHADOW_TARGET_DAY (
    set "TARGET_DAY=%EQIDV2_V11_LAB_SHADOW_TARGET_DAY%"
) else (
    for /f %%a in ('powershell -NoProfile -Command "$d=(Get-Date); while($d.DayOfWeek -eq 'Saturday' -or $d.DayOfWeek -eq 'Sunday'){$d=$d.AddDays(-1)}; $d.ToString('yyyy-MM-dd')"') do set "TARGET_DAY=%%a"
)
if not defined TARGET_DAY set "TARGET_DAY=%DATE%"

set "LOG_FILE=%LOG_DIR%\v11_lab_shadow_monitor_%TARGET_DAY%.log"
set "LATEST_LOG_FILE=%LOG_DIR%\v11_lab_shadow_monitor_latest.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START V11 Lab Shadow Monitor target=%TARGET_DAY%>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --date "%TARGET_DAY%" %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END V11 Lab Shadow Monitor ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1

endlocal & exit /b %EXIT_CODE%
