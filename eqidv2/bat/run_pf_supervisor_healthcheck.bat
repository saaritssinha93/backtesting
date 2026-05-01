@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "LOG_DIR=%BASE_DIR%\logs"
set "PS1=%BAT_DIR%\check_pf_supervisor_health.ps1"
set "RUN_LOG=%LOG_DIR%\pf_supervisor_healthcheck_runs.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [%DATE% %TIME%] START check_pf_supervisor_health.ps1 >> "%RUN_LOG%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" >> "%RUN_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END   check_pf_supervisor_health.ps1 (exit=%EXIT_CODE%) >> "%RUN_LOG%"

endlocal & exit /b %EXIT_CODE%
