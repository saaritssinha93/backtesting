@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\log_dashboard_server.log"
set "START_BAT=%BAT_DIR%\run_log_dashboard_server.bat"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [%DATE% %TIME%] START run_log_dashboard_restart_keep_url.bat
echo [%DATE% %TIME%] START run_log_dashboard_restart_keep_url.bat>>"%LOG_FILE%"

set "PS_SCRIPT=$ErrorActionPreference='SilentlyContinue';"
set "PS_SCRIPT=!PS_SCRIPT! $killed=0;"
set "PS_SCRIPT=!PS_SCRIPT! $self=$PID;"
set "PS_SCRIPT=!PS_SCRIPT! $targets=Get-CimInstance Win32_Process ^| Where-Object {"
set "PS_SCRIPT=!PS_SCRIPT!   $_.ProcessId -ne $self -and ("
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*run_log_dashboard_server.bat*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*log_dashboard_server.py*')"
set "PS_SCRIPT=!PS_SCRIPT!   )"
set "PS_SCRIPT=!PS_SCRIPT! };"
set "PS_SCRIPT=!PS_SCRIPT! foreach($p in $targets){ try { Stop-Process -Id $p.ProcessId -Force; $killed++ } catch {} };"
set "PS_SCRIPT=!PS_SCRIPT! Write-Output $killed;"

set "KILLED_COUNT="
for /f "delims=" %%a in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "!PS_SCRIPT!" ^| findstr /r "^[0-9][0-9]*$"') do set "KILLED_COUNT=%%a"
if not defined KILLED_COUNT set "KILLED_COUNT=0"

echo [INFO] Dashboard-only restart: killed !KILLED_COUNT! server process(es); keeping cloudflared/public-link alive.
echo [%DATE% %TIME%] Dashboard-only restart killed !KILLED_COUNT! server process(es).>>"%LOG_FILE%"

start "EQIDV2 Log Dashboard" /MIN cmd /c call "%START_BAT%"
timeout /t 2 >nul

echo [%DATE% %TIME%] END run_log_dashboard_restart_keep_url.bat
echo [%DATE% %TIME%] END run_log_dashboard_restart_keep_url.bat>>"%LOG_FILE%"

endlocal & exit /b 0
