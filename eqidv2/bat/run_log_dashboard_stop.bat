@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\log_dashboard_public_link.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [%DATE% %TIME%] START run_log_dashboard_stop.bat
echo [%DATE% %TIME%] START run_log_dashboard_stop.bat>>"%LOG_FILE%"

set "PS_SCRIPT=$ErrorActionPreference='SilentlyContinue';"
set "PS_SCRIPT=!PS_SCRIPT! $killed=0;"
set "PS_SCRIPT=!PS_SCRIPT! $self=$PID;"
set "PS_SCRIPT=!PS_SCRIPT! $procs=Get-CimInstance Win32_Process ^| Where-Object {"
set "PS_SCRIPT=!PS_SCRIPT!   $_.ProcessId -ne $self -and ("
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*cloudflared*' -and $_.CommandLine -like '*127.0.0.1:8787*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*run_log_dashboard_public_link.bat*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*run_log_dashboard_public_link_capture.ps1*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*run_log_dashboard_server.bat*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*log_dashboard_server.py*')"
set "PS_SCRIPT=!PS_SCRIPT!   )"
set "PS_SCRIPT=!PS_SCRIPT! };"
set "PS_SCRIPT=!PS_SCRIPT! foreach($p in $procs){ try { Stop-Process -Id $p.ProcessId -Force; $killed++ } catch {} };"
set "PS_SCRIPT=!PS_SCRIPT! Write-Output $killed;"

set "KILLED_COUNT="
for /f "delims=" %%a in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "%PS_SCRIPT%" ^| findstr /r "^[0-9][0-9]*$"') do set "KILLED_COUNT=%%a"
if not defined KILLED_COUNT set "KILLED_COUNT=0"
echo [INFO] KILLED=!KILLED_COUNT!
echo [!DATE! !TIME!] KILLED=!KILLED_COUNT!>>"%LOG_FILE%"

echo [%DATE% %TIME%] END run_log_dashboard_stop.bat
echo [%DATE% %TIME%] END run_log_dashboard_stop.bat>>"%LOG_FILE%"

endlocal & exit /b 0
