@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\zerodha_kite_export_scheduler.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "PS_SCRIPT=$ErrorActionPreference='SilentlyContinue';"
set "PS_SCRIPT=!PS_SCRIPT! $targets = Get-CimInstance Win32_Process ^| Where-Object {"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*zerodha_kite_export.py*--live*') -or"
set "PS_SCRIPT=!PS_SCRIPT!     ($_.CommandLine -like '*run_zerodha_kite_export_scheduler.bat*')"
set "PS_SCRIPT=!PS_SCRIPT! };"
set "PS_SCRIPT=!PS_SCRIPT! if ($targets) { $targets ^| ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; Write-Output ('Stopped PID=' + $_.ProcessId) } } else { Write-Output 'No matching process found.' }"

for /f "delims=" %%L in ('powershell -NoProfile -Command "!PS_SCRIPT!"') do (
  echo [%DATE% %TIME%] %%L
  echo [%DATE% %TIME%] %%L>>"%LOG_FILE%"
)

endlocal & exit /b 0
