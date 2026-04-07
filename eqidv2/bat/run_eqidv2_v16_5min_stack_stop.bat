@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PS1=%BASE_DIR%\bat\stop_eqidv2_v16_5min_stack.ps1"

if not exist "%PS1%" (
  echo [ERROR] Missing PowerShell helper: %PS1%
  endlocal & exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
