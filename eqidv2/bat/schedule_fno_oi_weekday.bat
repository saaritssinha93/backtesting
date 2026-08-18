@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "INSTALLER=%BASE_DIR%\bat\schedule_fno_oi_weekday.ps1"

powershell -NoProfile -ExecutionPolicy Bypass -File "%INSTALLER%"
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
