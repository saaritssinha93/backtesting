@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_NAME=EQIDV2_eod_5mins_data_0900"
set "BAT_FILE=%BAT_DIR%\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat"

if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)
if not exist "%BAT_FILE%" (
  echo [ERROR] Missing bat file: %BAT_FILE%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday 5-minute live data fetch task at 09:00 ...
schtasks /Create /F /TN "%TASK_NAME%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_FILE%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NAME%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_NAME%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_NAME%
  endlocal & exit /b 1
)

echo [INFO] Weekday 5-minute live data fetch task created/updated successfully:
echo        %TASK_NAME%   (Mon-Fri 09:00)

endlocal & exit /b 0
