@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "FETCH_BAT=%BAT_DIR%\run_nifty_guard_fetcher_v16_5min.bat"
set "TASK_NIFTY=EQIDV2_nifty_guard_fetch_v16_5min_0915"

if not exist "%FETCH_BAT%" (
  echo [ERROR] Missing bat file: %FETCH_BAT%
  endlocal & exit /b 1
)
if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16 5min NIFTY guard fetch task at 09:15 ...
schtasks /Create /F /TN "%TASK_NIFTY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /TR "%FETCH_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NIFTY%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_NIFTY%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] Task created/updated successfully:
echo        %TASK_NIFTY%  (Mon-Fri 09:15)

endlocal & exit /b 0
