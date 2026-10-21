@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "START_BAT=%BAT_DIR%\run_log_dashboard_public_link.bat"
set "START_SCHEDULED_BAT=%BAT_DIR%\run_log_dashboard_public_link_scheduled.bat"
set "STOP_BAT=%BAT_DIR%\run_log_dashboard_stop.bat"

set "TASK_START=EQIDV2_log_dashboard_start_0855"
set "TASK_STOP=EQIDV2_log_dashboard_stop_0000"
set "OLD_TASK_STOP=EQIDV2_log_dashboard_stop_1700"

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [WARN] LOG_DASH_PASS is not set in this shell.
  echo [WARN] Ensure the scheduled task has LOG_DASH_PASS configured at runtime.
)
if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)

set "TR_START=%START_SCHEDULED_BAT%"
set "TR_STOP=cmd /c \"call %STOP_BAT%\""

echo [INFO] Creating weekday dashboard START task at 08:55 ...
schtasks /Create /F /TN "%TASK_START%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 08:55 /TR "%TR_START%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_START%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_START%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_START%
  endlocal & exit /b 1
)

echo [INFO] Creating dashboard STOP task at 00:00 daily ...
schtasks /Create /F /TN "%TASK_STOP%" /SC DAILY /ST 00:00 /TR "%TR_STOP%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_STOP%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_STOP%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_STOP%
  endlocal & exit /b 1
)

rem Remove the old 17:00 stop only after the midnight task is ready.
schtasks /Query /TN "%OLD_TASK_STOP%" >nul 2>&1
if not errorlevel 1 schtasks /Delete /F /TN "%OLD_TASK_STOP%"

echo [INFO] Tasks created/updated successfully:
echo        %TASK_START%  (Mon-Fri 08:55)
echo        %TASK_STOP%   (daily 00:00)

endlocal & exit /b 0
