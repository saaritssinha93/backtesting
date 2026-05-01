@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "RUN_BAT=%BAT_DIR%\run_pf_supervisor_healthcheck.bat"
set "TASK_NAME=EQIDV2_pf_supervisor_healthcheck_0935"

if not exist "%RUN_BAT%" (
  echo [ERROR] Missing bat file: %RUN_BAT%
  endlocal & exit /b 1
)
if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday PF supervisor healthcheck task at 09:35 ...
schtasks /Create /F /TN "%TASK_NAME%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:35 /TR "%RUN_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NAME%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_NAME%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_NAME%
  endlocal & exit /b 1
)

echo [INFO] Task created/updated successfully:
echo        %TASK_NAME% (Mon-Fri 09:35)

endlocal & exit /b 0
