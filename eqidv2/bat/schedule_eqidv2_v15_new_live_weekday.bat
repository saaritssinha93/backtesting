@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_V15_NEW=EQIDV2_live_combined_csv_v15_new_0900"
set "RUN_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_new_persistent.bat"

if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)
if not exist "%RUN_BAT%" (
  echo [ERROR] Missing bat file: %RUN_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15_NEW persistent live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_V15_NEW%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%RUN_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V15_NEW%
  endlocal & exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V15_NEW%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V15_NEW%
  endlocal & exit /b 1
)

echo [INFO] Task created/updated successfully:
echo        %TASK_V15_NEW%   (Mon-Fri 09:00)

endlocal & exit /b 0
