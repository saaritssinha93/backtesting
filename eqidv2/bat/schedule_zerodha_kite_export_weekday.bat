@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "START_BAT=%BAT_DIR%\run_zerodha_kite_export_scheduler.bat"
set "STOP_BAT=%BAT_DIR%\run_zerodha_kite_export_stop.bat"

set "TASK_START=EQIDV2_kite_export_start_0915"
set "TASK_STOP=EQIDV2_kite_export_stop_1535"

set "TR_START=cmd /c \"call %START_BAT%\""
set "TR_STOP=cmd /c \"call %STOP_BAT%\""

echo [INFO] Creating weekday Kite export START task at 09:15 ...
schtasks /Create /F /TN "%TASK_START%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /TR "%TR_START%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_START%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday Kite export STOP task at 15:35 ...
schtasks /Create /F /TN "%TASK_STOP%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 15:35 /TR "%TR_STOP%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_STOP%
  endlocal & exit /b 1
)

echo [INFO] Tasks created/updated successfully:
echo        %TASK_START%  (Mon-Fri 09:15)
echo        %TASK_STOP%   (Mon-Fri 15:35)

endlocal & exit /b 0
