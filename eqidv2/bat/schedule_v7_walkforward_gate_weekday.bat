@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_WALKFORWARD=EQIDV2_v7_walkforward_gate_1620"
set "BAT_WALKFORWARD=%BAT_DIR%\run_v7_walkforward_gate_report.bat"

if not exist "%BAT_WALKFORWARD%" (
  echo [ERROR] Missing bat file: %BAT_WALKFORWARD%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Walkforward Gate task at 16:20 ...
schtasks /Create /F /TN "%TASK_WALKFORWARD%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:20 /TR "%BAT_WALKFORWARD%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_WALKFORWARD%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 Walkforward Gate task created/updated successfully:
echo        %TASK_WALKFORWARD%  ^(Mon-Fri 16:20^)

endlocal & exit /b 0
