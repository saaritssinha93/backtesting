@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_COST=EQIDV2_v7_nse_id_cost_1605"
set "BAT_COST=%BAT_DIR%\run_v7_nse_id_cost_report.bat"

if not exist "%BAT_COST%" (
  echo [ERROR] Missing bat file: %BAT_COST%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 NSE ID Cost task at 16:05 ...
schtasks /Create /F /TN "%TASK_COST%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:05 /TR "%BAT_COST%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_COST%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 NSE ID Cost task created/updated successfully:
echo        %TASK_COST%  ^(Mon-Fri 16:05^)

endlocal & exit /b 0
