@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_MOVING_FILES=EQIDV2_data_for_backtesting_1545"
set "BAT_MOVING_FILES=%BAT_DIR%\run_moving_files_1545.bat"

if not exist "%BAT_MOVING_FILES%" (
  echo [ERROR] Missing bat file: %BAT_MOVING_FILES%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday Data for backtesting task at 15:45 ...
schtasks /Create /F /TN "%TASK_MOVING_FILES%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 15:45 /TR "%BAT_MOVING_FILES%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_MOVING_FILES%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Data for backtesting task created/updated successfully:
echo        %TASK_MOVING_FILES%  ^(Mon-Fri 15:45^)

endlocal & exit /b 0
