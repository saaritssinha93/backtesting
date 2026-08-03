@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_SHADOW=EQIDV2_v11_lab_shadow_monitor_1655"
set "BAT_SHADOW=%BAT_DIR%\run_v11_lab_shadow_monitor.bat"

if not exist "%BAT_SHADOW%" (
  echo [ERROR] Missing bat file: %BAT_SHADOW%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V11 Lab Shadow Monitor task at 16:55 ...
schtasks /Create /F /TN "%TASK_SHADOW%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:55 /TR "%BAT_SHADOW%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_SHADOW%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V11 Lab Shadow Monitor task created/updated successfully:
echo        %TASK_SHADOW%  ^(Mon-Fri 16:55, shadow-only^)

endlocal & exit /b 0
