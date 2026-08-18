@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_NAME=EQIDV2_fundamental_price_action_v1_0915"
set "RUN_BAT=%BAT_DIR%\run_fundamental_price_action_v1_session.bat"

if not exist "%RUN_BAT%" (
  echo [ERROR] Missing runner: %RUN_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday fundamental_price_action_v1 task at 09:15 ...
schtasks /Create /F /TN "%TASK_NAME%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /TR "%RUN_BAT%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_NAME%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Created %TASK_NAME% ^(Mon-Fri 09:15; marker-gated every 5 minutes through 14:30^)
endlocal & exit /b 0
