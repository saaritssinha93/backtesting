@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_NAME=EQIDV2_fundamental_price_action_v1_papertrade_0920"
set "RUN_BAT=%BAT_DIR%\run_fundamental_price_action_v1_papertrade.bat"

if not exist "%RUN_BAT%" (
  echo [ERROR] Missing runner: %RUN_BAT%
  endlocal & exit /b 1
)

rem 09:20 starts five minutes after the FPA session, so the entry sheets for
rem the day exist before the first paper-trade cycle reads them.
echo [INFO] Creating weekday fundamental_price_action_v1 papertrade task at 09:20 ...
schtasks /Create /F /TN "%TASK_NAME%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:20 /TR "%RUN_BAT%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_NAME%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Created %TASK_NAME% ^(Mon-Fri 09:20; 1%% stop / 1%% target, square-off 15:15^)
endlocal & exit /b 0
