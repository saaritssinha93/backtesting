@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "OLD_TASK_BACKTEST=EQIDV2_backtesting_result_v7_v8_1600"
set "TASK_BACKTEST=EQIDV2_backtesting_result_v11_1600"
set "BAT_BACKTEST=%BAT_DIR%\run_backtesting_result_v11_1600.bat"

if not exist "%BAT_BACKTEST%" (
  echo [ERROR] Missing bat file: %BAT_BACKTEST%
  endlocal & exit /b 1
)

schtasks /Delete /F /TN "%OLD_TASK_BACKTEST%" >nul 2>&1

echo [INFO] Creating weekday Backtesting Result v11 task at 16:00 ...
schtasks /Create /F /TN "%TASK_BACKTEST%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:00 /TR "%BAT_BACKTEST%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_BACKTEST%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Backtesting Result v11 task created/updated successfully:
echo        %TASK_BACKTEST%  ^(Mon-Fri 16:00^)

endlocal & exit /b 0
