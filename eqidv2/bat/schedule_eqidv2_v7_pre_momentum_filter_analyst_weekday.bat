@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_ANALYST=EQIDV2_v7_pre_momentum_filter_analyst_0917"
set "BAT_ANALYST=%BAT_DIR%\run_eqidv2_v7_pre_momentum_filter_analyst.bat"

if not exist "%BAT_ANALYST%" (
  echo [ERROR] Missing bat file: %BAT_ANALYST%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday v7 pre momentum filter analyst task at 09:17 ...
schtasks /Create /F /TN "%TASK_ANALYST%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:17 /TR "%BAT_ANALYST%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_ANALYST%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] v7 pre momentum filter analyst task created/updated successfully:
echo        %TASK_ANALYST%  ^(Mon-Fri 09:17, shadow-only loop every 5m until 15:37, advisory suggestions every 15m^)

endlocal & exit /b 0
