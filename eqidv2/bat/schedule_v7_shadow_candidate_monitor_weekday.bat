@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_SHADOW=EQIDV2_v7_shadow_candidate_monitor_1645"
set "BAT_SHADOW=%BAT_DIR%\run_v7_shadow_candidate_monitor.bat"

if not exist "%BAT_SHADOW%" (
  echo [ERROR] Missing bat file: %BAT_SHADOW%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Shadow Candidate Monitor task at 16:45 ...
schtasks /Create /F /TN "%TASK_SHADOW%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:45 /TR "%BAT_SHADOW%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_SHADOW%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 Shadow Candidate Monitor task created/updated successfully:
echo        %TASK_SHADOW%  ^(Mon-Fri 16:45^)

endlocal & exit /b 0
