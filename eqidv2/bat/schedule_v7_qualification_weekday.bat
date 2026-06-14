@echo off
setlocal EnableExtensions

REM Registers the weekday V7 Qualification task at 16:30 (after the day's paper
REM trade file is complete). Scores the mirror-config paper run for live-enable.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_QUAL=EQIDV2_v7_qualification_1630"
set "BAT_QUAL=%BAT_DIR%\run_v7_qualification_report.bat"

if not exist "%BAT_QUAL%" (
  echo [ERROR] Missing bat file: %BAT_QUAL%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Qualification task at 16:30 ...
schtasks /Create /F /TN "%TASK_QUAL%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:30 /TR "%BAT_QUAL%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_QUAL%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 Qualification task created/updated successfully:
echo        %TASK_QUAL%  ^(Mon-Fri 16:30^)

endlocal & exit /b 0
