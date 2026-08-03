@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_RESEARCH=EQIDV2_v7_full_pipeline_entry_research_1620"
set "BAT_RESEARCH=%BAT_DIR%\run_v7_full_pipeline_entry_research.bat"

if not exist "%BAT_RESEARCH%" (
  echo [ERROR] Missing bat file: %BAT_RESEARCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Full Pipeline Entry Research task at 16:20 ...
schtasks /Create /F /TN "%TASK_RESEARCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:20 /TR "%BAT_RESEARCH%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_RESEARCH%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 Full Pipeline Entry Research task created/updated successfully:
echo        %TASK_RESEARCH%  ^(Mon-Fri 16:20^)

endlocal & exit /b 0
