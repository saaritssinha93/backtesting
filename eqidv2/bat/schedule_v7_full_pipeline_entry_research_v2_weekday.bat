@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_V2=EQIDV2_v7_full_pipeline_entry_research_v2_1630"
set "BAT_V2=%BAT_DIR%\run_v7_full_pipeline_entry_research_v2.bat"

if not exist "%BAT_V2%" (
  echo [ERROR] Missing bat file: %BAT_V2%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday Full-Pipeline Entry Research v2 task at 16:30 ...
schtasks /Create /F /TN "%TASK_V2%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:30 /TR "%BAT_V2%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V2%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Full-Pipeline Entry Research v2 task created/updated successfully:
echo        %TASK_V2%  ^(Mon-Fri 16:30^)

endlocal & exit /b 0
