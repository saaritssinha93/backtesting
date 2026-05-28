@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_SUGGESTIONS=EQIDV2_suggestions_v7_live_research_1615"
set "BAT_SUGGESTIONS=%BAT_DIR%\run_eqidv2_suggestions_v7_live_research_1605.bat"

if not exist "%BAT_SUGGESTIONS%" (
  echo [ERROR] Missing bat file: %BAT_SUGGESTIONS%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday Suggestions v7 live research task at 16:15 ...
schtasks /Create /F /TN "%TASK_SUGGESTIONS%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:15 /TR "%BAT_SUGGESTIONS%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_SUGGESTIONS%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Suggestions v7 live research task created/updated successfully:
echo        %TASK_SUGGESTIONS%  ^(Mon-Fri 16:15^)

endlocal & exit /b 0
