@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_DAILY_RESEARCH=EQIDV2_daily_live_v7_research_0917"
set "BAT_DAILY_RESEARCH=%BAT_DIR%\run_eqidv2_daily_live_v7_research_session.bat"

if not exist "%BAT_DAILY_RESEARCH%" (
  echo [ERROR] Missing bat file: %BAT_DAILY_RESEARCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday Daily Live V7 Research task at 09:17 ...
schtasks /Create /F /TN "%TASK_DAILY_RESEARCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:17 /TR "%BAT_DAILY_RESEARCH%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_DAILY_RESEARCH%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] Daily Live V7 Research task created/updated successfully:
echo        %TASK_DAILY_RESEARCH%  ^(Mon-Fri 09:17, script waits until 09:17:30 and runs every 15m until 16:00^)

endlocal & exit /b 0
