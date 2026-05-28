@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_RESEARCH=EQIDV2_v7_research_layer_0917"
set "BAT_RESEARCH=%BAT_DIR%\run_eqidv2_v7_research_layer.bat"

if not exist "%BAT_RESEARCH%" (
  echo [ERROR] Missing bat file: %BAT_RESEARCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 research layer task at 09:17 ...
schtasks /Create /F /TN "%TASK_RESEARCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:17 /TR "%BAT_RESEARCH%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_RESEARCH%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 research layer task created/updated successfully:
echo        %TASK_RESEARCH%  ^(Mon-Fri 09:17, script waits until 09:17:30 and runs every 15m until 16:00^)

endlocal & exit /b 0
