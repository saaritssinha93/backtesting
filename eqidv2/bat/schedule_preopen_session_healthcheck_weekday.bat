@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "RUN_BAT=%BAT_DIR%\run_preopen_session_healthcheck.bat"
set "TASK_NAME=EQIDV2_preopen_session_healthcheck_0905"

if not exist "%RUN_BAT%" (
  echo [ERROR] Missing bat file: %RUN_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday preopen healthcheck task at 09:05 ...
schtasks /Create /F /TN "%TASK_NAME%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:05 /TR "%RUN_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NAME%
  endlocal & exit /b 1
)

echo [INFO] Task created/updated successfully:
echo        %TASK_NAME% (Mon-Fri 09:05)

endlocal & exit /b 0
