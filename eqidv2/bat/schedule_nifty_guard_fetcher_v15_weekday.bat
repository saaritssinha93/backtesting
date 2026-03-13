@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "FETCH_BAT=%BAT_DIR%\run_nifty_guard_fetcher_v15.bat"
set "TASK_NIFTY=EQIDV2_nifty_guard_fetch_v15_0910"

if not exist "%FETCH_BAT%" (
  echo [ERROR] Missing bat file: %FETCH_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 NIFTY guard fetch task at 09:10 ...
schtasks /Create /F /TN "%TASK_NIFTY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:10 /TR "%FETCH_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] Task created/updated successfully:
echo        %TASK_NIFTY%  (Mon-Fri 09:10)

endlocal & exit /b 0
