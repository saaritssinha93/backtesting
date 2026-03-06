@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"

set "LIVE_SHORT_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v7_sweep_short.bat"
set "LIVE_LONG_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v7_sweep_long.bat"
set "LIVE_EXEC_BAT=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.bat"

set "TASK_LIVE_SHORT=EQIDV2_live_combined_csv_v7_sweep_short_0900"
set "TASK_LIVE_LONG=EQIDV2_live_combined_csv_v7_sweep_long_0900"
set "TASK_LIVE_EXEC=EQIDV2_avwap_live_trade_v7_sweep_0905"

if not exist "%LIVE_SHORT_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_SHORT_BAT%
  endlocal & exit /b 1
)
if not exist "%LIVE_LONG_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_LONG_BAT%
  endlocal & exit /b 1
)
if not exist "%LIVE_EXEC_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_EXEC_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Sweep short live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE_SHORT%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%LIVE_SHORT_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE_SHORT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Sweep long live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE_LONG%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%LIVE_LONG_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE_LONG%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Sweep LIVE trade executor task at 09:05 ...
schtasks /Create /F /TN "%TASK_LIVE_EXEC%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:05 /TR "%LIVE_EXEC_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE_EXEC%
  endlocal & exit /b 1
)

echo [INFO] Tasks created/updated successfully:
echo        %TASK_LIVE_SHORT%  (Mon-Fri 09:00)
echo        %TASK_LIVE_LONG%   (Mon-Fri 09:00)
echo        %TASK_LIVE_EXEC%   (Mon-Fri 09:05)

endlocal & exit /b 0

