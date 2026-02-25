@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"

set "LIVE_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v3.bat"
set "PAPER_BAT=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_v3.bat"

set "TASK_LIVE=EQIDV2_live_combined_csv_v3_0900"
set "TASK_PAPER=EQIDV2_avwap_paper_trade_v3_0900"

if not exist "%LIVE_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_BAT%
  endlocal & exit /b 1
)
if not exist "%PAPER_BAT%" (
  echo [ERROR] Missing bat file: %PAPER_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V3 live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%LIVE_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V3 papertrade task at 09:00 ...
schtasks /Create /F /TN "%TASK_PAPER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%PAPER_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_PAPER%
  endlocal & exit /b 1
)

echo [INFO] Tasks created/updated successfully:
echo        %TASK_LIVE%  (Mon-Fri 09:00)
echo        %TASK_PAPER% (Mon-Fri 09:00)

endlocal & exit /b 0
