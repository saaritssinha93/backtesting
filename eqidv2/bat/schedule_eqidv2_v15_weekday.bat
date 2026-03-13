@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"

set "LIVE_SHORT_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_short.bat"
set "LIVE_LONG_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_long.bat"
set "PAPER_BAT=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat"
set "NIFTY_FETCH_BAT=%BAT_DIR%\run_nifty_guard_fetcher_v15.bat"

set "TASK_LIVE_SHORT=EQIDV2_live_combined_csv_v15_short_0900"
set "TASK_LIVE_LONG=EQIDV2_live_combined_csv_v15_long_0900"
set "TASK_PAPER=EQIDV2_avwap_paper_trade_v15_0900"
set "TASK_NIFTY=EQIDV2_nifty_guard_fetch_v15_0910"

if not exist "%LIVE_SHORT_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_SHORT_BAT%
  endlocal & exit /b 1
)
if not exist "%LIVE_LONG_BAT%" (
  echo [ERROR] Missing bat file: %LIVE_LONG_BAT%
  endlocal & exit /b 1
)
if not exist "%PAPER_BAT%" (
  echo [ERROR] Missing bat file: %PAPER_BAT%
  endlocal & exit /b 1
)
if not exist "%NIFTY_FETCH_BAT%" (
  echo [ERROR] Missing bat file: %NIFTY_FETCH_BAT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 short live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE_SHORT%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%LIVE_SHORT_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE_SHORT%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 long live scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE_LONG%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%LIVE_LONG_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LIVE_LONG%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 unified papertrade task at 09:00 ...
schtasks /Create /F /TN "%TASK_PAPER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%PAPER_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_PAPER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 NIFTY guard fetch task at 09:10 ...
schtasks /Create /F /TN "%TASK_NIFTY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:10 /TR "%NIFTY_FETCH_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] Tasks created/updated successfully:
echo        %TASK_LIVE_SHORT%  (Mon-Fri 09:00)
echo        %TASK_LIVE_LONG%   (Mon-Fri 09:00)
echo        %TASK_PAPER%       (Mon-Fri 09:00)
echo        %TASK_NIFTY%       (Mon-Fri 09:10)

endlocal & exit /b 0

