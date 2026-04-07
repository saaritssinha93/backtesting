@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"

REM Task names for V16 5min stack
set "TASK_V16_5MIN_SCANNER=EQIDV2_live_combined_csv_v16_5min_0900"
set "TASK_V16_5MIN_FETCH=EQIDV2_eod_5mins_data_0900"
set "TASK_V16_5MIN_PAPER=EQIDV2_paper_trade_v16_5min_0900"
set "TASK_V16_5MIN_LIVE=EQIDV2_live_trade_v16_5min_0900"
set "TASK_V16_5MIN_NIFTY=EQIDV2_nifty_guard_fetch_v16_5min_0915"

set "BAT_SCANNER=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v16_5min.bat"
set "BAT_FETCH=%BAT_DIR%\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat"
set "BAT_PAPER=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.bat"
set "BAT_LIVE=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat"
set "BAT_NIFTY=%BAT_DIR%\run_nifty_guard_fetcher_v16_5min.bat"

if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)
if not exist "%BAT_SCANNER%" (
  echo [ERROR] Missing bat file: %BAT_SCANNER%
  endlocal & exit /b 1
)
if not exist "%BAT_FETCH%" (
  echo [ERROR] Missing bat file: %BAT_FETCH%
  endlocal & exit /b 1
)
if not exist "%BAT_PAPER%" (
  echo [ERROR] Missing bat file: %BAT_PAPER%
  endlocal & exit /b 1
)
if not exist "%BAT_LIVE%" (
  echo [ERROR] Missing bat file: %BAT_LIVE%
  endlocal & exit /b 1
)
if not exist "%BAT_NIFTY%" (
  echo [ERROR] Missing bat file: %BAT_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16_5MIN scanner task at 09:00 ...
schtasks /Create /F /TN "%TASK_V16_5MIN_SCANNER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_SCANNER%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V16_5MIN_SCANNER%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V16_5MIN_SCANNER%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V16_5MIN_SCANNER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16_5MIN 5-minute live data fetch task at 09:00 ...
schtasks /Create /F /TN "%TASK_V16_5MIN_FETCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V16_5MIN_FETCH%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V16_5MIN_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V16_5MIN_FETCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16_5MIN paper trade task at 09:00 ...
schtasks /Create /F /TN "%TASK_V16_5MIN_PAPER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_PAPER%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V16_5MIN_PAPER%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V16_5MIN_PAPER%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V16_5MIN_PAPER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16_5MIN live trade task at 09:00 ...
schtasks /Create /F /TN "%TASK_V16_5MIN_LIVE%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_LIVE%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V16_5MIN_LIVE%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V16_5MIN_LIVE%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V16_5MIN_LIVE%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V16_5MIN NIFTY guard fetch task at 09:15 ...
schtasks /Create /F /TN "%TASK_V16_5MIN_NIFTY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /TR "%BAT_NIFTY%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_V16_5MIN_NIFTY%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_V16_5MIN_NIFTY%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_V16_5MIN_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] All V16_5MIN tasks created/updated successfully:
echo        %TASK_V16_5MIN_SCANNER%   (Mon-Fri 09:00)
echo        %TASK_V16_5MIN_FETCH%     (Mon-Fri 09:00)
echo        %TASK_V16_5MIN_PAPER%     (Mon-Fri 09:00)
echo        %TASK_V16_5MIN_LIVE%      (Mon-Fri 09:00)
echo        %TASK_V16_5MIN_NIFTY%     (Mon-Fri 09:15)
echo.
echo [NOTE] DO NOT RUN THIS FILE until you are ready to go live.
echo        Tasks are created but Windows Task Scheduler only runs them on their trigger time.

endlocal & exit /b 0
