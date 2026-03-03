@echo off
REM schedule_eqidv2_parallel_runner_v5_weekday.bat
REM ================================================
REM Registers Windows Task Scheduler tasks for the V5 parallel runner.
REM
REM Tasks created:
REM   EQIDV2_parallel_runner_v5_0900   Mon-Fri 09:00
REM       -> run_eqidv2_parallel_runner_v5.bat
REM          (covers: eod_15min_data + live_combined_v5_short + live_combined_v5_long)
REM
REM   EQIDV2_avwap_paper_trade_v5_0900  Mon-Fri 09:00
REM       -> run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat
REM          (paper trade executor -- SAFE default)
REM
REM HOW TO SWITCH TO LIVE TRADE:
REM   Change EXEC_BAT and TASK_EXEC below to:
REM     EXEC_BAT  = run_avwap_trade_execution_PAPER_TRADE_FALSE_v5.bat
REM     TASK_EXEC = EQIDV2_avwap_live_trade_v5_0905   /ST 09:05
REM
REM AFTER RUNNING THIS SCRIPT:
REM   The following OLD tasks (if they exist) can be disabled since the
REM   parallel runner now covers all three:
REM     EQIDV2_live_combined_csv_v5_short_0900
REM     EQIDV2_live_combined_csv_v5_long_0900
REM     EQIDV2_eod_15min_data_* (any separate 15m scheduler task)
REM
REM   To disable them run:
REM     schtasks /Change /TN "EQIDV2_live_combined_csv_v5_short_0900" /DISABLE
REM     schtasks /Change /TN "EQIDV2_live_combined_csv_v5_long_0900" /DISABLE
REM
REM   (Disabling is safer than deleting -- you can re-enable if needed.)
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"

REM ---- Parallel runner (all three processes) ----
set "PARALLEL_BAT=%BAT_DIR%\run_eqidv2_parallel_runner_v5.bat"
set "TASK_PARALLEL=EQIDV2_parallel_runner_v5_0900"
set "PARALLEL_START=09:00"

REM ---- Trade executor (paper trade by default -- edit to switch to live) ----
set "EXEC_BAT=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat"
set "TASK_EXEC=EQIDV2_avwap_paper_trade_v5_0900"
set "EXEC_START=09:00"

REM ---- Validate files exist before touching Task Scheduler ----
if not exist "%PARALLEL_BAT%" (
  echo [ERROR] Missing bat file: %PARALLEL_BAT%
  echo         Run this from the correct BASE_DIR or check the path.
  endlocal & exit /b 1
)
if not exist "%EXEC_BAT%" (
  echo [ERROR] Missing bat file: %EXEC_BAT%
  echo         Check that run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat exists.
  endlocal & exit /b 1
)

echo.
echo [INFO] Creating weekday V5 PARALLEL RUNNER task ...
echo        Task : %TASK_PARALLEL%
echo        Time : Mon-Fri %PARALLEL_START%
echo        Bat  : %PARALLEL_BAT%
echo        (covers: eod_15min_data + live_combined_v5_short + live_combined_v5_long)
echo.
schtasks /Create /F /TN "%TASK_PARALLEL%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST %PARALLEL_START% /TR "%PARALLEL_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create task: %TASK_PARALLEL%
  endlocal & exit /b 1
)
echo [OK] %TASK_PARALLEL% created.
echo.

echo [INFO] Creating weekday V5 TRADE EXECUTOR task ...
echo        Task : %TASK_EXEC%
echo        Time : Mon-Fri %EXEC_START%
echo        Bat  : %EXEC_BAT%
echo.
schtasks /Create /F /TN "%TASK_EXEC%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST %EXEC_START% /TR "%EXEC_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create task: %TASK_EXEC%
  endlocal & exit /b 1
)
echo [OK] %TASK_EXEC% created.
echo.

echo ================================================
echo Tasks created/updated successfully:
echo   %TASK_PARALLEL%   (Mon-Fri %PARALLEL_START%)
echo   %TASK_EXEC%  (Mon-Fri %EXEC_START%)
echo ================================================
echo.
echo NEXT STEP -- disable old individual tasks if they exist:
echo   schtasks /Change /TN "EQIDV2_live_combined_csv_v5_short_0900" /DISABLE
echo   schtasks /Change /TN "EQIDV2_live_combined_csv_v5_long_0900" /DISABLE
echo.
echo To verify all eqidv2 tasks:
echo   schtasks /Query /FO LIST /V | findstr /i "EQIDV2"
echo.

endlocal & exit /b 0
