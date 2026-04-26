@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"

REM Task names for V16 5min two-stage stack
REM SEE (Signal Early Engine) replaces SE; output files/schema are identical.
set "TASK_SIGNAL_ENGINE=EQIDV2_signal_early_engine_v16_5min_0900"
set "TASK_PENDING_FETCH=EQIDV2_pending_data_fetcher_v16_5min_0900"
set "TASK_DETECTION=EQIDV2_detection_engine_v16_5min_0900"
REM P5 (2026-04-22): EOD reconciler runs at 16:00 IST on weekdays — well after
REM the 15:30 close and the 15:20 forced-flatten window, so all CSV writes and
REM broker fills are settled.
set "TASK_EOD_RECONCILER=EQIDV2_eod_reconciler_v16_5min_1600"
REM Audit #2 (2026-04-22): LF prewarm at 09:13 — auths the 8 Kite app sessions
REM and pre-resolves NSE instrument lists 2 minutes before the LF supervisor
REM wakes at 09:15, killing the "Too many requests" cascade at 09:20.
set "TASK_LF_PREWARM=EQIDV2_lf_prewarm_v16_5min_0913"

set "BAT_SIGNAL_ENGINE=%BAT_DIR%\run_eqidv2_signal_early_engine_v16_5min.bat"
set "BAT_PENDING_FETCH=%BAT_DIR%\run_eqidv2_pending_data_fetcher_v16_5min.bat"
set "BAT_DETECTION=%BAT_DIR%\run_eqidv2_detection_engine_v16_5min.bat"
set "BAT_EOD_RECONCILER=%BAT_DIR%\run_eqidv2_eod_reconciler_v16_5min.bat"
set "BAT_LF_PREWARM=%BAT_DIR%\run_eqidv2_lf_prewarm_v16_5min.bat"

if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)
if not exist "%BAT_SIGNAL_ENGINE%" (
  echo [ERROR] Missing bat file: %BAT_SIGNAL_ENGINE%
  endlocal & exit /b 1
)
if not exist "%BAT_PENDING_FETCH%" (
  echo [ERROR] Missing bat file: %BAT_PENDING_FETCH%
  endlocal & exit /b 1
)
if not exist "%BAT_DETECTION%" (
  echo [ERROR] Missing bat file: %BAT_DETECTION%
  endlocal & exit /b 1
)
if not exist "%BAT_EOD_RECONCILER%" (
  echo [ERROR] Missing bat file: %BAT_EOD_RECONCILER%
  endlocal & exit /b 1
)
if not exist "%BAT_LF_PREWARM%" (
  echo [ERROR] Missing bat file: %BAT_LF_PREWARM%
  endlocal & exit /b 1
)

REM M5 (2026-04-22): start times staggered so the three supervisors don't contend
REM on Kite-app pool init, log-dir mkdir, or status-file opens at the same ms.
REM Order matches the pipeline flow: SEE first (compute only), then PF (Kite),
REM then DE (needs SEE+PF to have completed at least one slot).
echo [INFO] Creating weekday LF prewarm task at 09:13 ...
schtasks /Create /F /TN "%TASK_LF_PREWARM%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:13 /TR "%BAT_LF_PREWARM%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_LF_PREWARM%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_LF_PREWARM%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_LF_PREWARM%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday two-stage Signal Early Engine (SEE) task at 09:00 ...
schtasks /Create /F /TN "%TASK_SIGNAL_ENGINE%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_SIGNAL_ENGINE%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_SIGNAL_ENGINE%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_SIGNAL_ENGINE%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_SIGNAL_ENGINE%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday two-stage Pending Data Fetcher task at 09:01 ...
schtasks /Create /F /TN "%TASK_PENDING_FETCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:01 /TR "%BAT_PENDING_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_PENDING_FETCH%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_PENDING_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_PENDING_FETCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday two-stage Detection Engine task at 09:02 ...
schtasks /Create /F /TN "%TASK_DETECTION%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:02 /TR "%BAT_DETECTION%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_DETECTION%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_DETECTION%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_DETECTION%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday EOD reconciler task at 16:00 ...
schtasks /Create /F /TN "%TASK_EOD_RECONCILER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:00 /TR "%BAT_EOD_RECONCILER%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_EOD_RECONCILER%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_EOD_RECONCILER%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_EOD_RECONCILER%
  endlocal & exit /b 1
)

echo [INFO] All V16_5MIN two-stage tasks created/updated successfully:
echo        %TASK_LF_PREWARM%      (Mon-Fri 09:13)
echo        %TASK_SIGNAL_ENGINE%   (Mon-Fri 09:00)
echo        %TASK_PENDING_FETCH%   (Mon-Fri 09:01)
echo        %TASK_DETECTION%       (Mon-Fri 09:02)
echo        %TASK_EOD_RECONCILER%  (Mon-Fri 16:00)
echo.
echo [NOTE] These tasks are intended to replace the single-stage V16 scanner.
echo        Disable \EQIDV2_live_combined_csv_v16_5min_0900 when using the two-stage stack.
echo        The Detection Engine writes to the same signals CSV, so executor sessions need no changes.

endlocal & exit /b 0
