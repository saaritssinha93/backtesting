@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"

REM Task names for V16 5min two-stage stack
set "TASK_SIGNAL_ENGINE=EQIDV2_signal_engine_v16_5min_0900"
set "TASK_PENDING_FETCH=EQIDV2_pending_data_fetcher_v16_5min_0900"
set "TASK_DETECTION=EQIDV2_detection_engine_v16_5min_0900"

set "BAT_SIGNAL_ENGINE=%BAT_DIR%\run_eqidv2_signal_engine_v16_5min.bat"
set "BAT_PENDING_FETCH=%BAT_DIR%\run_eqidv2_pending_data_fetcher_v16_5min.bat"
set "BAT_DETECTION=%BAT_DIR%\run_eqidv2_detection_engine_v16_5min.bat"

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

echo [INFO] Creating weekday two-stage Signal Engine task at 09:00 ...
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

echo [INFO] Creating weekday two-stage Pending Data Fetcher task at 09:00 ...
schtasks /Create /F /TN "%TASK_PENDING_FETCH%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_PENDING_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_PENDING_FETCH%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_PENDING_FETCH%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_PENDING_FETCH%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday two-stage Detection Engine task at 09:00 ...
schtasks /Create /F /TN "%TASK_DETECTION%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_DETECTION%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_DETECTION%
  endlocal & exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_DETECTION%"
if errorlevel 1 (
  echo [ERROR] Failed to harden %TASK_DETECTION%
  endlocal & exit /b 1
)

echo [INFO] All V16_5MIN two-stage tasks created/updated successfully:
echo        %TASK_SIGNAL_ENGINE%   (Mon-Fri 09:00)
echo        %TASK_PENDING_FETCH%   (Mon-Fri 09:00)
echo        %TASK_DETECTION%       (Mon-Fri 09:00)
echo.
echo [NOTE] These tasks are intended to replace the single-stage V16 scanner.
echo        Disable \EQIDV2_live_combined_csv_v16_5min_0900 when using the two-stage stack.
echo        The Detection Engine writes to the same signals CSV, so executor sessions need no changes.

endlocal & exit /b 0
