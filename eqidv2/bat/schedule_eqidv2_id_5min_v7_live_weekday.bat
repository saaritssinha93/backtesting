@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"

set "TASK_SIGNAL_DISCOVERY=EQIDV2_signal_discovery_v7_5mins_ID"
set "TASK_ENTRY_ENGINE=EQIDV2_entry_engine_1min_v5_ID"
set "TASK_PAPER=EQIDV2_paper_trade_id_5min_v7_0900"
set "TASK_LIVE=EQIDV2_live_trade_id_5min_v7_0900"

set "BAT_SIGNAL_DISCOVERY=%BAT_DIR%\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat"
set "BAT_ENTRY_ENGINE=%BAT_DIR%\run_eqidv2_entry_engine_1min_v5_id.bat"
set "BAT_PAPER=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat"
set "BAT_LIVE=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat"

if not exist "%TASK_HARDENER%" (
  echo [ERROR] Missing PowerShell helper: %TASK_HARDENER%
  endlocal & exit /b 1
)
for %%F in ("%BAT_SIGNAL_DISCOVERY%" "%BAT_ENTRY_ENGINE%" "%BAT_PAPER%" "%BAT_LIVE%") do (
  if not exist "%%~F" (
    echo [ERROR] Missing bat file: %%~F
    endlocal & exit /b 1
  )
)

echo [INFO] Creating weekday V7 ID signal discovery task at 09:00 ...
schtasks /Create /F /TN "%TASK_SIGNAL_DISCOVERY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_SIGNAL_DISCOVERY%"
if errorlevel 1 endlocal & exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_SIGNAL_DISCOVERY%"
if errorlevel 1 endlocal & exit /b 1

echo [INFO] Creating weekday V7 ID 1-min entry engine task at 09:00 ...
schtasks /Create /F /TN "%TASK_ENTRY_ENGINE%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_ENTRY_ENGINE%"
if errorlevel 1 endlocal & exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_ENTRY_ENGINE%"
if errorlevel 1 endlocal & exit /b 1

echo [INFO] Creating weekday V7 ID papertrade task at 09:00 ...
schtasks /Create /F /TN "%TASK_PAPER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_PAPER%"
if errorlevel 1 endlocal & exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_PAPER%"
if errorlevel 1 endlocal & exit /b 1

echo [INFO] Creating weekday V7 ID live Kite trade task at 09:00 ...
schtasks /Create /F /TN "%TASK_LIVE%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%BAT_LIVE%"
if errorlevel 1 endlocal & exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_LIVE%"
if errorlevel 1 endlocal & exit /b 1

echo [INFO] V7 ID tasks created/updated successfully:
echo        %TASK_SIGNAL_DISCOVERY%  ^(Mon-Fri 09:00^)
echo        %TASK_ENTRY_ENGINE%      ^(Mon-Fri 09:00^)
echo        %TASK_PAPER%             ^(Mon-Fri 09:00^)
echo        %TASK_LIVE%              ^(Mon-Fri 09:00^)

endlocal & exit /b 0
