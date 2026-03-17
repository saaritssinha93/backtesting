@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"

set "LEGACY_TASK_LIVE_LONG=EQIDV2_live_combined_csv_v15_long_0900"
set "LIVE_LONG_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_long.bat"
set "PAPER_BAT=%BAT_DIR%\run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat"
set "NIFTY_FETCH_BAT=%BAT_DIR%\run_nifty_guard_fetcher_v15.bat"

set "LEGACY_TASK_LIVE_SHORT=EQIDV2_live_combined_csv_v15_short_0900"
set "TASK_PAPER=EQIDV2_avwap_paper_trade_v15_0900"
set "TASK_NIFTY=EQIDV2_nifty_guard_fetch_v15_0915"

for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  if not exist "%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_short_shard_%%S.bat" (
    echo [ERROR] Missing bat file: %BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_short_shard_%%S.bat
    endlocal & exit /b 1
  )
)
for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  if not exist "%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_long_shard_%%S.bat" (
    echo [ERROR] Missing bat file: %BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_long_shard_%%S.bat
    endlocal & exit /b 1
  )
)
if not exist "%PAPER_BAT%" (
  echo [ERROR] Missing bat file: %PAPER_BAT%
  endlocal & exit /b 1
)
if not exist "%NIFTY_FETCH_BAT%" (
  echo [ERROR] Missing bat file: %NIFTY_FETCH_BAT%
  endlocal & exit /b 1
)

echo [INFO] Removing legacy single V15 short live scanner task if present ...
schtasks /Delete /F /TN "%LEGACY_TASK_LIVE_SHORT%" >nul 2>&1

for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  set "TASK_LIVE_SHORT=EQIDV2_live_combined_csv_v15_short_s%%S_0900"
  set "LIVE_SHORT_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_short_shard_%%S.bat"
  echo [INFO] Creating weekday V15 short live scanner shard %%S task at 09:00 ...
  schtasks /Create /F /TN "!TASK_LIVE_SHORT!" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "!LIVE_SHORT_BAT!"
  if errorlevel 1 (
    echo [ERROR] Failed to create !TASK_LIVE_SHORT!
    endlocal & exit /b 1
  )
)

echo [INFO] Removing legacy single V15 long live scanner task if present ...
schtasks /Delete /F /TN "%LEGACY_TASK_LIVE_LONG%" >nul 2>&1

for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  set "TASK_LIVE_LONG=EQIDV2_live_combined_csv_v15_long_s%%S_0900"
  set "LIVE_LONG_BAT=%BAT_DIR%\run_eqidv2_live_combined_analyser_csv_v15_long_shard_%%S.bat"
  echo [INFO] Creating weekday V15 long live scanner shard %%S task at 09:00 ...
  schtasks /Create /F /TN "!TASK_LIVE_LONG!" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "!LIVE_LONG_BAT!"
  if errorlevel 1 (
    echo [ERROR] Failed to create !TASK_LIVE_LONG!
    endlocal & exit /b 1
  )
)

echo [INFO] Creating weekday V15 unified papertrade task at 09:00 ...
schtasks /Create /F /TN "%TASK_PAPER%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /TR "%PAPER_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_PAPER%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V15 NIFTY guard fetch task at 09:15 ...
schtasks /Create /F /TN "%TASK_NIFTY%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /TR "%NIFTY_FETCH_BAT%"
if errorlevel 1 (
  echo [ERROR] Failed to create %TASK_NIFTY%
  endlocal & exit /b 1
)

echo [INFO] Tasks created/updated successfully:
for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  echo        EQIDV2_live_combined_csv_v15_short_s%%S_0900  (Mon-Fri 09:00)
)
for %%S in (01 02 03 04 05 06 07 08 09 10) do (
  echo        EQIDV2_live_combined_csv_v15_long_s%%S_0900   (Mon-Fri 09:00)
)
echo        %TASK_PAPER%       (Mon-Fri 09:00)
echo        %TASK_NIFTY%       (Mon-Fri 09:15)

endlocal & exit /b 0

