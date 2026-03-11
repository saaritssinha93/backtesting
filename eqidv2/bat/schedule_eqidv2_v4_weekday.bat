@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "TASK_LIVE_SHORT=EQIDV2_live_combined_csv_v4_short_0900"
set "TASK_LIVE_LONG=EQIDV2_live_combined_csv_v4_long_0900"
set "TASK_PAPER=EQIDV2_avwap_paper_trade_v4_0900"

echo [INFO] V4 scheduling is disabled.
echo [INFO] Disabling existing V4 scheduled tasks if they are present...
for %%T in ("%TASK_LIVE_SHORT%" "%TASK_LIVE_LONG%" "%TASK_PAPER%") do (
  schtasks /Change /TN %%~T /DISABLE >nul 2>&1
  if errorlevel 1 (
    echo [INFO] Task not found or already unavailable: %%~T
  ) else (
    echo [INFO] Disabled task: %%~T
  )
)
echo [INFO] No V4 tasks were created.

endlocal & exit /b 0
