@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "TASK_NAME=EQIDV2_eod_5mins_data_0900"

echo [INFO] 5-minute live data fetch scheduling is decommissioned.
echo [INFO] Removing scheduled task if present: %TASK_NAME%
schtasks /Delete /F /TN "%TASK_NAME%" >nul 2>&1
if errorlevel 1 (
  echo [INFO] No existing task found for %TASK_NAME%.
  endlocal & exit /b 0
)
echo [INFO] Removed scheduled task: %TASK_NAME%

endlocal & exit /b 0
