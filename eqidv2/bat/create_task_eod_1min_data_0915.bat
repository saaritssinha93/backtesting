@echo off
REM ============================================================
REM  create_task_eod_1min_data_0915.bat
REM  Creates the EQIDV2_eod_1min_data_0915 scheduled task.
REM  Launches the supervised live 1-minute data fetch scheduler
REM  at 09:15 Mon-Fri. The scheduler itself waits until 09:17:30
REM  for the first fetch, then fetches every 5 min until 15:40.
REM
REM  Run ONCE, as Administrator.
REM ============================================================

set "TASK_NAME=EQIDV2_eod_1min_data_0915"
set "BAT_PATH=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\bat\run_eqidv2_eod_scheduler_for_1min_data_live.bat"

echo [%DATE% %TIME%] Creating %TASK_NAME% ...

schtasks /Delete /TN "%TASK_NAME%" /F 2>nul
schtasks /Create /TN "%TASK_NAME%" /TR "cmd.exe /c \"%BAT_PATH%\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:15 /RL HIGHEST /F

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed. Run this script as Administrator.
    exit /b 1
)

echo [OK] Task created.
echo.
echo Verifying:
schtasks /Query /TN "%TASK_NAME%" /FO LIST /V | findstr /I "Task Name Start Time Status Days"
