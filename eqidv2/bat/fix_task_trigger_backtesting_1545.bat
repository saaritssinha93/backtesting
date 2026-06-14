@echo off
REM ============================================================
REM  fix_task_trigger_backtesting_1545.bat
REM  Corrects the trigger time of EQIDV2_data_for_backtesting_1545
REM  from the wrong 09:17:30 repeating trigger to a single
REM  weekday trigger at 15:45:00.
REM
REM  Run ONCE, as Administrator, OUTSIDE market hours.
REM  After running, verify with:
REM    schtasks /Query /TN EQIDV2_data_for_backtesting_1545 /FO LIST /V
REM ============================================================

echo [%DATE% %TIME%] Updating trigger for EQIDV2_data_for_backtesting_1545 ...

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $taskName='EQIDV2_data_for_backtesting_1545'; $trigger=New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 15:45; Set-ScheduledTask -TaskName $taskName -Trigger $trigger | Out-Null; Enable-ScheduledTask -TaskName $taskName | Out-Null"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Scheduled task update failed. Run this script as Administrator.
    exit /b 1
)

echo [OK] Trigger updated to one weekday run at 15:45:00 and task enabled.
echo.
echo Verifying:
schtasks /Query /TN "EQIDV2_data_for_backtesting_1545" /FO LIST /V | findstr /I "Scheduled Task State Next Run Time Start Time Days Repeat"
