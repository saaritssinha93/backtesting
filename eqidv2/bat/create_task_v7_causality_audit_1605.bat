@echo off
REM ============================================================
REM  create_task_v7_causality_audit_1605.bat
REM  Creates EQIDV2_v7_causality_audit_1605 scheduled task.
REM  Runs v7_causality_audit.py at 16:05 Mon-Fri (post-session).
REM
REM  Run ONCE, as Administrator.
REM ============================================================

set "TASK_NAME=EQIDV2_v7_causality_audit_1605"
set "BAT_PATH=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\bat\run_v7_causality_audit.bat"

echo [%DATE% %TIME%] Creating %TASK_NAME% ...

schtasks /Delete /TN "%TASK_NAME%" /F 2>nul
schtasks /Create /TN "%TASK_NAME%" /TR "cmd.exe /c \"%BAT_PATH%\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:05 /RL HIGHEST /F

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed. Run this script as Administrator.
    exit /b 1
)

echo [OK] Task created.
echo.
echo Verifying:
schtasks /Query /TN "%TASK_NAME%" /FO LIST /V | findstr /I "Task Name Start Time Status Days"
