@echo off
setlocal EnableExtensions

REM Registers the weekday V7 Gate Promotion task at 16:25 (after the 16:20
REM walk-forward gate has refreshed its report). DRY-RUN by default.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "TASK_HARDENER=%BAT_DIR%\harden_scheduled_task.ps1"
set "TASK_GATE_PROMO=EQIDV2_v7_gate_promotion_1625"
set "BAT_GATE_PROMO=%BAT_DIR%\run_v7_gate_promotion_report.bat"

if not exist "%BAT_GATE_PROMO%" (
  echo [ERROR] Missing bat file: %BAT_GATE_PROMO%
  endlocal & exit /b 1
)

echo [INFO] Creating weekday V7 Gate Promotion task at 16:25 ...
schtasks /Create /F /TN "%TASK_GATE_PROMO%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:25 /TR "%BAT_GATE_PROMO%"
if errorlevel 1 endlocal & exit /b 1

if exist "%TASK_HARDENER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_GATE_PROMO%"
  if errorlevel 1 endlocal & exit /b 1
)

echo [INFO] V7 Gate Promotion task created/updated successfully:
echo        %TASK_GATE_PROMO%  ^(Mon-Fri 16:25, dry-run^)

endlocal & exit /b 0
