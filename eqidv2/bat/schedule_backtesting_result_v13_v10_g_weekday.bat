@echo off
setlocal EnableExtensions
set "ACTIVE_STRATEGY_RECORD=%~dp0..\configs\strategies\active_profile.json"
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; if (Test-Path -LiteralPath $env:ACTIVE_STRATEGY_RECORD) { $deployment = Get-Content -LiteralPath $env:ACTIVE_STRATEGY_RECORD -Raw | ConvertFrom-Json; if ($deployment.profile -eq 'V13_V10_G_3') { Write-Error 'G task installation is blocked while persisted profile is G3. Use bat/migrate_fno_v13_v10_g_to_g3.ps1 after reviewing its dry-run plan.'; exit 2 } }"
if errorlevel 1 (
    endlocal & exit /b 2
)
rem End deployment guard: no scheduled-task command may precede this point.
set "BAT_DIR=%~dp0"
set "TASK_BACKTEST=EQIDV2_backtesting_result_v13_v10_g_1620"
set "BAT_BACKTEST=%BAT_DIR%run_backtesting_result_v13_v10_g_1620.bat"
set "TASK_HARDENER=%BAT_DIR%harden_scheduled_task.ps1"

if not exist "%BAT_BACKTEST%" (
    echo [ERROR] Missing G launcher: %BAT_BACKTEST%
    endlocal & exit /b 1
)
rem Retain the historical tasks for audit, but remove their future schedules.
for %%T in (EQIDV2_backtesting_result_v11_1600 EQIDV2_backtesting_result_v7_v8_1600) do (
    schtasks /Query /TN "%%T" >nul 2>&1
    if not errorlevel 1 (
        schtasks /Change /TN "%%T" /Disable >nul 2>&1
        if errorlevel 1 (
            echo [ERROR] Could not disable legacy task %%T; no G task was installed.
            endlocal & exit /b 1
        )
    )
)
echo [INFO] Creating Backtesting result v13-v10-G for weekdays at 16:20 IST.
schtasks /Create /F /TN "%TASK_BACKTEST%" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:20 /TR "%BAT_BACKTEST%"
if errorlevel 1 (
    endlocal & exit /b 1
)
if exist "%TASK_HARDENER%" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%TASK_HARDENER%" -TaskName "%TASK_BACKTEST%"
    if errorlevel 1 (
        endlocal & exit /b 1
    )
)
echo [INFO] %TASK_BACKTEST% scheduled. The G orchestrator handles holidays and same-day data readiness.
endlocal & exit /b 0
