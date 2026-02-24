@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_v2.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v2.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv_v2.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v2.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v2.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%ALERT_DIR%" mkdir "%ALERT_DIR%"

cd /d "%BASE_DIR%"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 900 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 1540 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%a"
  set "ALERT_FILE=%ALERT_DIR%\CRITICAL_%SCRIPT_NAME:.py=%_FAILED_!TS!.txt"

  (
    echo ================================================
    echo CRITICAL FAILURE: %SCRIPT_NAME%
    echo DateTime: %DATE% %TIME%
    echo ExitCode: %EXIT_CODE%
    echo Host: %COMPUTERNAME%
    echo User: %USERNAME%
    echo LogFile: %LOG_FILE%
    echo ================================================
  )>"!ALERT_FILE!"

  type "!ALERT_FILE!"

  (
    echo ================================================
    echo CRITICAL FAILURE: %SCRIPT_NAME%
    echo DateTime: %DATE% %TIME%
    echo ExitCode: %EXIT_CODE%
    echo AlertFile: !ALERT_FILE!
    echo LogFile: %LOG_FILE%
    echo ================================================
  )>>"%ALERT_LOG%"

  eventcreate /L APPLICATION /T ERROR /ID 9001 /SO EQIDV2 /D "CRITICAL: %SCRIPT_NAME% failed with exit code %EXIT_CODE%. See %LOG_FILE%" >nul 2>&1
  msg %USERNAME% /TIME:15 "CRITICAL: %SCRIPT_NAME% failed (exit=%EXIT_CODE%) - check %LOG_FILE%" >nul 2>&1

  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "try { $ws = New-Object -ComObject WScript.Shell; [void]$ws.Popup('CRITICAL FAILURE in eqidv2_live_combined_analyser_csv_v2.py`nExitCode: %EXIT_CODE%`nSee log: %LOG_FILE%', 10, 'EQIDV2 ALERT', 16) } catch { }"

  echo [ALERT] CRITICAL: %SCRIPT_NAME% failed. ExitCode=%EXIT_CODE%
  echo [ALERT] AlertFile=!ALERT_FILE!
)

endlocal & exit /b %EXIT_CODE%
