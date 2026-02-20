@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BAT_DIR=%~dp0"
for %%I in ("%BAT_DIR%..") do set "BASE_DIR=%%~fI"
set "PYTHON_CMD="
if defined PYTHON_EXE (
  if exist "%PYTHON_EXE%" (
    set "PYTHON_CMD=""%PYTHON_EXE%"""
  ) else (
    set "PYTHON_CMD=%PYTHON_EXE%"
  )
)
if not defined PYTHON_CMD (
  where py >nul 2>&1 && (set "PYTHON_CMD=py -3") || set "PYTHON_CMD=python"
)
set "LOG_DIR=%BASE_DIR%\logs"
set "ALERT_DIR=%LOG_DIR%\alerts"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv.log"
set "ALERT_LOG=%ALERT_DIR%\CRITICAL_eqidv2_live_combined_analyser_csv.log"
set "STATUS_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv.status"
set "RUN_OUT=%TEMP%\eqidv2_live_combined_analyser_csv_%RANDOM%_%RANDOM%.log"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" (
  echo [ERROR] Script not found: "%BASE_DIR%\%SCRIPT_NAME%"
  endlocal & exit /b 4
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%ALERT_DIR%" mkdir "%ALERT_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"

call %PYTHON_CMD% "%BASE_DIR%\%SCRIPT_NAME%" >"%RUN_OUT%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

if exist "%RUN_OUT%" (
  type "%RUN_OUT%"
  type "%RUN_OUT%" >>"%LOG_FILE%"
  del "%RUN_OUT%" >nul 2>&1
)

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"
if "%EXIT_CODE%"=="0" (
  >"%STATUS_FILE%" echo status=SUCCESS
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
) else (
  >"%STATUS_FILE%" echo status=FAILED
  >>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
  >>"%STATUS_FILE%" echo ts=!RUN_TS!
  >>"%STATUS_FILE%" echo exit_code=%EXIT_CODE%
  >>"%STATUS_FILE%" echo log_file=%LOG_FILE%
)

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
    "try { $ws = New-Object -ComObject WScript.Shell; [void]$ws.Popup('CRITICAL FAILURE in eqidv2_live_combined_analyser_csv.py`nExitCode: %EXIT_CODE%`nSee log: %LOG_FILE%', 10, 'EQIDV2 ALERT', 16) } catch { }"

  echo [ALERT] CRITICAL: %SCRIPT_NAME% failed. ExitCode=%EXIT_CODE%
  echo [ALERT] AlertFile=!ALERT_FILE!
)

endlocal & exit /b %EXIT_CODE%





