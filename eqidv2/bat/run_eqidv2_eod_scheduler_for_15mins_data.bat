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
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_15mins_data.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_15mins_data.log"
set "RUN_OUT=%TEMP%\eqidv2_eod_scheduler_for_15mins_data_%RANDOM%_%RANDOM%.log"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" (
  echo [ERROR] Script not found: "%BASE_DIR%\%SCRIPT_NAME%"
  endlocal & exit /b 4
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

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

endlocal & exit /b %EXIT_CODE%

