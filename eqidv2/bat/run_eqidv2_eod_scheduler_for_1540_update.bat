@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_1540_update.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_1540_update.log"
set "RUN_OUT=%TEMP%\eqidv2_eod_scheduler_for_1540_update_%RANDOM%_%RANDOM%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"

"%PYTHON_EXE%" "%BASE_DIR%\%SCRIPT_NAME%" >"%RUN_OUT%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

if exist "%RUN_OUT%" (
  type "%RUN_OUT%"
  type "%RUN_OUT%" >>"%LOG_FILE%"
  del "%RUN_OUT%" >nul 2>&1
)

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

endlocal & exit /b %EXIT_CODE%

