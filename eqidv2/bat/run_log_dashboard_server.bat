@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=log_dashboard_server.py"
set "RUN_LOG=%LOG_DIR%\log_dashboard_server.log"
set "HOST=127.0.0.1"
set "PORT=8787"

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [ERROR] LOG_DASH_PASS is not set. Set it before running.
  echo Example: set LOG_DASH_PASS=your_strong_password
  endlocal & exit /b 2
)
if "%LOG_DASH_TOKEN%"=="" set "LOG_DASH_TOKEN=%LOG_DASH_PASS%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START %SCRIPT_NAME% on %HOST%:%PORT%
echo [%DATE% %TIME%] START %SCRIPT_NAME% on %HOST%:%PORT%>>"%RUN_LOG%"

"%PYTHON_EXE%" "%BASE_DIR%\%SCRIPT_NAME%" --host "%HOST%" --port %PORT% --username "%LOG_DASH_USER%" --password "%LOG_DASH_PASS%" --api-token "%LOG_DASH_TOKEN%" >>"%RUN_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%RUN_LOG%"

endlocal & exit /b %EXIT_CODE%

