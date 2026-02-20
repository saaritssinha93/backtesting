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
set "SCRIPT_NAME=log_dashboard_server.py"
set "RUN_LOG=%LOG_DIR%\log_dashboard_server.log"
set "HOST=127.0.0.1"
set "PORT=8787"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" (
  echo [ERROR] Script not found: "%BASE_DIR%\%SCRIPT_NAME%"
  endlocal & exit /b 4
)

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

call %PYTHON_CMD% "%BASE_DIR%\%SCRIPT_NAME%" --host "%HOST%" --port %PORT% --username "%LOG_DASH_USER%" --password "%LOG_DASH_PASS%" --api-token "%LOG_DASH_TOKEN%" >>"%RUN_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%RUN_LOG%"

endlocal & exit /b %EXIT_CODE%

