@echo off
setlocal

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=python"
set "LOG_DIR=%BASE_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"
echo [%DATE% %TIME%] START authentication.py >> "%LOG_DIR%\authentication_runner.log"
"%PYTHON_EXE%" "%BASE_DIR%\authentication.py" >> "%LOG_DIR%\authentication_runner.log" 2>&1
echo [%DATE% %TIME%] END authentication.py >> "%LOG_DIR%\authentication_runner.log"

endlocal
