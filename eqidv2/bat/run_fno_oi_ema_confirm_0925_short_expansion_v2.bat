@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "SCRIPT_PATH=%BASE_DIR%\fno_oi_ema_confirm_0925_short_expansion_v2.py"

cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" %*
endlocal & exit /b %ERRORLEVEL%
