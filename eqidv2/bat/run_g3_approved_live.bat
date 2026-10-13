@echo off
setlocal EnableExtensions DisableDelayedExpansion
if not "%~1"=="" exit /b 2
cd /d "%~dp0.."
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
rem Requires this day's explicit operator approval. Does not auto-arm.
"%PYTHON_EXE%" -u tools\g3_live_operator.py run-approved
endlocal & exit /b %ERRORLEVEL%
