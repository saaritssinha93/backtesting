@echo off
setlocal EnableExtensions DisableDelayedExpansion
for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
cd /d "%BASE_DIR%"
rem Scheduled G3 capital-sized session is deliberately readiness-only.
rem The qty1 filename is retained for scheduler and state-directory compatibility.
rem No acknowledgement, arm record, LIVE worker, or broker request is created.
if not "%~1"=="" (
  endlocal & exit /b 2
)
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_v13_v10_g_3_live_kite_session.py" --readiness-only
endlocal & exit /b %ERRORLEVEL%
