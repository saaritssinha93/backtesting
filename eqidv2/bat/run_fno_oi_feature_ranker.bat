@echo off
setlocal EnableExtensions
for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
if not exist "%BASE_DIR%\logs" mkdir "%BASE_DIR%\logs"
cd /d "%BASE_DIR%"
REM Data-only supervisor preserves exclusivity, singleton, freshness, restart and cutoff checks.
"%PYTHON_EXE%" -u "%BASE_DIR%\tools\fno_data_supervisor.py" --service ranker >>"%BASE_DIR%\logs\fno_oi_feature_ranker.launcher.log" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
