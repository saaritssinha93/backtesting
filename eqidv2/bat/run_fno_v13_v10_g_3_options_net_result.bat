@echo off
setlocal EnableExtensions
title Options V13-V10-G-3 Paper Net Result
set "FNO_V6_STRATEGY_PROFILE=V13_V10_G_3"
set "FNO_V13_V10_G_3_OPTIONS_EXECUTION_MODE=PAPER"
set "SESSION_ID=fno_v13_v10_g_3_options_net_result"
for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
if not exist "%BASE_DIR%\logs" mkdir "%BASE_DIR%\logs" >nul 2>&1
cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_v13_v10_g_3_options_paper.py" --role net-result %* >>"%BASE_DIR%\logs\fno_v13_v10_g_3_options_net_result.log" 2>&1
endlocal & exit /b %ERRORLEVEL%

