@echo off
setlocal EnableExtensions
title Options V13-V10-G Continuous Paper Trade Log
set "FNO_V6_STRATEGY_PROFILE=V13_V10_G"
set "FNO_V13_V10_G_OPTIONS_EXECUTION_MODE=PAPER"
set "SESSION_ID=fno_v13_v10_g_options_trade_logger"
set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
if not exist "%BASE_DIR%\logs" mkdir "%BASE_DIR%\logs" >nul 2>&1
cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_v13_v10_g_options_paper.py" --role trade-logger %* >>"%BASE_DIR%\logs\fno_v13_v10_g_options_trade_logger.log" 2>&1
endlocal & exit /b %ERRORLEVEL%

