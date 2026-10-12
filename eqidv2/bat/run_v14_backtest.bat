@echo off
setlocal EnableExtensions DisableDelayedExpansion
title V14 MIS non-FNO backtest

rem Resolve the repository relative to this launcher, without changing V13 jobs.
for %%I in ("%~dp0..") do set "V14_BASE_DIR=%%~fI"
set "V14_PYTHON_EXE=%LocalAppData%\Programs\Python\Python312\python.exe"
if not exist "%V14_PYTHON_EXE%" set "V14_PYTHON_EXE=python"
if not defined V14_RUNTIME_ROOT set "V14_RUNTIME_ROOT=C:\TradingData\eqidv2\v14"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

pushd "%V14_BASE_DIR%"
if errorlevel 1 (
    echo [ERROR] Cannot open repository: %V14_BASE_DIR%
    endlocal & exit /b 2
)
if "%~1"=="" (
    "%V14_PYTHON_EXE%" -u -m v14 run-all --fetch-missing
) else (
    "%V14_PYTHON_EXE%" -u -m v14 %*
)
set "V14_EXIT_CODE=%ERRORLEVEL%"
popd
endlocal & exit /b %V14_EXIT_CODE%
