@echo off
setlocal EnableExtensions DisableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
set "SESSION_SCRIPT=%BASE_DIR%\fno_multi_paper_session.py"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\fno_v10_v11_v12_paper.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1

rem This runner is deliberately PAPER-only.  Reject arguments so a task or
rem manual caller cannot append a mode override to the frozen `run` command.
if not "%~1"=="" (
  >>"%LOG_FILE%" echo [FATAL] Unsupported arguments. This runner is frozen to PAPER mode.
  endlocal & exit /b 2
)

if not exist "%PYTHON_EXE%" (
  where python >nul 2>&1
  if errorlevel 1 (
    >>"%LOG_FILE%" echo [FATAL] Python was not found at the frozen path or on PATH.
    endlocal & exit /b 2
  )
  set "PYTHON_EXE=python"
)

if not exist "%SESSION_SCRIPT%" (
  >>"%LOG_FILE%" echo [FATAL] Missing multi-strategy paper session: %SESSION_SCRIPT%
  endlocal & exit /b 2
)

set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "FNO_MULTI_PAPER_EXECUTION_MODE=PAPER"
set "FNO_MULTI_PAPER_SESSION_ID=fno_v10_v11_v12_paper"

cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%SESSION_SCRIPT%" run >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
