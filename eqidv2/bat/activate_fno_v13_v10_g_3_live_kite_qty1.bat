@echo off
setlocal EnableExtensions DisableDelayedExpansion
title FnO V13-V10-G-3 Live Kite INR 10000 Capital 5x
set "FNO_V6_STRATEGY_PROFILE=V13_V10_G_3"
set "FNO_V6_EXECUTION_MODE=LIVE"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_OBSERVABILITY_ENABLED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
set "SESSION_SCRIPT=%BASE_DIR%\fno_v13_v10_g_3_live_kite_session.py"
set "SUPERVISOR_PY=%BASE_DIR%\tools\g3_live_supervisor.py"
set "LOG_DIR=%BASE_DIR%\logs"
set "LAUNCHER_LOG=%LOG_DIR%\fno_v13_v10_g_3_live_kite_qty1.launcher.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
rem The separately approved operator must supply the existing G3 LIVE ACK.
rem Never set it here; the Python supervisor and coordinator both verify it.
if not "%~1"=="" (
  >>"%LAUNCHER_LOG%" echo [FATAL] Unsupported arguments. This runner uses fixed G3 LIVE exposure.
  endlocal & exit /b 2
)
if not exist "%PYTHON_EXE%" (
  where python >nul 2>&1
  if errorlevel 1 (
    >>"%LAUNCHER_LOG%" echo [FATAL] Python was not found at the frozen path or on PATH.
    endlocal & exit /b 2
  )
  set "PYTHON_EXE=python"
)
if not exist "%SESSION_SCRIPT%" (
  >>"%LAUNCHER_LOG%" echo [FATAL] Missing live coordinator: %SESSION_SCRIPT%
  endlocal & exit /b 2
)
if not exist "%SUPERVISOR_PY%" (
  >>"%LAUNCHER_LOG%" echo [FATAL] Missing live supervisor: %SUPERVISOR_PY%
  endlocal & exit /b 2
)
cd /d "%BASE_DIR%"
"%PYTHON_EXE%" -u "%SUPERVISOR_PY%"
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
