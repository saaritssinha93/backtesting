@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=log_dashboard_server.py"
set "RUN_LOG=%LOG_DIR%\log_dashboard_server.log"
set "HOST=127.0.0.1"
set "PORT=8787"
set "END_CUTOFF_HHMM=2359"
set "MAX_RESTARTS=50"
set "RESTART_DELAY_SEC=10"
set /a RESTART_COUNT=0

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [ERROR] LOG_DASH_PASS is not set. Set it before running.
  echo Example: set LOG_DASH_PASS=your_strong_password
  endlocal & exit /b 2
)
if "%LOG_DASH_TOKEN%"=="" set "LOG_DASH_TOKEN=%LOG_DASH_PASS%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^) 
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%RUN_LOG%"
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME% on %HOST%:%PORT%
echo [%DATE% %TIME%] START %SCRIPT_NAME% on %HOST%:%PORT%>>"%RUN_LOG%"
echo [INFO] Auto-restart enabled: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%RUN_LOG%"

 :RUN_LOOP
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --host "%HOST%" --port %PORT% --username "%LOG_DASH_USER%" --password "%LOG_DASH_PASS%" --api-token "%LOG_DASH_TOKEN%" >>"%RUN_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%RUN_LOG%"

if "%EXIT_CODE%"=="0" goto DONE

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [WARN] Crash after cutoff ^(HHmm=!NOW_HHMM!^). Not restarting.>>"%RUN_LOG%"
  set "EXIT_CODE=0"
  goto DONE
)

set /a RESTART_COUNT+=1
if !RESTART_COUNT! GTR %MAX_RESTARTS% (
  echo [ERROR] Max restarts exceeded for %SCRIPT_NAME% ^(attempts=!RESTART_COUNT!^).>>"%RUN_LOG%"
  goto DONE
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%RUN_LOG%"
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

 :DONE
endlocal & exit /b %EXIT_CODE%

