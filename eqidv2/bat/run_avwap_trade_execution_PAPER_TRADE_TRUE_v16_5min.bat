@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV16_5MIN_SHORT_STOP_PCT=0.0075"
set "EQIDV16_5MIN_LONG_STOP_PCT=0.0075"
set "EQIDV16_5MIN_SHORT_TARGET_PCT=0.0100"
set "EQIDV16_5MIN_LONG_TARGET_PCT=0.0100"
set "EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS=10000"
REM I-8 + Option-A — accommodate lag-2 DE parity (slot+5min shift) plus 5-min poll cadence.
REM Natural worst-case lag from entry_slot_open ~605s; 900s allows headroom without rejecting.
set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900"
REM M2 (2026-04-22): uniform concurrency caps — see PAPER_TRADE_FALSE bat comment.
REM Paper executor reads PAPER-specific env vars (with fallback to the generic ones),
REM so we set both to keep env/code/CLI fully aligned.
REM 2026-04-24: bumped 10 -> 20 to stay in lockstep with the live executor.
set "EQIDV2_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_MAX_OPEN_POSITIONS=20"
set "EQIDV2_PAPER_V16_5MIN_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_PAPER_V16_5MIN_MAX_OPEN_POSITIONS=20"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.py"
set "MAX_TRADES=20"
set "END_CUTOFF_HHMM=1540"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)
  echo [%DATE% %TIME%] SKIP %SCRIPT_NAME% ^(current HHmm=!NOW_HHMM!, cutoff=%END_CUTOFF_HHMM%^)>>"%LOG_FILE%"
  endlocal & exit /b 0
)

echo [%DATE% %TIME%] START %SCRIPT_NAME%
echo [%DATE% %TIME%] START %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] Using daily log file: %LOG_FILE%
echo [INFO] Using daily log file: %LOG_FILE%>>"%LOG_FILE%"
echo [INFO] Target policy: short_stop=%EQIDV16_5MIN_SHORT_STOP_PCT%, short_target=%EQIDV16_5MIN_SHORT_TARGET_PCT%, long_stop=%EQIDV16_5MIN_LONG_STOP_PCT%, long_target=%EQIDV16_5MIN_LONG_TARGET_PCT%
echo [INFO] Target policy: short_stop=%EQIDV16_5MIN_SHORT_STOP_PCT%, short_target=%EQIDV16_5MIN_SHORT_TARGET_PCT%, long_stop=%EQIDV16_5MIN_LONG_STOP_PCT%, long_target=%EQIDV16_5MIN_LONG_TARGET_PCT%>>"%LOG_FILE%"
echo [INFO] Auto-restart enabled: max_restarts=%MAX_RESTARTS%, retry_delay=%RESTART_DELAY_SEC%s, cutoff=%END_CUTOFF_HHMM%>>"%LOG_FILE%"

:RUN_LOOP
"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" --max-trades %MAX_TRADES% --entry-price-source ltp_on_signal >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)
echo [%DATE% %TIME%] END %SCRIPT_NAME% ^(exit=%EXIT_CODE%^)>>"%LOG_FILE%"

if "%EXIT_CODE%"=="0" goto DONE

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('HHmm')"') do set "NOW_HHMM=%%a"
if !NOW_HHMM! GEQ %END_CUTOFF_HHMM% (
  echo [WARN] Crash after cutoff ^(HHmm=!NOW_HHMM!^). Not restarting.>>"%LOG_FILE%"
  set "EXIT_CODE=0"
  goto DONE
)

set /a RESTART_COUNT+=1
if !RESTART_COUNT! GTR %MAX_RESTARTS% (
  echo [ERROR] Max restarts exceeded for %SCRIPT_NAME% ^(attempts=!RESTART_COUNT!^).>>"%LOG_FILE%"
  goto DONE
)

echo [WARN] %SCRIPT_NAME% crashed ^(exit=%EXIT_CODE%^). Restart !RESTART_COUNT!/%MAX_RESTARTS% in %RESTART_DELAY_SEC%s...>>"%LOG_FILE%"
timeout /t %RESTART_DELAY_SEC% >nul
goto RUN_LOOP

:DONE
endlocal & exit /b %EXIT_CODE%
