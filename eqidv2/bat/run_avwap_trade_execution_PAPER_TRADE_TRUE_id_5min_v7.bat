@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
if not defined EQIDV2_LAUNCHER_NAME set "EQIDV2_LAUNCHER_NAME=run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat"
set "EQIDV2_PAPER_V7_ENTRY_WINDOW_START=09:30"
set "EQIDV2_PAPER_V7_ENTRY_WINDOW_END=14:30"
set "EQIDV2_PAPER_V7_ENTRY_LAG_MIN=1"
rem STOP/TARGET percentages are fallback controls in paper mode.
rem Signal-row values from v6.SETUP_EXIT_RULES are primary; these backfill only
rem when the signal row is missing or zero. Do not describe as informational/dead.
rem Keep in sync with run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat.
set "EQIDV7_ID_5MIN_SHORT_STOP_PCT=0.0075"
set "EQIDV7_ID_5MIN_LONG_STOP_PCT=0.0075"
set "EQIDV7_ID_5MIN_SHORT_TARGET_PCT=0.0100"
set "EQIDV7_ID_5MIN_LONG_TARGET_PCT=0.0100"
set "EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS=10000"
if not defined EQIDV2_LATE_DETECTION_MAX_LAG_SEC set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=30"
if not defined EQIDV2_MAX_CONCURRENT_TRADES set "EQIDV2_MAX_CONCURRENT_TRADES=100"
if not defined EQIDV2_MAX_OPEN_POSITIONS set "EQIDV2_MAX_OPEN_POSITIONS=100"
if not defined EQIDV2_MAX_CAPITAL_DEPLOYED_RS set "EQIDV2_MAX_CAPITAL_DEPLOYED_RS=2000000"
if not defined EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES set "EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES=100"
if not defined EQIDV2_PAPER_V7_ID_5MIN_MAX_OPEN_POSITIONS set "EQIDV2_PAPER_V7_ID_5MIN_MAX_OPEN_POSITIONS=100"
if not defined EQIDV2_PAPER_V7_ID_5MIN_MAX_CAPITAL_DEPLOYED_RS set "EQIDV2_PAPER_V7_ID_5MIN_MAX_CAPITAL_DEPLOYED_RS=2000000"
set "EQIDV2_PAPER_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN=0.97"
set "EQIDV2_PAPER_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN=3.50"
set "EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_ENABLED=1"
set "EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_RS=10000"
set "EQIDV2_PAPER_V7_C_OR_BREAKOUT_TIME_STOP_ENABLED=1"
set "EQIDV2_PAPER_V7_C_OR_BREAKOUT_TIME_STOP_MIN=30"
set "EQIDV2_PAPER_V7_C_OR_BREAKOUT_SESSION_CAP_ENABLED=1"
set "EQIDV2_PAPER_V7_C_OR_BREAKOUT_SESSION_CAP=50"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py"
if not defined MAX_TRADES set "MAX_TRADES=%EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES%"
set "END_CUTOFF_HHMM=1540"
set "MAX_RESTARTS=20"
set "RESTART_DELAY_SEC=15"
set /a RESTART_COUNT=0

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7_%TODAY_IST%.log"

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
echo [INFO] Target policy: short_stop=%EQIDV7_ID_5MIN_SHORT_STOP_PCT%, short_target=%EQIDV7_ID_5MIN_SHORT_TARGET_PCT%, long_stop=%EQIDV7_ID_5MIN_LONG_STOP_PCT%, long_target=%EQIDV7_ID_5MIN_LONG_TARGET_PCT%
echo [INFO] Target policy: short_stop=%EQIDV7_ID_5MIN_SHORT_STOP_PCT%, short_target=%EQIDV7_ID_5MIN_SHORT_TARGET_PCT%, long_stop=%EQIDV7_ID_5MIN_LONG_STOP_PCT%, long_target=%EQIDV7_ID_5MIN_LONG_TARGET_PCT%>>"%LOG_FILE%"
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
