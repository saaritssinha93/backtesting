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
REM TEMP (2026-04-27, user request): force qty=1 for every NEW live entry.
REM Remove this line to restore signal-row / DEFAULT_POSITION_SIZE sizing.
set "EQIDV16_5MIN_FORCE_ENTRY_QUANTITY=1"
set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900"
REM M2 (2026-04-22): uniform concurrency caps — both EQIDV2_MAX_CONCURRENT_TRADES
REM and EQIDV2_MAX_OPEN_POSITIONS set here, and --max-trades CLI arg below
REM passes the same value, so env/code/CLI agree. Do not change one without the others.
REM 2026-04-24: bumped 10 -> 20 at user request (mid-session restart).
set "EQIDV2_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_MAX_OPEN_POSITIONS=20"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py"
set "MAX_TRADES=20"
set "STATUS_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

REM Audit #1 (2026-04-22): write the runtime config claim file BEFORE handing
REM off to the supervisor. The Python executor verifies its os.environ against
REM this file at startup; mismatches log [STARTUP.CONFIG] DRIFT (soft mode by
REM default; EQIDV2_CONFIG_CHECK_STRICT=1 makes drift a boot exit).
"%PYTHON_EXE%" -m eqidv2_config_attestation write ^
  --process avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min ^
  --from-env EQIDV2_LATE_DETECTION_MAX_LAG_SEC ^
  --from-env EQIDV2_MAX_CONCURRENT_TRADES ^
  --from-env EQIDV2_MAX_OPEN_POSITIONS ^
  --from-env EQIDV2_FILL_WAIT_TIMEOUT_SEC ^
  --from-env EQIDV2_ENTRY_RETRY_DELAY_SEC ^
  --from-env EQIDV2_VOLUME_GATE_ENABLE ^
  --from-env EQIDV2_VOLUME_GATE_MAX_PARTICIPATION_PCT ^
  --from-env EQIDV2_RUNTIME_ROOT >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  echo [WARN] runtime config claim write failed; continuing in soft mode>> "%LOG_FILE%"
)

REM M1 (2026-04-22): CutoffHHmm extended 1540 -> 1545 so the supervisor keeps
REM the executor alive past the FORCED_CLOSE_TIME (15:20) with a safety margin.
REM OpenPositionsStateFilePattern points at the executor crash-recovery JSON;
REM supervisor overrides the cutoff stop whenever open_trades is non-empty.
REM {date} is substituted with today's IST date at runtime.
set "OPEN_POS_STATE_PATTERN=%EQIDV2_RUNTIME_ROOT%\live_signals\open_live_trades_state_{date}_v16_5min.json"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--max-trades","%MAX_TRADES%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 28800 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 1545 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff ^
  -OpenPositionsStateFilePattern "%OPEN_POS_STATE_PATTERN%" ^
  -NtpServer "pool.ntp.org" ^
  -NtpMaxDriftSec 30.0

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
