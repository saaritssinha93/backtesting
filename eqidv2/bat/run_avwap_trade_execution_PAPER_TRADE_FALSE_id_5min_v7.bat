@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV7_ID_5MIN_SHORT_STOP_PCT=0.0075"
set "EQIDV7_ID_5MIN_LONG_STOP_PCT=0.0075"
set "EQIDV7_ID_5MIN_SHORT_TARGET_PCT=0.0100"
set "EQIDV7_ID_5MIN_LONG_TARGET_PCT=0.0100"
set "EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS=10000"
set "EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY=1"
set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=75"
set "EQIDV2_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_MAX_OPEN_POSITIONS=20"
set "EQIDV2_CONFIG_CHECK_BYPASS=1"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py"
set "MAX_TRADES=20"
set "STATUS_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.status"
set "HEARTBEAT_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

"%PYTHON_EXE%" -m eqidv2_config_attestation write ^
  --process avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7 ^
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

set "OPEN_POS_STATE_PATTERN=%EQIDV2_RUNTIME_ROOT%\live_signals\open_live_trades_state_{date}_id_5min_v7.json"

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
