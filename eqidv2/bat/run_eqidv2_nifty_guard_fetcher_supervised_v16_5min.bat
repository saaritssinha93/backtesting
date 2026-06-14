@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Supervised replacement for run_nifty_guard_fetcher_v16_5min.bat.
REM Wraps a long-running Python loop in supervise_command.ps1 so NF gets:
REM   - heartbeat tracking, hung-process detection, auto-restart
REM   - the same status/lock/spawn-record patterns as PF/DE/Executor
REM   - protection against the bat-loop-starvation bug seen on 2026-04-28
REM     (40min gap between markers driven by per-poll powershell.exe spawns).

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"

REM NF guard wrapper config (mirrors legacy bat defaults; tunable via env)
set "EQIDV2_NF_FIRST_SLOT_HHMM=0915"
set "EQIDV2_NF_HARD_STOP_HHMM=1531"
set "EQIDV2_NF_SLOT_OFFSET_SEC=2"
set "EQIDV2_NF_POLL_SEC=1"
set "EQIDV2_NF_SLOT_MAX_RETRIES=3"
set "EQIDV2_NF_SLOT_RETRY_DELAY_SEC=5"
set "EQIDV2_NF_FETCH_TIMEOUT_SEC=60"
set "NIFTY_SYMBOL=NIFTYBEES"
REM Proxy primary OWNS the consumed aliases NIFTY/NIFTY50/NIFTY_50 so the live
REM VWAP/regime context keeps a volume-bearing ETF series and stays in parity
REM with the backtest store. True NIFTY 50 index is fetched ONLY under the
REM NIFTY50_INDEX alias (no consumer reads it) so a zero-volume index series can
REM never overwrite the regime aliases nor leak into _eq_live2 via moving_files.
set "NIFTY_ALIASES=NIFTYBEES,NIFTYBEES_PROXY,NIFTY,NIFTY50,NIFTY_50"
set "EQIDV2_NF_FETCH_NIFTY_INDEX=1"
set "NIFTY_INDEX_SYMBOL=NIFTY 50"
set "NIFTY_INDEX_ALIASES=NIFTY50_INDEX"

set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_nifty_guard_fetcher_supervised_v16_5min.py"
set "STATUS_FILE=%LOG_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.heartbeat"
set "WORKER_STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.status"
set "WORKER_HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.heartbeat"
set "LOCK_FILE=%RUNTIME_STATUS_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.supervisor.lock"
set "SPAWN_RECORD_FILE=%RUNTIME_STATUS_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min.supervisor.spawn"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "GMAIL_API_SCRIPT=%BASE_DIR%\bat\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%BASE_DIR%\bat\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%BASE_DIR%\bat\gmail_token.json"
if "%SUPERVISOR_ALERT_EMAIL_TO%"=="" set "SUPERVISOR_ALERT_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%SUPERVISOR_ALERT_EMAIL_FROM%"=="" set "SUPERVISOR_ALERT_EMAIL_FROM=saaritssinha93@gmail.com"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_nifty_guard_fetcher_supervised_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"

cd /d "%BASE_DIR%"

REM Runtime config attestation (matches PF/DE pattern). Stamps the timing
REM flags so [STARTUP.CONFIG] DRIFT surfaces if the bat ever silently changes.
"%PYTHON_EXE%" -m eqidv2_config_attestation write ^
  --process eqidv2_nifty_guard_fetcher_supervised_v16_5min ^
  --from-env EQIDV2_RUNTIME_ROOT ^
  --from-env EQIDV2_DATA_5M_DIR ^
  --from-env EQIDV2_NF_FIRST_SLOT_HHMM ^
  --from-env EQIDV2_NF_HARD_STOP_HHMM ^
  --from-env EQIDV2_NF_SLOT_OFFSET_SEC ^
  --from-env EQIDV2_NF_POLL_SEC ^
  --from-env EQIDV2_NF_FETCH_TIMEOUT_SEC ^
  --from-env NIFTY_SYMBOL ^
  --from-env NIFTY_ALIASES ^
  --from-env EQIDV2_NF_FETCH_NIFTY_INDEX ^
  --from-env NIFTY_INDEX_SYMBOL ^
  --from-env NIFTY_INDEX_ALIASES >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  echo [WARN] runtime config claim write failed; continuing in soft mode>> "%LOG_FILE%"
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -LockFile "%LOCK_FILE%" ^
  -SpawnRecordFile "%SPAWN_RECORD_FILE%" ^
  -WorkerStatusFile "%WORKER_STATUS_FILE%" ^
  -WorkerHeartbeatFile "%WORKER_HEARTBEAT_FILE%" ^
  -WorkerStaleTimeoutSec 180 ^
  -WorkerStartGraceSec 60 ^
  -WorkerDiscoveryTimeoutSec 30 ^
  -AlertEmailTo "%SUPERVISOR_ALERT_EMAIL_TO%" ^
  -AlertEmailFrom "%SUPERVISOR_ALERT_EMAIL_FROM%" ^
  -GmailApiScript "%GMAIL_API_SCRIPT%" ^
  -GmailCredentialsFile "%GMAIL_CREDENTIALS_FILE%" ^
  -GmailTokenFile "%GMAIL_TOKEN_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 28800 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -CutoffHHmm 1531 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
