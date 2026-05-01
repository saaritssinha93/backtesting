@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_CACHE_5MIN_DIR=C:\TradingData\eqidv2\stocks_cache_5min_eq_live"
set "EQIDV2_SLOT_READY_PENDING_DIR=C:\TradingData\eqidv2\slot_ready_5m_pending"
set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"
set "EQIDV2_PENDING_FETCH_ALIGN_TO_5MIN=1"
REM Headroom fix: PF wakes at slot+1s instead of slot+2s. Buys 1s on every
REM cycle for the Kite fetch -> verify -> marker write sequence without
REM eating into DE's late-lag budget. DE still wakes at slot+4s.
set "EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC=1"
REM Aggressive-poll mode: kill the 55s WAIT_PENDING_POOL idle band and poll
REM the pending pool every 2s from slot+1s onward. When SEE writes a signal,
REM PF picks it up within 2s and fetches immediately. Default behaviour
REM (RECHECK=55s, POLL_INTERVAL=5s) was tuned for low-churn operation; this
REM override prioritises responsiveness. Does NOT compress Kite historical_data
REM publish delay (the dominant bar-close -> fetchable latency, ~20-50s),
REM which is the real bottleneck in the restart-resume path.
set "EQIDV2_PENDING_FETCH_RECHECK_AFTER_SEC=1"
set "EQIDV2_PENDING_POOL_POLL_INTERVAL_SEC=1"
REM Closed-trigger-bar verify mode (2026-04-27): re-enables the 2026-04-23
REM PF-TRIGGER-FIX after the 2026-04-24 entry-bar-verify reversion.
REM - DISABLE_LAG_SHIFT=1 keeps trigger_slot = source_slot = entry_ist
REM - PF_VERIFY_TRIGGER_BAR=1 makes verify_slot = trigger_slot - 5min
REM Combined: PF verifies the just-CLOSED trigger bar at entry_slot+1s.
REM Kite serves closed bars within ~1-2s; no 52s forming-bar wait.
REM Marker filename still = entry_slot for DE compatibility. Rollback: =0.
set "EQIDV16_5MIN_DISABLE_LAG_SHIFT=1"
set "EQIDV16_5MIN_PF_VERIFY_TRIGGER_BAR=1"
REM Stale-marker expiry: refuse to publish markers for slots whose entry
REM time is already past MAX_PF_MARKER_LAG_SEC. Without this, a PF stall +
REM recovery publishes 30-40min-old markers (2026-04-27 cascade: 36 trades
REM rejected by executor late-detection gate). Default 90s; 0 disables.
set "EQIDV16_5MIN_MAX_PF_MARKER_LAG_SEC=90"
set "EQIDV2_PENDING_FETCH_MAX_WORKERS=64"
set "EQIDV2_PENDING_FETCH_MAX_WORKERS_PER_APP=8"
REM Cross-app retry ladder (2026-04-23): up to 5 attempts on 5 different Kite
REM apps via partition_shift rotation. Delays 1,2,4,8 = worst-case +15s added
REM to PF latency, still inside the LATE_ENTRY band per strategy_v2 §B1.
set "EQIDV2_PENDING_FETCH_RETRY_DELAYS_SEC=1,2,4,8"
REM Partial retry: narrow each attempt to still-failing tickers and accumulate
REM successes across attempts. Set to 0 to fall back to all-or-nothing retry.
set "EQIDV2_PENDING_FETCH_RETRY_PARTIAL=1"
REM PF tail-latency cap: was 12s -> 6s -> 3s. Tier-1 fix (2026-04-23).
REM PF only fetches a tiny pending set (typically 1-5 tickers per slot), so the
REM per-call Kite HTTP timeout dominates tail latency. 3s is the historical
REM_data p99 budget under normal conditions; anything slower is almost always
REM a genuine Kite stall (network blip, partial 502, gateway pause) where
REM failing fast and surfacing to the retry loop beats waiting. PF-scoped:
REM LF (separate process, separate bat) keeps its own value.
set "EQIDV2_5M_KITE_TIMEOUT_SEC=3"
set "EQIDV2_5M_ENFORCE_SESSION_COMPLETENESS=1"
set "EQIDV2_5M_LIVE_SLIM_MODE=1"
set "EQIDV2_5M_LIVE_SLIM_CALENDAR_DAYS=10"
set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_pending_data_fetcher_v16_5min.py"
set "STATUS_FILE=%LOG_DIR%\eqidv2_pending_data_fetcher_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_pending_data_fetcher_v16_5min.heartbeat"
set "WORKER_STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pending_data_fetcher_v16_5min.status"
set "WORKER_HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pending_data_fetcher_v16_5min.heartbeat"
set "LOCK_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pending_data_fetcher_v16_5min.supervisor.lock"
set "SPAWN_RECORD_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pending_data_fetcher_v16_5min.supervisor.spawn"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "GMAIL_API_SCRIPT=%BASE_DIR%\bat\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%BASE_DIR%\bat\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%BASE_DIR%\bat\gmail_token.json"
if "%SUPERVISOR_ALERT_EMAIL_TO%"=="" set "SUPERVISOR_ALERT_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%SUPERVISOR_ALERT_EMAIL_FROM%"=="" set "SUPERVISOR_ALERT_EMAIL_FROM=saaritssinha93@gmail.com"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_pending_data_fetcher_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"

cd /d "%BASE_DIR%"

REM P0 fix (2026-04-23): write the runtime config claim BEFORE handing off to
REM the supervisor. The Python PF verifies its os.environ against this file at
REM startup; mismatches log [STARTUP.CONFIG] DRIFT (soft mode by default;
REM EQIDV2_CONFIG_CHECK_STRICT=1 makes drift a boot exit). Whitelist must
REM include EQIDV2_DATA_5M_DIR — without it, PF can silently read/write the
REM EOD `_5min_eq` parquets instead of intraday `_5min_eq_live`.
"%PYTHON_EXE%" -m eqidv2_config_attestation write ^
  --process eqidv2_pending_data_fetcher_v16_5min ^
  --from-env EQIDV2_DATA_5M_DIR ^
  --from-env EQIDV2_RUNTIME_ROOT ^
  --from-env EQIDV2_LIVE_SIGNALS_DIR ^
  --from-env EQIDV16_5MIN_PF_VERIFY_TRIGGER_BAR ^
  --from-env EQIDV16_5MIN_DISABLE_LAG_SHIFT ^
  --from-env EQIDV16_5MIN_MAX_PF_MARKER_LAG_SEC ^
  --from-env EQIDV2_PENDING_FETCH_INTERVAL_SEC ^
  --from-env EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC ^
  --from-env EQIDV2_5M_KITE_TIMEOUT_SEC >> "%LOG_FILE%" 2>&1
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
  -WorkerStaleTimeoutSec 360 ^
  -WorkerStartGraceSec 120 ^
  -WorkerDiscoveryTimeoutSec 45 ^
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
  -CutoffHHmm 1520 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
