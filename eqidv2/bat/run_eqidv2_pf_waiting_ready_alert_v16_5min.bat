@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

REM Tier-1 alert tunables (Tier-1 fix, 2026-04-23). Defaults: alert when
REM DE reports waiting_ready > 0 continuously for 60s; cooldown 10 min.
set "EQIDV2_PF_WAITING_ALERT_THRESHOLD_SEC=60"
set "EQIDV2_PF_WAITING_ALERT_COOLDOWN_SEC=600"
set "EQIDV2_PF_WAITING_ALERT_POLL_SEC=5"

set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_pf_waiting_ready_alert_v16_5min.py"
set "STATUS_FILE=%LOG_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.status"
set "HEARTBEAT_FILE=%LOG_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.heartbeat"
set "WORKER_STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.status"
set "WORKER_HEARTBEAT_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.heartbeat"
set "LOCK_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.supervisor.lock"
set "SPAWN_RECORD_FILE=%RUNTIME_STATUS_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min.supervisor.spawn"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"
set "GMAIL_API_SCRIPT=%BASE_DIR%\bat\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%BASE_DIR%\bat\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%BASE_DIR%\bat\gmail_token.json"
if "%SUPERVISOR_ALERT_EMAIL_TO%"=="" set "SUPERVISOR_ALERT_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%SUPERVISOR_ALERT_EMAIL_FROM%"=="" set "SUPERVISOR_ALERT_EMAIL_FROM=saaritssinha93@gmail.com"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_pf_waiting_ready_alert_v16_5min_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"

cd /d "%BASE_DIR%"

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
  -CutoffHHmm 1535 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
