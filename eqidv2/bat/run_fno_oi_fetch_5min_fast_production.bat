@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "TRIAL_GATE_PS1=%BASE_DIR%\bat\fno_oi_fast_production_trial_date_gate.ps1"
set "LOG_DIR=%BASE_DIR%\logs"
set "TRIAL_GATE_LOG=%LOG_DIR%\fno_oi_fast_production_trial_gate.log"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%TRIAL_GATE_PS1%" exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%TRIAL_GATE_PS1%" -Role Trial -TrialDate 2026-09-02 >>"%TRIAL_GATE_LOG%" 2>&1
set "TRIAL_GATE_EXIT=%ERRORLEVEL%"
if "%TRIAL_GATE_EXIT%"=="42" endlocal & exit /b 0
if not "%TRIAL_GATE_EXIT%"=="0" endlocal & exit /b %TRIAL_GATE_EXIT%

set "EXCLUSIVITY_PS1=%BASE_DIR%\bat\assert_fno_oi_fast_production_trial_exclusive.ps1"
set "EXCLUSIVITY_LOG=%LOG_DIR%\fno_oi_fast_production_trial_exclusivity.log"
if not exist "%EXCLUSIVITY_PS1%" endlocal & exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -File "%EXCLUSIVITY_PS1%" >>"%EXCLUSIVITY_LOG%" 2>&1
set "EXCLUSIVITY_EXIT=%ERRORLEVEL%"
if not "%EXCLUSIVITY_EXIT%"=="0" endlocal & exit /b %EXCLUSIVITY_EXIT%

set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=fno_oi_fetch_5min_fast_production.py"
set "LOG_FILE=%LOG_DIR%\fno_oi_fetch_5min_fast_production.log"
set "STATUS_FILE=%LOG_DIR%\fno_oi_fetch_5min_fast_production.supervisor.status"
set "HEARTBEAT_FILE=%LOG_DIR%\fno_oi_fetch_5min_fast_production.supervisor.heartbeat"
set "FRESHNESS_FILE=%RUNTIME_STATUS_DIR%\fno_oi_fetch_5min_fast_production.heartbeat"
set "SUPERVISOR_PS1=%BASE_DIR%\bat\supervise_command.ps1"

if not exist "%BASE_DIR%\%SCRIPT_NAME%" endlocal & exit /b 1
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
cd /d "%BASE_DIR%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SUPERVISOR_PS1%" ^
  -Name "%SCRIPT_NAME%" ^
  -FilePath "%PYTHON_EXE%" ^
  -ArgumentList "-u","%BASE_DIR%\%SCRIPT_NAME%","--session-date","2026-09-02","--boundary-buffer-sec","3","--request-interval-sec","0.36","--workers-per-app","2","--writer-workers","8","--min-coverage","0.99","--bootstrap-days","60","--slot-retry-attempts","2","--slot-retry-delay-sec","2" ^
  -WorkDir "%BASE_DIR%" ^
  -LogFile "%LOG_FILE%" ^
  -StatusFile "%STATUS_FILE%" ^
  -HeartbeatFile "%HEARTBEAT_FILE%" ^
  -MaxRestarts 20 ^
  -RestartDelaySec 15 ^
  -MonitorIntervalSec 5 ^
  -HungTimeoutSec 0 ^
  -CooldownWindowSec 300 ^
  -CooldownMaxRestarts 6 ^
  -CooldownDelaySec 120 ^
  -FreshnessFile "%FRESHNESS_FILE%" ^
  -FreshnessTimeoutSec 180 ^
  -FreshnessGraceSec 180 ^
  -CutoffHHmm 1534 ^
  -SkipRunAfterCutoff ^
  -StopRestartsAfterCutoff

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
