@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=%~dp0.."
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

if "%~1"=="" (
  echo Usage:
  echo   %~nx0 "2026-06-09 10:20" "2026-06-09 10:25" "2026-06-09 10:30"
  exit /b 2
)

cd /d "%BASE_DIR%"

REM Isolated replay root. Scanner replay and entry-engine replay both write here.
if not defined EQIDV2_REPLAY_OUTPUT_ROOT set "EQIDV2_REPLAY_OUTPUT_ROOT=C:\TradingData\eqidv2\replay"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_DATA_1MIN_DIR=C:\TradingData\eqidv2\stocks_indicators_1min_eq"
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_LAUNCHER_NAME=run_eqidv2_entry_engine_v7_replay_today.bat"

REM Match production entry-engine contract, but do not wait for live handoff.
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DELAY_SEC=60
set EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC=90
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC=0
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_POLL_SEC=0.5
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC=90
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_PROCESS_RESERVE_SEC=2
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_ENTRY_LAG_MIN=1
set EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN=3
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES=1
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION=block
set EQIDV2_ENTRY_ENGINE_USE_SLOT_CANDIDATE_JSON=1
set EQIDV2_ENTRY_ENGINE_REQUIRE_SLOT_COMPLETE_MARKER=1
set EQIDV2_ENTRY_ENGINE_REPLAY_USE_LOCAL_1MIN=1
set EQIDV2_RUNTIME_SCRIPT_NAME=eqidv2_entry_engine_1min_v5_id.py

echo [REPLAY] Generating isolated signal-discovery candidates...
call "%~dp0run_eqidv2_signal_discovery_v7_replay_today.bat" %*
if %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Signal-discovery replay failed.
  exit /b %ERRORLEVEL%
)

echo.
echo [REPLAY] Running entry-engine replay with no live signal writes...
for %%S in (%*) do (
  echo [ENTRY-REPLAY] %%~S
  "%PYTHON_EXE%" -u eqidv2_entry_engine_1min_v5_id.py --replay-slot "%%~S" --no-write-live-entries
  if !ERRORLEVEL! NEQ 0 (
    echo [ERROR] Entry-engine replay failed for %%~S.
    exit /b !ERRORLEVEL!
  )
)

echo.
echo [OK] Entry-engine replay completed. Outputs:
echo   %EQIDV2_REPLAY_OUTPUT_ROOT%\signal_discovery_v7_5mins_ID
echo   %EQIDV2_REPLAY_OUTPUT_ROOT%\entry_engine_1min_v5_ID
endlocal & exit /b 0
