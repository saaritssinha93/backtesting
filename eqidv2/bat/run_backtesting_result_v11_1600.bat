@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_V11_SELECTED_STRATEGY_PROFILE=final_setup_conf"

rem ============================================================
rem  LIVE PARITY BLOCK — mirror run_eqidv2_signal_discovery_v7_5min_id_persistent.bat exactly.
rem  These env vars are read at scanner module import time (module-level constants).
rem  Every value here must stay in sync with the live scanner bat.
rem ============================================================

rem --- V8 gate ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_V8_GATE=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_V8_ACCEPTED_RULES=C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv"
set "EQIDV2_SIGNAL_DISCOVERY_V7_V11_TIER123=1"

rem --- Entry window (drives _filter_entry_window in pipeline) ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_START=09:30"
set "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_END=14:30"
set "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_LAG_MIN=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE=v8_setup_compatible"

rem --- Research filters ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTERS=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTER_MODE=active"
set "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_CLOSE_LOC_GT=0.97"
set "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_VWAP_DIST_ATR_GT=3.50"
set "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN=0.97"
set "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN=3.50"
set "EQIDV2_SIGNAL_DISCOVERY_V7_B_AVWAP_RECLAIM_RANKER_MIN=0.65"
set "EQIDV2_SIGNAL_DISCOVERY_V7_L_TREND_PULLBACK_PROBATION_BLOCK=1"

rem --- SHORT_FOCUS: disabled — both LONG and SHORT setups allowed ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS=0"
set "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_ALLOWED_SIDES=SHORT,LONG"
set "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS=A_MOD_BREAK_C1_HIGH,C_OR_BREAKOUT"

rem --- Uncovered fallback (default END_TIME=14:30 and MIN_RANKER=0.75 differ from live; must set) ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_START_TIME=11:05"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_END_TIME=13:55"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_RANKER=0.65"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_QUALITY=125"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MAX_PER_SLOT=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SIDES=SHORT,LONG"
set "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SETUPS=A_MOD_BREAK_C1_LOW,C_OR_BREAKDOWN,A_PULLBACK_C2_THEN_BREAK_C2_LOW,B_HUGE_RED_FAILED_BOUNCE,D_AVWAP_LOSE_REVERSAL,G_LOWER_LOW_BREAK,C_OR_BREAKOUT"

rem --- Early mode ---
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_5M_TRADED_VALUE_RS=1000000"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_VWAP_DIST_ATR=2.80"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_TIGHT_FILTERS=1"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_BLOCKED_SETUPS=E_RS_FIRST_HOUR_BREAK_LONG,E_RS_FIRST_HOUR_BREAK_SHORT,E_VWAP_RECLAIM_EARLY_LONG,E_FAILED_OR_BREAKOUT_TRAP_SHORT,E_ORB_RETEST_HOLD_SHORT,E_ORB_RETEST_HOLD_LONG,E_FAILED_OR_BREAKDOWN_TRAP_LONG,E_GAP_HOLD_CONTINUATION_LONG,E_GAP_HOLD_CONTINUATION_SHORT,E_OPENING_DRIVE_CONTINUATION_LONG,E_OPENING_DRIVE_CONTINUATION_SHORT"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VOL_RATIO=2.00"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MIN_RS_PCT=4.00"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VWAP_DIST_ATR=1.80"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_RS_PCT=3.00"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_QUALITY=160.00"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_RS_PCT=-1.50"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MAX_ATR_PCT=0.0065"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_BODY_PCT=0.82"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_RS_PCT=-1.20"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_CLOSE_LOC=0.08"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MAX_ATR_PCT=0.008"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MIN_SCORE=95"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SIDE=4"
set "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SLOT=8"

rem ============================================================
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_PATH=%BASE_DIR%\backtesting_result_v11_daily.py"
set "VERIFY_SCRIPT=%BASE_DIR%\data_for_backtesting_verify.py"
set "WAIT_SCRIPT=%BASE_DIR%\wait_for_data_backtesting_ready.py"

rem ============================================================
rem  Target the PREVIOUS completed trading day, not today.
rem  Day-D 1-min EOD data does not land until the NEXT morning (~09:15 fetch),
rem  so a same-day 16:00 run ALWAYS fails the 1-min verify (exit 2). Backtest D-1
rem  instead: D-1's 5-min landed ~15:45 on D-1 and D-1's 1-min landed this morning,
rem  so both stores are complete and we are safely past market close (no live-feed
rem  starvation). Weekends are skipped; holidays degrade gracefully (verifier FAILs
rem  and we abort with a clear log line).
rem ============================================================
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LATEST_LOG_FILE=%LOG_DIR%\backtesting_result_v11_latest.log"

for /f %%a in ('powershell -NoProfile -Command "$d=(Get-Date).AddDays(-1); while($d.DayOfWeek -eq 'Saturday' -or $d.DayOfWeek -eq 'Sunday'){$d=$d.AddDays(-1)}; $d.ToString('yyyy-MM-dd')"') do set "TARGET_DAY=%%a"
if not defined TARGET_DAY (
    echo [%DATE% %TIME%] ERROR could not compute TARGET_DAY; aborting Backtesting Result v11.>>"%LATEST_LOG_FILE%"
    endlocal & exit /b 3
)
set "LOG_FILE=%LOG_DIR%\backtesting_result_v11_%TARGET_DAY%.log"

cd /d "%BASE_DIR%"

echo [%DATE% %TIME%] START Backtesting Result v11 (target trading day=%TARGET_DAY%)>>"%LOG_FILE%"

rem --- Data completeness gate ---
rem  The fresh verifier is authoritative: it re-scans 5-min AND 1-min for TARGET_DAY
rem  and overwrites any stale data_verify_*.json from the failed same-day attempt.
rem  The old wait-for-session helper only guarded the same-day data-build race, which
rem  cannot occur for a previous completed day, so it is intentionally not used here.
echo [%DATE% %TIME%] Checking data completeness for %TARGET_DAY%...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%VERIFY_SCRIPT%" --date "%TARGET_DAY%" >>"%LOG_FILE%" 2>&1
set "VERIFY_EXIT=%ERRORLEVEL%"

if "%VERIFY_EXIT%"=="2" (
    echo [%DATE% %TIME%] DATA VERIFY FAILED - exit=2; aborting backtesting. Check data_verify_latest.json.>>"%LOG_FILE%"
    echo [%DATE% %TIME%] END Backtesting Result v11 - exit=2 data_verify_failed>>"%LOG_FILE%"
    copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
    endlocal & exit /b 2
)

if "%VERIFY_EXIT%"=="1" (
    echo [%DATE% %TIME%] DATA VERIFY WARN - exit=1; aborting parity run to avoid partial/false comparison>>"%LOG_FILE%"
    echo [%DATE% %TIME%] END Backtesting Result v11 - exit=1 data_verify_warn>>"%LOG_FILE%"
    copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
    endlocal & exit /b 1
)

if not "%VERIFY_EXIT%"=="0" if not "%VERIFY_EXIT%"=="1" if not "%VERIFY_EXIT%"=="2" (
    echo [%DATE% %TIME%] DATA VERIFY CRASHED - unexpected exit=%VERIFY_EXIT%>>"%LOG_FILE%"
    echo [%DATE% %TIME%] END Backtesting Result v11 - verifier_crash exit=%VERIFY_EXIT%>>"%LOG_FILE%"
    copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1
    endlocal & exit /b %VERIFY_EXIT%
)

rem --- Run backtesting ---
echo [%DATE% %TIME%] Running v11 backtest + parity analysis for %TARGET_DAY%...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --date "%TARGET_DAY%" %* >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

rem --- Conf-book adherence add-on (non-fatal; flags legacy leakage that live_parity is blind to) ---
echo [%DATE% %TIME%] Running conf-book adherence check...>>"%LOG_FILE%"
"%PYTHON_EXE%" -u "%BASE_DIR%\conf_adherence_check.py" --date "%TARGET_DAY%" >>"%LOG_FILE%" 2>&1
set "ADHERENCE_EXIT=%ERRORLEVEL%"
echo [%DATE% %TIME%] conf-book adherence exit=%ADHERENCE_EXIT% (0=clean 3=legacy-leak 4=no-data; informational, does not fail the session)>>"%LOG_FILE%"

echo [%DATE% %TIME%] END Backtesting Result v11 (exit=%EXIT_CODE%)>>"%LOG_FILE%"
copy /Y "%LOG_FILE%" "%LATEST_LOG_FILE%" >nul 2>&1

endlocal & exit /b %EXIT_CODE%
