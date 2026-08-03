@echo off
REM ============================================================================
REM Conf-path PAPER launcher (1-min entry engine) — enables the 11-setup
REM final_setup_conf (pre-momentum gates + exit levels from the conf), then calls
REM the normal entry-engine launcher, which inherits the flag. Reversible.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_LAUNCHER_NAME=run_conf_paper_entry_engine.bat"
REM Parity with V11 live_parity replay: read the exact per-slot scanner snapshot
REM instead of the moving latest_candidate_tickers.json pointer.
set "EQIDV2_ENTRY_ENGINE_USE_SLOT_CANDIDATE_JSON=1"
set "EQIDV2_ENTRY_ENGINE_REQUIRE_SLOT_COMPLETE_MARKER=1"
REM Give the scanner enough time to finish and atomically write the exact slot JSON,
REM without opening the T+3 stale-entry window. Keep executor stale gate in sync.
set "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC=90"
set "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_POLL_SEC=0.25"
set "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC=0.25"
set "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC=90"
set "EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC=90"
set "EQIDV2_ENTRY_ENGINE_RAW_FETCH_PARALLEL_APPS=1"
set "EQIDV2_ENTRY_ENGINE_RAW_FETCH_APP_COUNT=8"
set "EQIDV2_ENTRY_ENGINE_WRITE_SLA_SEC=10"
call "%~dp0run_eqidv2_entry_engine_1min_v5_id.bat"
