@echo off
REM ============================================================================
REM Conf-path PAPER launcher (paper executor) — sets the conf flag so the paper
REM executor mirrors LIVE risk config (20 positions, Rs10k daily brake ON, P0-19)
REM and runs the MTM-aware brake in OBSERVE mode (logs only; EQIDV2_BRAKE_MTM_ACT
REM stays off). Then calls the normal paper-executor launcher, which inherits all
REM of these. Reversible: launch the normal .bat to revert to research defaults.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_LAUNCHER_NAME=run_conf_paper_executor.bat"
REM Match the conf entry-engine handoff window. Without this, later-but-valid
REM slot-json entries can be emitted by the entry engine and then stale-skipped
REM by the paper executor's independent detection-lag gate.
set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=90"
set "EQIDV2_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_MAX_OPEN_POSITIONS=20"
set "EQIDV2_MAX_CAPITAL_DEPLOYED_RS=2000000"
set "EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES=20"
set "EQIDV2_PAPER_V7_ID_5MIN_MAX_OPEN_POSITIONS=20"
set "EQIDV2_PAPER_V7_ID_5MIN_MAX_CAPITAL_DEPLOYED_RS=2000000"
set "MAX_TRADES=20"
REM MTM brake: observe-only by default in conf mode. To let it actually block new
REM entries after you have watched the OBSERVE logs, uncomment the next line:
REM set "EQIDV2_BRAKE_MTM_ACT=1"
REM Flatten-on-breach stays OFF until explicitly enabled and watched:
REM set "EQIDV2_BRAKE_FLATTEN_ON_BREACH=1"
call "%~dp0run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat"
