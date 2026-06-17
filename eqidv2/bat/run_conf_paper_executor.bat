@echo off
REM ============================================================================
REM Conf-path PAPER launcher (paper executor) — sets the conf flag so the paper
REM executor mirrors LIVE risk config (20 positions, Rs10k daily brake ON, P0-19)
REM and runs the MTM-aware brake in OBSERVE mode (logs only; EQIDV2_BRAKE_MTM_ACT
REM stays off). Then calls the normal paper-executor launcher, which inherits all
REM of these. Reversible: launch the normal .bat to revert to research defaults.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
REM MTM brake: observe-only by default in conf mode. To let it actually block new
REM entries after you have watched the OBSERVE logs, uncomment the next line:
REM set "EQIDV2_BRAKE_MTM_ACT=1"
REM Flatten-on-breach stays OFF until explicitly enabled and watched:
REM set "EQIDV2_BRAKE_FLATTEN_ON_BREACH=1"
call "%~dp0run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat"
