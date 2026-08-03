@echo off
REM ============================================================================
REM Conf-path PAPER launcher (scanner) — enables the 11-setup final_setup_conf,
REM then calls the normal scanner launcher, which inherits the flag. Paper-day
REM qualification use only. Reversible: just launch the normal .bat to revert.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_LAUNCHER_NAME=run_conf_paper_signal_discovery.bat"
call "%~dp0run_eqidv2_signal_discovery_v7_5min_id_persistent.bat"
