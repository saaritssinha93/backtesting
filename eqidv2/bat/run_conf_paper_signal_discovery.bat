@echo off
REM ============================================================================
REM Conf-path PAPER launcher (scanner) — enables the 16-setup final_setup_conf,
REM then calls the normal scanner launcher, which inherits the flag. Paper-day
REM qualification use only. Reversible: just launch the normal .bat to revert.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
call "%~dp0run_eqidv2_signal_discovery_v7_5min_id_persistent.bat"
