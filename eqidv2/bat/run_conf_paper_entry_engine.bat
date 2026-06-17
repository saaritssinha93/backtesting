@echo off
REM ============================================================================
REM Conf-path PAPER launcher (1-min entry engine) — enables the 16-setup
REM final_setup_conf (pre-momentum gates + exit levels from the conf), then calls
REM the normal entry-engine launcher, which inherits the flag. Reversible.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
call "%~dp0run_eqidv2_entry_engine_1min_v5_id.bat"
