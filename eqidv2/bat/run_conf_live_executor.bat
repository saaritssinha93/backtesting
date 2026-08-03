@echo off
REM ============================================================================
REM Conf-path LIVE launcher (real executor) - sets final_setup_conf as the live
REM book, then calls the normal real executor. Reversible: launch the normal
REM run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat to revert.
REM ============================================================================
set "EQIDV2_USE_FINAL_SETUP_CONF=1"
set "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
set "EQIDV2_LAUNCHER_NAME=run_conf_live_executor.bat"
if /I "%~1"=="--help" (
  echo Usage: run_conf_live_executor.bat
  echo Sets EQIDV2_USE_FINAL_SETUP_CONF=1, then launches the real Kite executor.
  exit /b 0
)
call "%~dp0run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat"
