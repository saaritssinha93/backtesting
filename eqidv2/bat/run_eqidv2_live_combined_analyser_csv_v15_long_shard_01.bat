@echo off
setlocal
set "EQIDV15_LONG_SHARD_ID=01"
set "EQIDV15_LONG_SHARD_COUNT=10"
call "%~dp0run_eqidv2_live_combined_analyser_csv_v15_long_shard_common.bat"
endlocal
