@echo off
setlocal
set "EQIDV15_SHORT_SHARD_ID=07"
set "EQIDV15_SHORT_SHARD_COUNT=10"
call "%~dp0run_eqidv2_live_combined_analyser_csv_v15_short_shard_common.bat"
endlocal & exit /b %ERRORLEVEL%
