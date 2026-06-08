@echo off
setlocal

cd /d "%~dp0.."
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_DATA_1MIN_DIR=C:\TradingData\eqidv2\stocks_indicators_1min_eq"
rem T+1 execution: engine wakes one wall-clock minute after signal close.
rem The paper executor uses live LTP; minute OHLC is a reference/filter input.
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DELAY_SEC=60
rem Final handoff deadline is T+1:30; five seconds remain for processing.
set EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC=30
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_PROCESS_RESERVE_SEC=2
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC=1
rem T+1 is the universal production entry contract.
set EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_ENTRY_LAG_MIN=1
set EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN=3
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES=1
set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION=block

python eqidv2_entry_engine_1min_v5_id.py

endlocal
