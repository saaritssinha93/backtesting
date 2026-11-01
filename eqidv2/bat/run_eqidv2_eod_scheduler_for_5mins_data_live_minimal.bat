@echo off
REM Backup reference (2026-02-26):
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
REM - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "EQIDV2_DATA_5M_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
set "EQIDV2_CACHE_5MIN_DIR=C:\TradingData\eqidv2\stocks_cache_5min_eq_live"
set "EQIDV2_5M_ENFORCE_SESSION_COMPLETENESS=1"
set "EQIDV2_5M_SYNTHETIC_GAP_FILL=1"
REM Start at +2s, but recheck a new candle if Kite initially publishes an
REM exact OHLCV copy of the prior slot. Unsafe copies never receive a complete marker.
set "EQIDV2_5M_PROVISIONAL_DUPLICATE_RETRY=1"
set "EQIDV2_5M_PROVISIONAL_SETTLE_SEC=18"
set "EQIDV2_5M_PROVISIONAL_RETRY_ATTEMPTS=3"
set "EQIDV2_5M_PROVISIONAL_RETRY_INTERVAL_SEC=2"
REM FnO cash legs use five exact 1-minute candles, matching the V5/V6 backtest.
set "EQIDV2_FNO_5M_FROM_1M=1"
set "EQIDV2_FNO_UNIVERSE_PATH=C:\TradingData\eqidv2\fno_oi\universe\latest_near_month.parquet"
set "EQIDV2_5M_LIVE_SLIM_MODE=1"
set "EQIDV2_5M_LIVE_SLIM_CALENDAR_DAYS=10"
set "EQIDV2_5M_BUFFER_SEC=2"
set "EQIDV2_5M_QUARTER_HOUR_BUFFER_SEC=2"
REM 2026-04-27 outage tuning: with 4 retries x 12s timeout x 5 batches per
REM partition, a hung Kite endpoint took the partition to ~240s (>150s
REM partition_timeout) and SIGKILLed every worker for 5 consecutive slots.
REM Drop per-call timeout to 8s so worst-case per-ticker = 4*8 + backoff
REM (~40s), keeping partitions inside the 150s SIGKILL boundary.
set "EQIDV2_5M_KITE_TIMEOUT_SEC=8"
set "EQIDV2_5M_PARTITION_TIMEOUT_SEC=150"
set "EQIDV2_5M_ADAPTIVE_THROTTLE=1"
set "EQIDV2_5M_ADAPTIVE_MIN_WORKERS=40"
set "EQIDV2_5M_ADAPTIVE_MIN_WORKERS_PER_APP=5"
set "EQIDV2_5M_ADAPTIVE_TOTAL_STEP=32"
set "EQIDV2_5M_ADAPTIVE_PER_APP_STEP=4"
set "EQIDV2_5M_ADAPTIVE_RECOVERY_OK_RATIO=0.90"
set "EQIDV2_5M_ADAPTIVE_RECOVERY_STREAK=2"
REM Keep one authenticated process per app alive across slots. This removes
REM Windows spawn/auth startup from the decision-critical fetch path.
set "EQIDV2_5M_PERSISTENT_PARTITION_WORKERS=1"
REM Never launch an interactive Selenium login from the decision-critical
REM market-data loop.  The pre-open authentication task owns token repair;
REM this feed immediately repartitions work across whatever apps validate.
set "EQIDV2_5M_INLINE_APP_SELF_HEAL=0"
REM v2 universe (1262 syms after quarantine) takes ~22-24s/slot; bump warn from 20s to 30s
REM so the adaptive throttle does not keep stepping workers down on cosmetic SLA breaches.
set "EQIDV2_5M_SLOT_SLA_WARN_SEC=50"
set "EQIDV2_VERIFY_SAMPLE_SIZE=32"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=eqidv2_eod_scheduler_for_5mins_data_live_minimal.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.log"
REM Historical benchmark: 320/40 was used before persistent partition workers.
REM 40 workers gives ~4 batches per partition (vs 5 at 32, 3.3 at 48) — balances
REM speed and per-app spread. Combined with Fix A (1-slot lag tolerance), expect
REM ~24-26s clean slots.
REM 2026-06-11: 384/48 created 384 Python workers on a 16-logical-CPU host,
REM producing 150s partition timeouts under parquet/Kite contention. That result
REM motivates the bounded ceiling below; adaptive throttle may step down further.
REM 2026-09-29: 160/20 hit the 150s partition timeout across multiple apps on
REM Sep 25 and Sep 29 under shared load. Cap future launches at 128/16; retain
REM the exact same data, verification, deadline, and fail-closed marker rules.

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%BASE_DIR%"
REM Fixed data supervisor retains exact CLI, rotation, singleton and freshness rules.
"%PYTHON_EXE%" -u "%BASE_DIR%\tools\fno_data_supervisor.py" --service cash5m >>"%LOG_DIR%\eqidv2_eod_scheduler_for_5mins_data_live_minimal.launcher.log" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
