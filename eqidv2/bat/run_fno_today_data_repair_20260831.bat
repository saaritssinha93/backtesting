@echo off
setlocal EnableExtensions

rem One-time, post-close recovery for the 2026-08-31 FnO data session.
rem This runner uses fixed dates deliberately so StartWhenAvailable remains safe
rem if Windows starts the task late. It never enables market-hours overrides.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\fno_today_data_repair_20260831.log"
set "STATUS_FILE=%LOG_DIR%\fno_today_data_repair_20260831.status.txt"
set "REPAIR_ROOT=C:\TradingData\eqidv2\fno_oi\historical_repair"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%REPAIR_ROOT%" mkdir "%REPAIR_ROOT%"
cd /d "%BASE_DIR%"

rem The live cash 5-minute task can remain active until 15:50. Accept only this
rem date after that cutoff: a delayed StartWhenAvailable launch must never run
rem during a later trading session and race the production writer.
powershell -NoProfile -Command "$now=Get-Date; if ($now.Date -ne ([datetime]'2026-08-31').Date -or $now -lt [datetime]'2026-08-31T15:51:00') { exit 2 }"
if errorlevel 1 (
  >"%STATUS_FILE%" echo BLOCKED_OUTSIDE_SAFE_WINDOW
  >>"%LOG_FILE%" echo [%DATE% %TIME%] BLOCKED: repair is allowed only on 2026-08-31 after 15:51 IST
  exit /b 2
)

>"%STATUS_FILE%" echo RUNNING
>>"%LOG_FILE%" echo [%DATE% %TIME%] START full-day FnO data recovery for 2026-08-31

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 1/6 cash equity 1-minute full-day fetch
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_equity_1m_backfill.py" --all-mapped --start-date 2026-08-31 --end-date 2026-08-31 --force-window --backup-root "%REPAIR_ROOT%\equity_1m_before_20260831_eod" >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 2/6 cash equity exact 5-minute rebuild
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_equity_5m_repair.py" --one-minute-root "C:\TradingData\eqidv2\stocks_indicators_1min_eq" --backtest-root "C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2" --live-root "C:\TradingData\eqidv2\stocks_indicators_5min_eq_live" --backup-root "%REPAIR_ROOT%\equity_5m_before_20260831_eod" --report-path "%REPAIR_ROOT%\equity_5m_repair_20260831_eod.json" --workers 8 --live-retention-days 10 >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 3/6 futures OI exact 5-minute fetch
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_oi_backfill_5min.py" --contract-months 2026-09 --from-date 2026-08-31 --to-date 2026-08-31 --max-apps 6 >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 4/6 futures OI 1-minute full-day fetch
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_oi_fetch_1m_history.py" --from-date 2026-08-31 --to-date 2026-08-31 --contract-month 26SEP --full-refresh --max-apps 6 >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 5/6 historical OI rankings rebuild
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_oi_rank_history.py" --months 26SEP --from-date 2026-08-31 --to-date 2026-08-31 --cohort month --emit-live-layout >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>>"%LOG_FILE%" echo [%DATE% %TIME%] STEP 6/6 futures OI end-of-day audit
"%PYTHON_EXE%" -u "%BASE_DIR%\fno_oi_eod_qc.py" --session-date 2026-08-31 --no-repair --max-apps 6 >>"%LOG_FILE%" 2>&1
if errorlevel 1 goto :failed

>"%STATUS_FILE%" echo SUCCESS
>>"%LOG_FILE%" echo [%DATE% %TIME%] SUCCESS full-day FnO data recovery for 2026-08-31
exit /b 0

:failed
set "EXIT_CODE=%ERRORLEVEL%"
>"%STATUS_FILE%" echo FAILED exit=%EXIT_CODE%
>>"%LOG_FILE%" echo [%DATE% %TIME%] FAILED full-day FnO data recovery exit=%EXIT_CODE%
exit /b %EXIT_CODE%
