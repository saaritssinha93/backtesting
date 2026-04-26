@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM P5 (2026-04-22) — EOD reconciler runner. Cross-checks pool_lifecycle.jsonl
REM vs live_trades CSV vs Kite orderbook for the day. Writes
REM live_signals\eod_reconciliation_<date>_v16_5min.json. Sends alert email
REM (via send_gmail_api.py) when the reconciler exits 3 (mismatches found).
REM
REM Schedule (suggested): schtasks /TR ".\run_eqidv2_eod_reconciler_v16_5min.bat"
REM /SC WEEKDAYS /ST 16:00 /TN "eqidv2_eod_reconciler_v16_5min".
REM 16:00 IST is ~30m after market close; broker order-book is fully settled
REM by then and the executor has flushed its CSV.

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"

set "SCRIPT_NAME=eqidv2_eod_reconciler_v16_5min.py"
set "LOG_DIR=%BASE_DIR%\logs"
set "GMAIL_API_SCRIPT=%BASE_DIR%\bat\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%BASE_DIR%\bat\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%BASE_DIR%\bat\gmail_token.json"
if "%RECON_ALERT_EMAIL_TO%"=="" set "RECON_ALERT_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%RECON_ALERT_EMAIL_FROM%"=="" set "RECON_ALERT_EMAIL_FROM=saaritssinha93@gmail.com"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\eqidv2_eod_reconciler_v16_5min_%TODAY_IST%.log"
set "ALERT_FILE=%EQIDV2_RUNTIME_ROOT%\live_signals\eod_reconciliation_alert_%TODAY_IST%_v16_5min.json"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%BASE_DIR%"

REM Default: query broker orderbook. Pass NO_BROKER=1 for replay/paper-only days.
set "EXTRA_ARGS="
if "%NO_BROKER%"=="1" set "EXTRA_ARGS=--no-broker"

"%PYTHON_EXE%" -u "%BASE_DIR%\%SCRIPT_NAME%" %EXTRA_ARGS% >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="3" (
  echo [%TIME%] EOD reconciler reported mismatches; sending alert email. >> "%LOG_FILE%"
  if exist "%ALERT_FILE%" (
    if exist "%GMAIL_TOKEN_FILE%" (
      "%PYTHON_EXE%" "%GMAIL_API_SCRIPT%" ^
        --credentials "%GMAIL_CREDENTIALS_FILE%" ^
        --token "%GMAIL_TOKEN_FILE%" ^
        --to "%RECON_ALERT_EMAIL_TO%" ^
        --subject "[EQIDV2 V16] EOD reconciliation mismatches %TODAY_IST%" ^
        --body-file "%ALERT_FILE%" >> "%LOG_FILE%" 2>&1
    ) else (
      echo [%TIME%] WARN gmail token missing at %GMAIL_TOKEN_FILE%; skipping email. >> "%LOG_FILE%"
    )
  )
)

endlocal & exit /b %EXIT_CODE%
