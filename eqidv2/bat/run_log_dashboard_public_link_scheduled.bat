@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "DASH_BAT=%BAT_DIR%\run_log_dashboard_public_link.bat"

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [ERROR] LOG_DASH_PASS is not set.
  echo Set LOG_DASH_PASS in the task environment before scheduled launch.
  endlocal & exit /b 2
)
if "%LOG_DASH_EMAIL_TO%"=="" set "LOG_DASH_EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
if "%LOG_DASH_EMAIL_FROM%"=="" set "LOG_DASH_EMAIL_FROM=saaritssinha93@gmail.com"
if "%LOG_DASH_EMAIL_SEND_MODE%"=="" set "LOG_DASH_EMAIL_SEND_MODE=gmail_api_only"
if "%LOG_DASH_EMAIL_SUBJECT_PREFIX%"=="" set "LOG_DASH_EMAIL_SUBJECT_PREFIX=EQIDV2 Dashboard URL"
if "%LOG_DASH_GMAIL_CREDENTIALS_FILE%"=="" set "LOG_DASH_GMAIL_CREDENTIALS_FILE=%BAT_DIR%\gmail_client_secret.json"
if "%LOG_DASH_GMAIL_TOKEN_FILE%"=="" set "LOG_DASH_GMAIL_TOKEN_FILE=%BAT_DIR%\gmail_token.json"
if "%LOG_DASH_GMAIL_INTERACTIVE_AUTH%"=="" set "LOG_DASH_GMAIL_INTERACTIVE_AUTH=0"

call "%DASH_BAT%"
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
