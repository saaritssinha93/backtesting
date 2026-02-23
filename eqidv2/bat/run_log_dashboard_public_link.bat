@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "DASH_BAT=%BAT_DIR%\run_log_dashboard_server.bat"
set "DASH_URL=http://127.0.0.1:8787"
set "CF_BIN="
set "CF_RETRY_SEC=5"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\log_dashboard_public_link.log"
set "LATEST_URL_FILE=%LOG_DIR%\log_dashboard_latest_url.txt"

set "PYTHON_EXE=%LOG_DASH_PYTHON_EXE%"
if "%PYTHON_EXE%"=="" set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

set "GMAIL_API_SCRIPT=%BAT_DIR%\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%LOG_DASH_GMAIL_CREDENTIALS_FILE%"
if "%GMAIL_CREDENTIALS_FILE%"=="" set "GMAIL_CREDENTIALS_FILE=%BAT_DIR%\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%LOG_DASH_GMAIL_TOKEN_FILE%"
if "%GMAIL_TOKEN_FILE%"=="" set "GMAIL_TOKEN_FILE=%BAT_DIR%\gmail_token.json"
set "GMAIL_INTERACTIVE_AUTH=%LOG_DASH_GMAIL_INTERACTIVE_AUTH%"
if "%GMAIL_INTERACTIVE_AUTH%"=="" set "GMAIL_INTERACTIVE_AUTH=0"

set "EMAIL_TO=%LOG_DASH_EMAIL_TO%"
if "%EMAIL_TO%"=="" set "EMAIL_TO=saaritssinha93@gmail.com,dragontastic007@gmail.com"
set "EMAIL_FROM=%LOG_DASH_EMAIL_FROM%"
if "%EMAIL_FROM%"=="" set "EMAIL_FROM=saaritssinha93@gmail.com"
set "EMAIL_SEND_MODE=%LOG_DASH_EMAIL_SEND_MODE%"
if "%EMAIL_SEND_MODE%"=="" set "EMAIL_SEND_MODE=gmail_api_only"
set "EMAIL_SUBJECT_PREFIX=%LOG_DASH_EMAIL_SUBJECT_PREFIX%"
if "%EMAIL_SUBJECT_PREFIX%"=="" set "EMAIL_SUBJECT_PREFIX=EQIDV2 Dashboard URL"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

if "%LOG_DASH_USER%"=="" set "LOG_DASH_USER=eqidv2"
if "%LOG_DASH_PASS%"=="" (
  echo [ERROR] LOG_DASH_PASS is not set.
  echo Example: set LOG_DASH_PASS=your_strong_password
  endlocal & exit /b 2
)

for %%I in (cloudflared.exe) do set "CF_BIN=%%~$PATH:I"
if not defined CF_BIN if exist "C:\Program Files (x86)\cloudflared\cloudflared.exe" set "CF_BIN=C:\Program Files (x86)\cloudflared\cloudflared.exe"
if not defined CF_BIN if exist "C:\Program Files\cloudflared\cloudflared.exe" set "CF_BIN=C:\Program Files\cloudflared\cloudflared.exe"
if not defined CF_BIN if exist "C:\Program Files\Cloudflare\cloudflared\cloudflared.exe" set "CF_BIN=C:\Program Files\Cloudflare\cloudflared\cloudflared.exe"

if not defined CF_BIN (
  echo [ERROR] cloudflared is not installed.
  echo Install once with: winget install --id Cloudflare.cloudflared -e
  echo If already installed, restart terminal or run this directly:
  echo   "C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel --url %DASH_URL%
  endlocal & exit /b 3
)

echo [%DATE% %TIME%] START run_log_dashboard_public_link.bat
echo [%DATE% %TIME%] START run_log_dashboard_public_link.bat>>"%LOG_FILE%"

echo Starting local dashboard server...
start "EQIDV2 Log Dashboard" /MIN cmd /c call "%DASH_BAT%"

timeout /t 3 >nul

echo Using cloudflared: %CF_BIN%
"%CF_BIN%" --version
echo Starting cloudflared tunnel for %DASH_URL%
echo Open the https://*.trycloudflare.com URL shown below on your phone.
echo Email target: %EMAIL_TO%
echo Email send mode: %EMAIL_SEND_MODE%
echo Gmail credentials file: %GMAIL_CREDENTIALS_FILE%
echo Gmail token file: %GMAIL_TOKEN_FILE%
echo Gmail interactive auth: %GMAIL_INTERACTIVE_AUTH%
echo Python exe: %PYTHON_EXE%
echo Latest URL file: %LATEST_URL_FILE%
echo [INFO] Using protocol=http2 and edge-ip-version=4 for better stability on restrictive networks.

:RUN_TUNNEL
set "EMAIL_FROM_ARG=%EMAIL_FROM%"
if "%EMAIL_FROM_ARG%"=="" set "EMAIL_FROM_ARG=__EMPTY__"
powershell -NoProfile -ExecutionPolicy Bypass -File "%BAT_DIR%\run_log_dashboard_public_link_capture.ps1" -CloudflaredPath "%CF_BIN%" -DashUrl "%DASH_URL%" -LogFile "%LOG_FILE%" -LatestUrlFile "%LATEST_URL_FILE%" -PythonExe "%PYTHON_EXE%" -GmailApiScript "%GMAIL_API_SCRIPT%" -GmailCredentialsFile "%GMAIL_CREDENTIALS_FILE%" -GmailTokenFile "%GMAIL_TOKEN_FILE%" -GmailInteractiveAuth "%GMAIL_INTERACTIVE_AUTH%" -EmailTo "%EMAIL_TO%" -EmailFrom "%EMAIL_FROM_ARG%" -EmailSendMode "%EMAIL_SEND_MODE%" -EmailSubjectPrefix "%EMAIL_SUBJECT_PREFIX%"
set "CF_EXIT=%ERRORLEVEL%"

if "%CF_EXIT%"=="0" (
  echo [INFO] cloudflared exited normally.
  echo [%DATE% %TIME%] END run_log_dashboard_public_link.bat ^(exit=0^)>>"%LOG_FILE%"
  endlocal & exit /b 0
)

echo [WARN] cloudflared exited with code %CF_EXIT%. Retrying in %CF_RETRY_SEC%s...
echo [%DATE% %TIME%] cloudflared exited with code %CF_EXIT%. Retrying in %CF_RETRY_SEC%s...>>"%LOG_FILE%"
timeout /t %CF_RETRY_SEC% >nul
goto RUN_TUNNEL

endlocal
