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
echo [INFO] Using protocol=http2 and edge-ip-version=4 for better stability on restrictive networks.

:RUN_TUNNEL
powershell -NoProfile -Command "& { & '%CF_BIN%' tunnel --url '%DASH_URL%' --protocol http2 --edge-ip-version 4 *>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath '%LOG_FILE%' -Append; exit $LASTEXITCODE }"
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
