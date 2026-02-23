@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "BAT_DIR=%BASE_DIR%\bat"
set "PYTHON_EXE=%LOG_DASH_PYTHON_EXE%"
if "%PYTHON_EXE%"=="" set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

set "GMAIL_API_SCRIPT=%BAT_DIR%\send_gmail_api.py"
set "GMAIL_CREDENTIALS_FILE=%LOG_DASH_GMAIL_CREDENTIALS_FILE%"
if "%GMAIL_CREDENTIALS_FILE%"=="" set "GMAIL_CREDENTIALS_FILE=%BAT_DIR%\gmail_client_secret.json"
set "GMAIL_TOKEN_FILE=%LOG_DASH_GMAIL_TOKEN_FILE%"
if "%GMAIL_TOKEN_FILE%"=="" set "GMAIL_TOKEN_FILE=%BAT_DIR%\gmail_token.json"

echo [%DATE% %TIME%] START run_log_dashboard_gmail_api_bootstrap.bat
echo Python: %PYTHON_EXE%
echo Credentials file: %GMAIL_CREDENTIALS_FILE%
echo Token file: %GMAIL_TOKEN_FILE%
echo.
echo This opens browser once to authorize Gmail API send access.

"%PYTHON_EXE%" "%GMAIL_API_SCRIPT%" --credentials "%GMAIL_CREDENTIALS_FILE%" --token "%GMAIL_TOKEN_FILE%" --allow-interactive-auth --bootstrap-only
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="0" (
  echo [INFO] Gmail API token bootstrap completed.
) else (
  echo [ERROR] Gmail API token bootstrap failed with exit=%EXIT_CODE%.
)

echo [%DATE% %TIME%] END run_log_dashboard_gmail_api_bootstrap.bat ^(exit=%EXIT_CODE%^)
endlocal & exit /b %EXIT_CODE%
