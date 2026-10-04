@echo off
setlocal EnableExtensions DisableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "SCRIPT_PATH=%BASE_DIR%\tools\v13_shadow_automation.py"
set "LOG_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status\v13_shadow_automation"

if not "%~1"=="" (
    echo [ERROR] This scheduled runner accepts no arguments.
    endlocal & exit /b 2
)
if not exist "%SCRIPT_PATH%" (
    echo [ERROR] Missing no-authority shadow adapter: %SCRIPT_PATH%
    endlocal & exit /b 3
)
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set "SESSION_DATE="
for /f %%a in ('powershell -NoProfile -NonInteractive -Command "[TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow,'India Standard Time').ToString('yyyy-MM-dd')"') do set "SESSION_DATE=%%a"
if not defined SESSION_DATE (
    echo [ERROR] Could not resolve the current IST session date.
    endlocal & exit /b 3
)
set "LOG_FILE=%LOG_DIR%\seal_%SESSION_DATE%.log"

cd /d "%BASE_DIR%"
>>"%LOG_FILE%" echo [%DATE% %TIME%] START prospective-shadow seal ^(session=%SESSION_DATE%, execution_authority=false^)
"%PYTHON_EXE%" -u "%SCRIPT_PATH%" --session-date "%SESSION_DATE%" seal >>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
>>"%LOG_FILE%" echo [%DATE% %TIME%] END prospective-shadow seal ^(exit=%EXIT_CODE%^)
endlocal & exit /b %EXIT_CODE%
