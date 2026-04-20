@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "RUNTIME_STATUS_DIR=%EQIDV2_RUNTIME_ROOT%\runtime_status"
set "SCRIPT_NAME=eqidv2_live_combined_analyser_csv_v4_long.py"
set "LOG_FILE=%LOG_DIR%\eqidv2_live_combined_analyser_csv_v4_long.log"
set "STATUS_FILE=%RUNTIME_STATUS_DIR%\eqidv2_live_combined_analyser_csv_v4_long.status"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%RUNTIME_STATUS_DIR%" mkdir "%RUNTIME_STATUS_DIR%"
for /f %%a in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set "RUN_TS=%%a"

echo [%DATE% %TIME%] DISABLED %SCRIPT_NAME%
echo [%DATE% %TIME%] DISABLED %SCRIPT_NAME%>>"%LOG_FILE%"
>"%STATUS_FILE%" echo status=DISABLED
>>"%STATUS_FILE%" echo script=%SCRIPT_NAME%
>>"%STATUS_FILE%" echo ts=!RUN_TS!
>>"%STATUS_FILE%" echo note=V4 scanner disabled intentionally

endlocal & exit /b 0
