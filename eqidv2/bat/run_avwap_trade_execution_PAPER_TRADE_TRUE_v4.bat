@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "LOG_DIR=%BASE_DIR%\logs"
set "SCRIPT_NAME=avwap_trade_execution_PAPER_TRADE_TRUE_v4.py"

for /f %%a in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%a"
if not defined TODAY_IST set "TODAY_IST=%DATE%"
set "LOG_FILE=%LOG_DIR%\avwap_trade_execution_PAPER_TRADE_TRUE_v4_%TODAY_IST%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [%DATE% %TIME%] DISABLED %SCRIPT_NAME%
echo [%DATE% %TIME%] DISABLED %SCRIPT_NAME%>>"%LOG_FILE%"
echo [INFO] V4 papertrade disabled intentionally.>>"%LOG_FILE%"

endlocal & exit /b 0
