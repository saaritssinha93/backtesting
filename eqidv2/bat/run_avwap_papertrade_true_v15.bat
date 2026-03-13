@echo off
setlocal EnableExtensions

set "THIS_DIR=%~dp0"
call "%THIS_DIR%run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat"
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
