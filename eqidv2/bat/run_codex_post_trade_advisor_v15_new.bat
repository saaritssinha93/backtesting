@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "APP_ROOT=%%~fI"

set "PYTHON_EXE=python"
set "SCRIPT_PATH=%APP_ROOT%\codex_post_trade_advisor_v15_new.py"
set "LOG_DIR=%APP_ROOT%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd')"') do set "TODAY_IST=%%I"
set "LOG_FILE=%LOG_DIR%\codex_post_trade_advisor_v15_new_%TODAY_IST%.log"

echo [RUN] Codex post-trade advisor v15_new > "%LOG_FILE%"
echo [RUN] date=%TODAY_IST% >> "%LOG_FILE%"
echo [RUN] script=%SCRIPT_PATH% >> "%LOG_FILE%"

"%PYTHON_EXE%" "%SCRIPT_PATH%" --end-date "%TODAY_IST%" --days 5 >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"

echo [RUN] exit_code=%RC% >> "%LOG_FILE%"
type "%LOG_FILE%"
exit /b %RC%
