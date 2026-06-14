@echo off
setlocal

set "REPO_ROOT=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "LOG_DIR=%REPO_ROOT%\logs"
set "RUN_LOG=%LOG_DIR%\v7_causality_audit.log"
set "EQIDV2_V7_CAUSALITY_DATA_ROOT=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%REPO_ROOT%"

echo [%DATE% %TIME%] START v7_causality_audit.py >>"%RUN_LOG%"
"%PYTHON_EXE%" -u v7_causality_audit.py --data-root "%EQIDV2_V7_CAUSALITY_DATA_ROOT%" %* >>"%RUN_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%DATE% %TIME%] END v7_causality_audit.py (exit=%EXIT_CODE%) >>"%RUN_LOG%"

exit /b %EXIT_CODE%
