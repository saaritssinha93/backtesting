@echo off
REM ============================================================================
REM Post-close ONLY. Deep/aggressive multi-iteration PF>2 search over the conf
REM setups (premom-40 universe, deeper gates, finer grid, multi-start, maxpf-robust).
REM Self-validates on one small setup first; aborts the full sweep if that fails.
REM Writes per-setup proposals + _summary.json under Train_and_Test\aggressive_pf_proposals.
REM Review only — nothing is written to final_setup_conf.py.
REM ============================================================================
setlocal EnableExtensions
set "BASE=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PY=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PY%" set "PY=python"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
set "ENGINE=%BASE%\Train_and_Test\aggressive_pf_tuner.py"
set "LOG=%BASE%\logs\aggressive_pf_tuner_latest.log"
cd /d "%BASE%"

echo [%DATE% %TIME%] START aggressive_pf_tuner>"%LOG%"
echo [%DATE% %TIME%] validation (G_LOWER_LOW_BREAK, restarts=1)...>>"%LOG%"
"%PY%" -u "%ENGINE%" --setups G_LOWER_LOW_BREAK --restarts 1 --max-secs-per-setup 300 >>"%LOG%" 2>&1
if not "%ERRORLEVEL%"=="0" (
    echo [%DATE% %TIME%] ABORT validation failed exit=%ERRORLEVEL%; full sweep skipped>>"%LOG%"
    endlocal & exit /b 1
)
echo [%DATE% %TIME%] validation OK; running full 16-setup sweep...>>"%LOG%"
"%PY%" -u "%ENGINE%" %* >>"%LOG%" 2>&1
echo [%DATE% %TIME%] END aggressive_pf_tuner exit=%ERRORLEVEL%>>"%LOG%"
endlocal
