@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "RUNNER=%BASE_DIR%\bat\run_fno_oi_ema_confirm_0925_pf_v2.bat"
set "COMBO_SCRIPT=%BASE_DIR%\fno_oi_ema_confirm_0925_best_combo_v2.py"

echo Running V2 09:25 signal / 09:26 confirmation force-daily search...
call "%RUNNER%" --force-daily %*
if errorlevel 1 goto :failed

echo Running V2 09:25 signal / 09:26 confirmation filtered search...
call "%RUNNER%" %*
if errorlevel 1 goto :failed

echo Building V2 best trade-PF and best day-PF LONG plus SHORT portfolios...
"%PYTHON_EXE%" -u "%COMBO_SCRIPT%"
if errorlevel 1 goto :failed

echo Done.
endlocal & exit /b 0

:failed
echo Failed with exit code %ERRORLEVEL%.
endlocal & exit /b 1
