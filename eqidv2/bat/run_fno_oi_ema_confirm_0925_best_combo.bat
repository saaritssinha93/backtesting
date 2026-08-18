@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "RUNNER=%BASE_DIR%\bat\run_fno_oi_ema_confirm_0925_pf.bat"
set "COMBO_SCRIPT=%BASE_DIR%\fno_oi_ema_confirm_0925_best_combo.py"

echo Running 09:25 signal / 09:26 confirmation daily-coverage search...
call "%RUNNER%" --force-daily --top-n 50
if errorlevel 1 goto :failed

echo Running 09:25 signal / 09:26 confirmation filtered search...
call "%RUNNER%" --top-n 50
if errorlevel 1 goto :failed

echo Building best LONG plus best SHORT day-wise report...
"%PYTHON_EXE%" -u "%COMBO_SCRIPT%"
if errorlevel 1 goto :failed

echo Done.
endlocal & exit /b 0

:failed
echo Failed with exit code %ERRORLEVEL%.
endlocal & exit /b 1
