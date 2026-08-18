@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "RUNNER=%BASE_DIR%\bat\run_fno_oi_ema_confirm_0925_0930_pf_v3.bat"
set "COMBO_SCRIPT=%BASE_DIR%\fno_oi_ema_confirm_0925_0930_best_combo_v3.py"

echo Running V3 09:30/09:31 add-on force-daily search, max two per side...
call "%RUNNER%" --force-daily %*
if errorlevel 1 goto :failed

echo Running V3 09:30/09:31 add-on filtered search, max two per side...
call "%RUNNER%" %*
if errorlevel 1 goto :failed

echo Stacking the 09:31 add-on onto the locked V2 MORE_SHORT_2X_HIGH_PF baseline...
"%PYTHON_EXE%" -u "%COMBO_SCRIPT%"
if errorlevel 1 goto :failed

echo Done.
endlocal & exit /b 0

:failed
echo Failed with exit code %ERRORLEVEL%.
endlocal & exit /b 1
