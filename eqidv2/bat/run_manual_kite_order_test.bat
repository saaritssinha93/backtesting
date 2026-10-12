@echo off
setlocal EnableExtensions DisableDelayedExpansion
set "EXIT_CODE=3"
set "WORKSPACE_PUSHED=0"
pushd "%~dp0.."
if errorlevel 1 goto finish
set "WORKSPACE_PUSHED=1"
set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" (
    where python >nul 2>&1
    if errorlevel 1 goto finish
    set "PYTHON_EXE=python"
)
if "%~1"=="" (
    "%PYTHON_EXE%" "tools\manual_kite_order_test.py" --help
) else (
    "%PYTHON_EXE%" "tools\manual_kite_order_test.py" %*
)
set "EXIT_CODE=%ERRORLEVEL%"
:finish
if "%WORKSPACE_PUSHED%"=="1" popd
if "%~1"=="" pause
endlocal & exit /b %EXIT_CODE%
