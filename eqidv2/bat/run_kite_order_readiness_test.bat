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
    if errorlevel 1 (
        echo [ERROR] Python was not found.
        goto finish
    )
    set "PYTHON_EXE=python"
)
if not exist "tools\test_kite_order_readiness.py" (
    echo [ERROR] Missing tools\test_kite_order_readiness.py.
    goto finish
)
echo [Kite readiness] Read-only broker checks and optional margin preview. No order submission.
"%PYTHON_EXE%" "tools\test_kite_order_readiness.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
:finish
if "%WORKSPACE_PUSHED%"=="1" popd
if "%~1"=="" pause
endlocal & exit /b %EXIT_CODE%
