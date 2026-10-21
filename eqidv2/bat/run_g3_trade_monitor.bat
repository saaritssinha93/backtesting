@echo off
setlocal
cd /d "%~dp0.."
powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0run_g3_monitor.ps1" -Mode Monitor %*
exit /b %ERRORLEVEL%
