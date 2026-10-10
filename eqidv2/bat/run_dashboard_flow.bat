@echo off
setlocal
cd /d "%~dp0.."
python "tools\run_dashboard_flow.py" %*
if errorlevel 1 pause
endlocal
