@echo off
setlocal
cd /d "%~dp0.."
python fno_v5_0926_all_history_day_pf_optimize.py %*
exit /b %errorlevel%
