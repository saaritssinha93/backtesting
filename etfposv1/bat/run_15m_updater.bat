@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ROOT=%%~fI
set LOG=%ROOT%\logs\live_15m_updater.log

call C:\Users\Saarit\anaconda3\Scripts\activate.bat fin
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
cd /d "%ROOT%"

echo ================================ >> "%LOG%"
echo [%DATE% %TIME%] START 15m updater >> "%LOG%"

python etf_eod_daily_weekly_scheduler_for_15mins_data.py >> "%LOG%" 2>&1

echo [%DATE% %TIME%] END 15m updater >> "%LOG%"
