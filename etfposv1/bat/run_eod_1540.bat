@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ROOT=%%~fI
set LOG=%ROOT%\logs\eod_daily_weekly_1540.log

call C:\Users\Saarit\anaconda3\Scripts\activate.bat fin
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
cd /d "%ROOT%"

echo ================================ >> "%LOG%"
echo [%DATE% %TIME%] START EOD 1540 >> "%LOG%"

python etf_eod_daily_weekly_scheduler_for_daily_1540_update.py >> "%LOG%" 2>&1

echo [%DATE% %TIME%] END EOD 1540 >> "%LOG%"
