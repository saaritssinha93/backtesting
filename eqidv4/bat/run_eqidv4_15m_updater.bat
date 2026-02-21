@echo off
set ROOT=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\main\backtesting\eqidv4
set LOG=%ROOT%\logs\eqidv4_live_15m_updater.log

call C:\Users\Saarit\anaconda3\Scripts\activate.bat fin
cd /d %ROOT%

echo ================================ >> %LOG%
echo [%DATE% %TIME%] START eqidv4 15m updater >> %LOG%

python eqidv4_eod_scheduler_for_15mins_data.py >> %LOG% 2>&1

echo [%DATE% %TIME%] END eqidv4 15m updater >> %LOG%
