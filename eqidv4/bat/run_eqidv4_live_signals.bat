@echo off
set ROOT=C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\main\backtesting\eqidv4
set LOG=%ROOT%\logs\eqidv4_live_signals_15m.log

call C:\Users\Saarit\anaconda3\Scripts\activate.bat fin
cd /d %ROOT%

echo ================================ >> %LOG%
echo [%DATE% %TIME%] START eqidv4 ORB live signals >> %LOG%

python eqidv4_orb_vwap_rvol_live.py --run-once --require-two-close-confirm >> %LOG% 2>&1

echo [%DATE% %TIME%] END eqidv4 ORB live signals >> %LOG%
