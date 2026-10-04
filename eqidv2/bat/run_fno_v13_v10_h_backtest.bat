@echo off
setlocal
pushd "%~dp0.."
py -3.12 fno_v13_v10_h_backtest.py %*
set "H_RESEARCH_EXIT=%ERRORLEVEL%"
popd
exit /b %H_RESEARCH_EXIT%
