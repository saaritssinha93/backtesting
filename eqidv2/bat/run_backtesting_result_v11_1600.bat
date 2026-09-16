@echo off
rem Compatibility entry point: the canonical G daily session owns execution.
call "%~dp0run_backtesting_result_v13_v10_g_1620.bat" %*
exit /b %ERRORLEVEL%
