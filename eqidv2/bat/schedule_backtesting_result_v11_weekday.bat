@echo off
rem Compatibility installer: only the canonical G daily task is installed.
call "%~dp0schedule_backtesting_result_v13_v10_g_weekday.bat" %*
exit /b %ERRORLEVEL%
