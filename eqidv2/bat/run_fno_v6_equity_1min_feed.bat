@echo off
rem Compatibility entry point; the canonical G launcher owns all runtime checks.
call "%~dp0run_fno_v13_v10_g_equity_1min_feed.bat" %*
exit /b %ERRORLEVEL%
