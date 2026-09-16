@echo off
rem Compatibility entry point; the canonical G launcher owns all runtime checks.
call "%~dp0run_fno_v13_v10_g_live_short.bat" %*
exit /b %ERRORLEVEL%
