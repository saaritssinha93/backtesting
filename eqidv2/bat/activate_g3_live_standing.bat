@echo off
setlocal
cd /d "%~dp0.."
"%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -u tools\g3_live_standing.py %*
exit /b %ERRORLEVEL%
