@echo off
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0.."
set "PYTHON_EXE=C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
rem Default is a read-only plan. Explicit apply restores scheduled LIVE authorization.
"%PYTHON_EXE%" -u tools\repair_g3_standing_schedule.py %*
endlocal & exit /b %ERRORLEVEL%
