@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ROOT=%%~fI
set LOG=%ROOT%\logs\auth.log

call C:\Users\Saarit\anaconda3\Scripts\activate.bat fin
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
cd /d "%ROOT%"

echo ================================ >> "%LOG%"
echo [%DATE% %TIME%] START auth >> "%LOG%"

python authentication.py >> "%LOG%" 2>&1

echo [%DATE% %TIME%] END auth >> "%LOG%"
