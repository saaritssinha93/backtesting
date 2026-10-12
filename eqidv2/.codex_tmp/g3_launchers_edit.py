from pathlib import Path

bat = Path('bat')
for src in bat.glob('run_fno_v13_v10_g_*.bat'):
    if src.name.endswith('live_kite_qty1.bat'):
        continue
    dest = bat / src.name.replace('v13_v10_g', 'v13_v10_g_3')
    value = src.read_text().replace('v13_v10_g', 'v13_v10_g_3').replace('V13_V10_G', 'V13_V10_G_3').replace('V13-V10-G', 'V13-V10-G-3')
    value = value.replace('set "BASE_DIR=C:\\Users\\Saarit\\OneDrive\\Desktop\\Trading\\backtesting\\eqidv2\\backtesting\\eqidv2"', 'for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"')
    dest.write_text(value, encoding='utf-8', newline='')

# Retain the supervisor's restart/heartbeat/position safeguards in the explicit
# activation launcher. The normal scheduled entry point remains readiness only.
src = bat / 'run_fno_v13_v10_g_live_kite_qty1.bat'
value = src.read_text().replace('v13_v10_g', 'v13_v10_g_3').replace('V13_V10_G', 'V13_V10_G_3').replace('V13-V10-G', 'V13-V10-G-3')
value = value.replace('set "FNO_V6_LIVE_ACK=I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"', 'rem The explicit G3 acknowledgement must be supplied by the separately approved operator; this launcher never sets it.')
value = value.replace('"--auto-arm"', '"--activate-live"')
value = value.replace('set "BASE_DIR=C:\\Users\\Saarit\\OneDrive\\Desktop\\Trading\\backtesting\\eqidv2\\backtesting\\eqidv2"', 'for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"')
(bat / 'activate_fno_v13_v10_g_3_live_kite_qty1.bat').write_text(value, encoding='utf-8', newline='')

prefix = '''@echo off
setlocal EnableExtensions DisableDelayedExpansion
for %%I in ("%~dp0..") do set "BASE_DIR=%%~fI"
set "PYTHON_EXE=C:\\Users\\Saarit\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
set "EQIDV2_RUNTIME_ROOT=C:\\TradingData\\eqidv2"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"
cd /d "%BASE_DIR%"
'''
(bat / 'run_fno_v13_v10_g_3_live_kite_qty1.bat').write_text(prefix + '''rem Scheduled G3 quantity-one session is deliberately readiness-only.
rem No acknowledgement, arm record, LIVE worker, or broker request is created.
if not "%~1"=="" endlocal & exit /b 2
"%PYTHON_EXE%" -u "%BASE_DIR%\\fno_v13_v10_g_3_live_kite_session.py" --readiness-only
endlocal & exit /b %ERRORLEVEL%
''', encoding='utf-8', newline='')
(bat / 'run_backtesting_result_v13_v10_g_3_1620.bat').write_text(prefix + '''rem Uses the production G3 daily runner, never the archived historical CLI.
"%PYTHON_EXE%" -u "%BASE_DIR%\\backtesting_result_v13_v10_g_3_daily.py" --wait-for-data %*
endlocal & exit /b %ERRORLEVEL%
''', encoding='utf-8', newline='')
