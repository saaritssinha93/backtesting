@echo off
setlocal
cd /d "%~dp0.."

echo [Active strategy research] Building read-only execution diagnostics...
py -3.12 tools\v13_execution_research.py
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo [Active strategy research] Execution diagnostics FAILED with exit code %EXIT_CODE%.
  exit /b %EXIT_CODE%
)

echo [Active strategy research] Building read-only observability reports...
py -3.12 tools\v13_strategy_research.py %*
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo [Active strategy research] FAILED with exit code %EXIT_CODE%.
) else (
  echo [Active strategy research] Complete. No live trading configuration was changed.
)

exit /b %EXIT_CODE%
