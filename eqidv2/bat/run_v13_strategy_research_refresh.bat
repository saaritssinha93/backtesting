@echo off
setlocal
cd /d "%~dp0.."

echo [V13-V10-G research] Building path-aware execution diagnostics...
py -3.12 tools\v13_execution_research.py
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo [V13-V10-G research] Execution diagnostics FAILED with exit code %EXIT_CODE%.
  exit /b %EXIT_CODE%
)

echo [V13-V10-G research] Building read-only observability reports...
py -3.12 tools\v13_strategy_research.py %*
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo [V13-V10-G research] FAILED with exit code %EXIT_CODE%.
) else (
  echo [V13-V10-G research] Complete. No live trading configuration was changed.
)

exit /b %EXIT_CODE%
