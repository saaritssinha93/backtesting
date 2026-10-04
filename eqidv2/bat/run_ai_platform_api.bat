@echo off
setlocal
cd /d "%~dp0.."

if "%AI_PLATFORM_API_TOKEN%"=="" (
  echo [ERROR] AI_PLATFORM_API_TOKEN must be set in the process environment.
  exit /b 2
)

if not exist ".venv-ai-platform\Scripts\python.exe" (
  echo [ERROR] Missing .venv-ai-platform. See docs\ai_platform\STAGE2_API.md.
  exit /b 3
)

if "%EQIDV2_RUNTIME_ROOT%"=="" set "EQIDV2_RUNTIME_ROOT=C:\TradingData\eqidv2"
if "%AI_PLATFORM_API_PORT%"=="" set "AI_PLATFORM_API_PORT=8788"
if "%OTEL_EXPORTER_OTLP_TRACES_ENDPOINT%"=="" set "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces"
if "%OTEL_EXPORTER_OTLP_TRACES_PROTOCOL%"=="" set "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf"

".venv-ai-platform\Scripts\python.exe" -m ai_platform.api
exit /b %ERRORLEVEL%
