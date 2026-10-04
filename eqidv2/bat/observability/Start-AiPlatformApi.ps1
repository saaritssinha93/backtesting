[CmdletBinding()]
param(
    [string]$RuntimeRoot = "C:\TradingData\eqidv2",
    [ValidateRange(1024, 65535)]
    [int]$Port = 8788,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$python = Join-Path $repoRoot ".venv-ai-platform\Scripts\python.exe"
$tokenPath = Join-Path $repoRoot "configs\observability\runtime\secrets\ai_platform_api_token.txt"

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "The API virtual environment is missing. Create .venv-ai-platform from ai_platform\requirements.lock."
}
if (-not (Test-Path -LiteralPath $tokenPath -PathType Leaf)) {
    throw "The API token file is missing. Run Start-Observability.ps1 first."
}

$token = [System.IO.File]::ReadAllText($tokenPath).Trim()
if ($token.Length -lt 32) {
    throw "The API token file is invalid."
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -First 1
if ($null -ne $listener) {
    Write-Host "AI platform API is already listening on 127.0.0.1:$Port (PID $($listener.OwningProcess))."
    exit 0
}

$env:AI_PLATFORM_API_TOKEN = $token
$env:EQIDV2_RUNTIME_ROOT = [System.IO.Path]::GetFullPath($RuntimeRoot)
$env:AI_PLATFORM_API_PORT = [string]$Port
$env:OTEL_SDK_DISABLED = "false"
$env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://127.0.0.1:4318/v1/traces"
$env:OTEL_EXPORTER_OTLP_TRACES_PROTOCOL = "http/protobuf"

if ($Foreground) {
    & $python -m ai_platform.api
    exit $LASTEXITCODE
}

$logRoot = Join-Path $env:EQIDV2_RUNTIME_ROOT "observability\logs"
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$stdout = Join-Path $logRoot "ai-platform-api-console.log"
$stderr = Join-Path $logRoot "ai-platform-api-console.err.log"
$process = Start-Process `
    -FilePath $python `
    -ArgumentList @("-m", "ai_platform.api") `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -PassThru

Write-Host "AI platform API started on 127.0.0.1:$Port (PID $($process.Id))."
Write-Host "Console logs: $stdout and $stderr"
