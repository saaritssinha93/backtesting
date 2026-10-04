[CmdletBinding()]
param(
    [string]$RuntimeRoot = $(if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { "C:\TradingData\eqidv2" }),
    [string]$SessionDate = "",
    [double]$StatusMaxAgeSeconds = 30.0,
    [double]$ReconciliationMaxAgeSeconds = 120.0,
    [double]$PipelineMaxAgeSeconds = 120.0,
    [double]$MarketDataMaxAgeSeconds = 420.0,
    [string]$PythonExe = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonCommand) { throw "Python was not found at the configured path or on PATH." }
    $PythonExe = $pythonCommand.Source
}

$arguments = @(
    "-m", "ai_platform.observability.live_trust",
    "--runtime-root", $RuntimeRoot,
    "--status-max-age-seconds", $StatusMaxAgeSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--reconciliation-max-age-seconds", $ReconciliationMaxAgeSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--pipeline-max-age-seconds", $PipelineMaxAgeSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--market-data-max-age-seconds", $MarketDataMaxAgeSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)
)
if ($SessionDate) { $arguments += @("--session-date", $SessionDate) }

Push-Location $repoRoot
try {
    & $PythonExe @arguments
    $exitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $exitCode
