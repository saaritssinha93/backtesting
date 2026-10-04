[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet("collector-outage", "prometheus-restart", "loki-restart", "tempo-restart")]
    [string]$Drill,
    [ValidateRange(5, 120)]
    [int]$DurationSeconds = 30,
    [switch]$Execute,
    [string]$AcknowledgePaperOrMaintenance = "",
    [string]$ApplicationHealthUri = "http://127.0.0.1:8788/api/v1/health/live"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$perUserDockerBin = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin"
if (-not (Get-Command docker -ErrorAction SilentlyContinue) -and
    (Test-Path -LiteralPath (Join-Path $perUserDockerBin "docker.exe"))) {
    $env:Path = "$perUserDockerBin;$env:Path"
}

$serviceByDrill = @{
    "collector-outage"   = "alloy"
    "prometheus-restart" = "prometheus"
    "loki-restart"       = "loki"
    "tempo-restart"      = "tempo"
}
$service = $serviceByDrill[$Drill]

if (-not $Execute) {
    Write-Host "DRY RUN: would stop '$service' for $DurationSeconds seconds, verify the application health endpoint, then restore it."
    Write-Host "No process or container was changed. Add -Execute -AcknowledgePaperOrMaintenance PAPER_OR_MAINTENANCE in a paper or maintenance window."
    exit 0
}

if ($AcknowledgePaperOrMaintenance -cne "PAPER_OR_MAINTENANCE") {
    throw "Failure drills are blocked unless you explicitly pass -AcknowledgePaperOrMaintenance PAPER_OR_MAINTENANCE. Never run this during an unattended live session."
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker CLI was not found." }

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$configRoot = Join-Path $repoRoot "configs\observability"
$composeFile = Join-Path $configRoot "compose.yaml"
$startedAt = [DateTimeOffset]::UtcNow
$beforeHealthy = $false
try {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $ApplicationHealthUri -TimeoutSec 5
    $beforeHealthy = $response.StatusCode -ge 200 -and $response.StatusCode -lt 400
}
catch { }

Push-Location $configRoot
try {
    & docker compose -f $composeFile stop -t 10 $service
    if ($LASTEXITCODE -ne 0) { throw "Could not stop $service." }
    Start-Sleep -Seconds $DurationSeconds

    $duringHealthy = $false
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $ApplicationHealthUri -TimeoutSec 5
        $duringHealthy = $response.StatusCode -ge 200 -and $response.StatusCode -lt 400
    }
    catch { }

    if ($beforeHealthy -and -not $duringHealthy) {
        throw "Application health failed during the telemetry drill. Telemetry isolation acceptance did not pass."
    }
}
finally {
    & docker compose -f $composeFile start $service
    $restartExit = $LASTEXITCODE
    Pop-Location
    if ($restartExit -ne 0) { Write-Error "The drill could not restore $service; restore it manually now." }
}

$result = [ordered]@{
    schema_version = "eqidv2.observability.failure_drill.v1"
    drill = $Drill
    service = $service
    started_at = $startedAt.ToString("o")
    completed_at = [DateTimeOffset]::UtcNow.ToString("o")
    duration_seconds = $DurationSeconds
    application_healthy_before = $beforeHealthy
    application_healthy_during = $duringHealthy
    result = if ((-not $beforeHealthy) -or $duringHealthy) { "PASS_WITH_APPLICATION_PROBE" } else { "FAIL" }
    limitation = if (-not $beforeHealthy) { "Application was not healthy/reachable before the drill; telemetry recovery only was exercised." } else { $null }
}
$result | ConvertTo-Json -Depth 4
