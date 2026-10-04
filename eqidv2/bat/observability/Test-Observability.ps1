[CmdletBinding()]
param(
    [switch]$SkipContainerValidation,
    [switch]$RequireRunning
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$perUserDockerBin = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin"
if (-not (Get-Command docker -ErrorAction SilentlyContinue) -and
    (Test-Path -LiteralPath (Join-Path $perUserDockerBin "docker.exe"))) {
    $env:Path = "$perUserDockerBin;$env:Path"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$configRoot = Join-Path $repoRoot "configs\observability"
$composeFile = Join-Path $configRoot "compose.yaml"

$jsonFiles = Get-ChildItem -LiteralPath (Join-Path $configRoot "grafana\dashboards") -Filter *.json -File
foreach ($jsonFile in $jsonFiles) {
    try { Get-Content -LiteralPath $jsonFile.FullName -Raw | ConvertFrom-Json | Out-Null }
    catch { throw "Invalid dashboard JSON: $($jsonFile.FullName): $($_.Exception.Message)" }
}
Write-Host "Validated $($jsonFiles.Count) Grafana dashboard JSON files."

if (-not $SkipContainerValidation) {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Docker CLI was not found. Use -SkipContainerValidation for static JSON checks only."
    }
    Push-Location $configRoot
    try {
        & docker compose -f $composeFile config --quiet
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose validation failed." }

        & docker compose -f $composeFile run --rm --no-deps --entrypoint promtool prometheus check rules /etc/prometheus/rules/recording-rules.yml /etc/prometheus/rules/trading-alerts.yml
        if ($LASTEXITCODE -ne 0) { throw "Prometheus rule validation failed." }

        & docker compose -f $composeFile run --rm --no-deps --entrypoint amtool alertmanager check-config /etc/alertmanager/alertmanager.yml
        if ($LASTEXITCODE -ne 0) { throw "Alertmanager config validation failed." }

        & docker compose -f $composeFile run --rm --no-deps --entrypoint /bin/alloy alloy fmt --test /etc/alloy/config.alloy
        if ($LASTEXITCODE -ne 0) { throw "Alloy configuration formatting/parse validation failed." }
    }
    finally {
        Pop-Location
    }
}

$healthChecks = [ordered]@{
    Grafana      = "http://127.0.0.1:3000/api/health"
    Prometheus   = "http://127.0.0.1:9090/-/ready"
    Alertmanager = "http://127.0.0.1:9093/-/ready"
    Loki         = "http://127.0.0.1:3100/ready"
    Tempo        = "http://127.0.0.1:3200/ready"
    Alloy        = "http://127.0.0.1:12345/-/ready"
}

$failed = @()
foreach ($item in $healthChecks.GetEnumerator()) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $item.Value -TimeoutSec 3
        if ($response.StatusCode -ge 400) { $failed += $item.Key }
        else { Write-Host "READY $($item.Key) $($item.Value)" }
    }
    catch {
        $failed += $item.Key
        Write-Host "DOWN  $($item.Key) $($item.Value)"
    }
}

if ($RequireRunning -and $failed.Count -gt 0) {
    throw "Required running services were unavailable: $($failed -join ', ')"
}

if ($failed.Count -eq 0) { Write-Host "All runtime endpoints are ready." }
else { Write-Host "Static validation passed; runtime services not ready: $($failed -join ', ')." }
