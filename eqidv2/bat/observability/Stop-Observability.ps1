[CmdletBinding()]
param(
    [switch]$PurgeData,
    [string]$ConfirmPurge = ""
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

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI was not found."
}

Push-Location $configRoot
try {
    if ($PurgeData) {
        if ($ConfirmPurge -cne "PURGE_EQIDV2_OBSERVABILITY") {
            throw "Volume deletion requires -ConfirmPurge PURGE_EQIDV2_OBSERVABILITY. This permanently removes local metrics, logs, traces, dashboards and alert state."
        }
        & docker compose -f $composeFile down --volumes --remove-orphans
    }
    else {
        & docker compose -f $composeFile down --remove-orphans
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose shutdown failed." }
}
finally {
    Pop-Location
}

if ($PurgeData) {
    Write-Host "Observability containers and named data volumes were removed. Application evidence was not touched."
}
else {
    Write-Host "Observability containers stopped. Named data volumes were preserved."
}
