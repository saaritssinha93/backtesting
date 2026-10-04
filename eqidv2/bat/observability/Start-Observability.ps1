[CmdletBinding()]
param(
    [ValidateRange(30, 600)]
    [int]$WaitSeconds = 180,
    [switch]$SkipPull
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
$secretRoot = Join-Path $configRoot "runtime\secrets"
$grafanaSecret = Join-Path $secretRoot "grafana_admin_password.txt"
$apiTokenSecret = Join-Path $secretRoot "ai_platform_api_token.txt"

function New-UrlSafeRandomToken {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(32, 128)]
        [int]$ByteCount
    )

    # RandomNumberGenerator.Fill is unavailable in Windows PowerShell 5.1 on
    # older .NET Framework builds.  Create/GetBytes is cryptographically
    # equivalent and works in both Windows PowerShell and modern PowerShell.
    $randomBytes = New-Object byte[] $ByteCount
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($randomBytes)
    }
    finally {
        $generator.Dispose()
    }
    return [Convert]::ToBase64String($randomBytes).Replace("/", "_").Replace("+", "-").TrimEnd("=")
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI was not found. Install/start Docker Desktop, then rerun this script."
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker is installed but its engine is not available. Start Docker Desktop."
}

New-Item -ItemType Directory -Path $secretRoot -Force | Out-Null
if (-not (Test-Path -LiteralPath $grafanaSecret)) {
    $password = New-UrlSafeRandomToken -ByteCount 32
    [System.IO.File]::WriteAllText($grafanaSecret, $password, [System.Text.UTF8Encoding]::new($false))
    if ($env:OS -eq "Windows_NT") {
        & icacls.exe $grafanaSecret /inheritance:r /grant:r "$($env:USERNAME):(R,W)" *> $null
    }
    $password = $null
}
if (-not [string]::IsNullOrWhiteSpace($env:AI_PLATFORM_API_TOKEN)) {
    $apiToken = $env:AI_PLATFORM_API_TOKEN.Trim()
    if ($apiToken.Length -lt 32) { throw "AI_PLATFORM_API_TOKEN must contain at least 32 characters." }
    $storedToken = if (Test-Path -LiteralPath $apiTokenSecret) { [System.IO.File]::ReadAllText($apiTokenSecret) } else { "" }
    $writeApiToken = $storedToken -cne $apiToken
}
elseif (-not (Test-Path -LiteralPath $apiTokenSecret)) {
    $apiToken = New-UrlSafeRandomToken -ByteCount 48
    $writeApiToken = $true
}
else {
    $writeApiToken = $false
}
if ($writeApiToken) {
    [System.IO.File]::WriteAllText($apiTokenSecret, $apiToken, [System.Text.UTF8Encoding]::new($false))
    if ($env:OS -eq "Windows_NT") {
        & icacls.exe $apiTokenSecret /inheritance:r /grant:r "$($env:USERNAME):(R,W)" *> $null
    }
    $apiToken = $null
}

Push-Location $configRoot
try {
    & docker compose -f $composeFile config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose validation failed." }

    if (-not $SkipPull) {
        & docker compose -f $composeFile pull
        if ($LASTEXITCODE -ne 0) { throw "One or more observability images could not be pulled." }
    }

    & docker compose -f $composeFile up -d --remove-orphans
    if ($LASTEXITCODE -ne 0) { throw "Observability stack startup failed." }
}
finally {
    Pop-Location
}

$healthChecks = [ordered]@{
    Grafana      = "http://127.0.0.1:3000/api/health"
    Prometheus   = "http://127.0.0.1:9090/-/ready"
    Alertmanager = "http://127.0.0.1:9093/-/ready"
    Loki         = "http://127.0.0.1:3100/ready"
    Tempo        = "http://127.0.0.1:3200/ready"
    Alloy        = "http://127.0.0.1:12345/-/ready"
}

$deadline = [DateTimeOffset]::UtcNow.AddSeconds($WaitSeconds)
$pending = [System.Collections.Generic.HashSet[string]]::new([string[]]$healthChecks.Keys)
while ($pending.Count -gt 0 -and [DateTimeOffset]::UtcNow -lt $deadline) {
    foreach ($name in @($pending)) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $healthChecks[$name] -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
                $pending.Remove($name) | Out-Null
            }
        }
        catch { }
    }
    if ($pending.Count -gt 0) { Start-Sleep -Seconds 2 }
}

if ($pending.Count -gt 0) {
    throw "Stack started but these endpoints were not ready: $($pending -join ', '). Run docker compose logs from $configRoot."
}

Write-Host "Observability stack is ready."
Write-Host "Grafana:      http://127.0.0.1:3000"
Write-Host "Prometheus:   http://127.0.0.1:9090"
Write-Host "Alertmanager: http://127.0.0.1:9093"
Write-Host "Alloy UI:     http://127.0.0.1:12345"
Write-Host "Grafana user: eqidv2 (or GRAFANA_ADMIN_USER from .env)"
Write-Host "Grafana password is stored locally at: $grafanaSecret"
Write-Host "AI API/metrics token is stored locally at: $apiTokenSecret"
Write-Host "The secret value was intentionally not printed."
