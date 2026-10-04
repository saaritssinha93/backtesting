[CmdletBinding()]
param(
    [string]$LogRoot = "",
    [string]$RuntimeRoot = "",
    [ValidateRange(7, 3650)]
    [int]$KeepDays = 90,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($RuntimeRoot)) {
    $RuntimeRoot = if ([string]::IsNullOrWhiteSpace($env:EQIDV2_RUNTIME_ROOT)) {
        "C:\TradingData\eqidv2"
    } else {
        $env:EQIDV2_RUNTIME_ROOT
    }
}
$allowedRoot = [IO.Path]::GetFullPath(
    (Join-Path $RuntimeRoot "observability\logs")
).TrimEnd('\')
if ([string]::IsNullOrWhiteSpace($LogRoot)) { $LogRoot = $allowedRoot }
$resolvedTarget = [IO.Path]::GetFullPath($LogRoot).TrimEnd('\')

if (
    -not $resolvedTarget.Equals($allowedRoot, [StringComparison]::OrdinalIgnoreCase) -and
    -not $resolvedTarget.StartsWith($allowedRoot + '\', [StringComparison]::OrdinalIgnoreCase)
) {
    throw "Retention target must be the telemetry logs directory $allowedRoot or one of its children."
}

if (-not (Test-Path -LiteralPath $resolvedTarget -PathType Container)) {
    Write-Host "Telemetry log root does not exist; nothing to retain: $resolvedTarget"
    exit 0
}

$targetItem = Get-Item -LiteralPath $resolvedTarget -Force
if (($targetItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw "Retention target may not be a reparse point: $resolvedTarget"
}

function Get-SafeTelemetryFiles {
    param([Parameter(Mandatory)][string]$Root)

    $pending = [Collections.Generic.Stack[string]]::new()
    $pending.Push($Root)
    while ($pending.Count -gt 0) {
        $current = $pending.Pop()
        foreach ($item in @(Get-ChildItem -LiteralPath $current -Force -ErrorAction Stop)) {
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Retention refuses to traverse a reparse point: $($item.FullName)"
            }
            if ($item.PSIsContainer) {
                $pending.Push($item.FullName)
            }
            else {
                $item
            }
        }
    }
}

$cutoff = (Get-Date).ToUniversalTime().AddDays(-$KeepDays)
$candidates = @(
    Get-SafeTelemetryFiles -Root $resolvedTarget |
    Where-Object {
        $_.LastWriteTimeUtc -lt $cutoff -and
        $_.Name -match '(?i)(\.(jsonl|log)(\.\d+)?$|\.gz$)' -and
        $_.FullName -notmatch '(?i)(evidence|manifest|order[_-]?journal|journals|feature[_-]?ledger|experiment|raw[_-]?observation|reconciliation)'
    }
)

$measurement = $candidates | Measure-Object -Property Length -Sum
$bytes = if ($null -eq $measurement) { 0 } elseif ($null -eq $measurement.Sum) { 0 } else { $measurement.Sum }
Write-Host "$($candidates.Count) eligible telemetry log files ($bytes bytes) are older than $KeepDays days."

if (-not $Execute) {
    $candidates | Select-Object FullName, LastWriteTimeUtc, Length | Format-Table -AutoSize
    Write-Host "DRY RUN only. Re-run with -Execute after reviewing the exact paths."
    exit 0
}

foreach ($candidate in $candidates) {
    $resolvedFile = [IO.Path]::GetFullPath($candidate.FullName)
    if (-not $resolvedFile.StartsWith($resolvedTarget + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing out-of-scope path: $resolvedFile"
    }
    $currentItem = Get-Item -LiteralPath $resolvedFile -Force
    if (($currentItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Refusing reparse-point file: $resolvedFile"
    }
    Remove-Item -LiteralPath $resolvedFile -Force
}
Write-Host "Removed $($candidates.Count) expired telemetry log files. Immutable trading evidence was excluded."
