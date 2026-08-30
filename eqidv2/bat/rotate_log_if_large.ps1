param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [long]$MaxBytes = 104857600
)

$ErrorActionPreference = "Stop"

if ($MaxBytes -lt 1) {
    throw "MaxBytes must be positive."
}

$target = [System.IO.Path]::GetFullPath($Path)
if (-not (Test-Path -LiteralPath $target -PathType Leaf)) {
    exit 0
}

$item = Get-Item -LiteralPath $target
if ([long]$item.Length -lt $MaxBytes) {
    exit 0
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$archive = "$target.$stamp.bak"
if (Test-Path -LiteralPath $archive) {
    $archive = "$target.$stamp.$PID.bak"
}

Move-Item -LiteralPath $target -Destination $archive
Write-Output "[INFO] Rotated oversized log: $target -> $archive ($($item.Length) bytes)"
