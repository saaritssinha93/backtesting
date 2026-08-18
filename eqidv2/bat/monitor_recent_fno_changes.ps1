param(
    [string]$BaseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2",
    [string]$RuntimeRoot = "C:\TradingData\eqidv2\fno_oi",
    [string]$LogFile = "",
    [int]$DebounceMs = 750
)

$ErrorActionPreference = "Stop"
if (-not $LogFile) {
    $LogFile = Join-Path $BaseDir "logs\recent_fno_changes.log"
}
$logDir = Split-Path -Parent $LogFile
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

$global:FnoRecentMonitorLastSeen = @{}

function Write-MonitorLog {
    param([string]$Message)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff zzz"
    Add-Content -LiteralPath $LogFile -Value "$stamp`t$Message"
}

function Get-FileSummary {
    param([string]$Path)
    if (Test-Path -LiteralPath $Path -PathType Leaf) {
        $item = Get-Item -LiteralPath $Path -ErrorAction SilentlyContinue
        if ($item) {
            return "size=$($item.Length) mtime=$($item.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))"
        }
    }
    return "missing"
}

function Watch-Path {
    param(
        [string]$Name,
        [string]$Path,
        [string]$Filter,
        [bool]$IncludeSubdirectories
    )
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        Write-MonitorLog "SKIP`t$Name`t$Path does not exist"
        return $null
    }

    $watcher = [System.IO.FileSystemWatcher]::new($Path, $Filter)
    $watcher.IncludeSubdirectories = $IncludeSubdirectories
    $watcher.NotifyFilter = [System.IO.NotifyFilters]'FileName, LastWrite, Size, CreationTime'
    $watcher.EnableRaisingEvents = $true

    $action = {
        $eventType = $Event.SourceEventArgs.ChangeType.ToString()
        $fullPath = $Event.SourceEventArgs.FullPath
        if ($fullPath -match '\\(__pycache__|logs)\\') { return }

        $key = "$eventType|$fullPath"
        $now = Get-Date
        if ($global:FnoRecentMonitorLastSeen.ContainsKey($key)) {
            $elapsed = ($now - $global:FnoRecentMonitorLastSeen[$key]).TotalMilliseconds
            if ($elapsed -lt $Event.MessageData.DebounceMs) { return }
        }
        $global:FnoRecentMonitorLastSeen[$key] = $now

        if (Test-Path -LiteralPath $fullPath -PathType Leaf) {
            $item = Get-Item -LiteralPath $fullPath -ErrorAction SilentlyContinue
            if ($item) {
                $summary = "size=$($item.Length) mtime=$($item.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))"
            } else {
                $summary = "missing"
            }
        } else {
            $summary = "missing"
        }
        $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff zzz"
        Add-Content -LiteralPath $Event.MessageData.LogFile -Value "$stamp`t$($Event.MessageData.Name)`t$eventType`t$fullPath`t$summary"
    }

    foreach ($evt in @("Changed", "Created", "Deleted", "Renamed")) {
        Register-ObjectEvent -InputObject $watcher -EventName $evt -SourceIdentifier "fno-$Name-$evt" -MessageData @{
            Name = $Name
            LogFile = $LogFile
            DebounceMs = $DebounceMs
        } -Action $action | Out-Null
    }
    Write-MonitorLog "WATCH`t$Name`t$Path`tfilter=$Filter recursive=$IncludeSubdirectories"
    return $watcher
}

Write-MonitorLog "START`tpid=$PID"

$watchers = @()
$watchers += Watch-Path -Name "repo" -Path $BaseDir -Filter "*fno_oi*" -IncludeSubdirectories $true
$watchers += Watch-Path -Name "latest" -Path (Join-Path $RuntimeRoot "latest") -Filter "*" -IncludeSubdirectories $false
$watchers += Watch-Path -Name "strategy_research" -Path (Join-Path $RuntimeRoot "strategy_research") -Filter "*" -IncludeSubdirectories $true
$watchers = @($watchers | Where-Object { $_ -ne $null })

try {
    while ($true) {
        Wait-Event -Timeout 5 | Out-Null
    }
}
finally {
    foreach ($sub in Get-EventSubscriber | Where-Object { $_.SourceIdentifier -like "fno-*" }) {
        Unregister-Event -SubscriptionId $sub.SubscriptionId -ErrorAction SilentlyContinue
    }
    foreach ($watcher in $watchers) {
        $watcher.EnableRaisingEvents = $false
        $watcher.Dispose()
    }
    Write-MonitorLog "STOP`tpid=$PID"
}
