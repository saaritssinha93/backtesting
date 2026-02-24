[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$FilePath,
    [string[]]$ArgumentList = @(),
    [string]$WorkDir = (Get-Location).Path,
    [Parameter(Mandatory = $true)][string]$LogFile,
    [Parameter(Mandatory = $true)][string]$StatusFile,
    [Parameter(Mandatory = $true)][string]$HeartbeatFile,
    [int]$MaxRestarts = 20,
    [int]$RestartDelaySec = 15,
    [int]$MonitorIntervalSec = 5,
    [int]$HungTimeoutSec = 900,
    [int]$CooldownWindowSec = 300,
    [int]$CooldownMaxRestarts = 6,
    [int]$CooldownDelaySec = 120,
    [string]$CutoffHHmm = "",
    [switch]$SkipRunAfterCutoff,
    [switch]$StopRestartsAfterCutoff
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:SupervisorLogFile = ""

function Ensure-ParentDir {
    param([Parameter(Mandatory = $true)][string]$Path)
    $parent = Split-Path -Path $Path -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
}

function Write-KeyFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Data
    )
    Ensure-ParentDir -Path $Path
    $lines = @()
    foreach ($entry in ($Data.GetEnumerator() | Sort-Object Name)) {
        $lines += ("{0}={1}" -f $entry.Key, [string]$entry.Value)
    }
    Set-Content -Path $Path -Value $lines -Encoding UTF8
}

function Write-LogLine {
    param(
        [Parameter(Mandatory = $true)][string]$Level,
        [Parameter(Mandatory = $true)][string]$Message
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "{0} | {1} | {2}" -f $ts, $Level.ToUpperInvariant(), $Message
    Write-Host $line
    if (-not [string]::IsNullOrWhiteSpace($script:SupervisorLogFile)) {
        Add-Content -Path $script:SupervisorLogFile -Value $line -Encoding UTF8
    }
}

function Is-AfterCutoff {
    param([string]$HHmm)
    if ([string]::IsNullOrWhiteSpace($HHmm)) {
        return $false
    }
    $nowInt = [int](Get-Date -Format "HHmm")
    $cutoffInt = [int]$HHmm
    return ($nowInt -ge $cutoffInt)
}

function Quote-CmdArg {
    param([Parameter(Mandatory = $true)][string]$Value)
    if ($Value -match '[\s"&|<>^]') {
        return '"' + ($Value -replace '"', '""') + '"'
    }
    return $Value
}

function Write-Heartbeat {
    param(
        [string]$State,
        [int]$RestartCount,
        [Nullable[int]]$ChildPid,
        [Nullable[double]]$IdleSec,
        [string]$LastLogUtc,
        [string]$Note
    )
    $pidValue = ""
    if ($null -ne $ChildPid) {
        $pidValue = [int]$ChildPid
    }
    $idleValue = ""
    if ($null -ne $IdleSec) {
        $idleValue = ("{0:N1}" -f [double]$IdleSec)
    }
    $payload = @{
        name             = $Name
        ts_utc           = (Get-Date).ToUniversalTime().ToString("o")
        state            = $State
        pid              = $pidValue
        restart_count    = $RestartCount
        idle_sec         = $idleValue
        last_log_utc     = $LastLogUtc
        cutoff_hhmm      = $CutoffHHmm
        max_restarts     = $MaxRestarts
        restart_delay_s  = $RestartDelaySec
        hung_timeout_s   = $HungTimeoutSec
        monitor_every_s  = $MonitorIntervalSec
        cooldown_window_s = $CooldownWindowSec
        cooldown_limit   = $CooldownMaxRestarts
        cooldown_delay_s = $CooldownDelaySec
        note             = $Note
        log_file         = $LogFile
        supervisor_log_file = $script:SupervisorLogFile
        status_file      = $StatusFile
    }
    Write-KeyFile -Path $HeartbeatFile -Data $payload
}

function Write-Status {
    param(
        [string]$Status,
        [int]$ExitCode,
        [int]$RestartCount,
        [string]$Reason
    )
    $payload = @{
        status         = $Status
        name           = $Name
        ts             = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        exit_code      = $ExitCode
        restart_count  = $RestartCount
        reason         = $Reason
        cutoff_hhmm    = $CutoffHHmm
        log_file       = $LogFile
        supervisor_log_file = $script:SupervisorLogFile
        heartbeat_file = $HeartbeatFile
    }
    Write-KeyFile -Path $StatusFile -Data $payload
}

Ensure-ParentDir -Path $LogFile
Ensure-ParentDir -Path $StatusFile
Ensure-ParentDir -Path $HeartbeatFile
$script:SupervisorLogFile = "$LogFile.supervisor.log"
Ensure-ParentDir -Path $script:SupervisorLogFile

if (-not (Test-Path -LiteralPath $WorkDir)) {
    throw "WorkDir does not exist: $WorkDir"
}
if (-not (Test-Path -LiteralPath $FilePath)) {
    throw "FilePath does not exist: $FilePath"
}

$maxRestarts = [Math]::Max(0, $MaxRestarts)
$restartDelaySec = [Math]::Max(1, $RestartDelaySec)
$monitorIntervalSec = [Math]::Max(1, $MonitorIntervalSec)
$hungTimeoutSec = [Math]::Max(0, $HungTimeoutSec)
$cooldownWindowSec = [Math]::Max(0, $CooldownWindowSec)
$cooldownMaxRestarts = [Math]::Max(0, $CooldownMaxRestarts)
$cooldownDelaySec = [Math]::Max(1, $CooldownDelaySec)

if ($SkipRunAfterCutoff -and (Is-AfterCutoff -HHmm $CutoffHHmm)) {
    Write-LogLine -Level "INFO" -Message "SKIP $Name (after cutoff $CutoffHHmm)"
    Write-Heartbeat -State "SKIPPED_CUTOFF" -RestartCount 0 -ChildPid $null -IdleSec $null -LastLogUtc "" -Note "skip_run_after_cutoff"
    Write-Status -Status "SKIPPED_CUTOFF" -ExitCode 0 -RestartCount 0 -Reason "after_cutoff"
    exit 0
}

$restartCount = 0
$restartEvents = New-Object System.Collections.Generic.List[DateTime]

while ($true) {
    $runNo = $restartCount + 1
    $exe = Quote-CmdArg -Value $FilePath
    $argParts = @()
    foreach ($arg in $ArgumentList) {
        if ($null -eq $arg) { continue }
        $argParts += (Quote-CmdArg -Value ([string]$arg))
    }
    $joinedArgs = ($argParts -join " ").Trim()
    $cmdLine = if ([string]::IsNullOrWhiteSpace($joinedArgs)) {
        "$exe >> `"$LogFile`" 2>&1"
    } else {
        "$exe $joinedArgs >> `"$LogFile`" 2>&1"
    }

    Write-LogLine -Level "INFO" -Message "START $Name (run=$runNo, restart_count=$restartCount)"
    $process = Start-Process -FilePath "cmd.exe" -ArgumentList @("/d", "/c", $cmdLine) -WorkingDirectory $WorkDir -PassThru -WindowStyle Hidden

    $forcedHungKill = $false
    $runStartUtc = [DateTime]::UtcNow
    $lastActivityUtc = $runStartUtc
    while (-not $process.HasExited) {
        Start-Sleep -Seconds $monitorIntervalSec
        if (Test-Path -LiteralPath $LogFile) {
            $observedLogUtc = (Get-Item -LiteralPath $LogFile).LastWriteTimeUtc
            if ($observedLogUtc -gt $lastActivityUtc) {
                $lastActivityUtc = $observedLogUtc
            }
        }
        $idleSec = ([DateTime]::UtcNow - $lastActivityUtc).TotalSeconds
        Write-Heartbeat -State "RUNNING" -RestartCount $restartCount -ChildPid $process.Id -IdleSec $idleSec -LastLogUtc $lastActivityUtc.ToString("o") -Note "monitoring"

        if ($hungTimeoutSec -gt 0 -and $idleSec -ge $hungTimeoutSec) {
            Write-LogLine -Level "WARN" -Message "HUNG threshold reached for $Name (idle=${idleSec}s >= ${hungTimeoutSec}s). Killing process tree pid=$($process.Id)."
            try {
                & taskkill /PID $process.Id /T /F *> $null
            } catch {
                Write-LogLine -Level "WARN" -Message "taskkill failed for pid=$($process.Id): $($_.Exception.Message)"
            }
            $forcedHungKill = $true
            Start-Sleep -Seconds 1
            break
        }
    }

    try {
        $process.WaitForExit(5000) | Out-Null
    } catch {
        # no-op
    }

    $exitCode = 0
    if ($forcedHungKill) {
        $exitCode = 124
    } elseif ($process.HasExited) {
        $exitCode = $process.ExitCode
    } else {
        $exitCode = 1
    }

    $lastLogUtc = if (Test-Path -LiteralPath $LogFile) { (Get-Item -LiteralPath $LogFile).LastWriteTimeUtc.ToString("o") } else { "" }
    Write-Heartbeat -State "EXITED" -RestartCount $restartCount -ChildPid $null -IdleSec $null -LastLogUtc $lastLogUtc -Note "exit_code=$exitCode"
    Write-LogLine -Level "INFO" -Message "END $Name (exit=$exitCode)"

    if ($exitCode -eq 0) {
        Write-Status -Status "SUCCESS" -ExitCode 0 -RestartCount $restartCount -Reason "normal_exit"
        exit 0
    }

    if ($StopRestartsAfterCutoff -and (Is-AfterCutoff -HHmm $CutoffHHmm)) {
        Write-LogLine -Level "WARN" -Message "Non-zero exit after cutoff. Stopping restarts for $Name."
        Write-Status -Status "STOPPED_AFTER_CUTOFF" -ExitCode 0 -RestartCount $restartCount -Reason "after_cutoff_nonzero_exit"
        exit 0
    }

    $restartCount += 1
    if ($restartCount -gt $maxRestarts) {
        Write-LogLine -Level "ERROR" -Message "Max restarts exceeded for $Name (restart_count=$restartCount, max=$maxRestarts)."
        Write-Status -Status "FAILED" -ExitCode $exitCode -RestartCount $restartCount -Reason "max_restarts_exceeded"
        exit ([Math]::Min([Math]::Max($exitCode, 1), 255))
    }

    $restartEvents.Add((Get-Date).ToUniversalTime())
    if ($cooldownWindowSec -gt 0 -and $cooldownMaxRestarts -gt 0) {
        $windowStart = (Get-Date).ToUniversalTime().AddSeconds(-1 * $cooldownWindowSec)
        $recentCount = @($restartEvents | Where-Object { $_ -ge $windowStart }).Count
        if ($recentCount -ge $cooldownMaxRestarts) {
            Write-LogLine -Level "WARN" -Message "Restart storm detected for $Name ($recentCount restarts in last ${cooldownWindowSec}s). Cooling down for ${cooldownDelaySec}s."
            Write-Heartbeat -State "COOLDOWN" -RestartCount $restartCount -ChildPid $null -IdleSec $null -LastLogUtc $lastLogUtc -Note "restart_storm_recent=$recentCount"
            Start-Sleep -Seconds $cooldownDelaySec
        }
    }

    Write-LogLine -Level "WARN" -Message "Restarting $Name in ${restartDelaySec}s (attempt=$restartCount/$maxRestarts)."
    Write-Heartbeat -State "RESTARTING" -RestartCount $restartCount -ChildPid $null -IdleSec $null -LastLogUtc $lastLogUtc -Note "backoff"
    Start-Sleep -Seconds $restartDelaySec
}
