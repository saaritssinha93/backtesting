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
    [string]$FreshnessFile = "",
    [int]$FreshnessTimeoutSec = 0,
    [int]$FreshnessGraceSec = 0,
    [string]$CutoffHHmm = "",
    [switch]$SkipRunAfterCutoff,
    [switch]$StopRestartsAfterCutoff,
    [string]$LockFile = "",
    [string]$SpawnRecordFile = "",
    [string]$WorkerStatusFile = "",
    [string]$WorkerHeartbeatFile = "",
    [int]$WorkerStaleTimeoutSec = 0,
    [int]$WorkerStartGraceSec = 120,
    [int]$WorkerDiscoveryTimeoutSec = 30,
    [string]$AlertEmailTo = "",
    [string]$AlertEmailFrom = "",
    [string]$GmailApiScript = "",
    [string]$GmailCredentialsFile = "",
    [string]$GmailTokenFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:SupervisorLogFile = ""
$script:LockStream = $null
$script:CurrentRunId = ""
$script:CurrentLauncherPid = $null
$script:CurrentLauncherStartUtc = $null
$script:CurrentWorkerPid = $null
$script:CurrentWorkerStartUtc = $null
$script:ExpectedExeLeaf = ""
$script:ExpectedScriptToken = ""
$script:ExpectedScriptLeaf = ""
$script:IstZone = $null

try {
    $script:IstZone = [System.TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
} catch {
    $script:IstZone = [System.TimeZoneInfo]::Local
}

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

function Read-KeyFile {
    param([string]$Path)
    $result = @{}
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return $result
    }
    foreach ($line in (Get-Content -LiteralPath $Path -ErrorAction SilentlyContinue)) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $idx = $line.IndexOf("=")
        if ($idx -lt 1) {
            continue
        }
        $key = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1)
        if (-not [string]::IsNullOrWhiteSpace($key)) {
            $result[$key] = $value
        }
    }
    return $result
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

function Normalize-ArgumentList {
    param([string[]]$Raw)
    $items = @()
    if ($null -eq $Raw) {
        return $items
    }

    foreach ($arg in $Raw) {
        if ($null -eq $arg) {
            continue
        }
        $s = [string]$arg
        $trimmed = $s.Trim()
        if ($trimmed.Length -eq 0) {
            continue
        }

        $candidate = $trimmed
        if ($candidate.StartsWith("@(") -and $candidate.EndsWith(")")) {
            $candidate = $candidate.Substring(2, $candidate.Length - 3).Trim()
        }

        if ($candidate -match ",") {
            $parts = @(
                $candidate -split "," |
                ForEach-Object { $_.Trim().Trim("'`"") } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            )
            if ($parts.Count -gt 1) {
                $items += $parts
                continue
            }
        }

        $items += $s
    }
    return $items
}

function Try-ParseRuntimeTsUtc {
    param([string]$Raw)
    if ([string]::IsNullOrWhiteSpace($Raw)) {
        return $null
    }
    try {
        return ([DateTimeOffset]::Parse($Raw)).UtcDateTime
    } catch {
        # no-op
    }
    try {
        $naive = [DateTime]::ParseExact(
            $Raw,
            "yyyy-MM-dd_HH:mm:ss",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        )
        $unspecified = [DateTime]::SpecifyKind($naive, [DateTimeKind]::Unspecified)
        return [System.TimeZoneInfo]::ConvertTimeToUtc($unspecified, $script:IstZone)
    } catch {
        # no-op
    }
    try {
        return ([DateTime]::Parse($Raw)).ToUniversalTime()
    } catch {
        return $null
    }
}

function Convert-NullablePid {
    param([string]$Raw)
    $value = 0
    if ([int]::TryParse(([string]$Raw), [ref]$value) -and $value -gt 0) {
        return $value
    }
    return $null
}

function Get-ManagedProcessDetails {
    param([Nullable[int]]$ProcessId)
    if ($null -eq $ProcessId) {
        return $null
    }
    $pidValue = [int]$ProcessId
    if ($pidValue -le 0) {
        return $null
    }

    try {
        $proc = Get-Process -Id $pidValue -ErrorAction Stop
    } catch {
        return $null
    }

    $startUtc = $null
    try {
        $startUtc = $proc.StartTime.ToUniversalTime()
    } catch {
        # no-op
    }

    $cmdLine = ""
    $exePath = ""
    try {
        $cim = Get-CimInstance Win32_Process -Filter ("ProcessId = {0}" -f $pidValue) -ErrorAction SilentlyContinue
        if ($null -ne $cim) {
            $cmdLine = [string]$cim.CommandLine
            $exePath = [string]$cim.ExecutablePath
        }
    } catch {
        # no-op
    }

    return [pscustomobject]@{
        Pid           = $pidValue
        StartUtc      = $startUtc
        CommandLine   = $cmdLine
        ExecutablePath = $exePath
        ProcessName   = $proc.ProcessName
    }
}

function Same-ProcessIdentity {
    param(
        [object]$Details,
        [Nullable[int]]$ExpectedPid,
        [Nullable[datetime]]$ExpectedStartUtc
    )
    if ($null -eq $Details -or $null -eq $ExpectedPid) {
        return $false
    }
    if ([int]$Details.Pid -ne [int]$ExpectedPid) {
        return $false
    }
    if ($null -eq $ExpectedStartUtc) {
        return $true
    }
    if ($null -eq $Details.StartUtc) {
        return $false
    }
    return ([math]::Abs((($Details.StartUtc) - $ExpectedStartUtc).TotalSeconds) -lt 2.0)
}

function Get-ExpectedScriptToken {
    param([string[]]$Args)
    foreach ($arg in $Args) {
        if ($null -eq $arg) {
            continue
        }
        $candidate = [string]$arg
        if ($candidate -match '\.(py|ps1|cmd|bat)$') {
            try {
                return [System.IO.Path]::GetFullPath($candidate)
            } catch {
                return $candidate
            }
        }
    }
    return ""
}

function Test-ManagedCommandLineMatch {
    param([object]$Details)
    if ($null -eq $Details) {
        return $false
    }

    $cmd = ("" + $Details.CommandLine).ToLowerInvariant()
    $exeLeaf = ("" + [System.IO.Path]::GetFileName($Details.ExecutablePath)).ToLowerInvariant()
    if (-not [string]::IsNullOrWhiteSpace($script:ExpectedExeLeaf) -and -not [string]::IsNullOrWhiteSpace($exeLeaf)) {
        if ($exeLeaf -ne $script:ExpectedExeLeaf.ToLowerInvariant()) {
            return $false
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($script:ExpectedScriptToken) -and $cmd.Contains($script:ExpectedScriptToken.ToLowerInvariant())) {
        return $true
    }
    if (-not [string]::IsNullOrWhiteSpace($script:ExpectedScriptLeaf) -and $cmd.Contains($script:ExpectedScriptLeaf.ToLowerInvariant())) {
        return $true
    }
    return [string]::IsNullOrWhiteSpace($script:ExpectedScriptToken) -and [string]::IsNullOrWhiteSpace($script:ExpectedScriptLeaf)
}

function Get-MatchingManagedProcesses {
    $results = @()
    $nameFilter = ""
    if (-not [string]::IsNullOrWhiteSpace($script:ExpectedExeLeaf)) {
        $escapedLeaf = $script:ExpectedExeLeaf.Replace("'", "''")
        $nameFilter = "Name = '$escapedLeaf'"
    }

    try {
        $rows = if ([string]::IsNullOrWhiteSpace($nameFilter)) {
            Get-CimInstance Win32_Process -ErrorAction SilentlyContinue
        } else {
            Get-CimInstance Win32_Process -Filter $nameFilter -ErrorAction SilentlyContinue
        }
    } catch {
        $rows = @()
    }

    foreach ($row in $rows) {
        $pidValue = [int]$row.ProcessId
        if ($pidValue -eq $PID) {
            continue
        }
        $details = Get-ManagedProcessDetails -ProcessId $pidValue
        if ($null -ne $details -and (Test-ManagedCommandLineMatch -Details $details)) {
            $results += $details
        }
    }
    return $results
}

function Stop-ProcessTree {
    param(
        [Nullable[int]]$TargetProcessId,
        [string]$Reason
    )
    if ($null -eq $TargetProcessId) {
        return
    }
    $pidValue = [int]$TargetProcessId
    if ($pidValue -le 0) {
        return
    }
    Write-LogLine -Level "WARN" -Message "Stopping pid=$pidValue for $Name ($Reason)."
    try {
        & taskkill /PID $pidValue /T /F *> $null
    } catch {
        Write-LogLine -Level "WARN" -Message "taskkill failed for pid=${pidValue}: $($_.Exception.Message)"
    }
}

function Get-KeyFileSnapshot {
    param(
        [string]$Path,
        [string]$Kind
    )
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $item = Get-Item -LiteralPath $Path
    $data = Read-KeyFile -Path $Path
    $pidValue = $null
    if ($data.ContainsKey("pid")) {
        $pidValue = Convert-NullablePid -Raw ([string]$data["pid"])
    }
    $tsUtc = $null
    if ($data.ContainsKey("ts_utc")) {
        $tsUtc = Try-ParseRuntimeTsUtc -Raw ([string]$data["ts_utc"])
    }
    if ($null -eq $tsUtc -and $data.ContainsKey("ts")) {
        $tsUtc = Try-ParseRuntimeTsUtc -Raw ([string]$data["ts"])
    }
    if ($null -eq $tsUtc) {
        $tsUtc = $item.LastWriteTimeUtc
    }
    return [pscustomobject]@{
        Path      = $Path
        Kind      = $Kind
        Data      = $data
        Pid       = $pidValue
        TsUtc     = $tsUtc
        AgeSec    = ([DateTime]::UtcNow - $tsUtc).TotalSeconds
        LastWrite = $item.LastWriteTimeUtc
    }
}

function Get-WorkerSignalSnapshot {
    $candidates = @()
    $hb = Get-KeyFileSnapshot -Path $WorkerHeartbeatFile -Kind "heartbeat"
    if ($null -ne $hb) {
        $candidates += $hb
    }
    $st = Get-KeyFileSnapshot -Path $WorkerStatusFile -Kind "status"
    if ($null -ne $st) {
        $candidates += $st
    }
    if ($candidates.Count -eq 0) {
        return $null
    }
    return ($candidates | Sort-Object TsUtc -Descending | Select-Object -First 1)
}

function Get-DirectChildWorker {
    param([Nullable[int]]$LauncherPid)
    if ($null -eq $LauncherPid) {
        return $null
    }
    $pidValue = [int]$LauncherPid
    if ($pidValue -le 0) {
        return $null
    }
    try {
        $children = Get-CimInstance Win32_Process -Filter ("ParentProcessId = {0}" -f $pidValue) -ErrorAction SilentlyContinue
    } catch {
        $children = @()
    }
    foreach ($child in $children) {
        $details = Get-ManagedProcessDetails -ProcessId ([int]$child.ProcessId)
        if ($null -ne $details -and (Test-ManagedCommandLineMatch -Details $details)) {
            return $details
        }
    }
    foreach ($child in $children) {
        $details = Get-ManagedProcessDetails -ProcessId ([int]$child.ProcessId)
        if ($null -eq $details) {
            continue
        }
        $exeLeaf = ("" + [System.IO.Path]::GetFileName($details.ExecutablePath)).ToLowerInvariant()
        if (-not [string]::IsNullOrWhiteSpace($script:ExpectedExeLeaf) -and $exeLeaf -eq $script:ExpectedExeLeaf.ToLowerInvariant()) {
            return $details
        }
    }
    return $null
}

function Resolve-WorkerIdentity {
    param([Nullable[int]]$LauncherPid)
    $direct = Get-DirectChildWorker -LauncherPid $LauncherPid
    if ($null -ne $direct) {
        return $direct
    }
    $signal = Get-WorkerSignalSnapshot
    if ($null -ne $signal -and $null -ne $signal.Pid) {
        $details = Get-ManagedProcessDetails -ProcessId ([int]$signal.Pid)
        if ($null -ne $details -and (Test-ManagedCommandLineMatch -Details $details)) {
            return $details
        }
    }
    return $null
}

function Wait-ForWorkerDiscovery {
    param(
        [Nullable[int]]$LauncherPid,
        [int]$TimeoutSec
    )
    $timeout = [Math]::Max(5, $TimeoutSec)
    $deadline = [DateTime]::UtcNow.AddSeconds($timeout)
    while ([DateTime]::UtcNow -lt $deadline) {
        $resolved = Resolve-WorkerIdentity -LauncherPid $LauncherPid
        if ($null -ne $resolved) {
            return $resolved
        }
        $launcher = Get-ManagedProcessDetails -ProcessId $LauncherPid
        if ($null -eq $launcher) {
            break
        }
        Start-Sleep -Milliseconds 500
    }
    return $null
}

function Acquire-SingletonLock {
    param([string]$Path)
    Ensure-ParentDir -Path $Path
    try {
        $script:LockStream = [System.IO.File]::Open(
            $Path,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
    } catch {
        $msg = "Another supervisor instance appears to be active for $Name (lock=$Path)."
        Write-Host $msg
        if (-not [string]::IsNullOrWhiteSpace($script:SupervisorLogFile)) {
            Add-Content -Path $script:SupervisorLogFile -Value ("{0} | WARN | {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg) -Encoding UTF8
        }
        return $false
    }

    $meta = @(
        "name=$Name",
        "supervisor_pid=$PID",
        "acquired_utc=$(([DateTime]::UtcNow).ToString('o'))",
        "status_file=$StatusFile",
        "heartbeat_file=$HeartbeatFile",
        "spawn_record_file=$SpawnRecordFile"
    ) -join [Environment]::NewLine
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($meta)
    $script:LockStream.SetLength(0)
    $script:LockStream.Position = 0
    $script:LockStream.Write($bytes, 0, $bytes.Length)
    $script:LockStream.Flush()
    return $true
}

function Write-SpawnRecord {
    param(
        [string]$State,
        [int]$RunNo,
        [int]$RestartCount
    )
    if ([string]::IsNullOrWhiteSpace($SpawnRecordFile)) {
        return
    }
    $payload = @{
        state                = $State
        name                 = $Name
        supervisor_pid       = $PID
        run_id               = $script:CurrentRunId
        run_no               = $RunNo
        restart_count        = $RestartCount
        launcher_pid         = if ($null -ne $script:CurrentLauncherPid) { [int]$script:CurrentLauncherPid } else { "" }
        launcher_start_utc   = if ($null -ne $script:CurrentLauncherStartUtc) { $script:CurrentLauncherStartUtc.ToString("o") } else { "" }
        worker_pid           = if ($null -ne $script:CurrentWorkerPid) { [int]$script:CurrentWorkerPid } else { "" }
        worker_start_utc     = if ($null -ne $script:CurrentWorkerStartUtc) { $script:CurrentWorkerStartUtc.ToString("o") } else { "" }
        worker_status_file   = $WorkerStatusFile
        worker_heartbeat_file = $WorkerHeartbeatFile
        status_file          = $StatusFile
        heartbeat_file       = $HeartbeatFile
        log_file             = $LogFile
        updated_utc          = ([DateTime]::UtcNow).ToString("o")
    }
    Write-KeyFile -Path $SpawnRecordFile -Data $payload
}

function Stop-RecordedProcessIfMatch {
    param(
        [hashtable]$Record,
        [string]$PidKey,
        [string]$StartKey,
        [string]$Reason
    )
    if ($null -eq $Record -or -not $Record.ContainsKey($PidKey)) {
        return
    }
    $pidValue = Convert-NullablePid -Raw ([string]$Record[$PidKey])
    if ($null -eq $pidValue) {
        return
    }
    $expectedStart = $null
    if ($Record.ContainsKey($StartKey)) {
        $expectedStart = Try-ParseRuntimeTsUtc -Raw ([string]$Record[$StartKey])
    }
    $details = Get-ManagedProcessDetails -ProcessId $pidValue
    if ($null -eq $details) {
        return
    }
    if ($null -ne $expectedStart -and -not (Same-ProcessIdentity -Details $details -ExpectedPid $pidValue -ExpectedStartUtc $expectedStart)) {
        return
    }
    if (-not (Test-ManagedCommandLineMatch -Details $details)) {
        return
    }
    Stop-ProcessTree -TargetProcessId $pidValue -Reason $Reason
}

function Cleanup-OrphanedManagedProcesses {
    if (-not [string]::IsNullOrWhiteSpace($SpawnRecordFile) -and (Test-Path -LiteralPath $SpawnRecordFile)) {
        $record = Read-KeyFile -Path $SpawnRecordFile
        Stop-RecordedProcessIfMatch -Record $record -PidKey "launcher_pid" -StartKey "launcher_start_utc" -Reason "stale_spawn_record_launcher"
        Stop-RecordedProcessIfMatch -Record $record -PidKey "worker_pid" -StartKey "worker_start_utc" -Reason "stale_spawn_record_worker"
    }
}

function Send-AlertEmail {
    param(
        [string]$Reason,
        [string]$Message
    )
    if ([string]::IsNullOrWhiteSpace($AlertEmailTo)) {
        return
    }

    $resolvedScript = $GmailApiScript
    if ([string]::IsNullOrWhiteSpace($resolvedScript)) {
        $resolvedScript = Join-Path -Path (Split-Path -Parent $PSCommandPath) -ChildPath "send_gmail_api.py"
    }
    if (-not (Test-Path -LiteralPath $resolvedScript)) {
        Write-LogLine -Level "WARN" -Message "Alert email skipped: Gmail API script not found ($resolvedScript)."
        return
    }
    if ([string]::IsNullOrWhiteSpace($GmailCredentialsFile) -or -not (Test-Path -LiteralPath $GmailCredentialsFile)) {
        Write-LogLine -Level "WARN" -Message "Alert email skipped: Gmail credentials file missing."
        return
    }
    if ([string]::IsNullOrWhiteSpace($GmailTokenFile) -or -not (Test-Path -LiteralPath $GmailTokenFile)) {
        Write-LogLine -Level "WARN" -Message "Alert email skipped: Gmail token file missing."
        return
    }

    $pythonForAlert = if (Test-Path -LiteralPath $FilePath) { $FilePath } else { "python" }
    $subject = "[EQIDV2 Supervisor] $Name restarted ($Reason)"
    $body = @(
        "Managed process: $Name",
        "Reason: $Reason",
        "Supervisor pid: $PID",
        "Run id: $script:CurrentRunId",
        "Launcher pid: $script:CurrentLauncherPid",
        "Worker pid: $script:CurrentWorkerPid",
        "Worker status file: $WorkerStatusFile",
        "Worker heartbeat file: $WorkerHeartbeatFile",
        "Supervisor status file: $StatusFile",
        "Supervisor heartbeat file: $HeartbeatFile",
        "Spawn record file: $SpawnRecordFile",
        "Log file: $LogFile",
        "Supervisor log file: $script:SupervisorLogFile",
        "",
        $Message
    ) -join [Environment]::NewLine

    $recipients = @(
        $AlertEmailTo -split "[,;]" |
        ForEach-Object { $_.Trim() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    foreach ($recipient in $recipients) {
        $args = @(
            $resolvedScript,
            "--credentials", $GmailCredentialsFile,
            "--token", $GmailTokenFile,
            "--to", $recipient,
            "--subject", $subject,
            "--body", $body
        )
        if (-not [string]::IsNullOrWhiteSpace($AlertEmailFrom)) {
            $args += @("--from", $AlertEmailFrom)
        }
        try {
            & $pythonForAlert @args *> $null
            if ($LASTEXITCODE -eq 0) {
                Write-LogLine -Level "INFO" -Message "Alert email sent to $recipient ($Reason)."
            } else {
                Write-LogLine -Level "WARN" -Message "Alert email send failed for $recipient (exit=$LASTEXITCODE, reason=$Reason)."
            }
        } catch {
            Write-LogLine -Level "WARN" -Message "Alert email send failed for ${recipient}: $($_.Exception.Message)"
        }
    }
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
        name                  = $Name
        ts_utc                = (Get-Date).ToUniversalTime().ToString("o")
        state                 = $State
        pid                   = $pidValue
        supervisor_pid        = $PID
        restart_count         = $RestartCount
        idle_sec              = $idleValue
        last_log_utc          = $LastLogUtc
        cutoff_hhmm           = $CutoffHHmm
        max_restarts          = $MaxRestarts
        restart_delay_s       = $RestartDelaySec
        hung_timeout_s        = $HungTimeoutSec
        monitor_every_s       = $MonitorIntervalSec
        cooldown_window_s     = $CooldownWindowSec
        cooldown_limit        = $CooldownMaxRestarts
        cooldown_delay_s      = $CooldownDelaySec
        freshness_file        = $FreshnessFile
        freshness_timeout_s   = $FreshnessTimeoutSec
        freshness_grace_s     = $FreshnessGraceSec
        worker_status_file    = $WorkerStatusFile
        worker_heartbeat_file = $WorkerHeartbeatFile
        worker_stale_s        = $WorkerStaleTimeoutSec
        worker_grace_s        = $WorkerStartGraceSec
        lock_file             = $LockFile
        spawn_record_file     = $SpawnRecordFile
        launcher_pid          = if ($null -ne $script:CurrentLauncherPid) { [int]$script:CurrentLauncherPid } else { "" }
        launcher_start_utc    = if ($null -ne $script:CurrentLauncherStartUtc) { $script:CurrentLauncherStartUtc.ToString("o") } else { "" }
        worker_pid            = if ($null -ne $script:CurrentWorkerPid) { [int]$script:CurrentWorkerPid } else { "" }
        worker_start_utc      = if ($null -ne $script:CurrentWorkerStartUtc) { $script:CurrentWorkerStartUtc.ToString("o") } else { "" }
        run_id                = $script:CurrentRunId
        note                  = $Note
        log_file              = $LogFile
        supervisor_log_file   = $script:SupervisorLogFile
        status_file           = $StatusFile
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
        status                = $Status
        name                  = $Name
        ts                    = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        exit_code             = $ExitCode
        restart_count         = $RestartCount
        reason                = $Reason
        supervisor_pid        = $PID
        cutoff_hhmm           = $CutoffHHmm
        lock_file             = $LockFile
        spawn_record_file     = $SpawnRecordFile
        launcher_pid          = if ($null -ne $script:CurrentLauncherPid) { [int]$script:CurrentLauncherPid } else { "" }
        launcher_start_utc    = if ($null -ne $script:CurrentLauncherStartUtc) { $script:CurrentLauncherStartUtc.ToString("o") } else { "" }
        worker_pid            = if ($null -ne $script:CurrentWorkerPid) { [int]$script:CurrentWorkerPid } else { "" }
        worker_start_utc      = if ($null -ne $script:CurrentWorkerStartUtc) { $script:CurrentWorkerStartUtc.ToString("o") } else { "" }
        worker_status_file    = $WorkerStatusFile
        worker_heartbeat_file = $WorkerHeartbeatFile
        log_file              = $LogFile
        supervisor_log_file   = $script:SupervisorLogFile
        heartbeat_file        = $HeartbeatFile
        run_id                = $script:CurrentRunId
    }
    Write-KeyFile -Path $StatusFile -Data $payload
}

function Write-RunningStatus {
    param(
        [int]$RestartCount,
        [Nullable[int]]$ChildPid,
        [string]$Reason
    )
    $pidValue = ""
    if ($null -ne $ChildPid) {
        $pidValue = [int]$ChildPid
    }
    $payload = @{
        status                = "RUNNING"
        name                  = $Name
        ts                    = (Get-Date -Format "yyyy-MM-dd_HH:mm:ss")
        exit_code             = ""
        restart_count         = $RestartCount
        reason                = $Reason
        pid                   = $pidValue
        supervisor_pid        = $PID
        cutoff_hhmm           = $CutoffHHmm
        freshness_file        = $FreshnessFile
        freshness_timeout_s   = $FreshnessTimeoutSec
        freshness_grace_s     = $FreshnessGraceSec
        worker_status_file    = $WorkerStatusFile
        worker_heartbeat_file = $WorkerHeartbeatFile
        worker_stale_s        = $WorkerStaleTimeoutSec
        worker_grace_s        = $WorkerStartGraceSec
        lock_file             = $LockFile
        spawn_record_file     = $SpawnRecordFile
        launcher_pid          = if ($null -ne $script:CurrentLauncherPid) { [int]$script:CurrentLauncherPid } else { "" }
        launcher_start_utc    = if ($null -ne $script:CurrentLauncherStartUtc) { $script:CurrentLauncherStartUtc.ToString("o") } else { "" }
        worker_pid            = if ($null -ne $script:CurrentWorkerPid) { [int]$script:CurrentWorkerPid } else { "" }
        worker_start_utc      = if ($null -ne $script:CurrentWorkerStartUtc) { $script:CurrentWorkerStartUtc.ToString("o") } else { "" }
        log_file              = $LogFile
        supervisor_log_file   = $script:SupervisorLogFile
        heartbeat_file        = $HeartbeatFile
        run_id                = $script:CurrentRunId
    }
    Write-KeyFile -Path $StatusFile -Data $payload
}

Ensure-ParentDir -Path $LogFile
Ensure-ParentDir -Path $StatusFile
Ensure-ParentDir -Path $HeartbeatFile
$script:SupervisorLogFile = "$LogFile.supervisor.log"
Ensure-ParentDir -Path $script:SupervisorLogFile

if ([string]::IsNullOrWhiteSpace($LockFile)) {
    $LockFile = "$StatusFile.lock"
}
if ([string]::IsNullOrWhiteSpace($SpawnRecordFile)) {
    $SpawnRecordFile = "$StatusFile.spawn"
}

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
$freshnessTimeoutSec = [Math]::Max(0, $FreshnessTimeoutSec)
$freshnessGraceSec = [Math]::Max(0, $FreshnessGraceSec)
$workerStaleTimeoutSec = [Math]::Max(0, $WorkerStaleTimeoutSec)
$workerStartGraceSec = [Math]::Max(5, $WorkerStartGraceSec)
$workerDiscoveryTimeoutSec = [Math]::Max(5, $WorkerDiscoveryTimeoutSec)

$normalizedArgumentList = Normalize-ArgumentList -Raw $ArgumentList
$script:ExpectedExeLeaf = [System.IO.Path]::GetFileName($FilePath)
$script:ExpectedScriptToken = Get-ExpectedScriptToken -Args $normalizedArgumentList
$script:ExpectedScriptLeaf = if ([string]::IsNullOrWhiteSpace($script:ExpectedScriptToken)) { "" } else { [System.IO.Path]::GetFileName($script:ExpectedScriptToken) }

if (-not (Acquire-SingletonLock -Path $LockFile)) {
    exit 0
}

Cleanup-OrphanedManagedProcesses

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
    $script:CurrentRunId = ([guid]::NewGuid()).ToString("N")
    $script:CurrentLauncherPid = $null
    $script:CurrentLauncherStartUtc = $null
    $script:CurrentWorkerPid = $null
    $script:CurrentWorkerStartUtc = $null

    $exe = Quote-CmdArg -Value $FilePath
    $argParts = @()
    foreach ($arg in $normalizedArgumentList) {
        if ($null -eq $arg) {
            continue
        }
        $argParts += (Quote-CmdArg -Value ([string]$arg))
    }
    $joinedArgs = ($argParts -join " ").Trim()
    $cmdLine = if ([string]::IsNullOrWhiteSpace($joinedArgs)) {
        "$exe >> `"$LogFile`" 2>&1"
    } else {
        "$exe $joinedArgs >> `"$LogFile`" 2>&1"
    }

    Write-LogLine -Level "INFO" -Message "START $Name (run=$runNo, restart_count=$restartCount, supervisor_pid=$PID)"
    $process = Start-Process -FilePath "cmd.exe" -ArgumentList @("/d", "/c", $cmdLine) -WorkingDirectory $WorkDir -PassThru -WindowStyle Hidden
    $launcherDetails = Get-ManagedProcessDetails -ProcessId $process.Id
    $script:CurrentLauncherPid = $process.Id
    if ($null -ne $launcherDetails) {
        $script:CurrentLauncherStartUtc = $launcherDetails.StartUtc
    }

    Write-RunningStatus -RestartCount $restartCount -ChildPid $process.Id -Reason "started"
    Write-SpawnRecord -State "STARTED" -RunNo $runNo -RestartCount $restartCount

    $workerDetails = Wait-ForWorkerDiscovery -LauncherPid $process.Id -TimeoutSec $workerDiscoveryTimeoutSec
    if ($null -ne $workerDetails) {
        $script:CurrentWorkerPid = $workerDetails.Pid
        $script:CurrentWorkerStartUtc = $workerDetails.StartUtc
        Write-LogLine -Level "INFO" -Message "Worker discovered for $Name (launcher_pid=$($process.Id), worker_pid=$($workerDetails.Pid))."
        Write-SpawnRecord -State "RUNNING" -RunNo $runNo -RestartCount $restartCount
        Write-RunningStatus -RestartCount $restartCount -ChildPid $process.Id -Reason "worker_discovered"
    } else {
        Write-LogLine -Level "WARN" -Message "Worker discovery timed out for $Name after ${workerDiscoveryTimeoutSec}s."
    }

    $forcedHungKill = $false
    $forcedExitReason = ""
    $runStartUtc = [DateTime]::UtcNow
    $lastActivityUtc = $runStartUtc
    $alertSent = $false

    while (-not $process.HasExited) {
        Start-Sleep -Seconds $monitorIntervalSec

        if ($null -eq $script:CurrentWorkerPid) {
            $workerDetails = Resolve-WorkerIdentity -LauncherPid $process.Id
            if ($null -ne $workerDetails) {
                $script:CurrentWorkerPid = $workerDetails.Pid
                $script:CurrentWorkerStartUtc = $workerDetails.StartUtc
                Write-LogLine -Level "INFO" -Message "Worker resolved during monitoring for $Name (worker_pid=$($workerDetails.Pid))."
                Write-SpawnRecord -State "RUNNING" -RunNo $runNo -RestartCount $restartCount
            }
        }

        if (Test-Path -LiteralPath $LogFile) {
            $observedLogUtc = (Get-Item -LiteralPath $LogFile).LastWriteTimeUtc
            if ($observedLogUtc -gt $lastActivityUtc) {
                $lastActivityUtc = $observedLogUtc
            }
        }
        $idleSec = ([DateTime]::UtcNow - $lastActivityUtc).TotalSeconds
        Write-Heartbeat -State "RUNNING" -RestartCount $restartCount -ChildPid $process.Id -IdleSec $idleSec -LastLogUtc $lastActivityUtc.ToString("o") -Note "monitoring"
        Write-RunningStatus -RestartCount $restartCount -ChildPid $process.Id -Reason "monitoring"

        $launcherNow = Get-ManagedProcessDetails -ProcessId $process.Id
        if ($null -eq $launcherNow -or -not (Same-ProcessIdentity -Details $launcherNow -ExpectedPid $process.Id -ExpectedStartUtc $script:CurrentLauncherStartUtc)) {
            $forcedHungKill = $true
            $forcedExitReason = "launcher_identity_mismatch"
            break
        }

        $runAgeSec = ([DateTime]::UtcNow - $runStartUtc).TotalSeconds

        if ($null -eq $script:CurrentWorkerPid) {
            if ($runAgeSec -ge $workerStartGraceSec) {
                $forcedHungKill = $true
                $forcedExitReason = "worker_discovery_timeout"
                if (-not $alertSent) {
                    Send-AlertEmail -Reason $forcedExitReason -Message "The worker process was not discovered within the grace window."
                    $alertSent = $true
                }
                Stop-ProcessTree -TargetProcessId $process.Id -Reason $forcedExitReason
                Start-Sleep -Seconds 1
                break
            }
        } else {
            $workerNow = Get-ManagedProcessDetails -ProcessId $script:CurrentWorkerPid
            if ($null -eq $workerNow -or -not (Same-ProcessIdentity -Details $workerNow -ExpectedPid $script:CurrentWorkerPid -ExpectedStartUtc $script:CurrentWorkerStartUtc)) {
                $forcedHungKill = $true
                $forcedExitReason = "worker_identity_mismatch"
                if (-not $alertSent) {
                    Send-AlertEmail -Reason $forcedExitReason -Message "The managed worker pid disappeared or its start time no longer matches the supervisor spawn record."
                    $alertSent = $true
                }
                Stop-ProcessTree -TargetProcessId $process.Id -Reason $forcedExitReason
                Start-Sleep -Seconds 1
                break
            }
        }

        if ($workerStaleTimeoutSec -gt 0 -or -not [string]::IsNullOrWhiteSpace($WorkerStatusFile) -or -not [string]::IsNullOrWhiteSpace($WorkerHeartbeatFile)) {
            $workerSignal = Get-WorkerSignalSnapshot
            if ($runAgeSec -ge $workerStartGraceSec) {
                if ($null -eq $workerSignal) {
                    $forcedHungKill = $true
                    $forcedExitReason = "worker_heartbeat_missing"
                    if (-not $alertSent) {
                        Send-AlertEmail -Reason $forcedExitReason -Message "Neither worker status nor worker heartbeat file was updated within the configured grace window."
                        $alertSent = $true
                    }
                    Stop-ProcessTree -TargetProcessId $process.Id -Reason $forcedExitReason
                    Start-Sleep -Seconds 1
                    break
                }

                if ($null -ne $workerSignal.Pid -and $null -ne $script:CurrentWorkerPid) {
                    $signalPid = [int]$workerSignal.Pid
                    if ($signalPid -ne [int]$script:CurrentWorkerPid) {
                        $unexpected = Get-ManagedProcessDetails -ProcessId $signalPid
                        if ($null -ne $unexpected -and (Test-ManagedCommandLineMatch -Details $unexpected)) {
                            Stop-ProcessTree -TargetProcessId $signalPid -Reason "worker_pid_mismatch_orphan"
                        }
                        $forcedHungKill = $true
                        $forcedExitReason = "worker_pid_mismatch"
                        if (-not $alertSent) {
                            $msg = "Worker heartbeat/status file reported pid=$signalPid while supervisor expected pid=$($script:CurrentWorkerPid)."
                            Send-AlertEmail -Reason $forcedExitReason -Message $msg
                            $alertSent = $true
                        }
                        Stop-ProcessTree -TargetProcessId $process.Id -Reason $forcedExitReason
                        Start-Sleep -Seconds 1
                        break
                    }
                }

                if ($workerStaleTimeoutSec -gt 0 -and $workerSignal.AgeSec -ge $workerStaleTimeoutSec) {
                    $forcedHungKill = $true
                    $forcedExitReason = "worker_heartbeat_stale"
                    if (-not $alertSent) {
                        $msg = "Worker liveness file '$($workerSignal.Path)' age=$([math]::Round($workerSignal.AgeSec, 1))s exceeded threshold ${workerStaleTimeoutSec}s."
                        Send-AlertEmail -Reason $forcedExitReason -Message $msg
                        $alertSent = $true
                    }
                    Stop-ProcessTree -TargetProcessId $process.Id -Reason $forcedExitReason
                    Start-Sleep -Seconds 1
                    break
                }
            }
        }

        if ($hungTimeoutSec -gt 0 -and $idleSec -ge $hungTimeoutSec) {
            Write-LogLine -Level "WARN" -Message "HUNG threshold reached for $Name (idle=${idleSec}s >= ${hungTimeoutSec}s). Killing process tree pid=$($process.Id)."
            Stop-ProcessTree -TargetProcessId $process.Id -Reason "hung_idle"
            $forcedHungKill = $true
            $forcedExitReason = "hung_idle"
            Start-Sleep -Seconds 1
            break
        }

        if ($freshnessTimeoutSec -gt 0 -and -not [string]::IsNullOrWhiteSpace($FreshnessFile)) {
            if ($runAgeSec -ge $freshnessGraceSec) {
                if (Test-Path -LiteralPath $FreshnessFile) {
                    $freshnessUtc = (Get-Item -LiteralPath $FreshnessFile).LastWriteTimeUtc
                    $freshnessAgeSec = ([DateTime]::UtcNow - $freshnessUtc).TotalSeconds
                    if ($freshnessAgeSec -ge $freshnessTimeoutSec) {
                        Write-LogLine -Level "WARN" -Message "Freshness threshold reached for $Name (file=$FreshnessFile, age=${freshnessAgeSec}s >= ${freshnessTimeoutSec}s). Killing process tree pid=$($process.Id)."
                        Stop-ProcessTree -TargetProcessId $process.Id -Reason "freshness_stale"
                        $forcedHungKill = $true
                        $forcedExitReason = "freshness_stale"
                        Start-Sleep -Seconds 1
                        break
                    }
                } else {
                    Write-LogLine -Level "WARN" -Message "Freshness file missing for $Name after grace window (file=$FreshnessFile, grace=${freshnessGraceSec}s). Killing process tree pid=$($process.Id)."
                    Stop-ProcessTree -TargetProcessId $process.Id -Reason "freshness_missing"
                    $forcedHungKill = $true
                    $forcedExitReason = "freshness_missing"
                    Start-Sleep -Seconds 1
                    break
                }
            }
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
    $exitNote = if ([string]::IsNullOrWhiteSpace($forcedExitReason)) { "exit_code=$exitCode" } else { "exit_code=$exitCode reason=$forcedExitReason" }
    Write-Heartbeat -State "EXITED" -RestartCount $restartCount -ChildPid $null -IdleSec $null -LastLogUtc $lastLogUtc -Note $exitNote
    Write-SpawnRecord -State "EXITED" -RunNo $runNo -RestartCount $restartCount
    Write-LogLine -Level "INFO" -Message "END $Name (exit=$exitCode, reason=$forcedExitReason)"

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
