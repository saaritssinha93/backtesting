[CmdletBinding()]
param(
    [switch]$DryRun,
    [int]$HeartbeatMaxAgeSec = 60,
    [int]$WorkerHeartbeatMaxAgeSec = 360,
    [int]$WorkerLogMaxAgeSec = 360,
    [int]$ParquetMaxAgeSec = 300
)

$ErrorActionPreference = "Continue"

$BaseDir = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
$LogDir = Join-Path $BaseDir "logs"
$LogFile = Join-Path $LogDir "pf_supervisor_healthcheck.log"
$ReportFile = Join-Path $LogDir "pf_supervisor_healthcheck_latest.txt"

$DataDir = "C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
$RuntimeStatusDir = "C:\TradingData\eqidv2\runtime_status"

$HeartbeatFile = Join-Path $LogDir "eqidv2_pending_data_fetcher_v16_5min.heartbeat"
$WorkerHeartbeatFile = Join-Path $RuntimeStatusDir "eqidv2_pending_data_fetcher_v16_5min.heartbeat"
$Today = (Get-Date).ToString("yyyy-MM-dd")
$WorkerLogFile = Join-Path $LogDir ("eqidv2_pending_data_fetcher_v16_5min_{0}.log" -f $Today)
$SupervisorLogFile = "$WorkerLogFile.supervisor.log"
$MonitoredTaskName = "EQIDV2_pending_data_fetcher_v16_5min_0900"

$EmailTo = "saaritssinha93@gmail.com,dragontastic007@gmail.com"
$EmailFrom = "saaritssinha93@gmail.com"
$PythonExe = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
if (-not (Test-Path $PythonExe)) { $PythonExe = "python" }
$GmailScript = Join-Path $BaseDir "bat\send_gmail_api.py"
$GmailCreds = Join-Path $BaseDir "bat\gmail_client_secret.json"
$GmailToken = Join-Path $BaseDir "bat\gmail_token.json"

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

$findings = New-Object System.Collections.Generic.List[string]

function Add-Finding {
    param([string]$Severity, [string]$Message)
    $findings.Add(("[{0}] {1}" -f $Severity, $Message))
}

$monitoredTask = $null
try {
    $monitoredTask = Get-ScheduledTask -TaskName $MonitoredTaskName -ErrorAction Stop
} catch {
    Add-Finding "WARN" ("monitored task state lookup failed for {0}: {1}" -f $MonitoredTaskName, $_.Exception.Message)
}

if ($monitoredTask -and ([string]$monitoredTask.State -eq "Disabled")) {
    Add-Finding "OK" ("{0} is disabled; legacy PF supervisor checks skipped." -f $MonitoredTaskName)
    Add-Finding "OK" "Active 5-minute feed health is covered by the dashboard's eod_5min_data checks."

    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $header = "{0} PF supervisor health check: SKIPPED_DISABLED (failures=0)" -f $timestamp
    $body = $header + "`n`n" + ($findings -join "`n")

    $body | Out-File -FilePath $ReportFile -Encoding utf8
    Add-Content -Path $LogFile -Value $header -Encoding utf8
    foreach ($line in $findings) {
        Add-Content -Path $LogFile -Value ("  " + $line) -Encoding utf8
    }

    Write-Output $body
    exit 0
}

# Check 1: supervisor PowerShell process alive
$supervisor = $null
try {
    $supervisor = Get-CimInstance Win32_Process | Where-Object {
        $cmd = [string]$_.CommandLine
        if ([string]::IsNullOrWhiteSpace($cmd)) { return $false }
        ($cmd -match "supervise_command\.ps1") -and ($cmd -match "eqidv2_pending_data_fetcher_v16_5min")
    } | Select-Object -First 1
} catch {
    Add-Finding "WARN" ("supervisor process query failed: {0}" -f $_.Exception.Message)
}
if (-not $supervisor) {
    Add-Finding "FAIL" "PF supervisor PowerShell process NOT found in process table"
} else {
    Add-Finding "OK" ("PF supervisor alive: PID={0} started={1}" -f $supervisor.ProcessId, $supervisor.CreationDate)
}

# Check 2: supervisor heartbeat freshness
if (Test-Path $HeartbeatFile) {
    $hbAge = ((Get-Date) - (Get-Item $HeartbeatFile).LastWriteTime).TotalSeconds
    if ($hbAge -gt $HeartbeatMaxAgeSec) {
        Add-Finding "FAIL" ("supervisor heartbeat stale: age={0:N0}s (max={1}s)" -f $hbAge, $HeartbeatMaxAgeSec)
    } else {
        Add-Finding "OK" ("supervisor heartbeat fresh: age={0:N0}s" -f $hbAge)
    }
} else {
    Add-Finding "FAIL" ("supervisor heartbeat file missing: {0}" -f $HeartbeatFile)
}

# Check 3: worker heartbeat freshness
if (Test-Path $WorkerHeartbeatFile) {
    $whbAge = ((Get-Date) - (Get-Item $WorkerHeartbeatFile).LastWriteTime).TotalSeconds
    if ($whbAge -gt $WorkerHeartbeatMaxAgeSec) {
        Add-Finding "FAIL" ("worker heartbeat stale: age={0:N0}s (max={1}s)" -f $whbAge, $WorkerHeartbeatMaxAgeSec)
    } else {
        Add-Finding "OK" ("worker heartbeat fresh: age={0:N0}s" -f $whbAge)
    }
} else {
    Add-Finding "FAIL" ("worker heartbeat file missing: {0}" -f $WorkerHeartbeatFile)
}

# Check 4: worker log freshness + recent activity + no SLA breach pattern
if (Test-Path $WorkerLogFile) {
    $logAge = ((Get-Date) - (Get-Item $WorkerLogFile).LastWriteTime).TotalSeconds
    if ($logAge -gt $WorkerLogMaxAgeSec) {
        Add-Finding "FAIL" ("worker log stale: age={0:N0}s (max={1}s)" -f $logAge, $WorkerLogMaxAgeSec)
    } else {
        Add-Finding "OK" ("worker log fresh: age={0:N0}s" -f $logAge)
    }

    $tail = Get-Content $WorkerLogFile -Tail 200 -ErrorAction SilentlyContinue

    $breaches = @($tail | Select-String -Pattern "5min slot SLA breach" -SimpleMatch)
    if ($breaches.Count -gt 0) {
        $lastBreach = $breaches[-1].Line.Trim()
        Add-Finding "FAIL" ("worker log shows {0} SLA breach(es) in last 200 lines | last={1}" -f $breaches.Count, $lastBreach)
    } else {
        Add-Finding "OK" "no SLA breach pattern in recent worker log"
    }

    $retryAttempts = @($tail | Select-String -Pattern "PENDING_FETCH\] retry attempt=" -SimpleMatch)
    $partitionTimeouts = @($tail | Select-String -Pattern "partition exceeded timeout" -SimpleMatch)
    if ($retryAttempts.Count -ge 2 -and $partitionTimeouts.Count -ge 4) {
        Add-Finding "FAIL" ("retry-hell pattern: {0} retry attempts + {1} partition timeouts in last 200 lines" -f $retryAttempts.Count, $partitionTimeouts.Count)
    } elseif ($partitionTimeouts.Count -ge 4) {
        Add-Finding "WARN" ("{0} partition timeouts in last 200 lines (no active retry yet)" -f $partitionTimeouts.Count)
    }

    $lastTiming = $tail | Select-String -Pattern "\[5MIN\]\[TIMING\] scan=" | Select-Object -Last 1
    if ($lastTiming) {
        Add-Finding "OK" ("last fetch timing: {0}" -f $lastTiming.Line.Trim())
    } else {
        Add-Finding "WARN" "no [5MIN][TIMING] scan line in last 200 lines"
    }
} else {
    Add-Finding "FAIL" ("worker log not found: {0}" -f $WorkerLogFile)
}

# Check 5: newest parquet write age (only meaningful during market hours)
$now = Get-Date
$marketOpen = Get-Date -Hour 9 -Minute 15 -Second 0
$marketClose = Get-Date -Hour 15 -Minute 30 -Second 0
$inMarketHours = ($now -ge $marketOpen) -and ($now -le $marketClose)

if (Test-Path $DataDir) {
    $newest = Get-ChildItem $DataDir -Filter *.parquet -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $newest) {
        Add-Finding "FAIL" ("no parquet files in {0}" -f $DataDir)
    } else {
        $pAge = ((Get-Date) - $newest.LastWriteTime).TotalSeconds
        if ($inMarketHours -and ($pAge -gt $ParquetMaxAgeSec)) {
            Add-Finding "FAIL" ("newest parquet stale during market hours: age={0:N0}s file={1}" -f $pAge, $newest.Name)
        } else {
            Add-Finding "OK" ("newest parquet age={0:N0}s file={1}" -f $pAge, $newest.Name)
        }
    }
} else {
    Add-Finding "WARN" ("data dir not found: {0}" -f $DataDir)
}

# Compose report
$timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$failures = @($findings | Where-Object { $_ -match "^\[FAIL\]" })
$status = if ($failures.Count -gt 0) { "UNHEALTHY" } else { "HEALTHY" }
$header = "{0} PF supervisor health check: {1} (failures={2})" -f $timestamp, $status, $failures.Count
$body = $header + "`n`n" + ($findings -join "`n")

# Persist
$body | Out-File -FilePath $ReportFile -Encoding utf8
Add-Content -Path $LogFile -Value $header -Encoding utf8
foreach ($line in $findings) {
    Add-Content -Path $LogFile -Value ("  " + $line) -Encoding utf8
}

# Console
Write-Output $body

# Email only on failure
if ($failures.Count -gt 0 -and -not $DryRun) {
    if (-not (Test-Path $GmailScript)) {
        Write-Output ("[ALERT.SKIP] gmail script missing: {0}" -f $GmailScript)
    } elseif (-not (Test-Path $GmailToken)) {
        Write-Output ("[ALERT.SKIP] gmail token missing: {0}" -f $GmailToken)
    } else {
        $bodyFile = [System.IO.Path]::GetTempFileName()
        try {
            $body | Out-File -FilePath $bodyFile -Encoding utf8
            $subject = "[ALERT] PF supervisor UNHEALTHY ({0} failures) {1} IST" -f $failures.Count, $timestamp
            $emailOut = & $PythonExe $GmailScript `
                --credentials $GmailCreds `
                --token $GmailToken `
                --to $EmailTo `
                --from $EmailFrom `
                --subject $subject `
                --body-file $bodyFile
            Write-Output ("[ALERT.SENT] {0}" -f ($emailOut -join " "))
            Add-Content -Path $LogFile -Value ("  [ALERT.SENT] " + ($emailOut -join " ")) -Encoding utf8
        } catch {
            Write-Output ("[ALERT.FAIL] {0}" -f $_.Exception.Message)
            Add-Content -Path $LogFile -Value ("  [ALERT.FAIL] " + $_.Exception.Message) -Encoding utf8
        } finally {
            Remove-Item $bodyFile -Force -ErrorAction SilentlyContinue
        }
    }
}

if ($failures.Count -gt 0) { exit 1 } else { exit 0 }
