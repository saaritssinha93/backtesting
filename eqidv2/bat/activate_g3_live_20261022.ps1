[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$SessionDate = '2026-10-22'
$PriorSessionDate = '2026-10-21'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$AuthTask = 'EQIDV2_authentication_v2_0900'
$Workspace = Split-Path $PSScriptRoot -Parent
$Python = 'C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe'
$Approval = Join-Path $PSScriptRoot 'approve_g3_live.ps1'
$HealthCheck = Join-Path $Workspace 'preopen_session_healthcheck.py'
$HealthPath = Join-Path $Workspace 'logs\preopen_session_healthcheck_latest.json'
$HealthRefreshLog = Join-Path $Workspace 'logs\g3_live_activation_2026-10-22_healthcheck.log'
$AuthStatusPath = Join-Path $Workspace 'logs\authentication_v2_runner.status'
$ExpectedAuthLogPath = Join-Path $Workspace 'logs\authentication_v2_runner.log'
$BrokerReportPath = Join-Path $Workspace 'logs\kite_order_readiness\latest.json'
$ProfilePath = Join-Path $Workspace 'configs\strategies\active_profile.json'
$ArmPath = 'C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live\live_arm.json'
$LogPath = Join-Path $Workspace 'logs\g3_live_activation_2026-10-22.log'
$StatusHelpers = Join-Path $PSScriptRoot 'g3_activation_status_helpers.ps1'
$ExpectedHealthWarning = 'acceptance pending: waiting for the first canonical 09:20 stock-futures marker'
$ExpectedStrategyVersion = 'V13-V10-G-3-PRODUCTION-V12'
$ExpectedStrategyFingerprint = '7d4012b1886bce289f7d5036fd3a4d09ee9f8fd1f3da5d8265a83f8096d26b1c'
$ExpectedPriorStrategyVersion = 'V13-V10-G-3-PRODUCTION-V11'
$ExpectedPriorStrategyFingerprint = '82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4'
$ExpectedPolicyFingerprint = 'cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e'
$ExpectedPriorPolicyFingerprint = '979556c444f951ea1e79ec4a90b7570b010dc931f2af674aa0849da6c64f3362'
$ExpectedSetupCount = 61
$AuthMaxAgeSeconds = 5400
$ClockMaxSkewSeconds = 30

if (-not (Test-Path -LiteralPath $StatusHelpers -PathType Leaf)) {
    throw "Activation status parser is missing: $StatusHelpers"
}
. $StatusHelpers

function Write-G3ActivationLog([string]$Message) {
    try {
        $parent = Split-Path $LogPath -Parent
        if (-not (Test-Path -LiteralPath $parent)) {
            New-Item -ItemType Directory -Path $parent | Out-Null
        }
        Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value (
            "[$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))] $Message"
        )
    } catch {
        # A failed log sink can never turn a failed gate into an approval.
    }
}

function Read-StrictJsonWithRetry([string]$Path) {
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
                throw "Missing JSON evidence: $Path"
            }
            $value = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 |
                ConvertFrom-Json -ErrorAction Stop
            if ($null -eq $value -or $value -is [array]) {
                throw "JSON evidence is not an object: $Path"
            }
            return $value
        } catch {
            if ($attempt -eq 3) { throw }
            Start-Sleep -Milliseconds 200
        }
    }
}

function Assert-ReviewedTargetDefinitionXml {
    [xml]$definition = Export-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $namespace = New-Object System.Xml.XmlNamespaceManager($definition.NameTable)
    $namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $triggers = @($definition.SelectNodes('/t:Task/t:Triggers/*', $namespace))
    $actions = @($definition.SelectNodes('/t:Task/t:Actions/t:Exec', $namespace))
    if ($triggers.Count -ne 1 -or $triggers[0].LocalName -ne 'CalendarTrigger' -or
        $actions.Count -ne 1) {
        throw 'G3 LIVE target does not have exactly one reviewed weekly trigger and action.'
    }

    $trigger = $triggers[0]
    $start = $trigger.SelectSingleNode('t:StartBoundary', $namespace)
    $interval = $trigger.SelectSingleNode('t:ScheduleByWeek/t:WeeksInterval', $namespace)
    $days = @($trigger.SelectNodes('t:ScheduleByWeek/t:DaysOfWeek/*', $namespace) |
        ForEach-Object { $_.LocalName })
    if ($null -eq $start -or ([datetime]$start.InnerText).TimeOfDay -ne
            [timespan]::FromHours(9.25) -or
        $null -eq $interval -or [string]$interval.InnerText -ne '1' -or
        ($days -join '|') -ne 'Monday|Tuesday|Wednesday|Thursday|Friday' -or
        $null -ne $trigger.SelectSingleNode('t:Repetition', $namespace) -or
        $null -ne $trigger.SelectSingleNode('t:EndBoundary', $namespace)) {
        throw 'G3 LIVE target trigger is not the reviewed non-repeating Monday-Friday 09:15 trigger.'
    }

    $expectedCommand = Join-Path $env:SystemRoot 'System32\cmd.exe'
    $expectedLauncher = Join-Path $PSScriptRoot 'run_g3_approved_live.bat'
    $expectedArguments = '/d /c ""' + $expectedLauncher + '""'
    $command = $actions[0].SelectSingleNode('t:Command', $namespace)
    $arguments = $actions[0].SelectSingleNode('t:Arguments', $namespace)
    $workingDirectory = $actions[0].SelectSingleNode('t:WorkingDirectory', $namespace)
    if ($null -eq $command -or $command.InnerText -ne $expectedCommand -or
        $null -eq $arguments -or $arguments.InnerText -ne $expectedArguments -or
        ($null -ne $workingDirectory -and
            -not [string]::IsNullOrWhiteSpace([string]$workingDirectory.InnerText))) {
        throw 'G3 LIVE target action is not the exact reviewed approved launcher.'
    }
}

function Assert-TargetIsEnabledApproved {
    $task = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $TargetTask -ErrorAction Stop
    if (-not $task.Settings.Enabled -or [string]$task.State -ne 'Ready' -or
        [bool]$task.Settings.StartWhenAvailable -or
        [int]$task.Settings.RestartCount -ne 0 -or
        [string]$task.Settings.MultipleInstances -ne 'IgnoreNew') {
        throw 'G3 LIVE target is not the reviewed enabled, no-catch-up Ready task.'
    }
    $actions = @($task.Actions)
    $expectedCommand = Join-Path $env:SystemRoot 'System32\cmd.exe'
    $expectedLauncher = Join-Path $PSScriptRoot 'run_g3_approved_live.bat'
    $expectedArguments = '/d /c ""' + $expectedLauncher + '""'
    if ($actions.Count -ne 1 -or $actions[0].Execute -ne $expectedCommand -or
        $actions[0].Arguments -ne $expectedArguments -or
        -not [string]::IsNullOrWhiteSpace([string]$actions[0].WorkingDirectory)) {
        throw 'G3 LIVE target does not use only the reviewed approved launcher.'
    }
    if ([string]$task.Principal.LogonType -ne 'Interactive' -or
        [string]$task.Principal.RunLevel -ne 'Limited') {
        throw 'G3 LIVE target principal differs from Interactive/Limited.'
    }
    $expectedRun = [datetime]::ParseExact(
        '2026-10-22 09:15:00', 'yyyy-MM-dd HH:mm:ss',
        [Globalization.CultureInfo]::InvariantCulture
    )
    if ($info.NextRunTime -ne $expectedRun) {
        throw 'G3 LIVE target does not have the exact 2026-10-22 09:15 next run.'
    }
    $triggers = @($task.Triggers)
    if ($triggers.Count -ne 1 -or
        ([datetime]$triggers[0].StartBoundary).TimeOfDay -ne [timespan]::FromHours(9.25)) {
        throw 'G3 LIVE target no longer has its reviewed single 09:15 trigger.'
    }
    Assert-ReviewedTargetDefinitionXml
}

function Disable-G3TargetFailClosed {
    Disable-ScheduledTask -TaskName $TargetTask -ErrorAction Stop | Out-Null
    $disabled = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    if ($disabled.Settings.Enabled -or [string]$disabled.State -ne 'Disabled') {
        throw 'CRITICAL: G3 LIVE target could not be verified disabled.'
    }
}

function Assert-SameFileHash([string]$Expected, [string]$Actual, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Expected -PathType Leaf) -or
        -not (Test-Path -LiteralPath $Actual -PathType Leaf) -or
        (Get-FileHash -LiteralPath $Expected -Algorithm SHA256).Hash -ne
            (Get-FileHash -LiteralPath $Actual -Algorithm SHA256).Hash) {
        throw "Failed to verify restored ${Label}."
    }
}

function Assert-HttpsClockSkew {
    Add-Type -AssemblyName System.Net.Http -ErrorAction Stop
    $uri = [uri]'https://api.kite.trade/'
    $handler = New-Object System.Net.Http.HttpClientHandler
    $client = New-Object System.Net.Http.HttpClient($handler)
    $request = New-Object System.Net.Http.HttpRequestMessage(
        [System.Net.Http.HttpMethod]::Head, $uri
    )
    $response = $null
    $client.Timeout = [timespan]::FromSeconds(10)
    $sent = [datetimeoffset]::UtcNow
    try {
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        $received = [datetimeoffset]::UtcNow
        $dateHeader = $response.Headers.Date
        if ($null -eq $dateHeader) {
            throw 'Kite HTTPS response did not contain a Date header.'
        }
        $roundTrip = ($received - $sent).TotalSeconds
        if ($roundTrip -lt 0 -or $roundTrip -gt 15) {
            throw 'Kite HTTPS clock check exceeded the bounded request duration.'
        }
        $midpoint = $sent.AddTicks([long](($received - $sent).Ticks / 2))
        $server = ([datetimeoffset]$dateHeader).ToUniversalTime()
        $skew = [math]::Abs(($server - $midpoint).TotalSeconds)
        if ($skew -gt $ClockMaxSkewSeconds) {
            throw "Local clock differs from the Kite HTTPS Date header by $([math]::Round($skew, 3)) seconds."
        }
    } finally {
        if ($null -ne $response) { $response.Dispose() }
        $request.Dispose()
        $client.Dispose()
        $handler.Dispose()
    }
}

function Assert-NoG3LiveWorkers {
    $workers = @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        $command = [string]$_.CommandLine
        ($_.Name -like 'python*' -and (
            $command -like '*fno_v13_v10_g_3_live_kite_session.py*' -or
            ($command -like '*fno_v13_v10_g_3_live.py*' -and
                $command -match '--execution-mode[ ="]+LIVE')
        )) -or
        ($_.Name -like 'powershell*' -and
            $command -like '*supervise_command.ps1*' -and
            $command -like '*fno_v13_v10_g_3_live_kite_qty1*') -or
        ($_.Name -like 'cmd*' -and
            $command -like '*activate_fno_v13_v10_g_3_live_kite_qty1.bat*')
    })
    if ($workers.Count) {
        throw 'A G3 LIVE coordinator, worker, or supervisor is already running.'
    }
}

function Assert-StalePriorDayArmCannotAuthorizeCurrentSession {
    $session = [datetime]::ParseExact(
        $SessionDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture
    ).Date
    $profile = Read-StrictJsonWithRetry $ProfilePath
    if ($profile.profile -ne 'V13_V10_G_3' -or
        $profile.strategy_version -ne $ExpectedStrategyVersion -or
        $profile.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $profile.live_execution_policy.live_execution_policy_fingerprint -ne
            $ExpectedPolicyFingerprint) {
        throw 'The pre-approval deployment identity is not the reviewed G3 strategy and policy.'
    }

    $approvalDateText = [string]$profile.live_approval.session_date
    if ($profile.live_activation_authorized -eq $true) {
        $approvalDate = [datetime]::ParseExact(
            $approvalDateText, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture
        ).Date
        if ($approvalDate -ge $session) {
            throw 'An unexpected same-day or future profile authorization already exists.'
        }
    }

    if (-not (Test-Path -LiteralPath $ArmPath -PathType Leaf)) {
        return
    }
    $arm = Read-StrictJsonWithRetry $ArmPath
    if ($arm.enabled -ne $true) { return }
    $armDate = [datetime]::ParseExact(
        [string]$arm.session_date, 'yyyy-MM-dd',
        [Globalization.CultureInfo]::InvariantCulture
    ).Date
    if ($armDate -ge $session) {
        throw 'An unexpected same-day or future LIVE arm already exists.'
    }
    if ([string]$arm.session_date -ne $PriorSessionDate -or
        $arm.strategy_version -ne $ExpectedPriorStrategyVersion -or
        $arm.strategy_fingerprint -ne $ExpectedPriorStrategyFingerprint -or
        $arm.live_execution_policy_fingerprint -ne $ExpectedPriorPolicyFingerprint) {
        throw 'The stale LIVE arm is not the reviewed prior-session authorization.'
    }
    # run_g3_approved_live.bat delegates to validate_launch, which binds both
    # the approval and arm to now.date().  A 2026-10-21 arm therefore cannot
    # authorize the 2026-10-22 target if this one-time gate never succeeds.
}

function Assert-AuthenticationReady([datetime]$Now) {
    $task = Get-ScheduledTask -TaskName $AuthTask -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $AuthTask -ErrorAction Stop
    if ([string]$task.State -eq 'Running' -or
        $info.LastRunTime.Date -ne $Now.Date -or
        [int]$info.LastTaskResult -ne 0) {
        throw 'Authentication task has not completed successfully for this session.'
    }
    $requiredKeys = @('status', 'script', 'ts', 'exit_code', 'log_file')
    $status = Read-StrictLiteralKeyValueFile -Path $AuthStatusPath -RequiredKeys $requiredKeys
    if ((@($status.Keys | Sort-Object) -join '|') -ne
        (@($requiredKeys | Sort-Object) -join '|')) {
        throw 'Authentication status contains a missing or unreviewed key.'
    }
    $stamp = [datetime]::ParseExact(
        [string]$status.ts, 'yyyy-MM-dd_HH:mm:ss',
        [Globalization.CultureInfo]::InvariantCulture
    )
    $age = ($Now - $stamp).TotalSeconds
    if ($status.status -ne 'SUCCESS' -or
        $status.script -ne 'authentication_v2.py' -or
        [int]$status.exit_code -ne 0 -or
        $status.log_file -ne $ExpectedAuthLogPath -or
        $stamp.Date -ne $Now.Date -or $age -lt -5 -or
        $age -gt $AuthMaxAgeSeconds) {
        throw 'Authentication status file is not a strict, fresh, same-session success.'
    }
}

function Assert-PreopenHealth([datetime]$Now) {
    $health = Read-StrictJsonWithRetry $HealthPath
    $stamp = [datetimeoffset]::Parse(
        [string]$health.ts_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $age = ([datetimeoffset](Get-Date) - $stamp).TotalSeconds
    $notBefore = $Now.Date.AddHours(9).AddMinutes(6)
    if ($stamp.Date -ne $Now.Date -or $stamp.LocalDateTime -lt $notBefore -or
        $age -lt -5 -or $age -gt 120) {
        throw 'Pre-open health evidence is stale, early, or from another session.'
    }
    $checks = @($health.checks)
    if (-not $checks.Count) { throw 'Pre-open health evidence has no checks.' }
    $failed = @($checks | Where-Object { $_.status -eq 'FAIL' })
    $warned = @($checks | Where-Object { $_.status -eq 'WARN' })
    $unknown = @($checks | Where-Object { $_.status -notin @('PASS', 'WARN', 'FAIL') })
    if ($failed.Count -or $unknown.Count) {
        throw 'Pre-open health contains a failed or unknown gate.'
    }
    if ([string]$health.overall -eq 'WAIT') {
        if ($warned.Count -ne 1 -or
            [string]$warned[0].name -ne 'fno_fast_production_first_slot' -or
            [string]$warned[0].detail -ne $ExpectedHealthWarning) {
            throw 'Pre-open WAIT contains a warning other than the reviewed pre-09:20 marker wait.'
        }
    } elseif ([string]$health.overall -eq 'PASS') {
        if ($warned.Count) { throw 'Pre-open PASS contains a warning.' }
    } else {
        throw 'Pre-open overall state is not an acceptable PASS/expected WAIT.'
    }
    foreach ($required in @(
        'task_EQIDV2_authentication_v2_0900',
        'task_EQIDV2_fno_v13_v10_g_3_scanner_5min_0915',
        'task_EQIDV2_fno_v13_v10_g_3_equity_1min_feed_0915',
        'task_EQIDV2_fno_v13_v10_g_3_confirmation_1min_0915',
        'task_EQIDV2_fno_v13_v10_g_3_live_long_0915',
        'task_EQIDV2_fno_v13_v10_g_3_live_short_0915',
        'task_EQIDV2_fno_v13_v10_g_3_trade_logger_0915',
        'task_EQIDV2_fno_v13_v10_g_3_net_result_0915',
        'task_EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915',
        'authentication_v2',
        'fno_fast_production_runtime',
        'preopen_healthcheck'
    )) {
        $match = @($checks | Where-Object { $_.name -eq $required })
        if ($match.Count -ne 1 -or [string]$match[0].status -ne 'PASS') {
            throw "Required pre-open gate did not pass: $required"
        }
    }
}

function Refresh-PreopenHealth {
    $output = @(
        & $Python $HealthCheck --max-age-min 35 --report-path $HealthRefreshLog
    )
    $output | ForEach-Object { Write-G3ActivationLog ([string]$_) }
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh pre-open healthcheck reported a failed gate.'
    }
}

function Assert-FreshBrokerFlatReadReconciliation([datetime]$Now, $Profile) {
    $report = Read-StrictJsonWithRetry $BrokerReportPath
    $completed = [datetimeoffset]::Parse(
        [string]$report.completed_at_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $age = ([datetimeoffset]$Now - $completed).TotalSeconds
    if ($completed.Date -ne $Now.Date -or $age -lt -5 -or $age -gt 60 -or
        $report.mode -ne 'VALIDATE_ONLY' -or
        [int]$report.orders_submitted -ne 0 -or
        $report.order_mutations_available -ne $false -or
        [double]$report.minimum_live_balance_rupees -ne 20000.0 -or
        $report.state -ne 'READ_ACCESS_CONFIRMED' -or
        $report.market.date -ne $SessionDate -or
        $report.market.scheduled_trading_day -ne $true) {
        throw 'Broker read-only reconciliation is stale, unsafe, or for another session.'
    }
    $apps = @($report.apps | Where-Object { $_.credential_files_present -eq $true })
    $expectedApps = @('app1', 'app2', 'app3', 'app4', 'app5', 'app6', 'app7', 'app8')
    if ($apps.Count -ne $expectedApps.Count -or
        @($apps | Where-Object { $_.app -notin $expectedApps }).Count -or
        @($expectedApps | Where-Object { $_ -notin @($apps.app) }).Count) {
        throw 'Broker reconciliation did not cover exactly the eight configured applications.'
    }
    foreach ($app in $apps) {
        if ($app.state -ne 'READ_ACCESS_CONFIRMED' -or
            [int]$app.open_net_positions -ne 0 -or
            [int]$app.nonterminal_orders -ne 0 -or
            $app.funds_positive -ne $true -or
            [double]$app.minimum_live_balance_rupees -ne 20000.0 -or
            $app.live_balance_covers_minimum -ne $true -or
            $app.equity_segment_enabled -ne $true -or
            $app.checks.profile -ne 'PASS' -or
            $app.checks.margins -ne 'PASS' -or
            $app.checks.orders -ne 'PASS' -or
            $app.checks.positions -ne 'PASS') {
            throw "Broker application is not read-confirmed, funded, and flat: $($app.app)"
        }
        foreach ($capability in @('nse', 'mis', 'limit', 'market', 'sl')) {
            if ($app.capabilities.$capability -ne $true) {
                throw "Broker application lacks capability ${capability}: $($app.app)"
            }
        }
    }
    if ([string]$Profile.live_approval.broker_checked_at_ist -ne
        [string]$report.completed_at_ist) {
        throw 'The dated approval is not bound to this broker reconciliation.'
    }
}

function Assert-ApprovedState([datetime]$Now) {
    Assert-TargetIsEnabledApproved
    $profile = Read-StrictJsonWithRetry $ProfilePath
    $arm = Read-StrictJsonWithRetry $ArmPath
    if ($profile.profile -ne 'V13_V10_G_3' -or
        $profile.strategy_version -ne $ExpectedStrategyVersion -or
        $profile.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $profile.deployment_stage -ne 'LIVE_APPROVED' -or
        $profile.live_activation_authorized -ne $true -or
        $profile.live_approval.source -ne 'EXPLICIT_OPERATOR_APPLY' -or
        $profile.live_approval.session_date -ne $SessionDate -or
        $profile.live_approval.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $profile.live_approval.live_execution_policy_fingerprint -ne
            $ExpectedPolicyFingerprint -or
        @($profile.live_approval.setup_ids).Count -ne $ExpectedSetupCount -or
        $profile.live_execution_policy.live_execution_policy_fingerprint -ne
            $ExpectedPolicyFingerprint -or
        $arm.enabled -ne $true -or
        $arm.session_date -ne $SessionDate -or
        $arm.strategy_version -ne $ExpectedStrategyVersion -or
        $arm.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $arm.live_execution_policy_fingerprint -ne $ExpectedPolicyFingerprint -or
        @($arm.setup_ids).Count -ne $ExpectedSetupCount -or
        (@($profile.live_approval.setup_ids) -join '|') -ne (@($arm.setup_ids) -join '|') -or
        $profile.live_approval.entry_not_before_ist -ne $arm.entry_not_before_ist) {
        throw 'G3 LIVE dated approval, arm, setup set, strategy, or policy did not reconcile.'
    }
    $entryEpoch = [datetimeoffset]::Parse(
        [string]$arm.entry_not_before_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    if ($entryEpoch.Date -ne $Now.Date -or $entryEpoch -gt [datetimeoffset]$Now) {
        throw 'G3 LIVE entry epoch is not a valid timestamp for this session.'
    }
    if ($profile.live_execution_policy.capital_per_trade_rs -ne 10000.0 -or
        $profile.live_execution_policy.leverage -ne 5.0 -or
        $profile.live_execution_policy.target_exposure_rs -ne 50000.0 -or
        $null -ne $profile.live_execution_policy.max_reserved_open_positions -or
        $null -ne $profile.live_execution_policy.max_daily_entries -or
        $null -ne $profile.live_execution_policy.max_reserved_gross_exposure_rs) {
        throw 'G3 LIVE capital or unlimited portfolio-admission policy differs from the approved policy.'
    }
    Assert-FreshBrokerFlatReadReconciliation -Now $Now -Profile $profile
}

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Frozen Python interpreter is missing: $Python"
}
if (-not (Test-Path -LiteralPath $Approval -PathType Leaf)) {
    throw "Approval wrapper is missing: $Approval"
}
if (-not (Test-Path -LiteralPath $HealthCheck -PathType Leaf)) {
    throw "Healthcheck is missing: $HealthCheck"
}
if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
    throw 'The activation host is not using India Standard Time.'
}

$activationMutex = New-Object System.Threading.Mutex(
    $false, 'Global\EQIDV2_G3_LIVE_ACTIVATION_20261022'
)
if (-not $activationMutex.WaitOne(0)) {
    Disable-G3TargetFailClosed
    throw 'Another 22 October G3 activation attempt is already running; target disabled fail-closed.'
}
$approvalTransactionStarted = $false
$gateBackup = $null
$hadPriorArm = $false
try {
    $now = Get-Date
    $session = [datetime]::ParseExact(
        $SessionDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture
    )
    $windowStart = $session.Date.AddHours(9).AddMinutes(9)
    $windowEnd = $session.Date.AddHours(9).AddMinutes(13)
    if ($now.Date -ne $session.Date -or $now -lt $windowStart -or $now -ge $windowEnd) {
        throw 'This approval is valid only on 2026-10-22 from 09:09:00 through 09:12:59 IST.'
    }

    Write-G3ActivationLog 'Starting fail-closed dated activation checks.'
    Assert-HttpsClockSkew
    Assert-TargetIsEnabledApproved
    Assert-NoG3LiveWorkers
    Assert-StalePriorDayArmCannotAuthorizeCurrentSession
    Assert-AuthenticationReady $now
    Refresh-PreopenHealth
    Assert-PreopenHealth $now
    Assert-TargetIsEnabledApproved
    Assert-NoG3LiveWorkers

    $gateBackup = Join-Path $Workspace (
        '.codex_tmp/g3_live_activation_gate_20261022_' +
        (Get-Date -Format 'yyyyMMddTHHmmssfff')
    )
    New-Item -ItemType Directory -Path $gateBackup | Out-Null
    Copy-Item -LiteralPath $ProfilePath -Destination (
        Join-Path $gateBackup 'active_profile.json'
    ) -Force
    $hadPriorArm = Test-Path -LiteralPath $ArmPath -PathType Leaf
    if ($hadPriorArm) {
        Copy-Item -LiteralPath $ArmPath -Destination (
            Join-Path $gateBackup 'live_arm.json'
        ) -Force
    }
    Export-ScheduledTask -TaskName $TargetTask -ErrorAction Stop |
        Set-Content -LiteralPath (Join-Path $gateBackup 'target_task.xml') -Encoding UTF8
    $approvalTransactionStarted = $true

    $approvalOutput = @(
        & $Approval -SessionDate $SessionDate -Apply -Python $Python -EnableBefore '09:12:30'
    )
    if ($LASTEXITCODE -ne 0) { throw 'Dated approval wrapper returned a failure.' }
    $approvalOutput | ForEach-Object { Write-G3ActivationLog ([string]$_) }

    $verifiedAt = Get-Date
    Assert-NoG3LiveWorkers
    Assert-ApprovedState $verifiedAt
    Assert-HttpsClockSkew
    Write-G3ActivationLog 'SUCCESS: G3 LIVE is approved only for 2026-10-22; the existing 09:15 task remains enabled and was not started.'
} catch {
    $originalFailure = $_.Exception.Message
    $cleanupFailures = New-Object System.Collections.Generic.List[string]
    try {
        Disable-G3TargetFailClosed
    } catch {
        $cleanupFailures.Add($_.Exception.Message)
    }
    if ($approvalTransactionStarted) {
        try {
            $savedProfile = Join-Path $gateBackup 'active_profile.json'
            Copy-Item -LiteralPath $savedProfile -Destination $ProfilePath -Force
            Assert-SameFileHash -Expected $savedProfile -Actual $ProfilePath -Label 'pre-gate profile'
            $savedArm = Join-Path $gateBackup 'live_arm.json'
            if ($hadPriorArm) {
                Copy-Item -LiteralPath $savedArm -Destination $ArmPath -Force
                Assert-SameFileHash -Expected $savedArm -Actual $ArmPath -Label 'pre-gate arm'
            } elseif (Test-Path -LiteralPath $ArmPath) {
                Remove-Item -LiteralPath $ArmPath -Force
                if (Test-Path -LiteralPath $ArmPath) {
                    throw 'Failed to remove the newly created dated arm.'
                }
            }
        } catch {
            $cleanupFailures.Add($_.Exception.Message)
        }
    }
    $message = "BLOCKED: $originalFailure"
    if ($cleanupFailures.Count) {
        $message += ' FAIL-CLOSED CLEANUP ERROR: ' + ($cleanupFailures -join ' | ')
    }
    Write-G3ActivationLog $message
    throw $message
} finally {
    $activationMutex.ReleaseMutex()
    $activationMutex.Dispose()
}
