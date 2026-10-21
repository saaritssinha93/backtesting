[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$TargetTask = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
$AuthTask = 'EQIDV2_authentication_v2_0900'
$Workspace = Split-Path $PSScriptRoot -Parent
$Python = 'C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe'
$Operator = Join-Path $Workspace 'tools\g3_live_operator.py'
$BrokerProbe = Join-Path $Workspace 'tools\test_kite_order_readiness.py'
$StandingPolicyPath = Join-Path $Workspace 'configs\authorizations\g3_live_standing_policy_v1.json'
$HealthCheck = Join-Path $Workspace 'preopen_session_healthcheck.py'
$HealthPath = Join-Path $Workspace 'logs\preopen_session_healthcheck_latest.json'
$HealthRefreshLog = Join-Path $Workspace 'logs\g3_live_standing_healthcheck.log'
$AuthStatusPath = Join-Path $Workspace 'logs\authentication_v2_runner.status'
$ExpectedAuthLogPath = Join-Path $Workspace 'logs\authentication_v2_runner.log'
$BrokerReportPath = Join-Path $Workspace 'logs\kite_order_readiness\latest.json'
$ProfilePath = Join-Path $Workspace 'configs\strategies\active_profile.json'
$RuntimeRoot = 'C:\TradingData\eqidv2'
$ArmPath = Join-Path $RuntimeRoot 'fno_oi\v13_v10_g_3_live\live_arm.json'
$LogPath = Join-Path $Workspace 'logs\g3_live_standing_activation.log'
$StatusHelpers = Join-Path $PSScriptRoot 'g3_activation_status_helpers.ps1'
$EffectiveDate = [datetime]::ParseExact(
    '2026-10-23', 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture
).Date
$ExpectedHealthWarning = 'acceptance pending: waiting for the first canonical 09:20 stock-futures marker'
$ExpectedStrategyVersion = 'V13-V10-G-3-PRODUCTION-V11'
$ExpectedStrategyFingerprint = '82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4'
$ExpectedPolicyFingerprint = 'cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e'
$ExpectedStandingPolicyId = 'G3_LIVE_STANDING_20261023_V1'
$ExpectedStandingPolicyFingerprint = '25d18e6f028a004159a7caf9ce6e717321852c01435c17c4b3b6efb4ca5f7a43'
$ExpectedSetupIdsSha256 = 'f555f8f81cad15b9c5d33a74a6f277ae0ed5dfe077c07657a19f472b7e5028b5'
$ExpectedSetupIds = @(
    '0921_LONG|0921_SHORT|0926_LONG|0926_SHORT|0931_LONG|0931_SHORT|' +
    '0936_LONG|0936_SHORT|0941_LONG|0941_SHORT|0946_LONG|0946_SHORT|' +
    '0951_LONG|0951_SHORT|0956_LONG|1001_LONG|1011_LONG|1121_SHORT|' +
    '1006_LONG|1011_SHORT|1016_LONG|1016_SHORT|1026_SHORT|1031_SHORT|' +
    '1036_LONG|1036_SHORT|1041_LONG|1046_LONG|1046_SHORT|1051_LONG|' +
    '1056_SHORT|1111_LONG|1111_SHORT|1126_SHORT|1136_LONG|1136_SHORT|' +
    '1141_LONG|1141_SHORT|1146_SHORT|1206_LONG|1211_SHORT|1216_LONG|' +
    '1216_SHORT|1226_SHORT|1231_LONG|1231_SHORT|1236_LONG|1236_SHORT|' +
    '1246_LONG|1246_SHORT|1251_LONG|1251_SHORT|1256_SHORT|1301_SHORT|' +
    '1306_LONG|1316_LONG|1316_SHORT|1321_SHORT|1326_LONG|1326_SHORT|1331_LONG'
).Split('|')
$ExpectedSetupCount = 61
$AuthMaxAgeSeconds = 5400
$HealthMaxAgeSeconds = 120
$BrokerMaxAgeSeconds = 60
$ClockMaxSkewSeconds = 30

if (-not (Test-Path -LiteralPath $StatusHelpers -PathType Leaf)) {
    throw "Activation status parser is missing: $StatusHelpers"
}
. $StatusHelpers

function Write-G3StandingActivationLog([string]$Message) {
    try {
        $parent = Split-Path $LogPath -Parent
        if (-not (Test-Path -LiteralPath $parent)) {
            New-Item -ItemType Directory -Path $parent | Out-Null
        }
        Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value (
            "[$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))] $Message"
        )
    } catch {
        # Logging is evidence only and can never weaken a failed safety gate.
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

function Assert-ExactPropertySet($Object, [string[]]$Names, [string]$Label) {
    if ($null -eq $Object) { throw "$Label is missing." }
    $actual = @($Object.PSObject.Properties.Name | Sort-Object)
    $expected = @($Names | Sort-Object)
    if (($actual -join '|') -ne ($expected -join '|')) {
        throw "$Label contains a missing or unreviewed field."
    }
}

function Assert-ExactSetupIds($Actual, [string]$Label) {
    $ids = @($Actual)
    if ($ids.Count -ne $ExpectedSetupCount -or
        ($ids -join '|') -ne ($ExpectedSetupIds -join '|')) {
        throw "$Label does not contain the exact reviewed ordered 61-setup set."
    }
}

function Assert-LiveExecutionPolicy($Policy, [string]$Label) {
    Assert-ExactPropertySet $Policy @(
        'live_execution_policy_schema_version',
        'live_execution_policy_fingerprint',
        'live_execution_profile',
        'quantity_policy',
        'capital_per_trade_rs',
        'leverage',
        'target_exposure_rs',
        'max_reserved_open_positions',
        'max_daily_entries',
        'max_reserved_gross_exposure_rs'
    ) $Label
    if ($Policy.live_execution_policy_schema_version -ne 'g3_live_execution_policy_v1' -or
        $Policy.live_execution_policy_fingerprint -ne $ExpectedPolicyFingerprint -or
        $Policy.live_execution_profile -ne 'G3_LIVE_10K_5X' -or
        $Policy.quantity_policy -ne 'FLOOR_TARGET_EXPOSURE_EQUITY_SHARES' -or
        [double]$Policy.capital_per_trade_rs -ne 10000.0 -or
        [double]$Policy.leverage -ne 5.0 -or
        [double]$Policy.target_exposure_rs -ne 50000.0 -or
        $null -ne $Policy.max_reserved_open_positions -or
        $null -ne $Policy.max_daily_entries -or
        $null -ne $Policy.max_reserved_gross_exposure_rs) {
        throw "$Label differs from the reviewed INR 10,000 / 5x / unlimited portfolio-admission policy."
    }
}

function Assert-StandingPolicyArtifact {
    $policy = Read-StrictJsonWithRetry $StandingPolicyPath
    Assert-ExactPropertySet $policy @(
        'schema_version', 'source', 'policy_id', 'effective_from_session_date',
        'regular_verified_nse_sessions_only', 'enabled', 'revoked',
        'strategy_profile', 'strategy_version', 'strategy_fingerprint',
        'setup_ids', 'setup_ids_sha256', 'live_execution_policy',
        'standing_policy_fingerprint'
    ) 'Standing operator policy'
    if ($policy.schema_version -ne 'g3_live_standing_operator_policy_v1' -or
        $policy.source -ne 'STANDING_OPERATOR_POLICY' -or
        $policy.policy_id -ne $ExpectedStandingPolicyId -or
        $policy.effective_from_session_date -ne '2026-10-23' -or
        $policy.regular_verified_nse_sessions_only -ne $true -or
        $policy.enabled -ne $true -or $policy.revoked -ne $false -or
        $policy.strategy_profile -ne 'V13_V10_G_3' -or
        $policy.strategy_version -ne $ExpectedStrategyVersion -or
        $policy.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $policy.setup_ids_sha256 -ne $ExpectedSetupIdsSha256 -or
        $policy.standing_policy_fingerprint -ne $ExpectedStandingPolicyFingerprint) {
        throw 'Standing operator policy identity, effective date, or fingerprint differs from the reviewed artifact.'
    }
    Assert-ExactSetupIds $policy.setup_ids 'Standing operator policy'
    Assert-LiveExecutionPolicy $policy.live_execution_policy 'Standing operator policy LIVE limits'
    return $policy
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
        throw 'G3 LIVE target is not the reviewed non-repeating Monday-Friday 09:15 trigger.'
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

function Assert-TargetState([bool]$ExpectEnabled, [datetime]$SessionDate) {
    $task = Get-ScheduledTask -TaskName $TargetTask -ErrorAction Stop
    if ([bool]$task.Settings.StartWhenAvailable -or
        [int]$task.Settings.RestartCount -ne 0 -or
        [string]$task.Settings.MultipleInstances -ne 'IgnoreNew' -or
        [string]$task.Principal.LogonType -ne 'Interactive' -or
        [string]$task.Principal.RunLevel -ne 'Limited') {
        throw 'G3 LIVE target settings or principal differ from the reviewed fail-closed task.'
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
    if ($ExpectEnabled) {
        $info = Get-ScheduledTaskInfo -TaskName $TargetTask -ErrorAction Stop
        $expectedRun = $SessionDate.Date.AddHours(9).AddMinutes(15)
        if (-not $task.Settings.Enabled -or [string]$task.State -ne 'Ready' -or
            $info.NextRunTime -ne $expectedRun) {
            throw 'G3 LIVE target is not enabled/Ready for exactly 09:15 in this session.'
        }
    } elseif ($task.Settings.Enabled -or [string]$task.State -ne 'Disabled') {
        throw 'G3 LIVE target could not be verified disabled during gate evaluation.'
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

function Assert-BeforeDeadline([datetime]$SessionDate) {
    $deadline = $SessionDate.Date.AddHours(9).AddMinutes(12).AddSeconds(30)
    if ((Get-Date) -ge $deadline) {
        throw 'G3 standing activation missed the 09:12:30 IST enable deadline.'
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
        throw 'A G3 LIVE coordinator, worker, launcher, or supervisor is already running.'
    }
}

function Assert-AllKillSwitchesClear {
    foreach ($name in @('v13_v10_g_3_live', 'v6_live', 'v13_v10_g_live', 'control')) {
        $path = Join-Path $RuntimeRoot "fno_oi\$name\kill_switch.json"
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { continue }
        $value = Read-StrictJsonWithRetry $path
        if ($value.enabled -ne $false) {
            throw "G3 activation kill switch is not explicitly clear: $name"
        }
    }
}

function Assert-PreApprovalState([datetime]$SessionDate) {
    $profile = Read-StrictJsonWithRetry $ProfilePath
    if ($profile.profile -ne 'V13_V10_G_3' -or
        $profile.strategy_version -ne $ExpectedStrategyVersion -or
        $profile.strategy_fingerprint -ne $ExpectedStrategyFingerprint) {
        throw 'The deployment identity is not the exact reviewed G3 production strategy.'
    }
    Assert-LiveExecutionPolicy $profile.live_execution_policy 'Active profile LIVE limits'
    if ($profile.live_activation_authorized -eq $true) {
        $approvalDate = [datetime]::ParseExact(
            [string]$profile.live_approval.session_date, 'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture
        ).Date
        if ($approvalDate -ge $SessionDate.Date) {
            throw 'An unexpected same-day or future profile authorization already exists.'
        }
    }
    if (-not (Test-Path -LiteralPath $ArmPath -PathType Leaf)) { return }
    $arm = Read-StrictJsonWithRetry $ArmPath
    $armDate = [datetime]::ParseExact(
        [string]$arm.session_date, 'yyyy-MM-dd',
        [Globalization.CultureInfo]::InvariantCulture
    ).Date
    if ($armDate -ge $SessionDate.Date) {
        throw 'An unexpected same-day or future LIVE arm already exists.'
    }
    if ($arm.strategy_version -ne $ExpectedStrategyVersion -or
        $arm.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $arm.live_execution_policy_fingerprint -ne $ExpectedPolicyFingerprint) {
        throw 'The prior dated arm does not match the exact reviewed G3 identity.'
    }
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
        throw 'Authentication status is not a strict, fresh, same-session success.'
    }
}

function Assert-PreopenHealth([datetime]$Now) {
    $health = Read-StrictJsonWithRetry $HealthPath
    $stamp = [datetimeoffset]::Parse(
        [string]$health.ts_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $age = ([datetimeoffset]$Now - $stamp).TotalSeconds
    $notBefore = $Now.Date.AddHours(9).AddMinutes(6)
    if ($stamp.Date -ne $Now.Date -or $stamp.LocalDateTime -lt $notBefore -or
        $age -lt -5 -or $age -gt $HealthMaxAgeSeconds) {
        throw 'Pre-open health evidence is stale, early, or from another session.'
    }
    $checks = @($health.checks)
    if (-not $checks.Count) { throw 'Pre-open health evidence has no checks.' }
    $failed = @($checks | Where-Object { $_.status -eq 'FAIL' })
    $warned = @($checks | Where-Object { $_.status -eq 'WARN' })
    $unknown = @($checks | Where-Object { $_.status -notin @('PASS', 'WARN', 'FAIL') })
    if ($health.overall -ne 'WAIT' -or $failed.Count -or $unknown.Count -or
        $warned.Count -ne 1 -or
        [string]$warned[0].name -ne 'fno_fast_production_first_slot' -or
        [string]$warned[0].detail -ne $ExpectedHealthWarning) {
        throw 'Pre-open health is not the reviewed WAIT with only the expected pre-09:20 warning.'
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
            throw "Required pre-open gate did not pass exactly once: $required"
        }
    }
    # PASS details are intentionally not constrained. In particular, the G3
    # target's existing "session not enabled" detail is the expected recovery
    # state after a prior day failed closed.
}

function Refresh-PreopenHealth {
    $output = @(
        & $Python $HealthCheck --max-age-min 35 --report-path $HealthRefreshLog
    )
    $output | ForEach-Object { Write-G3StandingActivationLog ([string]$_) }
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh pre-open healthcheck reported a failed gate.'
    }
}

function Assert-FreshBrokerFlatReadReconciliation([datetime]$Now) {
    $report = Read-StrictJsonWithRetry $BrokerReportPath
    $checked = [datetimeoffset]::Parse(
        [string]$report.checked_at_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $completed = [datetimeoffset]::Parse(
        [string]$report.completed_at_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $nowOffset = [datetimeoffset]$Now
    $age = ($nowOffset - $completed).TotalSeconds
    $duration = ($completed - $checked).TotalSeconds
    $sessionText = $Now.ToString('yyyy-MM-dd')
    if ($checked.Offset -ne [timespan]::FromHours(5.5) -or
        $completed.Offset -ne [timespan]::FromHours(5.5) -or
        $completed.Date -ne $Now.Date -or $checked.Date -ne $Now.Date -or
        $duration -lt 0 -or $duration -gt 150 -or
        $age -lt 0 -or $age -gt $BrokerMaxAgeSeconds -or
        $report.schema_version -ne 'kite_order_readiness_validation_only_v1' -or
        $report.mode -ne 'VALIDATE_ONLY' -or
        [int]$report.orders_submitted -ne 0 -or
        $report.order_mutations_available -ne $false -or
        $report.strategy_controls_changed -ne $false -or
        [double]$report.minimum_live_balance_rupees -ne 20000.0 -or
        $report.state -ne 'READ_ACCESS_CONFIRMED' -or
        $report.market.date -ne $sessionText -or
        $report.market.state -ne 'BEFORE_REGULAR_SESSION' -or
        $report.market.scheduled_trading_day -ne $true -or
        $report.market.schedule_only -ne $true -or
        $report.market.source -ne 'https://www.nseindia.com/resources/exchange-communication-holidays?apiversion=v2' -or
        $report.market.regular_open_ist -ne '09:15' -or
        $report.market.regular_close_ist -ne '15:30') {
        throw 'Broker reconciliation or verified same-day regular-session calendar evidence is stale or unsafe.'
    }
    $apps = @($report.apps)
    $expectedApps = @('app1', 'app2', 'app3', 'app4', 'app5', 'app6', 'app7', 'app8')
    if ($apps.Count -ne $expectedApps.Count -or
        (@($apps.app | Sort-Object) -join '|') -ne
            (@($expectedApps | Sort-Object) -join '|') -or
        (@($report.readable_apps | Sort-Object) -join '|') -ne
            (@($expectedApps | Sort-Object) -join '|')) {
        throw 'Broker reconciliation did not cover exactly app1 through app8 once each.'
    }
    foreach ($app in $apps) {
        if ($app.credential_files_present -ne $true -or
            $app.state -ne 'READ_ACCESS_CONFIRMED' -or
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
            throw "Broker application is not readable, funded, capable, and flat: $($app.app)"
        }
        foreach ($capability in @('nse', 'mis', 'limit', 'market', 'sl', 'sl_m')) {
            if ($app.capabilities.$capability -ne $true) {
                throw "Broker application lacks capability ${capability}: $($app.app)"
            }
        }
    }
    return $report
}

function Assert-ApprovedStandingState(
    [datetime]$Now,
    [datetime]$ApprovalStarted,
    $Policy,
    $BrokerReport
) {
    $sessionText = $Now.ToString('yyyy-MM-dd')
    $profile = Read-StrictJsonWithRetry $ProfilePath
    $arm = Read-StrictJsonWithRetry $ArmPath
    Assert-ExactPropertySet $profile.live_approval @(
        'source', 'approved_at_ist', 'broker_checked_at_ist',
        'entry_not_before_ist', 'session_date', 'strategy_fingerprint',
        'live_execution_policy_fingerprint', 'setup_ids', 'paper_validation',
        'standing_policy_id', 'standing_policy_fingerprint', 'setup_ids_sha256'
    ) 'Dated standing approval'
    Assert-ExactPropertySet $arm @(
        'enabled', 'session_date', 'strategy_version', 'strategy_fingerprint',
        'live_execution_policy_fingerprint', 'source', 'setup_ids',
        'entry_not_before_ist', 'updated_at_ist', 'approved_at_ist',
        'broker_checked_at_ist', 'standing_policy_id',
        'standing_policy_fingerprint', 'setup_ids_sha256'
    ) 'Dated standing arm'
    if ($profile.profile -ne 'V13_V10_G_3' -or
        $profile.strategy_version -ne $ExpectedStrategyVersion -or
        $profile.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $profile.deployment_stage -ne 'LIVE_APPROVED' -or
        $profile.live_activation_authorized -ne $true -or
        $profile.live_approval.source -ne 'STANDING_OPERATOR_POLICY' -or
        $profile.live_approval.session_date -ne $sessionText -or
        $profile.live_approval.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $profile.live_approval.live_execution_policy_fingerprint -ne $ExpectedPolicyFingerprint -or
        $profile.live_approval.paper_validation -ne
            'STANDING_OPERATOR_POLICY_OVERRIDE_NOT_ATTESTED' -or
        $profile.live_approval.standing_policy_id -ne $ExpectedStandingPolicyId -or
        $profile.live_approval.standing_policy_fingerprint -ne $ExpectedStandingPolicyFingerprint -or
        $profile.live_approval.setup_ids_sha256 -ne $ExpectedSetupIdsSha256 -or
        $arm.enabled -ne $true -or $arm.session_date -ne $sessionText -or
        $arm.source -ne 'STANDING_OPERATOR_POLICY' -or
        $arm.strategy_version -ne $ExpectedStrategyVersion -or
        $arm.strategy_fingerprint -ne $ExpectedStrategyFingerprint -or
        $arm.live_execution_policy_fingerprint -ne $ExpectedPolicyFingerprint -or
        $arm.standing_policy_id -ne $ExpectedStandingPolicyId -or
        $arm.standing_policy_fingerprint -ne $ExpectedStandingPolicyFingerprint -or
        $arm.setup_ids_sha256 -ne $ExpectedSetupIdsSha256) {
        throw 'Standing-policy dated approval/arm identity or binding did not reconcile.'
    }
    Assert-LiveExecutionPolicy $profile.live_execution_policy 'Approved active profile LIVE limits'
    Assert-ExactSetupIds $profile.live_approval.setup_ids 'Dated standing approval'
    Assert-ExactSetupIds $arm.setup_ids 'Dated standing arm'
    if (($Policy.setup_ids -join '|') -ne ($arm.setup_ids -join '|') -or
        ($profile.live_approval.setup_ids -join '|') -ne ($arm.setup_ids -join '|')) {
        throw 'Standing policy, approval, and arm setup ordering differ.'
    }
    $entryText = [string]$arm.entry_not_before_ist
    if ($profile.live_approval.entry_not_before_ist -ne $entryText -or
        $profile.live_approval.approved_at_ist -ne $entryText -or
        $arm.approved_at_ist -ne $entryText -or
        $arm.updated_at_ist -ne $entryText -or
        $profile.live_approval.broker_checked_at_ist -ne
            [string]$BrokerReport.completed_at_ist -or
        $arm.broker_checked_at_ist -ne
            [string]$BrokerReport.completed_at_ist) {
        throw 'Standing approval is not bound to one entry epoch and the fresh broker report.'
    }
    $entryEpoch = [datetimeoffset]::Parse(
        $entryText,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $brokerEpoch = [datetimeoffset]::Parse(
        [string]$BrokerReport.completed_at_ist,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AllowWhiteSpaces
    )
    $approvalFloor = ([datetimeoffset]$ApprovalStarted).AddSeconds(-2)
    $deadline = ([datetimeoffset]$Now.Date.AddHours(9).AddMinutes(12).AddSeconds(30))
    $brokerAgeAtApproval = ($entryEpoch - $brokerEpoch).TotalSeconds
    if ($entryEpoch.Offset -ne [timespan]::FromHours(5.5) -or
        $entryEpoch.Date -ne $Now.Date -or
        $entryEpoch -lt $approvalFloor -or
        $entryEpoch -gt [datetimeoffset]$Now -or
        $entryEpoch -ge $deadline -or $brokerEpoch -gt $entryEpoch) {
        throw 'Standing approval entry epoch is outside this gate invocation or precedes its broker evidence.'
    }
    if ($brokerAgeAtApproval -lt 0 -or $brokerAgeAtApproval -gt $BrokerMaxAgeSeconds) {
        throw 'Standing approval broker evidence was not fresh at the bound approval epoch.'
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

function Disable-ArmFileFailClosed {
    $arm = $null
    try { $arm = Read-StrictJsonWithRetry $ArmPath } catch { $arm = [pscustomobject]@{} }
    if ($null -eq $arm.PSObject.Properties['enabled']) {
        $arm | Add-Member -NotePropertyName enabled -NotePropertyValue $false
    } else {
        $arm.enabled = $false
    }
    $temporary = $ArmPath + ".standing_gate_disabled_$PID.tmp"
    try {
        $arm | ConvertTo-Json -Depth 20 |
            Set-Content -LiteralPath $temporary -Encoding UTF8
        Move-Item -LiteralPath $temporary -Destination $ArmPath -Force
        $verified = Read-StrictJsonWithRetry $ArmPath
        if ($verified.enabled -ne $false) {
            throw 'New LIVE arm could not be verified disabled.'
        }
    } finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        }
    }
}

foreach ($requiredFile in @($Python, $Operator, $BrokerProbe, $StandingPolicyPath, $HealthCheck)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Standing activation prerequisite is missing: $requiredFile"
    }
}

$activationMutex = New-Object System.Threading.Mutex(
    $false, 'Global\EQIDV2_G3_LIVE_STANDING_ACTIVATION'
)
if (-not $activationMutex.WaitOne(0)) {
    Disable-G3TargetFailClosed
    throw 'Another G3 standing activation gate is running; target disabled fail-closed.'
}

$approvalTransactionStarted = $false
$gateBackup = $null
$hadPriorArm = $false
try {
    # The target is disabled before clock, calendar, auth, health, broker, or
    # authorization evaluation. Enabling it is the final commit only.
    Disable-G3TargetFailClosed
    $now = Get-Date
    Assert-TargetState $false $now.Date

    if ([System.TimeZoneInfo]::Local.Id -ne 'India Standard Time') {
        throw 'The activation host is not using India Standard Time.'
    }
    $session = $now.Date
    $windowStart = $session.AddHours(9).AddMinutes(9)
    $deadline = $session.AddHours(9).AddMinutes(12).AddSeconds(30)
    if ($session -lt $EffectiveDate -or
        [string]$session.DayOfWeek -in @('Saturday', 'Sunday') -or
        $now -lt $windowStart -or $now -ge $deadline) {
        throw 'Standing activation is valid from 2026-10-23 onward on weekdays only, from 09:09:00 through 09:12:29 IST.'
    }

    Write-G3StandingActivationLog "Starting fail-closed standing activation checks for $($session.ToString('yyyy-MM-dd'))."
    Assert-HttpsClockSkew
    Assert-BeforeDeadline $session
    $policy = Assert-StandingPolicyArtifact
    Assert-TargetState $false $session
    Assert-NoG3LiveWorkers
    Assert-AllKillSwitchesClear
    Assert-PreApprovalState $session
    Assert-AuthenticationReady (Get-Date)
    Refresh-PreopenHealth
    Assert-PreopenHealth (Get-Date)

    # Generate new read-only evidence. This program has no order mutation API.
    & $Python $BrokerProbe --app all --minimum-live-balance 20000
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh exact-eight broker read-only reconciliation failed.'
    }
    $brokerReport = Assert-FreshBrokerFlatReadReconciliation (Get-Date)
    Assert-BeforeDeadline $session
    Assert-TargetState $false $session
    Assert-NoG3LiveWorkers
    Assert-AllKillSwitchesClear

    $gateBackup = Join-Path $Workspace (
        '.codex_tmp/g3_live_standing_gate_' +
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
        Set-Content -LiteralPath (Join-Path $gateBackup 'target_task_disabled.xml') -Encoding UTF8
    $policy | ConvertTo-Json -Depth 20 |
        Set-Content -LiteralPath (Join-Path $gateBackup 'standing_policy_verified.json') -Encoding UTF8
    $brokerReport | ConvertTo-Json -Depth 20 |
        Set-Content -LiteralPath (Join-Path $gateBackup 'broker_report_verified.json') -Encoding UTF8
    $approvalTransactionStarted = $true

    Assert-BeforeDeadline $session
    $approvalStarted = Get-Date
    $priorRuntimeRoot = $env:EQIDV2_RUNTIME_ROOT
    try {
        $env:EQIDV2_RUNTIME_ROOT = $RuntimeRoot
        $approvalOutput = @(
            & $Python $Operator approve-standing-session `
                --session-date $session.ToString('yyyy-MM-dd') `
                --standing-policy-confirmed
        )
        if ($LASTEXITCODE -ne 0) {
            throw 'Standing-policy operator approval returned a failure.'
        }
    } finally {
        if ($null -eq $priorRuntimeRoot) {
            Remove-Item Env:\EQIDV2_RUNTIME_ROOT -ErrorAction SilentlyContinue
        } else {
            $env:EQIDV2_RUNTIME_ROOT = $priorRuntimeRoot
        }
    }
    $approvalOutput | ForEach-Object { Write-G3StandingActivationLog ([string]$_) }

    $verifiedAt = Get-Date
    Assert-BeforeDeadline $session
    Assert-TargetState $false $session
    Assert-NoG3LiveWorkers
    Assert-AllKillSwitchesClear
    Assert-AuthenticationReady $verifiedAt
    Assert-PreopenHealth $verifiedAt
    $brokerReport = Assert-FreshBrokerFlatReadReconciliation $verifiedAt
    Assert-ApprovedStandingState $verifiedAt $approvalStarted $policy $brokerReport
    Assert-HttpsClockSkew
    Assert-BeforeDeadline $session

    # This is the only enabling operation and the final commit. The task is
    # never started by the gate; its existing weekly trigger fires at 09:15.
    Enable-ScheduledTask -TaskName $TargetTask -ErrorAction Stop | Out-Null
    Assert-TargetState $true $session
    Write-G3StandingActivationLog (
        "SUCCESS: standing policy approved $($session.ToString('yyyy-MM-dd')); " +
        'the exact 09:15 target was enabled but not started.'
    )
} catch {
    $originalFailure = $_.Exception.Message
    $cleanupFailures = New-Object System.Collections.Generic.List[string]
    try {
        Disable-G3TargetFailClosed
        Assert-TargetState $false (Get-Date).Date
    } catch {
        $cleanupFailures.Add($_.Exception.Message)
    }
    if ($approvalTransactionStarted) {
        try {
            # First make any newly written arm inert. Restoration then returns
            # both artifacts byte-for-byte to their pre-call state.
            if (Test-Path -LiteralPath $ArmPath -PathType Leaf) {
                Disable-ArmFileFailClosed
            }
            $savedProfile = Join-Path $gateBackup 'active_profile.json'
            Copy-Item -LiteralPath $savedProfile -Destination $ProfilePath -Force
            Assert-SameFileHash $savedProfile $ProfilePath 'pre-gate profile'
            $savedArm = Join-Path $gateBackup 'live_arm.json'
            if ($hadPriorArm) {
                Copy-Item -LiteralPath $savedArm -Destination $ArmPath -Force
                Assert-SameFileHash $savedArm $ArmPath 'pre-gate arm'
            } elseif (Test-Path -LiteralPath $ArmPath) {
                Remove-Item -LiteralPath $ArmPath -Force
                if (Test-Path -LiteralPath $ArmPath) {
                    throw 'Failed to remove the newly created standing arm.'
                }
            }
        } catch {
            $cleanupFailures.Add($_.Exception.Message)
            try {
                if (Test-Path -LiteralPath $ArmPath -PathType Leaf) {
                    Disable-ArmFileFailClosed
                }
            } catch {
                $cleanupFailures.Add($_.Exception.Message)
            }
        }
    }
    $message = "BLOCKED: $originalFailure"
    if ($cleanupFailures.Count) {
        $message += ' FAIL-CLOSED CLEANUP ERROR: ' + ($cleanupFailures -join ' | ')
    }
    Write-G3StandingActivationLog $message
    throw $message
} finally {
    $activationMutex.ReleaseMutex()
    $activationMutex.Dispose()
}
