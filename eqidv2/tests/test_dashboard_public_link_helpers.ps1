$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$helper = Join-Path $repo 'bat\dashboard_public_link_helpers.ps1'
$capture = Join-Path $repo 'bat\run_log_dashboard_public_link_capture.ps1'
$assertions = 0
function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
    $script:assertions++
}
foreach ($path in @($helper, $capture)) {
    $tokens = $null
    $errors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
    Assert-True ($errors.Count -eq 0) "PowerShell syntax errors in $path"
}
. $helper

$origin = 'https://synthetic-dashboard-check.trycloudflare.com'
Assert-True ((Get-QuickTunnelPublicUrl "2026-10-10T00:00:00Z INF | $origin |") -eq $origin) 'Valid announcement was not captured.'
foreach ($line in @(
    'ERR failed to request quick Tunnel: Post "https://api.trycloudflare.com/tunnel": timeout',
    'INF | https://api.trycloudflare.com |',
    "ERR | $origin |",
    "INF retry request $origin",
    "INF | $origin/path |",
    "INF | $origin`?token=synthetic |",
    'INF | http://synthetic.trycloudflare.com |',
    'INF | https://synthetic.trycloudflare.com.evil.example |'
)) {
    Assert-True ([string]::IsNullOrEmpty((Get-QuickTunnelPublicUrl $line))) 'Rejected announcement was accepted.'
}
$body = Get-DashboardPublicEmailBody $origin
Assert-True ($body.Contains("$origin/")) 'Main link missing.'
Assert-True ($body.Contains("$origin/dashboard-flow")) 'Flow link missing.'
Assert-True ($body.Contains('existing dashboard username and password')) 'Login note missing.'
Assert-True ($body.Contains('PC, dashboard server, and tunnel running')) 'Running-service note missing.'
foreach ($badUrl in @("$origin`?token=synthetic", "$origin/dashboard-flow", 'https://user:synthetic@synthetic.trycloudflare.com')) {
    $threw = $false
    try { $null = Get-DashboardPublicEmailBody $badUrl } catch { $threw = $true }
    Assert-True $threw 'Email accepted a non-bare URL.'
}
Assert-True (Test-DashboardPublicProbeResult 401 'Basic realm="EQIDV2 Logs"') 'Expected dashboard authentication challenge rejected.'
Assert-True (-not (Test-DashboardPublicProbeResult 401 'Basic realm="Other service"')) 'Wrong service challenge accepted.'
Assert-True (-not (Test-DashboardPublicProbeResult 502 'Basic realm="EQIDV2 Logs"')) 'Unavailable origin accepted.'

$script:probes = [System.Collections.Generic.List[string]]::new()
$script:pauses = 0
$ready = Wait-DashboardPublicReady -Url $origin -Attempts 2 -Probe {
    param($Address)
    $script:probes.Add($Address)
    # Flow is initially unavailable, then both protected routes answer.
    if ($script:probes.Count -eq 2) { return @{ status = 502; authenticate = '' } }
    return @{ status = 401; authenticate = 'Basic realm="EQIDV2 Logs"' }
} -Pause { param($Seconds) $script:pauses++ }
Assert-True $ready 'Transient readiness failure did not recover.'
Assert-True ($probes.Count -eq 4 -and $pauses -eq 1) 'Readiness retry count differs.'
Assert-True ($probes[0] -eq "$origin/" -and $probes[1] -eq "$origin/dashboard-flow") 'Both routes were not checked.'
$script:probes.Clear()
$script:pauses = 0
$ready = Wait-DashboardPublicReady -Url $origin -Attempts 2 -Probe {
    param($Address)
    $script:probes.Add($Address)
    return @{ status = 401; authenticate = 'Basic realm="Unrelated"' }
} -Pause { param($Seconds) $script:pauses++ }
Assert-True (-not $ready) 'Wrong service passed readiness.'
Assert-True ($probes.Count -eq 4 -and $pauses -eq 1) 'Readiness retry bound was not respected.'
$script:defaultProbeCalls = 0
$script:defaultPauseSeconds = [System.Collections.Generic.List[int]]::new()
$ready = Wait-DashboardPublicReady -Url $origin -Probe {
    param($Address)
    $script:defaultProbeCalls++
    return @{ status = 0; authenticate = '' }
} -Pause { param($Seconds) $script:defaultPauseSeconds.Add($Seconds) }
Assert-True (-not $ready -and $defaultProbeCalls -eq 120) 'Default readiness window must check both routes for 60 attempts.'
Assert-True ($defaultPauseSeconds.Count -eq 59 -and ($defaultPauseSeconds | Measure-Object -Sum).Sum -eq 295) 'Default DNS propagation wait must span approximately five minutes.'

$script:gmailCalls = 0
$script:smtpCalls = 0
$result = Invoke-DashboardEmailDelivery -Mode gmail_api_then_smtp -GmailSend {
    $script:gmailCalls++
    return @{ ok = $false; retryable = $true; delivery_unknown = $true }
} -SmtpSend { $script:smtpCalls++; return @{ ok = $true } } -Pause { param($Seconds) }
Assert-True (-not $result.ok -and $gmailCalls -eq 1 -and $smtpCalls -eq 0) 'Ambiguous delivery retried or fell back.'
$script:gmailCalls = 0
$result = Invoke-DashboardEmailDelivery -Mode gmail_api_only -GmailSend {
    $script:gmailCalls++
    if ($script:gmailCalls -eq 1) { return @{ ok = $false; retryable = $true; delivery_unknown = $false } }
    return @{ ok = $true }
} -SmtpSend { throw 'Unexpected SMTP' } -Pause { param($Seconds) }
Assert-True ($result.ok -and $gmailCalls -eq 2) 'Safe authentication retry did not recover.'
$script:gmailCalls = 0
$script:smtpCalls = 0
$result = Invoke-DashboardEmailDelivery -Mode gmail_api_then_smtp -GmailSend {
    $script:gmailCalls++
    return @{ ok = $false; retryable = $true; delivery_unknown = $false }
} -SmtpSend { $script:smtpCalls++; return @{ ok = $true } } -Pause { param($Seconds) }
Assert-True ($result.ok -and $gmailCalls -eq 2 -and $smtpCalls -eq 1) 'Definite pre-send failure did not use bounded retries then fallback.'
$result = Invoke-DashboardEmailDelivery -Mode link_only -GmailSend { throw 'Unexpected Gmail' } -SmtpSend { throw 'Unexpected SMTP' }
Assert-True ($result.ok -and $result.skipped) 'link_only sent email.'

# Load only function definitions, never the launcher's top-level tunnel pipeline.
$tokens = $null
$errors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile($capture, [ref]$tokens, [ref]$errors)
foreach ($name in @('Send-UrlByGmailApi', 'Send-UrlBySmtp')) {
    $definition = $tree.Find({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name }, $true)
    . ([scriptblock]::Create($definition.Extent.Text))
}
function Invoke-SyntheticPython {
    $bodyIndex = [array]::IndexOf($args, '--body-file')
    Assert-True ($bodyIndex -ge 0) 'Gmail did not use a body file.'
    $script:observedBodyFile = $args[$bodyIndex + 1]
    $content = [System.IO.File]::ReadAllText($script:observedBodyFile)
    Assert-True ($content.Contains('/dashboard-flow')) 'Gmail did not use the shared body.'
    $global:LASTEXITCODE = $script:fakeExitCode
    'Synthetic sender result'
}
foreach ($code in @(3, 4, 5, 99)) {
    $script:fakeExitCode = $code
    $result = Send-UrlByGmailApi -Url $origin -To 'synthetic@example.invalid' -PythonPath Invoke-SyntheticPython -ScriptPath $helper -CredentialsPath $helper -TokenPath 'unused'
    Assert-True (-not $result.ok) 'Failed Gmail command marked successful.'
    Assert-True ($result.delivery_unknown -eq ($code -notin @(3, 4))) 'Gmail delivery uncertainty classified incorrectly.'
    Assert-True ($result.retryable -eq ($code -eq 3)) 'Gmail retry safety classified incorrectly.'
    Assert-True (-not (Test-Path -LiteralPath $script:observedBodyFile)) 'Temporary email body remained after failure.'
}
$script:fakeExitCode = 0
$result = Send-UrlByGmailApi -Url $origin -To 'synthetic@example.invalid' -PythonPath Invoke-SyntheticPython -ScriptPath $helper -CredentialsPath $helper -TokenPath 'unused'
Assert-True $result.ok 'Successful Gmail command not recognized.'
Assert-True (-not (Test-Path -LiteralPath $script:observedBodyFile)) 'Temporary email body remained after success.'
function Send-MailMessage {
    [CmdletBinding()]
    param($SmtpServer, $Port, [switch]$UseSsl, $Credential, $From, $To, $Subject, $Body, [switch]$BodyAsHtml)
    Assert-True ($Body.Contains('/dashboard-flow')) 'SMTP did not use the shared body.'
    Write-Error 'Synthetic SMTP error'
}
# Exercise the launcher's Continue setting; explicit -ErrorAction Stop must catch it.
$ErrorActionPreference = 'Continue'
$result = Send-UrlBySmtp -Url $origin -To 'synthetic@example.invalid' -From 'synthetic@example.invalid' -User 'synthetic' -AppPassword 'synthetic' -Server 'synthetic.invalid' -Port 587
$ErrorActionPreference = 'Stop'
Assert-True (-not $result.ok -and $result.delivery_unknown) 'SMTP failure marked successful or safe to retry.'
Write-Output "PASS: $assertions assertions; no network, email, tunnel, or strategy executed."
