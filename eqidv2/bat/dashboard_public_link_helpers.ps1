# Side-effect-free on load. Probe/send callbacks allow offline validation.
function Get-QuickTunnelPublicUrl {
    param([string]$Line)
    if ($Line -match '(?i)\b(ERR|error|failed|failure|exception)\b') { return "" }
    # Only the URL row in cloudflared's successful quick-tunnel announcement.
    if ($Line -notmatch '\|\s*(https://[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.trycloudflare\.com)/?\s*\|') { return "" }
    $candidate = $Matches[1]
    if (([uri]$candidate).Host -eq 'api.trycloudflare.com') { return "" }
    return $candidate
}

function Get-DashboardPublicEmailBody {
    param([string]$Url)
    $address = [uri]$Url
    if (-not $address.IsAbsoluteUri -or $address.Scheme -ne 'https' -or
        $address.Host -notmatch '^[a-z0-9-]+\.trycloudflare\.com$' -or
        $address.Host -eq 'api.trycloudflare.com' -or $address.UserInfo -or
        $address.Query -or $address.Fragment -or $address.AbsolutePath -ne '/') {
        throw 'Expected a bare HTTPS dashboard tunnel URL.'
    }
    $origin = $address.GetLeftPart([System.UriPartial]::Authority)
    return @"
EQIDV2 dashboards are ready.

Main dashboard:
$origin/

Dashboard Flow:
$origin/dashboard-flow

Sign in using your existing dashboard username and password when prompted.
Keep the trading PC, dashboard server, and tunnel running while using these links.
These temporary links change when a new tunnel is created.

Generated at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')
"@
}

function Test-DashboardPublicProbeResult {
    param([int]$StatusCode, [string]$AuthenticateHeader)
    # An anonymous challenge from this exact realm proves the protected dashboard
    # answered, without sending its login or access token through a readiness probe.
    return ($StatusCode -eq 401 -and
        $AuthenticateHeader -match '(?i)\bBasic\s+realm="EQIDV2 Logs"')
}

function Wait-DashboardPublicReady {
    param(
        [string]$Url,
        # New quick-tunnel hostnames can outlast Windows' negative DNS cache.
        [int]$Attempts = 60,
        [int]$DelaySeconds = 5,
        [scriptblock]$Probe = {
            param($Address)
            try {
                $response = Invoke-WebRequest -Uri $Address -UseBasicParsing -Method Get -TimeoutSec 5 -MaximumRedirection 0 -ErrorAction Stop
                return @{ status = [int]$response.StatusCode; authenticate = [string]$response.Headers['WWW-Authenticate'] }
            } catch {
                $response = $_.Exception.Response
                if ($null -eq $response) { return @{ status = 0; authenticate = '' } }
                return @{ status = [int]$response.StatusCode; authenticate = [string]$response.Headers['WWW-Authenticate'] }
            }
        },
        [scriptblock]$Pause = { param($Seconds) Start-Sleep -Seconds $Seconds }
    )
    # Validate the origin using the same strict rules as the email builder.
    $null = Get-DashboardPublicEmailBody -Url $Url
    $origin = $Url.TrimEnd('/')
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $ready = $true
        foreach ($path in @('/', '/dashboard-flow')) {
            try {
                $result = & $Probe ($origin + $path)
                if (-not (Test-DashboardPublicProbeResult -StatusCode $result.status -AuthenticateHeader $result.authenticate)) { $ready = $false }
            } catch { $ready = $false }
        }
        if ($ready) { return $true }
        if ($attempt -lt $Attempts) { & $Pause $DelaySeconds }
    }
    return $false
}

function Invoke-DashboardEmailDelivery {
    param(
        [string]$Mode,
        [scriptblock]$GmailSend,
        [scriptblock]$SmtpSend,
        [int]$Attempts = 2,
        [scriptblock]$Pause = { param($Seconds) Start-Sleep -Seconds $Seconds }
    )
    if ($Mode -eq 'link_only') { return @{ ok = $true; skipped = $true; reason = 'Email disabled by link_only mode.' } }
    if ($Mode -eq 'smtp_only') { return (& $SmtpSend) }
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $result = & $GmailSend
        if ($result.ok) { return $result }
        # A send error can occur after the provider accepted the message. Never
        # retry or fall back unless the sender proves delivery was not attempted.
        if ($result.delivery_unknown -or -not $result.retryable) { break }
        if ($attempt -lt $Attempts) { & $Pause 3 }
    }
    if ($Mode -eq 'gmail_api_then_smtp' -and $result.delivery_unknown -eq $false) {
        return (& $SmtpSend)
    }
    return $result
}
