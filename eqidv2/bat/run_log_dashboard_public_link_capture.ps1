param(
    [Parameter(Mandatory = $true)]
    [string]$CloudflaredPath,

    [Parameter(Mandatory = $true)]
    [string]$DashUrl,

    [Parameter(Mandatory = $true)]
    [string]$LogFile,

    [Parameter(Mandatory = $true)]
    [string]$LatestUrlFile,

    [Parameter(Mandatory = $true)]
    [string]$PythonExe,

    [Parameter(Mandatory = $true)]
    [string]$GmailApiScript,

    [Parameter(Mandatory = $true)]
    [string]$GmailCredentialsFile,

    [Parameter(Mandatory = $true)]
    [string]$GmailTokenFile,

    [string]$GmailInteractiveAuth = "0",
    [string]$EmailTo = "",
    [string]$EmailFrom = "",
    [string]$EmailSendMode = "gmail_api_only",
    [string]$EmailSubjectPrefix = "EQIDV2 Dashboard URL",
    [string]$SmtpUser = "",
    [string]$SmtpAppPassword = "",
    [string]$SmtpServer = "smtp.gmail.com",
    [int]$SmtpPort = 587
)

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot 'dashboard_public_link_helpers.ps1')

function Write-LogLine {
    param(
        [string]$Message,
        [string]$Path
    )
    Write-Output $Message
    try { Add-Content -Path $Path -Value $Message -Encoding utf8 } catch {}
}

function Normalize-EmailSendMode {
    param([string]$Mode)
    $m = ("" + $Mode).Trim().ToLowerInvariant()
    switch ($m) {
        "gmail_api_only" { return "gmail_api_only" }
        "gmail_api_then_smtp" { return "gmail_api_then_smtp" }
        "smtp_only" { return "smtp_only" }
        "link_only" { return "link_only" }
        default { return "gmail_api_only" }
    }
}

function Parse-BoolLike {
    param([string]$Value)
    $v = ("" + $Value).Trim().ToLowerInvariant()
    return ($v -eq "1" -or $v -eq "true" -or $v -eq "yes" -or $v -eq "y")
}

function Send-UrlByGmailApi {
    param(
        [string]$Url,
        [string]$To,
        [string]$From,
        [string]$SubjectPrefix,
        [string]$PythonPath,
        [string]$ScriptPath,
        [string]$CredentialsPath,
        [string]$TokenPath,
        [bool]$AllowInteractiveAuth
    )

    $toAddr = ("" + $To).Trim()
    if ([string]::IsNullOrWhiteSpace($toAddr)) {
        return @{ ok = $false; retryable = $false; delivery_unknown = $false; reason = "Missing LOG_DASH_EMAIL_TO." }
    }
    if (-not (Test-Path -LiteralPath $ScriptPath)) {
        return @{ ok = $false; retryable = $false; delivery_unknown = $false; reason = "Gmail API script not found: $ScriptPath" }
    }
    if (-not (Test-Path -LiteralPath $CredentialsPath)) {
        return @{ ok = $false; retryable = $false; delivery_unknown = $false; reason = "Gmail OAuth client file not found: $CredentialsPath" }
    }

    $subjectBase = ("" + $SubjectPrefix).Trim()
    if ([string]::IsNullOrWhiteSpace($subjectBase)) { $subjectBase = "EQIDV2 Dashboard URL" }
    $subject = "$subjectBase - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')"
    $body = Get-DashboardPublicEmailBody -Url $Url

    $senderArgs = @(
        $ScriptPath,
        "--credentials", $CredentialsPath,
        "--token", $TokenPath,
        "--to", $toAddr,
        "--subject", $subject
    )

    $fromAddr = ("" + $From).Trim()
    if (-not [string]::IsNullOrWhiteSpace($fromAddr)) {
        $senderArgs += @("--from", $fromAddr)
    }
    if ($AllowInteractiveAuth) {
        $senderArgs += "--allow-interactive-auth"
    }

    $bodyFile = $null
    $commandInvoked = $false
    try {
        $bodyFile = [System.IO.Path]::GetTempFileName()
        [System.IO.File]::WriteAllText($bodyFile, $body, (New-Object System.Text.UTF8Encoding($false)))
        $senderArgs += @("--body-file", $bodyFile)
        $commandInvoked = $true
        $output = & $PythonPath @senderArgs 2>&1
        $exitCode = $LASTEXITCODE
    } catch {
        return @{ ok = $false; retryable = $false; delivery_unknown = $commandInvoked; reason = "Gmail command failed: $($_.Exception.Message)" }
    } finally {
        if ($bodyFile -and (Test-Path -LiteralPath $bodyFile)) {
            Remove-Item -LiteralPath $bodyFile -Force -ErrorAction SilentlyContinue
        }
    }
    $outText = ($output | ForEach-Object { $_.ToString() }) -join "`n"

    if ($exitCode -eq 0) {
        return @{ ok = $true; reason = $outText }
    }
    # Exit 3 is authentication failure before sending; exit 4 is input failure.
    # Exit 5 and unexpected failures cannot establish whether Gmail accepted it.
    return @{ ok = $false; retryable = ($exitCode -eq 3); delivery_unknown = ($exitCode -notin @(3, 4)); reason = "exit=$exitCode output=$outText" }
}

function Send-UrlBySmtp {
    param(
        [string]$Url,
        [string]$To,
        [string]$From,
        [string]$User,
        [string]$AppPassword,
        [string]$Server,
        [int]$Port,
        [string]$SubjectPrefix
    )

    $toAddr = ("" + $To).Trim()
    if ([string]::IsNullOrWhiteSpace($toAddr)) {
        return @{ ok = $false; reason = "Missing LOG_DASH_EMAIL_TO." }
    }

    $fromAddr = ("" + $From).Trim()
    if ([string]::IsNullOrWhiteSpace($fromAddr)) {
        $fromAddr = ("" + $User).Trim()
    }
    if ([string]::IsNullOrWhiteSpace($fromAddr)) {
        return @{ ok = $false; reason = "Missing LOG_DASH_EMAIL_FROM (or LOG_DASH_SMTP_USER as sender)." }
    }

    if ([string]::IsNullOrWhiteSpace($User) -or [string]::IsNullOrWhiteSpace($AppPassword)) {
        return @{ ok = $false; reason = "Missing LOG_DASH_SMTP_USER or LOG_DASH_SMTP_APP_PASSWORD." }
    }

    $subjectBase = ("" + $SubjectPrefix).Trim()
    if ([string]::IsNullOrWhiteSpace($subjectBase)) { $subjectBase = "EQIDV2 Dashboard URL" }
    $subject = "$subjectBase - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')"
    $body = Get-DashboardPublicEmailBody -Url $Url

    try {
        $secure = ConvertTo-SecureString $AppPassword -AsPlainText -Force
        $cred = New-Object System.Management.Automation.PSCredential($User, $secure)
        $recipients = $toAddr -split '\s*[,;]\s*' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        Send-MailMessage -SmtpServer $Server -Port $Port -UseSsl -Credential $cred -From $fromAddr -To $recipients -Subject $subject -Body $body -BodyAsHtml:$false -ErrorAction Stop
        return @{ ok = $true; reason = "" }
    }
    catch {
        $reason = $_.Exception.Message
        try {
            if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
                $reason = $_.ErrorDetails.Message
            }
        }
        catch {}
        return @{ ok = $false; retryable = $false; delivery_unknown = $true; reason = $reason }
    }
}

try {
    $logDir = Split-Path -Parent $LogFile
    if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
        New-Item -Path $logDir -ItemType Directory -Force | Out-Null
    }

    if ($EmailTo -eq "__EMPTY__") { $EmailTo = "" }
    if ($EmailFrom -eq "__EMPTY__") { $EmailFrom = "" }
    if ($SmtpUser -eq "__EMPTY__") { $SmtpUser = "" }
    if ($SmtpAppPassword -eq "__EMPTY__") { $SmtpAppPassword = "" }

    $sendMode = Normalize-EmailSendMode $EmailSendMode
    $allowInteractive = Parse-BoolLike $GmailInteractiveAuth

    $emailToResolved = ("" + $EmailTo).Trim()
    if ([string]::IsNullOrWhiteSpace($emailToResolved)) {
        Write-LogLine "[WARN] Email recipient is not set; URL email will not be sent." $LogFile
    } else {
        Write-LogLine "[INFO] Email send mode active: $sendMode | recipient=$emailToResolved" $LogFile
    }

    $announced = $false
    $publicationAttempted = $false
    $candidateUrl = ""
    $registered = $false
    & $CloudflaredPath tunnel --url $DashUrl --protocol http2 --edge-ip-version 4 2>&1 | ForEach-Object {
        $line = $_.ToString()
        Write-LogLine $line $LogFile

        $foundUrl = Get-QuickTunnelPublicUrl -Line $line
        if ($foundUrl) { $candidateUrl = $foundUrl }
        if ($line -match '\bRegistered tunnel connection\b' -and $line -notmatch '(?i)\b(ERR|error|failed|failure|exception)\b') { $registered = $true }

        if (-not $announced -and -not $publicationAttempted -and $candidateUrl -and $registered) {
            $publicationAttempted = $true
            $url = $candidateUrl
            if (-not (Wait-DashboardPublicReady -Url $url)) {
                Write-LogLine "[ERROR] Public dashboard readiness checks exhausted; URL was not saved or emailed." $LogFile
            } else {
                Set-Content -LiteralPath $LatestUrlFile -Value $url -Encoding utf8 -ErrorAction Stop
                Write-LogLine "[INFO] Public Main and Flow routes verified; URL saved: $url" $LogFile
                $send = Invoke-DashboardEmailDelivery -Mode $sendMode -GmailSend {
                    Send-UrlByGmailApi -Url $url -To $emailToResolved -From $EmailFrom -SubjectPrefix $EmailSubjectPrefix -PythonPath $PythonExe -ScriptPath $GmailApiScript -CredentialsPath $GmailCredentialsFile -TokenPath $GmailTokenFile -AllowInteractiveAuth $allowInteractive
                } -SmtpSend {
                    Send-UrlBySmtp -Url $url -To $emailToResolved -From $EmailFrom -User $SmtpUser -AppPassword $SmtpAppPassword -Server $SmtpServer -Port $SmtpPort -SubjectPrefix $EmailSubjectPrefix
                }
                if ($send.ok) {
                    if ($send.skipped) {
                        Write-LogLine "[INFO] Email send mode link_only: verified URLs were saved; email skipped." $LogFile
                    } else {
                        Write-LogLine "[INFO] Dashboard Main and Flow email sent successfully to $emailToResolved. $($send.reason)" $LogFile
                    }
                    $announced = $true
                } else {
                    Write-LogLine "[ERROR] Dashboard email failed; not marked sent. $($send.reason)" $LogFile
                    if ($send.delivery_unknown) { Write-LogLine "[WARN] Delivery outcome is unknown; automatic retry/fallback suppressed to avoid duplicates." $LogFile }
                }
                try { Set-Clipboard -Value $url } catch {}
            }
        }
    }

    exit $LASTEXITCODE
}
catch {
    $err = "[ERROR] run_log_dashboard_public_link_capture.ps1 failed: $($_.Exception.Message)"
    Write-Output $err
    try { Add-Content -Path $LogFile -Value $err -Encoding utf8 } catch {}
    exit 1
}
