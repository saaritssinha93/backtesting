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
    [string]$EmailSubjectPrefix = "EQIDV2 Dashboard URL"
)

$ErrorActionPreference = "Continue"

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
        return @{ ok = $false; reason = "Missing LOG_DASH_EMAIL_TO." }
    }
    if (-not (Test-Path -LiteralPath $ScriptPath)) {
        return @{ ok = $false; reason = "Gmail API script not found: $ScriptPath" }
    }
    if (-not (Test-Path -LiteralPath $CredentialsPath)) {
        return @{ ok = $false; reason = "Gmail OAuth client file not found: $CredentialsPath" }
    }

    $subjectBase = ("" + $SubjectPrefix).Trim()
    if ([string]::IsNullOrWhiteSpace($subjectBase)) { $subjectBase = "EQIDV2 Dashboard URL" }
    $subject = "$subjectBase - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')"
    $body = @"
EQIDV2 dashboard URL generated.

URL:
$Url

Generated at:
$(Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz")
"@

    $args = @(
        $ScriptPath,
        "--credentials", $CredentialsPath,
        "--token", $TokenPath,
        "--to", $toAddr,
        "--subject", $subject,
        "--body", $body
    )

    $fromAddr = ("" + $From).Trim()
    if (-not [string]::IsNullOrWhiteSpace($fromAddr)) {
        $args += @("--from", $fromAddr)
    }
    if ($AllowInteractiveAuth) {
        $args += "--allow-interactive-auth"
    }

    $output = & $PythonPath @args 2>&1
    $exitCode = $LASTEXITCODE
    $outText = ($output | ForEach-Object { $_.ToString() }) -join "`n"

    if ($exitCode -eq 0) {
        return @{ ok = $true; reason = $outText }
    }
    return @{ ok = $false; reason = "exit=$exitCode output=$outText" }
}

try {
    $logDir = Split-Path -Parent $LogFile
    if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
        New-Item -Path $logDir -ItemType Directory -Force | Out-Null
    }

    if ($EmailTo -eq "__EMPTY__") { $EmailTo = "" }
    if ($EmailFrom -eq "__EMPTY__") { $EmailFrom = "" }

    $sendMode = Normalize-EmailSendMode $EmailSendMode
    $allowInteractive = Parse-BoolLike $GmailInteractiveAuth

    $emailToResolved = ("" + $EmailTo).Trim()
    if ([string]::IsNullOrWhiteSpace($emailToResolved)) {
        Write-LogLine "[WARN] Email recipient is not set; URL email will not be sent." $LogFile
    } else {
        Write-LogLine "[INFO] Email send mode active: $sendMode | recipient=$emailToResolved" $LogFile
    }

    $announced = $false
    & $CloudflaredPath tunnel --url $DashUrl --protocol http2 --edge-ip-version 4 2>&1 | ForEach-Object {
        $line = $_.ToString()
        Write-LogLine $line $LogFile

        if (-not $announced -and $line -match "https://[a-z0-9-]+\.trycloudflare\.com") {
            $url = $Matches[0]
            Set-Content -Path $LatestUrlFile -Value $url -Encoding utf8
            Write-LogLine "[INFO] Public URL saved: $url" $LogFile

            if ($sendMode -eq "gmail_api_only") {
                $send = Send-UrlByGmailApi -Url $url -To $emailToResolved -From $EmailFrom -SubjectPrefix $EmailSubjectPrefix -PythonPath $PythonExe -ScriptPath $GmailApiScript -CredentialsPath $GmailCredentialsFile -TokenPath $GmailTokenFile -AllowInteractiveAuth $allowInteractive
                if ($send.ok) {
                    if ([string]::IsNullOrWhiteSpace($send.reason)) {
                        Write-LogLine "[INFO] Gmail API email sent successfully to $emailToResolved." $LogFile
                    } else {
                        Write-LogLine "[INFO] Gmail API email sent successfully to $emailToResolved. $($send.reason)" $LogFile
                    }
                } else {
                    Write-LogLine "[ERROR] Gmail API email send failed: $($send.reason)" $LogFile
                }
            } else {
                Write-LogLine "[INFO] Email send mode link_only: URL was generated but email was skipped." $LogFile
            }

            try { Set-Clipboard -Value $url } catch {}
            $announced = $true
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
