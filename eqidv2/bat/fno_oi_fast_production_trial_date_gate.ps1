param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("Legacy", "Trial")]
    [string]$Role,

    [string]$TrialDate = "2026-09-02",

    # Test-only clock injection. Scheduled runners deliberately omit it.
    [string]$ObservedDate = ""
)

$ErrorActionPreference = "Stop"
$skipExitCode = 42

try {
    $target = [datetime]::ParseExact(
        $TrialDate,
        "yyyy-MM-dd",
        [Globalization.CultureInfo]::InvariantCulture
    ).Date

    if ([string]::IsNullOrWhiteSpace($ObservedDate)) {
        $indiaZone = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
        $observed = [TimeZoneInfo]::ConvertTimeFromUtc(
            [datetime]::UtcNow,
            $indiaZone
        ).Date
    }
    else {
        $observed = [datetime]::ParseExact(
            $ObservedDate,
            "yyyy-MM-dd",
            [Globalization.CultureInfo]::InvariantCulture
        ).Date
    }

    $allowed = if ($Role -eq "Trial") {
        $observed -eq $target
    }
    else {
        $observed -ne $target
    }

    $state = if ($allowed) { "ALLOW" } else { "SKIP" }
    $message = "[{0}] role={1} observed_date={2} trial_date={3}" -f (
        $state,
        $Role,
        $observed.ToString("yyyy-MM-dd"),
        $target.ToString("yyyy-MM-dd")
    )
    Write-Output $message
    if ($allowed) {
        exit 0
    }
    exit $skipExitCode
}
catch {
    Write-Error "FnO fast-production trial date gate failed closed: $($_.Exception.Message)"
    exit 1
}
