$ErrorActionPreference = "Stop"

$blockedEntrypoints = @(
    "fno_oi_fetch_5min.py",
    "fno_oi_fetch_5min_fast_shadow.py",
    "fno_oi_fetch_5min_fast_production.py"
)

try {
    $conflicts = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $commandLine = [string]$_.CommandLine
                if ([string]::IsNullOrWhiteSpace($commandLine)) {
                    return $false
                }
                foreach ($entrypoint in $blockedEntrypoints) {
                    if ($commandLine.IndexOf(
                        $entrypoint,
                        [StringComparison]::OrdinalIgnoreCase
                    ) -ge 0) {
                        return $true
                    }
                }
                return $false
            } |
            Select-Object ProcessId, Name, CommandLine
    )
    if ($conflicts.Count -gt 0) {
        $details = $conflicts | ForEach-Object {
            "pid=$($_.ProcessId) name=$($_.Name) command=$($_.CommandLine)"
        }
        throw "Conflicting FnO fetch process already exists: $($details -join ' | ')"
    }
    Write-Output "[PASS] No legacy, shadow, or duplicate fast-production process is running."
    exit 0
}
catch {
    Write-Error "FnO fast-production exclusivity preflight failed closed: $($_.Exception.Message)"
    exit 1
}
