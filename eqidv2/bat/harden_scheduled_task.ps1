param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName
)

$ErrorActionPreference = "Stop"

function Resolve-TaskPathParts {
    param([string]$Name)

    $normalized = ("" + $Name).Trim()
    if ([string]::IsNullOrWhiteSpace($normalized)) {
        throw "TaskName cannot be empty."
    }
    if (-not $normalized.StartsWith("\")) {
        $normalized = "\" + $normalized
    }

    $lastSlash = $normalized.LastIndexOf("\")
    if ($lastSlash -lt 0) {
        return @{ Folder = "\"; Name = $normalized }
    }

    $folder = $normalized.Substring(0, $lastSlash)
    if ([string]::IsNullOrWhiteSpace($folder)) {
        $folder = "\"
    }
    $leaf = $normalized.Substring($lastSlash + 1)
    if ([string]::IsNullOrWhiteSpace($leaf)) {
        throw "TaskName must include a task leaf name."
    }

    return @{ Folder = $folder; Name = $leaf }
}

try {
    $parts = Resolve-TaskPathParts -Name $TaskName

    $service = New-Object -ComObject "Schedule.Service"
    $service.Connect()

    $folder = $service.GetFolder($parts.Folder)
    $task = $folder.GetTask($parts.Name)
    $definition = $task.Definition

    $definition.Settings.DisallowStartIfOnBatteries = $false
    $definition.Settings.StopIfGoingOnBatteries = $false
    $definition.Settings.ExecutionTimeLimit = "PT0S"
    $definition.Settings.StartWhenAvailable = $true
    $definition.Settings.RunOnlyIfNetworkAvailable = $false
    $definition.Settings.WakeToRun = $false
    $definition.Settings.Enabled = $true
    $definition.Settings.Hidden = $false
    $definition.Settings.Priority = 7
    $definition.Settings.MultipleInstances = 2
    $definition.Settings.IdleSettings.StopOnIdleEnd = $false
    $definition.Settings.IdleSettings.RestartOnIdle = $false

    $userId = $definition.Principal.UserId
    $logonType = [int]$definition.Principal.LogonType
    $null = $folder.RegisterTaskDefinition($parts.Name, $definition, 6, $userId, $null, $logonType, $null)

    $updated = $folder.GetTask($parts.Name).Definition
    $summary = @(
        "task=$TaskName"
        "logon_type=$([int]$updated.Principal.LogonType)"
        "disallow_start_on_batteries=$($updated.Settings.DisallowStartIfOnBatteries)"
        "stop_on_batteries=$($updated.Settings.StopIfGoingOnBatteries)"
        "execution_time_limit=$($updated.Settings.ExecutionTimeLimit)"
        "start_when_available=$($updated.Settings.StartWhenAvailable)"
        "multiple_instances=$($updated.Settings.MultipleInstances)"
    ) -join " | "

    Write-Output "[INFO] Hardened scheduled task: $summary"

    if ([int]$updated.Principal.LogonType -eq 3) {
        Write-Output "[WARN] $TaskName still uses InteractiveToken. Battery and runtime safeguards were hardened, but changing the logon type requires Windows credentials/elevation."
    }
}
catch {
    Write-Error "Failed to harden scheduled task '$TaskName': $($_.Exception.Message)"
    exit 1
}
