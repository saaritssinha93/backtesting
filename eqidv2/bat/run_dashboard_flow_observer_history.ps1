[CmdletBinding()]
param(
    [string]$DataRoot = $(if ($env:EQIDV2_RUNTIME_ROOT) { $env:EQIDV2_RUNTIME_ROOT } else { 'C:\TradingData\eqidv2' }),
    [string]$Python = 'python.exe',
    [ValidatePattern('^app[1-8]$')][string]$App = 'app1',
    [switch]$Scheduled,
    [switch]$Fetch
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$historyScript = Join-Path $projectRoot 'dashboard_flow_observer_history.py'
$resolvedRoot = [IO.Path]::GetFullPath($DataRoot)
$observerRoot = Join-Path $resolvedRoot 'dashboard_flow_observer'
foreach ($argument in @($historyScript, $resolvedRoot, $Python)) {
    if ($argument -match '["\r\n]') { throw 'Paths must not contain quotes or newlines.' }
}
$pythonCommand = (Get-Command $Python -ErrorAction Stop).Source
if (-not (Test-Path -LiteralPath $historyScript -PathType Leaf)) { throw 'Opening-history module is missing.' }
$pythonArguments = @('"' + $historyScript + '"', '--data-root', '"' + $resolvedRoot + '"', '--app', $App, '--latest-completed')
if (-not ($Fetch -or $Scheduled)) {
    & $pythonCommand $historyScript --data-root $resolvedRoot --app $App --latest-completed
    exit $LASTEXITCODE
}
$pythonArguments += '--fetch'
if ($Scheduled) { $pythonArguments += '--scheduled' }
$mutexHash = [BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($resolvedRoot.ToLowerInvariant()))).Replace('-', '').Substring(0, 16)
$mutex = New-Object Threading.Mutex($false, ('Local\EQIDV2_dashboard_flow_observer_history_' + $mutexHash))
$acquired = $false
try {
    try { $acquired = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $acquired = $true }
    if (-not $acquired) { Write-Output 'Opening-history recovery already has a runner; duplicate skipped.'; exit 0 }
    $logDirectory = Join-Path $observerRoot 'logs'
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
    $stamp = Get-Date -Format 'yyyyMMdd_HHmmss_fff'
    $worker = Start-Process -FilePath $pythonCommand -ArgumentList $pythonArguments -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -Wait -RedirectStandardOutput (Join-Path $logDirectory "history_$stamp.stdout.log") -RedirectStandardError (Join-Path $logDirectory "history_$stamp.stderr.log")
    exit $worker.ExitCode
}
finally {
    if ($acquired) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
