[CmdletBinding()]
param([string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$Workspace = Split-Path $PSScriptRoot -Parent
# This calls only the local preflight module. It never invokes any trading BAT.
& $Python (Join-Path $Workspace 'fno_v13_v10_g_3_preflight.py')
exit $LASTEXITCODE
