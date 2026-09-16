$ErrorActionPreference = "Stop"
# Compatibility installer: only the canonical G task is installed.
& (Join-Path $PSScriptRoot "schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1") @args
if (-not $?) { exit 1 }
exit 0
