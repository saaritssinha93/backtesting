"""Execute only installer guard prefixes in disposable roots; no task commands."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INSTALLERS = (
    "schedule_fno_oi_weekday.ps1",
    "schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1",
    "schedule_fno_v13_v10_g_options_weekday.ps1",
    "schedule_backtesting_result_v13_v10_g_weekday.bat",
)


@pytest.mark.skipif(os.name != "nt" or not shutil.which("powershell"), reason="Windows installer guard")
@pytest.mark.parametrize("installer", INSTALLERS)
@pytest.mark.parametrize("persisted", ["V13_V10_G_3", "V13_V10_G", "MALFORMED_JSON"])
def test_legacy_installer_guard_uses_persisted_profile_before_any_task_operation(tmp_path, installer, persisted):
    source = (ROOT / "bat" / installer).read_text(encoding="utf-8")
    marker = "End deployment guard: no scheduled-task command may precede this point."
    assert source.count(marker) == 1
    prefix = source.split(marker)[0].rsplit("\n", 1)[0] + "\n"
    # Only the prefix enters this subprocess, so an assertion failure can never
    # accidentally create/query/change a scheduled task on the host.
    assert "schtasks" not in prefix.lower()
    assert "ScheduledTask" not in prefix
    script = tmp_path / "bat" / installer
    script.parent.mkdir()
    config = tmp_path / "configs/strategies/active_profile.json"
    config.parent.mkdir(parents=True)
    config.write_text("{invalid" if persisted == "MALFORMED_JSON" else json.dumps({"profile": persisted}))
    if script.suffix == ".ps1":
        prefix += "Write-Output 'GUARD_ALLOWED'\nexit 0\n"
        command = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)]
    else:
        prefix += "echo GUARD_ALLOWED\nendlocal & exit /b 0\n"
        command = [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c", str(script)]
    script.write_text(prefix, encoding="utf-8")
    env = {**os.environ, "FNO_ACTIVE_STRATEGY_PROFILE": "V13_V10_G"}
    result = subprocess.run(command, capture_output=True, text=True, timeout=20, env=env)
    output = result.stdout + result.stderr
    if persisted == "V13_V10_G":
        assert result.returncode == 0, output
        assert "GUARD_ALLOWED" in output
    else:
        assert result.returncode != 0
        assert "GUARD_ALLOWED" not in output
        if persisted == "V13_V10_G_3":
            assert "migrate_fno_v13_v10_g_to_g3.ps1" in output
