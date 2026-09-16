"""Canonical G launcher/task wiring; tests never install or run scheduled tasks."""
from pathlib import Path
import os
import re
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"
ROLES = ("scanner_5min", "equity_1min_feed", "confirmation_1min", "live_long", "live_short",
         "trade_logger", "net_result", "live_kite_qty1")
PAPER_ROLE_FLAGS = {"scanner_5min": "scanner-5m", "confirmation_1min": "confirmation-1m",
                    "live_long": "long-entry", "live_short": "short-entry",
                    "trade_logger": "trade-logger", "net_result": "net-result"}


@pytest.mark.parametrize("role", ROLES)
def test_new_launcher_has_canonical_session_and_preserves_profile(role):
    source = (BAT / f"run_fno_v13_v10_g_{role}.bat").read_text(encoding="utf-8")
    assert "title FnO V13-V10-G" in source
    assert 'set "FNO_V6_STRATEGY_PROFILE=V13_V10_G"' in source
    assert f'set "SESSION_ID=fno_v13_v10_g_{role}"' in source
    assert "fno_v6_live.py" not in source
    assert "fno_v6_live_kite_session.py" not in source
    if role in PAPER_ROLE_FLAGS:
        assert 'set "FNO_V6_EXECUTION_MODE=PAPER"' in source
        assert f'fno_v13_v10_g_live.py" --role {PAPER_ROLE_FLAGS[role]}' in source
        assert f"logs\\fno_v13_v10_g_{role}.log" in source
    if role == "equity_1min_feed":
        assert 'fno_equity_fetch_1min.py" --generation v6' in source
        assert "logs\\fno_v13_v10_g_equity_1min_feed.log" in source


@pytest.mark.parametrize("role", ROLES)
def test_legacy_launcher_only_forwards_to_one_canonical_owner(role):
    source = (BAT / f"run_fno_v6_{role}.bat").read_text(encoding="utf-8")
    assert f'call "%~dp0run_fno_v13_v10_g_{role}.bat" %*' in source
    assert "exit /b %ERRORLEVEL%" in source
    assert "python" not in source.lower()
    assert "supervise_command" not in source


@pytest.mark.skipif(os.name != "nt", reason="Windows BAT forwarding contract")
@pytest.mark.parametrize("role", ROLES)
def test_legacy_forwarder_preserves_arguments_and_exit_code_in_isolation(tmp_path, role):
    old = tmp_path / f"run_fno_v6_{role}.bat"
    old.write_text((BAT / old.name).read_text(encoding="utf-8"), encoding="utf-8")
    target = tmp_path / f"run_fno_v13_v10_g_{role}.bat"
    target.write_text("@echo off\necho FORWARDED %*\nexit /b 37\n", encoding="utf-8")
    completed = subprocess.run(["cmd.exe", "/d", "/c", str(old), "--example", "value"],
                               capture_output=True, text=True, timeout=10)
    assert completed.returncode == 37
    assert completed.stdout.strip() == "FORWARDED --example value"


def test_live_launcher_preserves_ack_quantity_supervision_and_auto_arms_each_session():
    source = (BAT / "run_fno_v13_v10_g_live_kite_qty1.bat").read_text(encoding="utf-8")
    assert 'set "FNO_V6_EXECUTION_MODE=LIVE"' in source
    assert 'set "FNO_V6_LIVE_ACK=I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"' in source
    assert 'if not "%~1"=="" (' in source
    assert "LIVE quantity 1" in source
    assert "fno_v13_v10_g_live_kite_session.py" in source
    assert "supervise_command.ps1" in source
    assert "v13_v10_g_live\\live_kite\\open_positions_{date}.json" in source
    assert "-StopRestartsAfterCutoff" in source
    assert "-OpenPositionsStateFilePattern" in source
    assert '"--auto-arm"' in source
    assert not re.search(r"kill_switch", source, re.I)


def test_generic_scheduler_has_only_seven_canonical_g_entries_at_0915():
    source = (BAT / "schedule_fno_oi_weekday.ps1").read_text(encoding="utf-8")
    active = source.split("$tasks = @(", 1)[1].split("\n)", 1)[0]
    entries = re.findall(r'Name = "([^"]+)"; Time = "([^"]+)"; Runner = "([^"]+)"', active)
    canonical = [entry for entry in entries if "fno_v13_v10_g" in entry[0]]
    expected = {(f"EQIDV2_fno_v13_v10_g_{role}_0915", "09:15", f"run_fno_v13_v10_g_{role}.bat")
                for role in ROLES if role != "live_kite_qty1"}
    assert set(canonical) == expected
    assert len(canonical) == 7
    assert "fno_v6" not in active
    # The unrelated universe, production OI fetcher, feature ranker and EOD QC stay scheduled.
    assert len(entries) == 11
    assert "MON,TUE,WED,THU,FRI" in source
    retired = source.split("$retiredTasks = @(", 1)[1].split("\n    )", 1)[0]
    for role, old_time in (("scanner_5min", "0918"), ("equity_1min_feed", "0919"),
                           ("confirmation_1min", "0919"), ("live_long", "0920"),
                           ("live_short", "0920"), ("trade_logger", "0920"), ("net_result", "0920")):
        assert f"EQIDV2_fno_v6_{role}_{old_time}" in retired


def test_live_scheduler_installs_only_canonical_task_and_retires_old_alias():
    source = (BAT / "schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1").read_text(encoding="utf-8")
    assert '$taskLeaf = "EQIDV2_fno_v13_v10_g_live_kite_qty1_0915"' in source
    assert '$startTime = "09:15"' in source
    assert "run_fno_v13_v10_g_live_kite_qty1.bat" in source
    assert 'Get-TaskIfPresent -Leaf $legacyLeaf' in source
    assert 'Disable-ScheduledTask -TaskName $legacyLeaf' in source
    assert 'Legacy V6 live task is running; migration was refused.' in source
    assert "-match '(?i)live_arm|kill_switch'" in source
    assert source.count("schtasks.exe /Create") == 1
    assert "schtasks.exe /Run" not in source
    assert "Start-ScheduledTask" not in source
    compatibility = (BAT / "schedule_fno_v6_live_kite_qty1_weekday.ps1").read_text(encoding="utf-8")
    assert 'Join-Path $PSScriptRoot "schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1"' in compatibility
    assert "schtasks" not in compatibility


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser")
def test_scheduler_scripts_parse_without_execution():
    files = [BAT / name for name in ("schedule_fno_oi_weekday.ps1",
                                     "schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1",
                                     "schedule_fno_v6_live_kite_qty1_weekday.ps1")]
    command = ("$files = @(" + ",".join("'" + str(path).replace("'", "''") + "'" for path in files) + "); "
               "foreach ($path in $files) { $tokens = $null; $parseErrors = $null; "
               "[System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$parseErrors) | Out-Null; "
               "if ($parseErrors.Count) { throw ($parseErrors | Out-String) } }")
    completed = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
                               capture_output=True, text=True, timeout=20)
    assert completed.returncode == 0, completed.stderr
