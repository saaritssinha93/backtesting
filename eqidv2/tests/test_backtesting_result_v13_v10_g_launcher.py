"""G daily launch/install contracts; tests never touch real scheduled tasks."""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from zoneinfo import ZoneInfo

import pytest


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"
CANONICAL = "run_backtesting_result_v13_v10_g_1620.bat"


def test_launcher_invokes_only_g_and_forwards_one_resolved_date():
    source = (BAT / CANONICAL).read_text(encoding="utf-8")
    assert 'set "SCRIPT_PATH=%BASE_DIR%\\backtesting_result_v13_v10_g_daily.py"' in source
    assert '--date "%TARGET_DAY%" --wait-for-data' in source
    assert "backtesting_result_v13_v10_g_%TARGET_DAY%.log" in source
    assert "backtesting_result_v13_v10_g_latest.log" in source
    assert "EQIDV2_FNO_BACKTEST_TARGET_DAY" in source
    assert "India Standard Time" in source
    assert "AddDays" not in source and "DayOfWeek" not in source
    assert "fno_daily_strategy_dashboard.py" not in source
    assert "data_for_backtesting_verify.py" not in source
    assert "%*" not in source


@pytest.fixture
def isolated_launcher(tmp_path):
    source = (BAT / CANONICAL).read_text(encoding="utf-8")
    source = re.sub(r'^set "BASE_DIR=.*"$', lambda _m: f'set "BASE_DIR={tmp_path}"', source, flags=re.MULTILINE)
    source = re.sub(r'^set "PYTHON_EXE=.*"$', lambda _m: f'set "PYTHON_EXE={sys.executable}"', source, flags=re.MULTILINE)
    path = tmp_path / CANONICAL
    path.write_text(source, encoding="utf-8")
    (tmp_path / "backtesting_result_v13_v10_g_daily.py").write_text(
        "import json, sys\nprint('ARGS=' + json.dumps(sys.argv[1:]))\nraise SystemExit(37)\n",
        encoding="utf-8",
    )
    return path


def _environment(**overrides):
    env = dict(os.environ)
    for key in ("EQIDV2_FNO_BACKTEST_TARGET_DAY", "EQIDV2_V11_TARGET_DAY", "TARGET_DAY", "VALIDATED_TARGET_DAY"):
        env.pop(key, None)
    env.update(overrides)
    return env


@pytest.mark.skipif(os.name != "nt", reason="Windows BAT date contract")
@pytest.mark.parametrize("args,overrides,expected", [
    ([], {"EQIDV2_FNO_BACKTEST_TARGET_DAY": "2026-09-13"}, "2026-09-13"),
    ([], {"EQIDV2_V11_TARGET_DAY": "2026-09-12"}, "2026-09-12"),
    ([], {"EQIDV2_FNO_BACKTEST_TARGET_DAY": "2026-09-11", "EQIDV2_V11_TARGET_DAY": "2026-09-10"}, "2026-09-11"),
    (["--date", "2026-09-09"], {"EQIDV2_FNO_BACKTEST_TARGET_DAY": "2026-09-11"}, "2026-09-09"),
    ([], {}, None),
])
def test_executed_date_log_date_latest_log_and_exit_code_are_consistent(isolated_launcher, args, overrides, expected):
    expected = expected or datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", str(isolated_launcher), *args],
        env=_environment(**overrides), capture_output=True, text=True, timeout=20,
    )
    assert completed.returncode == 37, completed.stdout + completed.stderr
    log_dir = isolated_launcher.parent / "logs"
    dated = log_dir / f"backtesting_result_v13_v10_g_{expected}.log"
    latest = log_dir / "backtesting_result_v13_v10_g_latest.log"
    content = dated.read_text(encoding="utf-8")
    recorded = next(line.removeprefix("ARGS=") for line in content.splitlines() if line.startswith("ARGS="))
    assert json.loads(recorded) == ["--date", expected, "--wait-for-data"]
    assert f"session date={expected} IST" in content
    assert latest.read_bytes() == dated.read_bytes()
    assert len(list(log_dir.glob("*.log"))) == 2


@pytest.mark.skipif(os.name != "nt", reason="Windows BAT validation contract")
@pytest.mark.parametrize("args,code", [
    (["--date", "2026-99-99"], 3),
    (["--date", "2026-09-15", "--date", "2026-09-16"], 2),
    (["--wait-for-data"], 2),
])
def test_invalid_or_conflicting_arguments_cannot_start_a_backtest(isolated_launcher, args, code):
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", str(isolated_launcher), *args],
        env=_environment(), capture_output=True, text=True, timeout=20,
    )
    assert completed.returncode == code, completed.stdout + completed.stderr
    assert not (isolated_launcher.parent / "logs").exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows BAT forwarding contract")
@pytest.mark.parametrize("old_name,new_name", [
    ("run_backtesting_result_v11_1600.bat", CANONICAL),
    ("schedule_backtesting_result_v11_weekday.bat", "schedule_backtesting_result_v13_v10_g_weekday.bat"),
])
def test_legacy_forwarders_preserve_arguments_and_exit_codes_without_old_execution(tmp_path, old_name, new_name):
    source = (BAT / old_name).read_text(encoding="utf-8")
    assert f'call "%~dp0{new_name}" %*' in source
    assert "schtasks" not in source and "python" not in source.lower()
    old = tmp_path / old_name
    old.write_text(source, encoding="utf-8")
    (tmp_path / new_name).write_text("@echo off\necho FORWARDED %*\nexit /b 41\n", encoding="utf-8")
    completed = subprocess.run(["cmd.exe", "/d", "/c", str(old), "--date", "2026-09-15"],
                               capture_output=True, text=True, timeout=10)
    assert completed.returncode == 41
    assert completed.stdout.strip() == "FORWARDED --date 2026-09-15"


def test_installer_and_preopen_use_only_canonical_daily_task():
    source = (BAT / "schedule_backtesting_result_v13_v10_g_weekday.bat").read_text(encoding="utf-8")
    assert 'set "TASK_BACKTEST=EQIDV2_backtesting_result_v13_v10_g_1620"' in source
    assert "run_backtesting_result_v13_v10_g_1620.bat" in source
    assert source.count("schtasks /Create") == 1
    assert "/SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:20" in source
    assert 'schtasks /Change /TN "%%T" /Disable' in source
    assert "schtasks /Delete" not in source and "schtasks /Run" not in source
    assert "harden_scheduled_task.ps1" in source
    preopen = (ROOT / "preopen_session_healthcheck.py").read_text(encoding="utf-8")
    assert '"EQIDV2_backtesting_result_v13_v10_g_1620"' in preopen
    assert '"EQIDV2_backtesting_result_v11_1600"' not in preopen
