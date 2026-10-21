"""Safety contract for the explicitly approved 21 October handoff."""
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def source(name: str) -> str:
    return (ROOT / "bat" / name).read_text(encoding="utf-8")


def run_status_parser(path: Path, required: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    helper = str(ROOT / "bat" / "g3_activation_status_helpers.ps1").replace("'", "''")
    status_path = str(path).replace("'", "''")
    required_args = ", ".join(f"'{key}'" for key in required)
    command = (
        "try { "
        f". '{helper}'; "
        f"$value = Read-StrictLiteralKeyValueFile -Path '{status_path}' "
        f"-RequiredKeys @({required_args}); "
        "$value | ConvertTo-Json -Compress; "
        "} catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }"
    )
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    return subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def test_one_time_task_is_exactly_dated_and_cannot_catch_up_or_retry():
    script = source("schedule_g3_live_activation_20261021.ps1")
    assert "EQIDV2_g3_live_approve_20261021_0910" in script
    assert "2026-10-21 09:10:00" in script
    assert "New-ScheduledTaskTrigger -Once" in script
    assert "-StartWhenAvailable" not in script
    assert "RestartCount -ne 0" in script
    assert "Start-ScheduledTask" not in script
    assert "-Weekly" not in script


def test_activation_requires_exact_session_auth_health_and_safe_deadline():
    script = source("activate_g3_live_20261021.ps1")
    for evidence in (
        "2026-10-21",
        "09:09:00 through 09:12:59",
        "EQIDV2_authentication_v2_0900",
        "authentication_v2_runner.status",
        "preopen_session_healthcheck_latest.json",
        "fno_fast_production_runtime",
        "fno_fast_production_first_slot",
        "max_reserved_open_positions",
        "max_daily_entries",
        "max_reserved_gross_exposure_rs",
        "-EnableBefore '09:12:30'",
    ):
        assert evidence in script
    assert "Start-ScheduledTask" not in script
    assert "test_kite_order_readiness.py" not in script  # owned transactionally by approve_g3_live.ps1
    assert "ConvertFrom-StringData" not in script
    assert "Read-StrictLiteralKeyValueFile" in script
    assert "'status', 'script', 'ts', 'exit_code', 'log_file'" in script


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="PowerShell status parser contract",
)
def test_auth_status_parser_preserves_windows_paths_and_first_equals(tmp_path: Path):
    status_file = tmp_path / "authentication.status"
    log_path = r"C:\Users\Saarit\logs\authentication_v2_runner.log?token=a=b"
    status_file.write_text(
        "status=SUCCESS\n"
        "script=authentication_v2.py\n"
        "ts=2026-10-21_08:33:38\n"
        "exit_code=0\n"
        f"log_file={log_path}\n",
        encoding="utf-8",
    )

    result = run_status_parser(
        status_file, ("status", "script", "ts", "exit_code", "log_file")
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["log_file"] == log_path


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell") is None,
    reason="PowerShell status parser contract",
)
@pytest.mark.parametrize(
    ("contents", "error_text"),
    [
        ("status=SUCCESS\nstatus=FAILED\nts=2026-10-21_08:33:38\n", "Duplicate key"),
        ("status=SUCCESS\nmalformed\nts=2026-10-21_08:33:38\n", "expected key=value"),
        ("status=SUCCESS\nts=\n", "missing or blank"),
        ("status=SUCCESS\n", "missing or blank"),
    ],
)
def test_auth_status_parser_fails_closed_on_malformed_duplicate_or_required_fields(
    tmp_path: Path, contents: str, error_text: str
):
    status_file = tmp_path / "authentication.status"
    status_file.write_text(contents, encoding="utf-8")

    result = run_status_parser(status_file, ("status", "ts"))

    assert result.returncode != 0
    assert error_text in result.stderr


def test_approved_target_disables_late_catchup_before_it_is_enabled():
    approve = source("approve_g3_live.ps1")
    helper = source("g3_task_xml_helpers.ps1")
    assert "-FailClosedLiveActivation" in approve
    enable_at = approve.index("Enable-ScheduledTask")
    assert "Assert-G3PreparedApproval $false" in approve[:enable_at]
    assert "Assert-G3EnableDeadline" in approve[enable_at:]
    assert "Assert-G3PreparedApproval $true" in approve[enable_at:]
    assert "Assert-G3EnableDeadline" in approve
    assert "@('StartWhenAvailable', 'false')" in helper
    assert "RestartOnFailure" in helper
