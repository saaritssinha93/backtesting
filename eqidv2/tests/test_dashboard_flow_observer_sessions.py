from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from dashboard_flow_observer_sessions import CARD_TASK_NAMES, SESSION_SPECS, session_items, status_path

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 13, 9, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


def test_reporting_cards_share_one_live_paper_worker_without_invented_execution(tmp_path):
    before = list(tmp_path.rglob("*"))
    items = session_items(tmp_path, now_ist=NOW)
    assert list(tmp_path.rglob("*")) == before
    assert len(items) == 2
    assert all(item["status"]["execution_mode"] == "REPORTING_ONLY" for item in items)
    assert all(item["status"]["status"] == "NOT_SCHEDULED" for item in items)
    assert items[0]["status"]["consumers"] == "G3 LIVE and PAPER 09:20 inputs and flow analysis (one shared data worker)"
    assert all(item["dashboard_flow_path"] == "/dashboard-flow#live" for item in items)
    assert set(CARD_TASK_NAMES) == set(SESSION_SPECS)


def test_status_reads_only_dedicated_observer_files(tmp_path):
    card = "dashboard_flow_opening_observer"
    path = status_path(tmp_path, card)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"status": "WAITING_INPUTS", "phase": "AUTH", "session_date": "2026-10-13", "updated_at_ist": NOW.isoformat(), "reason": "Waiting for existing authentication"}))
    os.utime(path, (NOW.timestamp(), NOW.timestamp()))
    unrelated = tmp_path / "runtime_status" / "session.json"
    unrelated.parent.mkdir()
    unrelated.write_text('{"status": "FAILED"}')
    item = session_items(tmp_path, now_ist=NOW)[0]
    assert item["status"]["status"] == "WAITING"
    assert item["status"]["phase"] == "AUTH"
    assert item["file_name"] == str(path)
    assert "Waiting for existing authentication" in item["tail"]


@pytest.mark.parametrize("content", ["[]", "{broken", '"scalar"'])
def test_bad_observer_status_does_not_break_dashboard(tmp_path, content):
    path = status_path(tmp_path, "dashboard_flow_opening_history")
    path.parent.mkdir(parents=True)
    path.write_text(content)
    items = session_items(tmp_path, now_ist=NOW)
    assert items[1]["status"]["status"] == "PARTIAL"
    assert items[0]["status"]["status"] == "NOT_SCHEDULED"


def test_stale_running_file_is_not_reported_as_live(tmp_path):
    path = status_path(tmp_path, "dashboard_flow_opening_observer")
    path.parent.mkdir(parents=True)
    path.write_text('{"status": "RUNNING", "session_date": "2026-10-12"}')
    os.utime(path, (NOW.timestamp() - 86400, NOW.timestamp() - 86400))
    item = session_items(tmp_path, now_ist=NOW)[0]
    assert item["status"]["status"] == "STALE_OUTPUT"
    assert item["status"]["session_date"] == "2026-10-12"


def test_installed_schedule_waits_for_first_run_and_removed_schedule_does_not(tmp_path):
    path = tmp_path / "dashboard_flow_observer" / "state" / "task_schedule.json"
    path.parent.mkdir(parents=True)
    manifest = {"schema": "dashboard_flow_observer_schedule_v1", "installed": True,
                "tasks": [SESSION_SPECS["dashboard_flow_opening_observer"]["task"].lstrip("\\")]}
    path.write_text(json.dumps(manifest))
    observer, history = session_items(tmp_path, now_ist=NOW)
    assert observer["status"]["status"] == "SCHEDULED"
    assert observer["status"]["phase"] == "WAITING_FIRST_RUN"
    assert observer["exists"] is False
    assert history["status"]["status"] == "NOT_SCHEDULED"
    manifest["installed"] = False
    path.write_text(json.dumps(manifest))
    assert session_items(tmp_path, now_ist=NOW)[0]["status"]["status"] == "NOT_SCHEDULED"


def test_dashboard_wires_real_tasks_without_execution_controls():
    import log_dashboard_server as server
    for card_id, names in CARD_TASK_NAMES.items():
        assert server.CARD_TASK_NAMES[card_id] == names
        assert card_id not in server.RESTARTABLE_CARDS
        assert card_id not in server.STATUS_FILES
    source = (ROOT / "log_dashboard_server.py").read_text(encoding="utf-8")
    assert "items.extend(opening_observer_session_items(runtime_dir()))" in source
    assert "G3 Opening Data Monitoring" in source
    assert "apiUrl('/dashboard-flow') + '#live'" in source


@pytest.mark.skipif(os.name != "nt", reason="Windows runners")
@pytest.mark.parametrize("script,module,flag,expected", [
    ("run_dashboard_flow_observer.ps1", "dashboard_flow_observer.py", "-Run", ["--run"]),
    ("run_dashboard_flow_observer_history.ps1", "dashboard_flow_observer_history.py", "-Fetch", ["--latest-completed", "--fetch"]),
])
def test_hidden_runner_preserves_quoted_paths_and_uses_only_own_logs(tmp_path, script, module, flag, expected):
    project = tmp_path / "test project with spaces"
    scripts = project / "bat"
    scripts.mkdir(parents=True)
    shutil.copy(ROOT / "bat" / script, scripts / script)
    (project / module).write_text("import json, sys\nprint(json.dumps(sys.argv[1:]))\n")
    data = tmp_path / "data with spaces"
    process = subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(scripts / script), "-DataRoot", str(data), "-Python", sys.executable, flag], capture_output=True, text=True, timeout=30)
    assert process.returncode == 0, process.stdout + process.stderr
    logs = list((data / "dashboard_flow_observer" / "logs").glob("*.stdout.log"))
    assert len(logs) == 1
    args = json.loads(logs[0].read_text(encoding="utf-8-sig"))
    assert args[:4] == ["--data-root", str(data), "--app", "app1"]
    assert args[4:] == expected
    assert set(path.name for path in data.iterdir()) == {"dashboard_flow_observer"}


@pytest.mark.skipif(os.name != "nt", reason="Windows scheduler plan")
def test_scheduler_plan_is_non_mutating_and_exactly_two_tasks(tmp_path):
    data = tmp_path / "uncreated data"
    process = subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "bat" / "schedule_dashboard_flow_observer.ps1"), "-Plan", "-DataRoot", str(data), "-Python", sys.executable], capture_output=True, text=True, timeout=30)
    assert process.returncode == 0, process.stdout + process.stderr
    rows = json.loads(process.stdout)
    assert {row["task"] for row in rows} == {value["task"].lstrip("\\") for value in SESSION_SPECS.values()}
    assert [row["time_ist"] for row in rows] == ["08:55", "17:00"]
    assert all(row["start_when_available"] is False and row["starts_now"] is False for row in rows)
    assert all(row["hidden"] and row["multiple_instances"] == "IgnoreNew" for row in rows)
    assert not data.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows scheduler transaction")
@pytest.mark.parametrize("fail_second,remove_after", [(False, False), (True, False), (False, True)])
def test_scheduler_transaction_with_mocked_task_service(tmp_path, fail_second, remove_after):
    """Exercise install and rollback without registering an operating-system task."""
    harness = r'''
$ErrorActionPreference = 'Stop'
$global:testObserverTasks = @{}
$global:testObserverActions = New-Object 'System.Collections.Generic.List[string]'
function Get-ScheduledTask { param($TaskPath,$TaskName,$ErrorAction) return $global:testObserverTasks[$TaskName] }
function Get-TimeZone { [pscustomobject]@{BaseUtcOffset=[TimeSpan]::FromMinutes(330)} }
function New-ScheduledTaskAction { param($Execute,$Argument,$WorkingDirectory) [pscustomobject]@{Execute=$Execute;Arguments=$Argument;WorkingDirectory=$WorkingDirectory} }
function New-ScheduledTaskTrigger { param([switch]$Weekly,$WeeksInterval,$DaysOfWeek,$At) [pscustomobject]@{DaysOfWeek=$DaysOfWeek;At=$At} }
function New-ScheduledTaskSettingsSet { param([switch]$Hidden,$MultipleInstances,$ExecutionTimeLimit,[switch]$AllowStartIfOnBatteries,[switch]$DontStopIfGoingOnBatteries) [pscustomobject]@{Hidden=$Hidden.IsPresent;MultipleInstances=$MultipleInstances;StartWhenAvailable=$true;RestartCount=9;ExecutionTimeLimit=$ExecutionTimeLimit} }
function New-ScheduledTaskPrincipal { param($UserId,$LogonType,$RunLevel) [pscustomobject]@{UserId=$UserId;LogonType=$LogonType;RunLevel=$RunLevel} }
function New-ScheduledTask { param($Action,$Trigger,$Settings,$Principal,$Description) [pscustomobject]@{Actions=@($Action);Triggers=@($Trigger);Settings=$Settings;Principal=$Principal;Description=$Description;State='Ready'} }
function Register-ScheduledTask { param($TaskPath,$TaskName,$InputObject,[switch]$Force,$ErrorAction) $global:testObserverActions.Add('REGISTER '+$TaskName); $global:testObserverTasks[$TaskName]=$InputObject; if (__FAIL__ -and $TaskName -like '*history*') { $InputObject.Settings.Hidden=$false }; $InputObject }
function Unregister-ScheduledTask { param($TaskPath,$TaskName,$Confirm,$ErrorAction) $global:testObserverActions.Add('REMOVE '+$TaskName); $global:testObserverTasks.Remove($TaskName) }
function Export-ScheduledTask { param($TaskPath,$TaskName,$ErrorAction) '<Task />' }
$caught = ''
try {
    & '__INSTALLER__' -Apply -DataRoot '__DATA__' -Python '__PYTHON__' | Out-Null
    if (__REMOVE__) { & '__INSTALLER__' -Remove -DataRoot '__DATA__' -Python '__PYTHON__' | Out-Null }
} catch { $caught = $_.Exception.Message }
[pscustomobject]@{actions=@($global:testObserverActions.ToArray()); tasks=@($global:testObserverTasks.Keys); error=$caught; task_data=@($global:testObserverTasks.Values)} | ConvertTo-Json -Depth 8
'''
    values = {"__FAIL__": "$true" if fail_second else "$false", "__REMOVE__": "$true" if remove_after else "$false", "__INSTALLER__": str(ROOT / "bat" / "schedule_dashboard_flow_observer.ps1"), "__DATA__": str(tmp_path / "data"), "__PYTHON__": sys.executable}
    for key, value in values.items():
        harness = harness.replace(key, value.replace("'", "''"))
    path = tmp_path / "mock_tasks.ps1"
    path.write_text(harness, encoding="utf-8")
    process = subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(path)], capture_output=True, text=True, timeout=30)
    assert process.returncode == 0, process.stdout + process.stderr
    result = json.loads(process.stdout)
    expected = {spec["task"].lstrip("\\") for spec in SESSION_SPECS.values()}
    assert {action.split(" ", 1)[1] for action in result["actions"]} == expected
    manifest_path = tmp_path / "data" / "dashboard_flow_observer" / "state" / "task_schedule.json"
    if fail_second:
        assert "failed verification" in result["error"]
        assert result["tasks"] == []
        assert len(result["actions"]) == 4
        assert not manifest_path.exists()
    elif remove_after:
        assert result["error"] == ""
        assert result["tasks"] == []
        assert len(result["actions"]) == 4
        manifest = json.loads(manifest_path.read_text())
        assert manifest["installed"] is False
        assert manifest["tasks"] == []
        assert all(item["status"]["status"] == "NOT_SCHEDULED" for item in session_items(tmp_path / "data", now_ist=NOW))
    else:
        assert result["error"] == ""
        assert set(result["tasks"]) == expected
        assert len(result["actions"]) == 2
        manifest = json.loads(manifest_path.read_text())
        assert manifest["installed"] is True
        assert set(manifest["tasks"]) == expected
        assert all(item["status"]["phase"] == "WAITING_FIRST_RUN" for item in session_items(tmp_path / "data", now_ist=NOW))
        for task in result["task_data"]:
            assert task["Settings"]["StartWhenAvailable"] is False
            assert task["Settings"]["RestartCount"] == 0
            assert "-Scheduled" in task["Actions"][0]["Arguments"]
            assert "-WindowStyle Hidden" in task["Actions"][0]["Arguments"]


@pytest.mark.skipif(os.name != "nt", reason="Windows runner validation")
@pytest.mark.parametrize("script", ["run_dashboard_flow_observer.ps1", "run_dashboard_flow_observer_history.ps1", "schedule_dashboard_flow_observer.ps1"])
def test_runner_rejects_unconfigured_app_without_writes(tmp_path, script):
    data = tmp_path / "uncreated"
    result = subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "bat" / script), "-DataRoot", str(data), "-App", "app9"], capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "does not match" in result.stderr
    assert not data.exists()
