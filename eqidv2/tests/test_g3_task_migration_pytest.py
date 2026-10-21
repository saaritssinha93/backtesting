from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import subprocess

import pytest


def _powershell(script: str, root: Path) -> subprocess.CompletedProcess[str]:
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand", encoded],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )


@pytest.mark.skipif(os.name != "nt", reason="PowerShell Task Scheduler XML test")
def test_g3_pipeline_xml_recovery_policy_is_narrow_and_complete() -> None:
    root = Path(__file__).resolve().parents[1]
    helper = str(root / "bat" / "g3_task_xml_helpers.ps1").replace("'", "''")
    script = rf"""
. '{helper}'
$source = @'
<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task" version="1.4">
<RegistrationInfo><URI>\old</URI></RegistrationInfo>
<Triggers><CalendarTrigger><StartBoundary>2026-10-01T09:15:00</StartBoundary>
<Repetition><Interval>PT5M</Interval><Duration>PT2H10M</Duration></Repetition>
<ScheduleByWeek><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek>
</CalendarTrigger></Triggers><Principals/><Settings><MultipleInstancesPolicy>Parallel</MultipleInstancesPolicy></Settings>
<Actions><Exec><Command>old.bat</Command></Exec></Actions></Task>
'@
$converted = ConvertTo-G3ScheduledTaskXml -SourceXml $source -NewTaskName 'pipeline' -Launcher 'new.bat' -PipelineRecovery
$plain = ConvertTo-G3ScheduledTaskXml -SourceXml $source -NewTaskName 'plain' -Launcher 'new.bat'
    $live = ConvertTo-G3ScheduledTaskXml -SourceXml $source -NewTaskName 'live' -Launcher 'new.bat' -FailClosedLiveActivation
function Read-Policy([string]$raw) {{
    [xml]$xml = $raw
    $ns = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $ns.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
    $rep = $xml.SelectSingleNode('/t:Task/t:Triggers/t:CalendarTrigger/t:Repetition', $ns)
    $settings = $xml.SelectSingleNode('/t:Task/t:Settings', $ns)
    $restart = $settings.SelectSingleNode('t:RestartOnFailure', $ns)
    [ordered]@{{
        Duration = $rep.SelectSingleNode('t:Duration', $ns).InnerText
        StopAtDurationEnd = $rep.SelectSingleNode('t:StopAtDurationEnd', $ns).InnerText
        MultipleInstances = $settings.SelectSingleNode('t:MultipleInstancesPolicy', $ns).InnerText
        StartWhenAvailable = $settings.SelectSingleNode('t:StartWhenAvailable', $ns).InnerText
        ExecutionTimeLimit = $settings.SelectSingleNode('t:ExecutionTimeLimit', $ns).InnerText
        RestartCount = if ($null -ne $restart) {{ $restart.SelectSingleNode('t:Count', $ns).InnerText }} else {{ '' }}
        RestartInterval = if ($null -ne $restart) {{ $restart.SelectSingleNode('t:Interval', $ns).InnerText }} else {{ '' }}
    }}
}}
    [ordered]@{{ Pipeline = (Read-Policy $converted); Plain = (Read-Policy $plain); Live = (Read-Policy $live) }} | ConvertTo-Json -Compress -Depth 4
"""
    result = _powershell(script, root)
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["Pipeline"] == {
        "Duration": "PT4H20M",
        "StopAtDurationEnd": "false",
        "MultipleInstances": "IgnoreNew",
        "StartWhenAvailable": "true",
        "ExecutionTimeLimit": "PT0S",
        "RestartCount": "3",
        "RestartInterval": "PT1M",
    }
    assert payload["Plain"]["Duration"] == "PT2H10M"
    assert payload["Plain"]["RestartCount"] == ""
    assert payload["Live"]["MultipleInstances"] == "IgnoreNew"
    assert payload["Live"]["StartWhenAvailable"] == "false"
    assert payload["Live"]["RestartCount"] == ""


@pytest.mark.skipif(os.name != "nt", reason="PowerShell migration dry run")
def test_g3_migration_marks_only_three_pipeline_recovery_tasks() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
            str(root / "bat" / "migrate_fno_v13_v10_g_to_g3.ps1"),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    recovery = [item["NewTask"] for item in payload["Tasks"] if item["PipelineRecovery"]]
    assert recovery == [
        "EQIDV2_fno_v13_v10_g_3_scanner_5min_0915",
        "EQIDV2_fno_v13_v10_g_3_equity_1min_feed_0915",
        "EQIDV2_fno_v13_v10_g_3_confirmation_1min_0915",
    ]


@pytest.mark.skipif(os.name != "nt", reason="PowerShell repair dry run")
def test_g3_pipeline_repair_is_dry_by_default_and_never_starts_tasks() -> None:
    root = Path(__file__).resolve().parents[1]
    repair = root / "bat" / "repair_fno_v13_v10_g_3_pipeline_tasks.ps1"
    result = subprocess.run(
        [
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
            str(repair),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["Mode"] == "DRY_RUN"
    assert payload["StartsTasks"] is False
    assert payload["StopsTasks"] is False
    assert len(payload["Tasks"]) == 3
    source = repair.read_text(encoding="utf-8")
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "Export-ScheduledTask" in source
    assert "rollbackErrors" in source
