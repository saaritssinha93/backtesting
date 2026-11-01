"""Repair only the stuck, data-only Options ATM scheduled task."""
from datetime import datetime
from pathlib import Path
import argparse
import json
import sys
import time
import win32com.client

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.fno_data_supervisor import native_processes

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--apply', action='store_true')
args = parser.parse_args()
name = 'EQIDV2_fno_options_atm_fetch_5min_0907'
service = win32com.client.Dispatch('Schedule.Service')
service.Connect()
folder = service.GetFolder('\\')
task = folder.GetTask(name)
definition = task.Definition
actions = list(definition.Actions)
expected = ROOT / 'bat/run_fno_options_atm_fetch_5min.bat'
assert len(actions) == 1 and Path(actions[0].Path).resolve() == expected.resolve()
assert not actions[0].Arguments
assert definition.Principal.LogonType == 3, 'Expected interactive task; preserve credentials.'
assert definition.Settings.MultipleInstances == 2, 'Expected IgnoreNew singleton scheduling.'
instances = list(task.GetInstances(0))
processes = native_processes()
owners = [p for p in processes if 'fno_options_atm_fetch_5min' in p.get('command', '').lower()]
assert not any(p['name'].lower() in ('python.exe', 'pythonw.exe', 'py.exe') for p in owners), 'A data worker exists; require fresh ownership review.'
assert all(p['name'].lower() in ('cmd.exe', 'powershell.exe') for p in owners), 'Unexpected owner.'
engine_pids = {i.EnginePID for i in instances}
assert all(p['pid'] in engine_pids or p['ppid'] in engine_pids for p in owners), 'Owner not bound to this task instance.'
report = dict(task=name, apply=args.apply, old_limit=definition.Settings.ExecutionTimeLimit,
              new_limit='PT7H', old_state=task.State, next_run_local_wall=str(task.NextRunTime),
              stale_owners=[{k:p[k] for k in ('pid','ppid','name')} for p in owners])
output = ROOT / 'outputs/readiness_repair_20261102'
output.mkdir(parents=True, exist_ok=True)
if args.apply:
    backup = output/'options_task_before.xml'
    assert not backup.exists(), 'Backup exists; inspect prior repair instead of overwriting.'
    backup.write_text(task.Xml, encoding='utf-8')
    if instances:
        task.Stop(0)
    for attempt in range(30):
        remaining = [p for p in native_processes() if 'fno_options_atm_fetch_5min' in p.get('command','').lower()]
        if not remaining and not list(task.GetInstances(0)):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError('Task stop did not clear all data-only owners; no task replacement performed.')
    definition.Settings.ExecutionTimeLimit = 'PT7H'
    installed = folder.RegisterTaskDefinition(name, definition, 6, definition.Principal.UserId, None, 3)
    assert installed.Definition.Settings.ExecutionTimeLimit == 'PT7H'
    assert installed.Enabled and installed.State == 3 and not list(installed.GetInstances(0))
    (output/'options_task_after.xml').write_text(installed.Xml, encoding='utf-8')
    report.update(new_state=installed.State, enabled=installed.Enabled,
                  next_run_local_wall=str(installed.NextRunTime), remaining_owners=[],
                  stopped_only_data_task=True, starts_worker=False)
    (output/'options_task_repair.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
