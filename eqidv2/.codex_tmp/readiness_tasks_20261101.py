"""Read-only native scheduler/process inventory; no credentials or controls modified."""
from pathlib import Path
from datetime import datetime
import json
import sys
import win32com.client

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.fno_data_supervisor import native_processes

service = win32com.client.Dispatch('Schedule.Service')
service.Connect()
folder = service.GetFolder('\\')
rows = []
for task in folder.GetTasks(0):
    name = task.Name
    if not name.startswith('EQIDV2_') or not any(part in name for part in ('g3_live_standing', 'authentication_v2', 'fno_options_atm', 'fno_v13_v10_g_3_', 'preopen_session_healthcheck', 'fno_oi_feature_ranker', 'fno_oi_fetch_5min_fast_production')):
        continue
    definition = task.Definition
    rows.append(dict(name=name, enabled=task.Enabled, state=task.State,
                     last_run=str(task.LastRunTime), next_run=str(task.NextRunTime),
                     last_result=task.LastTaskResult,
                     limit=definition.Settings.ExecutionTimeLimit,
                     multiple_instances=definition.Settings.MultipleInstances,
                     start_when_available=definition.Settings.StartWhenAvailable,
                     actions=[dict(path=a.Path, arguments=a.Arguments, working_directory=a.WorkingDirectory) for a in definition.Actions],
                     instances=[dict(pid=i.EnginePID, state=i.State, guid=i.InstanceGuid) for i in task.GetInstances(0)]))
processes = []
for row in native_processes():
    command = row.get('command', '').lower()
    matched = [part for part in ('fno_options_atm_fetch_5min', 'g3_live_standing', 'fno_v13_v10_g_3_live_kite', 'fno_v13_v10_g_3_options', 'authentication_v2') if part in command]
    if matched:
        processes.append(dict(pid=row['pid'], ppid=row['ppid'], name=row['name'], matches=matched))
report = dict(timestamp=datetime.now().isoformat(), tasks=rows, processes=processes)
output = ROOT / 'outputs/readiness_repair_20261102'
output.mkdir(parents=True, exist_ok=True)
(output/'tasks_before.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
