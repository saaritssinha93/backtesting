"""After-cutoff integration smoke of the data-only scheduler; no feed worker may start."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys
import time
import win32com.client

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.fno_data_supervisor import native_processes
now=datetime.now(timezone(timedelta(hours=5,minutes=30)))
assert now.date().isoformat()=='2026-11-01' and now.strftime('%H:%M')>'16:00'
launcher=ROOT/'bat/run_fno_options_atm_fetch_5min.bat'
assert 'powershell' not in launcher.read_text().lower()
assert '--service options' in launcher.read_text()
s=win32com.client.Dispatch('Schedule.Service'); s.Connect(); f=s.GetFolder('\\')
name='EQIDV2_fno_options_atm_fetch_5min_0907'
task=f.GetTask(name)
assert task.Enabled and task.State==3 and not list(task.GetInstances(0))
assert task.Definition.Settings.ExecutionTimeLimit=='PT7H'
assert Path(task.Definition.Actions.Item(1).Path).resolve()==launcher.resolve()
assert not [p for p in native_processes() if 'fno_options_atm_fetch_5min' in p.get('command','').lower()]
started=time.time()
instance=task.Run('')
for _ in range(40):
    time.sleep(.5)
    current=f.GetTask(name)
    if current.State==3 and not list(current.GetInstances(0)):
        break
else:
    task.Stop(0)
    raise RuntimeError('After-cutoff launcher did not finish within 20 seconds')
status=ROOT/'logs/fno_options_atm_fetch_5min.supervisor.status'
assert status.stat().st_mtime>=started
payload=dict(line.split('=',1) for line in status.read_text().splitlines() if '=' in line)
assert payload['state']=='SKIPPED_CUTOFF', payload
assert not payload['worker_pid'] and current.LastTaskResult==0
assert not [p for p in native_processes() if 'fno_options_atm_fetch_5min' in p.get('command','').lower()]
report=dict(task=name,status=payload['state'],worker_started=False,task_result=current.LastTaskResult,
            enabled=current.Enabled,next_run_local_wall=current.NextRunTime.replace(tzinfo=None).isoformat(),
            execution_limit=current.Definition.Settings.ExecutionTimeLimit,
            elapsed_seconds=round(time.time()-started,3),checked_at_ist=now.isoformat())
(ROOT/'outputs/readiness_repair_20261102/options_scheduler_smoke.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
