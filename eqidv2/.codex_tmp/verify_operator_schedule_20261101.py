"""Read-only task/control verification following the user's schedule install."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json
import sys
import win32com.client

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.repair_g3_standing_schedule import verify_registered, new_action, next_weekday_run, TASK
now = datetime.now(timezone(timedelta(hours=5,minutes=30)))
service=win32com.client.Dispatch('Schedule.Service'); service.Connect()
folder=service.GetFolder('\\')
backup=ROOT/'outputs/readiness_repair_20261102/standing_install_20261101T233325079264'
task=folder.GetTask(TASK)
verify_registered(task,(backup/'after.xml').read_text(encoding='utf-8'),
                  expected_action=new_action(),expected_next_run=next_weekday_run(now))
names=['EQIDV2_authentication_v2_0900','EQIDV2_fno_options_atm_fetch_5min_0907',TASK,
       'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915',
       'EQIDV2_fno_v13_v10_g_3_options_live_long_0915','EQIDV2_fno_v13_v10_g_3_options_live_short_0915']
rows=[]
for name in names:
    t=folder.GetTask(name)
    rows.append(dict(task=name,enabled=bool(t.Enabled),state=int(t.State),
        next_run_ist=t.NextRunTime.replace(tzinfo=None).isoformat(),
        running_instances=len(list(t.GetInstances(0))),
        execution_limit=t.Definition.Settings.ExecutionTimeLimit))
arm=json.loads(Path('C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/live_arm.json').read_text(encoding='utf-8-sig'))
profile=json.loads((ROOT/'configs/strategies/active_profile.json').read_text(encoding='utf-8-sig'))
report=dict(checked_at_ist=now.isoformat(),standing_install_verified=True,tasks=rows,
            dated_arm=arm['session_date'],dated_approval=profile['live_approval']['session_date'],
            note='Current dated approval is expected to be refreshed by Nov2 09:10 gate after same-day checks; schedule install does not mint approval.',
            controls_changed=False,workers_started=False)
(ROOT/'outputs/readiness_repair_20261102/operator_install_verified.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
