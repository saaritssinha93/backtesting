"""Read-only cross-check after the operator installed the Python standing gate."""
from pathlib import Path
import sys
import json
import winreg
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import g3_live_standing as gate
from tools import repair_g3_standing_schedule as repair

now = gate.datetime.now(gate.IST)
with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                   r'SYSTEM\CurrentControlSet\Control\TimeZoneInformation') as key:
    timezone_name = winreg.QueryValueEx(key, 'TimeZoneKeyName')[0].rstrip('\x00')
standing, target = gate.task_snapshot(repair.TASK), gate.task_snapshot(gate.TASK)
assert repair.action_identity(standing['xml']) == repair.new_action()
assert repair.prepared_xml(standing['xml'], target['xml'])
assert standing['enabled'] is True and standing['state'] == 3
expected = repair.next_weekday_run(now)
assert gate.datetime.fromisoformat(standing['next_run']) == expected
gate.validate_task(target, ROOT, enabled=True, day=expected.date())
rows = []
for name, task in ((repair.TASK, standing), (gate.TASK, target)):
    xml = ET.fromstring(task['xml'])
    rows.append(dict(task=name, enabled=task['enabled'], state=task['state'],
        next_run_local_wall=task['next_run'], last_run_local_wall=task['last_run'],
        last_result=task['last_result'],
        start_boundary=xml.findtext('{*}Triggers/{*}CalendarTrigger/{*}StartBoundary'),
        action=dict(command=xml.findtext('{*}Actions/{*}Exec/{*}Command'),
                    arguments=xml.findtext('{*}Actions/{*}Exec/{*}Arguments')),
        execution_limit=xml.findtext('{*}Settings/{*}ExecutionTimeLimit'),
        start_when_available=xml.findtext('{*}Settings/{*}StartWhenAvailable', 'false'),
        multiple_instances=xml.findtext('{*}Settings/{*}MultipleInstancesPolicy')))
print(json.dumps(dict(checked_at_ist=now.isoformat(), host_timezone=timezone_name,
    definitions_verified=True, tasks=rows, mutates_controls=False, broker_calls=False), indent=2))
