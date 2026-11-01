"""Read-only readiness evidence, including a bounded trivial PowerShell probe."""
from pathlib import Path
import subprocess
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

print('AUDIT_IST', datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat())
began = time.monotonic()
try:
    result = subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command','Write-Output PS_READY'], capture_output=True, text=True, timeout=5, creationflags=subprocess.CREATE_NO_WINDOW)
    print('POWERSHELL', json.dumps({'returncode':result.returncode, 'stdout':result.stdout.strip(), 'elapsed_seconds':round(time.monotonic()-began,2)}))
except subprocess.TimeoutExpired:
    print('POWERSHELL timeout_after_5s_own_probe_killed')

tasks = ['EQIDV2_g3_live_standing_approval_0910','EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915','EQIDV2_authentication_v2_0900']
for name in tasks:
    for mode in ('info','xml'):
        command = ['schtasks.exe','/Query','/TN',name] + (['/XML'] if mode=='xml' else ['/V','/FO','LIST'])
        try:
            proc = subprocess.run(command,capture_output=True,timeout=8,creationflags=subprocess.CREATE_NO_WINDOW)
            data = proc.stdout.decode('utf-16' if proc.stdout.startswith(b'\xff\xfe') else 'utf-8-sig',errors='replace')
            if mode == 'info':
                selected = [line for line in data.splitlines() if any(line.startswith(field) for field in ('TaskName:','Next Run Time:','Status:','Last Run Time:','Last Result:','Scheduled Task State:','Task To Run:','Run As User:','Schedule Type:'))]
                print(name, 'INFO', json.dumps(selected))
            elif proc.returncode==0:
                root = ET.fromstring(data)
                ns={'t':'http://schemas.microsoft.com/windows/2004/02/mit/task'}
                print(name,'XML',json.dumps({'enabled':root.findtext('t:Settings/t:Enabled',namespaces=ns),'limit':root.findtext('t:Settings/t:ExecutionTimeLimit',namespaces=ns),'start_when_available':root.findtext('t:Settings/t:StartWhenAvailable',namespaces=ns),'action':[{child.tag.split('}')[-1]:child.text for child in act} for act in root.findall('t:Actions/t:Exec',ns)],'trigger':ET.tostring(root.find('t:Triggers',ns),encoding='unicode')}))
        except subprocess.TimeoutExpired:
            print(name, mode, 'timeout_own_query_killed')

for file in ['logs/g3_live_standing_activation.log','logs/g3_live_standing_healthcheck.log']:
    path=Path(file)
    if path.exists():
        print(file, 'TAIL', '\n'.join(path.read_text(errors='replace').splitlines()[-12:]))
