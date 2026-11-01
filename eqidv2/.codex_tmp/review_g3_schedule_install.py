from pathlib import Path
import sys
import win32com.client
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.repair_g3_standing_schedule import prepared_xml, TASK, TARGET
s=win32com.client.Dispatch('Schedule.Service'); s.Connect(); f=s.GetFolder('\\')
out=ROOT/'outputs/readiness_repair_20261102'
a,b=f.GetTask(TASK).Xml,f.GetTask(TARGET).Xml
(out/'standing_before.xml').write_text(a,encoding='utf-8')
(out/'live_target_before.xml').write_text(b,encoding='utf-8')
prepared_xml(a,b)
print('PASS')
