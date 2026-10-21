"""Read the running main dashboard without emitting authentication material."""
import json
from pathlib import Path
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE.parent / 'g3_selected_ten_20261013/helpers'))
import psutil
import fno_v13_v10_g_3_contract as contract

found = []
for process in psutil.process_iter(['pid', 'name']):
    if 'python' not in (process.info['name'] or '').lower():
        continue
    try:
        if Path(process.cwd()).resolve() == ROOT.resolve() and any(Path(v).name == 'log_dashboard_server.py' for v in process.cmdline()):
            found.append(process)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
assert len(found) == 1
token = found[0].environ()['LOG_DASH_TOKEN']
with urlopen('http://127.0.0.1:8787/api/snapshot?' + urlencode(dict(token=token, lines=80)), timeout=60) as response:
    payload = json.load(response)
card = next(item for item in payload['items'] if item['id'] == 'backtesting_result_v13_v10_g')
assert card['exists'] and contract.STRATEGY_VERSION in card['tail'] and '2026-10-13' in card['tail']
assert card['status']['strategy_version'] == contract.STRATEGY_VERSION
assert card['status']['strategy_fingerprint'] == contract.strategy_fingerprint()
assert card['status']['state'] == 'SUCCESS'
assert 'Executed trades: 15' in card['tail'] and '-2544.198834' in card['tail']
assert card['status']['scheduler_next_run'].endswith('17:15:00')
result = dict(status='PASS', id=card['id'], exists=True, strategy_version_present=True,
    session_date_present=True, tail=card['tail'], card_status=card['status'])
(HERE/'main_daily_card_validation.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps({k:v for k,v in result.items() if k not in ('tail', 'card_status')}, indent=2))
