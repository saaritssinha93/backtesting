import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path
import re
import sys
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_flow_observer_history import history_path
from dashboard_flow_observer_store import read_json, write_json
root = Path(r'C:\TradingData\eqidv2')
launcher = Path('bat/run_log_dashboard_server.bat').read_text()
user = re.search(r'set "LOG_DASH_USER=([^"\r\n]+)"', launcher)[1]
password = re.search(r'set "LOG_DASH_PASS=([^"\r\n]+)"', launcher)[1]
authorization = 'Basic '+base64.b64encode((user+':'+password).encode()).decode()

def verify(args):
    port, day = args
    request = Request(f'http://127.0.0.1:{port}/api/dashboard-flow/live?date={day}', headers={'Authorization': authorization})
    with urlopen(request, timeout=150) as response:
        payload = json.load(response)
    assert payload['opening_observer']['enabled']
    slots = {slot['time']: slot for slot in payload['slots']}
    assert all(row.get('opening_context', {}).get('first_minute_oi') is None for row in slots['09:15']['rows'])
    checked = 0
    for row in slots['09:20']['rows']:
        context = row.get('opening_context') or {}
        if context.get('first_minute_oi') is None:
            continue
        saved = read_json(root, history_path(date.fromisoformat(day), row['contract']))
        first = next(item for item in saved['candles'] if item['candle_start'].startswith(day+'T09:15:00'))
        assert context['first_minute_oi'] == first['oi']
        assert context['first_minute_end'] == first['candle_end']
        checked += 1
    assert checked > 0
    return {'port': port, 'session_date': day, 'visible_first_minutes_verified': checked,
            'first_minutes_at_0915': 0, 'opening_state': payload['opening_observer']['state']}

with ThreadPoolExecutor(max_workers=2) as pool:
    checks = list(pool.map(verify, [(8787,'2026-07-29'),(8787,'2026-08-31'),(8787,'2026-09-30'),(8790,'2026-10-12')]))
write_json(root, 'coverage/http_range_2026-07-28_2026-10-12.json', checks)
print(json.dumps(checks, indent=2))
