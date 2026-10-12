import base64
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen

text = Path('bat/run_log_dashboard_server.bat').read_text()
username = re.search(r'set "LOG_DASH_USER=([^"\r\n]+)"', text)[1]
password = re.search(r'set "LOG_DASH_PASS=([^"\r\n]+)"', text)[1]
authorization = 'Basic ' + base64.b64encode((username + ':' + password).encode()).decode()

def fetch(port, path):
    with urlopen(Request(f'http://127.0.0.1:{port}{path}', headers={'Authorization': authorization}), timeout=120) as response:
        return json.load(response)

def verify(port):
    data = fetch(port, '/api/dashboard-flow/live?date=2026-10-12')
    assert data['opening_observer']['enabled']
    slots = {slot['time']: slot for slot in data['slots']}
    opening = slots['09:15']['rows']
    next_slot = slots['09:20']['rows']
    assert all(row.get('oi_change_pct') is None and row.get('change5m_pct') is None for row in opening)
    assert all(row.get('opening_context', {}).get('first_minute_oi') is None for row in opening)
    context_count = sum(row.get('opening_context', {}).get('first_minute_oi') is not None for row in next_slot)
    assert context_count > 0
    return {'port': port, 'opening_state': data['opening_observer']['state'],
            'first_minute_contexts_at_0920': context_count,
            'first_minute_contexts_at_0915': 0, 'regular_0915_changes_unavailable': True}

with ThreadPoolExecutor(max_workers=2) as pool:
    results = list(pool.map(verify, [8787, 8790]))
snapshot = fetch(8787, '/api/snapshot?lines=10')
items = [row for row in snapshot['items'] if row['id'].startswith('dashboard_flow_opening_')]
assert len(items) == 2
assert all(row['status']['execution_mode'] == 'REPORTING_ONLY' for row in items)
results.append({'session_cards': [{'id': row['id'], 'state': row['status']['status'], 'phase': row['status']['phase']} for row in items]})
Path('outputs/opening-deployment-qa.json').write_text(json.dumps(results, indent=2))
print(json.dumps(results, indent=2))
