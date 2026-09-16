"""Authenticated local dashboard check; credentials are never printed or saved."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import log_dashboard_server as dashboard

bat = (ROOT / 'bat/run_log_dashboard_server.bat').read_text(encoding='utf-8')
def setting(name):
    return os.environ.get(name) or re.search(r'set "' + name + r'=([^"\r\n]*)"', bat).group(1)

authorization = 'Basic ' + base64.b64encode((setting('LOG_DASH_USER') + ':' + setting('LOG_DASH_PASS')).encode()).decode()
responses = {}
for route in ('/', '/api/snapshot?lines=30'):
    started = time.monotonic()
    with urlopen(Request('http://127.0.0.1:8787' + route, headers={'Authorization': authorization}), timeout=30) as response:
        body = response.read().decode('utf-8')
        responses[route] = dict(http_status=response.status, elapsed_seconds=round(time.monotonic()-started, 3), body=body)

html = responses['/']['body']
assert 'Backtesting result v13-v10-G' in html
assert 'Backtesting result v6/v8/v10/v11/v12' not in html
assert '"backtesting_result_v11"' not in html
items = {row['id']: row for row in json.loads(responses['/api/snapshot?lines=30']['body'])['items']}
card = items['backtesting_result_v13_v10_g']
assert 'backtesting_result_v11' not in items
assert 'backtesting_result_v13_v10_g' in card['file_name']
assert 'SKIPPED_NON_TRADING_DAY' in card['tail'] and '2026-09-14' in card['tail']
assert card['status']['session_date'] == '2026-09-14'
assert card['status']['status'] == 'SKIPPED_NON_TRADING_DAY'
assert '15-09-2026 16:20:00' == card['status']['scheduler_next_run']
assert 'backtesting_result_v13_v10_g_1620' in json.dumps(card['status'])
prior = json.loads(Path('C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/session_rename_20260914/dashboard_verification.json').read_text())
for previous in prior['views']:
    assert items[previous['id']]['status']['strategy_identity_state'] == 'MATCH'
identity = json.loads(dashboard.DASHBOARD_RUNTIME_IDENTITY_PATH.read_text())
assert identity['source_sha256'] == hashlib.sha256((ROOT / 'log_dashboard_server.py').read_bytes()).hexdigest()
proof = dict(
    responses={route: {key: value for key, value in data.items() if key != 'body'} for route, data in responses.items()},
    dashboard_pid=identity['pid'], source_sha256=identity['source_sha256'],
    session_id=card['id'], file_name=card['file_name'], status=card['status']['status'],
    session_date=card['status']['session_date'], next_run=card['status']['scheduler_next_run'],
    prior_g_views_verified=len(prior['views']), legacy_card_absent=True)
destination = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g/migration_20260914/dashboard_verification.json')
destination.write_text(json.dumps(proof, indent=2), encoding='utf-8')
print(json.dumps(proof, indent=2))
