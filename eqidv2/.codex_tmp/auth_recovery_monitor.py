"""Print only stage markers and dates; never auth credentials or tokens."""
from pathlib import Path
import json
import re

log = Path('logs/authentication_v2_runner.log').read_text(errors='replace')
start = log.rfind('START authentication_v2.py')
tail = log[max(0, log.rfind('\n', 0, start)):]
patterns = ('START authentication_v2.py', 'END authentication_v2.py', 'Fresh browser login required', 'Preparing Chrome driver', 'driver ready', 'Opening broker login page', 'TOTP source:', 'Single-pass auth completed', '[AUTH-SUMMARY]', '[AUTH] Another authentication_v2')
for line in tail.splitlines():
    if any(pattern in line for pattern in patterns):
        print(line)
    elif '[WARN]' in line or '[ERROR]' in line:
        # Preserve app/attempt information, drop arbitrary exception payloads.
        tag = re.search(r'\[AUTH\d?\]', line)
        attempt = re.search(r'attempt \d+/\d+', line, re.IGNORECASE)
        print('AUTH_WARNING', tag.group() if tag else '', attempt.group() if attempt else '')
state_path = Path('auth_v2_state.json')
if state_path.exists():
    state = json.loads(state_path.read_text())
    print('STATE_DATES', json.dumps({key:value for key,value in state.items() if key.startswith(('session_date_ist','updated_at_ist'))}, sort_keys=True))
status_path = Path('logs/authentication_v2_runner.status')
if status_path.exists():
    print('RUNNER_STATUS', '\n'.join(line for line in status_path.read_text().splitlines() if line.startswith(('status=', 'ts=', 'exit_code='))))
