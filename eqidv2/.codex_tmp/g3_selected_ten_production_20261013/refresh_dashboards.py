"""Restart only the two verified local dashboard listeners and verify their APIs."""
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'g3_selected_ten_20261013/helpers'))
import psutil

ROOT = HERE.parents[1]
records = {}
duplicates = {}
for process in psutil.process_iter(['pid', 'name']):
    if 'python' not in (process.info['name'] or '').lower():
        continue
    try:
        argv, env, cwd = process.cmdline(), process.environ(), Path(process.cwd())
        if cwd.resolve() != ROOT.resolve():
            continue
        names = {Path(arg).name for arg in argv if arg.endswith('.py')}
        port = 8787 if 'log_dashboard_server.py' in names else 8790 if 'run_dashboard_flow.py' in names else None
        if port is None:
            continue
        if '--port' in argv and int(argv[argv.index('--port')+1]) != port:
            continue
        record = (process, argv, env, cwd)
        duplicates.setdefault(port, []).append(record)
        if port not in records or process.create_time() > records[port][0].create_time():
            records[port] = record
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        continue
assert set(records) == {8787, 8790}, sorted(records)
token = records[8787][2].get('LOG_DASH_TOKEN')
assert token, 'Keep the existing main-dashboard authentication token'
receipt = []
for port in (8787, 8790):
    process, argv, env, cwd = records[port]
    # Reuse existing authentication for the same loopback-only dashboard.
    if port == 8790:
        env['LOG_DASH_TOKEN'] = token
    for duplicate, _, _, _ in duplicates[port]:
        duplicate.terminate()
        duplicate.wait(timeout=10)
    with (HERE / f'dashboard_{port}.stdout.log').open('ab') as out, (HERE / f'dashboard_{port}.stderr.log').open('ab') as err:
        new = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
            stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
    endpoint = f'http://127.0.0.1:{port}/api/dashboard-flow?' + urlencode({'token': token, 'strategy':'V13-V10-G-3'})
    for attempt in range(30):
        try:
            with urlopen(endpoint, timeout=10) as response:
                payload = json.load(response)
            break
        except Exception:
            if new.poll() is not None or attempt == 29:
                raise RuntimeError(f'Dashboard {port} did not restart') from None
            time.sleep(1)
    chosen, stats = payload['selected_run'], payload['summary']
    assert chosen['active_setup_count'] == 32 and chosen['strategy_matches_backtest'] and chosen['strategy_matches_active']
    assert chosen['strategy_version'] == 'V13-V10-G-3-PRODUCTION-V5'
    assert chosen['current_full_strategy'] and stats['trades'] == 257
    assert abs(stats['net_pnl'] - 743590.4379964245) < .02
    receipt.append(dict(port=port, previous_pid=process.pid, pid=new.pid, state=payload['state'],
        run_id=chosen['id'], strategy_version=chosen['strategy_version'], sessions=stats['sessions'], trades=stats['trades'], net_pnl=stats['net_pnl']))
(HERE / 'dashboard_validation.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt, indent=2))
