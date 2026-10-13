"""Restart only the two verified local dashboard listeners and verify their APIs."""
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / 'helpers'))
import psutil

ROOT = HERE.parents[1]
listeners = {c.laddr.port: c.pid for c in psutil.net_connections(kind='inet')
             if c.status == 'LISTEN' and c.laddr.port in (8787, 8790)}
assert set(listeners) == {8787, 8790}, listeners
records = {}
for port, pid in listeners.items():
    process = psutil.Process(pid)
    argv, env, cwd = process.cmdline(), process.environ(), Path(process.cwd())
    assert cwd.resolve() == ROOT.resolve()
    expected = 'log_dashboard_server.py' if port == 8787 else 'run_dashboard_flow.py'
    assert any(Path(arg).name == expected for arg in argv), port
    records[port] = (process, argv, env, cwd)
token = records[8787][2].get('LOG_DASH_TOKEN')
assert token, 'Keep the existing main-dashboard authentication token'
receipt = []
for port in (8787, 8790):
    process, argv, env, cwd = records[port]
    # Reuse existing authentication for the same loopback-only dashboard.
    if port == 8790:
        env['LOG_DASH_TOKEN'] = token
    process.terminate()
    process.wait(timeout=10)
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
    assert chosen['active_setup_count'] == 32 and chosen['strategy_matches_backtest']
    assert chosen['current_full_strategy'] and stats['trades'] == 257
    assert abs(stats['net_pnl'] - 743590.4379964245) < .02
    receipt.append(dict(port=port, previous_pid=process.pid, pid=new.pid, state=payload['state'],
        run_id=chosen['id'], sessions=stats['sessions'], trades=stats['trades'], net_pnl=stats['net_pnl']))
(HERE / 'dashboard_validation.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt, indent=2))
