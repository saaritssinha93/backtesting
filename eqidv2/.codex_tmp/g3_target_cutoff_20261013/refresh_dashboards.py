"""Reload the two existing dashboard processes and verify backtest-only scope."""
from pathlib import Path
import json
import subprocess
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE.parent / 'g3_selected_ten_20261013/helpers'))
import psutil
from research import g3_target_cutoff_contract as backtest
import fno_v13_v10_g_3_contract as production
DATA = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def processes():
    found = {}
    for p in psutil.process_iter(['pid', 'name']):
        if 'python' not in (p.info['name'] or '').lower():
            continue
        try:
            if Path(p.cwd()).resolve() != ROOT.resolve():
                continue
            args = p.cmdline()
            names = {Path(a).name for a in args if a.endswith('.py')}
            port = 8787 if 'log_dashboard_server.py' in names else 8790 if 'run_dashboard_flow.py' in names else None
            if port is None or ('--port' in args and int(args[args.index('--port') + 1]) != port):
                continue
            assert port not in found, 'Duplicate dashboard process'
            found[port] = p
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return found


def api(port, token, endpoint, **query):
    # Never print credential-bearing URLs or process environments.
    try:
        with urlopen(f'http://127.0.0.1:{port}{endpoint}?' + urlencode(dict(token=token, **query)), timeout=45) as response:
            return json.load(response)
    except Exception:
        raise RuntimeError(f'Dashboard {port} {endpoint} request failed') from None


found = processes()
assert set(found) == {8787, 8790}
main, flow = found[8787], found[8790]
parent = main.parent()
assert parent and parent.name().lower() == 'cmd.exe'
assert any('run_log_dashboard_server.bat' in a.lower() for a in parent.cmdline())
token = main.environ().get('LOG_DASH_TOKEN')
flow_args, flow_env = flow.cmdline(), flow.environ()
assert token and flow_env.get('LOG_DASH_TOKEN') == token
old_pids = {port: p.pid for port, p in found.items()}
if '--verify-only' not in sys.argv:
    main.terminate(); main.wait(timeout=10)
    flow.terminate(); flow.wait(timeout=10)
    if '--no-browser' not in flow_args:
        flow_args.append('--no-browser')
    with (HERE / 'flow.stdout.log').open('ab') as out, (HERE / 'flow.stderr.log').open('ab') as err:
        child = subprocess.Popen(flow_args, cwd=ROOT, env=flow_env, stdin=subprocess.DEVNULL,
            stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
    for attempt in range(50):
        found = processes()
        if set(found) == {8787, 8790} and all(found[p].pid != old_pids[p] for p in found):
            try:
                for port in found:
                    api(port, token, '/api/dashboard-flow', strategy='V13-V10-G-3')
                break
            except RuntimeError:
                pass
        assert child.poll() is None, 'Flow dashboard exited'
        time.sleep(1)
    else:
        raise RuntimeError('Dashboard reload timed out')

history = read(DATA / read(DATA / 'latest/current_backtest.json')['run_dir'] / 'summary.json')
daily_pointer = read(DATA / 'latest/current_run.json')
daily = read(DATA / daily_pointer['run_dir'] / 'run_manifest.json')
checks = []
for port in found:
    payload = api(port, token, '/api/dashboard-flow', strategy='V13-V10-G-3')
    selected, metrics = payload['selected_run'], payload['summary']
    assert selected['strategy_version'] == backtest.STRATEGY_VERSION and selected['active_setup_count'] == 46
    assert selected['strategy_fingerprint'] == backtest.strategy_fingerprint()
    assert selected['strategy_matches_backtest'] and selected['strategy_matches_active'] is False
    assert metrics['sessions'] == history['sessions'] and metrics['trades'] == history['current_g3']['executed_trades']
    assert abs(metrics['net_pnl'] - history['current_g3']['net_pnl_rs']) < .02
    assert any('59' in message and ('PAPER' in message or 'paper' in message) for message in payload['warnings'])
    candidates = [r for r in payload['runs'] if r.get('kind') == 'production_daily'
        and r.get('period_end') == '2026-10-13' and r.get('strategy_version') == backtest.STRATEGY_VERSION]
    assert candidates
    today = api(port, token, '/api/dashboard-flow', run=candidates[0]['id'])
    assert today['summary']['trades'] == daily['metrics']['trades']
    assert abs(today['summary']['net_pnl'] - daily['metrics']['net_profit_rupees']) < .02
    for mode in ('PAPER', 'LIVE'):
        cockpit = api(port, token, '/api/cockpit', date='2026-10-14', mode=mode)
        assert cockpit['identity']['version'] == production.STRATEGY_VERSION
    checks.append(dict(port=port, backtest_setups=46, paper_live_version=production.STRATEGY_VERSION,
        trades=metrics['trades'], net_pnl=metrics['net_pnl'], daily_trades=today['summary']['trades'],
        daily_net_pnl=today['summary']['net_pnl']))
snapshot = api(8787, token, '/api/snapshot', lines=80)
card = next(row for row in snapshot['items'] if row['id'] == 'backtesting_result_v13_v10_g')
assert card['status']['strategy_version'] == backtest.STRATEGY_VERSION
assert card['status']['strategy_fingerprint'] == backtest.strategy_fingerprint()
assert card['status']['state'] == 'SUCCESS'
receipt = dict(status='PASS', dashboards=checks, main_daily_card_matches=True)
(HERE / 'dashboard_validation.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
print(json.dumps(receipt, indent=2))
