"""Reload only the verified two dashboard servers, then verify saved evidence."""
from html.parser import HTMLParser
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode, urlparse, unquote
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE.parent / "g3_selected_ten_20261013/helpers"))
import psutil
import fno_v13_v10_g_3_contract as contract

DATA = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3")


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def records():
    found = {}
    for process in psutil.process_iter(["pid", "name"]):
        if "python" not in (process.info["name"] or "").lower():
            continue
        try:
            if Path(process.cwd()).resolve() != ROOT.resolve():
                continue
            argv = process.cmdline()
            names = {Path(arg).name for arg in argv if arg.endswith(".py")}
            port = 8787 if "log_dashboard_server.py" in names else 8790 if "run_dashboard_flow.py" in names else None
            if port is None or ("--port" in argv and int(argv[argv.index("--port")+1]) != port):
                continue
            if port in found:
                raise RuntimeError(f"Multiple dashboard processes for port {port}; no automatic restart")
            found[port] = process
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return found


def api(port, token, endpoint, **query):
    # Existing credentials remain in memory, never emitted in receipts/errors.
    with urlopen(f"http://127.0.0.1:{port}{endpoint}?" + urlencode(dict(token=token, **query)), timeout=45) as response:
        assert response.status == 200
        return json.load(response)


found = records()
assert set(found) == {8787, 8790}
main, flow = found[8787], found[8790]
parent = main.parent()
assert parent and parent.name().lower() == "cmd.exe"
assert any("run_log_dashboard_server.bat" in value.lower() for value in parent.cmdline())
token = main.environ().get("LOG_DASH_TOKEN")
flow_argv, flow_env = flow.cmdline(), flow.environ()
assert token and flow_env.get("LOG_DASH_TOKEN") == token, "Dashboard credentials must already match"
old_pids = {port:p.pid for port,p in found.items()}

# Let the verified existing main-dashboard batch supervisor restart its child;
# do not create a competing second main server. The flow server is standalone.
verify_only = "--verify-only" in sys.argv
if verify_only:
    current = found
    payloads = {port:api(port, token, "/api/dashboard-flow", strategy="V13-V10-G-3") for port in current}
else:
    main.terminate()
    main.wait(timeout=10)
    flow.terminate()
    flow.wait(timeout=10)
    if "--no-browser" not in flow_argv:
        flow_argv.append("--no-browser")
    with (HERE / "dashboard_8790.stdout.log").open("ab") as out, (HERE / "dashboard_8790.stderr.log").open("ab") as err:
        new_flow = subprocess.Popen(flow_argv, cwd=ROOT, env=flow_env, stdin=subprocess.DEVNULL,
            stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
    for attempt in range(55):
        try:
            current = records()
            if set(current) == {8787, 8790} and all(current[port].pid != old_pids[port] for port in current):
                payloads = {port:api(port, token, "/api/dashboard-flow", strategy="V13-V10-G-3") for port in current}
                break
        except Exception:
            pass
        if new_flow.poll() is not None or attempt == 54:
            raise RuntimeError("Verified dashboard restart did not become ready") from None
        time.sleep(1)

backtest_pointer = read(DATA / "latest/current_backtest.json")
backtest_dir = DATA / backtest_pointer["run_dir"]
summary = read(backtest_dir / "summary.json")
daily = read(HERE / "daily_validation.json")
expected_pnl = float(__import__("pandas").read_csv(backtest_dir / "daily_results.csv").net_pnl.sum())
expected_trades = int(__import__("pandas").read_csv(backtest_dir / "daily_results.csv").trades.sum())
receipt = []
for port, payload in payloads.items():
    chosen, stats = payload["selected_run"], payload["summary"]
    assert chosen["active_setup_count"] == 44
    assert chosen["strategy_version"] == contract.STRATEGY_VERSION
    assert chosen["strategy_fingerprint"] == contract.strategy_fingerprint()
    assert chosen["strategy_matches_active"] and chosen["strategy_matches_backtest"] and chosen["current_full_strategy"]
    assert stats["sessions"] == summary["sessions"] == 50
    assert stats["trades"] == expected_trades and abs(stats["net_pnl"] - expected_pnl) < .02
    assert any("44-setup decision configuration" in item for item in payload["warnings"])
    candidates = [row for row in payload["runs"] if row.get("kind") == "production_daily"
        and row.get("period_end") == "2026-10-13" and row.get("strategy_version") == contract.STRATEGY_VERSION]
    assert candidates
    selected_daily = api(port, token, "/api/dashboard-flow", run=candidates[0]["id"])
    daily_stats = selected_daily["summary"]
    assert daily_stats["sessions"] == 1 and daily_stats["trades"] == daily["daily_metrics"]["trades"]
    assert abs(daily_stats["net_pnl"] - daily["daily_metrics"]["net_profit_rupees"]) < .02
    modes = []
    for mode in ("PAPER", "LIVE"):
        cockpit = api(port, token, "/api/cockpit", date="2026-10-14", mode=mode)
        assert cockpit["identity"]["version"] == contract.STRATEGY_VERSION
        assert cockpit["session_date"] == "2026-10-14" and cockpit["mode"] == mode
        assert cockpit["evidence"]["state"] == "NO_EVIDENCE" and cockpit["summary"]["orders"] == 0
        assert not cockpit["orders"] and not cockpit["events"]
        modes.append(dict(mode=mode, evidence="NO_EVIDENCE", orders=0))
    receipt.append(dict(port=port, previous_pid=None if verify_only else old_pids[port], pid=current[port].pid,
        supervised=port==8787, strategy_version=chosen["strategy_version"], active_setup_count=44,
        run_id=chosen["id"], sessions=stats["sessions"], trades=stats["trades"], net_pnl=stats["net_pnl"],
        daily_run_id=selected_daily["selected_run"]["id"], daily_trades=daily_stats["trades"],
        daily_net_pnl=daily_stats["net_pnl"], tomorrow_cockpit=modes))

class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links=[]
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.links.extend(value for key,value in attrs if key == "href")

links = Links()
links.feed((backtest_dir / "report.html").read_text(encoding="utf-8"))
verified_links = []
for value in links.links:
    parsed = urlparse(value)
    if not parsed.scheme and parsed.path:
        target = (backtest_dir / unquote(parsed.path)).resolve()
        assert target.is_file(), value
        verified_links.append(value)
result = dict(status="PASS", dashboards=receipt, report=str(backtest_dir / "report.html"), verified_report_links=verified_links)
(HERE / "dashboard_validation.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
