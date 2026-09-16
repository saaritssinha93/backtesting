"""Capture the three repaired dashboard cards without storing credentials."""
import base64
import json
import os
from pathlib import Path
import re
from urllib.request import Request, urlopen

root = Path(__file__).resolve().parents[2]
launcher = (root / "bat/run_log_dashboard_server.bat").read_text()

def setting(name):
    return os.environ.get(name) or re.search(r'set "' + name + r'=([^"\r\n]*)"', launcher).group(1)

auth = "Basic " + base64.b64encode((setting("LOG_DASH_USER") + ":" + setting("LOG_DASH_PASS")).encode()).decode()
with urlopen(Request("http://127.0.0.1:8787/api/snapshot?lines=120", headers={"Authorization": auth}), timeout=60) as response:
    snapshot = json.load(response)
names = {"backtesting_result_v13_v10_g", "v13_research_run_vintage", "v7_live_5min_monitor"}
cards = [{key: item.get(key) for key in ("id", "label", "status", "tail")} for item in snapshot["items"] if item["id"] in names]
assert len(cards) == 3
result = {"server_time": snapshot["server_time"], "cards": cards}
Path(__file__).with_name("dashboard_verification.json").write_text(json.dumps(result, indent=2))
for item in cards:
    print(item["id"], json.dumps(item["status"]))
