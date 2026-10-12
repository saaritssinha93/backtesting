"""Authenticated local dashboard reads; no broker, arm or runtime writes."""
import base64
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fno_v13_v10_g_3_live_execution_policy import execution_metadata

authorization = "Basic " + base64.b64encode((os.environ["LOG_DASH_USER"] + ":" + os.environ["LOG_DASH_PASS"]).encode()).decode()
day = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()


def get(port, path):
    with urlopen(Request(f"http://127.0.0.1:{port}{path}", headers={"Authorization": authorization}), timeout=45) as response:
        assert response.status == 200
        return response.read()


checks = []
for port in (8787, 8790):
    for mode in ("LIVE", "PAPER"):
        data = json.loads(get(port, f"/api/cockpit?date={day}&mode={mode}"))
        assert data["identity"]["profile"] == "V13_V10_G_3"
        assert data["identity"]["configured_live_execution_policy"] == execution_metadata()
        assert data["read_only"] is True
        if mode == "LIVE":
            assert data["sizing"]["capital_per_entry_rs"] == 10000
            assert data["sizing"]["leverage"] == 5
            assert data["sizing"]["target_exposure_rs"] == 50000
        checks.append(dict(port=port, mode=mode, status="PASS", sizing_label=data["sizing"]["label"]))
snapshot = json.loads(get(8787, "/api/snapshot?lines=5"))
assert snapshot["active_strategy"]["live_execution_policy"] == execution_metadata()
assert snapshot["active_strategy"]["live_activation_authorized"] is False
assert snapshot["active_strategy"]["deployment_stage"] == "PAPER_VALIDATION"
html = get(8787, "/").decode("utf-8")
assert "V13-V10-G-3 Live Kite - ₹10,000 × 5" in html
assert "V13-V10-G-3 Live Kite Qty 1" not in html
checks.append(dict(port=8787, status="PASS", active_profile="V13_V10_G_3", live_activation_authorized=False))
Path(__file__).with_name("g3_live_sizing_dashboard_checks.json").write_text(json.dumps(checks, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(checks, indent=2))
