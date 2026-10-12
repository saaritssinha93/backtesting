"""Authenticated read-only checks; never prints authentication material."""
import base64
import argparse
import json
import math
import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

parser = argparse.ArgumentParser()
parser.add_argument("--require-history", action="store_true")
parser.add_argument("--public-url")
args = parser.parse_args()
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fno_v13_v10_g_3_contract import STRATEGY_VERSION, strategy_fingerprint
from ai_platform.observability.g3_analysis import verified_package
expected = None
if args.require_history:
    _, expected, _ = verified_package(Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3"))

authorization = "Basic " + base64.b64encode((os.environ["LOG_DASH_USER"] + ":" + os.environ["LOG_DASH_PASS"]).encode()).decode()
results = []
for port in (8787, 8790):
    base = f"http://127.0.0.1:{port}"
    for path in ("/", "/dashboard-flow", "/api/dashboard-flow", "/api/cockpit?date=2026-10-09&mode=PAPER"):
        with urlopen(Request(base + path, headers={"Authorization": authorization}), timeout=45) as response:
            body = response.read()
            record = dict(port=port, path=path, status=response.status, bytes=len(body))
            if path == "/api/dashboard-flow":
                data = json.loads(body)
                assert data["selected_run"]["verified"] is True
                assert data["selected_run"]["strategy_version"] == STRATEGY_VERSION
                assert data["selected_run"]["strategy_fingerprint"] == strategy_fingerprint()
                if expected:
                    assert data["selected_run"]["role"] == "cumulative_history"
                    assert data["selected_run"]["sessions"] == expected["metrics"]["sessions"]
                    assert data["summary"]["trades"] == expected["metrics"]["trades"]
                    assert math.isclose(data["summary"]["net_pnl"], expected["metrics"]["net_profit_rupees"], abs_tol=.02)
                    assert data["selected_run"]["period_end"] == expected["session_date"]
                record.update(selected=data["selected_run"], summary=data["summary"], warnings=data["warnings"])
            elif path.startswith("/api/cockpit"):
                data = json.loads(body)
                record.update(identity=data.get("identity"), state=data.get("state"))
            results.append(record)
    try:
        urlopen(base + "/api/dashboard-flow", timeout=10)
        raise AssertionError("Unauthenticated dashboard unexpectedly available")
    except HTTPError as error:
        assert error.code == 401
        results.append(dict(port=port, unauthenticated_status=401))
with urlopen(Request("http://127.0.0.1:8787/api/snapshot?lines=20", headers={"Authorization": authorization}), timeout=60) as response:
    data = json.load(response)
    assert data["active_strategy"]["profile"] == "V13_V10_G_3"
    assert data["active_strategy"]["deployment_stage"] == "PAPER_VALIDATION"
    assert data["active_strategy"]["live_activation_authorized"] is False
    cards = {item["id"]: item for item in data["items"]}
    backtest = cards["backtesting_result_v13_v10_g"]
    assert "VERIFIED_PRODUCTION_DAILY" in backtest["tail"]
    assert "v13_v10_g_3_1620" in backtest["status"].get("scheduler_tasks", "")
    if expected:
        assert cards["fno_v13_v10_g_research_baseline"]["status"]["status"] == "READY"
        assert cards["fno_v13_v10_g_research_predictions"]["status"]["status"] == "AWAITING_G3_PROSPECTIVE_EVIDENCE"
        assert cards["fno_v13_v10_g_research_shadow"]["status"]["status"] == "AWAITING_G3_PROSPECTIVE_EVIDENCE"
    results.append(dict(port=8787, path="/api/snapshot", active_strategy=data.get("active_strategy"),
                        active_cards=[dict(id=item["id"], file_name=item.get("file_name"), status=item.get("status"))
                                      for item in data["items"] if item["id"] in {"backtesting_result_v13_v10_g", "fno_v13_v10_g_scanner_5min", "kite_trade_fno_id_v13_v10_g"}]))
if args.public_url:
    for path in ("/", "/dashboard-flow", "/api/dashboard-flow"):
        with urlopen(Request(args.public_url.rstrip("/") + path, headers={"Authorization": authorization}), timeout=60) as response:
            body = response.read()
            assert response.status == 200
            if path == "/api/dashboard-flow":
                data = json.loads(body)
                assert data["selected_run"]["verified"] is True
                if expected:
                    assert data["selected_run"]["role"] == "cumulative_history"
                    assert math.isclose(data["summary"]["net_pnl"], expected["metrics"]["net_profit_rupees"], abs_tol=.02)
            results.append(dict(port="public", path=path, status=response.status))
Path(__file__).with_name("running_dashboard_checks.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
print(json.dumps([dict(port=item["port"], path=item.get("path"), status=item.get("status", item.get("unauthenticated_status")), active_profile=(item.get("active_strategy") or item.get("identity") or {}).get("profile"), selected_role=(item.get("selected") or {}).get("role")) for item in results], indent=2))
