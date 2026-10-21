"""Verify the refreshed G3 monitor APIs without exposing dashboard credentials."""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE.parent / "g3_selected_ten_20261013" / "helpers"))
import psutil


def dashboard_processes():
    result = {}
    for process in psutil.process_iter(["pid", "name"]):
        if "python" not in (process.info["name"] or "").lower():
            continue
        try:
            if Path(process.cwd()).resolve() != ROOT.resolve():
                continue
            args = process.cmdline()
            names = {Path(value).name for value in args if value.endswith(".py")}
            port = 8787 if "log_dashboard_server.py" in names else 8790 if "run_dashboard_flow.py" in names else None
            if port is not None:
                result[port] = process
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return result


def fetch(port, token, path, **query):
    parameters = urlencode(dict(token=token, **query))
    try:
        with urlopen(f"http://127.0.0.1:{port}{path}?{parameters}", timeout=45) as response:
            return response.read(), response.headers.get_content_type()
    except Exception:
        raise RuntimeError(f"Dashboard {port} failed to serve {path}") from None


def load_json(port, token, path, **query):
    body, content_type = fetch(port, token, path, **query)
    assert content_type == "application/json"
    return json.loads(body)


processes = dashboard_processes()
assert set(processes) == {8787, 8790}
token = processes[8787].environ().get("LOG_DASH_TOKEN")
assert token and processes[8790].environ().get("LOG_DASH_TOKEN") == token

try:
    urlopen("http://127.0.0.1:8787/api/g3-trade-monitor", timeout=10)
except HTTPError as error:
    assert error.code in {401, 403}
else:
    raise AssertionError("The monitor API accepted an unauthenticated request")

checks = []
for port in (8787, 8790):
    snapshot = load_json(port, token, "/api/g3-trade-monitor", date="2026-10-14")
    assert snapshot["read_only"] is True and snapshot["advisory_only"] is True
    assert snapshot["execution_authority"] is False and snapshot["refresh_seconds"] == 15
    assert snapshot["state"] == "READY"
    assert snapshot["market_data_feed"]["state"] in {"READY", "PARTIAL_MARKET_DATA"}
    for mode in ("PAPER", "LIVE"):
        book = snapshot["modes"][mode]
        assert book["open_count"] == book["open"]
        assert 0 <= book["eligible_count"] <= book["open_count"]
        assert sum(book["open_direction_counts"].values()) == book["open_count"]
        assert book["gross_marked_pnl_coverage_count"] <= book["open_count"]
        if book["gross_marked_pnl_rs"] is not None:
            assert book["gross_marked_pnl_coverage_count"] == book["open_count"]
    research = load_json(port, token, "/api/g3-trade-monitor/backtest")
    assert research["state"] == "VERIFIED" and research["execution_authority"] is False
    assert research["price_granularity"] == "COMPLETED_1M"
    assert len(research["policy_results"]) == 12 and len(research["daily_comparison"]) == 200
    assert {row["policy"] for row in research["policy_results"]} == {
        "baseline", "trend_failure", "profit_giveback", "combined"
    }
    html, html_type = fetch(port, token, "/")
    text = html.decode("utf-8")
    assert html_type == "text/html"
    assert "G3 Trade Monitor ·15s" in text and "#g3-trade-monitor" in text
    report, report_type = fetch(port, token, "/g3-trade-monitor-backtest/report.html")
    assert report_type == "text/html" and b"G3 fixed advisory exit research" in report
    serialized = json.dumps({"snapshot": snapshot, "research": research})
    assert not re.search(r"[A-Za-z]:[\\/]", serialized)
    checks.append({
        "port": port,
        "snapshot_state": snapshot["state"],
        "feed_state": snapshot["market_data_feed"]["state"],
        "paper_open": snapshot["modes"]["PAPER"]["open_count"],
        "paper_overall": snapshot["modes"]["PAPER"]["overall_direction"],
        "live_open": snapshot["modes"]["LIVE"]["open_count"],
        "research_rows": len(research["policy_results"]),
        "daywise_rows": len(research["daily_comparison"]),
        "report_served": True,
    })

receipt = {"status": "PASS", "authenticated": True, "unauthenticated_blocked": True,
           "execution_authority": False, "dashboards": checks}
(HERE / "dashboard_api_validation.json").write_text(
    json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(receipt, indent=2))
