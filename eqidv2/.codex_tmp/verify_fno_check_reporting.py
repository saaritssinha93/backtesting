"""Local-only, read-only deployment checks; never print dashboard credentials."""
import base64
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
launcher = (ROOT / "bat/run_log_dashboard_server.bat").read_text(encoding="utf-8")


def setting(name):
    found = re.search(r'set "' + re.escape(name) + r'=([^"\r\n]*)"', launcher, re.I)
    value = os.environ.get(name) or (found.group(1) if found else "")
    if not value:
        raise RuntimeError("Dashboard authentication configuration unavailable")
    return value


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else "2026-10-08"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", day)
    url = "http://127.0.0.1:8787/api/fno-monitor?date=" + day
    try:
        with urlopen(url, timeout=15):
            raise AssertionError("Expected authentication challenge")
    except HTTPError as exc:
        assert exc.code == 401
    auth = "Basic " + base64.b64encode((setting("LOG_DASH_USER") + ":" + setting("LOG_DASH_PASS")).encode()).decode()
    with urlopen(Request(url, headers={"Authorization": auth}), timeout=45) as response:
        payload = json.load(response)
        assert response.headers["Cache-Control"] == "no-store"
    assert payload["session_date"] == day
    examples = []
    rows = payload["rows_5m"] + payload["rows_1m"]
    for row in rows:
        for check in row["checks"]:
            if check["name"].startswith("gate_"):
                assert "actual_text" in check and "required_text" in check
            if row["symbol"] == "BANKINDIA" and row["minute"] == "09:26" and check["status"] == "FAIL":
                examples.append({"symbol": row["symbol"], "minute": row["minute"],
                    **{key: check.get(key) for key in ("name", "label", "status", "actual_text", "required_text", "margin_text")}})
    with urlopen(Request("http://127.0.0.1:8787/", headers={"Authorization": auth}), timeout=30) as response:
        markup = response.read().decode("utf-8")
    assert "renderCheckIssues" in markup and "fno-check-issues" in markup
    assert "Shortfall / margin" in markup
    print(json.dumps({"status": "PASS", "authentication": "REQUIRED", "day": day,
        "rows_5m": len(payload["rows_5m"]), "rows_1m": len(payload["rows_1m"]),
        "updated_html": True, "examples": examples}, indent=2))


if __name__ == "__main__":
    main()
