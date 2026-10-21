"""Reload the identified dashboard child and verify the RELIANCE display payload."""
import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".codex_tmp/g3_selected_ten_20261013/helpers"))
import psutil


def server():
    found = []
    for process in psutil.process_iter(["pid", "name"]):
        try:
            if "python" not in (process.info["name"] or "").lower():
                continue
            args = process.cmdline()
            if not any(Path(arg).name == "log_dashboard_server.py" for arg in args):
                continue
            if Path(process.cwd()).resolve() != ROOT.resolve():
                continue
            if "--port" not in args or args[args.index("--port") + 1] != "8787":
                continue
            found.append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if len(found) > 1:
        raise RuntimeError("Multiple matching dashboard servers")
    return found[0] if found else None


def fetch(token, endpoint, **parameters):
    url = "http://127.0.0.1:8787" + endpoint + "?" + urlencode(dict(token=token, **parameters))
    try:
        with urlopen(url, timeout=45) as response:
            return response.read()
    except Exception:
        raise RuntimeError("Dashboard verification request failed") from None


def main():
    current = server()
    assert current is not None, "Dashboard server is not running"
    supervisor = current.parent()
    assert supervisor and supervisor.name().lower() == "cmd.exe"
    assert any("run_log_dashboard_server.bat" in arg.lower() for arg in supervisor.cmdline())
    token = current.environ().get("LOG_DASH_TOKEN")
    assert token, "Existing dashboard authentication is unavailable"
    # Confirm authentication and endpoint before touching the verified child.
    before = json.loads(fetch(token, "/api/fno-monitor", date="2026-10-15"))
    assert any(row.get("symbol") == "RELIANCE" and row.get("minute") == "10:11"
               for row in before["rows_1m"])
    source = Path("C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/confirmation_1m/2026-10-15/slot_1011.json")
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    old_pid = current.pid
    current.terminate()
    current.wait(timeout=10)
    ready = None
    for _ in range(45):
        candidate = server()
        if candidate is not None and candidate.pid != old_pid:
            try:
                data = json.loads(fetch(token, "/api/fno-monitor", date="2026-10-15"))
                ready = candidate
                break
            except RuntimeError:
                pass
        time.sleep(1)
    assert ready is not None, "Dashboard supervisor did not restart its child"
    row = next(row for row in data["rows_1m"]
               if row.get("symbol") == "RELIANCE" and row.get("minute") == "10:11"
               and row.get("stage") == "1M_CONFIRMATION_SETUP")
    gates = [check for check in row["checks"] if check["name"] != "final_selection"]
    failed = [check["name"] for check in gates if check["status"] == "FAIL"]
    skipped = [check for check in gates if check["status"] == "NOT_APPLICABLE"]
    assert row["setup_status"] == "NOT_CONFIGURED"
    assert row["decision_reason"] == "No LONG setup configured for 10:10."
    assert row["decision"] == "CONFIRMATION_OR_SETUP_REJECTED"
    assert failed == ["gate_setup_exists"] and len(skipped) == 7
    assert all(check["recorded_status"] == "FAIL" for check in skipped)
    html = fetch(token, "/").decode("utf-8")
    assert "No configured setup" in html and "Check result / recorded flag" in html
    assert "setup clock and threshold checks require a configured setup" in html
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
    receipt = dict(status="PASS", port=8787, previous_pid=old_pid, current_pid=ready.pid,
                   reason=row["decision_reason"], failed_checks=len(failed), not_applicable_checks=len(skipped),
                   original_evidence_unchanged=True, updated_ui_served=True)
    (Path(__file__).parent / "verification.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
